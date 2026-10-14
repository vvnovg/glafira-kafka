use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::{Offset, TopicPartitionList};
use serde::Deserialize;

use crate::browse::filter::{CompiledFilter, FilterSpec};
use crate::browse::format::{decode, header_text, DataFormat};
use crate::browse::plan::{
    plan_earliest, plan_from_offsets, plan_latest, plan_offset, EndTracker, PartitionBounds, PartitionPlan, StartPosition,
};
use crate::browse::types::{BrowseEvent, BrowsedMessage, FinishReason, HeaderView};
use crate::error::{from_kafka, AppError, AppResult};
use crate::kafka::config::build_client_config;
use crate::kafka::context::CapturingContext;
use crate::kafka::metadata::convert;
use crate::kafka::topics::fetch_bounds;
use crate::secrets::SecretStore;
use crate::storage::connections::ConnectionProfile;

pub trait EventSink: Send + 'static {
    /// false — получатель закрыт, сессию нужно завершить.
    fn send(&self, event: BrowseEvent) -> bool;
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowseParams {
    pub connection_id: String,
    pub topic: String,
    #[serde(default)]
    pub partitions: Option<Vec<i32>>,
    pub start: StartPosition,
    pub limit: usize,
    pub live: bool,
    pub key_format: DataFormat,
    pub value_format: DataFormat,
    #[serde(default)]
    pub filter: FilterSpec,
}

pub const MAX_LIMIT: usize = 100_000;
const BATCH_SIZE: usize = 200;
const BATCH_INTERVAL: Duration = Duration::from_millis(100);
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);
const POLL_TIMEOUT: Duration = Duration::from_millis(100);

pub fn validate(params: &BrowseParams) -> AppResult<()> {
    if params.topic.trim().is_empty() {
        return Err(AppError::invalid("Не указан топик"));
    }
    if params.limit == 0 || params.limit > MAX_LIMIT {
        return Err(AppError::invalid(format!("Лимит должен быть от 1 до {MAX_LIMIT}")));
    }
    match params.start {
        StartPosition::Latest { count } if count <= 0 => {
            Err(AppError::invalid("Количество последних сообщений должно быть больше 0"))
        }
        StartPosition::LastMinutes { minutes } if minutes <= 0 => {
            Err(AppError::invalid("Количество минут должно быть больше 0"))
        }
        _ => Ok(()),
    }
}

pub fn run_browse(
    profile: &ConnectionProfile,
    secrets: &dyn SecretStore,
    params: BrowseParams,
    cancel: Arc<AtomicBool>,
    sink: &dyn EventSink,
) {
    if let Err(error) = run_inner(profile, secrets, &params, &cancel, sink) {
        sink.send(BrowseEvent::Error { error });
    }
}

fn run_inner(
    profile: &ConnectionProfile,
    secrets: &dyn SecretStore,
    params: &BrowseParams,
    cancel: &AtomicBool,
    sink: &dyn EventSink,
) -> AppResult<()> {
    validate(params)?;
    let filter = CompiledFilter::compile(&params.filter)?;
    let ctx = CapturingContext::default();
    let consumer = create_consumer(profile, secrets, &ctx)?;
    let kerr = |e: KafkaError| from_kafka(&e, ctx.last_error().as_deref());
    let timeout = Duration::from_millis(profile.request_timeout_ms);

    let bounds = load_bounds(&consumer, params, timeout, &kerr)?;
    let plans = resolve_plans(&consumer, params, &bounds, timeout).map_err(&kerr)?;
    assign(&consumer, &params.topic, &plans, params.live).map_err(&kerr)?;

    let mut tracker = EndTracker::new(&plans);
    let mut out = Output::new(sink);
    let reason = loop {
        if cancel.load(Ordering::Relaxed) || out.closed {
            break FinishReason::Cancelled;
        }
        if !params.live && out.matched >= params.limit as u64 {
            break FinishReason::Limit;
        }
        if !params.live && tracker.all_done() {
            break FinishReason::EndOfTopic;
        }
        match consumer.poll(POLL_TIMEOUT) {
            None => {}
            Some(Err(KafkaError::PartitionEOF(p))) => tracker.mark_done(p),
            Some(Err(e)) => return Err(kerr(e)),
            Some(Ok(msg)) => {
                let in_snapshot = tracker.observe(msg.partition(), msg.offset());
                if in_snapshot || params.live {
                    out.scanned += 1;
                    let m = to_browsed(&msg, params.key_format, params.value_format);
                    if filter.matches(&m.view()) {
                        out.push(m);
                    }
                }
            }
        }
        if params.live && !out.reached_end_sent && tracker.all_done() {
            out.flush();
            out.reached_end();
        }
        out.maybe_flush();
    };
    out.flush();
    out.emit(BrowseEvent::Finished { scanned: out.scanned, matched: out.matched, reason });
    Ok(())
}

fn create_consumer(
    p: &ConnectionProfile,
    secrets: &dyn SecretStore,
    ctx: &CapturingContext,
) -> AppResult<BaseConsumer<CapturingContext>> {
    let mut cfg = build_client_config(p, secrets)?;
    // group.id обязателен для librdkafka-консьюмера, но мы не подписываемся и не коммитим:
    // партиции назначаются через assign, поэтому группа на брокере не появляется.
    cfg.set("group.id", format!("kafka-explorer-browse-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "true")
        .set("auto.offset.reset", "earliest");
    cfg.create_with_context(ctx.clone()).map_err(|e| from_kafka(&e, None))
}

fn load_bounds(
    consumer: &BaseConsumer<CapturingContext>,
    params: &BrowseParams,
    timeout: Duration,
    kerr: &impl Fn(KafkaError) -> AppError,
) -> AppResult<Vec<PartitionBounds>> {
    let md = consumer.fetch_metadata(Some(&params.topic), timeout).map_err(kerr)?;
    let (_, topics) = convert(&md);
    let topic = topics
        .into_iter()
        .find(|t| t.name == params.topic && !t.partitions.is_empty())
        .ok_or_else(|| AppError::not_found(format!("Топик «{}» не найден", params.topic)))?;
    let all: Vec<i32> = topic.partitions.iter().map(|p| p.id).collect();
    let selected = match &params.partitions {
        Some(list) if !list.is_empty() => {
            if let Some(bad) = list.iter().find(|p| !all.contains(p)) {
                return Err(AppError::invalid(format!("В топике нет партиции {bad}")));
            }
            list.clone()
        }
        _ => all,
    };
    let keys: Vec<(String, i32)> = selected.iter().map(|p| (params.topic.clone(), *p)).collect();
    let bounds = fetch_bounds(consumer, &keys, timeout).map_err(kerr)?;
    let mut out = Vec::with_capacity(selected.len());
    for p in &selected {
        match bounds.get(&(params.topic.clone(), *p)) {
            Some(Ok((low, high))) => out.push(PartitionBounds { partition: *p, low: *low, high: *high }),
            other => {
                let reason = match other {
                    Some(Err(e)) => e.clone(),
                    _ => "нет ответа брокера".to_string(),
                };
                return Err(AppError::internal(format!("Не удалось получить оффсеты партиции {p}")).with_details(reason));
            }
        }
    }
    out.sort_by_key(|b| b.partition);
    Ok(out)
}

fn resolve_plans(
    consumer: &BaseConsumer<CapturingContext>,
    params: &BrowseParams,
    bounds: &[PartitionBounds],
    timeout: Duration,
) -> KafkaResult<Vec<PartitionPlan>> {
    Ok(match &params.start {
        StartPosition::Earliest => plan_earliest(bounds),
        StartPosition::Latest { count } => plan_latest(bounds, *count),
        StartPosition::Offset { offset } => plan_offset(bounds, *offset),
        StartPosition::Timestamp { timestamp_ms } => {
            plan_from_offsets(bounds, &offsets_for_timestamp(consumer, &params.topic, bounds, *timestamp_ms, timeout)?)
        }
        StartPosition::LastMinutes { minutes } => {
            let ts = now_ms() - minutes * 60_000;
            plan_from_offsets(bounds, &offsets_for_timestamp(consumer, &params.topic, bounds, ts, timeout)?)
        }
    })
}

fn offsets_for_timestamp(
    consumer: &BaseConsumer<CapturingContext>,
    topic: &str,
    bounds: &[PartitionBounds],
    ts: i64,
    timeout: Duration,
) -> KafkaResult<HashMap<i32, i64>> {
    let mut tpl = TopicPartitionList::new();
    for b in bounds {
        tpl.add_partition_offset(topic, b.partition, Offset::Offset(ts))?;
    }
    let res = consumer.offsets_for_times(tpl, timeout)?;
    Ok(res
        .elements()
        .iter()
        .filter_map(|e| match e.offset() {
            Offset::Offset(o) if o >= 0 => Some((e.partition(), o)),
            _ => None,
        })
        .collect())
}

fn assign(consumer: &BaseConsumer<CapturingContext>, topic: &str, plans: &[PartitionPlan], live: bool) -> KafkaResult<()> {
    let mut tpl = TopicPartitionList::new();
    for p in plans.iter().filter(|p| live || p.start < p.end) {
        tpl.add_partition_offset(topic, p.partition, Offset::Offset(p.start))?;
    }
    consumer.assign(&tpl)
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn to_browsed(m: &BorrowedMessage<'_>, key_format: DataFormat, value_format: DataFormat) -> BrowsedMessage {
    let headers = m
        .headers()
        .map(|hs| hs.iter().map(|h| HeaderView { name: h.key.to_string(), value: h.value.map(header_text) }).collect())
        .unwrap_or_default();
    BrowsedMessage {
        partition: m.partition(),
        offset: m.offset(),
        timestamp: m.timestamp().to_millis(),
        key: m.key().map(|k| decode(k, key_format)),
        value: m.payload().map(|v| decode(v, value_format)),
        headers,
        size: m.key().map_or(0, <[u8]>::len) + m.payload().map_or(0, <[u8]>::len),
    }
}

/// Буферизация: до BATCH_SIZE сообщений или раз в BATCH_INTERVAL; Progress — не реже раза в секунду.
struct Output<'a> {
    sink: &'a dyn EventSink,
    batch: Vec<BrowsedMessage>,
    scanned: u64,
    matched: u64,
    last_flush: Instant,
    last_progress: Instant,
    reached_end_sent: bool,
    closed: bool,
}

impl<'a> Output<'a> {
    fn new(sink: &'a dyn EventSink) -> Self {
        Self {
            sink,
            batch: Vec::with_capacity(BATCH_SIZE),
            scanned: 0,
            matched: 0,
            last_flush: Instant::now(),
            last_progress: Instant::now(),
            reached_end_sent: false,
            closed: false,
        }
    }

    fn push(&mut self, m: BrowsedMessage) {
        self.matched += 1;
        self.batch.push(m);
    }

    fn maybe_flush(&mut self) {
        if self.batch.len() >= BATCH_SIZE || (!self.batch.is_empty() && self.last_flush.elapsed() >= BATCH_INTERVAL) {
            self.flush();
        } else if self.last_progress.elapsed() >= PROGRESS_INTERVAL {
            self.progress();
        }
    }

    fn flush(&mut self) {
        if !self.batch.is_empty() {
            let messages = std::mem::take(&mut self.batch);
            self.emit(BrowseEvent::Messages { messages });
        }
        self.last_flush = Instant::now();
        self.progress();
    }

    fn progress(&mut self) {
        self.emit(BrowseEvent::Progress { scanned: self.scanned, matched: self.matched });
        self.last_progress = Instant::now();
    }

    fn reached_end(&mut self) {
        self.reached_end_sent = true;
        self.emit(BrowseEvent::ReachedEnd);
    }

    fn emit(&mut self, event: BrowseEvent) {
        if !self.sink.send(event) {
            self.closed = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    fn params(start: StartPosition, limit: usize) -> BrowseParams {
        BrowseParams {
            connection_id: "c".into(),
            topic: "t".into(),
            partitions: None,
            start,
            limit,
            live: false,
            key_format: DataFormat::Auto,
            value_format: DataFormat::Auto,
            filter: FilterSpec::default(),
        }
    }

    #[test]
    fn validation() {
        assert!(validate(&params(StartPosition::Earliest, 500)).is_ok());
        assert_eq!(validate(&params(StartPosition::Earliest, 0)).unwrap_err().code, ErrorCode::InvalidInput);
        assert_eq!(validate(&params(StartPosition::Earliest, MAX_LIMIT + 1)).unwrap_err().code, ErrorCode::InvalidInput);
        assert_eq!(validate(&params(StartPosition::Latest { count: 0 }, 10)).unwrap_err().code, ErrorCode::InvalidInput);
        assert_eq!(validate(&params(StartPosition::LastMinutes { minutes: -1 }, 10)).unwrap_err().code, ErrorCode::InvalidInput);
        let mut p = params(StartPosition::Earliest, 10);
        p.topic = " ".into();
        assert_eq!(validate(&p).unwrap_err().code, ErrorCode::InvalidInput);
    }

    #[test]
    fn params_json_contract() {
        let p: BrowseParams = serde_json::from_str(
            r#"{"connectionId":"c","topic":"t","partitions":[0,2],"start":{"type":"latest","count":10},
                "limit":500,"live":true,"keyFormat":"string","valueFormat":"auto",
                "filter":{"value":{"pattern":"x"},"jsonPath":"$.a"}}"#,
        )
        .unwrap();
        assert_eq!(p.partitions, Some(vec![0, 2]));
        assert!(p.live);
        assert_eq!(p.key_format, DataFormat::String);
        assert_eq!(p.filter.json_path.as_deref(), Some("$.a"));
    }
}
