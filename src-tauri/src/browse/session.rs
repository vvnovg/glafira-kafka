use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::{Offset, TopicPartitionList};
use serde::Deserialize;

use crate::browse::filter::{CompiledFilter, FilterSpec};
use crate::browse::format::{DataFormat, decode, header_text};
use crate::browse::plan::{
    EndTracker, PartitionBounds, PartitionPlan, StartPosition, plan_earliest, plan_from_offsets,
    plan_latest, plan_offset,
};
use crate::browse::types::{BrowseEvent, BrowsedMessage, FinishReason, HeaderView};
use crate::error::{AppError, AppResult, ErrorCode, from_kafka};
use crate::kafka::config::build_client_config;
use crate::kafka::context::CapturingContext;
use crate::kafka::errors::{fetch_metadata_fast_fail, op_error};
use crate::kafka::metadata::convert;
use crate::kafka::recovery::{PROBE_TIMEOUT, RecoveryMonitor, is_transient};
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
pub(crate) const BATCH_SIZE: usize = 200;
/// Верхняя граница объёма одного события Messages (сообщения до 1 МиБ текста каждое).
pub(crate) const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const BATCH_INTERVAL: Duration = Duration::from_millis(100);
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);
const POLL_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_LAST_MINUTES: i64 = 5_256_000; // 10 лет

pub fn validate(params: &BrowseParams) -> AppResult<()> {
    if params.topic.trim().is_empty() {
        return Err(AppError::invalid("Не указан топик"));
    }
    if params.limit == 0 || params.limit > MAX_LIMIT {
        return Err(AppError::invalid(format!(
            "Лимит должен быть от 1 до {MAX_LIMIT}"
        )));
    }
    match params.start {
        StartPosition::Latest { count } if count <= 0 || count > MAX_LIMIT as i64 => {
            Err(AppError::invalid(format!(
                "Количество последних сообщений должно быть от 1 до {MAX_LIMIT}"
            )))
        }
        StartPosition::LastMinutes { minutes } if !(1..=MAX_LAST_MINUTES).contains(&minutes) => {
            Err(AppError::invalid(format!(
                "Количество минут должно быть от 1 до {MAX_LAST_MINUTES}"
            )))
        }
        StartPosition::Timestamp { timestamp_ms } if timestamp_ms < 0 => Err(AppError::invalid(
            "Метка времени не может быть отрицательной",
        )),
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
    // Ошибка операции, начатой в `started`: консьюмер опрашивается, чтобы получить причину (TLS/SASL).
    let kerr = |started: Instant| {
        let consumer = &consumer;
        move |e: KafkaError| op_error(consumer, started, e)
    };
    let timeout = Duration::from_millis(profile.request_timeout_ms);

    let bounds = load_bounds(&consumer, params, timeout, &kerr(Instant::now()))?;
    let plans = resolve_plans(&consumer, params, &bounds, timeout, &kerr(Instant::now()))?;
    assign(&consumer, &params.topic, &plans, params.live).map_err(kerr(Instant::now()))?;

    let mut tracker = EndTracker::new(&plans);
    let mut out = Output::new(sink);
    let mut recovery = RecoveryMonitor::default();
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
        let now = Instant::now();
        if let Some((since, e)) = recovery.expired(now) {
            let e = e.clone();
            out.flush();
            return Err(kerr(since)(e));
        }
        if recovery.should_probe(now) {
            let ok = consumer
                .fetch_metadata(Some(&params.topic), PROBE_TIMEOUT)
                .is_ok();
            if recovery.on_probe(Instant::now(), ok) {
                out.emit(BrowseEvent::Recovered);
            }
        }
        let polled_at = Instant::now();
        match consumer.poll(POLL_TIMEOUT) {
            None => {}
            Some(Err(KafkaError::PartitionEOF(p))) => {
                if recovery.on_progress() {
                    out.emit(BrowseEvent::Recovered);
                }
                tracker.mark_done(p);
            }
            Some(Err(e)) if is_transient(&e) => {
                tracing::warn!("временная ошибка консьюмера, librdkafka переподключается: {e}");
                let message = from_kafka(&e, ctx.last_error_since(polled_at).as_deref()).message;
                if recovery.on_transient(Instant::now(), e) {
                    out.flush();
                    out.emit(BrowseEvent::Reconnecting { message });
                }
            }
            Some(Err(e)) => {
                out.flush();
                return Err(kerr(polled_at)(e));
            }
            Some(Ok(msg)) => {
                if recovery.on_progress() {
                    out.emit(BrowseEvent::Recovered);
                }
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
    out.emit(BrowseEvent::Finished {
        scanned: out.scanned,
        matched: out.matched,
        reason,
    });
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
    cfg.set(
        "group.id",
        format!("kafka-explorer-browse-{}", uuid::Uuid::new_v4()),
    )
    .set("enable.auto.commit", "false")
    .set("enable.auto.offset.store", "false")
    .set("enable.partition.eof", "true")
    .set("auto.offset.reset", "earliest");
    cfg.create_with_context(ctx.clone())
        .map_err(|e| from_kafka(&e, None))
}

fn load_bounds(
    consumer: &BaseConsumer<CapturingContext>,
    params: &BrowseParams,
    timeout: Duration,
    kerr: &impl Fn(KafkaError) -> AppError,
) -> AppResult<Vec<PartitionBounds>> {
    let md = fetch_metadata_fast_fail(consumer, Some(&params.topic), timeout)?;
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
    let keys: Vec<(String, i32)> = selected
        .iter()
        .map(|p| (params.topic.clone(), *p))
        .collect();
    let bounds = fetch_bounds(consumer, &keys, timeout).map_err(kerr)?;
    let mut out = Vec::with_capacity(selected.len());
    for p in &selected {
        match bounds.get(&(params.topic.clone(), *p)) {
            Some(Ok((low, high))) => out.push(PartitionBounds {
                partition: *p,
                low: *low,
                high: *high,
            }),
            other => {
                let reason = match other {
                    Some(Err(e)) => e.clone(),
                    _ => "нет ответа брокера".to_string(),
                };
                let code = crate::error::classify_message(&reason).unwrap_or(ErrorCode::Internal);
                return Err(AppError::new(
                    code,
                    format!("Не удалось получить оффсеты партиции {p}"),
                )
                .with_details(reason));
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
    kerr: &impl Fn(KafkaError) -> AppError,
) -> AppResult<Vec<PartitionPlan>> {
    Ok(match &params.start {
        StartPosition::Earliest => plan_earliest(bounds),
        StartPosition::Latest { count } => plan_latest(bounds, *count),
        StartPosition::Offset { offset } => plan_offset(bounds, *offset),
        StartPosition::Timestamp { timestamp_ms } => plan_from_offsets(
            bounds,
            &offsets_for_timestamp(
                consumer,
                &params.topic,
                bounds,
                *timestamp_ms,
                timeout,
                kerr,
            )?,
        ),
        StartPosition::LastMinutes { minutes } => {
            let ts = now_ms() - *minutes * 60_000;
            plan_from_offsets(
                bounds,
                &offsets_for_timestamp(consumer, &params.topic, bounds, ts, timeout, kerr)?,
            )
        }
    })
}

fn offsets_for_timestamp(
    consumer: &BaseConsumer<CapturingContext>,
    topic: &str,
    bounds: &[PartitionBounds],
    ts: i64,
    timeout: Duration,
    kerr: &impl Fn(KafkaError) -> AppError,
) -> AppResult<HashMap<i32, i64>> {
    let mut tpl = TopicPartitionList::new();
    for b in bounds {
        tpl.add_partition_offset(topic, b.partition, Offset::Offset(ts))
            .map_err(kerr)?;
    }
    let res = consumer.offsets_for_times(tpl, timeout).map_err(kerr)?;
    let mut found = HashMap::new();
    for e in res.elements() {
        let p = e.partition();
        // Ошибка по партиции — не «нет сообщений новее метки»: молча читать с конца нельзя.
        if let Err(err) = e.error() {
            let text = err.to_string();
            let code = crate::error::classify_message(&text).unwrap_or(ErrorCode::Internal);
            return Err(AppError::new(
                code,
                format!("Не удалось найти оффсет по времени для партиции {p}"),
            )
            .with_details(text));
        }
        // Успех с End/-1: сообщений новее метки нет → «с конца» (partition отсутствует в карте).
        if let Offset::Offset(o) = e.offset()
            && o >= 0
        {
            found.insert(p, o);
        }
    }
    Ok(found)
}

fn assign(
    consumer: &BaseConsumer<CapturingContext>,
    topic: &str,
    plans: &[PartitionPlan],
    live: bool,
) -> KafkaResult<()> {
    let mut tpl = TopicPartitionList::new();
    for p in plans.iter().filter(|p| live || p.start < p.end) {
        tpl.add_partition_offset(topic, p.partition, Offset::Offset(p.start))?;
    }
    consumer.assign(&tpl)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub(crate) fn to_browsed(
    m: &BorrowedMessage<'_>,
    key_format: DataFormat,
    value_format: DataFormat,
) -> BrowsedMessage {
    let headers = m
        .headers()
        .map(|hs| {
            hs.iter()
                .map(|h| HeaderView {
                    name: h.key.to_string(),
                    value: h.value.map(header_text),
                })
                .collect()
        })
        .unwrap_or_default();
    BrowsedMessage {
        topic: m.topic().to_string(),
        partition: m.partition(),
        offset: m.offset(),
        timestamp: m.timestamp().to_millis(),
        key: m.key().map(|k| decode(k, key_format)),
        value: m.payload().map(|v| decode(v, value_format)),
        headers,
        size: m.key().map_or(0, <[u8]>::len) + m.payload().map_or(0, <[u8]>::len),
    }
}

/// Оценка объёма сообщения в событии: текст ключа и значения плюс заголовки.
pub(crate) fn estimated_bytes(m: &BrowsedMessage) -> usize {
    let text =
        |d: &Option<crate::browse::format::DecodedData>| d.as_ref().map_or(0, |d| d.text.len());
    text(&m.key)
        + text(&m.value)
        + m.headers
            .iter()
            .map(|h| h.name.len() + h.value.as_ref().map_or(0, String::len))
            .sum::<usize>()
}

/// Буферизация: до BATCH_SIZE сообщений / MAX_BATCH_BYTES или раз в BATCH_INTERVAL;
/// Progress — не реже раза в секунду.
struct Output<'a> {
    sink: &'a dyn EventSink,
    batch: Vec<BrowsedMessage>,
    batch_bytes: usize,
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
            batch_bytes: 0,
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
        self.batch_bytes += estimated_bytes(&m);
        self.batch.push(m);
    }

    fn maybe_flush(&mut self) {
        if self.batch.len() >= BATCH_SIZE
            || self.batch_bytes > MAX_BATCH_BYTES
            || (!self.batch.is_empty() && self.last_flush.elapsed() >= BATCH_INTERVAL)
        {
            self.flush();
        } else if self.last_progress.elapsed() >= PROGRESS_INTERVAL {
            self.progress();
        }
    }

    fn flush(&mut self) {
        if !self.batch.is_empty() {
            let messages = std::mem::take(&mut self.batch);
            self.batch_bytes = 0;
            self.emit(BrowseEvent::Messages { messages });
        }
        self.last_flush = Instant::now();
        self.progress();
    }

    fn progress(&mut self) {
        self.emit(BrowseEvent::Progress {
            scanned: self.scanned,
            matched: self.matched,
        });
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
        assert_eq!(
            validate(&params(StartPosition::Earliest, 0))
                .unwrap_err()
                .code,
            ErrorCode::InvalidInput
        );
        assert_eq!(
            validate(&params(StartPosition::Earliest, MAX_LIMIT + 1))
                .unwrap_err()
                .code,
            ErrorCode::InvalidInput
        );
        assert_eq!(
            validate(&params(StartPosition::Latest { count: 0 }, 10))
                .unwrap_err()
                .code,
            ErrorCode::InvalidInput
        );
        assert!(validate(&params(StartPosition::Latest { count: 100_000 }, 10)).is_ok());
        for count in [100_001, i64::MAX] {
            assert_eq!(
                validate(&params(StartPosition::Latest { count }, 10))
                    .unwrap_err()
                    .code,
                ErrorCode::InvalidInput
            );
        }
        assert_eq!(
            validate(&params(StartPosition::LastMinutes { minutes: -1 }, 10))
                .unwrap_err()
                .code,
            ErrorCode::InvalidInput
        );
        assert_eq!(
            validate(&params(StartPosition::LastMinutes { minutes: 0 }, 10))
                .unwrap_err()
                .code,
            ErrorCode::InvalidInput
        );
        assert!(
            validate(&params(
                StartPosition::LastMinutes { minutes: 5_256_000 },
                10
            ))
            .is_ok()
        );
        assert_eq!(
            validate(&params(
                StartPosition::LastMinutes { minutes: 5_256_001 },
                10
            ))
            .unwrap_err()
            .code,
            ErrorCode::InvalidInput
        );
        assert_eq!(
            validate(&params(
                StartPosition::LastMinutes { minutes: i64::MAX },
                10
            ))
            .unwrap_err()
            .code,
            ErrorCode::InvalidInput
        );
        assert!(validate(&params(StartPosition::Timestamp { timestamp_ms: 0 }, 10)).is_ok());
        assert_eq!(
            validate(&params(StartPosition::Timestamp { timestamp_ms: -1 }, 10))
                .unwrap_err()
                .code,
            ErrorCode::InvalidInput
        );
        let mut p = params(StartPosition::Earliest, 10);
        p.topic = " ".into();
        assert_eq!(validate(&p).unwrap_err().code, ErrorCode::InvalidInput);
    }

    struct VecSink(std::sync::Mutex<Vec<BrowseEvent>>);

    impl EventSink for std::sync::Arc<VecSink> {
        fn send(&self, event: BrowseEvent) -> bool {
            self.0.lock().unwrap().push(event);
            true
        }
    }

    fn big_message(offset: i64, value_bytes: usize) -> BrowsedMessage {
        BrowsedMessage {
            topic: "t".into(),
            partition: 0,
            offset,
            timestamp: None,
            key: None,
            value: Some(crate::browse::format::DecodedData {
                format: DataFormat::String,
                text: "x".repeat(value_bytes),
                size: value_bytes,
                truncated: false,
                error: None,
            }),
            headers: vec![HeaderView {
                name: "h".into(),
                value: Some("v".into()),
            }],
            size: value_bytes,
        }
    }

    #[test]
    fn estimated_bytes_counts_key_value_and_headers() {
        let m = big_message(0, 100);
        assert_eq!(estimated_bytes(&m), 100 + 2);
    }

    #[test]
    fn batch_is_flushed_when_byte_budget_exceeded() {
        let sink = std::sync::Arc::new(VecSink(Default::default()));
        let mut out = Output::new(&sink);
        // По ~1 МиБ: меньше BATCH_SIZE, интервал не истёк; 4-е сообщение превышает 4 МиБ.
        for i in 0..5 {
            out.push(big_message(i, 1024 * 1024));
            out.maybe_flush();
        }
        let events = sink.0.lock().unwrap();
        let batches: Vec<usize> = events
            .iter()
            .filter_map(|e| match e {
                BrowseEvent::Messages { messages } => Some(messages.len()),
                _ => None,
            })
            .collect();
        assert_eq!(batches, vec![4], "сброс по объёму после превышения 4 МиБ");
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
