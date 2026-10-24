//! Чтение сообщений для MCP (спец. G4 §6): планирование окна по `offsets_for_times`, отдельный
//! консьюмер без группы на каждый вызов, фильтры и форматы экрана «Сообщения». Данные уходят
//! в крейт без маскировки — маскирует крейт. Блокирующий код: вызывать на blocking-потоке.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use glafira_mcp::commits::{CommitHistoryBuilder, CommitRecord, CommitStats};
use glafira_mcp::mask::{self as mm, MASK_PARSE_LIMIT};
use glafira_mcp::messages::{
    COMMIT_LOOKBACK_MS, FormatArg, MAX_HEADERS, MessageFilter, MessagesBody, MessagesOp,
    PartitionRange, Range, RawData, RawMessage, SCAN_RECORD_LIMIT, ScanCtx, ScanStats, StopReason,
    TimestampType,
};
use glafira_mcp::summary::{SummaryBuilder, SummaryRecord};
use glafira_mcp::topn::TopN;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Message, Timestamp};

use crate::browse::filter::{CompiledFilter, FilterSpec, HeaderMatch, TextMatch};
use crate::browse::format::DataFormat;
use crate::browse::plan::{EndTracker, PartitionBounds, PartitionPlan};
use crate::browse::session::{
    POLL_TIMEOUT, assign, create_consumer, load_bounds, offsets_for_timestamp, to_browsed,
};
use crate::browse::types::BrowsedMessage;
use crate::error::{AppError, AppResult};
use crate::kafka::context::CapturingContext;
use crate::kafka::errors::op_error;
use crate::kafka::offsets_log::{self, OFFSETS_TOPIC, OffsetsRecord};
use crate::kafka::recovery::{PROBE_TIMEOUT, RecoveryMonitor, is_transient};
use crate::secrets::SecretStore;
use crate::storage::connections::ConnectionProfile;

/// `client.id` трафика LLM, если в дополнительных свойствах подключения своего нет (спец. §6.2).
pub const MCP_CLIENT_ID: &str = "glafira-kafka-mcp";
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

/// `client.id` для консьюмера MCP: свой у подключения (квоты и ACL организации) важнее.
pub fn mcp_client_id(profile: &ConnectionProfile) -> Option<&'static str> {
    (!profile.extra_properties.contains_key("client.id")).then_some(MCP_CLIENT_ID)
}

/// План окна: начало — первый офсет не раньше `from`, конец — первый офсет позже `to` или high
/// watermark; нет офсета для начала — партиция пуста (спец. §6.1).
pub fn window_plans(
    bounds: &[PartitionBounds],
    starts: &HashMap<i32, i64>,
    ends: &HashMap<i32, i64>,
) -> Vec<PartitionPlan> {
    bounds
        .iter()
        .map(|b| {
            let high = b.high.max(b.low);
            let start = starts
                .get(&b.partition)
                .copied()
                .unwrap_or(high)
                .clamp(b.low, high);
            let end = ends
                .get(&b.partition)
                .copied()
                .unwrap_or(high)
                .clamp(start, high);
            PartitionPlan {
                partition: b.partition,
                start,
                end,
            }
        })
        .collect()
}

/// План режима «офсеты»: `[start, end)` в пределах партиции.
pub fn offsets_plan(b: &PartitionBounds, start: i64, end: Option<i64>) -> PartitionPlan {
    let high = b.high.max(b.low);
    let start = start.clamp(b.low, high);
    PartitionPlan {
        partition: b.partition,
        start,
        end: end.unwrap_or(high).clamp(start, high),
    }
}

fn data_format(f: FormatArg) -> DataFormat {
    match f {
        FormatArg::Auto => DataFormat::Auto,
        FormatArg::String => DataFormat::String,
        FormatArg::Json => DataFormat::Json,
        FormatArg::Hex => DataFormat::Hex,
        FormatArg::Base64 => DataFormat::Base64,
    }
}

/// Формат для крейта: `auto` уже разрешён `browse::format::decode`.
fn mask_format(f: DataFormat) -> mm::DataFormat {
    match f {
        DataFormat::Json => mm::DataFormat::Json,
        DataFormat::Hex => mm::DataFormat::Hex,
        DataFormat::Base64 => mm::DataFormat::Base64,
        DataFormat::String | DataFormat::Auto => mm::DataFormat::String,
    }
}

fn text_match(m: &glafira_mcp::messages::TextMatch) -> TextMatch {
    TextMatch {
        pattern: m.pattern.clone(),
        regex: m.regex,
        case_sensitive: m.case_sensitive,
    }
}

/// Фильтры MCP → фильтры экрана «Сообщения»; окно — через `from/to_timestamp`.
pub fn filter_spec(f: &MessageFilter, window: Option<(i64, i64)>) -> FilterSpec {
    FilterSpec {
        key: f.key.as_ref().map(text_match),
        value: f.value.as_ref().map(text_match),
        header: f.header.as_ref().map(|h| HeaderMatch {
            name: h.name.clone(),
            value: h.value.as_ref().map(text_match),
        }),
        json_path: f.json_path.clone(),
        from_timestamp: window.map(|w| w.0),
        to_timestamp: window.map(|w| w.1),
    }
}

fn timestamp_type(t: Timestamp) -> TimestampType {
    match t {
        Timestamp::CreateTime(_) => TimestampType::Create,
        Timestamp::LogAppendTime(_) => TimestampType::LogAppend,
        Timestamp::NotAvailable => TimestampType::Unknown,
    }
}

fn raw_data(bytes: &[u8], format: DataFormat) -> RawData {
    RawData {
        bytes: bytes[..bytes.len().min(MASK_PARSE_LIMIT)].to_vec(),
        size: bytes.len(),
        format: mask_format(format),
    }
}

fn raw_message(msg: &BorrowedMessage<'_>, m: &BrowsedMessage) -> RawMessage {
    RawMessage {
        partition: m.partition,
        offset: m.offset,
        timestamp: m.timestamp,
        timestamp_type: timestamp_type(msg.timestamp()),
        key: msg
            .key()
            .zip(m.key.as_ref())
            .map(|(k, d)| raw_data(k, d.format)),
        value: msg
            .payload()
            .zip(m.value.as_ref())
            .map(|(v, d)| raw_data(v, d.format)),
        headers: m
            .headers
            .iter()
            .take(MAX_HEADERS)
            .map(|h| (h.name.clone(), h.value.clone()))
            .collect(),
        headers_truncated: m.headers.len() > MAX_HEADERS,
    }
}

/// Консьюмер одного просмотра.
struct Scan<'a> {
    consumer: BaseConsumer<CapturingContext>,
    ctx: &'a ScanCtx,
    timeout: Duration,
}

/// Итог цикла чтения: просмотрено, где остановились и почему.
struct Ran {
    scanned: u64,
    next: HashMap<i32, i64>,
    stopped_by: Option<StopReason>,
}

impl Ran {
    fn ranges(&self, plans: &[PartitionPlan]) -> Vec<PartitionRange> {
        plans
            .iter()
            .map(|p| PartitionRange {
                partition: p.partition,
                start_offset: p.start,
                end_offset: p.end,
                scanned_to: self
                    .stopped_by
                    .and_then(|_| self.next.get(&p.partition).copied())
                    .filter(|n| *n < p.end),
            })
            .collect()
    }
}

impl<'a> Scan<'a> {
    fn open(
        profile: &ConnectionProfile,
        secrets: &dyn SecretStore,
        ctx: &'a ScanCtx,
    ) -> AppResult<Self> {
        let capturing = CapturingContext::default();
        let client_id = mcp_client_id(profile);
        let consumer = create_consumer(profile, secrets, &capturing, "mcp", |cfg| {
            if let Some(id) = client_id {
                cfg.set("client.id", id);
            }
        })?;
        Ok(Self {
            consumer,
            ctx,
            timeout: Duration::from_millis(profile.request_timeout_ms),
        })
    }

    /// Ошибка операции, начатой в `started`: консьюмер опрашивается, чтобы получить причину (TLS/SASL).
    fn kerr(&self, started: Instant) -> impl Fn(KafkaError) -> AppError + '_ {
        move |e| op_error(&self.consumer, started, e)
    }

    fn bounds(&self, topic: &str, partitions: Option<&[i32]>) -> AppResult<Vec<PartitionBounds>> {
        load_bounds(
            &self.consumer,
            topic,
            partitions,
            self.timeout,
            &self.kerr(Instant::now()),
        )
    }

    fn offsets_at(
        &self,
        topic: &str,
        bounds: &[PartitionBounds],
        ts: i64,
    ) -> AppResult<HashMap<i32, i64>> {
        offsets_for_timestamp(
            &self.consumer,
            topic,
            bounds,
            ts,
            self.timeout,
            &self.kerr(Instant::now()),
        )
    }

    fn window(
        &self,
        topic: &str,
        partitions: Option<&[i32]>,
        from_ms: i64,
        to_ms: i64,
    ) -> AppResult<Vec<PartitionPlan>> {
        let bounds = self.bounds(topic, partitions)?;
        let starts = self.offsets_at(topic, &bounds, from_ms)?;
        let ends = self.offsets_at(topic, &bounds, to_ms.saturating_add(1))?;
        Ok(window_plans(&bounds, &starts, &ends))
    }

    /// Читает диапазоны планов; каждое сообщение внутри них отдаётся `visit`. Останавливается в
    /// конце диапазонов, по отмене, дедлайну или пределу записей (спец. §3.1).
    fn run(
        &self,
        topic: &str,
        plans: &[PartitionPlan],
        mut visit: impl FnMut(&BorrowedMessage<'_>),
    ) -> AppResult<Ran> {
        assign(&self.consumer, topic, plans, false).map_err(self.kerr(Instant::now()))?;
        let total: u64 = plans.iter().map(|p| (p.end - p.start).max(0) as u64).sum();
        let mut tracker = EndTracker::new(plans);
        let mut next: HashMap<i32, i64> = plans.iter().map(|p| (p.partition, p.start)).collect();
        let mut recovery = RecoveryMonitor::default();
        let mut scanned = 0u64;
        let mut last_progress = Instant::now();
        let stopped_by = loop {
            if tracker.all_done() {
                break None;
            }
            // Отменённый вызов LLM уже не ждёт: собранное уходит только в журнал.
            if self.ctx.cancelled() || self.ctx.expired() {
                break Some(StopReason::TimeBudget);
            }
            if scanned >= SCAN_RECORD_LIMIT {
                break Some(StopReason::ScanLimit);
            }
            let now = Instant::now();
            if let Some((since, e)) = recovery.expired(now) {
                return Err(self.kerr(since)(e.clone()));
            }
            if recovery.should_probe(now) {
                let ok = self
                    .consumer
                    .fetch_metadata(Some(topic), PROBE_TIMEOUT)
                    .is_ok();
                recovery.on_probe(Instant::now(), ok);
            }
            let polled_at = Instant::now();
            match self.consumer.poll(POLL_TIMEOUT) {
                None => {}
                Some(Err(KafkaError::PartitionEOF(p))) => {
                    recovery.on_progress();
                    tracker.mark_done(p);
                }
                Some(Err(e)) if is_transient(&e) => {
                    tracing::warn!(
                        "mcp scan: transient consumer error, librdkafka is reconnecting: {e}"
                    );
                    recovery.on_transient(Instant::now(), e);
                }
                Some(Err(e)) => return Err(self.kerr(polled_at)(e)),
                Some(Ok(msg)) => {
                    recovery.on_progress();
                    if tracker.observe(msg.partition(), msg.offset()) {
                        scanned += 1;
                        next.insert(msg.partition(), msg.offset() + 1);
                        visit(&msg);
                    }
                }
            }
            if last_progress.elapsed() >= PROGRESS_INTERVAL {
                (self.ctx.progress)(scanned, total);
                last_progress = Instant::now();
            }
        };
        (self.ctx.progress)(scanned, total);
        Ok(Ran {
            scanned,
            next,
            stopped_by,
        })
    }
}

/// Выполняет операцию чтения сообщений. Доступ к подключению проверяет вызывающий; фильтры
/// проверяются до обращения к кластеру.
pub fn scan(
    profile: &ConnectionProfile,
    secrets: &dyn SecretStore,
    op: MessagesOp,
    ctx: &ScanCtx,
) -> AppResult<MessagesBody> {
    match op {
        MessagesOp::Read {
            topic,
            range,
            filter,
            key_format,
            value_format,
            limit,
            after,
            ..
        } => {
            let window = match &range {
                Range::Window { from_ms, to_ms, .. } => Some((*from_ms, *to_ms)),
                Range::Offsets { .. } => None,
            };
            let compiled = CompiledFilter::compile(&filter_spec(&filter, window))?;
            let s = Scan::open(profile, secrets, ctx)?;
            let plans = match &range {
                Range::Window {
                    from_ms,
                    to_ms,
                    partitions,
                } => s.window(&topic, partitions.as_deref(), *from_ms, *to_ms)?,
                Range::Offsets {
                    partition,
                    start,
                    end,
                } => {
                    let bounds = s.bounds(&topic, Some(&[*partition]))?;
                    vec![offsets_plan(&bounds[0], *start, *end)]
                }
            };
            let (kf, vf) = (data_format(key_format), data_format(value_format));
            let mut topn = TopN::new(limit, after);
            let mut matched = 0u64;
            let ran = s.run(&topic, &plans, |msg| {
                let m = to_browsed(msg, kf, vf);
                if !compiled.matches(&m.view()) {
                    return;
                }
                matched += 1;
                let key = (m.timestamp.unwrap_or(i64::MIN), m.partition, m.offset);
                topn.offer(key, || raw_message(msg, &m));
            })?;
            let more = topn.more();
            Ok(MessagesBody::Read {
                stats: ScanStats {
                    partitions: ran.ranges(&plans),
                    scanned: ran.scanned,
                    matched,
                    stopped_by: ran.stopped_by,
                },
                messages: topn.into_sorted().into_iter().map(|(_, m)| m).collect(),
                more,
            })
        }
        MessagesOp::Summarize {
            topic,
            from_ms,
            to_ms,
            partitions,
            filter,
            key_format,
            value_format,
            bucket_ms,
            top_keys,
            ..
        } => {
            let compiled = CompiledFilter::compile(&filter_spec(&filter, Some((from_ms, to_ms))))?;
            let s = Scan::open(profile, secrets, ctx)?;
            let plans = s.window(&topic, partitions.as_deref(), from_ms, to_ms)?;
            let (kf, vf) = (data_format(key_format), data_format(value_format));
            let mut builder = SummaryBuilder::new(from_ms, to_ms, bucket_ms, top_keys);
            let mut matched = 0u64;
            let ran = s.run(&topic, &plans, |msg| {
                let m = to_browsed(msg, kf, vf);
                if !compiled.matches(&m.view()) {
                    return;
                }
                matched += 1;
                let names: Vec<&str> = m.headers.iter().map(|h| h.name.as_str()).collect();
                builder.add(&SummaryRecord {
                    partition: m.partition,
                    offset: m.offset,
                    timestamp: m.timestamp,
                    timestamp_type: timestamp_type(msg.timestamp()),
                    key: msg
                        .key()
                        .zip(m.key.as_ref())
                        .map(|(k, d)| (k, mask_format(d.format))),
                    value_size: msg.payload().map_or(0, <[u8]>::len),
                    header_names: &names,
                });
            })?;
            Ok(MessagesBody::Summarize(builder.finish(ScanStats {
                partitions: ran.ranges(&plans),
                scanned: ran.scanned,
                matched,
                stopped_by: ran.stopped_by,
            })))
        }
        MessagesOp::CommitHistory {
            group_id,
            from_ms,
            to_ms,
            topic,
            ..
        } => {
            let s = Scan::open(profile, secrets, ctx)?;
            let all = s.bounds(OFFSETS_TOPIC, None)?;
            let partition = offsets_log::partition_for(&group_id, all.len() as i32);
            let bounds: Vec<PartitionBounds> = all
                .into_iter()
                .filter(|b| b.partition == partition)
                .collect();
            let lookback = (from_ms - COMMIT_LOOKBACK_MS).max(0);
            let starts = s.offsets_at(OFFSETS_TOPIC, &bounds, lookback)?;
            let ends = s.offsets_at(OFFSETS_TOPIC, &bounds, to_ms.saturating_add(1))?;
            let plans = window_plans(&bounds, &starts, &ends);
            let mut builder = CommitHistoryBuilder::new(from_ms, to_ms, topic);
            let mut skipped = 0u64;
            let ran = s.run(OFFSETS_TOPIC, &plans, |msg| {
                let key = msg.key().unwrap_or_default();
                // Сначала только ключ: битое значение записи чужой группы не считается.
                let ours = matches!(
                    offsets_log::decode(key, None),
                    Ok(OffsetsRecord::Commit { ref group, .. }) if *group == group_id
                );
                if !ours {
                    return;
                }
                let record_ts = msg.timestamp().to_millis().unwrap_or(0);
                match offsets_log::decode(key, msg.payload()) {
                    Ok(OffsetsRecord::Commit {
                        topic,
                        partition,
                        value,
                        ..
                    }) => builder.add(CommitRecord {
                        topic,
                        partition,
                        offset: value.as_ref().map(|v| v.offset),
                        at_ms: value
                            .as_ref()
                            .map(|v| v.commit_timestamp)
                            .filter(|t| *t >= 0)
                            .unwrap_or(record_ts),
                        metadata: value.map(|v| v.metadata).unwrap_or_default(),
                    }),
                    Ok(OffsetsRecord::Other) => {}
                    Err(_) => skipped += 1,
                }
            })?;
            Ok(MessagesBody::CommitHistory(builder.finish(CommitStats {
                offsets_partition: partition,
                scanned: ran.scanned,
                skipped_records: skipped,
                stopped_by: ran.stopped_by,
            })))
        }
    }
}

/// Текущий сдвиг машины от UTC в минутах (для `timeZone` ответа).
pub fn local_utc_offset_minutes() -> i32 {
    chrono::Local::now().offset().local_minus_utc() / 60
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::connections::sample_profile;

    fn b(partition: i32, low: i64, high: i64) -> PartitionBounds {
        PartitionBounds {
            partition,
            low,
            high,
        }
    }

    fn spans(plans: &[PartitionPlan]) -> Vec<(i32, i64, i64)> {
        plans
            .iter()
            .map(|p| (p.partition, p.start, p.end))
            .collect()
    }

    #[test]
    fn window_plans_follow_offsets_by_time() {
        let bounds = [b(0, 0, 100), b(1, 10, 50), b(2, 0, 0)];
        let starts = HashMap::from([(0, 20), (1, 5)]);
        let ends = HashMap::from([(0, 40)]);
        assert_eq!(
            spans(&window_plans(&bounds, &starts, &ends)),
            [(0, 20, 40), (1, 10, 50), (2, 0, 0)],
            "нет офсета для конца — до high; офсет до low зажимается"
        );
        // Нет офсета для начала: сообщений не раньше `from` нет — партиция пуста.
        let plans = window_plans(&bounds, &HashMap::new(), &HashMap::new());
        assert_eq!(spans(&plans), [(0, 100, 100), (1, 50, 50), (2, 0, 0)]);
        // Конец раньше начала (метки не по порядку) — пустой диапазон, не отрицательный.
        let plans = window_plans(
            &[b(0, 0, 100)],
            &HashMap::from([(0, 30)]),
            &HashMap::from([(0, 10)]),
        );
        assert_eq!(spans(&plans), [(0, 30, 30)]);
    }

    #[test]
    fn offsets_plan_stays_inside_the_partition() {
        assert_eq!(
            spans(&[offsets_plan(&b(0, 10, 50), 3, Some(20))]),
            [(0, 10, 20)]
        );
        assert_eq!(
            spans(&[offsets_plan(&b(0, 10, 50), 30, None)]),
            [(0, 30, 50)]
        );
        assert_eq!(
            spans(&[offsets_plan(&b(0, 10, 50), 70, Some(90))]),
            [(0, 50, 50)]
        );
    }

    #[test]
    fn client_id_of_the_connection_wins() {
        let mut p = sample_profile("a");
        assert_eq!(mcp_client_id(&p), Some(MCP_CLIENT_ID));
        p.extra_properties
            .insert("client.id".into(), "corp-approved".into());
        assert_eq!(mcp_client_id(&p), None);
    }

    #[test]
    fn filters_map_to_the_browser_filters() {
        let f = MessageFilter {
            key: Some(glafira_mcp::messages::TextMatch {
                pattern: "k".into(),
                regex: true,
                case_sensitive: true,
            }),
            value: None,
            header: Some(glafira_mcp::messages::HeaderMatch {
                name: "trace".into(),
                value: None,
            }),
            json_path: Some("$.a".into()),
        };
        let spec = filter_spec(&f, Some((1, 2)));
        assert!(
            spec.key
                .as_ref()
                .is_some_and(|k| k.regex && k.case_sensitive)
        );
        assert_eq!(spec.header.as_ref().map(|h| h.name.as_str()), Some("trace"));
        assert_eq!((spec.from_timestamp, spec.to_timestamp), (Some(1), Some(2)));
        assert!(CompiledFilter::compile(&spec).is_ok());
        let spec = filter_spec(&MessageFilter::default(), None);
        assert_eq!((spec.from_timestamp, spec.to_timestamp), (None, None));
        assert_eq!(mask_format(DataFormat::Auto), mm::DataFormat::String);
        assert_eq!(data_format(FormatArg::Hex), DataFormat::Hex);
    }
}
