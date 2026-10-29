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
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Message, Timestamp};

use crate::browse::filter::{CompiledFilter, FilterSpec, HeaderMatch, TextMatch};
use crate::browse::format::DataFormat;
use crate::browse::plan::{EndTracker, PartitionBounds, PartitionPlan};
use crate::browse::session::{
    POLL_TIMEOUT, assign, create_consumer, offsets_for_timestamp, partition_bounds, to_browsed,
    topic_partitions,
};
use crate::browse::types::{BrowsedMessage, HeaderView};
use crate::error::{AppError, AppResult, ErrorCode};
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

/// Настройки консьюмера MCP поверх общих (спец. §6.2). `create_consumer` применяет их последними,
/// после `extraProperties` подключения: `read_committed` они не ослабят, а свой `client.id`
/// подключения сохраняется — `mcp_client_id` тогда ничего не ставит.
fn mcp_tune(profile: &ConnectionProfile) -> impl FnOnce(&mut ClientConfig) {
    let client_id = mcp_client_id(profile);
    move |cfg| {
        cfg.set("isolation.level", "read_committed");
        if let Some(id) = client_id {
            cfg.set("client.id", id);
        }
    }
}

/// Отмена или дедлайн до конца планирования: прочитать ещё ничего не успели, поэтому ошибка,
/// а не частичный ответ (спец. §6.2).
pub(crate) fn planning_stopped() -> AppError {
    app_err!(
        ErrorCode::Timeout,
        "errors.kafka.timeout",
        "The operation timed out"
    )
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

/// Заголовки для крейта: не больше [`MAX_HEADERS`] и признак, что часть отброшена.
fn raw_headers(headers: &[HeaderView]) -> (Vec<(String, Option<String>)>, bool) {
    let kept = headers
        .iter()
        .take(MAX_HEADERS)
        .map(|h| (h.name.clone(), h.value.clone()))
        .collect();
    (kept, headers.len() > MAX_HEADERS)
}

fn raw_message(msg: &BorrowedMessage<'_>, m: &BrowsedMessage) -> RawMessage {
    let (headers, headers_truncated) = raw_headers(&m.headers);
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
        headers,
        headers_truncated,
    }
}

/// Консьюмер одного просмотра.
struct Scan<'a> {
    consumer: BaseConsumer<CapturingContext>,
    ctx: &'a ScanCtx,
    timeout: Duration,
}

/// Ход чтения по партициям: что ещё читать (`EndTracker`) и следующий офсет каждой партиции.
/// Законченная партиция — EOF или запись за концом диапазона — пройдена до `end`, даже если
/// хвост диапазона не доставлен (маркеры транзакций при `read_committed`, пропуски после сжатия).
struct Positions {
    tracker: EndTracker,
    ends: HashMap<i32, i64>,
    next: HashMap<i32, i64>,
}

impl Positions {
    fn new(plans: &[PartitionPlan]) -> Self {
        Self {
            tracker: EndTracker::new(plans),
            ends: plans.iter().map(|p| (p.partition, p.end)).collect(),
            next: plans.iter().map(|p| (p.partition, p.start)).collect(),
        }
    }

    /// Запись партиции: true — она в диапазоне, её нужно просмотреть.
    fn record(&mut self, partition: i32, offset: i64) -> bool {
        let inside = self.tracker.observe(partition, offset);
        if inside {
            self.next.insert(partition, offset + 1);
        } else if self.ends.get(&partition).is_some_and(|end| offset >= *end) {
            self.finish(partition);
        }
        inside
    }

    /// Конец партиции (EOF).
    fn eof(&mut self, partition: i32) {
        self.tracker.mark_done(partition);
        self.finish(partition);
    }

    fn finish(&mut self, partition: i32) {
        if let Some(end) = self.ends.get(&partition) {
            self.next.insert(partition, *end);
        }
    }

    fn all_done(&self) -> bool {
        self.tracker.all_done()
    }
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
        let consumer = create_consumer(profile, secrets, &capturing, "mcp", mcp_tune(profile))?;
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

    /// Ожидание одного запроса планирования: таймаут запроса подключения, но не дольше остатка
    /// бюджета. Отменённый или просроченный вызов дальше не планируется (спец. §6.2): консьюмер
    /// закрывается вместе с `Scan`, когда ошибка выходит из `scan`.
    fn wait(&self) -> AppResult<Duration> {
        let left = self.ctx.deadline.saturating_duration_since(Instant::now());
        if self.ctx.cancelled() || left.is_zero() {
            return Err(planning_stopped());
        }
        Ok(self.timeout.min(left))
    }

    fn bounds(&self, topic: &str, partitions: Option<&[i32]>) -> AppResult<Vec<PartitionBounds>> {
        let selected = topic_partitions(&self.consumer, topic, partitions, self.wait()?)?;
        self.partition_bounds(topic, &selected)
    }

    fn partition_bounds(&self, topic: &str, selected: &[i32]) -> AppResult<Vec<PartitionBounds>> {
        partition_bounds(
            &self.consumer,
            topic,
            selected,
            self.wait()?,
            &self.kerr(Instant::now()),
        )
    }

    /// Число партиций `__consumer_offsets` по метаданным; без права Describe — отказ в правах
    /// (вызывающий добавит подсказку про Read), а не «топика нет».
    fn offsets_partitions(&self) -> AppResult<i32> {
        let all = topic_partitions(&self.consumer, OFFSETS_TOPIC, None, self.wait()?)?;
        Ok(all.len() as i32)
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
            self.wait()?,
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
        let mut pos = Positions::new(plans);
        let mut recovery = RecoveryMonitor::default();
        let mut scanned = 0u64;
        let mut last_progress = Instant::now();
        let stopped_by = loop {
            if pos.all_done() {
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
                    pos.eof(p);
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
                    if pos.record(msg.partition(), msg.offset()) {
                        scanned += 1;
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
            next: pos.next,
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
            // Границы — только у партиции группы: сбой чужой партиции не мешает чтению.
            let partition = offsets_log::partition_for(&group_id, s.offsets_partitions()?);
            let bounds = s.partition_bounds(OFFSETS_TOPIC, &[partition])?;
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
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::browse::session::consumer_config;
    use crate::secrets::MemoryStore;
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

    #[test]
    fn mcp_consumer_reads_committed_even_if_extra_properties_say_otherwise() {
        let mut p = sample_profile("a");
        p.extra_properties
            .insert("isolation.level".into(), "read_uncommitted".into());
        let cfg = consumer_config(&p, &MemoryStore::default(), "mcp", mcp_tune(&p)).unwrap();
        assert_eq!(cfg.get("isolation.level"), Some("read_committed"));
    }

    #[test]
    fn mcp_consumer_client_id_is_the_connection_one_or_its_own() {
        let mut p = sample_profile("a");
        let secrets = MemoryStore::default();
        let cfg = consumer_config(&p, &secrets, "mcp", mcp_tune(&p)).unwrap();
        assert_eq!(cfg.get("client.id"), Some(MCP_CLIENT_ID));
        let prefix = format!("{}-mcp-", crate::product::KAFKA_CLIENT_PREFIX);
        assert!(cfg.get("group.id").is_some_and(|g| g.starts_with(&prefix)));
        p.extra_properties
            .insert("client.id".into(), "corp-approved".into());
        let cfg = consumer_config(&p, &secrets, "mcp", mcp_tune(&p)).unwrap();
        assert_eq!(cfg.get("client.id"), Some("corp-approved"));
    }

    fn plan(partition: i32, start: i64, end: i64) -> PartitionPlan {
        PartitionPlan {
            partition,
            start,
            end,
        }
    }

    #[test]
    fn scanned_to_is_reported_only_for_unfinished_partitions() {
        let plans = [
            plan(0, 0, 10),
            plan(1, 0, 10),
            plan(2, 5, 5),
            plan(3, 0, 10),
        ];
        let mut pos = Positions::new(&plans);
        assert!(pos.record(0, 3));
        // Хвост партиции 1 — маркер транзакции: последняя доставленная запись 8, затем EOF.
        assert!(pos.record(1, 8));
        pos.eof(1);
        // В партиции 3 после 4 — пропуск после сжатия: следующая запись уже за концом диапазона.
        assert!(pos.record(3, 4));
        assert!(!pos.record(3, 12));
        assert!(!pos.all_done(), "партиция 0 не дочитана");
        let ran = Ran {
            scanned: 3,
            next: pos.next,
            stopped_by: Some(StopReason::TimeBudget),
        };
        let scanned_to: Vec<(i32, Option<i64>)> = ran
            .ranges(&plans)
            .iter()
            .map(|r| (r.partition, r.scanned_to))
            .collect();
        assert_eq!(
            scanned_to,
            [(0, Some(4)), (1, None), (2, None), (3, None)],
            "законченная и пустая партиции пройдены до конца"
        );
        let ran = Ran {
            stopped_by: None,
            ..ran
        };
        assert!(
            ran.ranges(&plans).iter().all(|r| r.scanned_to.is_none()),
            "без остановки по бюджету `scannedTo` нет"
        );
    }

    #[test]
    fn headers_are_capped_at_max_headers() {
        let headers: Vec<HeaderView> = (0..=MAX_HEADERS)
            .map(|i| HeaderView {
                name: format!("h{i}"),
                value: Some(i.to_string()),
            })
            .collect();
        let (kept, truncated) = raw_headers(&headers);
        assert_eq!((kept.len(), truncated), (MAX_HEADERS, true));
        assert_eq!(kept[MAX_HEADERS - 1].0, format!("h{}", MAX_HEADERS - 1));
        let (kept, truncated) = raw_headers(&headers[..MAX_HEADERS]);
        assert_eq!((kept.len(), truncated), (MAX_HEADERS, false));
    }

    const API_VERSIONS: i16 = 18;
    const METADATA: i16 = 3;

    /// Фальшивый брокер на 127.0.0.1: на каждый запрос `reply(api_key, correlation_id, port)`
    /// отвечает готовым ответом или молчит.
    fn fake_broker(
        reply: impl Fn(i16, &[u8], u16) -> Option<Vec<u8>> + Send + Sync + 'static,
    ) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let reply = Arc::new(reply);
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(mut conn) = conn else { return };
                let reply = reply.clone();
                std::thread::spawn(move || {
                    let mut size = [0u8; 4];
                    while conn.read_exact(&mut size).is_ok() {
                        let mut req = vec![0u8; u32::from_be_bytes(size) as usize];
                        if conn.read_exact(&mut req).is_err() {
                            return;
                        }
                        // Заголовок запроса: api_key, версия, correlation_id.
                        if req.len() < 8 {
                            continue;
                        }
                        let api_key = i16::from_be_bytes([req[0], req[1]]);
                        if let Some(out) = reply(api_key, &req[4..8], addr.port()) {
                            let _ = conn.write_all(&out);
                        }
                    }
                });
            }
        });
        addr
    }

    /// Брокер, который отвечает только на ApiVersions (соединение готово), а метаданные и
    /// ListOffsets оставляет без ответа: каждый запрос планирования ждёт свой таймаут целиком.
    /// Недоступный адрес для этого не годится: `fetch_metadata` без готового брокера сразу
    /// после первой порции ожидания отвечает «Broker transport failure».
    fn stalled_broker() -> SocketAddr {
        fake_broker(|api_key, correlation_id, _| {
            (api_key == API_VERSIONS).then(|| api_versions_reply(correlation_id, 12))
        })
    }

    /// Брокер с метаданными: на любой запрос Metadata — один брокер (он сам) и топик `topic` с
    /// кодом ошибки `error` без партиций (без права Describe брокер отвечает так: 29).
    fn metadata_broker(topic: &'static str, error: i16) -> SocketAddr {
        fake_broker(move |api_key, correlation_id, port| match api_key {
            API_VERSIONS => Some(api_versions_reply(correlation_id, 1)),
            METADATA => Some(metadata_reply(correlation_id, port, topic, error)),
            _ => None,
        })
    }

    /// Ответ ApiVersions v3 (заголовок ответа v0): брокер знает только ApiVersions и Metadata
    /// до версии `metadata_max`.
    fn api_versions_reply(correlation_id: &[u8], metadata_max: i16) -> Vec<u8> {
        let mut body = correlation_id.to_vec();
        body.extend(0i16.to_be_bytes()); // error_code
        body.push(3); // compact array: 2 элемента + 1
        for (api_key, max_version) in [(API_VERSIONS, 3i16), (METADATA, metadata_max)] {
            body.extend(api_key.to_be_bytes());
            body.extend(0i16.to_be_bytes());
            body.extend(max_version.to_be_bytes());
            body.push(0); // tagged fields
        }
        body.extend(0i32.to_be_bytes()); // throttle_time_ms
        body.push(0); // tagged fields
        framed(body)
    }

    /// Ответ Metadata v1 (заголовок ответа v0): брокер 1 на `127.0.0.1:port`, он же контроллер,
    /// и один топик без партиций.
    fn metadata_reply(correlation_id: &[u8], port: u16, topic: &str, error: i16) -> Vec<u8> {
        let mut body = correlation_id.to_vec();
        body.extend(1i32.to_be_bytes()); // brokers: 1
        body.extend(1i32.to_be_bytes()); // node_id
        put_str(&mut body, "127.0.0.1");
        body.extend(i32::from(port).to_be_bytes());
        body.extend((-1i16).to_be_bytes()); // rack: null
        body.extend(1i32.to_be_bytes()); // controller_id
        body.extend(1i32.to_be_bytes()); // topics: 1
        body.extend(error.to_be_bytes());
        put_str(&mut body, topic);
        body.push(u8::from(topic == OFFSETS_TOPIC)); // is_internal
        body.extend(0i32.to_be_bytes()); // partitions: 0
        framed(body)
    }

    fn put_str(buf: &mut Vec<u8>, s: &str) {
        buf.extend((s.len() as i16).to_be_bytes());
        buf.extend(s.as_bytes());
    }

    fn framed(body: Vec<u8>) -> Vec<u8> {
        let mut out = (body.len() as u32).to_be_bytes().to_vec();
        out.extend(body);
        out
    }

    fn profile_at(broker: SocketAddr, request_timeout_ms: u64) -> ConnectionProfile {
        let mut p = sample_profile("a");
        p.bootstrap_servers = broker.to_string();
        p.request_timeout_ms = request_timeout_ms;
        p
    }

    fn read(range: Range) -> MessagesOp {
        MessagesOp::Read {
            connection_id: "a".into(),
            topic: "orders".into(),
            range,
            filter: MessageFilter::default(),
            key_format: FormatArg::Auto,
            value_format: FormatArg::Auto,
            limit: 50,
            after: None,
        }
    }

    fn window() -> Range {
        Range::Window {
            from_ms: 0,
            to_ms: 1_000,
            partitions: None,
        }
    }

    fn summarize() -> MessagesOp {
        MessagesOp::Summarize {
            connection_id: "a".into(),
            topic: "orders".into(),
            from_ms: 0,
            to_ms: 1_000,
            partitions: None,
            filter: MessageFilter::default(),
            key_format: FormatArg::Auto,
            value_format: FormatArg::Auto,
            bucket_ms: 60_000,
            top_keys: 10,
        }
    }

    fn commits() -> MessagesOp {
        MessagesOp::CommitHistory {
            connection_id: "a".into(),
            group_id: "g".into(),
            from_ms: 0,
            to_ms: 1_000,
            topic: None,
        }
    }

    #[test]
    fn planning_stops_on_cancel_and_expired_deadline() {
        let p = profile_at(stalled_broker(), 5_000);
        let expired = ScanCtx::new(Duration::ZERO);
        let cancelled = ScanCtx::new(Duration::from_secs(40));
        cancelled.cancel.store(true, Ordering::Relaxed);
        let offsets = Range::Offsets {
            partition: 0,
            start: 0,
            end: None,
        };
        let cases = [
            ("read window, expired", read(window()), &expired),
            ("read offsets, cancelled", read(offsets), &cancelled),
            ("summarize, expired", summarize(), &expired),
            ("commits, expired", commits(), &expired),
            ("commits, cancelled", commits(), &cancelled),
        ];
        for (name, op, ctx) in cases {
            let started = Instant::now();
            let e = scan(&p, &MemoryStore::default(), op, ctx).expect_err(name);
            let took = started.elapsed();
            assert!(took < Duration::from_secs(3), "{name}: {took:?}");
            assert_eq!(
                (e.code, e.key.as_str()),
                (ErrorCode::Timeout, "errors.kafka.timeout"),
                "{name}"
            );
        }
    }

    #[test]
    fn planning_waits_no_longer_than_the_budget() {
        let p = profile_at(stalled_broker(), 30_000);
        let started = Instant::now();
        let e = scan(
            &p,
            &MemoryStore::default(),
            read(window()),
            &ScanCtx::new(Duration::from_secs(1)),
        )
        .expect_err("брокер не отвечает на метаданные");
        let took = started.elapsed();
        // Главное — время: 30-секундный таймаут подключения не применяется. На нагруженном раннере соединение с
        // фальшивым брокером может не успеть подняться за бюджет, тогда ошибка — транспортная, а не таймаут.
        assert!(took < Duration::from_secs(8), "{took:?}");
        assert!(
            matches!(e.code, ErrorCode::Timeout | ErrorCode::ConnectionFailed),
            "{e:?}"
        );
    }

    #[test]
    fn topic_without_describe_is_not_authorized_and_a_missing_one_is_not_found() {
        let cases = [
            (29, ErrorCode::NotAuthorized, "errors.kafka.notAuthorized"),
            (3, ErrorCode::NotFound, "errors.topics.notFound"),
        ];
        for (error, code, key) in cases {
            let p = profile_at(metadata_broker("orders", error), 5_000);
            for (name, op) in [("read", read(window())), ("summarize", summarize())] {
                let ctx = ScanCtx::new(Duration::from_secs(10));
                let e = scan(&p, &MemoryStore::default(), op, &ctx).expect_err(name);
                assert_eq!(
                    (e.code, e.key.as_str()),
                    (code, key),
                    "{name}, {error}: {e:?}"
                );
            }
        }
    }

    #[test]
    fn offsets_topic_without_describe_is_not_authorized() {
        let p = profile_at(metadata_broker(OFFSETS_TOPIC, 29), 5_000);
        let ctx = ScanCtx::new(Duration::from_secs(10));
        let e = scan(&p, &MemoryStore::default(), commits(), &ctx).expect_err("нет Describe");
        assert_eq!(
            (e.code, e.key.as_str()),
            (ErrorCode::NotAuthorized, "errors.kafka.notAuthorized")
        );
    }
}
