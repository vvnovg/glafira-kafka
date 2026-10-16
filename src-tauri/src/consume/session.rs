use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::{Offset, TopicPartitionList};

use crate::browse::filter::CompiledFilter;
use crate::browse::session::{
    BATCH_INTERVAL, BATCH_SIZE, MAX_BATCH_BYTES, estimated_bytes, to_browsed,
};
use crate::browse::types::BrowsedMessage;
use crate::consume::control::{ConsumerCommand, socket_timeout_ms};
use crate::consume::types::{
    CommitTarget, ConsumeEvent, ConsumerParams, IsolationLevel, OffsetReset, subscription, validate,
};
use crate::error::{AppError, AppResult, ErrorCode, from_kafka};
use crate::kafka::config::build_client_config;
use crate::kafka::context::CapturingContext;
use crate::kafka::errors::op_error;
use crate::kafka::groups::{OffsetTarget, TopicPartitionRef};
use crate::kafka::recovery::{PROBE_TIMEOUT, RecoveryMonitor, is_transient};
use crate::secrets::SecretStore;
use crate::storage::connections::ConnectionProfile;

const POLL_TIMEOUT: Duration = Duration::from_millis(100);
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

pub trait ConsumeSink: Send + 'static {
    /// false — получатель закрыт, сессию нужно завершить.
    fn send(&self, event: ConsumeEvent) -> bool;
}

/// Буферизация как в browse: до BATCH_SIZE сообщений / MAX_BATCH_BYTES или раз в BATCH_INTERVAL;
/// Progress — не реже раза в секунду.
struct Out<'a> {
    sink: &'a dyn ConsumeSink,
    batch: Vec<BrowsedMessage>,
    bytes: usize,
    consumed: u64,
    matched: u64,
    last_flush: Instant,
    last_progress: Instant,
    closed: bool,
}

impl Out<'_> {
    fn emit(&mut self, e: ConsumeEvent) {
        if !self.sink.send(e) {
            self.closed = true;
        }
    }

    fn push(&mut self, m: BrowsedMessage) {
        self.matched += 1;
        self.bytes += estimated_bytes(&m);
        self.batch.push(m);
    }

    fn flush(&mut self) {
        if !self.batch.is_empty() {
            let messages = std::mem::take(&mut self.batch);
            self.bytes = 0;
            self.emit(ConsumeEvent::Messages { messages });
        }
        self.last_flush = Instant::now();
        let (consumed, matched) = (self.consumed, self.matched);
        self.emit(ConsumeEvent::Progress { consumed, matched });
        self.last_progress = Instant::now();
    }

    fn maybe_flush(&mut self) {
        let due = self.batch.len() >= BATCH_SIZE
            || self.bytes > MAX_BATCH_BYTES
            || (!self.batch.is_empty() && self.last_flush.elapsed() >= BATCH_INTERVAL)
            || self.last_progress.elapsed() >= PROGRESS_INTERVAL;
        if due {
            self.flush();
        }
    }
}

/// Consumer-сессия в группе: subscribe, чтение с фильтром, журнал ребалансов, команды
/// commit/pause/resume. Блокирующая — запускать на выделенном потоке, не на async-рантайме.
/// Завершается по `cancel`, при закрытом sink или при фатальной ошибке (событие Error).
pub fn run_consumer(
    profile: &ConnectionProfile,
    secrets: &dyn SecretStore,
    params: ConsumerParams,
    cancel: Arc<AtomicBool>,
    commands: Receiver<ConsumerCommand>,
    sink: &dyn ConsumeSink,
) {
    let result = catch_unwind(AssertUnwindSafe(|| {
        run_inner(profile, secrets, &params, &cancel, &commands, sink)
    }));
    let error = match result {
        Ok(Ok(())) => return,
        Ok(Err(error)) => error,
        // Паника не должна оставлять UI без терминального события.
        Err(_) => AppError::internal_with("consumer session panicked"),
    };
    sink.send(ConsumeEvent::Error { error });
}

fn create(
    p: &ConnectionProfile,
    secrets: &dyn SecretStore,
    params: &ConsumerParams,
    ctx: &CapturingContext,
) -> AppResult<BaseConsumer<CapturingContext>> {
    let mut cfg = build_client_config(p, secrets)?;
    cfg.set("group.id", params.group_id.trim())
        .set(
            "client.id",
            format!("{}-session", crate::product::KAFKA_CLIENT_PREFIX),
        )
        .set(
            "socket.timeout.ms",
            socket_timeout_ms(p.request_timeout_ms).to_string(),
        )
        .set(
            "enable.auto.commit",
            if params.enable_auto_commit {
                "true"
            } else {
                "false"
            },
        )
        .set(
            "auto.offset.reset",
            match params.auto_offset_reset {
                OffsetReset::Earliest => "earliest",
                OffsetReset::Latest => "latest",
            },
        )
        .set(
            "isolation.level",
            match params.isolation_level {
                IsolationLevel::ReadCommitted => "read_committed",
                IsolationLevel::ReadUncommitted => "read_uncommitted",
            },
        );
    cfg.create_with_context(ctx.clone())
        .map_err(|e| from_kafka(&e, None))
}

fn tpl_of(partitions: &[TopicPartitionRef]) -> AppResult<TopicPartitionList> {
    let mut tpl = TopicPartitionList::new();
    for p in partitions {
        tpl.add_partition(&p.topic, p.partition);
    }
    if tpl.count() == 0 {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.consume.noPartitions",
            "No partitions selected"
        ));
    }
    Ok(tpl)
}

/// Список партиций для pause/resume. rdkafka возвращает ошибки по партициям лишь внутри
/// переданного списка (обёртка их отбрасывает), поэтому проверяем заранее: все запрошенные
/// партиции должны входить в текущее назначение, иначе ответ — ошибка, а не молчаливый успех.
fn assigned_tpl(
    consumer: &BaseConsumer<CapturingContext>,
    ctx: &CapturingContext,
    partitions: &[TopicPartitionRef],
) -> AppResult<TopicPartitionList> {
    let tpl = tpl_of(partitions)?;
    let assignment = consumer.assignment().map_err(|e| command_error(ctx, e))?;
    let missing: Vec<String> = partitions
        .iter()
        .filter(|p| assignment.find_partition(&p.topic, p.partition).is_none())
        .map(|p| format!("{}[{}]", p.topic, p.partition))
        .collect();
    if !missing.is_empty() {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.consume.notAssigned",
            "Partitions not assigned to this session: {{partitions}}",
            partitions = missing.join(", ")
        ));
    }
    Ok(tpl)
}

/// Ошибки команд не завершают сессию; причину берём из последней ошибки клиента без опроса
/// консьюмера (опрос забрал бы сообщения из сессии).
fn command_error(ctx: &CapturingContext, e: KafkaError) -> AppError {
    from_kafka(&e, ctx.last_error().as_deref())
}

fn commit(
    consumer: &BaseConsumer<CapturingContext>,
    ctx: &CapturingContext,
    target: CommitTarget,
) -> AppResult<Vec<OffsetTarget>> {
    let offsets: Vec<OffsetTarget> = match target {
        CommitTarget::Selected {
            topic,
            partition,
            offset,
        } => {
            if offset < 0 {
                return Err(app_err!(
                    ErrorCode::InvalidInput,
                    "errors.consume.badOffset",
                    "Invalid message offset"
                ));
            }
            vec![OffsetTarget {
                topic,
                partition,
                offset: offset + 1,
            }]
        }
        CommitTarget::AllConsumed => consumer
            .position()
            .map_err(|e| command_error(ctx, e))?
            .elements()
            .iter()
            .filter_map(|e| match e.offset() {
                Offset::Offset(o) if o >= 0 => Some(OffsetTarget {
                    topic: e.topic().to_string(),
                    partition: e.partition(),
                    offset: o,
                }),
                _ => None,
            })
            .collect(),
    };
    if offsets.is_empty() {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.consume.nothingToCommit",
            "No consumed messages to commit"
        ));
    }
    let mut tpl = TopicPartitionList::new();
    for o in &offsets {
        tpl.add_partition_offset(&o.topic, o.partition, Offset::Offset(o.offset))
            .map_err(|e| command_error(ctx, e))?;
    }
    consumer
        .commit(&tpl, CommitMode::Sync)
        .map_err(|e| command_error(ctx, e))?;
    Ok(offsets)
}

fn handle(
    cmd: ConsumerCommand,
    consumer: &BaseConsumer<CapturingContext>,
    ctx: &CapturingContext,
    out: &mut Out,
) {
    match cmd {
        ConsumerCommand::Commit {
            target,
            reply,
            deadline,
        } => {
            if Instant::now() >= deadline {
                return;
            }
            let r = commit(consumer, ctx, target);
            if let Ok(offsets) = &r {
                out.emit(ConsumeEvent::Committed {
                    offsets: offsets.clone(),
                });
            }
            let _ = reply.send(r);
        }
        ConsumerCommand::Pause {
            partitions,
            reply,
            deadline,
        } => {
            if Instant::now() >= deadline {
                return;
            }
            let r = assigned_tpl(consumer, ctx, &partitions)
                .and_then(|t| consumer.pause(&t).map_err(|e| command_error(ctx, e)));
            if r.is_ok() {
                out.emit(ConsumeEvent::Paused { partitions });
            }
            let _ = reply.send(r);
        }
        ConsumerCommand::Resume {
            partitions,
            reply,
            deadline,
        } => {
            if Instant::now() >= deadline {
                return;
            }
            let r = assigned_tpl(consumer, ctx, &partitions)
                .and_then(|t| consumer.resume(&t).map_err(|e| command_error(ctx, e)));
            if r.is_ok() {
                out.emit(ConsumeEvent::Resumed { partitions });
            }
            let _ = reply.send(r);
        }
    }
}

fn run_inner(
    profile: &ConnectionProfile,
    secrets: &dyn SecretStore,
    params: &ConsumerParams,
    cancel: &AtomicBool,
    commands: &Receiver<ConsumerCommand>,
    sink: &dyn ConsumeSink,
) -> AppResult<()> {
    validate(params)?;
    let filter = CompiledFilter::compile(&params.filter)?;
    let ctx = CapturingContext::default();
    let consumer = create(profile, secrets, params, &ctx)?;
    let topics = subscription(params);
    let refs: Vec<&str> = topics.iter().map(String::as_str).collect();
    consumer
        .subscribe(&refs)
        .map_err(|e| from_kafka(&e, None))?;

    let mut out = Out {
        sink,
        batch: Vec::with_capacity(BATCH_SIZE),
        bytes: 0,
        consumed: 0,
        matched: 0,
        last_flush: Instant::now(),
        last_progress: Instant::now(),
        closed: false,
    };
    let mut recovery = RecoveryMonitor::default();
    let result = loop {
        if cancel.load(Ordering::Relaxed) || out.closed {
            break Ok(());
        }
        let now = Instant::now();
        if let Some((since, e)) = recovery.expired(now) {
            let e = e.clone();
            out.flush();
            break Err(op_error(&consumer, since, e));
        }
        if recovery.should_probe(now) {
            let ok = consumer.fetch_metadata(None, PROBE_TIMEOUT).is_ok();
            if recovery.on_probe(Instant::now(), ok) {
                out.emit(ConsumeEvent::Recovered);
            }
        }
        let polled_at = Instant::now();
        match consumer.poll(POLL_TIMEOUT) {
            None => {}
            // Конец партиции — штатное событие, не ошибка.
            Some(Err(KafkaError::PartitionEOF(_))) => {}
            Some(Err(e)) if is_transient(&e) => {
                let message = from_kafka(&e, ctx.last_error_since(polled_at).as_deref()).text();
                if recovery.on_transient(Instant::now(), e) {
                    out.flush();
                    out.emit(ConsumeEvent::Reconnecting { message });
                }
            }
            Some(Err(e)) => {
                out.flush();
                break Err(op_error(&consumer, polled_at, e));
            }
            Some(Ok(msg)) => {
                if recovery.on_progress() {
                    out.emit(ConsumeEvent::Recovered);
                }
                out.consumed += 1;
                let m = to_browsed(&msg, params.key_format, params.value_format);
                if filter.matches(&m.view()) {
                    out.push(m);
                }
            }
        }
        for record in ctx.take_rebalances() {
            out.flush();
            out.emit(ConsumeEvent::Rebalance { record });
        }
        while let Ok(cmd) = commands.try_recv() {
            handle(cmd, &consumer, &ctx, &mut out);
        }
        out.maybe_flush();
    };
    out.flush();
    // Выход из группы: unsubscribe и закрытие консьюмера (Drop дожидается consumer_close).
    consumer.unsubscribe();
    drop(consumer);
    for record in ctx.take_rebalances() {
        out.emit(ConsumeEvent::Rebalance { record });
    }
    result?;
    let (consumed, matched) = (out.consumed, out.matched);
    out.emit(ConsumeEvent::Finished { consumed, matched });
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;

    use super::*;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::sample_profile;

    /// Паникует на первом событии, остальные запоминает.
    struct PanicOnce(AtomicBool, Mutex<Vec<ConsumeEvent>>);
    impl ConsumeSink for PanicOnce {
        fn send(&self, e: ConsumeEvent) -> bool {
            if !self.0.swap(true, Ordering::SeqCst) {
                panic!("boom");
            }
            self.1.lock().unwrap().push(e);
            true
        }
    }

    #[test]
    fn panic_yields_terminal_error_event() {
        let params: ConsumerParams = serde_json::from_value(serde_json::json!({
            "connectionId": "c", "groupId": "g", "topics": ["t"],
            "autoOffsetReset": "earliest", "enableAutoCommit": false, "isolationLevel": "readCommitted",
            "keyFormat": "auto", "valueFormat": "auto"
        }))
        .unwrap();
        let mut profile = sample_profile("c");
        profile.bootstrap_servers = "127.0.0.1:1".into();
        let sink = PanicOnce(AtomicBool::new(false), Mutex::default());
        let (_tx, rx) = mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(true));
        run_consumer(&profile, &MemoryStore::default(), params, cancel, rx, &sink);
        let events = sink.1.lock().unwrap();
        match events.last() {
            Some(ConsumeEvent::Error { error }) => {
                assert_eq!(error.key, "errors.internal")
            }
            other => panic!("ожидали Error, получили {other:?}"),
        }
    }
}
