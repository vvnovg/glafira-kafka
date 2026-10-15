use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rdkafka::client::ClientContext;
use rdkafka::error::KafkaError;
use rdkafka::producer::{
    BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext, PurgeConfig,
};
use rdkafka::types::RDKafkaErrorCode;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult, from_kafka};
use crate::kafka::config::build_client_config;
use crate::kafka::context::CapturingContext;
use crate::kafka::producer::{ProducerSettings, owned_headers};
use crate::produce::payload::{ProduceMessage, encode_message};
use crate::produce::rate::RateLimiter;
use crate::produce::source::{FileFormat, SourceItem, open_file};
use crate::produce::template::{RenderContext, render_message, system_env};
use crate::secrets::SecretStore;
use crate::storage::connections::ConnectionProfile;
use crate::sync::MutexExt;

pub const MAX_COPIES: u64 = 1_000_000;
pub const MAX_REPORTED_ERRORS: usize = 20;
const TICK: Duration = Duration::from_millis(100);
/// Сколько после отмены ждём подтверждений уже отправленных сообщений.
const CANCEL_DRAIN: Duration = Duration::from_secs(2);
/// Локальная очередь librdkafka пакета (по умолчанию 100 000 сообщений / 1 ГБ). Время в очереди
/// входит в message.timeout.ms: длинная очередь на медленном канале истекала бы, не уйдя в сеть.
const QUEUE_MAX_MESSAGES: u32 = 10_000;
const QUEUE_MAX_KBYTES: u32 = 65_536;
/// Нижняя граница message.timeout.ms пакетного producer'а (ожидание в очереди + доставка).
const MIN_MESSAGE_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BatchSource {
    Copies {
        count: u64,
        message: ProduceMessage,
    },
    File {
        path: String,
        format: FileFormat,
        topic: String,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchParams {
    pub connection_id: String,
    pub source: BatchSource,
    pub settings: ProducerSettings,
    #[serde(default)]
    pub rate_per_sec: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchError {
    /// Номер строки файла / копии (1-based); None — сводная ошибка, не относящаяся к строке.
    pub line: Option<u64>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BatchEvent {
    Progress {
        sent: u64,
        failed: u64,
        total: Option<u64>,
    },
    /// Первые MAX_REPORTED_ERRORS ошибок (список целиком, при каждом изменении).
    Errors {
        errors: Vec<BatchError>,
    },
    /// sent + failed + notSent = число обработанных элементов источника.
    Finished {
        sent: u64,
        failed: u64,
        /// Отменено до отправки (UI: «отменено до отправки: N»): вынуто из локальной очереди
        /// librdkafka при отмене или не поставлено в неё. Не ошибки и не в failed.
        not_sent: u64,
        cancelled: bool,
    },
    Error {
        error: AppError,
    },
}

pub trait BatchSink: Send + 'static {
    /// false — получатель закрыт.
    fn send(&self, event: BatchEvent) -> bool;
}

pub fn validate(p: &BatchParams) -> AppResult<()> {
    p.settings.validate()?;
    match &p.source {
        BatchSource::Copies { count, .. } if *count == 0 || *count > MAX_COPIES => Err(
            AppError::invalid(format!("Количество копий — от 1 до {MAX_COPIES}")),
        ),
        // Шаблон проверяется заранее на первой копии: иначе ошибка плейсхолдера повторилась бы N раз.
        BatchSource::Copies { message, .. } => {
            let mut rng = fastrand::Rng::new();
            let first = render_message(
                message,
                &mut RenderContext {
                    seq: 0,
                    now: chrono::Utc::now(),
                    rng: &mut rng,
                    env: &system_env,
                },
            )?;
            encode_message(&first).map(|_| ())
        }
        BatchSource::File { path, topic, .. }
            if path.trim().is_empty() || topic.trim().is_empty() =>
        {
            Err(AppError::invalid("Укажите файл и топик"))
        }
        BatchSource::File { .. } => Ok(()),
    }
}

/// N копий сообщения: плейсхолдеры рендерятся для каждой копии, `{{seq}}` = 0..N−1.
fn copies_iter(count: u64, message: ProduceMessage) -> impl Iterator<Item = SourceItem> + Send {
    let mut rng = fastrand::Rng::new();
    (0..count).map(move |seq| {
        let rendered = render_message(
            &message,
            &mut RenderContext {
                seq,
                now: chrono::Utc::now(),
                rng: &mut rng,
                env: &system_env,
            },
        );
        SourceItem {
            line: seq + 1,
            message: rendered
                .and_then(|m| encode_message(&m))
                .map_err(|e| e.message),
        }
    })
}

enum Outcome {
    Sent,
    Failed(String),
    /// Сообщение вынуто из локальной очереди при отмене (PurgeQueue) — в сеть не уходило.
    NotSent,
}

type Delivery = (u64, Outcome);

struct BatchContext {
    capture: CapturingContext,
    deliveries: Mutex<Sender<Delivery>>,
}

impl ClientContext for BatchContext {
    fn error(&self, error: KafkaError, reason: &str) {
        self.capture.error(error, reason);
    }
}

impl ProducerContext for BatchContext {
    type DeliveryOpaque = Box<u64>;

    fn delivery(&self, result: &DeliveryResult<'_>, line: Box<u64>) {
        let r = match result {
            Ok(_) => Outcome::Sent,
            Err((e, _)) if e.rdkafka_error_code() == Some(RDKafkaErrorCode::PurgeQueue) => {
                Outcome::NotSent
            }
            Err((e, _)) => Outcome::Failed(e.to_string()),
        };
        let _ = self.deliveries.lock_safe().send((*line, r));
    }
}

struct Stats<'a> {
    sink: &'a dyn BatchSink,
    total: Option<u64>,
    sent: u64,
    failed: u64,
    not_sent: u64,
    in_flight: u64,
    errors: Vec<BatchError>,
    errors_dirty: bool,
    last_progress: Instant,
    closed: bool,
}

impl Stats<'_> {
    fn fail(&mut self, line: Option<u64>, message: String) {
        self.failed += 1;
        if self.errors.len() < MAX_REPORTED_ERRORS {
            self.errors.push(BatchError { line, message });
            self.errors_dirty = true;
        }
    }

    fn drain(&mut self, rx: &Receiver<Delivery>) {
        while let Ok((line, r)) = rx.try_recv() {
            self.in_flight = self.in_flight.saturating_sub(1);
            match r {
                Outcome::Sent => self.sent += 1,
                Outcome::Failed(e) => self.fail(Some(line), e),
                Outcome::NotSent => self.not_sent += 1,
            }
        }
    }

    fn emit(&mut self, e: BatchEvent) {
        if !self.sink.send(e) {
            self.closed = true;
        }
    }

    fn progress(&mut self, force: bool) {
        if !force && self.last_progress.elapsed() < TICK {
            return;
        }
        if self.errors_dirty {
            self.errors_dirty = false;
            let errors = self.errors.clone();
            self.emit(BatchEvent::Errors { errors });
        }
        let (sent, failed, total) = (self.sent, self.failed, self.total);
        self.emit(BatchEvent::Progress {
            sent,
            failed,
            total,
        });
        self.last_progress = Instant::now();
    }
}

pub fn run_batch(
    profile: &ConnectionProfile,
    secrets: &dyn SecretStore,
    params: BatchParams,
    cancel: Arc<AtomicBool>,
    sink: &dyn BatchSink,
) {
    let result = catch_unwind(AssertUnwindSafe(|| {
        run_inner(profile, secrets, params, &cancel, sink)
    }));
    let error = match result {
        Ok(Ok(())) => return,
        Ok(Err(error)) => error,
        // Паника не должна оставлять UI в «идёт отправка» без терминального события.
        Err(_) => AppError::internal("Внутренняя ошибка пакетной отправки"),
    };
    sink.send(BatchEvent::Error { error });
}

fn run_inner(
    profile: &ConnectionProfile,
    secrets: &dyn SecretStore,
    params: BatchParams,
    cancel: &AtomicBool,
    sink: &dyn BatchSink,
) -> AppResult<()> {
    validate(&params)?;
    let (items, total): (Box<dyn Iterator<Item = SourceItem> + Send>, Option<u64>) =
        match params.source {
            BatchSource::Copies { count, message } => {
                (Box::new(copies_iter(count, message)), Some(count))
            }
            BatchSource::File {
                path,
                format,
                topic,
            } => (open_file(Path::new(&path), format, &topic)?, None),
        };
    let mut cfg = build_client_config(profile, secrets)?;
    params.settings.apply(&mut cfg);
    // Очередь ограничена (память и время ожидания в ней), при заполнении send возвращает
    // QueueFull и цикл ждёт подтверждений. Таймаут сообщения покрывает и ожидание в очереди,
    // поэтому для пакета он не меньше MIN_MESSAGE_TIMEOUT.
    let message_timeout =
        Duration::from_millis(profile.request_timeout_ms).max(MIN_MESSAGE_TIMEOUT);
    cfg.set(
        "message.timeout.ms",
        message_timeout.as_millis().to_string(),
    )
    .set(
        "queue.buffering.max.messages",
        QUEUE_MAX_MESSAGES.to_string(),
    )
    .set("queue.buffering.max.kbytes", QUEUE_MAX_KBYTES.to_string());
    let (tx, rx) = mpsc::channel();
    let capture = CapturingContext::default();
    let ctx = BatchContext {
        capture: capture.clone(),
        deliveries: Mutex::new(tx),
    };
    let producer: BaseProducer<BatchContext> = cfg
        .create_with_context(ctx)
        .map_err(|e| from_kafka(&e, None))?;

    let mut stats = Stats {
        sink,
        total,
        sent: 0,
        failed: 0,
        not_sent: 0,
        in_flight: 0,
        errors: Vec::new(),
        errors_dirty: false,
        last_progress: Instant::now(),
        closed: false,
    };
    let mut limiter = RateLimiter::new(params.rate_per_sec, Instant::now());
    let mut cancelled = false;
    let stop = |stats: &Stats| cancel.load(Ordering::Relaxed) || stats.closed;

    'items: for item in items {
        // Ограничение скорости: ждём короткими шагами, чтобы отмена срабатывала быстро.
        loop {
            if stop(&stats) {
                cancelled = true;
                stats.not_sent += 1;
                break 'items;
            }
            let wait = limiter.wait_time(Instant::now());
            if wait.is_zero() {
                break;
            }
            producer.poll(wait.min(TICK));
            stats.drain(&rx);
            stats.progress(false);
        }
        limiter.record();
        let m = match item.message {
            Ok(m) => m,
            Err(e) => {
                stats.fail(Some(item.line), e);
                continue;
            }
        };
        let mut record: BaseRecord<'_, [u8], [u8], Box<u64>> =
            BaseRecord::with_opaque_to(&m.topic, Box::new(item.line))
                .headers(owned_headers(&m.headers));
        if let Some(k) = &m.key {
            record = record.key(k.as_slice());
        }
        if let Some(v) = &m.value {
            record = record.payload(v.as_slice());
        }
        if let Some(p) = m.partition {
            record = record.partition(p);
        }
        loop {
            match producer.send(record) {
                Ok(()) => {
                    stats.in_flight += 1;
                    break;
                }
                // Очередь librdkafka заполнена: даём ей отправить накопленное.
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), r)) => {
                    record = r;
                    producer.poll(TICK);
                    stats.drain(&rx);
                    stats.progress(false);
                    if stop(&stats) {
                        cancelled = true;
                        stats.not_sent += 1;
                        break 'items;
                    }
                }
                Err((e, _)) => {
                    stats.fail(Some(item.line), e.to_string());
                    break;
                }
            }
        }
        producer.poll(Duration::ZERO);
        stats.drain(&rx);
        stats.progress(false);
    }

    // Отмена: локальная очередь (ещё не ушедшее в сеть) вынимается сразу — эти сообщения придут
    // в delivery с PurgeQueue и попадут в notSent; отправленные брокеру ждём до CANCEL_DRAIN.
    let purge_queue = || producer.purge(PurgeConfig::default().queue());
    if cancelled {
        purge_queue();
    }
    // Без отмены ждём до message.timeout.ms (librdkafka гарантирует отчёт), запас — на сам poll.
    let timeout = message_timeout + Duration::from_secs(1);
    let mut deadline = Instant::now() + if cancelled { CANCEL_DRAIN } else { timeout };
    while stats.in_flight > 0 && Instant::now() < deadline {
        producer.poll(TICK);
        stats.drain(&rx);
        stats.progress(false);
        if !cancelled && stop(&stats) {
            cancelled = true;
            purge_queue();
            deadline = deadline.min(Instant::now() + CANCEL_DRAIN);
        }
    }
    if stats.in_flight > 0 {
        // Неподтверждённые сообщения считаются неудачными: доставлены ли они — неизвестно.
        let lost = stats.in_flight;
        stats.in_flight = 0;
        stats.failed += lost;
        if stats.errors.len() < MAX_REPORTED_ERRORS {
            let mut message = if cancelled {
                format!("{lost} сообщений не подтверждены брокером к моменту отмены")
            } else {
                format!("{lost} сообщений не подтверждены брокером до таймаута")
            };
            if let Some(cause) = capture.last_error() {
                message.push_str(&format!(": {cause}"));
            }
            stats.errors.push(BatchError {
                line: None,
                message,
            });
            stats.errors_dirty = true;
        }
    }
    stats.progress(true);
    let (sent, failed, not_sent) = (stats.sent, stats.failed, stats.not_sent);
    stats.emit(BatchEvent::Finished {
        sent,
        failed,
        not_sent,
        cancelled,
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::produce::payload::{PayloadFormat, PayloadInput};

    fn copies(count: u64) -> BatchParams {
        BatchParams {
            connection_id: "c".into(),
            source: BatchSource::Copies {
                count,
                message: ProduceMessage {
                    topic: "t".into(),
                    partition: None,
                    key: PayloadInput {
                        format: PayloadFormat::Null,
                        text: String::new(),
                    },
                    value: PayloadInput {
                        format: PayloadFormat::String,
                        text: "{{seq}}".into(),
                    },
                    headers: vec![],
                },
            },
            settings: ProducerSettings::default(),
            rate_per_sec: None,
        }
    }

    #[test]
    fn validation() {
        assert!(validate(&copies(1)).is_ok());
        assert!(validate(&copies(MAX_COPIES)).is_ok());
        for bad in [copies(0), copies(MAX_COPIES + 1)] {
            assert_eq!(validate(&bad).unwrap_err().code, ErrorCode::InvalidInput);
        }
        let mut bad_template = copies(1);
        if let BatchSource::Copies { message, .. } = &mut bad_template.source {
            message.value.text = "{{nope}}".into();
        }
        assert_eq!(
            validate(&bad_template).unwrap_err().code,
            ErrorCode::InvalidInput
        );
        let file = BatchParams {
            source: BatchSource::File {
                path: " ".into(),
                format: FileFormat::Jsonl,
                topic: "t".into(),
            },
            ..copies(1)
        };
        assert_eq!(validate(&file).unwrap_err().code, ErrorCode::InvalidInput);
    }

    #[test]
    fn params_json() {
        let p: BatchParams = serde_json::from_value(serde_json::json!({
            "connectionId": "c",
            "source": {"type": "file", "path": "/tmp/x.jsonl", "format": "jsonl", "topic": "t"},
            "settings": {"acks": "1", "compression": "none", "idempotence": false},
            "ratePerSec": 100
        }))
        .unwrap();
        assert!(matches!(
            p.source,
            BatchSource::File {
                format: FileFormat::Jsonl,
                ..
            }
        ));
        assert_eq!(p.rate_per_sec, Some(100));
        let v = serde_json::to_value(BatchEvent::Finished {
            sent: 3,
            failed: 1,
            not_sent: 2,
            cancelled: false,
        })
        .unwrap();
        assert_eq!(
            v,
            serde_json::json!({"type": "finished", "sent": 3, "failed": 1, "notSent": 2, "cancelled": false})
        );
        let v = serde_json::to_value(BatchEvent::Errors {
            errors: vec![BatchError {
                line: None,
                message: "m".into(),
            }],
        })
        .unwrap();
        assert_eq!(
            v,
            serde_json::json!({"type": "errors", "errors": [{"line": null, "message": "m"}]})
        );
    }

    struct VecSink(Mutex<Vec<BatchEvent>>);

    impl BatchSink for VecSink {
        fn send(&self, e: BatchEvent) -> bool {
            self.0.lock_safe().push(e);
            true
        }
    }

    #[test]
    fn cancel_of_unthrottled_batch_counts_queued_as_not_sent() {
        // Брокер недоступен: всё отправленное лежит в локальной очереди librdkafka,
        // очередь заполняется (QUEUE_MAX_MESSAGES) и цикл ждёт в QueueFull.
        let mut profile = crate::storage::connections::sample_profile("c");
        profile.bootstrap_servers = "127.0.0.1:1".into();
        let cancel = Arc::new(AtomicBool::new(false));
        let c2 = cancel.clone();
        let canceller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
            c2.store(true, Ordering::Relaxed);
        });
        let sink = VecSink(Mutex::new(Vec::new()));
        let started = Instant::now();
        run_batch(
            &profile,
            &crate::secrets::MemoryStore::default(),
            copies(50_000),
            cancel,
            &sink,
        );
        canceller.join().unwrap();
        assert!(started.elapsed() < Duration::from_secs(5), "отмена быстрая");
        let events = sink.0.lock_safe();
        match events.last() {
            Some(BatchEvent::Finished {
                sent,
                failed,
                not_sent,
                cancelled,
            }) => {
                assert!(*cancelled);
                assert_eq!((*sent, *failed), (0, 0), "вынутое из очереди — не ошибки");
                assert!(
                    *not_sent >= u64::from(QUEUE_MAX_MESSAGES),
                    "notSent = {not_sent}"
                );
                assert!(*not_sent < 50_000);
            }
            other => panic!("ожидали Finished, получили {other:?}"),
        }
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, BatchEvent::Errors { .. })),
            "отмена не порождает ошибок"
        );
    }

    #[test]
    fn copies_render_seq_per_message() {
        let BatchSource::Copies { message, .. } = copies(3).source else {
            unreachable!()
        };
        let items: Vec<SourceItem> = copies_iter(3, message).collect();
        let values: Vec<Vec<u8>> = items
            .into_iter()
            .map(|i| i.message.unwrap().value.unwrap())
            .collect();
        assert_eq!(values, [b"0".to_vec(), b"1".to_vec(), b"2".to_vec()]);
    }

    /// Паникует на первом событии, остальные запоминает.
    struct PanicOnce(AtomicBool, Mutex<Vec<BatchEvent>>);

    impl BatchSink for PanicOnce {
        fn send(&self, e: BatchEvent) -> bool {
            if !self.0.swap(true, Ordering::SeqCst) {
                panic!("boom");
            }
            self.1.lock_safe().push(e);
            true
        }
    }

    #[test]
    fn panic_yields_terminal_error_event() {
        let mut profile = crate::storage::connections::sample_profile("c");
        profile.bootstrap_servers = "127.0.0.1:1".into();
        let sink = PanicOnce(AtomicBool::new(false), Mutex::default());
        // Отменено сразу: первое же событие (Finished) паникует в sink.
        run_batch(
            &profile,
            &crate::secrets::MemoryStore::default(),
            copies(1),
            Arc::new(AtomicBool::new(true)),
            &sink,
        );
        let events = sink.1.lock_safe();
        match events.last() {
            Some(BatchEvent::Error { error }) => {
                assert_eq!(error.message, "Внутренняя ошибка пакетной отправки")
            }
            other => panic!("ожидали Error, получили {other:?}"),
        }
    }
}
