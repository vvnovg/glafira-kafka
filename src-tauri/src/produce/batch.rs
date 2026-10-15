use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rdkafka::client::ClientContext;
use rdkafka::error::KafkaError;
use rdkafka::producer::{BaseProducer, BaseRecord, DeliveryResult, ProducerContext};
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
    pub line: u64,
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
    Finished {
        sent: u64,
        failed: u64,
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

type Delivery = (u64, Result<(), String>);

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
        let r = result.as_ref().map(|_| ()).map_err(|(e, _)| e.to_string());
        let _ = self.deliveries.lock_safe().send((*line, r));
    }
}

struct Stats<'a> {
    sink: &'a dyn BatchSink,
    total: Option<u64>,
    sent: u64,
    failed: u64,
    in_flight: u64,
    errors: Vec<BatchError>,
    errors_dirty: bool,
    last_progress: Instant,
    closed: bool,
}

impl Stats<'_> {
    fn fail(&mut self, line: u64, message: String) {
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
                Ok(()) => self.sent += 1,
                Err(e) => self.fail(line, e),
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
    if let Err(error) = run_inner(profile, secrets, params, &cancel, sink) {
        sink.send(BatchEvent::Error { error });
    }
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
    cfg.set("message.timeout.ms", profile.request_timeout_ms.to_string());
    // Ограничиваем очередь librdkafka (по умолчанию до 1 ГБ): память пакета остаётся ограниченной,
    // при заполнении send возвращает QueueFull и цикл ждёт подтверждений.
    cfg.set("queue.buffering.max.kbytes", "65536");
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
                stats.fail(item.line, e);
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
                        break 'items;
                    }
                }
                Err((e, _)) => {
                    stats.fail(item.line, e.to_string());
                    break;
                }
            }
        }
        producer.poll(Duration::ZERO);
        stats.drain(&rx);
        stats.progress(false);
    }

    // Ждём подтверждения уже отправленного: до таймаута запроса, после отмены — до CANCEL_DRAIN.
    // message.timeout.ms = request_timeout_ms гарантирует отчёт librdkafka; запас — на сам poll.
    let timeout = Duration::from_millis(profile.request_timeout_ms) + Duration::from_secs(1);
    let mut deadline = Instant::now() + if cancelled { CANCEL_DRAIN } else { timeout };
    while stats.in_flight > 0 && Instant::now() < deadline {
        producer.poll(TICK);
        stats.drain(&rx);
        stats.progress(false);
        if !cancelled && stop(&stats) {
            cancelled = true;
            deadline = deadline.min(Instant::now() + CANCEL_DRAIN);
        }
    }
    if stats.in_flight > 0 {
        // Неподтверждённые сообщения считаются неудачными: доставлены ли они — неизвестно.
        let lost = stats.in_flight;
        stats.in_flight = 0;
        stats.failed += lost;
        if stats.errors.len() < MAX_REPORTED_ERRORS {
            let mut message = format!("{lost} сообщений не подтверждены брокером до таймаута");
            if let Some(cause) = capture.last_error() {
                message.push_str(&format!(": {cause}"));
            }
            stats.errors.push(BatchError { line: 0, message });
            stats.errors_dirty = true;
        }
    }
    stats.progress(true);
    let (sent, failed) = (stats.sent, stats.failed);
    stats.emit(BatchEvent::Finished {
        sent,
        failed,
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
            cancelled: false,
        })
        .unwrap();
        assert_eq!(
            v,
            serde_json::json!({"type": "finished", "sent": 3, "failed": 1, "cancelled": false})
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
}
