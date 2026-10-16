use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult, ErrorCode, classify_message, from_kafka};
use crate::i18n::LocalizedText;
use crate::kafka::config::build_client_config;
use crate::kafka::context::CapturingContext;
use crate::kafka::errors::item_error;
use crate::kafka::pool::KeyedPool;
use crate::produce::payload::EncodedMessage;
use crate::secrets::SecretStore;
use crate::storage::connections::ConnectionProfile;
use crate::sync::MutexExt;

pub const MAX_PRODUCERS_PER_CONNECTION: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum Acks {
    #[serde(rename = "0")]
    Zero,
    #[serde(rename = "1")]
    One,
    #[default]
    #[serde(rename = "all")]
    All,
}

impl Acks {
    fn as_str(self) -> &'static str {
        match self {
            Acks::Zero => "0",
            Acks::One => "1",
            Acks::All => "all",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl Compression {
    fn as_str(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Gzip => "gzip",
            Compression::Snappy => "snappy",
            Compression::Lz4 => "lz4",
            Compression::Zstd => "zstd",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProducerSettings {
    pub acks: Acks,
    pub compression: Compression,
    pub idempotence: bool,
}

impl ProducerSettings {
    pub fn validate(&self) -> AppResult<()> {
        if self.idempotence && self.acks != Acks::All {
            return Err(app_err!(
                ErrorCode::InvalidInput,
                "errors.produce.idempotenceAcks",
                "Idempotence requires acks = all"
            ));
        }
        Ok(())
    }

    pub fn apply(&self, cfg: &mut ClientConfig) {
        // Как в Java-клиентах: ключ попадает в ту же партицию, что у Java/Spring-producer'ов.
        cfg.set("partitioner", "murmur2_random");
        cfg.set("acks", self.acks.as_str())
            .set("compression.type", self.compression.as_str())
            .set(
                "enable.idempotence",
                if self.idempotence { "true" } else { "false" },
            );
    }

    pub fn pool_key(&self) -> String {
        format!(
            "acks={};compression={};idempotence={}",
            self.acks.as_str(),
            self.compression.as_str(),
            self.idempotence
        )
    }
}

/// Producer одного подключения. Освобождение (drop) сбрасывает очередь и ждёт потоки
/// librdkafka (до ~500 мс): создавать и отпускать — только из blocking-контекста.
pub struct ProducerHandle {
    pub producer: FutureProducer<CapturingContext>,
    pub ctx: CapturingContext,
    pub timeout: Duration,
}

impl ProducerHandle {
    pub fn create(
        p: &ConnectionProfile,
        secrets: &dyn SecretStore,
        s: &ProducerSettings,
    ) -> AppResult<Self> {
        s.validate()?;
        let mut cfg = build_client_config(p, secrets)?;
        s.apply(&mut cfg);
        cfg.set("message.timeout.ms", p.request_timeout_ms.to_string());
        let ctx = CapturingContext::default();
        let producer = cfg
            .create_with_context(ctx.clone())
            .map_err(|e| from_kafka(&e, None))?;
        Ok(Self {
            producer,
            ctx,
            timeout: Duration::from_millis(p.request_timeout_ms),
        })
    }
}

/// Producer'ы по ключу «подключение|настройки».
///
/// Освобождение producer'а сбрасывает очередь и ждёт потоки librdkafka (до ~500 мс), поэтому
/// `get`, `evict_connection` и финальный drop handle — только из blocking-контекста.
#[derive(Default)]
pub struct ProducerPool {
    inner: KeyedPool<ProducerHandle>,
    /// Эпоха подключения: растёт при каждом вытеснении, даже если producer'ов ещё нет.
    epochs: Mutex<HashMap<String, u64>>,
}

impl ProducerPool {
    fn epoch(&self, connection_id: &str) -> u64 {
        self.epochs
            .lock_safe()
            .get(connection_id)
            .copied()
            .unwrap_or(0)
    }

    /// Блокирующий (в т. ч. вытеснение лишних producer'ов): читает keychain и создаёт клиент. Не более MAX_PRODUCERS_PER_CONNECTION
    /// на подключение.
    pub fn get(
        &self,
        p: &ConnectionProfile,
        secrets: &dyn SecretStore,
        s: &ProducerSettings,
    ) -> AppResult<Arc<ProducerHandle>> {
        self.get_with(p, s, || ProducerHandle::create(p, secrets, s))
    }

    fn get_with(
        &self,
        p: &ConnectionProfile,
        s: &ProducerSettings,
        create: impl FnOnce() -> AppResult<ProducerHandle>,
    ) -> AppResult<Arc<ProducerHandle>> {
        let prefix = format!("{}|", p.id);
        let key = format!("{prefix}{}", s.pool_key());
        let existing = self.inner.ids();
        let of_connection = existing.iter().filter(|id| id.starts_with(&prefix)).count();
        if !existing.contains(&key) && of_connection >= MAX_PRODUCERS_PER_CONNECTION {
            // Редкий случай (много разных настроек): проще пересоздать, чем вести LRU.
            self.evict_connection(&p.id);
        }
        let epoch = self.epoch(&p.id);
        let h = self.inner.get_or_create(&key, create)?;
        if self.epoch(&p.id) != epoch {
            // Подключение вытеснили во время создания (правка/удаление): не кэшируем.
            self.inner.evict(&key);
        }
        Ok(h)
    }

    pub fn evict_connection(&self, connection_id: &str) {
        *self
            .epochs
            .lock_safe()
            .entry(connection_id.to_string())
            .or_insert(0) += 1;
        let prefix = format!("{connection_id}|");
        self.inner.evict_where(|id| id.starts_with(&prefix));
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProduceResult {
    pub partition: i32,
    /// None, если librdkafka не сообщил offset (отрицательное значение, например acks=0).
    pub offset: Option<i64>,
    /// CreateTime (мс): время, которое клиент проставил в сообщение при отправке, а не
    /// время записи брокером (LogAppendTime топика может отличаться).
    pub timestamp: i64,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn owned_headers(headers: &[(String, Vec<u8>)]) -> OwnedHeaders {
    headers.iter().fold(OwnedHeaders::new(), |acc, (k, v)| {
        acc.insert(Header {
            key: k,
            value: Some(v.as_slice()),
        })
    })
}

/// Ошибка доставки: сначала известные коды librdkafka, затем классификация по тексту
/// (с учётом причины от клиента — TLS/SASL не должны выглядеть таймаутом).
fn delivery_error(code: RDKafkaErrorCode, cause: Option<&str>) -> AppError {
    use RDKafkaErrorCode as C;
    let context = || ltext!("errors.produce.sendFailed", "Failed to send the message");
    let details = |app: ErrorCode, text: LocalizedText| {
        let d = match cause {
            Some(c) => format!("{code}\n{c}"),
            None => code.to_string(),
        };
        AppError::localized(app, text).with_details(d)
    };
    // Причина от клиента (TLS/SASL/сеть) важнее кода только для таймаута и транспортных кодов;
    // детерминированные ответы брокера (слишком большое сообщение, нет топика) её не перекрывают.
    let client_cause_applies = matches!(
        code,
        C::MessageTimedOut
            | C::BrokerTransportFailure
            | C::AllBrokersDown
            | C::Resolve
            | C::Authentication
            | C::SSL
            | C::Fail
    );
    if client_cause_applies
        && let Some(c) = cause
        && let Some(app) = classify_message(c)
        && matches!(
            app,
            ErrorCode::TlsError | ErrorCode::AuthFailed | ErrorCode::ConnectionFailed
        )
    {
        return from_kafka(&KafkaError::MessageProduction(code), Some(c));
    }
    match code {
        C::MessageTimedOut => details(
            ErrorCode::Timeout,
            ltext!(
                "errors.produce.deliveryTimeout",
                "The message was not delivered within the timeout"
            ),
        ),
        C::UnknownTopic | C::UnknownTopicOrPartition => details(
            ErrorCode::NotFound,
            ltext!(
                "errors.produce.topicOrPartitionNotFound",
                "Topic or partition not found"
            ),
        ),
        C::MessageSizeTooLarge | C::InvalidMessageSize | C::MessageBatchTooLarge => details(
            ErrorCode::InvalidInput,
            ltext!(
                "errors.produce.messageTooLarge",
                "The message is too large for the broker"
            ),
        ),
        _ => {
            let e = item_error(code, cause.unwrap_or(""), context());
            if e.code != ErrorCode::Internal {
                e
            } else {
                let app = classify_message(&code.to_string()).unwrap_or(ErrorCode::Internal);
                AppError::localized(app, context()).with_details(e.details.unwrap_or_default())
            }
        }
    }
}

/// Отправка с ожиданием подтверждения не дольше таймаута запроса.
pub async fn send(h: &ProducerHandle, m: &EncodedMessage) -> AppResult<ProduceResult> {
    let timestamp = now_ms();
    let mut record: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(&m.topic)
        .timestamp(timestamp)
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
    let started = Instant::now();
    match h.producer.send(record, Timeout::After(h.timeout)).await {
        Ok((partition, offset)) => Ok(ProduceResult {
            partition,
            offset: (offset >= 0).then_some(offset),
            timestamp,
        }),
        Err((e, _)) => {
            let cause = h.ctx.last_error_since(started);
            Err(match &e {
                KafkaError::MessageProduction(code) => delivery_error(*code, cause.as_deref()),
                _ => from_kafka(&e, cause.as_deref()),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    /// Every codec offered in the Producer must be compiled into librdkafka on
    /// this platform (on Windows snappy needs `.cargo/config.toml` →
    /// `cmake/rdkafka-windows.cmake`, see that file).
    #[test]
    fn librdkafka_is_built_with_every_compression_codec() {
        let features = ClientConfig::new()
            .create_native_config()
            .unwrap()
            .get("builtin.features")
            .unwrap();
        let features: Vec<&str> = features.split(',').collect();
        for c in [
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let name = c.as_str();
            assert!(features.contains(&name), "{name} missing in {features:?}");
        }
    }

    #[test]
    fn settings_json_and_config() {
        let s: ProducerSettings = serde_json::from_value(
            serde_json::json!({"acks": "1", "compression": "zstd", "idempotence": false}),
        )
        .unwrap();
        let mut cfg = ClientConfig::new();
        s.apply(&mut cfg);
        assert_eq!(cfg.get("partitioner"), Some("murmur2_random"));
        assert_eq!(cfg.get("acks"), Some("1"));
        assert_eq!(cfg.get("compression.type"), Some("zstd"));
        assert_eq!(cfg.get("enable.idempotence"), Some("false"));
        assert_eq!(s.pool_key(), "acks=1;compression=zstd;idempotence=false");
        assert_eq!(
            serde_json::to_value(ProducerSettings::default()).unwrap(),
            serde_json::json!({"acks": "all", "compression": "none", "idempotence": false})
        );
    }

    #[test]
    fn delivery_errors_are_classified() {
        use RDKafkaErrorCode as C;
        let cases = [
            (C::MessageTimedOut, ErrorCode::Timeout),
            (C::UnknownTopic, ErrorCode::NotFound),
            (C::UnknownTopicOrPartition, ErrorCode::NotFound),
            (C::UnknownPartition, ErrorCode::InvalidInput),
            (C::MessageSizeTooLarge, ErrorCode::InvalidInput),
            (C::InvalidMessageSize, ErrorCode::InvalidInput),
            (C::TopicAuthorizationFailed, ErrorCode::NotAuthorized),
            (C::BrokerTransportFailure, ErrorCode::ConnectionFailed),
        ];
        for (code, expected) in cases {
            assert_eq!(delivery_error(code, None).code, expected, "{code:?}");
        }
        let e = delivery_error(C::MessageTimedOut, Some("ssl handshake failed"));
        assert_eq!(e.code, ErrorCode::TlsError, "TLS не выглядит таймаутом");
        let e = delivery_error(C::MessageTimedOut, Some("SASL authentication error"));
        assert_eq!(e.code, ErrorCode::AuthFailed);
    }

    #[test]
    fn deterministic_codes_win_over_connection_cause() {
        use RDKafkaErrorCode as C;
        let cause = Some("Connect to ipv4#127.0.0.1:9092 failed: Connection refused");
        let cases = [
            (C::MessageSizeTooLarge, ErrorCode::InvalidInput),
            (C::UnknownTopic, ErrorCode::NotFound),
            (C::UnknownTopicOrPartition, ErrorCode::NotFound),
        ];
        for (code, expected) in cases {
            assert_eq!(delivery_error(code, cause).code, expected, "{code:?}");
        }
        assert_eq!(
            delivery_error(C::MessageTimedOut, cause).code,
            ErrorCode::ConnectionFailed,
            "таймаут с причиной-соединением остаётся ConnectionFailed"
        );
        assert_eq!(
            delivery_error(C::BrokerTransportFailure, cause).code,
            ErrorCode::ConnectionFailed
        );
    }

    #[test]
    fn idempotence_requires_acks_all() {
        let s = ProducerSettings {
            acks: Acks::One,
            compression: Compression::None,
            idempotence: true,
        };
        assert_eq!(s.validate().unwrap_err().code, ErrorCode::InvalidInput);
        assert!(
            ProducerSettings {
                acks: Acks::All,
                ..s
            }
            .validate()
            .is_ok()
        );
    }
}

#[cfg(test)]
mod pool_tests {
    use super::*;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::sample_profile;

    #[test]
    fn caps_per_connection_and_evicts_with_connection() {
        let pool = ProducerPool::default();
        let secrets = MemoryStore::default();
        let (a, b) = (sample_profile("a"), sample_profile("b"));
        let s = |acks, compression| ProducerSettings {
            acks,
            compression,
            idempotence: false,
        };
        let a1 = pool
            .get(&a, &secrets, &s(Acks::All, Compression::None))
            .unwrap();
        let a2 = pool
            .get(&a, &secrets, &s(Acks::All, Compression::None))
            .unwrap();
        assert!(Arc::ptr_eq(&a1, &a2));
        let b1 = pool
            .get(&b, &secrets, &ProducerSettings::default())
            .unwrap();
        for c in [Compression::Gzip, Compression::Snappy, Compression::Lz4] {
            pool.get(&a, &secrets, &s(Acks::All, c)).unwrap();
        }
        assert_eq!(
            pool.inner
                .ids()
                .iter()
                .filter(|i| i.starts_with("a|"))
                .count(),
            4
        );
        pool.get(&a, &secrets, &s(Acks::One, Compression::Zstd))
            .unwrap();
        let n = pool
            .inner
            .ids()
            .iter()
            .filter(|i| i.starts_with("a|"))
            .count();
        assert!(n <= MAX_PRODUCERS_PER_CONNECTION);
        assert!(Arc::ptr_eq(
            &b1,
            &pool
                .get(&b, &secrets, &ProducerSettings::default())
                .unwrap()
        ));
        pool.evict_connection("a");
        assert!(pool.inner.ids().iter().all(|i| i.starts_with("b|")));
    }

    #[test]
    fn eviction_during_creation_discards_the_new_producer() {
        use std::sync::mpsc::channel;
        let pool = Arc::new(ProducerPool::default());
        let p = sample_profile("a");
        let s = ProducerSettings::default();
        let secrets = MemoryStore::default();
        let (started_tx, started_rx) = channel();
        let (go_tx, go_rx) = channel::<()>();
        let (pool2, p2) = (pool.clone(), p.clone());
        let t = std::thread::spawn(move || {
            pool2.get_with(&p2, &s, || {
                started_tx.send(()).unwrap();
                go_rx.recv().unwrap();
                ProducerHandle::create(&p2, &MemoryStore::default(), &s)
            })
        });
        started_rx.recv().unwrap();
        pool.evict_connection("a");
        go_tx.send(()).unwrap();
        let stale = t.join().unwrap().unwrap();
        assert!(
            pool.inner.ids().is_empty(),
            "устаревший producer не кэшируется"
        );
        let fresh = pool.get(&p, &secrets, &s).unwrap();
        assert!(!Arc::ptr_eq(&stale, &fresh));
    }
}
