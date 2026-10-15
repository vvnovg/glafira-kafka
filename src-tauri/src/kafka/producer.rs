use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult, from_kafka};
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
            return Err(AppError::invalid("Идемпотентность требует acks = all"));
        }
        Ok(())
    }

    pub fn apply(&self, cfg: &mut ClientConfig) {
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

    /// Блокирующий: читает keychain и создаёт клиент. Не более MAX_PRODUCERS_PER_CONNECTION
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
    pub offset: i64,
    /// CreateTime, проставленный при отправке (мс).
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
            offset,
            timestamp,
        }),
        Err((e, _)) => {
            let cause = h.ctx.last_error_since(started);
            Err(match &e {
                KafkaError::MessageProduction(code) => item_error(
                    *code,
                    cause.as_deref().unwrap_or(""),
                    "Не удалось отправить сообщение",
                ),
                _ => from_kafka(&e, cause.as_deref()),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[test]
    fn settings_json_and_config() {
        let s: ProducerSettings = serde_json::from_value(
            serde_json::json!({"acks": "1", "compression": "zstd", "idempotence": false}),
        )
        .unwrap();
        let mut cfg = ClientConfig::new();
        s.apply(&mut cfg);
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
