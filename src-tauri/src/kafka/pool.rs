use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rdkafka::admin::AdminClient;
use rdkafka::consumer::BaseConsumer;
use rdkafka::error::KafkaError;

use crate::error::{AppError, AppResult, from_kafka};
use crate::kafka::config::build_client_config;
use crate::kafka::context::CapturingContext;
use crate::kafka::errors::op_error;
use crate::secrets::SecretStore;
use crate::storage::connections::ConnectionProfile;

/// Клиенты одного подключения: admin — для DescribeConfigs и т. п.,
/// consumer — для метаданных и ListOffsets (никогда не подписывается и не коммитит).
pub struct KafkaHandles {
    pub admin: AdminClient<CapturingContext>,
    pub consumer: BaseConsumer<CapturingContext>,
    pub ctx: CapturingContext,
    pub timeout: Duration,
}

impl KafkaHandles {
    pub fn create(p: &ConnectionProfile, secrets: &dyn SecretStore) -> AppResult<Self> {
        let base = build_client_config(p, secrets)?;
        let ctx = CapturingContext::default();
        let admin: AdminClient<CapturingContext> = base
            .create_with_context(ctx.clone())
            .map_err(|e| from_kafka(&e, None))?;
        let mut consumer_cfg = base.clone();
        consumer_cfg
            .set("group.id", "kafka-explorer-metadata")
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false");
        let consumer: BaseConsumer<CapturingContext> = consumer_cfg
            .create_with_context(ctx.clone())
            .map_err(|e| from_kafka(&e, None))?;
        Ok(Self {
            admin,
            consumer,
            ctx,
            timeout: Duration::from_millis(p.request_timeout_ms),
        })
    }

    /// Ошибка операции, начатой в `started`: опрашивает pooled-консьюмер, чтобы получить
    /// причину (TLS/SASL). Блокирует до ~1.5 с — вызывать только из blocking-контекста.
    pub fn kafka_err(&self, started: Instant, e: KafkaError) -> AppError {
        op_error(&self.consumer, started, e)
    }
}

#[derive(Default)]
pub struct ClientPool {
    inner: Mutex<HashMap<String, Arc<KafkaHandles>>>,
}

impl ClientPool {
    pub fn get(
        &self,
        p: &ConnectionProfile,
        secrets: &dyn SecretStore,
    ) -> AppResult<Arc<KafkaHandles>> {
        let mut map = self.inner.lock().unwrap();
        if let Some(h) = map.get(&p.id) {
            return Ok(h.clone());
        }
        let handles = Arc::new(KafkaHandles::create(p, secrets)?);
        map.insert(p.id.clone(), handles.clone());
        Ok(handles)
    }

    pub fn evict(&self, id: &str) {
        self.inner.lock().unwrap().remove(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::sample_profile;

    #[test]
    fn pool_reuses_handles_until_evicted() {
        let pool = ClientPool::default();
        let secrets = MemoryStore::default();
        let p = sample_profile("c1");
        let a = pool.get(&p, &secrets).unwrap();
        let b = pool.get(&p, &secrets).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        pool.evict("c1");
        let c = pool.get(&p, &secrets).unwrap();
        assert!(!Arc::ptr_eq(&a, &c));
        assert_eq!(c.timeout, Duration::from_millis(30_000));
    }
}
