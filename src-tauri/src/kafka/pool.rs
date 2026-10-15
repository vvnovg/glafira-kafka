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
use crate::sync::MutexExt;

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

/// Кэш «id → клиент». Клиент создаётся и освобождается вне мьютекса: чтение keychain и
/// создание/уничтожение клиента librdkafka занимают до секунд. Поколение id растёт при каждом
/// вытеснении: клиент, начатый до вытеснения (возможно, со старыми учётными данными),
/// возвращается вызывающему, но в пул не попадает.
pub struct KeyedPool<T> {
    inner: Mutex<PoolState<T>>,
}

struct PoolState<T> {
    items: HashMap<String, Arc<T>>,
    generations: HashMap<String, u64>,
}

impl<T> Default for KeyedPool<T> {
    fn default() -> Self {
        Self {
            inner: Mutex::new(PoolState {
                items: HashMap::new(),
                generations: HashMap::new(),
            }),
        }
    }
}

impl<T> KeyedPool<T> {
    pub fn get_or_create(
        &self,
        id: &str,
        create: impl FnOnce() -> AppResult<T>,
    ) -> AppResult<Arc<T>> {
        let generation = {
            let st = self.inner.lock_safe();
            if let Some(item) = st.items.get(id) {
                return Ok(item.clone());
            }
            st.generations.get(id).copied().unwrap_or(0)
        };
        let created = Arc::new(create()?);
        let mut st = self.inner.lock_safe();
        if st.generations.get(id).copied().unwrap_or(0) != generation {
            return Ok(created);
        }
        if let Some(existing) = st.items.get(id).cloned() {
            // Параллельный вызов успел раньше: наш экземпляр освобождаем уже вне мьютекса.
            drop(st);
            drop(created);
            return Ok(existing);
        }
        st.items.insert(id.to_string(), created.clone());
        Ok(created)
    }

    pub fn evict(&self, id: &str) {
        let removed = {
            let mut st = self.inner.lock_safe();
            *st.generations.entry(id.to_string()).or_insert(0) += 1;
            st.items.remove(id)
        };
        // Последняя ссылка (если она здесь) освобождается вне мьютекса.
        drop(removed);
    }
}

#[derive(Default)]
pub struct ClientPool {
    handles: KeyedPool<KafkaHandles>,
}

impl ClientPool {
    pub fn get(
        &self,
        p: &ConnectionProfile,
        secrets: &dyn SecretStore,
    ) -> AppResult<Arc<KafkaHandles>> {
        self.handles
            .get_or_create(&p.id, || KafkaHandles::create(p, secrets))
    }

    pub fn evict(&self, id: &str) {
        self.handles.evict(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;
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

    #[test]
    fn keyed_pool_reuses_until_evicted() {
        let pool = KeyedPool::<u32>::default();
        let a = pool.get_or_create("c", || Ok(1)).unwrap();
        let b = pool.get_or_create("c", || Ok(2)).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        pool.evict("c");
        assert_eq!(*pool.get_or_create("c", || Ok(3)).unwrap(), 3);
    }

    #[test]
    fn creation_errors_are_not_cached() {
        let pool = KeyedPool::<u32>::default();
        let e = pool
            .get_or_create("c", || Err(AppError::internal("нет")))
            .unwrap_err();
        assert_eq!(e.message, "нет");
        assert_eq!(*pool.get_or_create("c", || Ok(5)).unwrap(), 5);
    }

    #[test]
    fn create_runs_without_holding_the_lock() {
        let pool = Arc::new(KeyedPool::<u32>::default());
        let p2 = pool.clone();
        // Если бы мьютекс удерживался во время create, вложенный вызов зависал бы.
        let a = pool
            .get_or_create("a", || {
                p2.get_or_create("b", || Ok(7)).unwrap();
                Ok(1)
            })
            .unwrap();
        assert_eq!(*a, 1);
        assert_eq!(*pool.get_or_create("b", || Ok(0)).unwrap(), 7);
    }

    #[test]
    fn client_created_before_eviction_is_not_cached() {
        let pool = Arc::new(KeyedPool::<u32>::default());
        let p2 = pool.clone();
        // Вытеснение посреди создания (например, сохранили новый пароль): клиент отдаётся
        // вызывающему, но в пул не попадает — следующий вызов создаёт новый.
        let a = pool
            .get_or_create("c", || {
                p2.evict("c");
                Ok(1)
            })
            .unwrap();
        assert_eq!(*a, 1);
        assert_eq!(*pool.get_or_create("c", || Ok(2)).unwrap(), 2);
    }
}
