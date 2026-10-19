//! Сборка `ClusterSnapshot` (спец. G2 §3): метаданные, конфиги брокеров и топиков, группы.
//! Источник — за трейтом `Source`: лимиты и дедлайн проверяются без Kafka.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::Instant;

use super::snapshot::{ClusterSnapshot, ConfigMap, Coverage, GroupSnapshot};
use crate::error::{AppResult, ErrorCode};

pub type Fut<T> = Pin<Box<dyn Future<Output = AppResult<T>> + Send + 'static>>;

pub use crate::kafka::cluster::ClusterMetadata as Metadata;

/// Описание группы для снимка; `None` — брокер вернул ошибку для группы.
pub type GroupRead = (String, Option<GroupSnapshot>);

/// Чтение кластера для снимка. Реализация на Kafka — `kafka_source.rs`, в тестах — фейк.
pub trait Source: Send + Sync + 'static {
    fn metadata(&self) -> Fut<Metadata>;
    fn broker_config(&self, broker_id: i32) -> Fut<ConfigMap>;
    /// Конфиги пакета топиков; `None` у топика — не прочитан.
    fn topic_configs(&self, topics: Vec<String>) -> Fut<Vec<(String, Option<ConfigMap>)>>;
    /// Идентификаторы групп по имени.
    fn list_groups(&self) -> Fut<Vec<String>>;
    fn describe_groups(&self, ids: Vec<String>) -> Fut<Vec<GroupRead>>;
    /// Суммарный лаг; `None` — не прочитан.
    fn groups_lag(&self, ids: Vec<String>) -> Fut<Vec<(String, Option<i64>)>>;
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_topics: usize,
    pub max_groups: usize,
    pub topic_batch: usize,
    pub group_batch: usize,
    pub parallel: usize,
    pub deadline: Duration,
}

pub const LIMITS: Limits = Limits {
    max_topics: 2000,
    max_groups: 500,
    topic_batch: 100,
    group_batch: 50,
    parallel: 4,
    deadline: Duration::from_secs(45),
};

/// Задачи под общим лимитом параллельности и дедлайном. `None` — не успела к дедлайну.
async fn limited<T: Send + 'static>(
    jobs: Vec<Fut<T>>,
    parallel: usize,
    deadline: Instant,
) -> Vec<Option<AppResult<T>>> {
    let n = jobs.len();
    let sem = Arc::new(Semaphore::new(parallel.max(1)));
    let mut set = JoinSet::new();
    for (i, job) in jobs.into_iter().enumerate() {
        let sem = sem.clone();
        set.spawn(async move {
            let _permit = sem.acquire_owned().await;
            (i, tokio::time::timeout_at(deadline, job).await.ok())
        });
    }
    let mut out: Vec<Option<AppResult<T>>> = (0..n).map(|_| None).collect();
    while let Some(joined) = set.join_next().await {
        if let Ok((i, r)) = joined {
            out[i] = r;
        }
    }
    out
}

fn timeout_error() -> crate::error::AppError {
    app_err!(
        ErrorCode::Timeout,
        "errors.health.checkTimeout",
        "The cluster check did not get metadata in time"
    )
}

pub async fn collect(src: Arc<dyn Source>, limits: Limits) -> AppResult<ClusterSnapshot> {
    let deadline = Instant::now() + limits.deadline;
    let md = tokio::time::timeout_at(deadline, src.metadata())
        .await
        .map_err(|_| timeout_error())??;
    let mut snap = ClusterSnapshot {
        cluster_id: md.cluster_id,
        controller_id: md.controller_id,
        brokers: md.brokers,
        topics: md.topics,
        ..Default::default()
    };
    let mut c = Coverage::default();

    // Конфиги брокеров.
    let ids: Vec<i32> = snap.brokers.iter().map(|b| b.id).collect();
    let jobs = ids.iter().map(|&id| src.broker_config(id)).collect();
    for (id, r) in ids
        .into_iter()
        .zip(limited(jobs, limits.parallel, deadline).await)
    {
        match r {
            Some(Ok(cfg)) => {
                snap.broker_configs.insert(id, cfg);
            }
            Some(Err(_)) => c.broker_config_errors += 1,
            None => {
                c.broker_config_errors += 1;
                c.timed_out = true;
            }
        }
    }

    // Конфиги топиков: сначала внутренние, затем по имени, не больше max_topics.
    let mut names: Vec<(bool, String)> = snap
        .topics
        .iter()
        .map(|t| (!t.internal(), t.name.clone()))
        .collect();
    names.sort();
    let names: Vec<String> = names
        .into_iter()
        .take(limits.max_topics)
        .map(|(_, n)| n)
        .collect();
    c.topics_total = snap.topics.len();
    let batches: Vec<Vec<String>> = names
        .chunks(limits.topic_batch.max(1))
        .map(<[String]>::to_vec)
        .collect();
    let jobs = batches
        .iter()
        .map(|b| src.topic_configs(b.clone()))
        .collect();
    for (batch, r) in batches
        .iter()
        .zip(limited(jobs, limits.parallel, deadline).await)
    {
        match r {
            Some(Ok(items)) => {
                for (name, cfg) in items {
                    match cfg {
                        Some(cfg) => {
                            snap.topic_configs.insert(name, cfg);
                        }
                        None => c.topic_config_errors += 1,
                    }
                }
            }
            Some(Err(_)) => c.topic_config_errors += batch.len(),
            None => c.timed_out = true,
        }
    }
    c.topics_checked = snap.topic_configs.len();

    // Группы: список, описание и лаг не больше max_groups.
    match tokio::time::timeout_at(deadline, src.list_groups()).await {
        Ok(Ok(all)) => {
            c.groups_total = all.len();
            let ids: Vec<String> = all.into_iter().take(limits.max_groups).collect();
            let batches: Vec<Vec<String>> = ids
                .chunks(limits.group_batch.max(1))
                .map(<[String]>::to_vec)
                .collect();
            let jobs = batches
                .iter()
                .map(|b| src.describe_groups(b.clone()))
                .collect();
            for (batch, r) in batches
                .iter()
                .zip(limited(jobs, limits.parallel, deadline).await)
            {
                match r {
                    Some(Ok(items)) => {
                        for (_, g) in items {
                            match g {
                                Some(g) => snap.groups.push(g),
                                None => c.group_errors += 1,
                            }
                        }
                    }
                    Some(Err(_)) => c.group_errors += batch.len(),
                    None => c.timed_out = true,
                }
            }
            let described: Vec<String> = snap.groups.iter().map(|g| g.group_id.clone()).collect();
            let batches: Vec<Vec<String>> = described
                .chunks(limits.group_batch.max(1))
                .map(<[String]>::to_vec)
                .collect();
            let jobs = batches.iter().map(|b| src.groups_lag(b.clone())).collect();
            let mut lags = std::collections::HashMap::new();
            for r in limited(jobs, limits.parallel, deadline).await {
                match r {
                    Some(Ok(items)) => lags.extend(items),
                    Some(Err(_)) => {}
                    None => c.timed_out = true,
                }
            }
            for g in &mut snap.groups {
                g.total_lag = lags.get(&g.group_id).copied().flatten();
            }
        }
        Ok(Err(_)) => c.group_errors += 1,
        Err(_) => c.timed_out = true,
    }
    snap.groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
    c.groups_checked = snap.groups.len();
    snap.coverage = c;
    Ok(snap)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use super::*;
    use crate::health::rules::fixtures::{brokers, topic};
    use crate::health::snapshot::ConfigValue;
    use crate::kafka::metadata::TopicMeta;
    use crate::sync::MutexExt;

    #[derive(Default)]
    struct Fake {
        topics: usize,
        groups: usize,
        /// Пакеты топиков, которые «зависают» дольше дедлайна.
        slow_topic_batches: Vec<usize>,
        failing_broker: Option<i32>,
        calls: Mutex<Vec<String>>,
        in_flight: Arc<Mutex<(usize, usize)>>,
    }

    fn cfg(v: &str) -> ConfigMap {
        BTreeMap::from([(
            "min.insync.replicas".to_string(),
            ConfigValue {
                value: Some(v.into()),
                source: "Default".into(),
            },
        )])
    }

    /// Счётчик одновременно выполняемых пакетов: текущее и максимум.
    struct Track(Arc<Mutex<(usize, usize)>>);

    impl Track {
        fn start(m: Arc<Mutex<(usize, usize)>>) -> Self {
            let mut g = m.lock_safe();
            g.0 += 1;
            g.1 = g.1.max(g.0);
            drop(g);
            Self(m)
        }
    }

    impl Drop for Track {
        fn drop(&mut self) {
            self.0.lock_safe().0 -= 1;
        }
    }

    impl Source for Fake {
        fn metadata(&self) -> Fut<Metadata> {
            let mut topics: Vec<TopicMeta> = (0..self.topics)
                .map(|i| topic(&format!("t{i:04}"), &[(1, &[1], &[1])]))
                .collect();
            topics.push(topic("__consumer_offsets", &[(1, &[1], &[1])]));
            Box::pin(async move {
                Ok(Metadata {
                    cluster_id: Some("c".into()),
                    controller_id: Some(1),
                    brokers: brokers(3),
                    topics,
                })
            })
        }
        fn broker_config(&self, broker_id: i32) -> Fut<ConfigMap> {
            let fail = self.failing_broker == Some(broker_id);
            Box::pin(async move {
                if fail {
                    Err(app_err!(
                        ErrorCode::Timeout,
                        "errors.health.checkTimeout",
                        "x"
                    ))
                } else {
                    Ok(cfg("2"))
                }
            })
        }
        fn topic_configs(&self, topics: Vec<String>) -> Fut<Vec<(String, Option<ConfigMap>)>> {
            let mut calls = self.calls.lock_safe();
            let batch = calls.iter().filter(|c| c.starts_with("topics")).count();
            calls.push(format!("topics:{}:{}", topics.len(), topics[0]));
            drop(calls);
            let slow = self.slow_topic_batches.contains(&batch);
            let in_flight = self.in_flight.clone();
            Box::pin(async move {
                let _track = Track::start(in_flight);
                tokio::time::sleep(Duration::from_secs(if slow { 120 } else { 1 })).await;
                Ok(topics
                    .into_iter()
                    .map(|t| {
                        let ok = t != "t0003";
                        (t, ok.then(|| cfg("2")))
                    })
                    .collect())
            })
        }
        fn list_groups(&self) -> Fut<Vec<String>> {
            let ids = (0..self.groups).map(|i| format!("g{i:04}")).collect();
            Box::pin(async move { Ok(ids) })
        }
        fn describe_groups(&self, ids: Vec<String>) -> Fut<Vec<GroupRead>> {
            Box::pin(async move {
                Ok(ids
                    .into_iter()
                    .map(|id| {
                        let g = (id != "g0001").then(|| GroupSnapshot {
                            group_id: id.clone(),
                            state: "Empty".into(),
                            group_type: "classic".into(),
                            member_partitions: vec![],
                            total_lag: None,
                        });
                        (id, g)
                    })
                    .collect())
            })
        }
        fn groups_lag(&self, ids: Vec<String>) -> Fut<Vec<(String, Option<i64>)>> {
            Box::pin(async move { Ok(ids.into_iter().map(|id| (id, Some(7))).collect()) })
        }
    }

    fn small() -> Limits {
        Limits {
            max_topics: 10,
            max_groups: 3,
            topic_batch: 4,
            group_batch: 2,
            parallel: 2,
            deadline: Duration::from_secs(45),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn collects_within_limits() {
        let fake = Arc::new(Fake {
            topics: 20,
            groups: 5,
            ..Default::default()
        });
        let s = collect(fake.clone(), small()).await.unwrap();
        let c = &s.coverage;
        assert_eq!(
            (c.topics_total, c.topics_checked, c.topic_config_errors),
            (21, 9, 1)
        );
        // Внутренний топик — первым; пакеты по 4.
        let calls = fake.calls.lock_safe().clone();
        assert_eq!(calls[0], "topics:4:__consumer_offsets");
        assert_eq!(calls.len(), 3);
        assert!(s.topic_configs.contains_key("__consumer_offsets"));
        // Параллельно не больше двух пакетов.
        assert_eq!(fake.in_flight.lock_safe().1, 2);
        assert_eq!(
            (c.groups_total, c.groups_checked, c.group_errors),
            (5, 2, 1)
        );
        assert_eq!(s.groups[0].total_lag, Some(7));
        assert_eq!(s.broker_configs.len(), 3);
        assert!(!c.timed_out);
        assert!(c.partial());
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_marks_check_incomplete() {
        let fake = Arc::new(Fake {
            topics: 8,
            slow_topic_batches: vec![1],
            failing_broker: Some(2),
            ..Default::default()
        });
        let started = Instant::now();
        let s = collect(fake, small()).await.unwrap();
        assert!(started.elapsed() <= Duration::from_secs(46));
        let c = &s.coverage;
        assert!(c.timed_out);
        assert_eq!(c.broker_config_errors, 1);
        // Пакет 0 (4 топика, внутренний первым) прочитан, пакет 1 — нет, пакет 2 (1 топик) — да.
        assert_eq!(c.topics_checked, 5);
    }
}
