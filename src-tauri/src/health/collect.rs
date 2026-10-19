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

/// Лаг группы для снимка: `Ok(None)` — прочитан, но коммитов нет; `Err(())` — не прочитан.
pub type LagRead = (String, Result<Option<i64>, ()>);

/// Чтение кластера для снимка. Реализация на Kafka — `kafka_source.rs`, в тестах — фейк.
pub trait Source: Send + Sync + 'static {
    fn metadata(&self) -> Fut<Metadata>;
    fn broker_config(&self, broker_id: i32) -> Fut<ConfigMap>;
    /// Конфиги пакета топиков; `None` у топика — не прочитан.
    fn topic_configs(&self, topics: Vec<String>) -> Fut<Vec<(String, Option<ConfigMap>)>>;
    /// Идентификаторы групп по имени и число частичных ошибок ListGroups: часть координаторов
    /// не ответила, список может быть неполным.
    fn list_groups(&self) -> Fut<(Vec<String>, usize)>;
    fn describe_groups(&self, ids: Vec<String>) -> Fut<Vec<GroupRead>>;
    /// Суммарный лаг по группам; `Err(())` у группы — лаг не прочитан (это не то же, что «нет коммитов»).
    fn groups_lag(&self, ids: Vec<String>) -> Fut<Vec<LagRead>>;
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

/// Задачи под общим лимитом параллельности и дедлайном. `None` — не успела к дедлайну: ни ожидание
/// места, ни старт задачи не выходят за дедлайн, после него вызов Kafka уже не начинается.
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
            let Ok(Ok(_permit)) = tokio::time::timeout_at(deadline, sem.acquire_owned()).await
            else {
                return (i, None);
            };
            // `timeout_at` опрашивает задачу хотя бы раз, поэтому дедлайн проверяется до него.
            if Instant::now() >= deadline {
                return (i, None);
            }
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

    // Группы: список, описание и лаг не больше max_groups. К дедлайну не успели — группы пропущены.
    if Instant::now() >= deadline {
        c.timed_out = true;
    } else {
        match tokio::time::timeout_at(deadline, src.list_groups()).await {
            Ok(Ok((all, list_errors))) => {
                // Часть координаторов не ответила: список может быть неполным.
                c.group_errors += list_errors;
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
                let described: Vec<String> =
                    snap.groups.iter().map(|g| g.group_id.clone()).collect();
                let batches: Vec<Vec<String>> = described
                    .chunks(limits.group_batch.max(1))
                    .map(<[String]>::to_vec)
                    .collect();
                let jobs = batches.iter().map(|b| src.groups_lag(b.clone())).collect();
                let mut lags = std::collections::HashMap::new();
                for (batch, r) in batches
                    .iter()
                    .zip(limited(jobs, limits.parallel, deadline).await)
                {
                    match r {
                        Some(Ok(items)) => {
                            for (id, lag) in items {
                                match lag {
                                    // `Ok(None)` — коммитов нет, это не ошибка.
                                    Ok(l) => {
                                        lags.insert(id, l);
                                    }
                                    Err(()) => c.lag_errors += 1,
                                }
                            }
                        }
                        Some(Err(_)) => c.lag_errors += batch.len(),
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

    /// Как фейк отвечает на `groups_lag`.
    #[derive(Default, Clone, Copy)]
    enum LagMode {
        /// Лаг 7 у каждой группы.
        #[default]
        Value,
        /// Прочитано, коммитов нет.
        NoCommits,
        /// Ошибка у одной группы по имени.
        GroupError(&'static str),
        /// Ошибка всего пакета.
        BatchError,
    }

    #[derive(Default)]
    struct Fake {
        topics: usize,
        groups: usize,
        /// Пакеты топиков, которые «зависают» дольше дедлайна.
        slow_topic_batches: Vec<usize>,
        failing_broker: Option<i32>,
        lag: LagMode,
        /// Частичные ошибки ListGroups.
        list_errors: usize,
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
        fn list_groups(&self) -> Fut<(Vec<String>, usize)> {
            let ids = (0..self.groups).map(|i| format!("g{i:04}")).collect();
            let errors = self.list_errors;
            Box::pin(async move { Ok((ids, errors)) })
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
        fn groups_lag(&self, ids: Vec<String>) -> Fut<Vec<LagRead>> {
            let mode = self.lag;
            Box::pin(async move {
                match mode {
                    LagMode::BatchError => Err(app_err!(
                        ErrorCode::Timeout,
                        "errors.health.checkTimeout",
                        "x"
                    )),
                    _ => Ok(ids
                        .into_iter()
                        .map(|id| {
                            let lag = match mode {
                                LagMode::NoCommits => Ok(None),
                                LagMode::GroupError(bad) if bad == id => Err(()),
                                _ => Ok(Some(7)),
                            };
                            (id, lag)
                        })
                        .collect()),
                }
            })
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

    #[tokio::test(start_paused = true)]
    async fn lag_read_errors_make_check_partial() {
        // Ошибка лага у одной группы: описание без ошибок, но проверка неполная.
        let fake = Arc::new(Fake {
            groups: 1,
            lag: LagMode::GroupError("g0000"),
            ..Default::default()
        });
        let s = collect(fake, small()).await.unwrap();
        let c = &s.coverage;
        assert_eq!((c.group_errors, c.lag_errors), (0, 1));
        assert!(c.partial());
        assert_eq!(s.groups[0].total_lag, None);

        // Ошибка всего пакета: g0001 не описана, в пакете остались g0000 и g0002.
        let fake = Arc::new(Fake {
            groups: 3,
            lag: LagMode::BatchError,
            ..Default::default()
        });
        let s = collect(fake, small()).await.unwrap();
        assert_eq!(s.coverage.lag_errors, 2);
        assert!(s.coverage.partial());
        assert!(s.groups.iter().all(|g| g.total_lag.is_none()));
    }

    #[tokio::test(start_paused = true)]
    async fn group_without_commits_is_not_a_lag_error() {
        let fake = Arc::new(Fake {
            groups: 1,
            lag: LagMode::NoCommits,
            ..Default::default()
        });
        let s = collect(fake, small()).await.unwrap();
        assert_eq!(s.coverage.lag_errors, 0);
        assert!(!s.coverage.partial());
        assert_eq!(s.groups[0].total_lag, None);
    }

    #[tokio::test(start_paused = true)]
    async fn list_groups_partial_errors_make_check_partial() {
        let fake = Arc::new(Fake {
            groups: 1,
            list_errors: 2,
            ..Default::default()
        });
        let s = collect(fake, small()).await.unwrap();
        let c = &s.coverage;
        assert_eq!(c.group_errors, 2);
        assert!(c.partial());
        // Перечисленные группы всё равно описаны.
        assert_eq!((c.groups_total, c.groups_checked), (1, 1));
        assert_eq!(s.groups[0].total_lag, Some(7));
    }

    /// Источник для проверки дедлайна: пишет момент ПЕРВОГО опроса каждого вызова (а не вызова
    /// метода): именно первый опрос запускает блокирующий вызов Kafka.
    struct Probe {
        origin: Instant,
        polled: Arc<Mutex<Vec<(&'static str, Duration)>>>,
        in_flight: Arc<Mutex<(usize, usize)>>,
        topics: usize,
        groups: usize,
        topic_sleep: Duration,
        describe_sleep: Duration,
    }

    impl Probe {
        fn new(topics: usize, groups: usize, topic_sleep: u64, describe_sleep: u64) -> Self {
            Self {
                origin: Instant::now(),
                polled: Default::default(),
                in_flight: Default::default(),
                topics,
                groups,
                topic_sleep: Duration::from_secs(topic_sleep),
                describe_sleep: Duration::from_secs(describe_sleep),
            }
        }

        /// Запись первого опроса; вызывается внутри `async`-блока.
        fn marker(&self, what: &'static str) -> impl FnOnce() + Send + 'static {
            let (polled, origin) = (self.polled.clone(), self.origin);
            move || polled.lock_safe().push((what, origin.elapsed()))
        }

        fn count(&self, what: &str) -> usize {
            self.polled
                .lock_safe()
                .iter()
                .filter(|(w, _)| *w == what)
                .count()
        }
    }

    impl Source for Probe {
        fn metadata(&self) -> Fut<Metadata> {
            let mark = self.marker("metadata");
            let topics: Vec<TopicMeta> = (0..self.topics)
                .map(|i| topic(&format!("t{i:04}"), &[(1, &[1], &[1])]))
                .collect();
            Box::pin(async move {
                mark();
                Ok(Metadata {
                    cluster_id: None,
                    controller_id: Some(1),
                    brokers: brokers(2),
                    topics,
                })
            })
        }
        fn broker_config(&self, _: i32) -> Fut<ConfigMap> {
            let mark = self.marker("broker");
            Box::pin(async move {
                mark();
                Ok(cfg("2"))
            })
        }
        fn topic_configs(&self, topics: Vec<String>) -> Fut<Vec<(String, Option<ConfigMap>)>> {
            let (mark, in_flight, sleep) = (
                self.marker("topics"),
                self.in_flight.clone(),
                self.topic_sleep,
            );
            Box::pin(async move {
                mark();
                let _track = Track::start(in_flight);
                tokio::time::sleep(sleep).await;
                Ok(topics.into_iter().map(|t| (t, Some(cfg("2")))).collect())
            })
        }
        fn list_groups(&self) -> Fut<(Vec<String>, usize)> {
            let mark = self.marker("list");
            let ids = (0..self.groups).map(|i| format!("g{i:04}")).collect();
            Box::pin(async move {
                mark();
                Ok((ids, 0))
            })
        }
        fn describe_groups(&self, ids: Vec<String>) -> Fut<Vec<GroupRead>> {
            let (mark, in_flight, sleep) = (
                self.marker("describe"),
                self.in_flight.clone(),
                self.describe_sleep,
            );
            Box::pin(async move {
                mark();
                let _track = Track::start(in_flight);
                tokio::time::sleep(sleep).await;
                Ok(ids
                    .into_iter()
                    .map(|id| {
                        let g = GroupSnapshot {
                            group_id: id.clone(),
                            state: "Empty".into(),
                            group_type: "classic".into(),
                            member_partitions: vec![],
                            total_lag: None,
                        };
                        (id, Some(g))
                    })
                    .collect())
            })
        }
        fn groups_lag(&self, ids: Vec<String>) -> Fut<Vec<LagRead>> {
            let mark = self.marker("lag");
            Box::pin(async move {
                mark();
                Ok(ids.into_iter().map(|id| (id, Ok(Some(1)))).collect())
            })
        }
    }

    fn tight() -> Limits {
        Limits {
            max_topics: 100,
            max_groups: 100,
            topic_batch: 2,
            group_batch: 2,
            parallel: 2,
            deadline: Duration::from_secs(10),
        }
    }

    /// После дедлайна ни один вызов не начинается (иначе блокирующий вызов Kafka остался бы
    /// висеть вне лимита параллельности), а группы пропускаются целиком.
    #[tokio::test(start_paused = true)]
    async fn no_call_starts_after_deadline() {
        // 21 топик по 2 в пакете — 11 пакетов; каждый занимает 4 с, мест два: стартуют в 0, 4 и 8 с.
        let probe = Arc::new(Probe::new(20, 4, 4, 0));
        let limits = tight();
        let s = collect(probe.clone(), limits).await.unwrap();
        assert!(s.coverage.timed_out);
        assert!(s.coverage.partial());
        assert!(
            probe
                .polled
                .lock_safe()
                .iter()
                .all(|(_, at)| *at < limits.deadline)
        );
        assert_eq!(probe.count("topics"), 6);
        assert_eq!(probe.in_flight.lock_safe().1, 2);
        // К дедлайну группы уже не успевают: ни список, ни описание, ни лаг не запрашиваются.
        assert_eq!(
            (
                probe.count("list"),
                probe.count("describe"),
                probe.count("lag")
            ),
            (0, 0, 0)
        );
        assert_eq!(s.coverage.groups_total, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_inside_group_stage_stops_remaining_batches() {
        // Описание пакета длится дольше дедлайна: 6 групп — 3 пакета, мест два, третий не стартует.
        let probe = Arc::new(Probe::new(0, 6, 0, 20));
        let limits = tight();
        let s = collect(probe.clone(), limits).await.unwrap();
        assert!(s.coverage.timed_out);
        assert!(
            probe
                .polled
                .lock_safe()
                .iter()
                .all(|(_, at)| *at < limits.deadline)
        );
        assert_eq!(probe.count("list"), 1);
        assert_eq!(probe.count("describe"), 2);
        assert_eq!(probe.count("lag"), 0);
        assert_eq!(probe.in_flight.lock_safe().1, 2);
        assert!(s.groups.is_empty());
    }
}
