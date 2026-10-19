//! Запуск проверки (спец. G2 §3): одна проверка на подключение одновременно — второй вызов
//! ждёт идущую и получает её результат. После проверки и любого изменения замечаний —
//! уведомление интерфейсу (`health://changed`).

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::watch;

use super::collect::{LIMITS, Limits, Source, collect};
use super::kafka_source::KafkaSource;
use super::rules;
use super::snapshot::PreviousLag;
use super::store::{CheckRun, HealthStore, Scope, Trigger};
use crate::error::{AppError, AppResult, ErrorCode};
use crate::state::AppState;
use crate::storage::connections::Environment;
use crate::sync::{MutexExt, RwLockExt};

pub type Notify = Arc<dyn Fn(&str) + Send + Sync>;

type Outcome = Option<Result<Arc<CheckRun>, AppError>>;

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub struct HealthService {
    pub store: HealthStore,
    running: Mutex<HashMap<String, watch::Receiver<Outcome>>>,
    notify: RwLock<Option<Notify>>,
}

impl HealthService {
    pub fn open(data_dir: &Path) -> Self {
        Self {
            store: HealthStore::open(data_dir),
            running: Mutex::new(HashMap::new()),
            notify: RwLock::new(None),
        }
    }

    pub fn set_notify(&self, f: Notify) {
        *self.notify.write_safe() = Some(f);
    }

    /// Состояние подключения изменилось (проверка, скрытие, замечания).
    pub fn changed(&self, connection_id: &str) {
        let f = self.notify.read_safe().clone();
        if let Some(f) = f {
            f(connection_id);
        }
    }

    /// Новая проверка подключения на Kafka (или ожидание уже идущей).
    pub async fn run(
        self: &Arc<Self>,
        state: &AppState,
        connection_id: &str,
        trigger: Trigger,
    ) -> AppResult<Arc<CheckRun>> {
        let env = state.profile(connection_id)?.environment;
        let src = Arc::new(KafkaSource::new(state.clone(), connection_id));
        self.run_source(connection_id, env, trigger, src, LIMITS)
            .await
    }

    /// Последняя проверка, если ей меньше `max_age_ms`, иначе новая. `true` — использована прошлая.
    pub async fn latest_or_run(
        self: &Arc<Self>,
        state: &AppState,
        connection_id: &str,
        trigger: Trigger,
        max_age_ms: i64,
    ) -> AppResult<(Arc<CheckRun>, bool)> {
        let now = now_ms();
        if let Some(last) = self.store.load(connection_id, now)?.last_check
            && now - last.started_at_ms < max_age_ms
        {
            return Ok((Arc::new(last), true));
        }
        Ok((self.run(state, connection_id, trigger).await?, false))
    }

    /// Ядро запуска на любом источнике (тесты — на фейке).
    pub async fn run_source(
        self: &Arc<Self>,
        connection_id: &str,
        env: Environment,
        trigger: Trigger,
        src: Arc<dyn Source>,
        limits: Limits,
    ) -> AppResult<Arc<CheckRun>> {
        let rx = {
            let mut running = self.running.lock_safe();
            match running.get(connection_id) {
                Some(rx) => rx.clone(),
                None => {
                    let (tx, rx) = watch::channel(None);
                    running.insert(connection_id.to_string(), rx.clone());
                    let this = self.clone();
                    let id = connection_id.to_string();
                    // Проверка доводится до конца, даже если вызвавший перестал ждать.
                    tauri::async_runtime::spawn(async move {
                        let r = this
                            .check(&id, env, trigger, src, limits)
                            .await
                            .map(Arc::new);
                        this.running.lock_safe().remove(&id);
                        if r.is_ok() {
                            this.changed(&id);
                        }
                        let _ = tx.send(Some(r));
                    });
                    rx
                }
            }
        };
        wait(rx).await
    }

    async fn check(
        &self,
        connection_id: &str,
        env: Environment,
        trigger: Trigger,
        src: Arc<dyn Source>,
        limits: Limits,
    ) -> AppResult<CheckRun> {
        let started_at = now_ms();
        let previous = self.store.load(connection_id, started_at)?.last_check;
        let mut snap = collect(src, limits).await?;
        snap.previous = previous.map(|p| PreviousLag {
            age_ms: started_at - p.started_at_ms,
            lag: p.group_lag,
        });
        let findings = rules::evaluate(&snap, env);
        let c = &snap.coverage;
        let run = CheckRun {
            run_id: uuid::Uuid::new_v4().to_string(),
            started_at_ms: started_at,
            duration_ms: now_ms() - started_at,
            trigger,
            environment: env,
            incomplete: c.partial(),
            scope: Scope {
                brokers: snap.brokers.len(),
                topics_checked: c.topics_checked,
                topics_total: c.topics_total,
                groups_checked: c.groups_checked,
                groups_total: c.groups_total,
            },
            findings,
            group_lag: snap
                .groups
                .iter()
                .filter_map(|g| g.total_lag.map(|l| (g.group_id.clone(), l)))
                .collect::<BTreeMap<_, _>>(),
        };
        self.store
            .save_check(connection_id, run.clone(), now_ms())?;
        Ok(run)
    }
}

async fn wait(mut rx: watch::Receiver<Outcome>) -> AppResult<Arc<CheckRun>> {
    loop {
        if let Some(r) = rx.borrow().clone() {
            return r;
        }
        if rx.changed().await.is_err() {
            return Err(app_err!(
                ErrorCode::Internal,
                "errors.app.taskPanicked",
                "Background task failed: {{error}}",
                error = "health check"
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::health::collect::{Fut, GroupRead, Metadata};
    use crate::health::rules::fixtures::{brokers, topic};
    use crate::health::snapshot::ConfigMap;

    #[derive(Default)]
    struct Counting {
        metadata_calls: AtomicUsize,
        lag: i64,
    }

    impl Source for Counting {
        fn metadata(&self) -> Fut<Metadata> {
            self.metadata_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(Metadata {
                    cluster_id: None,
                    controller_id: Some(1),
                    brokers: brokers(1),
                    topics: vec![topic("t", &[(1, &[1], &[1])])],
                })
            })
        }
        fn broker_config(&self, _: i32) -> Fut<ConfigMap> {
            Box::pin(async { Ok(ConfigMap::new()) })
        }
        fn topic_configs(&self, t: Vec<String>) -> Fut<Vec<(String, Option<ConfigMap>)>> {
            Box::pin(
                async move { Ok(t.into_iter().map(|n| (n, Some(ConfigMap::new()))).collect()) },
            )
        }
        fn list_groups(&self) -> Fut<Vec<String>> {
            Box::pin(async { Ok(vec!["g".to_string()]) })
        }
        fn describe_groups(&self, ids: Vec<String>) -> Fut<Vec<GroupRead>> {
            Box::pin(async move {
                Ok(ids
                    .into_iter()
                    .map(|id| {
                        let g = crate::health::snapshot::GroupSnapshot {
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
        fn groups_lag(&self, ids: Vec<String>) -> Fut<Vec<(String, Option<i64>)>> {
            let lag = self.lag;
            Box::pin(async move { Ok(ids.into_iter().map(|id| (id, Some(lag))).collect()) })
        }
    }

    #[tokio::test]
    async fn concurrent_callers_share_one_check() {
        let dir = tempfile::tempdir().unwrap();
        let svc = Arc::new(HealthService::open(dir.path()));
        let notified = Arc::new(AtomicUsize::new(0));
        let n = notified.clone();
        svc.set_notify(Arc::new(move |_| {
            n.fetch_add(1, Ordering::SeqCst);
        }));
        let src = Arc::new(Counting {
            lag: 3,
            ..Default::default()
        });
        let (a, b) = tokio::join!(
            svc.run_source("c1", Environment::Prod, Trigger::Ui, src.clone(), LIMITS),
            svc.run_source(
                "c1",
                Environment::Prod,
                Trigger::Mcp { client: None },
                src.clone(),
                LIMITS
            ),
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(a.run_id, b.run_id);
        assert_eq!(src.metadata_calls.load(Ordering::SeqCst), 1);
        assert_eq!(notified.load(Ordering::SeqCst), 1);
        // Один брокер на prod и брошенная группа с лагом.
        let ids: Vec<_> = a.findings.iter().map(|f| f.rule_id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "cluster.singleBroker",
                "topic.replicationFactorOne",
                "group.abandoned"
            ]
        );
        assert_eq!(a.group_lag.get("g"), Some(&3));
        assert_eq!(a.scope.brokers, 1);
        // Сохранена в файл; следующая проверка — отдельная.
        let saved = svc.store.load("c1", now_ms()).unwrap().last_check.unwrap();
        assert_eq!(saved.run_id, a.run_id);
        let c = svc
            .run_source("c1", Environment::Prod, Trigger::Ui, src.clone(), LIMITS)
            .await
            .unwrap();
        assert_ne!(c.run_id, a.run_id);
        assert_eq!(src.metadata_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn previous_check_feeds_lag_growth() {
        let dir = tempfile::tempdir().unwrap();
        let svc = Arc::new(HealthService::open(dir.path()));
        let first = svc
            .run_source(
                "c1",
                Environment::Prod,
                Trigger::Ui,
                Arc::new(Counting {
                    lag: 10,
                    ..Default::default()
                }),
                LIMITS,
            )
            .await
            .unwrap();
        // Прошлая проверка — 10 минут назад.
        let mut old = (*first).clone();
        old.started_at_ms -= 600_000;
        svc.store.save_check("c1", old, now_ms()).unwrap();
        let second = svc
            .run_source(
                "c1",
                Environment::Prod,
                Trigger::Ui,
                Arc::new(Counting {
                    lag: 5000,
                    ..Default::default()
                }),
                LIMITS,
            )
            .await
            .unwrap();
        let f = second
            .findings
            .iter()
            .find(|f| f.rule_id == "group.lagGrowing")
            .unwrap();
        assert_eq!(f.params["previousLag"], 10);
        assert_eq!(f.params["minutesAgo"], 10);
    }
}
