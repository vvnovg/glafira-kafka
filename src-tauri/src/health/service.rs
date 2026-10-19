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
use super::store::{CheckRun, HealthStore, Scope, Trigger, connection_not_found};
use crate::commands::blocking;
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
    /// Поколение данных подключения: растёт при удалении (`forget`), нет записи — 0. Идущая проверка
    /// сохраняет результат, только если поколение с её начала не изменилось.
    generations: Mutex<HashMap<String, u64>>,
    notify: RwLock<Option<Notify>>,
}

impl HealthService {
    pub fn open(data_dir: &Path) -> Self {
        Self {
            store: HealthStore::open(data_dir),
            running: Mutex::new(HashMap::new()),
            generations: Mutex::new(HashMap::new()),
            notify: RwLock::new(None),
        }
    }

    pub fn set_notify(&self, f: Notify) {
        *self.notify.write_safe() = Some(f);
    }

    /// Подключение удалено: данные проверки стираются, идущая проверка своё не сохранит.
    pub fn forget(&self, connection_id: &str) -> AppResult<()> {
        let mut generations = self.generations.lock_safe();
        *generations.entry(connection_id.to_string()).or_insert(0) += 1;
        self.store.delete_connection(connection_id)
    }

    /// Поколение данных подключения: читается до проверки существования подключения, чтобы
    /// `write` отказал, если подключение удалили в промежутке.
    pub fn generation(&self, connection_id: &str) -> u64 {
        self.generations
            .lock_safe()
            .get(connection_id)
            .copied()
            .unwrap_or(0)
    }

    /// Единственный путь записи в `health/<id>.json`: под тем же замком, что и `forget`, и только
    /// если подключение не удаляли с момента чтения `generation` — иначе файл удалённого
    /// подключения появился бы заново.
    pub fn write<T>(
        &self,
        connection_id: &str,
        generation: u64,
        f: impl FnOnce(&HealthStore) -> AppResult<T>,
    ) -> AppResult<T> {
        let generations = self.generations.lock_safe();
        if generations.get(connection_id).copied().unwrap_or(0) != generation {
            return Err(connection_not_found(connection_id));
        }
        let out = f(&self.store);
        drop(generations);
        out
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
        let (this, id) = (self.clone(), connection_id.to_string());
        let last = blocking(move || Ok(this.store.load(&id, now)?.last_check)).await?;
        if let Some(last) = last
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
        let generation = self.generation(connection_id);
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
        // Подключение, удалённое за время проверки, не получит обратно свой файл.
        self.write(connection_id, generation, |s| {
            s.save_check(connection_id, run.clone(), now_ms())
        })?;
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
    use crate::health::collect::{Fut, GroupRead, LagRead, Metadata};
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
        fn list_groups(&self) -> Fut<(Vec<String>, usize)> {
            Box::pin(async { Ok((vec!["g".to_string()], 0)) })
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
        fn groups_lag(&self, ids: Vec<String>) -> Fut<Vec<LagRead>> {
            let lag = self.lag;
            Box::pin(async move { Ok(ids.into_iter().map(|id| (id, Ok(Some(lag)))).collect()) })
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

    /// `Counting`, у которого `metadata` ждёт разрешения; о входе в проверку сообщает `entered`.
    struct Gated {
        inner: Counting,
        entered: Arc<tokio::sync::Notify>,
        gate: Arc<tokio::sync::Notify>,
    }

    impl Source for Gated {
        fn metadata(&self) -> Fut<Metadata> {
            let (entered, gate) = (self.entered.clone(), self.gate.clone());
            let next = self.inner.metadata();
            Box::pin(async move {
                entered.notify_one();
                gate.notified().await;
                next.await
            })
        }
        fn broker_config(&self, id: i32) -> Fut<ConfigMap> {
            self.inner.broker_config(id)
        }
        fn topic_configs(&self, t: Vec<String>) -> Fut<Vec<(String, Option<ConfigMap>)>> {
            self.inner.topic_configs(t)
        }
        fn list_groups(&self) -> Fut<(Vec<String>, usize)> {
            self.inner.list_groups()
        }
        fn describe_groups(&self, ids: Vec<String>) -> Fut<Vec<GroupRead>> {
            self.inner.describe_groups(ids)
        }
        fn groups_lag(&self, ids: Vec<String>) -> Fut<Vec<LagRead>> {
            self.inner.groups_lag(ids)
        }
    }

    #[tokio::test]
    async fn check_of_deleted_connection_is_not_saved() {
        let dir = tempfile::tempdir().unwrap();
        let svc = Arc::new(HealthService::open(dir.path()));
        let file = dir.path().join("health").join("c1.json");
        let counting = || {
            Arc::new(Counting {
                lag: 1,
                ..Default::default()
            })
        };
        svc.run_source("c1", Environment::Prod, Trigger::Ui, counting(), LIMITS)
            .await
            .unwrap();
        assert!(file.exists());

        // Подключение удаляют, пока проверка ещё читает кластер.
        let (entered, gate) = (
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        );
        let gated = Arc::new(Gated {
            inner: Counting::default(),
            entered: entered.clone(),
            gate: gate.clone(),
        });
        let running = tokio::spawn({
            let svc = svc.clone();
            async move {
                svc.run_source("c1", Environment::Prod, Trigger::Ui, gated, LIMITS)
                    .await
            }
        });
        entered.notified().await;
        svc.forget("c1").unwrap();
        assert!(!file.exists());
        gate.notify_one();
        let err = running.await.unwrap().unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        assert_eq!(err.key, "errors.connections.notFound");
        assert!(!file.exists(), "результат удалённого подключения сохранён");

        // Подключение с тем же id, созданное заново, сохраняется как обычно.
        svc.run_source("c1", Environment::Prod, Trigger::Ui, counting(), LIMITS)
            .await
            .unwrap();
        assert!(file.exists());
        assert!(svc.store.load("c1", now_ms()).unwrap().last_check.is_some());
    }

    #[test]
    fn write_after_forget_is_refused_and_leaves_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let svc = HealthService::open(dir.path());
        let file = dir.path().join("health").join("c1.json");
        let ignore = |s: &HealthStore| s.set_ignored("c1", "k", true, now_ms());

        // Текущее поколение: запись выполняется.
        let generation = svc.generation("c1");
        svc.write("c1", generation, ignore).unwrap();
        assert!(file.exists());

        // Подключение удалено после чтения поколения: замыкание не вызывается, файла нет.
        svc.forget("c1").unwrap();
        assert!(!file.exists());
        let called = std::cell::Cell::new(false);
        let err = svc
            .write("c1", generation, |s| {
                called.set(true);
                ignore(s)
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        assert_eq!(err.key, "errors.connections.notFound");
        assert!(!called.get());
        assert!(!file.exists(), "запись удалённого подключения создала файл");

        // Поколение, прочитанное после удаления, пишет как обычно.
        let fresh = svc.generation("c1");
        assert_ne!(fresh, generation);
        svc.write("c1", fresh, ignore).unwrap();
        assert!(svc.store.load("c1", now_ms()).unwrap().is_ignored("k"));
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
