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

/// Снимает проверку с учёта `running`, но только свою запись (по каналу): запись проверки,
/// зарегистрированной следом с тем же id, не трогается. Сбрасывается явно перед отправкой
/// результата, а при панике задачи — при раскрутке стека.
struct Unregister {
    service: Arc<HealthService>,
    id: String,
    rx: watch::Receiver<Outcome>,
}

impl Drop for Unregister {
    fn drop(&mut self) {
        let mut running = self.service.running.lock_safe();
        if running
            .get(&self.id)
            .is_some_and(|(_, rx)| rx.same_channel(&self.rx))
        {
            running.remove(&self.id);
        }
    }
}

pub struct HealthService {
    pub store: HealthStore,
    /// Идущие проверки: окружение, под которым считается строгость, и канал результата.
    running: Mutex<HashMap<String, (Environment, watch::Receiver<Outcome>)>>,
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
        // Поколение — до проверки подключения, как в `commands::health::change`: подключение,
        // удалённое в промежутке, своего файла обратно не получит.
        let generation = self.generation(connection_id);
        let env = state.profile(connection_id)?.environment;
        let src = Arc::new(KafkaSource::new(state.clone(), connection_id));
        self.run_source(connection_id, generation, env, trigger, src, LIMITS)
            .await
    }

    /// Последняя проверка, если ей меньше `max_age_ms` и она сделана в текущем окружении
    /// подключения, иначе новая. `true` — использована прошлая.
    pub async fn latest_or_run(
        self: &Arc<Self>,
        state: &AppState,
        connection_id: &str,
        trigger: Trigger,
        max_age_ms: i64,
    ) -> AppResult<(Arc<CheckRun>, bool)> {
        let now = now_ms();
        let env = state.profile(connection_id)?.environment;
        let (this, id) = (self.clone(), connection_id.to_string());
        let last = blocking(move || Ok(this.store.load(&id, now)?.last_check)).await?;
        // Окружение подключения сменили (например, dev → prod): у прошлой проверки другая строгость.
        if let Some(last) = last
            && last.environment == env
            && now - last.started_at_ms < max_age_ms
        {
            return Ok((Arc::new(last), true));
        }
        Ok((self.run(state, connection_id, trigger).await?, false))
    }

    /// Ядро запуска на любом источнике (тесты — на фейке). `generation` прочитано до проверки
    /// подключения: результат сохраняется, только если подключение с тех пор не удаляли.
    pub async fn run_source(
        self: &Arc<Self>,
        connection_id: &str,
        generation: u64,
        env: Environment,
        trigger: Trigger,
        src: Arc<dyn Source>,
        limits: Limits,
    ) -> AppResult<Arc<CheckRun>> {
        loop {
            let (rx, same_env) = {
                let mut running = self.running.lock_safe();
                match running.get(connection_id) {
                    Some((running_env, rx)) => (rx.clone(), *running_env == env),
                    None => {
                        let (tx, rx) = watch::channel(None);
                        running.insert(connection_id.to_string(), (env, rx.clone()));
                        let this = self.clone();
                        let id = connection_id.to_string();
                        let (trigger, src) = (trigger.clone(), src.clone());
                        let own = rx.clone();
                        // Проверка доводится до конца, даже если вызвавший перестал ждать.
                        tauri::async_runtime::spawn(async move {
                            let unregister = Unregister {
                                service: this.clone(),
                                id: id.clone(),
                                rx: own,
                            };
                            let r = this
                                .check(&id, generation, env, trigger, src, limits)
                                .await
                                .map(Arc::new);
                            // Запись снимается ровно один раз — до уведомления и отправки результата,
                            // чтобы разбуженный ждущий не увидел свою же завершённую проверку.
                            drop(unregister);
                            if r.is_ok() {
                                this.changed(&id);
                            }
                            let _ = tx.send(Some(r));
                        });
                        (rx, true)
                    }
                }
            };
            if same_env {
                return wait(rx).await;
            }
            // Идущая проверка считается под другим окружением (его сменили в форме подключения):
            // её строгость не годится — дождаться её и запустить свою.
            let _ = wait(rx).await;
        }
    }

    async fn check(
        self: &Arc<Self>,
        connection_id: &str,
        generation: u64,
        env: Environment,
        trigger: Trigger,
        src: Arc<dyn Source>,
        limits: Limits,
    ) -> AppResult<CheckRun> {
        let started_at = now_ms();
        // Файл читается и пишется на blocking-потоке: `write` держит замок поколений на время fsync.
        let (this, id) = (self.clone(), connection_id.to_string());
        let previous = blocking(move || Ok(this.store.load(&id, started_at)?.last_check)).await?;
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
        let (this, id, saved) = (self.clone(), connection_id.to_string(), run.clone());
        blocking(move || this.write(&id, generation, |s| s.save_check(&id, saved, now_ms())))
            .await?;
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
            svc.run_source(
                "c1",
                svc.generation("c1"),
                Environment::Prod,
                Trigger::Ui,
                src.clone(),
                LIMITS
            ),
            svc.run_source(
                "c1",
                svc.generation("c1"),
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
            .run_source(
                "c1",
                svc.generation("c1"),
                Environment::Prod,
                Trigger::Ui,
                src.clone(),
                LIMITS,
            )
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
        svc.run_source(
            "c1",
            svc.generation("c1"),
            Environment::Prod,
            Trigger::Ui,
            counting(),
            LIMITS,
        )
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
                svc.run_source(
                    "c1",
                    svc.generation("c1"),
                    Environment::Prod,
                    Trigger::Ui,
                    gated,
                    LIMITS,
                )
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
        svc.run_source(
            "c1",
            svc.generation("c1"),
            Environment::Prod,
            Trigger::Ui,
            counting(),
            LIMITS,
        )
        .await
        .unwrap();
        assert!(file.exists());
        assert!(svc.store.load("c1", now_ms()).unwrap().last_check.is_some());
    }

    #[tokio::test]
    async fn check_of_another_environment_waits_and_runs_its_own() {
        let dir = tempfile::tempdir().unwrap();
        let svc = Arc::new(HealthService::open(dir.path()));
        // Проверка на prod стоит у ворот; тем временем окружение подключения сменили на dev.
        let (entered, gate) = (
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        );
        let gated = Arc::new(Gated {
            inner: Counting::default(),
            entered: entered.clone(),
            gate: gate.clone(),
        });
        let prod = tokio::spawn({
            let svc = svc.clone();
            async move {
                svc.run_source(
                    "c1",
                    svc.generation("c1"),
                    Environment::Prod,
                    Trigger::Ui,
                    gated,
                    LIMITS,
                )
                .await
            }
        });
        entered.notified().await;
        let dev_src = Arc::new(Counting {
            lag: 1,
            ..Default::default()
        });
        let dev = tokio::spawn({
            let (svc, dev_src) = (svc.clone(), dev_src.clone());
            async move {
                svc.run_source(
                    "c1",
                    svc.generation("c1"),
                    Environment::Dev,
                    Trigger::Mcp { client: None },
                    dev_src,
                    LIMITS,
                )
                .await
            }
        });
        // Пока prod не закончилась, dev ждёт и свой источник не трогает.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(dev_src.metadata_calls.load(Ordering::SeqCst), 0);
        gate.notify_one();
        let prod = prod.await.unwrap().unwrap();
        let dev = dev.await.unwrap().unwrap();
        assert_ne!(prod.run_id, dev.run_id);
        assert_eq!(
            (prod.environment, dev.environment),
            (Environment::Prod, Environment::Dev)
        );
        assert_eq!(dev_src.metadata_calls.load(Ordering::SeqCst), 1);
        // В файле — последняя (dev) проверка.
        let saved = svc.store.load("c1", now_ms()).unwrap().last_check.unwrap();
        assert_eq!(saved.run_id, dev.run_id);
    }

    /// Источник, у которого `metadata` паникует: задача проверки падает, не отправив результат.
    struct Panicking;

    impl Source for Panicking {
        fn metadata(&self) -> Fut<Metadata> {
            Box::pin(async { panic!("metadata exploded") })
        }
        fn broker_config(&self, _: i32) -> Fut<ConfigMap> {
            Box::pin(async { unreachable!("metadata panics first") })
        }
        fn topic_configs(&self, _: Vec<String>) -> Fut<Vec<(String, Option<ConfigMap>)>> {
            Box::pin(async { unreachable!("metadata panics first") })
        }
        fn list_groups(&self) -> Fut<(Vec<String>, usize)> {
            Box::pin(async { unreachable!("metadata panics first") })
        }
        fn describe_groups(&self, _: Vec<String>) -> Fut<Vec<GroupRead>> {
            Box::pin(async { unreachable!("metadata panics first") })
        }
        fn groups_lag(&self, _: Vec<String>) -> Fut<Vec<LagRead>> {
            Box::pin(async { unreachable!("metadata panics first") })
        }
    }

    #[tokio::test]
    async fn panicking_check_does_not_trap_callers_of_another_environment() {
        let dir = tempfile::tempdir().unwrap();
        let svc = Arc::new(HealthService::open(dir.path()));
        let prod = svc
            .run_source(
                "c1",
                svc.generation("c1"),
                Environment::Prod,
                Trigger::Ui,
                Arc::new(Panicking),
                LIMITS,
            )
            .await;
        assert_eq!(prod.unwrap_err().key, "errors.app.taskPanicked");
        // Запись об упавшей проверке снята: вызов под другим окружением запускает свою проверку,
        // а не кружит по закрытому каналу (без исправления тест упирается в таймаут).
        let dev = tokio::time::timeout(
            Duration::from_secs(5),
            svc.run_source(
                "c1",
                svc.generation("c1"),
                Environment::Dev,
                Trigger::Ui,
                Arc::new(Counting {
                    lag: 1,
                    ..Default::default()
                }),
                LIMITS,
            ),
        )
        .await
        .expect("run_source must finish")
        .unwrap();
        assert_eq!(dev.environment, Environment::Dev);
        assert!(svc.running.lock_safe().is_empty());
    }

    #[tokio::test]
    async fn finished_check_does_not_unregister_the_next_one() {
        let dir = tempfile::tempdir().unwrap();
        let svc = Arc::new(HealthService::open(dir.path()));
        // Прошлая проверка упала: следующая под тем же окружением запускается как обычно.
        let _ = svc
            .run_source(
                "c1",
                svc.generation("c1"),
                Environment::Prod,
                Trigger::Ui,
                Arc::new(Panicking),
                LIMITS,
            )
            .await;
        svc.run_source(
            "c1",
            svc.generation("c1"),
            Environment::Prod,
            Trigger::Ui,
            Arc::new(Counting {
                lag: 1,
                ..Default::default()
            }),
            LIMITS,
        )
        .await
        .unwrap();
        // Проверка A стоит у ворот; пока она не закончилась, регистрируется чужая запись с тем же id.
        // Завершение A не должно снять её: последующий вызов присоединяется к ней, а не запускает новую.
        let (entered, gate) = (
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        );
        let gated = Arc::new(Gated {
            inner: Counting::default(),
            entered: entered.clone(),
            gate: gate.clone(),
        });
        let a = tokio::spawn({
            let svc = svc.clone();
            async move {
                svc.run_source(
                    "c1",
                    svc.generation("c1"),
                    Environment::Prod,
                    Trigger::Ui,
                    gated,
                    LIMITS,
                )
                .await
            }
        });
        entered.notified().await;
        let (foreign_tx, foreign_rx) = watch::channel(None);
        // Подменяем запись: так выглядит проверка, зарегистрированная между снятием записи A и концом её задачи.
        svc.running
            .lock_safe()
            .insert("c1".into(), (Environment::Dev, foreign_rx.clone()));
        gate.notify_one();
        a.await.unwrap().unwrap();
        // Чужая запись на месте: страж A снял только свою.
        let kept = svc
            .running
            .lock_safe()
            .get("c1")
            .map(|(e, rx)| (*e, rx.same_channel(&foreign_rx)));
        assert_eq!(kept, Some((Environment::Dev, true)));
        drop(foreign_tx);
        svc.running.lock_safe().remove("c1");
    }

    #[tokio::test]
    async fn generation_read_before_deletion_is_not_saved() {
        let dir = tempfile::tempdir().unwrap();
        let svc = Arc::new(HealthService::open(dir.path()));
        let file = dir.path().join("health").join("c1.json");
        // Поколение прочитано до проверки подключения, подключение удалили до старта проверки:
        // проверка пишет по этому поколению, а не по прочитанному позже.
        let generation = svc.generation("c1");
        svc.forget("c1").unwrap();
        let err = svc
            .run_source(
                "c1",
                generation,
                Environment::Prod,
                Trigger::Ui,
                Arc::new(Counting::default()),
                LIMITS,
            )
            .await
            .unwrap_err();
        assert_eq!(err.key, "errors.connections.notFound");
        assert!(!file.exists(), "результат удалённого подключения сохранён");
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
                svc.generation("c1"),
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
                svc.generation("c1"),
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
