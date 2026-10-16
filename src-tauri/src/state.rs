use std::path::Path;
use std::sync::Arc;

use crate::browse::registry::SessionRegistry;
use crate::error::{AppError, AppResult, ErrorCode};
use crate::kafka::pool::{ClientPool, KafkaHandles};
use crate::secrets::SecretStore;
use crate::storage::connections::{ConnectionProfile, ConnectionStore};

#[derive(Clone)]
pub struct AppState {
    pub connections: Arc<ConnectionStore>,
    pub secrets: Arc<dyn SecretStore>,
    pub pool: Arc<ClientPool>,
    pub producers: Arc<crate::kafka::producer::ProducerPool>,
    pub sessions: Arc<SessionRegistry>,
    pub consumers: Arc<crate::consume::control::ConsumerControls>,
    pub history: Arc<crate::storage::history::HistoryStore>,
    pub templates: Arc<crate::storage::templates::TemplateStore>,
    pub policy: Arc<crate::policy::Policy>,
    pub license: Arc<crate::license::LicenseManager>,
}

impl AppState {
    pub fn new(data_dir: &Path, secrets: Arc<dyn SecretStore>) -> AppResult<Self> {
        Ok(Self {
            connections: Arc::new(ConnectionStore::open(data_dir)?),
            secrets,
            pool: Arc::new(ClientPool::default()),
            producers: Arc::new(Default::default()),
            sessions: Arc::new(SessionRegistry::default()),
            consumers: Arc::new(Default::default()),
            history: Arc::new(crate::storage::history::HistoryStore::open(data_dir)?),
            templates: Arc::new(crate::storage::templates::TemplateStore::open(data_dir)),
            policy: Arc::new(crate::policy::Policy::default()),
            license: Arc::new(crate::license::LicenseManager::open(
                data_dir,
                crate::license::LicenseManager::embedded_key(),
            )),
        })
    }

    /// Политика администратора (читается при запуске в lib.rs).
    pub fn with_policy(mut self, policy: crate::policy::Policy) -> Self {
        self.license.set_policy_key(policy.license_key.clone());
        self.policy = Arc::new(policy);
        self
    }

    pub fn profile(&self, id: &str) -> AppResult<ConnectionProfile> {
        self.connections.get(id)
    }

    /// Профиль подключения, если мутирующая операция на нём разрешена (см. `guard`).
    /// Не читает keychain и не ходит в сеть — можно вызывать на async-потоке.
    pub fn guarded(
        &self,
        connection_id: &str,
        confirm: Option<&str>,
    ) -> AppResult<ConnectionProfile> {
        let p = self.connections.get(connection_id)?;
        crate::guard::ensure_mutation_allowed(&p, confirm)?;
        Ok(p)
    }

    pub fn handles(&self, id: &str) -> AppResult<Arc<KafkaHandles>> {
        let profile = self.connections.get(id)?;
        self.pool.get(&profile, self.secrets.as_ref())
    }

    /// Выполняет операцию на pooled-handles подключения. При AUTH_FAILED/TLS_ERROR handles
    /// вытесняются из пула: librdkafka подавляет повтор одинаковой ошибки 30 с, а свежий клиент
    /// снова сообщит причину (и подхватит исправленные учётные данные).
    /// Блокирующий вызов — только из blocking-контекста.
    pub fn with_handles<T>(
        &self,
        id: &str,
        op: impl FnOnce(&KafkaHandles) -> AppResult<T>,
    ) -> AppResult<T> {
        let h = self.handles(id)?;
        let r = op(&h);
        if let Err(e) = &r {
            self.evict_if_client_broken(id, e);
        }
        // Последняя ссылка на вытесненные handles освобождается здесь, вне мьютекса пула.
        drop(h);
        r
    }

    /// Вытесняет handles подключения, если ошибка означает, что клиент с текущими
    /// учётными данными/TLS работать не будет. Возвращает true, если вытеснил.
    pub fn evict_if_client_broken(&self, id: &str, e: &AppError) -> bool {
        let broken = matches!(e.code, ErrorCode::AuthFailed | ErrorCode::TlsError);
        if broken {
            self.evict_connection(id);
        }
        broken
    }

    /// Удаляет подключение целиком. Само удаление и секрет — обязательные шаги; очистка
    /// истории и шаблонов best-effort: сбой логируется и не ломает уже выполненное удаление.
    pub fn delete_connection_full(&self, id: &str) -> AppResult<()> {
        // Живые browse-сессии удаляемого подключения больше не нужны — даже если удаление ниже упадёт.
        self.sessions.cancel_for(id);
        self.connections.delete(id)?;
        for field in crate::secrets::CONNECTION_SECRET_FIELDS {
            self.secrets
                .delete(&crate::secrets::secret_key(id, field))?;
        }
        self.evict_connection(id);
        if let Err(e) = self.history.delete_connection(id) {
            tracing::warn!("failed to delete history of connection {id}: {}", e.message);
        }
        if let Err(e) = self.templates.delete_for_connection(id) {
            tracing::warn!(
                "failed to delete templates of connection {id}: {}",
                e.message
            );
        }
        Ok(())
    }

    /// Вытесняет все клиенты подключения: admin/consumer-handles и producer'ы.
    pub fn evict_connection(&self, id: &str) {
        self.pool.evict(id);
        self.producers.evict_connection(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::sample_profile;

    #[test]
    fn handles_for_unknown_connection_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        assert_eq!(
            state.handles("nope").err().unwrap().code,
            ErrorCode::NotFound
        );
        state.connections.upsert(sample_profile("a")).unwrap();
        assert!(state.handles("a").is_ok());
    }

    #[test]
    fn auth_and_tls_errors_evict_pooled_handles() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        state.connections.upsert(sample_profile("a")).unwrap();
        let h1 = state.handles("a").unwrap();

        let r: AppResult<()> =
            state.with_handles("a", |_| Err(AppError::new(ErrorCode::Timeout, "t")));
        assert!(r.is_err());
        assert!(
            Arc::ptr_eq(&h1, &state.handles("a").unwrap()),
            "таймаут не вытесняет"
        );

        for code in [ErrorCode::AuthFailed, ErrorCode::TlsError] {
            let before = state.handles("a").unwrap();
            let r: AppResult<()> = state.with_handles("a", |_| Err(AppError::new(code, "x")));
            assert_eq!(r.unwrap_err().code, code);
            assert!(
                !Arc::ptr_eq(&before, &state.handles("a").unwrap()),
                "{code:?} вытесняет"
            );
        }
    }

    #[test]
    fn broken_client_errors_evict_producers() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        state.connections.upsert(sample_profile("a")).unwrap();
        let p = state.profile("a").unwrap();
        let s = crate::kafka::producer::ProducerSettings::default();
        let h1 = state.producers.get(&p, state.secrets.as_ref(), &s).unwrap();
        assert!(!state.evict_if_client_broken("a", &AppError::new(ErrorCode::Timeout, "t")));
        assert!(Arc::ptr_eq(
            &h1,
            &state.producers.get(&p, state.secrets.as_ref(), &s).unwrap()
        ));
        assert!(state.evict_if_client_broken("a", &AppError::new(ErrorCode::AuthFailed, "x")));
        assert!(!Arc::ptr_eq(
            &h1,
            &state.producers.get(&p, state.secrets.as_ref(), &s).unwrap()
        ));
    }

    #[test]
    fn delete_connection_data_is_best_effort() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        state.connections.upsert(sample_profile("a")).unwrap();
        // На месте файла истории — непустой каталог: remove_file упадёт.
        let hp = dir.path().join("history").join("a.json");
        std::fs::create_dir_all(&hp).unwrap();
        std::fs::write(hp.join("x"), b"x").unwrap();
        state.delete_connection_full("a").unwrap();
        assert!(state.connections.get("a").is_err(), "подключение удалено");
    }

    #[test]
    fn delete_connection_removes_all_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        state.connections.upsert(sample_profile("a")).unwrap();
        for f in crate::secrets::CONNECTION_SECRET_FIELDS {
            state
                .secrets
                .set(&crate::secrets::secret_key("a", f), "x")
                .unwrap();
        }
        state.delete_connection_full("a").unwrap();
        for f in crate::secrets::CONNECTION_SECRET_FIELDS {
            assert_eq!(
                state
                    .secrets
                    .get(&crate::secrets::secret_key("a", f))
                    .unwrap(),
                None,
                "{f}"
            );
        }
    }

    #[test]
    fn guarded_checks_profile() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        assert_eq!(
            state.guarded("nope", None).unwrap_err().code,
            ErrorCode::NotFound
        );
        let mut p = sample_profile("a");
        p.read_only = true;
        state.connections.upsert(p).unwrap();
        assert_eq!(
            state.guarded("a", None).unwrap_err().code,
            ErrorCode::ReadOnly
        );
        state.connections.upsert(sample_profile("b")).unwrap();
        assert_eq!(state.guarded("b", None).unwrap().id, "b");
    }

    #[test]
    fn newer_templates_file_does_not_block_startup() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("templates.json"),
            r#"{"schemaVersion":99,"templates":[]}"#,
        )
        .unwrap();
        let state = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        assert!(state.templates.list("a").is_err());
        state.connections.upsert(sample_profile("a")).unwrap();
        assert!(state.handles("a").is_ok(), "остальное работает");
    }

    #[test]
    fn policy_license_key_reaches_manager() {
        let dir = tempfile::tempdir().unwrap();
        let mut st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        st.license = Arc::new(crate::license::LicenseManager::open(
            dir.path(),
            license_format::dev::verifying_key(),
        ));
        let policy = crate::policy::Policy {
            license_key: Some(license_format::dev::pro_token("ACME", None)),
            ..Default::default()
        };
        let st = st.with_policy(policy);
        assert_eq!(st.license.status().licensee.as_deref(), Some("ACME"));
    }
}
