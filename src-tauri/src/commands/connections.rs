use std::sync::Arc;

use serde::Deserialize;
use tauri::State;

use super::blocking;
use crate::error::AppResult;
use crate::kafka::config::SASL_PASSWORD;
use crate::kafka::connectivity::{self, TestReport};
use crate::secrets::{MemoryStore, SecretStore, secret_key};
use crate::state::AppState;
use crate::storage::connections::ConnectionProfile;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInput {
    pub profile: ConnectionProfile,
    #[serde(default)]
    pub sasl_password: Option<String>,
}

#[tauri::command]
pub fn list_connections(state: State<'_, AppState>) -> AppResult<Vec<ConnectionProfile>> {
    Ok(state.connections.list())
}

#[tauri::command]
pub async fn save_connection(
    state: State<'_, AppState>,
    input: ConnectionInput,
) -> AppResult<ConnectionProfile> {
    let st = state.inner().clone();
    blocking(move || save_connection_blocking(&st, input)).await
}

/// Сохраняет профиль и секрет согласованно: если keychain не принял изменение,
/// профиль возвращается к прежнему состоянию (или удаляется, если он новый).
pub fn save_connection_blocking(
    st: &AppState,
    input: ConnectionInput,
) -> AppResult<ConnectionProfile> {
    let mut profile = input.profile;
    if profile.id.is_empty() {
        profile.id = uuid::Uuid::new_v4().to_string();
    }
    let previous = st.connections.get(&profile.id).ok();
    // upsert валидирует профиль — секрет трогаем только после успешного сохранения.
    let saved = st.connections.upsert(profile)?;
    let key = secret_key(&saved.id, SASL_PASSWORD);
    let secret_result = if !saved.security.uses_sasl() {
        st.secrets.delete(&key)
    } else if let Some(pw) = input.sasl_password.filter(|p| !p.is_empty()) {
        st.secrets.set(&key, &pw)
    } else {
        Ok(())
    };
    if let Err(e) = secret_result {
        let rollback = match previous {
            Some(p) => st.connections.upsert(p).map(|_| ()),
            None => st.connections.delete(&saved.id),
        };
        if let Err(r) = rollback {
            tracing::error!("не удалось откатить профиль {}: {r}", saved.id);
        }
        return Err(e);
    }
    st.evict_connection(&saved.id);
    Ok(saved)
}

#[tauri::command]
pub async fn delete_connection(state: State<'_, AppState>, id: String) -> AppResult<()> {
    let st = state.inner().clone();
    blocking(move || {
        // Живые browse-сессии удаляемого подключения больше не нужны — даже если удаление ниже упадёт.
        st.sessions.cancel_for(&id);
        st.connections.delete(&id)?;
        st.secrets.delete(&secret_key(&id, SASL_PASSWORD))?;
        st.evict_connection(&id);
        st.history.delete_connection(&id)?;
        st.templates.delete_for_connection(&id)?;
        Ok(())
    })
    .await
}

#[tauri::command]
pub async fn disconnect(state: State<'_, AppState>, connection_id: String) -> AppResult<()> {
    let st = state.inner().clone();
    blocking(move || {
        st.evict_connection(&connection_id);
        Ok(())
    })
    .await
}

/// Проверяет и несохранённый профиль: если пароль передан, берём его, а не из keychain.
#[tauri::command]
pub async fn test_connection(
    state: State<'_, AppState>,
    input: ConnectionInput,
) -> AppResult<TestReport> {
    let secrets: Arc<dyn SecretStore> = match input.sasl_password.filter(|p| !p.is_empty()) {
        Some(pw) => {
            let mem = MemoryStore::default();
            mem.set(&secret_key(&input.profile.id, SASL_PASSWORD), &pw)?;
            Arc::new(mem)
        }
        None => state.secrets.clone(),
    };
    let profile = input.profile;
    blocking(move || Ok(connectivity::test_connection(&profile, secrets.as_ref()))).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{AppError, ErrorCode};
    use crate::storage::connections::{SaslConfig, SaslMechanism, SecurityConfig, sample_profile};

    /// Keychain, в который ничего нельзя записать.
    struct BrokenKeychain;

    impl SecretStore for BrokenKeychain {
        fn get(&self, _: &str) -> AppResult<Option<String>> {
            Ok(None)
        }
        fn set(&self, _: &str, _: &str) -> AppResult<()> {
            Err(AppError::internal("keychain недоступен"))
        }
        fn delete(&self, _: &str) -> AppResult<()> {
            Err(AppError::internal("keychain недоступен"))
        }
    }

    fn sasl(mut p: ConnectionProfile) -> ConnectionProfile {
        p.security = SecurityConfig::SaslPlaintext {
            sasl: SaslConfig {
                mechanism: SaslMechanism::Plain,
                username: "u".into(),
            },
        };
        p
    }

    fn input(profile: ConnectionProfile, pw: Option<&str>) -> ConnectionInput {
        ConnectionInput {
            profile,
            sasl_password: pw.map(String::from),
        }
    }

    fn state(secrets: Arc<dyn SecretStore>) -> (tempfile::TempDir, AppState) {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), secrets).unwrap();
        (dir, st)
    }

    #[test]
    fn switching_off_sasl_deletes_secret() {
        let (_d, st) = state(Arc::new(MemoryStore::default()));
        save_connection_blocking(&st, input(sasl(sample_profile("a")), Some("pw"))).unwrap();
        let key = secret_key("a", SASL_PASSWORD);
        assert_eq!(st.secrets.get(&key).unwrap().as_deref(), Some("pw"));
        // Форма может прислать старый пароль и для PLAINTEXT — он не должен сохраниться.
        save_connection_blocking(&st, input(sample_profile("a"), Some("pw"))).unwrap();
        assert_eq!(st.secrets.get(&key).unwrap(), None);
    }

    #[test]
    fn empty_password_keeps_existing_secret() {
        let (_d, st) = state(Arc::new(MemoryStore::default()));
        save_connection_blocking(&st, input(sasl(sample_profile("a")), Some("pw"))).unwrap();
        save_connection_blocking(&st, input(sasl(sample_profile("a")), Some(""))).unwrap();
        let key = secret_key("a", SASL_PASSWORD);
        assert_eq!(st.secrets.get(&key).unwrap().as_deref(), Some("pw"));
    }

    #[test]
    fn keychain_failure_does_not_leave_new_profile() {
        let (_d, st) = state(Arc::new(BrokenKeychain));
        let err = save_connection_blocking(&st, input(sasl(sample_profile("a")), Some("pw")))
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(
            st.connections.get("a").unwrap_err().code,
            ErrorCode::NotFound
        );
    }

    #[test]
    fn keychain_failure_restores_previous_profile() {
        let (_d, st) = state(Arc::new(BrokenKeychain));
        st.connections.upsert(sample_profile("a")).unwrap();
        let mut changed = sasl(sample_profile("a"));
        changed.name = "новое имя".into();
        assert!(save_connection_blocking(&st, input(changed, Some("pw"))).is_err());
        assert_eq!(st.connections.get("a").unwrap(), sample_profile("a"));
    }
}
