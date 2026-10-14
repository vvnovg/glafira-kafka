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
    blocking(move || {
        let mut profile = input.profile;
        if profile.id.is_empty() {
            profile.id = uuid::Uuid::new_v4().to_string();
        }
        // upsert валидирует профиль — секрет пишем только после успешного сохранения.
        let saved = st.connections.upsert(profile)?;
        if let Some(pw) = input.sasl_password.filter(|p| !p.is_empty()) {
            st.secrets.set(&secret_key(&saved.id, SASL_PASSWORD), &pw)?;
        }
        st.pool.evict(&saved.id);
        Ok(saved)
    })
    .await
}

#[tauri::command]
pub async fn delete_connection(state: State<'_, AppState>, id: String) -> AppResult<()> {
    let st = state.inner().clone();
    blocking(move || {
        st.connections.delete(&id)?;
        st.secrets.delete(&secret_key(&id, SASL_PASSWORD))?;
        st.pool.evict(&id);
        Ok(())
    })
    .await
}

#[tauri::command]
pub async fn disconnect(state: State<'_, AppState>, connection_id: String) -> AppResult<()> {
    let st = state.inner().clone();
    blocking(move || {
        st.pool.evict(&connection_id);
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
