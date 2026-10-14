use std::sync::Arc;

use serde::Deserialize;
use tauri::State;

use super::blocking;
use crate::error::AppResult;
use crate::kafka::config::SASL_PASSWORD;
use crate::kafka::connectivity::{self, TestReport};
use crate::secrets::{secret_key, MemoryStore, SecretStore};
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
pub fn list_connections(state: State<'_, AppState>) -> Vec<ConnectionProfile> {
    state.connections.list()
}

#[tauri::command]
pub fn save_connection(state: State<'_, AppState>, input: ConnectionInput) -> AppResult<ConnectionProfile> {
    let mut profile = input.profile;
    if profile.id.is_empty() {
        profile.id = uuid::Uuid::new_v4().to_string();
    }
    // upsert валидирует профиль — секрет пишем только после успешного сохранения.
    let saved = state.connections.upsert(profile)?;
    if let Some(pw) = input.sasl_password.filter(|p| !p.is_empty()) {
        state.secrets.set(&secret_key(&saved.id, SASL_PASSWORD), &pw)?;
    }
    state.pool.evict(&saved.id);
    Ok(saved)
}

#[tauri::command]
pub fn delete_connection(state: State<'_, AppState>, id: String) -> AppResult<()> {
    state.connections.delete(&id)?;
    state.secrets.delete(&secret_key(&id, SASL_PASSWORD))?;
    state.pool.evict(&id);
    Ok(())
}

#[tauri::command]
pub fn disconnect(state: State<'_, AppState>, connection_id: String) {
    state.pool.evict(&connection_id);
}

/// Проверяет и несохранённый профиль: если пароль передан, берём его, а не из keychain.
#[tauri::command]
pub async fn test_connection(state: State<'_, AppState>, input: ConnectionInput) -> AppResult<TestReport> {
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
