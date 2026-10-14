use rdkafka::admin::ResourceSpecifier;
use tauri::State;

use super::blocking;
use crate::error::AppResult;
use crate::kafka::cluster::{self, ClusterOverview};
use crate::kafka::configs::{describe_configs, ConfigEntryDto};
use crate::state::AppState;

#[tauri::command]
pub async fn get_cluster_overview(state: State<'_, AppState>, connection_id: String) -> AppResult<ClusterOverview> {
    let h = state.handles(&connection_id)?;
    blocking(move || cluster::cluster_overview(&h)).await
}

#[tauri::command]
pub async fn get_broker_config(
    state: State<'_, AppState>,
    connection_id: String,
    broker_id: i32,
) -> AppResult<Vec<ConfigEntryDto>> {
    let h = state.handles(&connection_id)?;
    describe_configs(&h, ResourceSpecifier::Broker(broker_id)).await
}
