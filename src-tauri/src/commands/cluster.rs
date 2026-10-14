use rdkafka::admin::ResourceSpecifier;
use tauri::State;

use super::{blocking, pooled_configs};
use crate::error::AppResult;
use crate::kafka::cluster::{self, ClusterOverview};
use crate::kafka::configs::ConfigEntryDto;
use crate::state::AppState;

#[tauri::command]
pub async fn get_cluster_overview(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<ClusterOverview> {
    let st = state.inner().clone();
    blocking(move || st.with_handles(&connection_id, cluster::cluster_overview)).await
}

#[tauri::command]
pub async fn get_broker_config(
    state: State<'_, AppState>,
    connection_id: String,
    broker_id: i32,
) -> AppResult<Vec<ConfigEntryDto>> {
    pooled_configs(
        state.inner(),
        connection_id,
        ResourceSpecifier::Broker(broker_id),
    )
    .await
}
