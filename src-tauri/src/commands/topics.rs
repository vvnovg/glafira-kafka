use rdkafka::admin::ResourceSpecifier;
use tauri::State;

use super::blocking;
use crate::error::AppResult;
use crate::kafka::configs::{describe_configs, ConfigEntryDto};
use crate::kafka::topics::{self, TopicDetails, TopicStats, TopicSummary};
use crate::state::AppState;

#[tauri::command]
pub async fn list_topics(state: State<'_, AppState>, connection_id: String) -> AppResult<Vec<TopicSummary>> {
    let h = state.handles(&connection_id)?;
    blocking(move || topics::list_topics(&h)).await
}

#[tauri::command]
pub async fn get_topic_stats(
    state: State<'_, AppState>,
    connection_id: String,
    topics: Vec<String>,
) -> AppResult<Vec<TopicStats>> {
    let h = state.handles(&connection_id)?;
    blocking(move || topics::topic_stats(&h, &topics)).await
}

#[tauri::command]
pub async fn get_topic(state: State<'_, AppState>, connection_id: String, topic: String) -> AppResult<TopicDetails> {
    let h = state.handles(&connection_id)?;
    blocking(move || topics::topic_details(&h, &topic)).await
}

#[tauri::command]
pub async fn get_topic_configs(
    state: State<'_, AppState>,
    connection_id: String,
    topic: String,
) -> AppResult<Vec<ConfigEntryDto>> {
    let h = state.handles(&connection_id)?;
    describe_configs(&h, ResourceSpecifier::Topic(&topic)).await
}
