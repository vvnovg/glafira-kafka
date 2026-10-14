use rdkafka::admin::ResourceSpecifier;
use tauri::State;

use super::{blocking, pooled_configs};
use crate::error::AppResult;
use crate::kafka::configs::ConfigEntryDto;
use crate::kafka::topics::{self, TopicDetails, TopicStats, TopicSummary};
use crate::state::AppState;

#[tauri::command]
pub async fn list_topics(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<Vec<TopicSummary>> {
    let st = state.inner().clone();
    blocking(move || st.with_handles(&connection_id, topics::list_topics)).await
}

#[tauri::command]
pub async fn get_topic_stats(
    state: State<'_, AppState>,
    connection_id: String,
    topics: Vec<String>,
) -> AppResult<Vec<TopicStats>> {
    let st = state.inner().clone();
    blocking(move || st.with_handles(&connection_id, |h| topics::topic_stats(h, &topics))).await
}

#[tauri::command]
pub async fn get_topic(
    state: State<'_, AppState>,
    connection_id: String,
    topic: String,
) -> AppResult<TopicDetails> {
    let st = state.inner().clone();
    blocking(move || st.with_handles(&connection_id, |h| topics::topic_details(h, &topic))).await
}

#[tauri::command]
pub async fn get_topic_configs(
    state: State<'_, AppState>,
    connection_id: String,
    topic: String,
) -> AppResult<Vec<ConfigEntryDto>> {
    pooled_configs(
        state.inner(),
        connection_id,
        ResourceSpecifier::Topic(&topic),
    )
    .await
}
