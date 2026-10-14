use rdkafka::admin::ResourceSpecifier;
use tauri::State;

use super::blocking;
use crate::error::AppResult;
use crate::kafka::configs::{ConfigEntryDto, describe_configs};
use crate::kafka::topics::{self, TopicDetails, TopicStats, TopicSummary};
use crate::state::AppState;

#[tauri::command]
pub async fn list_topics(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<Vec<TopicSummary>> {
    let st = state.inner().clone();
    blocking(move || {
        let h = st.handles(&connection_id)?;
        topics::list_topics(&h)
    })
    .await
}

#[tauri::command]
pub async fn get_topic_stats(
    state: State<'_, AppState>,
    connection_id: String,
    topics: Vec<String>,
) -> AppResult<Vec<TopicStats>> {
    let st = state.inner().clone();
    blocking(move || {
        let h = st.handles(&connection_id)?;
        topics::topic_stats(&h, &topics)
    })
    .await
}

#[tauri::command]
pub async fn get_topic(
    state: State<'_, AppState>,
    connection_id: String,
    topic: String,
) -> AppResult<TopicDetails> {
    let st = state.inner().clone();
    blocking(move || {
        let h = st.handles(&connection_id)?;
        topics::topic_details(&h, &topic)
    })
    .await
}

#[tauri::command]
pub async fn get_topic_configs(
    state: State<'_, AppState>,
    connection_id: String,
    topic: String,
) -> AppResult<Vec<ConfigEntryDto>> {
    let st = state.inner().clone();
    let h = blocking(move || st.handles(&connection_id)).await?;
    describe_configs(&h, ResourceSpecifier::Topic(&topic)).await
}
