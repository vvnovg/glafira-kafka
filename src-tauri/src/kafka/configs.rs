use rdkafka::admin::{AdminOptions, ResourceSpecifier};
use rdkafka::error::KafkaError;
use serde::Serialize;

use crate::error::{AppError, AppResult};
use crate::kafka::pool::KafkaHandles;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigEntryDto {
    pub name: String,
    pub value: Option<String>,
    /// Debug-имя ConfigSource: Default, DynamicTopic, DynamicBroker, DynamicDefaultBroker, StaticBroker, Unknown.
    pub source: String,
    pub is_default: bool,
    pub is_read_only: bool,
    pub is_sensitive: bool,
}

pub async fn describe_configs(h: &KafkaHandles, spec: ResourceSpecifier<'_>) -> AppResult<Vec<ConfigEntryDto>> {
    let opts = AdminOptions::new().request_timeout(Some(h.timeout));
    let mut results = h.admin.describe_configs(&[spec], &opts).await.map_err(|e| h.kafka_err(e))?;
    let resource = results
        .pop()
        .ok_or_else(|| AppError::internal("Пустой ответ DescribeConfigs"))?
        .map_err(|code| h.kafka_err(KafkaError::AdminOp(code)))?;
    let mut entries: Vec<ConfigEntryDto> = resource
        .entries
        .into_iter()
        .map(|e| ConfigEntryDto {
            name: e.name,
            value: e.value,
            source: format!("{:?}", e.source),
            is_default: e.is_default,
            is_read_only: e.is_read_only,
            is_sensitive: e.is_sensitive,
        })
        .collect();
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(entries)
}
