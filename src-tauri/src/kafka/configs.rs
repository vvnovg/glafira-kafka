use rdkafka::admin::{AdminOptions, ResourceSpecifier};
use rdkafka::error::KafkaError;
use serde::Serialize;

use std::sync::Arc;
use std::time::Instant;

use crate::error::{AppError, AppResult};
use crate::kafka::errors::admin_error;
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

pub async fn describe_configs(
    h: &Arc<KafkaHandles>,
    spec: ResourceSpecifier<'_>,
) -> AppResult<Vec<ConfigEntryDto>> {
    let started = Instant::now();
    let opts = AdminOptions::new().request_timeout(Some(h.timeout));
    let resource = match h.admin.describe_configs(&[spec], &opts).await {
        Ok(mut results) => results
            .pop()
            .ok_or_else(|| AppError::internal("Пустой ответ DescribeConfigs"))?
            .map_err(KafkaError::AdminOp),
        Err(e) => Err(e),
    };
    let resource = match resource {
        Ok(r) => r,
        Err(e) => return Err(admin_error(h, started, e).await),
    };
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
