use rdkafka::admin::{AdminOptions, OwnedResourceSpecifier, ResourceSpecifier};
use rdkafka::error::KafkaError;
use serde::Serialize;

use std::sync::Arc;
use std::time::Instant;

use crate::error::{AppResult, ErrorCode};
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
            .ok_or_else(|| {
                app_err!(
                    ErrorCode::Internal,
                    "errors.kafka.emptyResponse",
                    "Empty {{request}} response",
                    request = "DescribeConfigs"
                )
            })?
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

/// Выбранные ключи конфигов набора топиков одним пакетным DescribeConfigs (для проверки
/// кластера). `None` у топика — брокер вернул ошибку для ресурса. Значения чувствительных
/// ключей не возвращаются.
pub async fn describe_topic_configs(
    h: &Arc<KafkaHandles>,
    topics: &[String],
    keys: &[&str],
) -> AppResult<Vec<(String, Option<Vec<ConfigEntryDto>>)>> {
    if topics.is_empty() {
        return Ok(Vec::new());
    }
    let specs: Vec<ResourceSpecifier> =
        topics.iter().map(|t| ResourceSpecifier::Topic(t)).collect();
    let started = Instant::now();
    let opts = AdminOptions::new().request_timeout(Some(h.timeout));
    let results = match h.admin.describe_configs(&specs, &opts).await {
        Ok(r) => r,
        Err(e) => return Err(admin_error(h, started, e).await),
    };
    // Ответ сопоставляется по имени ресурса: порядок librdkafka не гарантирует.
    let mut found: std::collections::HashMap<String, Vec<ConfigEntryDto>> = Default::default();
    for r in results.into_iter().flatten() {
        if let OwnedResourceSpecifier::Topic(name) = &r.specifier {
            let entries = r
                .entries
                .iter()
                .filter(|e| keys.contains(&e.name.as_str()))
                .map(|e| ConfigEntryDto {
                    name: e.name.clone(),
                    value: if e.is_sensitive {
                        None
                    } else {
                        e.value.clone()
                    },
                    source: format!("{:?}", e.source),
                    is_default: e.is_default,
                    is_read_only: e.is_read_only,
                    is_sensitive: e.is_sensitive,
                })
                .collect();
            found.insert(name.clone(), entries);
        }
    }
    Ok(topics
        .iter()
        .map(|t| (t.clone(), found.remove(t)))
        .collect())
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicPolicy {
    pub name: String,
    /// None — значение не получено (ошибка ресурса или нет прав).
    pub cleanup_policy: Option<String>,
}

/// cleanup.policy для набора топиков одним пакетным DescribeConfigs.
pub async fn describe_topic_policies(
    h: &Arc<KafkaHandles>,
    topics: &[String],
) -> AppResult<Vec<TopicPolicy>> {
    if topics.is_empty() {
        return Ok(Vec::new());
    }
    let specs: Vec<ResourceSpecifier> =
        topics.iter().map(|t| ResourceSpecifier::Topic(t)).collect();
    let started = Instant::now();
    let opts = AdminOptions::new().request_timeout(Some(h.timeout));
    let results = match h.admin.describe_configs(&specs, &opts).await {
        Ok(r) => r,
        Err(e) => return Err(admin_error(h, started, e).await),
    };
    // Ответ сопоставляется по имени ресурса: порядок librdkafka не гарантирует.
    let mut found: std::collections::HashMap<String, Option<String>> = Default::default();
    for r in results.into_iter().flatten() {
        if let OwnedResourceSpecifier::Topic(name) = &r.specifier {
            let value = r.get("cleanup.policy").and_then(|e| e.value.clone());
            found.insert(name.clone(), value);
        }
    }
    Ok(topics
        .iter()
        .map(|t| TopicPolicy {
            name: t.clone(),
            cleanup_policy: found.get(t).cloned().flatten(),
        })
        .collect())
}
