use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use rdkafka::admin::{AdminOptions, NewPartitions, NewTopic, TopicReplication, TopicResult};
use rdkafka::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::kafka::admin_ext;
use crate::kafka::errors::{admin_error, fetch_metadata_fast_fail, item_error};
use crate::kafka::metadata::convert;
use crate::kafka::pool::KafkaHandles;

const MAX_TOPIC_NAME: usize = 249;

pub fn validate_topic_name(name: &str) -> AppResult<()> {
    if name.is_empty() {
        return Err(AppError::invalid("Имя топика не может быть пустым"));
    }
    if name == "." || name == ".." {
        return Err(AppError::invalid("Имя топика не может быть «.» или «..»"));
    }
    if name.len() > MAX_TOPIC_NAME {
        return Err(AppError::invalid(format!(
            "Имя топика длиннее {MAX_TOPIC_NAME} символов"
        )));
    }
    if let Some(c) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
    {
        return Err(AppError::invalid(format!(
            "Недопустимый символ «{c}»: разрешены латинские буквы, цифры, «.», «_» и «-»"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewTopicSpec {
    pub name: String,
    pub partitions: i32,
    /// -1 — значение брокера по умолчанию (default.replication.factor).
    pub replication_factor: i32,
    #[serde(default)]
    pub configs: BTreeMap<String, String>,
}

pub fn validate_new_topic(spec: &NewTopicSpec) -> AppResult<()> {
    validate_topic_name(&spec.name)?;
    if spec.partitions < 1 {
        return Err(AppError::invalid("Число партиций должно быть не меньше 1"));
    }
    if spec.replication_factor < 1 && spec.replication_factor != -1 {
        return Err(AppError::invalid(
            "Фактор репликации должен быть не меньше 1",
        ));
    }
    if spec.configs.keys().any(|k| k.trim().is_empty()) {
        return Err(AppError::invalid("Пустое имя параметра конфигурации"));
    }
    Ok(())
}

fn admin_opts(h: &KafkaHandles) -> AdminOptions {
    AdminOptions::new()
        .request_timeout(Some(h.timeout))
        .operation_timeout(Some(h.timeout))
}

fn topic_result(r: TopicResult, context: &str) -> AppResult<String> {
    // Имя объекта не сообщение брокера: в details только код.
    r.map_err(|(_name, code)| item_error(code, "", context))
}

pub async fn create_topic(h: &Arc<KafkaHandles>, spec: &NewTopicSpec) -> AppResult<()> {
    validate_new_topic(spec)?;
    let mut topic = NewTopic::new(
        &spec.name,
        spec.partitions,
        TopicReplication::Fixed(spec.replication_factor),
    );
    for (k, v) in &spec.configs {
        topic = topic.set(k, v);
    }
    let started = Instant::now();
    let results = match h.admin.create_topics([&topic], &admin_opts(h)).await {
        Ok(r) => r,
        Err(e) => return Err(admin_error(h, started, e).await),
    };
    for r in results {
        topic_result(r, "Не удалось создать топик")?;
    }
    Ok(())
}

/// Партиции топика по метаданным. Блокирующая.
pub fn topic_partition_ids(h: &KafkaHandles, topic: &str) -> AppResult<Vec<i32>> {
    let md = fetch_metadata_fast_fail(&h.consumer, Some(topic), h.timeout)?;
    let (_, topics) = convert(&md);
    let t = topics
        .into_iter()
        .find(|t| t.name == topic && !t.partitions.is_empty())
        .ok_or_else(|| AppError::not_found(format!("Топик «{topic}» не найден")))?;
    let mut ids: Vec<i32> = t.partitions.iter().map(|p| p.id).collect();
    ids.sort_unstable();
    Ok(ids)
}

pub fn validate_partition_increase(current: usize, new_total: i32) -> AppResult<()> {
    if new_total <= 0 || (new_total as usize) <= current {
        return Err(AppError::invalid(format!(
            "Новое число партиций должно быть больше текущего ({current})"
        )));
    }
    Ok(())
}

pub async fn add_partitions(h: &Arc<KafkaHandles>, topic: &str, new_total: usize) -> AppResult<()> {
    let np = NewPartitions::new(topic, new_total);
    let started = Instant::now();
    let results = match h.admin.create_partitions([&np], &admin_opts(h)).await {
        Ok(r) => r,
        Err(e) => return Err(admin_error(h, started, e).await),
    };
    for r in results {
        topic_result(r, "Не удалось добавить партиции")?;
    }
    Ok(())
}

pub fn resolve_partitions(all: &[i32], requested: Option<&[i32]>) -> AppResult<Vec<i32>> {
    let Some(req) = requested else {
        return Ok(all.to_vec());
    };
    if req.is_empty() {
        return Err(AppError::invalid("Не выбраны партиции"));
    }
    if let Some(bad) = req.iter().find(|p| !all.contains(p)) {
        return Err(AppError::invalid(format!("В топике нет партиции {bad}")));
    }
    let mut out = req.to_vec();
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgedPartition {
    pub partition: i32,
    /// Новый low watermark; None — партицию очистить не удалось (см. error).
    pub low_watermark: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// DeleteRecords до high watermark (Offset::End) по выбранным партициям.
pub async fn purge_topic(
    h: &Arc<KafkaHandles>,
    topic: &str,
    partitions: &[i32],
) -> AppResult<Vec<PurgedPartition>> {
    let mut tpl = TopicPartitionList::with_capacity(partitions.len());
    for p in partitions {
        tpl.add_partition_offset(topic, *p, Offset::End)
            .map_err(|e| AppError::internal(e.to_string()))?;
    }
    let started = Instant::now();
    let res = match h.admin.delete_records(&tpl, &admin_opts(h)).await {
        Ok(r) => r,
        Err(e) => return Err(admin_error(h, started, e).await),
    };
    let mut out: Vec<PurgedPartition> = res
        .elements()
        .iter()
        .map(|e| match (e.error(), e.offset()) {
            (Ok(()), Offset::Offset(o)) => PurgedPartition {
                partition: e.partition(),
                low_watermark: Some(o),
                error: None,
            },
            (Ok(()), other) => PurgedPartition {
                partition: e.partition(),
                low_watermark: None,
                error: Some(format!("Неожиданный оффсет {other:?}")),
            },
            (Err(err), _) => PurgedPartition {
                partition: e.partition(),
                low_watermark: None,
                error: Some(err.to_string()),
            },
        })
        .collect();
    out.sort_by_key(|p| p.partition);
    Ok(out)
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicDeleteResult {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<AppError>,
}

pub async fn delete_topics(
    h: &Arc<KafkaHandles>,
    names: &[String],
) -> AppResult<Vec<TopicDeleteResult>> {
    if names.is_empty() {
        return Err(AppError::invalid("Не выбраны топики"));
    }
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let started = Instant::now();
    let results = match h.admin.delete_topics(&refs, &admin_opts(h)).await {
        Ok(r) => r,
        Err(e) => return Err(admin_error(h, started, e).await),
    };
    Ok(results
        .into_iter()
        .map(|r| match r {
            Ok(name) => TopicDeleteResult { name, error: None },
            Err((name, code)) => TopicDeleteResult {
                error: Some(item_error(code, "", "Не удалось удалить топик")),
                name,
            },
        })
        .collect())
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ConfigChange {
    Set {
        name: String,
        value: String,
    },
    /// Сброс к значению по умолчанию.
    Delete {
        name: String,
    },
}

impl ConfigChange {
    fn name(&self) -> &str {
        match self {
            ConfigChange::Set { name, .. } | ConfigChange::Delete { name } => name,
        }
    }
}

pub fn validate_config_changes(changes: &[ConfigChange]) -> AppResult<()> {
    if changes.is_empty() {
        return Err(AppError::invalid("Нет изменений конфигурации"));
    }
    let mut seen = HashSet::new();
    for c in changes {
        let name = c.name().trim();
        if name.is_empty() {
            return Err(AppError::invalid("Пустое имя параметра конфигурации"));
        }
        if !seen.insert(name) {
            return Err(AppError::invalid(format!(
                "Параметр «{name}» изменяется дважды"
            )));
        }
    }
    Ok(())
}

/// IncrementalAlterConfigs через FFI. Блокирующая.
pub fn alter_topic_configs(
    h: &KafkaHandles,
    topic: &str,
    changes: &[ConfigChange],
) -> AppResult<()> {
    validate_config_changes(changes)?;
    let raw: Vec<(String, Option<String>)> = changes
        .iter()
        .map(|c| match c {
            ConfigChange::Set { name, value } => (name.trim().to_string(), Some(value.clone())),
            ConfigChange::Delete { name } => (name.trim().to_string(), None),
        })
        .collect();
    let started = Instant::now();
    match admin_ext::incremental_alter_topic_configs(&h.admin, h.timeout, topic, &raw) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(item)) => Err(item_error(
            item.code,
            &item.message,
            "Не удалось изменить конфигурацию топика",
        )),
        Err(e) => Err(h.kafka_err(started, e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[test]
    fn topic_names_follow_kafka_rules() {
        for ok in ["orders", "a.b-c_d", "X1", &"a".repeat(249)] {
            assert!(validate_topic_name(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            ".",
            "..",
            "with space",
            "заказы",
            "a/b",
            &"a".repeat(250),
        ] {
            let e = validate_topic_name(bad).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput, "{bad}");
        }
    }

    fn spec(partitions: i32, rf: i32) -> NewTopicSpec {
        NewTopicSpec {
            name: "t".into(),
            partitions,
            replication_factor: rf,
            configs: BTreeMap::new(),
        }
    }

    #[test]
    fn new_topic_validation() {
        assert!(validate_new_topic(&spec(3, 1)).is_ok());
        assert!(
            validate_new_topic(&spec(1, -1)).is_ok(),
            "-1 — RF брокера по умолчанию"
        );
        for bad in [spec(0, 1), spec(-1, 1), spec(1, 0), spec(1, -2)] {
            assert_eq!(
                validate_new_topic(&bad).unwrap_err().code,
                ErrorCode::InvalidInput
            );
        }
        let mut s = spec(1, 1);
        s.configs.insert(" ".into(), "x".into());
        assert_eq!(
            validate_new_topic(&s).unwrap_err().code,
            ErrorCode::InvalidInput
        );
    }

    #[test]
    fn new_topic_spec_json() {
        let s: NewTopicSpec = serde_json::from_value(serde_json::json!({
            "name": "t", "partitions": 3, "replicationFactor": 1
        }))
        .unwrap();
        assert_eq!((s.partitions, s.replication_factor), (3, 1));
        assert!(s.configs.is_empty());
    }

    #[test]
    fn partitions_can_only_grow() {
        assert!(validate_partition_increase(3, 4).is_ok());
        for bad in [3, 2, 0, -1] {
            assert_eq!(
                validate_partition_increase(3, bad).unwrap_err().code,
                ErrorCode::InvalidInput,
                "{bad}"
            );
        }
    }

    #[test]
    fn resolve_partitions_validates_selection() {
        assert_eq!(resolve_partitions(&[0, 1, 2], None).unwrap(), [0, 1, 2]);
        assert_eq!(
            resolve_partitions(&[0, 1, 2], Some(&[2, 0])).unwrap(),
            [0, 2]
        );
        assert_eq!(resolve_partitions(&[0, 1, 2], Some(&[2, 2])).unwrap(), [2]);
        for bad in [&[][..], &[5][..]] {
            assert_eq!(
                resolve_partitions(&[0, 1, 2], Some(bad)).unwrap_err().code,
                ErrorCode::InvalidInput
            );
        }
    }

    #[test]
    fn config_changes_json_and_validation() {
        let changes: Vec<ConfigChange> = serde_json::from_value(serde_json::json!([
            {"op": "set", "name": "retention.ms", "value": "1000"},
            {"op": "delete", "name": "cleanup.policy"}
        ]))
        .unwrap();
        assert_eq!(
            changes,
            [
                ConfigChange::Set {
                    name: "retention.ms".into(),
                    value: "1000".into()
                },
                ConfigChange::Delete {
                    name: "cleanup.policy".into()
                },
            ]
        );
        assert!(validate_config_changes(&changes).is_ok());
        assert!(validate_config_changes(&[]).is_err());
        let dup = [
            ConfigChange::Delete { name: "a".into() },
            ConfigChange::Set {
                name: "a".into(),
                value: "1".into(),
            },
        ];
        assert_eq!(
            validate_config_changes(&dup).unwrap_err().code,
            ErrorCode::InvalidInput
        );
        assert!(validate_config_changes(&[ConfigChange::Delete { name: " ".into() }]).is_err());
    }
}
