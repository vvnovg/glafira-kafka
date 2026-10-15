use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::browse::filter::FilterSpec;
use crate::browse::format::DataFormat;
use crate::browse::types::BrowsedMessage;
use crate::error::{AppError, AppResult};
use crate::kafka::context::RebalanceRecord;
use crate::kafka::groups::{OffsetTarget, TopicPartitionRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OffsetReset {
    Earliest,
    Latest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum IsolationLevel {
    ReadCommitted,
    ReadUncommitted,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerParams {
    pub connection_id: String,
    pub group_id: String,
    #[serde(default)]
    pub topics: Vec<String>,
    /// Regex-подписка (librdkafka: имя топика, начинающееся с «^»).
    #[serde(default)]
    pub topic_pattern: Option<String>,
    pub auto_offset_reset: OffsetReset,
    pub enable_auto_commit: bool,
    pub isolation_level: IsolationLevel,
    pub key_format: DataFormat,
    pub value_format: DataFormat,
    #[serde(default)]
    pub filter: FilterSpec,
}

pub fn validate(p: &ConsumerParams) -> AppResult<()> {
    if p.group_id.trim().is_empty() {
        return Err(AppError::invalid("Укажите group.id"));
    }
    let has_topics = p.topics.iter().any(|t| !t.trim().is_empty());
    let pattern = p
        .topic_pattern
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    match (has_topics, pattern) {
        (true, None) => Ok(()),
        (false, Some(re)) => Regex::new(re)
            .map(|_| ())
            .map_err(|e| AppError::invalid(format!("Некорректное регулярное выражение: {e}"))),
        (false, None) => Err(AppError::invalid("Укажите топики или регулярное выражение")),
        (true, Some(_)) => Err(AppError::invalid(
            "Укажите либо список топиков, либо регулярное выражение",
        )),
    }
}

pub fn subscription(p: &ConsumerParams) -> Vec<String> {
    match p
        .topic_pattern
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(re) if re.starts_with('^') => vec![re.to_string()],
        Some(re) => vec![format!("^{re}")],
        None => p
            .topics
            .iter()
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect(),
    }
}

/// kafka-explorer-<user>-<suffix>: в имени пользователя — только латиница, цифры, «_» и «-».
pub fn default_group_id(user: Option<&str>, suffix: &str) -> String {
    let clean: String = user
        .unwrap_or("")
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
        .collect();
    let user = if clean.is_empty() {
        "user"
    } else {
        clean.as_str()
    };
    format!("kafka-explorer-{user}-{suffix}")
}

pub fn current_user() -> Option<String> {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .ok()
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum CommitTarget {
    /// Закоммитить до выбранного сообщения включительно: offset + 1.
    Selected {
        topic: String,
        partition: i32,
        offset: i64,
    },
    /// Текущие позиции по всем назначенным партициям.
    AllConsumed,
}

#[derive(Debug, Clone, Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ConsumeEvent {
    Messages {
        messages: Vec<BrowsedMessage>,
    },
    Progress {
        consumed: u64,
        matched: u64,
    },
    Rebalance {
        record: RebalanceRecord,
    },
    Committed {
        offsets: Vec<OffsetTarget>,
    },
    Paused {
        partitions: Vec<TopicPartitionRef>,
    },
    Resumed {
        partitions: Vec<TopicPartitionRef>,
    },
    /// Временная потеря связи с брокером: librdkafka переподключается.
    Reconnecting {
        message: String,
    },
    /// Связь восстановлена после Reconnecting.
    Recovered,
    Finished {
        consumed: u64,
        matched: u64,
    },
    Error {
        error: AppError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    fn params() -> ConsumerParams {
        serde_json::from_value(serde_json::json!({
            "connectionId": "c",
            "groupId": "g",
            "topics": ["a", "b"],
            "autoOffsetReset": "earliest",
            "enableAutoCommit": false,
            "isolationLevel": "readCommitted",
            "keyFormat": "auto",
            "valueFormat": "json"
        }))
        .unwrap()
    }

    #[test]
    fn validation_and_subscription() {
        let p = params();
        assert!(validate(&p).is_ok());
        assert_eq!(subscription(&p), ["a", "b"]);
        let mut re = params();
        re.topics.clear();
        re.topic_pattern = Some("orders\\..*".into());
        assert!(validate(&re).is_ok());
        assert_eq!(subscription(&re), ["^orders\\..*"]);
        re.topic_pattern = Some("^orders".into());
        assert_eq!(subscription(&re), ["^orders"], "^ не дублируется");

        let mut bad = vec![];
        let mut p1 = params();
        p1.group_id = " ".into();
        bad.push(p1);
        let mut p2 = params();
        p2.topics.clear();
        bad.push(p2);
        let mut p3 = params();
        p3.topic_pattern = Some("x".into());
        bad.push(p3); // и список, и regex
        let mut p4 = params();
        p4.topics.clear();
        p4.topic_pattern = Some("(".into());
        bad.push(p4);
        for p in bad {
            assert_eq!(validate(&p).unwrap_err().code, ErrorCode::InvalidInput);
        }
    }

    #[test]
    fn generated_group_id() {
        assert_eq!(
            default_group_id(Some("Анна Petrova"), "a1b2c3"),
            "kafka-explorer-Petrova-a1b2c3"
        );
        assert_eq!(
            default_group_id(None, "a1b2c3"),
            "kafka-explorer-user-a1b2c3"
        );
        assert_eq!(
            default_group_id(Some("qa.bot"), "ffffff"),
            "kafka-explorer-qabot-ffffff"
        );
    }

    #[test]
    fn events_and_commit_target_json() {
        let t: CommitTarget = serde_json::from_value(
            serde_json::json!({"type": "selected", "topic": "t", "partition": 1, "offset": 9}),
        )
        .unwrap();
        assert_eq!(
            t,
            CommitTarget::Selected {
                topic: "t".into(),
                partition: 1,
                offset: 9
            }
        );
        let all: CommitTarget =
            serde_json::from_value(serde_json::json!({"type": "allConsumed"})).unwrap();
        assert_eq!(all, CommitTarget::AllConsumed);
        let v = serde_json::to_value(ConsumeEvent::Finished {
            consumed: 3,
            matched: 2,
        })
        .unwrap();
        assert_eq!(
            v,
            serde_json::json!({"type": "finished", "consumed": 3, "matched": 2})
        );
    }
}
