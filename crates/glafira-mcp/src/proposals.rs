//! Предложения LLM (спец. G3 §3–§4): виды изменения (их схема уходит LLM), операции
//! [`ProposalOp`] и ответы. Ни одна операция не меняет кластер: предложение пишется в Glafira,
//! а применяет его человек заполненной формой приложения.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::health::{Severity, StatusFilter, Target};
use crate::model::SessionView;

/// Длина `relatedFindingKey`.
pub const MAX_RELATED_KEY_CHARS: usize = 200;

/// One topic config change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigChangeSpec {
    /// Topic config name, e.g. min.insync.replicas (use names from get_topic_configs).
    pub name: String,
    /// New value as a string; null resets the config to the broker default.
    #[serde(default)]
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PartitionRef {
    pub topic: String,
    pub partition: i32,
}

/// Where to reset the offsets (`type`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ResetStrategySpec {
    /// Beginning of each partition.
    Earliest,
    /// End of each partition.
    Latest,
    /// First offset at or after this Unix time in milliseconds.
    #[serde(rename_all = "camelCase")]
    Timestamp { timestamp_ms: i64 },
    /// This exact offset on every selected partition.
    Offset { offset: i64 },
    /// Move by delta from the committed offset; negative moves back.
    Shift { delta: i64 },
}

/// Which partitions to reset (`type`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ResetScopeSpec {
    /// Every partition the group has committed offsets for.
    All,
    /// All committed partitions of these topics.
    Topics { topics: Vec<String> },
    /// These partitions only.
    Partitions { partitions: Vec<PartitionRef> },
}

/// The change an LLM proposes (`kind`): the same JSON shape Glafira stores and shows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ChangeSpec {
    /// Set or reset topic configs; the user applies them in the config editor with a before/after review.
    AlterTopicConfigs {
        topic: String,
        /// 1 to 50 entries, unique names.
        changes: Vec<ConfigChangeSpec>,
    },
    /// Create a topic; the user applies it in the create-topic form.
    #[serde(rename_all = "camelCase")]
    CreateTopic {
        name: String,
        partitions: i32,
        /// Omit or -1 for the broker default (default.replication.factor).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replication_factor: Option<i32>,
        /// Topic configs to set at creation, e.g. {"cleanup.policy": "compact"}; up to 50.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        configs: Option<BTreeMap<String, String>>,
    },
    /// Increase the number of partitions (it can never decrease).
    #[serde(rename_all = "camelCase")]
    AddPartitions { topic: String, new_total: i32 },
    /// Reset committed offsets of a consumer group (the group must be empty when the user applies it); the user previews the plan first.
    #[serde(rename_all = "camelCase")]
    ResetOffsets {
        group_id: String,
        strategy: ResetStrategySpec,
        scope: ResetScopeSpec,
    },
}

impl ChangeSpec {
    /// Имя вида — как `kind` в JSON (журнал).
    pub fn kind_name(&self) -> &'static str {
        match self {
            ChangeSpec::AlterTopicConfigs { .. } => "alterTopicConfigs",
            ChangeSpec::CreateTopic { .. } => "createTopic",
            ChangeSpec::AddPartitions { .. } => "addPartitions",
            ChangeSpec::ResetOffsets { .. } => "resetOffsets",
        }
    }

    /// Имя объекта: топик (у `createTopic` — будущий) или группа (журнал).
    pub fn target_name(&self) -> &str {
        match self {
            ChangeSpec::AlterTopicConfigs { topic, .. }
            | ChangeSpec::AddPartitions { topic, .. } => topic,
            ChangeSpec::CreateTopic { name, .. } => name,
            ChangeSpec::ResetOffsets { group_id, .. } => group_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewProposal {
    pub severity: Severity,
    pub title: String,
    pub rationale: String,
    pub change: ChangeSpec,
    pub related_rule_id: Option<String>,
    pub related_finding_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalOp {
    Propose {
        connection_id: String,
        session: SessionView,
        proposal: NewProposal,
    },
    List {
        connection_id: String,
        status: StatusFilter,
    },
    Withdraw {
        connection_id: String,
        session: SessionView,
        proposal_id: String,
    },
}

impl ProposalOp {
    pub fn connection_id(&self) -> &str {
        match self {
            ProposalOp::Propose { connection_id, .. }
            | ProposalOp::List { connection_id, .. }
            | ProposalOp::Withdraw { connection_id, .. } => connection_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProposalView {
    pub id: String,
    pub severity: Severity,
    pub title: String,
    pub rationale: String,
    pub target: Target,
    pub change: ChangeSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_rule_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_finding_key: Option<String>,
    /// Клиент, оставивший предложение; `None` — безымянный.
    pub client: Option<String>,
    /// open, applied (человек применил), rejected (человек отклонил), withdrawn (LLM отозвала).
    pub status: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_at_ms: Option<i64>,
    /// Только у applied: применено без правок (`true`) или с правками.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied_as_proposed: Option<bool>,
    /// What was actually applied when it differs from the proposal (present only with appliedAsProposed: false).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied_change: Option<ChangeSpec>,
    /// Только у rejected: комментарий человека, если он его оставил.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reject_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProposalOutput {
    Proposed {
        proposal_id: String,
        created: bool,
        /// Подключение только для чтения: применить сможет человек, сняв флажок.
        read_only: bool,
    },
    Proposals(Vec<ProposalView>),
    Withdrawn,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn change_json_matches_the_stored_form() {
        let c: ChangeSpec = serde_json::from_value(json!({
            "kind": "alterTopicConfigs", "topic": "orders",
            "changes": [{ "name": "min.insync.replicas", "value": "2" }, { "name": "retention.ms" }]
        }))
        .unwrap();
        assert_eq!(
            (c.kind_name(), c.target_name()),
            ("alterTopicConfigs", "orders")
        );
        assert_eq!(
            serde_json::to_value(&c).unwrap()["changes"][1],
            json!({ "name": "retention.ms", "value": null })
        );
        let c: ChangeSpec = serde_json::from_value(
            json!({ "kind": "createTopic", "name": "dlq", "partitions": 3 }),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(&c).unwrap(),
            json!({ "kind": "createTopic", "name": "dlq", "partitions": 3 })
        );
        let c: ChangeSpec = serde_json::from_value(json!({
            "kind": "resetOffsets", "groupId": "billing",
            "strategy": { "type": "timestamp", "timestampMs": 5 },
            "scope": { "type": "partitions", "partitions": [{ "topic": "orders", "partition": 0 }] }
        }))
        .unwrap();
        assert_eq!(c.target_name(), "billing");
        assert_eq!(
            serde_json::to_value(&c).unwrap()["strategy"],
            json!({ "type": "timestamp", "timestampMs": 5 })
        );
        let c: ChangeSpec = serde_json::from_value(
            json!({ "kind": "addPartitions", "topic": "orders", "newTotal": 6 }),
        )
        .unwrap();
        assert_eq!(serde_json::to_value(&c).unwrap()["newTotal"], 6);
        assert!(
            serde_json::from_value::<ChangeSpec>(json!({ "kind": "deleteTopic", "topic": "x" }))
                .is_err()
        );
    }

    #[test]
    fn schema_uses_camel_case_field_names() {
        let schema = serde_json::to_value(schemars::schema_for!(ChangeSpec)).unwrap();
        let text = schema.to_string();
        for name in ["newTotal", "groupId", "replicationFactor", "timestampMs"] {
            assert!(text.contains(name), "{name}: {text}");
        }
        assert!(!text.contains("new_total"), "{text}");
    }
}
