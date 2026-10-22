//! Предложения LLM (спец. G3 §3, §5): виды изменения, проверка против фактов кластера и
//! сравнение применённого с предложенным. Модуль не ходит в Kafka и ничего в нём не меняет:
//! применяет изменение только человек через формы и их команды.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::model::{Severity, Target};
use crate::kafka::admin::{ConfigChange, NewTopicSpec, validate_new_topic};
use crate::kafka::groups::{ResetRequest, ResetScope, ResetStrategy, validate_reset_request};
use crate::kafka::metadata::is_internal;

/// Записей в `alterTopicConfigs` и ключей конфигов в `createTopic`.
pub const MAX_CONFIG_ENTRIES: usize = 50;
/// Длина значения конфига.
pub const MAX_CONFIG_VALUE_CHARS: usize = 1024;
/// Топиков или партиций в области сброса оффсетов.
pub const MAX_SCOPE_ITEMS: usize = 100;
pub const MAX_RATIONALE_CHARS: usize = 2000;
pub const MAX_REJECT_REASON_CHARS: usize = 300;
pub const MAX_RELATED_KEY_CHARS: usize = 200;
/// Открытых предложений на подключение.
pub const MAX_OPEN_PROPOSALS: usize = 50;

/// Одна запись изменения конфига топика: `value: None` — вернуть значение по умолчанию.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigValueChange {
    pub name: String,
    #[serde(default)]
    pub value: Option<String>,
}

fn default_replication_factor() -> i32 {
    -1
}

/// Что предлагает LLM. Одна и та же JSON-форма у модели, в файле и во фронтенде.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Change {
    AlterTopicConfigs {
        topic: String,
        changes: Vec<ConfigValueChange>,
    },
    CreateTopic {
        name: String,
        partitions: i32,
        /// `-1` — значение брокера по умолчанию.
        #[serde(default = "default_replication_factor")]
        replication_factor: i32,
        #[serde(default)]
        configs: BTreeMap<String, String>,
    },
    AddPartitions {
        topic: String,
        new_total: i32,
    },
    ResetOffsets {
        group_id: String,
        strategy: ResetStrategy,
        scope: ResetScope,
    },
}

impl Change {
    /// Имя вида изменения — как `kind` в JSON (журнал, дубли).
    pub fn kind_name(&self) -> &'static str {
        match self {
            Change::AlterTopicConfigs { .. } => "alterTopicConfigs",
            Change::CreateTopic { .. } => "createTopic",
            Change::AddPartitions { .. } => "addPartitions",
            Change::ResetOffsets { .. } => "resetOffsets",
        }
    }

    /// Объект предложения: топик (у `createTopic` — будущий) или группа.
    pub fn target(&self) -> Target {
        match self {
            Change::AlterTopicConfigs { topic, .. } | Change::AddPartitions { topic, .. } => {
                Target::topic(topic)
            }
            Change::CreateTopic { name, .. } => Target::topic(name),
            Change::ResetOffsets { group_id, .. } => Target::group(group_id),
        }
    }

    /// Имена по краям обрезаются (как у `annotate` сверка идёт по обрезанному имени).
    pub fn trimmed(mut self) -> Self {
        match &mut self {
            Change::AlterTopicConfigs { topic, .. } | Change::AddPartitions { topic, .. } => {
                *topic = topic.trim().to_string();
            }
            Change::CreateTopic { name, .. } => *name = name.trim().to_string(),
            Change::ResetOffsets { group_id, .. } => *group_id = group_id.trim().to_string(),
        }
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProposalStatus {
    Open,
    /// Человек применил формой (ставит только Rust после успеха операции).
    Applied,
    /// Человек отклонил; «Открыть снова» возвращает в `Open`.
    Rejected,
    /// LLM отозвала своё.
    Withdrawn,
}

impl ProposalStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            ProposalStatus::Open => "open",
            ProposalStatus::Applied => "applied",
            ProposalStatus::Rejected => "rejected",
            ProposalStatus::Withdrawn => "withdrawn",
        }
    }
}

/// Предложение в файле `health/<connectionId>.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Proposal {
    pub id: String,
    pub severity: Severity,
    pub title: String,
    pub rationale: String,
    pub target: Target,
    pub change: Change,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub related_rule_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub related_finding_key: Option<String>,
    /// Имя клиента из диалога подтверждения; `None` — безымянный.
    #[serde(default)]
    pub client: Option<String>,
    /// Сессия безымянного клиента: отозвать можно только из неё.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub status: ProposalStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_at_ms: Option<i64>,
    /// Только у `applied`: человек применил без правок (`true`) или с правками.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_as_proposed: Option<bool>,
    /// Только у `rejected`: необязательный комментарий человека.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reject_reason: Option<String>,
}

/// Факты о кластере для проверки предложения. Собирает их `mcp/backend.rs` существующими
/// функциями чтения; здесь — только сверка.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Facts {
    pub brokers: usize,
    pub topics: Vec<TopicFact>,
    /// Ключи конфигов топика из `DescribeConfigs` — только для `alterTopicConfigs`.
    pub config_keys: Option<BTreeSet<String>>,
    /// Группы кластера — только для `resetOffsets`.
    pub groups: Option<BTreeSet<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicFact {
    pub name: String,
    pub partitions: usize,
    pub internal: bool,
}

impl Facts {
    fn topic(&self, name: &str) -> Option<&TopicFact> {
        self.topics.iter().find(|t| t.name == name)
    }
}

/// Почему предложение отвергнуто: `invalid_argument` или `not_found` для LLM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalError {
    Invalid(String),
    NotFound(String),
}

fn no_topic(name: &str) -> ProposalError {
    ProposalError::NotFound(format!("There is no topic {name} in this cluster"))
}

fn internal_topic(name: &str) -> ProposalError {
    ProposalError::Invalid(format!(
        "{name} is an internal topic: internal topics cannot be changed through Glafira"
    ))
}

fn check_config_value(name: &str, value: &str) -> Result<(), ProposalError> {
    if value.trim().is_empty() {
        return Err(ProposalError::Invalid(format!(
            "value of {name} must not be empty; use null to reset it to the default"
        )));
    }
    if value.chars().count() > MAX_CONFIG_VALUE_CHARS {
        return Err(ProposalError::Invalid(format!(
            "value of {name} is longer than {MAX_CONFIG_VALUE_CHARS} characters"
        )));
    }
    Ok(())
}

/// Проверка предложения против фактов кластера (спец. G3 §5). Значения конфигов Kafka
/// проверяет сама при применении — здесь только форма и объекты.
pub fn validate(change: &Change, facts: &Facts) -> Result<(), ProposalError> {
    match change {
        Change::AlterTopicConfigs { topic, changes } => {
            let t = facts.topic(topic).ok_or_else(|| no_topic(topic))?;
            if t.internal {
                return Err(internal_topic(topic));
            }
            if changes.is_empty() || changes.len() > MAX_CONFIG_ENTRIES {
                return Err(ProposalError::Invalid(format!(
                    "changes must have 1 to {MAX_CONFIG_ENTRIES} entries, got {}",
                    changes.len()
                )));
            }
            let mut seen = BTreeSet::new();
            for c in changes {
                if c.name.trim().is_empty() {
                    return Err(ProposalError::Invalid(
                        "a config change has an empty name".into(),
                    ));
                }
                if !seen.insert(c.name.as_str()) {
                    return Err(ProposalError::Invalid(format!(
                        "config {} is listed more than once",
                        c.name
                    )));
                }
                if let Some(keys) = &facts.config_keys
                    && !keys.contains(&c.name)
                {
                    return Err(ProposalError::Invalid(format!(
                        "unknown topic config {}: use names from get_topic_configs",
                        c.name
                    )));
                }
                if let Some(v) = &c.value {
                    check_config_value(&c.name, v)?;
                }
            }
            Ok(())
        }
        Change::CreateTopic {
            name,
            partitions,
            replication_factor,
            configs,
        } => {
            let spec = NewTopicSpec {
                name: name.clone(),
                partitions: *partitions,
                replication_factor: *replication_factor,
                configs: configs.clone(),
            };
            validate_new_topic(&spec).map_err(|e| ProposalError::Invalid(e.message))?;
            if is_internal(name) {
                return Err(internal_topic(name));
            }
            if facts.topic(name).is_some() {
                return Err(ProposalError::Invalid(format!(
                    "topic {name} already exists; propose alterTopicConfigs or addPartitions instead"
                )));
            }
            if *replication_factor > 0 && *replication_factor as usize > facts.brokers {
                return Err(ProposalError::Invalid(format!(
                    "replicationFactor {replication_factor} is above the number of brokers ({})",
                    facts.brokers
                )));
            }
            if configs.len() > MAX_CONFIG_ENTRIES {
                return Err(ProposalError::Invalid(format!(
                    "configs must have at most {MAX_CONFIG_ENTRIES} entries, got {}",
                    configs.len()
                )));
            }
            for (k, v) in configs {
                check_config_value(k, v)?;
            }
            Ok(())
        }
        Change::AddPartitions { topic, new_total } => {
            let t = facts.topic(topic).ok_or_else(|| no_topic(topic))?;
            if t.internal {
                return Err(internal_topic(topic));
            }
            if *new_total <= 0 || *new_total as usize <= t.partitions {
                return Err(ProposalError::Invalid(format!(
                    "topic {topic} has {} partitions; newTotal must be greater, got {new_total}",
                    t.partitions
                )));
            }
            Ok(())
        }
        Change::ResetOffsets {
            group_id,
            strategy,
            scope,
        } => {
            if let Some(groups) = &facts.groups
                && !groups.contains(group_id)
            {
                return Err(ProposalError::NotFound(format!(
                    "There is no consumer group {group_id} in this cluster"
                )));
            }
            let request = ResetRequest {
                group_id: group_id.clone(),
                strategy: strategy.clone(),
                scope: scope.clone(),
            };
            validate_reset_request(&request).map_err(|e| ProposalError::Invalid(e.message))?;
            match scope {
                ResetScope::All => {}
                ResetScope::Topics { topics } => {
                    if topics.len() > MAX_SCOPE_ITEMS {
                        return Err(ProposalError::Invalid(format!(
                            "scope.topics must have at most {MAX_SCOPE_ITEMS} entries"
                        )));
                    }
                    for t in topics {
                        facts.topic(t).ok_or_else(|| no_topic(t))?;
                    }
                }
                ResetScope::Partitions { partitions } => {
                    if partitions.len() > MAX_SCOPE_ITEMS {
                        return Err(ProposalError::Invalid(format!(
                            "scope.partitions must have at most {MAX_SCOPE_ITEMS} entries"
                        )));
                    }
                    for p in partitions {
                        let t = facts.topic(&p.topic).ok_or_else(|| no_topic(&p.topic))?;
                        if p.partition < 0 || p.partition as usize >= t.partitions {
                            return Err(ProposalError::Invalid(format!(
                                "topic {} has {} partitions; partition {} does not exist",
                                p.topic, t.partitions, p.partition
                            )));
                        }
                    }
                }
            }
            Ok(())
        }
    }
}

/// Что человек применил формой — для сравнения с предложением (спец. G3 §6).
#[derive(Debug, Clone, PartialEq)]
pub enum Applied {
    AlterTopicConfigs {
        topic: String,
        changes: Vec<ConfigChange>,
    },
    CreateTopic(NewTopicSpec),
    AddPartitions {
        topic: String,
        new_total: i32,
    },
    /// `None` — форма не передала стратегию и область: считается «с правками».
    ResetOffsets(Option<ResetRequest>),
}

impl Applied {
    /// Вид и объект применённого: предложение другого вида или объекта не отмечается.
    pub fn matches(&self, change: &Change) -> bool {
        match (self, change) {
            (
                Applied::AlterTopicConfigs { topic: a, .. },
                Change::AlterTopicConfigs { topic: b, .. },
            )
            | (Applied::AddPartitions { topic: a, .. }, Change::AddPartitions { topic: b, .. }) => {
                a == b
            }
            (Applied::CreateTopic(_), Change::CreateTopic { .. }) => true,
            (Applied::ResetOffsets(_), Change::ResetOffsets { .. }) => true,
            _ => false,
        }
    }
}

fn config_set(changes: &[ConfigChange]) -> BTreeSet<(String, Option<String>)> {
    changes
        .iter()
        .map(|c| match c {
            ConfigChange::Set { name, value } => (name.clone(), Some(value.clone())),
            ConfigChange::Delete { name } => (name.clone(), None),
        })
        .collect()
}

/// Применено ровно то, что предлагалось: конфиги — множеством пар без учёта порядка, топик —
/// всеми полями, партиции — числом, сброс — стратегией и областью.
pub fn applied_as_proposed(change: &Change, applied: &Applied) -> bool {
    match (change, applied) {
        (
            Change::AlterTopicConfigs { changes, .. },
            Applied::AlterTopicConfigs {
                changes: applied, ..
            },
        ) => {
            let proposed: BTreeSet<(String, Option<String>)> = changes
                .iter()
                .map(|c| (c.name.clone(), c.value.clone()))
                .collect();
            proposed == config_set(applied)
        }
        (
            Change::CreateTopic {
                name,
                partitions,
                replication_factor,
                configs,
            },
            Applied::CreateTopic(spec),
        ) => {
            &spec.name == name
                && spec.partitions == *partitions
                && spec.replication_factor == *replication_factor
                && &spec.configs == configs
        }
        (Change::AddPartitions { new_total, .. }, Applied::AddPartitions { new_total: n, .. }) => {
            n == new_total
        }
        (
            Change::ResetOffsets {
                group_id,
                strategy,
                scope,
            },
            Applied::ResetOffsets(Some(r)),
        ) => &r.group_id == group_id && &r.strategy == strategy && &r.scope == scope,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::groups::TopicPartitionRef;

    fn facts() -> Facts {
        Facts {
            brokers: 3,
            topics: vec![
                TopicFact {
                    name: "orders".into(),
                    partitions: 3,
                    internal: false,
                },
                TopicFact {
                    name: "__consumer_offsets".into(),
                    partitions: 50,
                    internal: true,
                },
            ],
            config_keys: Some(
                ["min.insync.replicas", "retention.ms", "cleanup.policy"]
                    .into_iter()
                    .map(String::from)
                    .collect(),
            ),
            groups: Some(["billing".to_string()].into_iter().collect()),
        }
    }

    fn set(name: &str, value: Option<&str>) -> ConfigValueChange {
        ConfigValueChange {
            name: name.into(),
            value: value.map(String::from),
        }
    }

    fn alter(topic: &str, changes: Vec<ConfigValueChange>) -> Change {
        Change::AlterTopicConfigs {
            topic: topic.into(),
            changes,
        }
    }

    fn create(name: &str, rf: i32) -> Change {
        Change::CreateTopic {
            name: name.into(),
            partitions: 6,
            replication_factor: rf,
            configs: BTreeMap::from([("cleanup.policy".to_string(), "compact".to_string())]),
        }
    }

    fn reset(group: &str, scope: ResetScope) -> Change {
        Change::ResetOffsets {
            group_id: group.into(),
            strategy: ResetStrategy::Earliest,
            scope,
        }
    }

    fn invalid(r: Result<(), ProposalError>) -> String {
        match r {
            Err(ProposalError::Invalid(m)) => m,
            other => panic!("expected invalid_argument, got {other:?}"),
        }
    }

    fn not_found(r: Result<(), ProposalError>) -> String {
        match r {
            Err(ProposalError::NotFound(m)) => m,
            other => panic!("expected not_found, got {other:?}"),
        }
    }

    #[test]
    fn change_json_uses_kind_tag_and_camel_case() {
        let c = alter(
            "orders",
            vec![
                set("min.insync.replicas", Some("2")),
                set("retention.ms", None),
            ],
        );
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "kind": "alterTopicConfigs",
                "topic": "orders",
                "changes": [
                    { "name": "min.insync.replicas", "value": "2" },
                    { "name": "retention.ms", "value": null }
                ]
            })
        );
        let back: Change = serde_json::from_value(v).unwrap();
        assert_eq!(back, c);
        // createTopic без replicationFactor и configs — значение брокера и пусто.
        let c: Change = serde_json::from_value(serde_json::json!({
            "kind": "createTopic", "name": "dlq", "partitions": 1
        }))
        .unwrap();
        assert_eq!(
            c,
            Change::CreateTopic {
                name: "dlq".into(),
                partitions: 1,
                replication_factor: -1,
                configs: BTreeMap::new()
            }
        );
        let r = reset(
            "billing",
            ResetScope::Partitions {
                partitions: vec![TopicPartitionRef {
                    topic: "orders".into(),
                    partition: 1,
                }],
            },
        );
        assert_eq!(
            serde_json::to_value(&r).unwrap(),
            serde_json::json!({
                "kind": "resetOffsets", "groupId": "billing",
                "strategy": { "type": "earliest" },
                "scope": { "type": "partitions", "partitions": [{ "topic": "orders", "partition": 1 }] }
            })
        );
    }

    #[test]
    fn kind_target_and_trim() {
        let c = alter(" orders ", vec![]).trimmed();
        assert_eq!(c.kind_name(), "alterTopicConfigs");
        assert_eq!(c.target(), Target::topic("orders"));
        assert_eq!(create("dlq", 3).target(), Target::topic("dlq"));
        assert_eq!(
            reset(" billing", ResetScope::All).trimmed().target(),
            Target::group("billing")
        );
        assert_eq!(
            Change::AddPartitions {
                topic: "orders".into(),
                new_total: 6
            }
            .kind_name(),
            "addPartitions"
        );
    }

    #[test]
    fn alter_configs_is_checked_against_topic_and_known_keys() {
        let f = facts();
        assert_eq!(
            validate(
                &alter(
                    "orders",
                    vec![
                        set("min.insync.replicas", Some("2")),
                        set("retention.ms", None)
                    ]
                ),
                &f
            ),
            Ok(())
        );
        assert!(
            not_found(validate(
                &alter("missing", vec![set("retention.ms", None)]),
                &f
            ))
            .contains("missing")
        );
        assert!(
            invalid(validate(
                &alter("__consumer_offsets", vec![set("retention.ms", None)]),
                &f
            ))
            .contains("internal")
        );
        assert!(invalid(validate(&alter("orders", vec![]), &f)).contains("1 to 50"));
        let many: Vec<_> = (0..51).map(|i| set(&format!("k{i}"), Some("1"))).collect();
        assert!(invalid(validate(&alter("orders", many), &f)).contains("1 to 50"));
        assert!(
            invalid(validate(&alter("orders", vec![set("nope", Some("1"))]), &f))
                .contains("unknown topic config nope")
        );
        assert!(
            invalid(validate(
                &alter(
                    "orders",
                    vec![set("retention.ms", Some("1")), set("retention.ms", None)]
                ),
                &f
            ))
            .contains("more than once")
        );
        assert!(
            invalid(validate(
                &alter("orders", vec![set("retention.ms", Some("  "))]),
                &f
            ))
            .contains("must not be empty")
        );
        assert!(
            invalid(validate(
                &alter("orders", vec![set("retention.ms", Some(&"x".repeat(1025)))]),
                &f
            ))
            .contains("1024")
        );
        assert!(
            invalid(validate(&alter("orders", vec![set(" ", Some("1"))]), &f))
                .contains("empty name")
        );
        // Без списка ключей (факты не собраны) имена не проверяются.
        let mut no_keys = facts();
        no_keys.config_keys = None;
        assert_eq!(
            validate(&alter("orders", vec![set("nope", Some("1"))]), &no_keys),
            Ok(())
        );
    }

    #[test]
    fn create_topic_is_checked_against_name_rules_brokers_and_existing_topics() {
        let f = facts();
        assert_eq!(validate(&create("orders-dlq", 3), &f), Ok(()));
        assert_eq!(validate(&create("orders-dlq", -1), &f), Ok(()));
        assert!(invalid(validate(&create("orders", 3), &f)).contains("already exists"));
        assert!(invalid(validate(&create("bad name", 3), &f)).contains("Invalid character"));
        assert!(
            invalid(validate(&create("orders-dlq", 4), &f))
                .contains("above the number of brokers (3)")
        );
        assert!(invalid(validate(&create("orders-dlq", 0), &f)).contains("at least 1"));
        assert!(invalid(validate(&create("__internal", 1), &f)).contains("internal"));
        let mut many = create("dlq", 1);
        if let Change::CreateTopic { configs, .. } = &mut many {
            *configs = (0..51)
                .map(|i| (format!("k{i}"), "1".to_string()))
                .collect();
        }
        assert!(invalid(validate(&many, &f)).contains("at most 50"));
        let mut empty_value = create("dlq", 1);
        if let Change::CreateTopic { configs, .. } = &mut empty_value {
            configs.insert("retention.ms".into(), " ".into());
        }
        assert!(invalid(validate(&empty_value, &f)).contains("must not be empty"));
    }

    #[test]
    fn add_partitions_needs_an_existing_topic_and_a_greater_total() {
        let f = facts();
        let add = |topic: &str, n: i32| Change::AddPartitions {
            topic: topic.into(),
            new_total: n,
        };
        assert_eq!(validate(&add("orders", 4), &f), Ok(()));
        assert!(invalid(validate(&add("orders", 3), &f)).contains("has 3 partitions"));
        assert!(invalid(validate(&add("orders", 0), &f)).contains("has 3 partitions"));
        assert!(not_found(validate(&add("missing", 4), &f)).contains("missing"));
        assert!(invalid(validate(&add("__consumer_offsets", 60), &f)).contains("internal"));
    }

    #[test]
    fn reset_offsets_is_checked_against_groups_topics_and_partitions() {
        let f = facts();
        assert_eq!(validate(&reset("billing", ResetScope::All), &f), Ok(()));
        assert!(
            not_found(validate(&reset("nope", ResetScope::All), &f))
                .contains("consumer group nope")
        );
        assert!(
            not_found(validate(
                &reset(
                    "billing",
                    ResetScope::Topics {
                        topics: vec!["missing".into()]
                    }
                ),
                &f
            ))
            .contains("topic missing")
        );
        assert!(
            invalid(validate(
                &reset("billing", ResetScope::Topics { topics: vec![] }),
                &f
            ))
            .contains("No topics")
        );
        let part = |p: i32| ResetScope::Partitions {
            partitions: vec![TopicPartitionRef {
                topic: "orders".into(),
                partition: p,
            }],
        };
        assert_eq!(validate(&reset("billing", part(2)), &f), Ok(()));
        assert!(
            invalid(validate(&reset("billing", part(3)), &f))
                .contains("partition 3 does not exist")
        );
        assert!(invalid(validate(&reset("billing", part(-1)), &f)).contains("does not exist"));
        let too_many = ResetScope::Topics {
            topics: (0..101).map(|_| "orders".to_string()).collect(),
        };
        assert!(invalid(validate(&reset("billing", too_many), &f)).contains("at most 100"));
        let negative = Change::ResetOffsets {
            group_id: "billing".into(),
            strategy: ResetStrategy::Offset { offset: -1 },
            scope: ResetScope::All,
        };
        assert!(invalid(validate(&negative, &f)).contains("negative"));
        // Пустая группа не обязана быть пустой: форма сама не даст применить.
        let mut no_groups = facts();
        no_groups.groups = None;
        assert_eq!(
            validate(&reset("anything", ResetScope::All), &no_groups),
            Ok(())
        );
    }

    #[test]
    fn applied_matches_only_the_same_kind_and_object() {
        let c = alter("orders", vec![set("retention.ms", Some("1"))]);
        let applied = Applied::AlterTopicConfigs {
            topic: "orders".into(),
            changes: vec![],
        };
        assert!(applied.matches(&c));
        assert!(!applied.matches(&alter("other", vec![])));
        assert!(!applied.matches(&Change::AddPartitions {
            topic: "orders".into(),
            new_total: 4
        }));
        assert!(
            Applied::CreateTopic(NewTopicSpec {
                name: "x".into(),
                partitions: 1,
                replication_factor: 1,
                configs: BTreeMap::new()
            })
            .matches(&create("dlq", 1))
        );
        assert!(Applied::ResetOffsets(None).matches(&reset("billing", ResetScope::All)));
    }

    #[test]
    fn applied_as_proposed_compares_sets_fields_and_requests() {
        let c = alter(
            "orders",
            vec![
                set("min.insync.replicas", Some("2")),
                set("retention.ms", None),
            ],
        );
        let same = Applied::AlterTopicConfigs {
            topic: "orders".into(),
            changes: vec![
                ConfigChange::Delete {
                    name: "retention.ms".into(),
                },
                ConfigChange::Set {
                    name: "min.insync.replicas".into(),
                    value: "2".into(),
                },
            ],
        };
        assert!(applied_as_proposed(&c, &same));
        let edited = Applied::AlterTopicConfigs {
            topic: "orders".into(),
            changes: vec![ConfigChange::Set {
                name: "min.insync.replicas".into(),
                value: "3".into(),
            }],
        };
        assert!(!applied_as_proposed(&c, &edited));

        let spec = |rf: i32| {
            Applied::CreateTopic(NewTopicSpec {
                name: "dlq".into(),
                partitions: 6,
                replication_factor: rf,
                configs: BTreeMap::from([("cleanup.policy".to_string(), "compact".to_string())]),
            })
        };
        assert!(applied_as_proposed(&create("dlq", -1), &spec(-1)));
        // Форма подставила число брокеров вместо «-1» — это правка.
        assert!(!applied_as_proposed(&create("dlq", -1), &spec(3)));

        let add = Change::AddPartitions {
            topic: "orders".into(),
            new_total: 6,
        };
        assert!(applied_as_proposed(
            &add,
            &Applied::AddPartitions {
                topic: "orders".into(),
                new_total: 6
            }
        ));
        assert!(!applied_as_proposed(
            &add,
            &Applied::AddPartitions {
                topic: "orders".into(),
                new_total: 8
            }
        ));

        let r = reset("billing", ResetScope::All);
        let request = |strategy| ResetRequest {
            group_id: "billing".into(),
            strategy,
            scope: ResetScope::All,
        };
        assert!(applied_as_proposed(
            &r,
            &Applied::ResetOffsets(Some(request(ResetStrategy::Earliest)))
        ));
        assert!(!applied_as_proposed(
            &r,
            &Applied::ResetOffsets(Some(request(ResetStrategy::Latest)))
        ));
        assert!(!applied_as_proposed(&r, &Applied::ResetOffsets(None)));
        // Разные виды — никогда.
        assert!(!applied_as_proposed(&r, &same));
    }
}
