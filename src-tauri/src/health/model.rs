//! Находки проверки: строгость, объект, ключ (спец. G2 §4).

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::storage::connections::Environment;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TargetKind {
    Cluster,
    Broker,
    Topic,
    Group,
}

impl TargetKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cluster => "cluster",
            Self::Broker => "broker",
            Self::Topic => "topic",
            Self::Group => "group",
        }
    }
}

/// Объект находки или замечания: у кластера имени нет, у брокера имя — его id.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Target {
    pub kind: TargetKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl Target {
    pub fn cluster() -> Self {
        Self {
            kind: TargetKind::Cluster,
            name: None,
        }
    }

    pub fn broker(id: i32) -> Self {
        Self {
            kind: TargetKind::Broker,
            name: Some(id.to_string()),
        }
    }

    pub fn topic(name: &str) -> Self {
        Self {
            kind: TargetKind::Topic,
            name: Some(name.to_string()),
        }
    }

    pub fn group(id: &str) -> Self {
        Self {
            kind: TargetKind::Group,
            name: Some(id.to_string()),
        }
    }
}

/// Строгость правила по окружению подключения: prod, stage и остальные (test, dev, local).
/// `None` — в этом окружении правило не срабатывает.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Levels {
    pub prod: Option<Severity>,
    pub stage: Option<Severity>,
    pub other: Option<Severity>,
}

impl Levels {
    pub const fn new(
        prod: Option<Severity>,
        stage: Option<Severity>,
        other: Option<Severity>,
    ) -> Self {
        Self { prod, stage, other }
    }

    pub fn for_env(&self, env: Environment) -> Option<Severity> {
        match env {
            Environment::Prod => self.prod,
            Environment::Stage => self.stage,
            Environment::Test | Environment::Dev | Environment::Local => self.other,
        }
    }
}

/// Находка правила. `key` — правило, объект и уточнение (ключ конфига у `broker.configDrift`):
/// по нему работают скрытие и значки.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Finding {
    pub key: String,
    pub rule_id: String,
    pub severity: Severity,
    pub target: Target,
    #[serde(default)]
    pub params: Map<String, Value>,
}

pub fn finding_key(rule_id: &str, target: &Target, qualifier: Option<&str>) -> String {
    let mut key = format!(
        "{rule_id}:{}:{}",
        target.kind.as_str(),
        target.name.as_deref().unwrap_or("")
    );
    if let Some(q) = qualifier {
        key.push(':');
        key.push_str(q);
    }
    key
}

impl Finding {
    pub fn new(
        rule_id: &str,
        severity: Severity,
        target: Target,
        qualifier: Option<&str>,
        params: Value,
    ) -> Self {
        Self {
            key: finding_key(rule_id, &target, qualifier),
            rule_id: rule_id.to_string(),
            severity,
            target,
            params: match params {
                Value::Object(m) => m,
                _ => Map::new(),
            },
        }
    }

    pub fn param_i64(&self, name: &str) -> Option<i64> {
        self.params.get(name).and_then(Value::as_i64)
    }

    pub fn param_str(&self, name: &str) -> Option<&str> {
        self.params.get(name).and_then(Value::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_has_rule_target_and_qualifier() {
        assert_eq!(
            finding_key("topic.replicationFactorOne", &Target::topic("orders"), None),
            "topic.replicationFactorOne:topic:orders"
        );
        assert_eq!(
            finding_key(
                "broker.configDrift",
                &Target::cluster(),
                Some("num.partitions")
            ),
            "broker.configDrift:cluster::num.partitions"
        );
    }

    #[test]
    fn target_serializes_without_name_for_cluster() {
        assert_eq!(
            serde_json::to_value(Target::cluster()).unwrap(),
            serde_json::json!({ "kind": "cluster" })
        );
        assert_eq!(
            serde_json::to_value(Target::broker(2)).unwrap(),
            serde_json::json!({ "kind": "broker", "name": "2" })
        );
    }

    #[test]
    fn levels_map_environments() {
        let l = Levels::new(Some(Severity::Critical), Some(Severity::Warning), None);
        assert_eq!(l.for_env(Environment::Prod), Some(Severity::Critical));
        assert_eq!(l.for_env(Environment::Stage), Some(Severity::Warning));
        for env in [Environment::Test, Environment::Dev, Environment::Local] {
            assert_eq!(l.for_env(env), None);
        }
    }
}
