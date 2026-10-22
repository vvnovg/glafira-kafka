//! Проверка кластера и замечания для LLM (спец. G2 §6): операции [`HealthOp`] и ответы.
//! Ни одна операция не меняет кластер: проверка только читает его, замечания пишутся в Glafira.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::model::SessionView;
use crate::rules::{Page, page};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum TargetKind {
    Cluster,
    Broker,
    Topic,
    Group,
}

/// Object of a finding or note: `cluster` has no name, a `broker` is named by its id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Target {
    /// cluster, broker, topic or group.
    pub kind: TargetKind,
    /// Broker id, topic name or group id; omit for cluster.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl Target {
    /// Имя обязательно для брокера, топика и группы и не нужно кластеру.
    pub fn check(&self) -> Result<(), String> {
        let named = self.name.as_deref().is_some_and(|n| !n.trim().is_empty());
        match (self.kind, named) {
            (TargetKind::Cluster, _) => Ok(()),
            (_, true) => Ok(()),
            (_, false) => Err("target.name is required for broker, topic and group".into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthFinding {
    pub key: String,
    pub rule_id: String,
    pub severity: Severity,
    pub target: Target,
    pub title: String,
    pub details: String,
    pub params: Map<String, Value>,
    /// Человек скрыл находку для этого объекта.
    pub ignored: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthScope {
    pub brokers: usize,
    pub topics_checked: usize,
    pub topics_total: usize,
    pub groups_checked: usize,
    pub groups_total: usize,
}

/// Проверка целиком (все находки); фильтры и страницы — в инструменте.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthRun {
    pub run_id: String,
    pub started_at_ms: i64,
    pub duration_ms: i64,
    /// `{ "kind": "ui" }` или `{ "kind": "mcp", "client": "…" }`.
    pub trigger: Value,
    pub environment: String,
    pub incomplete: bool,
    /// Использована прошлая проверка (моложе `maxAgeSeconds`).
    pub reused: bool,
    pub scope: HealthScope,
    pub findings: Vec<HealthFinding>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum StatusFilter {
    #[default]
    Open,
    Closed,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnnotationView {
    pub id: String,
    pub target: Target,
    pub severity: Severity,
    pub title: String,
    pub body: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_rule_id: Option<String>,
    /// Клиент, оставивший замечание; `None` — безымянный.
    pub client: Option<String>,
    /// open, resolved (человек: «решено»), dismissed (человек: «не актуально»).
    pub status: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewNote {
    pub target: Target,
    pub severity: Severity,
    pub title: String,
    pub body: String,
    pub related_rule_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthOp {
    /// Последняя проверка, если ей меньше `max_age_ms`, иначе новая; с `run_id` — только эта
    /// проверка (`not_found`, если её уже заменила новая).
    Check {
        connection_id: String,
        max_age_ms: i64,
        run_id: Option<String>,
        session: SessionView,
    },
    Annotate {
        connection_id: String,
        session: SessionView,
        note: NewNote,
    },
    ListAnnotations {
        connection_id: String,
        status: StatusFilter,
    },
    RemoveAnnotation {
        connection_id: String,
        session: SessionView,
        annotation_id: String,
    },
}

impl HealthOp {
    pub fn connection_id(&self) -> &str {
        match self {
            HealthOp::Check { connection_id, .. }
            | HealthOp::Annotate { connection_id, .. }
            | HealthOp::ListAnnotations { connection_id, .. }
            | HealthOp::RemoveAnnotation { connection_id, .. } => connection_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum HealthOutput {
    Run(HealthRun),
    Annotated {
        annotation_id: String,
        created: bool,
    },
    Annotations(Vec<AnnotationView>),
    Removed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuleCount {
    pub rule_id: String,
    pub severity: Severity,
    pub count: usize,
}

/// Фильтры `health_check`.
#[derive(Debug, Clone, Default)]
pub struct FindingFilter {
    pub rule_ids: Option<Vec<String>>,
    pub min_severity: Option<Severity>,
    pub include_ignored: bool,
}

/// Сводка по всем находкам проверки и отфильтрованная страница.
pub fn select(
    findings: Vec<HealthFinding>,
    filter: &FindingFilter,
    offset: usize,
    limit: usize,
) -> (Vec<RuleCount>, Page<HealthFinding>) {
    let mut counts: BTreeMap<(String, Severity), usize> = BTreeMap::new();
    for f in &findings {
        *counts.entry((f.rule_id.clone(), f.severity)).or_default() += 1;
    }
    let mut summary: Vec<RuleCount> = counts
        .into_iter()
        .map(|((rule_id, severity), count)| RuleCount {
            rule_id,
            severity,
            count,
        })
        .collect();
    summary.sort_by(|a, b| {
        b.severity
            .cmp(&a.severity)
            .then_with(|| a.rule_id.cmp(&b.rule_id))
    });
    let kept: Vec<HealthFinding> = findings
        .into_iter()
        .filter(|f| filter.include_ignored || !f.ignored)
        .filter(|f| filter.min_severity.is_none_or(|m| f.severity >= m))
        .filter(|f| {
            filter
                .rule_ids
                .as_ref()
                .is_none_or(|ids| ids.iter().any(|id| id == &f.rule_id))
        })
        .collect();
    (summary, page(kept, offset, limit))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finding(rule: &str, sev: Severity, name: &str, ignored: bool) -> HealthFinding {
        HealthFinding {
            key: format!("{rule}:topic:{name}"),
            rule_id: rule.into(),
            severity: sev,
            target: Target {
                kind: TargetKind::Topic,
                name: Some(name.into()),
            },
            title: "t".into(),
            details: "d".into(),
            params: Map::new(),
            ignored,
        }
    }

    fn sample() -> Vec<HealthFinding> {
        vec![
            finding("topic.replicationFactorOne", Severity::Critical, "a", false),
            finding("topic.replicationFactorOne", Severity::Critical, "b", true),
            finding("group.abandoned", Severity::Warning, "g", false),
            finding("group.rebalancing", Severity::Info, "r", false),
        ]
    }

    #[test]
    fn summary_counts_all_findings() {
        let (summary, page) = select(sample(), &FindingFilter::default(), 0, 100);
        assert_eq!(summary[0].rule_id, "topic.replicationFactorOne");
        assert_eq!(summary[0].count, 2);
        assert_eq!(summary.len(), 3);
        // Без includeIgnored скрытая не попадает в страницу, но считается в сводке.
        assert_eq!(page.total, 3);
    }

    #[test]
    fn filters_and_paging() {
        let f = FindingFilter {
            min_severity: Some(Severity::Warning),
            include_ignored: true,
            ..Default::default()
        };
        let (_, p) = select(sample(), &f, 1, 1);
        assert_eq!(p.total, 3);
        assert_eq!(p.items[0].target.name.as_deref(), Some("b"));
        assert_eq!(p.next_offset, Some(2));
        let f = FindingFilter {
            rule_ids: Some(vec!["group.rebalancing".into()]),
            include_ignored: true,
            ..Default::default()
        };
        let (_, p) = select(sample(), &f, 0, 10);
        assert_eq!(p.items.len(), 1);
    }

    #[test]
    fn target_needs_name_except_cluster() {
        let t = |kind, name: Option<&str>| Target {
            kind,
            name: name.map(String::from),
        };
        assert!(t(TargetKind::Cluster, None).check().is_ok());
        assert!(t(TargetKind::Topic, Some("x")).check().is_ok());
        assert!(t(TargetKind::Group, None).check().is_err());
        assert!(t(TargetKind::Broker, Some(" ")).check().is_err());
    }
}
