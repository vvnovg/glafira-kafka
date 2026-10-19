//! Каталог правил и их строгость по окружению (спец. G2 §4). Правила — чистые функции над
//! `ClusterSnapshot`: сеть и файлы здесь не трогаются.

mod brokers;

use serde_json::Value;

use super::model::{Finding, Levels, Severity, Target};
use super::snapshot::ClusterSnapshot;
use crate::storage::connections::Environment;

const C: Option<Severity> = Some(Severity::Critical);
const W: Option<Severity> = Some(Severity::Warning);
const I: Option<Severity> = Some(Severity::Info);
const N: Option<Severity> = None;

pub struct RuleDef {
    pub id: &'static str,
    pub levels: Levels,
}

const fn rule(
    id: &'static str,
    prod: Option<Severity>,
    stage: Option<Severity>,
    other: Option<Severity>,
) -> RuleDef {
    RuleDef {
        id,
        levels: Levels::new(prod, stage, other),
    }
}

/// Таблица §4 спецификации: prod, stage, остальные окружения.
pub const RULES: &[RuleDef] = &[
    rule("cluster.noController", C, C, C),
    rule("partition.offline", C, C, C),
    rule("partition.underReplicated", C, W, I),
    rule("cluster.singleBroker", C, W, N),
    rule("broker.leaderSkew", W, W, N),
    rule("partition.notPreferredLeader", I, I, N),
    rule("topic.replicationFactorOne", C, W, N),
    rule("topic.minIsrUnreachable", C, C, W),
    rule("topic.minIsrEqualsRf", W, W, N),
    rule("topic.minIsrOne", W, I, N),
    rule("topic.uncleanElection", W, I, N),
    rule("broker.autoCreateTopics", W, I, N),
    rule("broker.defaultReplicationFactorOne", W, I, N),
    rule("broker.defaultMinIsrOne", W, I, N),
    rule("broker.configDrift", W, W, I),
    rule("group.abandoned", W, W, I),
    rule("group.rebalancing", I, I, I),
    rule("group.idleMembers", I, I, I),
    rule("group.lagGrowing", W, W, I),
    rule("check.partial", I, I, I),
];

pub fn rule_def(id: &str) -> Option<&'static RuleDef> {
    RULES.iter().find(|r| r.id == id)
}

/// Идентификаторы всех правил (для `relatedRuleId`, словарей и MCP).
pub fn rule_ids() -> impl Iterator<Item = &'static str> {
    RULES.iter().map(|r| r.id)
}

/// Накопитель находок: строгость берётся из таблицы по окружению, «—» отбрасывается.
pub struct Findings {
    env: Environment,
    items: Vec<Finding>,
}

impl Findings {
    pub fn new(env: Environment) -> Self {
        Self {
            env,
            items: Vec::new(),
        }
    }

    pub fn emit(
        &mut self,
        rule_id: &'static str,
        target: Target,
        qualifier: Option<&str>,
        params: Value,
    ) {
        let def = rule_def(rule_id);
        debug_assert!(def.is_some(), "unknown rule {rule_id}");
        if let Some(severity) = def.and_then(|d| d.levels.for_env(self.env)) {
            self.items
                .push(Finding::new(rule_id, severity, target, qualifier, params));
        }
    }

    /// Сначала строгие, затем по порядку правил в таблице и по объекту.
    pub fn into_sorted(mut self) -> Vec<Finding> {
        let order = |id: &str| RULES.iter().position(|r| r.id == id).unwrap_or(usize::MAX);
        self.items.sort_by(|a, b| {
            b.severity
                .cmp(&a.severity)
                .then_with(|| order(&a.rule_id).cmp(&order(&b.rule_id)))
                .then_with(|| a.target.cmp(&b.target))
                .then_with(|| a.key.cmp(&b.key))
        });
        self.items
    }
}

/// Все правила над снимком.
pub fn evaluate(s: &ClusterSnapshot, env: Environment) -> Vec<Finding> {
    let mut out = Findings::new(env);
    brokers::check(s, &mut out);
    out.into_sorted()
}

#[cfg(test)]
pub(crate) mod fixtures {
    use crate::kafka::metadata::{BrokerInfo, PartitionMeta, TopicMeta};

    pub fn brokers(n: i32) -> Vec<BrokerInfo> {
        (1..=n)
            .map(|id| BrokerInfo {
                id,
                host: format!("b{id}"),
                port: 9092,
            })
            .collect()
    }

    /// Топик: партиции `(лидер, реплики, isr)`.
    pub fn topic(name: &str, parts: &[(i32, &[i32], &[i32])]) -> TopicMeta {
        TopicMeta {
            name: name.into(),
            partitions: parts
                .iter()
                .enumerate()
                .map(|(i, (leader, replicas, isr))| PartitionMeta {
                    id: i as i32,
                    leader: *leader,
                    replicas: replicas.to_vec(),
                    isr: isr.to_vec(),
                })
                .collect(),
        }
    }

    pub const ENVS: [crate::storage::connections::Environment; 5] = {
        use crate::storage::connections::Environment::*;
        [Prod, Stage, Test, Dev, Local]
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rule_ids_are_unique() {
        let mut ids: Vec<_> = rule_ids().collect();
        let n = ids.len();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), n);
        assert_eq!(n, 20);
    }

    #[test]
    fn severity_matrix_matches_spec() {
        // Строки таблицы §4: правило, prod, stage, прочие («К», «П», «И», «—»).
        let spec = [
            ("cluster.noController", "ККК"),
            ("partition.offline", "ККК"),
            ("partition.underReplicated", "КПИ"),
            ("cluster.singleBroker", "КП—"),
            ("broker.leaderSkew", "ПП—"),
            ("partition.notPreferredLeader", "ИИ—"),
            ("topic.replicationFactorOne", "КП—"),
            ("topic.minIsrUnreachable", "ККП"),
            ("topic.minIsrEqualsRf", "ПП—"),
            ("topic.minIsrOne", "ПИ—"),
            ("topic.uncleanElection", "ПИ—"),
            ("broker.autoCreateTopics", "ПИ—"),
            ("broker.defaultReplicationFactorOne", "ПИ—"),
            ("broker.defaultMinIsrOne", "ПИ—"),
            ("broker.configDrift", "ППИ"),
            ("group.abandoned", "ППИ"),
            ("group.rebalancing", "ИИИ"),
            ("group.idleMembers", "ИИИ"),
            ("group.lagGrowing", "ППИ"),
            ("check.partial", "ИИИ"),
        ];
        let sym = |s: Option<Severity>| match s {
            Some(Severity::Critical) => 'К',
            Some(Severity::Warning) => 'П',
            Some(Severity::Info) => 'И',
            None => '—',
        };
        assert_eq!(spec.len(), RULES.len());
        for (id, row) in spec {
            let l = rule_def(id).unwrap().levels;
            let got: String = [l.prod, l.stage, l.other].into_iter().map(sym).collect();
            assert_eq!(got, row, "{id}");
        }
    }

    #[test]
    fn findings_drop_hidden_levels_and_sort_by_severity() {
        let mut f = Findings::new(Environment::Local);
        f.emit("cluster.singleBroker", Target::cluster(), None, Value::Null);
        f.emit(
            "partition.underReplicated",
            Target::topic("a"),
            None,
            Value::Null,
        );
        f.emit("cluster.noController", Target::cluster(), None, Value::Null);
        let v = f.into_sorted();
        let ids: Vec<_> = v.iter().map(|x| x.rule_id.as_str()).collect();
        assert_eq!(ids, ["cluster.noController", "partition.underReplicated"]);
        assert_eq!(v[1].severity, Severity::Info);
    }
}
