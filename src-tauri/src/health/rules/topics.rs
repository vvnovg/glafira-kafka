//! Надёжность топиков: RF = 1, `min.insync.replicas` против RF, unclean election.

use serde_json::json;

use super::Findings;
use crate::health::model::Target;
use crate::health::snapshot::{ClusterSnapshot, ConfigValue};

fn int(c: Option<&ConfigValue>) -> Option<i64> {
    c.and_then(|v| v.value.as_deref())
        .and_then(|v| v.trim().parse().ok())
}

pub(super) fn check(s: &ClusterSnapshot, out: &mut Findings) {
    for t in &s.topics {
        let rf = t.replication_factor() as i64;
        if rf == 1 {
            out.emit(
                "topic.replicationFactorOne",
                Target::topic(&t.name),
                None,
                json!({ "replicationFactor": rf }),
            );
        }
        // Конфиги не прочитаны — правила конфигов по топику не срабатывают.
        let Some(cfg) = s.topic_configs.get(&t.name) else {
            continue;
        };
        if let Some(c) = cfg.get("min.insync.replicas")
            && let Some(min_isr) = int(Some(c))
        {
            let params = json!({ "minIsr": min_isr, "replicationFactor": rf, "source": c.source });
            if rf > 0 && min_isr > rf {
                out.emit(
                    "topic.minIsrUnreachable",
                    Target::topic(&t.name),
                    None,
                    params,
                );
            } else if rf > 1 && min_isr == rf {
                out.emit("topic.minIsrEqualsRf", Target::topic(&t.name), None, params);
            } else if min_isr == 1 && rf >= 3 {
                out.emit("topic.minIsrOne", Target::topic(&t.name), None, params);
            }
        }
        if let Some(c) = cfg.get("unclean.leader.election.enable")
            && c.value.as_deref() == Some("true")
        {
            out.emit(
                "topic.uncleanElection",
                Target::topic(&t.name),
                None,
                json!({ "value": c.value, "source": c.source }),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::evaluate;
    use super::super::fixtures::*;
    use crate::health::model::Severity;
    use crate::health::snapshot::{ClusterSnapshot, ConfigMap, ConfigValue};
    use crate::storage::connections::Environment;

    fn cfg(pairs: &[(&str, &str, &str)]) -> ConfigMap {
        pairs
            .iter()
            .map(|(k, v, src)| {
                (
                    k.to_string(),
                    ConfigValue {
                        value: Some(v.to_string()),
                        source: src.to_string(),
                    },
                )
            })
            .collect()
    }

    /// Топик с одной партицией и RF = `rf` на трёх брокерах.
    fn snap(rf: usize, config: Option<ConfigMap>) -> ClusterSnapshot {
        let replicas: Vec<i32> = (1..=rf as i32).collect();
        let t = topic("orders", &[(1, &replicas, &replicas)]);
        let mut topic_configs = BTreeMap::new();
        if let Some(c) = config {
            topic_configs.insert("orders".to_string(), c);
        }
        ClusterSnapshot {
            controller_id: Some(1),
            brokers: brokers(3),
            topics: vec![t],
            topic_configs,
            ..Default::default()
        }
    }

    fn rules(s: &ClusterSnapshot, env: Environment) -> Vec<(String, Severity)> {
        evaluate(s, env)
            .into_iter()
            .map(|f| (f.rule_id, f.severity))
            .collect()
    }

    #[test]
    fn replication_factor_one_by_environment() {
        let s = snap(1, None);
        assert_eq!(
            rules(&s, Environment::Prod),
            [("topic.replicationFactorOne".into(), Severity::Critical)]
        );
        assert_eq!(
            rules(&s, Environment::Stage),
            [("topic.replicationFactorOne".into(), Severity::Warning)]
        );
        for env in [Environment::Test, Environment::Dev, Environment::Local] {
            assert!(rules(&s, env).is_empty());
        }
    }

    #[test]
    fn min_isr_above_rf_is_unreachable() {
        let s = snap(
            2,
            Some(cfg(&[("min.insync.replicas", "3", "DynamicTopic")])),
        );
        let f = evaluate(&s, Environment::Dev);
        assert_eq!(f[0].rule_id, "topic.minIsrUnreachable");
        assert_eq!(f[0].severity, Severity::Warning);
        assert_eq!(f[0].params["source"], "DynamicTopic");
        assert_eq!(f[0].params["minIsr"], 3);
        assert_eq!(f[0].params["replicationFactor"], 2);
    }

    #[test]
    fn min_isr_equal_to_rf_and_one_with_rf_three() {
        let s = snap(
            3,
            Some(cfg(&[("min.insync.replicas", "3", "DynamicTopic")])),
        );
        assert_eq!(
            rules(&s, Environment::Prod),
            [("topic.minIsrEqualsRf".into(), Severity::Warning)]
        );
        let s = snap(3, Some(cfg(&[("min.insync.replicas", "1", "Default")])));
        assert_eq!(
            rules(&s, Environment::Prod),
            [("topic.minIsrOne".into(), Severity::Warning)]
        );
        assert_eq!(
            rules(&s, Environment::Stage),
            [("topic.minIsrOne".into(), Severity::Info)]
        );
        // RF = 1 и min.isr = 1 — это RF = 1, а не minIsr.
        let s = snap(1, Some(cfg(&[("min.insync.replicas", "1", "Default")])));
        assert_eq!(
            rules(&s, Environment::Prod),
            [("topic.replicationFactorOne".into(), Severity::Critical)]
        );
        // Нормальная конфигурация: RF 3, min.isr 2.
        let s = snap(3, Some(cfg(&[("min.insync.replicas", "2", "Default")])));
        assert!(rules(&s, Environment::Prod).is_empty());
    }

    #[test]
    fn unclean_election_true() {
        let s = snap(
            3,
            Some(cfg(&[(
                "unclean.leader.election.enable",
                "true",
                "DynamicTopic",
            )])),
        );
        assert_eq!(
            rules(&s, Environment::Prod),
            [("topic.uncleanElection".into(), Severity::Warning)]
        );
        let f = evaluate(&s, Environment::Prod);
        assert_eq!(f[0].params["value"], "true");
        assert_eq!(f[0].params["source"], "DynamicTopic");
        let s = snap(
            3,
            Some(cfg(&[(
                "unclean.leader.election.enable",
                "false",
                "Default",
            )])),
        );
        assert!(rules(&s, Environment::Prod).is_empty());
    }

    #[test]
    fn unread_configs_give_no_config_findings() {
        let s = snap(2, None);
        assert!(rules(&s, Environment::Prod).is_empty());
    }
}
