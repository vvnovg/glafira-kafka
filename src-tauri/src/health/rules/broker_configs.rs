//! Конфиг брокеров: автосоздание топиков, RF и min.isr по умолчанию, расхождение между брокерами.

use std::collections::BTreeMap;

use serde_json::json;

use super::Findings;
use crate::health::model::Target;
use crate::health::snapshot::{ClusterSnapshot, ConfigMap};

/// Ключи, расхождение которых между брокерами — находка `broker.configDrift`.
pub const DRIFT_KEYS: &[&str] = &[
    "default.replication.factor",
    "min.insync.replicas",
    "auto.create.topics.enable",
    "unclean.leader.election.enable",
    "num.partitions",
    "log.retention.ms",
    "log.retention.hours",
    "message.max.bytes",
];

/// Брокер, чьи значения считаются значениями кластера: контроллер, а если его конфиг не
/// прочитан — брокер с меньшим id.
fn reference(s: &ClusterSnapshot) -> Option<(i32, &ConfigMap)> {
    if let Some(id) = s.controller_id
        && let Some(c) = s.broker_configs.get(&id)
    {
        return Some((id, c));
    }
    s.broker_configs.iter().next().map(|(id, c)| (*id, c))
}

fn value<'a>(c: &'a ConfigMap, key: &str) -> Option<&'a str> {
    c.get(key).and_then(|v| v.value.as_deref()).map(str::trim)
}

pub(super) fn check(s: &ClusterSnapshot, out: &mut Findings) {
    if let Some((id, c)) = reference(s) {
        let brokers = s.brokers.len();
        if value(c, "auto.create.topics.enable") == Some("true") {
            out.emit(
                "broker.autoCreateTopics",
                Target::cluster(),
                None,
                json!({ "brokerId": id }),
            );
        }
        if brokers >= 3 && value(c, "default.replication.factor") == Some("1") {
            out.emit(
                "broker.defaultReplicationFactorOne",
                Target::cluster(),
                None,
                json!({ "brokerId": id, "brokers": brokers }),
            );
        }
        if brokers >= 3 && value(c, "min.insync.replicas") == Some("1") {
            out.emit(
                "broker.defaultMinIsrOne",
                Target::cluster(),
                None,
                json!({ "brokerId": id, "brokers": brokers }),
            );
        }
    }
    drift(s, out);
}

fn drift(s: &ClusterSnapshot, out: &mut Findings) {
    if s.broker_configs.len() < 2 {
        return;
    }
    for key in DRIFT_KEYS {
        // Брокер без ключа в ответе не сравнивается. Ключ без значения (null — не задан) — тоже
        // значение: у одного брокера задано, у другого нет — расхождение.
        let values: BTreeMap<i32, Option<&str>> = s
            .broker_configs
            .iter()
            .filter_map(|(id, c)| {
                c.get(*key)
                    .map(|v| (*id, v.value.as_deref().map(str::trim)))
            })
            .collect();
        let mut distinct: Vec<_> = values.values().collect();
        distinct.sort();
        distinct.dedup();
        if distinct.len() > 1 {
            let listed: Vec<_> = values
                .iter()
                .map(|(id, v)| json!({ "brokerId": id, "value": v }))
                .collect();
            out.emit(
                "broker.configDrift",
                Target::cluster(),
                Some(key),
                json!({ "config": key, "values": listed }),
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

    fn cfg(pairs: &[(&str, &str)]) -> ConfigMap {
        pairs
            .iter()
            .map(|(k, v)| {
                (
                    k.to_string(),
                    ConfigValue {
                        value: Some(v.to_string()),
                        source: "StaticBroker".into(),
                    },
                )
            })
            .collect()
    }

    fn snap(n: i32, configs: Vec<(i32, ConfigMap)>) -> ClusterSnapshot {
        ClusterSnapshot {
            controller_id: Some(1),
            brokers: brokers(n),
            broker_configs: configs.into_iter().collect::<BTreeMap<_, _>>(),
            ..Default::default()
        }
    }

    fn rules(s: &ClusterSnapshot, env: Environment) -> Vec<String> {
        evaluate(s, env).into_iter().map(|f| f.rule_id).collect()
    }

    #[test]
    fn defaults_on_prod_with_three_brokers() {
        let c = cfg(&[
            ("auto.create.topics.enable", "true"),
            ("default.replication.factor", "1"),
            ("min.insync.replicas", "1"),
        ]);
        let s = snap(3, vec![(1, c.clone()), (2, c.clone()), (3, c)]);
        let f = evaluate(&s, Environment::Prod);
        assert_eq!(
            f.iter().map(|x| x.rule_id.as_str()).collect::<Vec<_>>(),
            [
                "broker.autoCreateTopics",
                "broker.defaultReplicationFactorOne",
                "broker.defaultMinIsrOne"
            ]
        );
        assert!(f.iter().all(|x| x.severity == Severity::Warning));
        assert_eq!(f[0].params["brokerId"], 1);
        assert!(rules(&s, Environment::Dev).is_empty());
    }

    #[test]
    fn rf_and_min_isr_defaults_need_three_brokers() {
        let c = cfg(&[
            ("default.replication.factor", "1"),
            ("min.insync.replicas", "1"),
        ]);
        let s = snap(2, vec![(1, c.clone()), (2, c)]);
        assert!(rules(&s, Environment::Prod).is_empty());
    }

    #[test]
    fn reference_falls_back_to_lowest_broker() {
        let s = ClusterSnapshot {
            controller_id: Some(3),
            ..snap(3, vec![(2, cfg(&[("auto.create.topics.enable", "true")]))])
        };
        assert_eq!(evaluate(&s, Environment::Prod)[0].params["brokerId"], 2);
    }

    #[test]
    fn drift_is_one_finding_per_key() {
        let s = snap(
            3,
            vec![
                (
                    1,
                    cfg(&[("num.partitions", "3"), ("message.max.bytes", "1048588")]),
                ),
                (
                    2,
                    cfg(&[("num.partitions", "6"), ("message.max.bytes", "1048588")]),
                ),
                (3, cfg(&[("num.partitions", "3")])),
            ],
        );
        let f: Vec<_> = evaluate(&s, Environment::Prod)
            .into_iter()
            .filter(|x| x.rule_id == "broker.configDrift")
            .collect();
        // message.max.bytes у брокера 3 не прочитан — не расхождение.
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].key, "broker.configDrift:cluster::num.partitions");
        assert_eq!(f[0].params["config"], "num.partitions");
        assert_eq!(
            f[0].params["values"][1],
            serde_json::json!({ "brokerId": 2, "value": "6" })
        );
        assert_eq!(evaluate(&s, Environment::Local)[0].severity, Severity::Info);
    }

    #[test]
    fn unset_value_differs_from_set_value() {
        let mut unset = cfg(&[]);
        unset.insert(
            "log.retention.ms".into(),
            ConfigValue {
                value: None,
                source: "Default".into(),
            },
        );
        let s = snap(
            2,
            vec![(1, cfg(&[("log.retention.ms", "86400000")])), (2, unset)],
        );
        let f: Vec<_> = evaluate(&s, Environment::Prod)
            .into_iter()
            .filter(|x| x.rule_id == "broker.configDrift")
            .collect();
        assert_eq!(f.len(), 1);
        assert_eq!(
            f[0].params["values"][1],
            serde_json::json!({ "brokerId": 2, "value": null })
        );
    }

    #[test]
    fn controller_values_are_the_cluster_values() {
        let auto = |s: &ClusterSnapshot| -> Vec<serde_json::Value> {
            evaluate(s, Environment::Prod)
                .into_iter()
                .filter(|x| x.rule_id == "broker.autoCreateTopics")
                .map(|x| x.params["brokerId"].clone())
                .collect()
        };
        let s = ClusterSnapshot {
            controller_id: Some(3),
            ..snap(
                3,
                vec![
                    (1, cfg(&[("auto.create.topics.enable", "false")])),
                    (3, cfg(&[("auto.create.topics.enable", "true")])),
                ],
            )
        };
        assert_eq!(auto(&s), [serde_json::json!(3)]);
        // Включено только у брокера 1 (не контроллера) — не срабатывает.
        let s = ClusterSnapshot {
            controller_id: Some(3),
            ..snap(
                3,
                vec![
                    (1, cfg(&[("auto.create.topics.enable", "true")])),
                    (3, cfg(&[("auto.create.topics.enable", "false")])),
                ],
            )
        };
        assert!(auto(&s).is_empty());
    }
}
