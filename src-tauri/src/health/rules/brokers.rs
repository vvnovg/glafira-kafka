//! Брокеры и партиции: контроллер, offline и under-replicated партиции, один брокер,
//! перекос лидеров, лидер не на preferred-реплике.

use std::collections::BTreeMap;

use serde_json::json;

use super::Findings;
use crate::health::model::Target;
use crate::health::snapshot::ClusterSnapshot;

/// Сколько номеров партиций и примеров попадает в параметры находки.
const MAX_LISTED: usize = 20;
const MAX_EXAMPLES: usize = 10;
/// `broker.leaderSkew`: среднее лидеров на брокер не меньше этого и отклонение больше 1/5 (20 %).
/// Сравнение — в целых числах: дробное среднее даёт ложное срабатывание ровно на границе.
const SKEW_MIN_AVERAGE: usize = 10;
const SKEW_DENOMINATOR: usize = 5;

pub(super) fn check(s: &ClusterSnapshot, out: &mut Findings) {
    if s.controller_id.is_none() {
        out.emit("cluster.noController", Target::cluster(), None, json!({}));
    }
    if s.brokers.len() == 1 {
        out.emit("cluster.singleBroker", Target::cluster(), None, json!({}));
    }
    for t in &s.topics {
        let offline: Vec<i32> = t
            .partitions
            .iter()
            .filter(|p| p.offline())
            .map(|p| p.id)
            .collect();
        if !offline.is_empty() {
            out.emit(
                "partition.offline",
                Target::topic(&t.name),
                None,
                json!({ "count": offline.len(), "partitions": listed(&offline) }),
            );
        }
        // Offline-партиция не считается ещё и недореплицированной.
        let urp: Vec<i32> = t
            .partitions
            .iter()
            .filter(|p| !p.offline() && p.under_replicated())
            .map(|p| p.id)
            .collect();
        if !urp.is_empty() {
            out.emit(
                "partition.underReplicated",
                Target::topic(&t.name),
                None,
                json!({ "count": urp.len(), "partitions": listed(&urp) }),
            );
        }
    }
    leader_skew(s, out);
    not_preferred(s, out);
}

fn listed(ids: &[i32]) -> Vec<i32> {
    ids.iter().copied().take(MAX_LISTED).collect()
}

fn leader_skew(s: &ClusterSnapshot, out: &mut Findings) {
    if s.brokers.len() < 2 {
        return;
    }
    let mut leaders: BTreeMap<i32, usize> = s.brokers.iter().map(|b| (b.id, 0)).collect();
    for p in s.topics.iter().flat_map(|t| &t.partitions) {
        if let Some(n) = leaders.get_mut(&p.leader) {
            *n += 1;
        }
    }
    let total: usize = leaders.values().sum();
    let brokers = leaders.len();
    // Среднее ≥ 10 ⇔ total ≥ 10 · brokers.
    if total < SKEW_MIN_AVERAGE * brokers {
        return;
    }
    let average = total as f64 / brokers as f64;
    for (id, n) in leaders {
        // |n − total/brokers| > total/brokers / 5 ⇔ 5 · |n · brokers − total| > total.
        if SKEW_DENOMINATOR * (n * brokers).abs_diff(total) > total {
            let deviation = (n as f64 - average) / average;
            out.emit(
                "broker.leaderSkew",
                Target::broker(id),
                None,
                json!({
                    "leaders": n,
                    "average": (average * 10.0).round() / 10.0,
                    "deviationPercent": (deviation * 100.0).round() as i64,
                }),
            );
        }
    }
}

fn not_preferred(s: &ClusterSnapshot, out: &mut Findings) {
    let mut count = 0usize;
    let mut examples = Vec::new();
    for t in &s.topics {
        for p in &t.partitions {
            if p.leader >= 0 && p.replicas.first().is_some_and(|&r| r != p.leader) {
                count += 1;
                if examples.len() < MAX_EXAMPLES {
                    examples.push(format!("{}-{}", t.name, p.id));
                }
            }
        }
    }
    if count > 0 {
        out.emit(
            "partition.notPreferredLeader",
            Target::cluster(),
            None,
            json!({ "count": count, "examples": examples }),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::*;
    use super::super::{Findings, evaluate};
    use crate::health::model::{Severity, TargetKind};
    use crate::health::snapshot::ClusterSnapshot;
    use crate::kafka::metadata::TopicMeta;
    use crate::storage::connections::Environment;

    fn snap(brokers_n: i32, topics: Vec<TopicMeta>) -> ClusterSnapshot {
        ClusterSnapshot {
            controller_id: Some(1),
            brokers: brokers(brokers_n),
            topics,
            ..Default::default()
        }
    }

    fn ids(s: &ClusterSnapshot, env: Environment) -> Vec<String> {
        evaluate(s, env).into_iter().map(|f| f.key).collect()
    }

    #[test]
    fn healthy_cluster_has_no_findings() {
        let s = snap(
            3,
            vec![topic(
                "a",
                &[(1, &[1, 2, 3], &[1, 2, 3]), (2, &[2, 3, 1], &[2, 3, 1])],
            )],
        );
        for env in ENVS {
            assert!(ids(&s, env).is_empty(), "{env:?}");
        }
    }

    #[test]
    fn no_controller_is_critical_everywhere() {
        let mut s = snap(3, vec![]);
        s.controller_id = None;
        for env in ENVS {
            let f = evaluate(&s, env);
            assert_eq!(f[0].rule_id, "cluster.noController");
            assert_eq!(f[0].severity, Severity::Critical);
        }
    }

    #[test]
    fn offline_and_under_replicated_are_reported_per_topic() {
        let s = snap(
            3,
            vec![topic(
                "a",
                &[
                    (-1, &[1, 2], &[]),
                    (2, &[2, 3], &[2]),
                    (3, &[3, 1], &[3, 1]),
                ],
            )],
        );
        let f = evaluate(&s, Environment::Stage);
        let offline = f.iter().find(|x| x.rule_id == "partition.offline").unwrap();
        assert_eq!(offline.target.name.as_deref(), Some("a"));
        assert_eq!(offline.params["partitions"], serde_json::json!([0]));
        let urp = f
            .iter()
            .find(|x| x.rule_id == "partition.underReplicated")
            .unwrap();
        assert_eq!(urp.params["count"], 1);
        assert_eq!(urp.params["partitions"], serde_json::json!([1]));
        assert_eq!(urp.severity, Severity::Warning);
    }

    #[test]
    fn partition_lists_are_capped() {
        let parts: Vec<(i32, &[i32], &[i32])> = (0..30).map(|_| (-1, &[1][..], &[][..])).collect();
        let s = snap(3, vec![topic("a", &parts)]);
        let f = evaluate(&s, Environment::Prod);
        let offline = f.iter().find(|x| x.rule_id == "partition.offline").unwrap();
        assert_eq!(offline.params["count"], 30);
        assert_eq!(offline.params["partitions"].as_array().unwrap().len(), 20);
    }

    #[test]
    fn single_broker_depends_on_environment() {
        let s = snap(1, vec![]);
        assert_eq!(
            evaluate(&s, Environment::Prod)[0].severity,
            Severity::Critical
        );
        assert_eq!(
            evaluate(&s, Environment::Stage)[0].severity,
            Severity::Warning
        );
        assert!(evaluate(&s, Environment::Local).is_empty());
    }

    /// Топик с `n` партициями, лидер у всех — `leader`.
    fn led_by(name: &str, leader: i32, n: usize) -> TopicMeta {
        let parts: Vec<(i32, &[i32], &[i32])> =
            (0..n).map(|_| (leader, &[][..], &[][..])).collect();
        let mut t = topic(name, &parts);
        for p in &mut t.partitions {
            p.replicas = vec![leader];
            p.isr = vec![leader];
        }
        t
    }

    #[test]
    fn leader_skew_needs_average_of_ten_and_more_than_twenty_percent() {
        // Среднее 10: 13 / 10 / 7 — отклонение 30 % у двух брокеров.
        let s = snap(
            3,
            vec![led_by("a", 1, 13), led_by("b", 2, 10), led_by("c", 3, 7)],
        );
        let f: Vec<_> = evaluate(&s, Environment::Prod)
            .into_iter()
            .filter(|x| x.rule_id == "broker.leaderSkew")
            .collect();
        assert_eq!(f.len(), 2);
        assert_eq!(f[0].target.kind, TargetKind::Broker);
        assert_eq!(f[0].params["average"], 10.0);
        // Ровно 20 %: 12 / 10 / 8 — не срабатывает.
        let s = snap(
            3,
            vec![led_by("a", 1, 12), led_by("b", 2, 10), led_by("c", 3, 8)],
        );
        assert!(
            !ids(&s, Environment::Prod)
                .iter()
                .any(|k| k.starts_with("broker.leaderSkew"))
        );
        // Ровно 20 % при дробном среднем: 14 / 11 / 10 (среднее 35/3) — не срабатывает.
        let s = snap(
            3,
            vec![led_by("a", 1, 14), led_by("b", 2, 11), led_by("c", 3, 10)],
        );
        assert!(
            !ids(&s, Environment::Prod)
                .iter()
                .any(|k| k.starts_with("broker.leaderSkew"))
        );
        // Среднее 9: 13 / 9 / 5 — не срабатывает.
        let s = snap(
            3,
            vec![led_by("a", 1, 13), led_by("b", 2, 9), led_by("c", 3, 5)],
        );
        assert!(
            !ids(&s, Environment::Prod)
                .iter()
                .any(|k| k.starts_with("broker.leaderSkew"))
        );
    }

    #[test]
    fn not_preferred_leader_is_one_cluster_finding() {
        let s = snap(
            3,
            vec![
                topic(
                    "a",
                    &[(2, &[1, 2, 3], &[1, 2, 3]), (1, &[1, 2, 3], &[1, 2, 3])],
                ),
                topic("b", &[(3, &[2, 3], &[2, 3])]),
            ],
        );
        let f: Vec<_> = evaluate(&s, Environment::Stage)
            .into_iter()
            .filter(|x| x.rule_id == "partition.notPreferredLeader")
            .collect();
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].params["count"], 2);
        assert_eq!(f[0].params["examples"], serde_json::json!(["a-0", "b-0"]));
        assert!(evaluate(&s, Environment::Dev).is_empty());
    }

    #[test]
    fn emit_uses_table_levels() {
        let mut f = Findings::new(Environment::Test);
        f.emit(
            "partition.offline",
            crate::health::model::Target::topic("x"),
            None,
            serde_json::json!({}),
        );
        assert_eq!(f.into_sorted()[0].severity, Severity::Critical);
    }
}
