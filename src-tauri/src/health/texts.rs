//! Английские тексты находок для LLM (MCP). Интерфейс берёт свои из словарей
//! (`health.rules.<ruleId>.*`), здесь — короткие факты без перевода.

use serde_json::Value;

use super::model::Finding;

fn list(v: Option<&Value>) -> String {
    match v {
        Some(Value::Array(items)) => items
            .iter()
            .map(|x| match x {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .collect::<Vec<_>>()
            .join(", "),
        _ => String::new(),
    }
}

fn num(f: &Finding, name: &str) -> String {
    f.params
        .get(name)
        .map(|v| match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .unwrap_or_default()
}

/// `broker.configDrift`: «1=3, 2=6».
fn drift_values(f: &Finding) -> String {
    match f.params.get("values") {
        Some(Value::Array(items)) => items
            .iter()
            .map(|v| {
                format!(
                    "{}={}",
                    v.get("brokerId").map(Value::to_string).unwrap_or_default(),
                    v.get("value").and_then(Value::as_str).unwrap_or("(unset)")
                )
            })
            .collect::<Vec<_>>()
            .join(", "),
        _ => String::new(),
    }
}

/// Заголовок и пояснение находки.
pub fn english(f: &Finding) -> (String, String) {
    let name = f.target.name.as_deref().unwrap_or("");
    match f.rule_id.as_str() {
        "cluster.noController" => (
            "No active controller".into(),
            match f.param_i64("leaderless") {
                Some(n) => format!(
                    "The cluster reports no controller and {n} partition(s) have no leader: metadata changes, leader elections and topic operations will not work until a controller is elected."
                ),
                None => "The cluster reports no controller: metadata changes, leader elections and topic operations will not work until a controller is elected.".into(),
            },
        ),
        "cluster.controllerUnknown" => (
            "Controller unknown".into(),
            "Glafira could not learn the controller id in time, but every partition has a leader, so the cluster is probably healthy; repeat the check. If the controller stays unknown, check the controller quorum (KRaft) or ZooKeeper.".into(),
        ),
        "partition.offline" => (
            format!("Topic {name} has offline partitions"),
            format!(
                "{} partition(s) have no leader and cannot be read or written: {}.",
                num(f, "count"),
                list(f.params.get("partitions"))
            ),
        ),
        "partition.underReplicated" => (
            format!("Topic {name} has under-replicated partitions"),
            format!(
                "{} partition(s) have fewer in-sync replicas than replicas: {}. A broker may be down or lagging.",
                num(f, "count"),
                list(f.params.get("partitions"))
            ),
        ),
        "cluster.singleBroker" => (
            "Single-broker cluster".into(),
            "The cluster has one broker: any restart or failure makes every partition unavailable.".into(),
        ),
        "broker.leaderSkew" => (
            format!("Broker {name} leads a skewed share of partitions"),
            format!(
                "It leads {} partitions while the average is {} ({:+}%). Load on brokers is uneven; consider a preferred leader election or reassignment.",
                num(f, "leaders"),
                num(f, "average"),
                f.param_i64("deviationPercent").unwrap_or(0)
            ),
        ),
        "partition.notPreferredLeader" => (
            "Partitions led by a non-preferred replica".into(),
            format!(
                "{} partition(s) are led by a replica other than the first (preferred) one, e.g. {}. Usually left after a broker restart; auto.leader.rebalance.enable or a preferred leader election fixes it.",
                num(f, "count"),
                list(f.params.get("examples"))
            ),
        ),
        "topic.replicationFactorOne" => (
            format!("Topic {name} has replication factor 1"),
            "Every partition has a single replica: losing that broker loses the data and makes the partitions unavailable.".into(),
        ),
        "topic.minIsrUnreachable" => (
            format!("Topic {name}: min.insync.replicas is above the replication factor"),
            format!(
                "min.insync.replicas = {} ({}) but the replication factor is {}: producers with acks=all can never write.",
                num(f, "minIsr"),
                num(f, "source"),
                num(f, "replicationFactor")
            ),
        ),
        "topic.minIsrEqualsRf" => (
            format!("Topic {name}: min.insync.replicas equals the replication factor"),
            format!(
                "min.insync.replicas = {} ({}) with replication factor {}: one replica down stops writes with acks=all.",
                num(f, "minIsr"),
                num(f, "source"),
                num(f, "replicationFactor")
            ),
        ),
        "topic.minIsrOne" => (
            format!("Topic {name}: min.insync.replicas is 1"),
            format!(
                "min.insync.replicas = 1 ({}) with replication factor {}: acks=all writes are acknowledged by a single replica, so a broker failure can lose acknowledged data.",
                num(f, "source"),
                num(f, "replicationFactor")
            ),
        ),
        "topic.uncleanElection" => (
            format!("Topic {name} allows unclean leader election"),
            format!(
                "unclean.leader.election.enable = true ({}): an out-of-sync replica can become leader and committed messages can be lost.",
                num(f, "source")
            ),
        ),
        "broker.autoCreateTopics" => (
            "Brokers auto-create topics".into(),
            format!(
                "auto.create.topics.enable = true on broker {}: a typo in a topic name creates a new topic with default settings.",
                num(f, "brokerId")
            ),
        ),
        "broker.defaultReplicationFactorOne" => (
            "Default replication factor is 1".into(),
            format!(
                "default.replication.factor = 1 on broker {} in a cluster of {} brokers: topics created without an explicit factor get a single replica, with no redundancy.",
                num(f, "brokerId"),
                num(f, "brokers")
            ),
        ),
        "broker.defaultMinIsrOne" => (
            "Default min.insync.replicas is 1".into(),
            format!(
                "min.insync.replicas = 1 on broker {} in a cluster of {} brokers: topics without their own value acknowledge acks=all writes with one replica.",
                num(f, "brokerId"),
                num(f, "brokers")
            ),
        ),
        "broker.configDrift" => (
            format!("{} differs between brokers", num(f, "config")),
            format!(
                "Values by broker: {}. Brokers with different settings behave differently after a leader moves.",
                drift_values(f)
            ),
        ),
        "group.abandoned" => (
            format!("Group {name} is empty but has lag"),
            format!(
                "The group has no members and {} unconsumed messages. If the application is retired, the group can be deleted; otherwise its consumers are not running.",
                num(f, "lag")
            ),
        ),
        "group.rebalancing" => (
            format!("Group {name} is rebalancing"),
            format!(
                "State {} at the moment of the check. To see whether rebalances repeat, use observe_consumer_group. Groups on the consumer protocol (KIP-848) report transitional states as Unknown, so this check can miss their rebalances.",
                num(f, "state")
            ),
        ),
        "group.idleMembers" => (
            format!("Group {name} has members without partitions"),
            format!(
                "{} of {} members have no partitions ({} partitions assigned): there are more consumers than partitions they read.",
                num(f, "idle"),
                num(f, "members"),
                num(f, "partitions")
            ),
        ),
        "group.lagGrowing" => (
            format!("Lag of group {name} is growing"),
            format!(
                "Lag grew from {} to {} since the previous check {} min ago: consumers do not keep up with producers.",
                num(f, "previousLag"),
                num(f, "lag"),
                num(f, "minutesAgo")
            ),
        ),
        "check.partial" => (
            "The check is incomplete".into(),
            format!(
                "Topic configs checked: {} of {} ({} errors); broker config errors: {}; groups checked: {} of {} ({} errors, lag not read for {}); time limit reached: {}. Findings cover only what was read.",
                num(f, "topicsChecked"),
                num(f, "topicsTotal"),
                num(f, "topicConfigErrors"),
                num(f, "brokerConfigErrors"),
                num(f, "groupsChecked"),
                num(f, "groupsTotal"),
                num(f, "groupErrors"),
                num(f, "lagErrors"),
                num(f, "timedOut")
            ),
        ),
        other => (other.to_string(), String::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::model::{Severity, Target};

    #[test]
    fn texts_use_params() {
        let f = Finding::new(
            "partition.offline",
            Severity::Critical,
            Target::topic("orders"),
            None,
            serde_json::json!({ "count": 2, "partitions": [0, 3] }),
        );
        let (title, details) = english(&f);
        assert_eq!(title, "Topic orders has offline partitions");
        assert!(details.starts_with("2 partition(s) have no leader"));
        assert!(details.contains("0, 3"));
    }

    #[test]
    fn no_controller_text_with_and_without_leaderless() {
        let with = Finding::new(
            "cluster.noController",
            Severity::Critical,
            Target::cluster(),
            None,
            serde_json::json!({ "leaderless": 2 }),
        );
        assert!(english(&with).1.contains("2 partition(s) have no leader"));
        // Находка, сохранённая до появления параметра: без двойных пробелов и пустого числа.
        let without = Finding::new(
            "cluster.noController",
            Severity::Critical,
            Target::cluster(),
            None,
            serde_json::json!({}),
        );
        let (_, details) = english(&without);
        assert!(details.starts_with("The cluster reports no controller:"));
        assert!(!details.contains("  "));
    }

    #[test]
    fn default_replication_factor_one_means_a_single_replica() {
        let f = Finding::new(
            "broker.defaultReplicationFactorOne",
            Severity::Warning,
            Target::broker(1),
            None,
            serde_json::json!({ "brokerId": 1, "brokers": 3 }),
        );
        let (_, details) = english(&f);
        // RF = 1 — одна реплика без резерва, а не «нет реплик».
        assert!(
            details.ends_with("get a single replica, with no redundancy."),
            "{details}"
        );
    }

    #[test]
    fn every_rule_has_a_text() {
        for id in crate::health::rules::rule_ids() {
            let f = Finding::new(
                id,
                Severity::Info,
                Target::cluster(),
                None,
                serde_json::json!({}),
            );
            let (title, details) = english(&f);
            assert_ne!(title, id, "{id}");
            assert!(!details.is_empty(), "{id}");
        }
    }

    #[test]
    fn partial_text_includes_lag_errors() {
        let f = Finding::new(
            "check.partial",
            Severity::Info,
            Target::cluster(),
            None,
            serde_json::json!({
                "topicsChecked": 5,
                "topicsTotal": 5,
                "topicConfigErrors": 0,
                "brokerConfigErrors": 0,
                "groupsChecked": 3,
                "groupsTotal": 4,
                "groupErrors": 1,
                "lagErrors": 2,
                "timedOut": false,
            }),
        );
        let (_, details) = english(&f);
        assert!(
            details.contains("groups checked: 3 of 4 (1 errors, lag not read for 2)"),
            "{details}"
        );
    }

    #[test]
    fn rebalancing_text_mentions_kip_848() {
        let f = Finding::new(
            "group.rebalancing",
            Severity::Info,
            Target::group("g"),
            None,
            serde_json::json!({ "state": "PreparingRebalance" }),
        );
        let (title, details) = english(&f);
        assert_eq!(title, "Group g is rebalancing");
        assert!(details.contains("PreparingRebalance"));
        assert!(details.contains("KIP-848"));
    }
}
