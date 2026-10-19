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
                    v.get("value").and_then(Value::as_str).unwrap_or("")
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
            "The cluster reports no controller: metadata changes, leader elections and topic operations will not work until a controller is elected.".into(),
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
                "default.replication.factor = 1 on broker {} in a cluster of {} brokers: topics created without an explicit factor have no replicas.",
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
}
