use rdkafka::metadata::Metadata;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionMeta {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

impl PartitionMeta {
    pub fn under_replicated(&self) -> bool {
        self.isr.len() < self.replicas.len()
    }

    pub fn offline(&self) -> bool {
        self.leader < 0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicMeta {
    pub name: String,
    pub partitions: Vec<PartitionMeta>,
}

impl TopicMeta {
    pub fn internal(&self) -> bool {
        is_internal(&self.name)
    }

    pub fn replication_factor(&self) -> usize {
        self.partitions.iter().map(|p| p.replicas.len()).max().unwrap_or(0)
    }
}

pub fn is_internal(name: &str) -> bool {
    name.starts_with("__") || name.starts_with("_confluent") || name == "_schemas"
}

pub fn convert(md: &Metadata) -> (Vec<BrokerInfo>, Vec<TopicMeta>) {
    let brokers = md
        .brokers()
        .iter()
        .map(|b| BrokerInfo { id: b.id(), host: b.host().to_string(), port: b.port() })
        .collect();
    let topics = md
        .topics()
        .iter()
        .map(|t| TopicMeta {
            name: t.name().to_string(),
            partitions: t
                .partitions()
                .iter()
                .map(|p| PartitionMeta {
                    id: p.id(),
                    leader: p.leader(),
                    replicas: p.replicas().to_vec(),
                    isr: p.isr().to_vec(),
                })
                .collect(),
        })
        .collect();
    (brokers, topics)
}
