use std::ffi::{CStr, c_void};

use rdkafka::consumer::Consumer;
use serde::Serialize;

use crate::error::AppResult;
use crate::kafka::errors::fetch_metadata_fast_fail;
use crate::kafka::metadata::{BrokerInfo, TopicMeta, convert};
use crate::kafka::pool::KafkaHandles;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterOverview {
    pub cluster_id: Option<String>,
    pub controller_id: Option<i32>,
    pub brokers: Vec<BrokerInfo>,
    pub topic_count: usize,
    pub internal_topic_count: usize,
    pub partition_count: usize,
    pub under_replicated_partitions: usize,
    pub offline_partitions: usize,
}

pub fn summarize_cluster(
    mut brokers: Vec<BrokerInfo>,
    topics: &[TopicMeta],
    cluster_id: Option<String>,
    controller_id: Option<i32>,
) -> ClusterOverview {
    brokers.sort_by_key(|b| b.id);
    let partitions = topics.iter().flat_map(|t| t.partitions.iter());
    ClusterOverview {
        cluster_id,
        controller_id,
        brokers,
        topic_count: topics.len(),
        internal_topic_count: topics.iter().filter(|t| t.internal()).count(),
        partition_count: partitions.clone().count(),
        under_replicated_partitions: partitions.clone().filter(|p| p.under_replicated()).count(),
        offline_partitions: partitions.filter(|p| p.offline()).count(),
    }
}

/// Метаданные кластера одним запросом: брокеры, все топики с партициями, id и контроллер.
#[derive(Debug, Clone, Default)]
pub struct ClusterMetadata {
    pub cluster_id: Option<String>,
    pub controller_id: Option<i32>,
    pub brokers: Vec<BrokerInfo>,
    pub topics: Vec<TopicMeta>,
}

pub fn cluster_metadata(h: &KafkaHandles) -> AppResult<ClusterMetadata> {
    let md = fetch_metadata_fast_fail(&h.consumer, None, h.timeout)?;
    let (brokers, topics) = convert(&md);
    let (cluster_id, controller_id) = native_cluster_info(h);
    Ok(ClusterMetadata {
        cluster_id,
        controller_id,
        brokers,
        topics,
    })
}

pub fn cluster_overview(h: &KafkaHandles) -> AppResult<ClusterOverview> {
    let m = cluster_metadata(h)?;
    Ok(summarize_cluster(
        m.brokers,
        &m.topics,
        m.cluster_id,
        m.controller_id,
    ))
}

/// rust-rdkafka не оборачивает rd_kafka_clusterid/rd_kafka_controllerid — вызываем C API.
/// Если в вашей версии rdkafka нет реэкспорта `rdkafka::bindings`, добавьте в Cargo.toml
/// `rdkafka-sys` той же версии, что в Cargo.lock, и замените путь на `rdkafka_sys::bindings`.
fn native_cluster_info(h: &KafkaHandles) -> (Option<String>, Option<i32>) {
    use rdkafka::bindings as rd;
    let rk = h.consumer.client().native_ptr();
    let ms = h.timeout.as_millis().min(i32::MAX as u128) as i32;
    unsafe {
        let ptr = rd::rd_kafka_clusterid(rk, ms);
        let cluster_id = if ptr.is_null() {
            None
        } else {
            let s = CStr::from_ptr(ptr).to_string_lossy().into_owned();
            rd::rd_kafka_mem_free(rk, ptr as *mut c_void);
            Some(s)
        };
        // Без повтора: сразу после fetch_metadata librdkafka вернула бы тот же закэшированный `-1`.
        let controller = rd::rd_kafka_controllerid(rk, ms);
        (cluster_id, (controller >= 0).then_some(controller))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::metadata::{PartitionMeta, TopicMeta};

    fn part(id: i32, leader: i32, replicas: &[i32], isr: &[i32]) -> PartitionMeta {
        PartitionMeta {
            id,
            leader,
            replicas: replicas.to_vec(),
            isr: isr.to_vec(),
        }
    }

    #[test]
    fn summarizes_partitions_and_internal_topics() {
        let topics = vec![
            TopicMeta {
                name: "orders".into(),
                partitions: vec![part(0, 1, &[1, 2], &[1, 2]), part(1, 2, &[1, 2], &[2])],
            },
            TopicMeta {
                name: "__consumer_offsets".into(),
                partitions: vec![part(0, -1, &[1], &[])],
            },
        ];
        let brokers = vec![BrokerInfo {
            id: 1,
            host: "a".into(),
            port: 9092,
        }];
        let o = summarize_cluster(brokers, &topics, Some("cid".into()), Some(1));
        assert_eq!(o.topic_count, 2);
        assert_eq!(o.internal_topic_count, 1);
        assert_eq!(o.partition_count, 3);
        assert_eq!(o.under_replicated_partitions, 2); // orders/1 и __consumer_offsets/0
        assert_eq!(o.offline_partitions, 1);
        assert_eq!(o.cluster_id.as_deref(), Some("cid"));
    }

    #[test]
    fn replication_factor_and_internal_detection() {
        let t = TopicMeta {
            name: "_schemas".into(),
            partitions: vec![part(0, 1, &[1, 2, 3], &[1, 2, 3])],
        };
        assert!(t.internal());
        assert_eq!(t.replication_factor(), 3);
        assert!(!crate::kafka::metadata::is_internal("orders"));
        assert!(crate::kafka::metadata::is_internal("_confluent-metrics"));
    }
}
