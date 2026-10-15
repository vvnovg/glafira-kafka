mod common;

use kafka_explorer_lib::kafka::cluster::cluster_overview;
use kafka_explorer_lib::kafka::configs::describe_configs;
use kafka_explorer_lib::kafka::pool::KafkaHandles;
use kafka_explorer_lib::secrets::MemoryStore;
use rdkafka::admin::ResourceSpecifier;

#[tokio::test(flavor = "multi_thread")]
async fn overview_and_broker_config() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let _topic = common::TempTopic::create(&bs, "it-cluster", 2).await;

    let h = KafkaHandles::create(&common::plaintext_profile(&bs), &MemoryStore::default()).unwrap();
    let h = std::sync::Arc::new(h);
    let h2 = h.clone();
    let o = tokio::task::spawn_blocking(move || cluster_overview(&h2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(o.brokers.len(), 1);
    assert!(o.cluster_id.is_some());
    assert_eq!(o.controller_id, Some(1));
    assert!(o.partition_count >= 2);

    let entries = describe_configs(&h, ResourceSpecifier::Broker(1))
        .await
        .unwrap();
    assert!(entries.iter().any(|e| e.name == "log.retention.hours"));
}
