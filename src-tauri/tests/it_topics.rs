mod common;

use std::sync::Arc;

use common::TestMsg;
use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::kafka::configs::describe_configs;
use kafka_explorer_lib::kafka::pool::KafkaHandles;
use kafka_explorer_lib::kafka::topics::{list_topics, topic_details, topic_stats};
use kafka_explorer_lib::secrets::MemoryStore;
use rdkafka::admin::ResourceSpecifier;

#[tokio::test(flavor = "multi_thread")]
async fn list_stats_details_configs() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = common::unique_topic("it-topics");
    common::create_topic(&bs, &topic, 3).await;
    let msgs: Vec<TestMsg> = (0..5).map(|i| TestMsg { partition: i % 3, key: None, value: "v", headers: &[] }).collect();
    common::produce(&bs, &topic, &msgs).await;

    let h = Arc::new(KafkaHandles::create(&common::plaintext_profile(&bs), &MemoryStore::default()).unwrap());
    let (h1, t1) = (h.clone(), topic.clone());
    let (list, stats, details, missing) = tokio::task::spawn_blocking(move || {
        (
            list_topics(&h1).unwrap(),
            topic_stats(&h1, &[t1.clone(), "does-not-exist".into()]).unwrap(),
            topic_details(&h1, &t1).unwrap(),
            topic_details(&h1, "does-not-exist").unwrap_err(),
        )
    })
    .await
    .unwrap();

    let summary = list.iter().find(|t| t.name == topic).expect("топик в списке");
    assert_eq!(summary.partition_count, 3);
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].message_count, 5);
    assert_eq!(details.partitions.len(), 3);
    assert_eq!(details.partitions[0].high, 2); // сообщения 0 и 3
    assert_eq!(details.message_count, 5);
    assert_eq!(missing.code, ErrorCode::NotFound);

    let configs = describe_configs(&h, ResourceSpecifier::Topic(&topic)).await.unwrap();
    let cleanup = configs.iter().find(|c| c.name == "cleanup.policy").unwrap();
    assert_eq!(cleanup.value.as_deref(), Some("delete"));
}
