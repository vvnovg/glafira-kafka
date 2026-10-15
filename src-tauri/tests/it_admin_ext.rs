mod common;

use std::sync::Arc;

use kafka_explorer_lib::kafka::admin_ext::incremental_alter_topic_configs;
use kafka_explorer_lib::kafka::configs::describe_configs;
use kafka_explorer_lib::kafka::pool::KafkaHandles;
use kafka_explorer_lib::secrets::MemoryStore;
use rdkafka::admin::ResourceSpecifier;
use rdkafka::types::RDKafkaErrorCode;

fn handles(bs: &str) -> Arc<KafkaHandles> {
    Arc::new(KafkaHandles::create(&common::plaintext_profile(bs), &MemoryStore::default()).unwrap())
}

fn alter(
    h: &Arc<KafkaHandles>,
    topic: &str,
    changes: &[(&str, Option<&str>)],
) -> Result<(), RDKafkaErrorCode> {
    let changes: Vec<(String, Option<String>)> = changes
        .iter()
        .map(|(k, v)| (k.to_string(), v.map(String::from)))
        .collect();
    incremental_alter_topic_configs(&h.admin, h.timeout, topic, &changes)
        .expect("запрос выполнен")
        .map_err(|e| e.code)
}

async fn config(h: &Arc<KafkaHandles>, topic: &str, name: &str) -> (Option<String>, bool) {
    let entries = describe_configs(h, ResourceSpecifier::Topic(topic))
        .await
        .unwrap();
    let e = entries.iter().find(|e| e.name == name).unwrap();
    (e.value.clone(), e.is_default)
}

#[tokio::test(flavor = "multi_thread")]
async fn incremental_alter_sets_and_deletes_without_touching_other_configs() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-alter", 1).await;
    let h = handles(&bs);

    let (h1, t1) = (h.clone(), topic.to_string());
    tokio::task::spawn_blocking(move || {
        alter(
            &h1,
            &t1,
            &[
                ("retention.ms", Some("123456")),
                ("cleanup.policy", Some("compact")),
            ],
        )
        .unwrap();
    })
    .await
    .unwrap();
    assert_eq!(
        config(&h, &topic, "retention.ms").await,
        (Some("123456".into()), false)
    );
    assert_eq!(
        config(&h, &topic, "cleanup.policy").await.0.as_deref(),
        Some("compact")
    );

    // DELETE возвращает только retention.ms к default; cleanup.policy остаётся (в отличие от AlterConfigs).
    let (h1, t1) = (h.clone(), topic.to_string());
    tokio::task::spawn_blocking(move || alter(&h1, &t1, &[("retention.ms", None)]).unwrap())
        .await
        .unwrap();
    assert!(
        config(&h, &topic, "retention.ms").await.1,
        "retention.ms снова default"
    );
    assert_eq!(
        config(&h, &topic, "cleanup.policy").await.0.as_deref(),
        Some("compact")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn incremental_alter_reports_item_errors() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-alter-bad", 1).await;
    let h = handles(&bs);
    let t1 = topic.to_string();
    let (bad_value, missing) = tokio::task::spawn_blocking(move || {
        (
            alter(&h, &t1, &[("retention.ms", Some("abc"))]),
            alter(
                &h,
                "does-not-exist-topic",
                &[("retention.ms", Some("1000"))],
            ),
        )
    })
    .await
    .unwrap();
    assert_eq!(bad_value, Err(RDKafkaErrorCode::InvalidConfig));
    assert_eq!(missing, Err(RDKafkaErrorCode::UnknownTopicOrPartition));
}
