mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::TestMsg;
use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::kafka::admin::{
    ConfigChange, NewTopicSpec, add_partitions, alter_topic_configs, create_topic, delete_topics,
    purge_topic, topic_partition_ids,
};
use kafka_explorer_lib::kafka::pool::KafkaHandles;
use kafka_explorer_lib::kafka::topics::topic_details;
use kafka_explorer_lib::secrets::MemoryStore;

fn handles(bs: &str) -> Arc<KafkaHandles> {
    Arc::new(KafkaHandles::create(&common::plaintext_profile(bs), &MemoryStore::default()).unwrap())
}

async fn wait_topic(bs: &str, name: &str, partitions: usize) {
    let (bs, name) = (bs.to_string(), name.to_string());
    tokio::task::spawn_blocking(move || common::wait_for_topic_sync(&bs, &name, partitions))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn topic_lifecycle() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let h = handles(&bs);
    let name = common::unique_topic("it-ops");
    let _cleanup = common::TempTopic::adopt(&bs, name.clone());

    // create
    let spec = NewTopicSpec {
        name: name.clone(),
        partitions: 1,
        replication_factor: 1,
        configs: BTreeMap::from([("retention.ms".to_string(), "600000".to_string())]),
    };
    create_topic(&h, &spec).await.unwrap();
    wait_topic(&bs, &name, 1).await;
    assert_eq!(
        create_topic(&h, &spec).await.unwrap_err().code,
        ErrorCode::InvalidInput
    );
    assert_eq!(
        common::topic_config(&h, &name, "retention.ms")
            .await
            .0
            .as_deref(),
        Some("600000")
    );

    // alter: set + delete
    let (h1, n1) = (h.clone(), name.clone());
    tokio::task::spawn_blocking(move || {
        alter_topic_configs(
            &h1,
            &n1,
            &[ConfigChange::Set {
                name: "cleanup.policy".into(),
                value: "compact,delete".into(),
            }],
        )
        .unwrap();
        alter_topic_configs(
            &h1,
            &n1,
            &[ConfigChange::Delete {
                name: "retention.ms".into(),
            }],
        )
        .unwrap();
        let bad = alter_topic_configs(
            &h1,
            &n1,
            &[ConfigChange::Set {
                name: "retention.ms".into(),
                value: "abc".into(),
            }],
        )
        .unwrap_err();
        assert_eq!(bad.code, ErrorCode::InvalidInput);
    })
    .await
    .unwrap();
    assert_eq!(
        common::topic_config_eventually(&h, &name, "cleanup.policy", |v| v.0.as_deref()
            == Some("compact,delete"))
        .await
        .0
        .as_deref(),
        Some("compact,delete")
    );
    assert!(
        common::topic_config_eventually(&h, &name, "retention.ms", |v| v.1)
            .await
            .1
    );

    // add partitions 1 → 3
    add_partitions(&h, &name, 3).await.unwrap();
    wait_topic(&bs, &name, 3).await;
    let (h1, n1) = (h.clone(), name.clone());
    let ids = tokio::task::spawn_blocking(move || topic_partition_ids(&h1, &n1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ids, [0, 1, 2]);

    // purge
    let msgs: Vec<TestMsg> = (0..6)
        .map(|i| TestMsg {
            partition: i % 3,
            key: Some("k"),
            value: "v",
            headers: &[],
        })
        .collect();
    common::produce(&bs, &name, &msgs).await;
    let purged = purge_topic(&h, &name, &[0, 1, 2]).await.unwrap();
    assert_eq!(
        purged
            .iter()
            .map(|p| (p.partition, p.low_watermark))
            .collect::<Vec<_>>(),
        [(0, Some(2)), (1, Some(2)), (2, Some(2))]
    );
    let (h1, n1) = (h.clone(), name.clone());
    let details = tokio::task::spawn_blocking(move || topic_details(&h1, &n1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(details.message_count, Some(0));

    // delete: существующий и несуществующий топики — результат по каждому
    let results = delete_topics(&h, &[name.clone(), "it-ops-missing-topic".into()])
        .await
        .unwrap();
    assert!(results[0].error.is_none(), "{:?}", results[0]);
    assert_eq!(results[1].error.as_ref().unwrap().code, ErrorCode::NotFound);
    // Имя объекта не подставляется как сообщение брокера в details.
    let details = results[1].error.as_ref().unwrap().details.clone();
    assert!(
        !details
            .as_deref()
            .unwrap_or("")
            .contains("it-ops-missing-topic"),
        "{details:?}"
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (h1, n1) = (h.clone(), name.clone());
        let r = tokio::task::spawn_blocking(move || topic_details(&h1, &n1))
            .await
            .unwrap();
        if matches!(&r, Err(e) if e.code == ErrorCode::NotFound) {
            break;
        }
        assert!(Instant::now() < deadline, "топик не исчез из метаданных");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_policies_in_one_request() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let a = common::TempTopic::create(&bs, "it-policy-a", 1).await;
    let b = common::TempTopic::create(&bs, "it-policy-b", 1).await;
    let h = handles(&bs);
    let (h1, b1) = (h.clone(), b.to_string());
    tokio::task::spawn_blocking(move || {
        alter_topic_configs(
            &h1,
            &b1,
            &[ConfigChange::Set {
                name: "cleanup.policy".into(),
                value: "compact".into(),
            }],
        )
    })
    .await
    .unwrap()
    .unwrap();
    let names = [a.to_string(), b.to_string()];
    let policies = common::eventually(
        || async {
            kafka_explorer_lib::kafka::configs::describe_topic_policies(&h, &names)
                .await
                .unwrap()
        },
        |p| p[1].cleanup_policy.as_deref() == Some("compact"),
    )
    .await;
    assert_eq!(policies[0].cleanup_policy.as_deref(), Some("delete"));
    assert_eq!(policies[1].cleanup_policy.as_deref(), Some("compact"));
}

/// Сразу после увеличения числа партиций метаданные могут отдавать NotLeaderForPartition;
/// воспроизводится не всегда, но чтение обязано проходить.
#[tokio::test(flavor = "multi_thread")]
async fn topic_details_right_after_add_partitions() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let h = handles(&bs);
    let name = common::unique_topic("it-ops-grow");
    let _cleanup = common::TempTopic::adopt(&bs, name.clone());
    let spec = NewTopicSpec {
        name: name.clone(),
        partitions: 3,
        replication_factor: 1,
        configs: BTreeMap::new(),
    };
    create_topic(&h, &spec).await.unwrap();
    wait_topic(&bs, &name, 3).await;
    add_partitions(&h, &name, 5).await.unwrap();
    for _ in 0..10 {
        let h2 = h.clone();
        let n = name.clone();
        tokio::task::spawn_blocking(move || topic_details(&h2, &n))
            .await
            .unwrap()
            .unwrap();
    }
}
