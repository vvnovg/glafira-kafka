mod common;

use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::kafka::producer::{ProducerHandle, ProducerSettings, send};
use kafka_explorer_lib::produce::payload::EncodedMessage;
use kafka_explorer_lib::secrets::MemoryStore;

fn message(topic: &str, partition: Option<i32>) -> EncodedMessage {
    EncodedMessage {
        topic: topic.into(),
        partition,
        key: Some(b"k1".to_vec()),
        value: Some(br#"{"n":1}"#.to_vec()),
        headers: vec![("trace".into(), b"abc".to_vec())],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sends_and_reports_partition_offset() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-produce", 2).await;
    let h = ProducerHandle::create(
        &common::plaintext_profile(&bs),
        &MemoryStore::default(),
        &ProducerSettings::default(),
    )
    .unwrap();
    let r1 = send(&h, &message(&topic, Some(1))).await.unwrap();
    let r2 = send(&h, &message(&topic, Some(1))).await.unwrap();
    assert_eq!((r1.partition, r1.offset), (1, 0));
    assert_eq!((r2.partition, r2.offset), (1, 1));
    assert!(r1.timestamp > 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_partition_is_reported() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-produce-bad", 1).await;
    let mut p = common::plaintext_profile(&bs);
    p.request_timeout_ms = 5_000;
    let h =
        ProducerHandle::create(&p, &MemoryStore::default(), &ProducerSettings::default()).unwrap();
    let e = send(&h, &message(&topic, Some(7))).await.unwrap_err();
    assert_ne!(e.code, ErrorCode::Internal, "{e:?}");
}
