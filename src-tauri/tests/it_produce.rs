mod common;

use kafka_explorer_lib::commands::produce::produce_impl;
use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::kafka::producer::{ProducerHandle, ProducerSettings, send};
use kafka_explorer_lib::produce::payload::{
    EncodedMessage, PayloadFormat, PayloadInput, ProduceMessage,
};
use kafka_explorer_lib::secrets::MemoryStore;
use kafka_explorer_lib::state::AppState;
use std::sync::Arc;

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
    assert_eq!((r1.partition, r1.offset), (1, Some(0)));
    assert_eq!((r2.partition, r2.offset), (1, Some(1)));
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

#[tokio::test(flavor = "multi_thread")]
async fn same_key_without_partition_lands_on_same_partition() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-produce-key", 4).await;
    let h = ProducerHandle::create(
        &common::plaintext_profile(&bs),
        &MemoryStore::default(),
        &ProducerSettings::default(),
    )
    .unwrap();
    let a = send(&h, &message(&topic, None)).await.unwrap();
    let b = send(&h, &message(&topic, None)).await.unwrap();
    assert_eq!(a.partition, b.partition);
    assert_eq!(b.offset, Some(a.offset.unwrap() + 1));
}

#[tokio::test(flavor = "multi_thread")]
async fn produce_command_renders_placeholders_and_records_history() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-produce-cmd", 1).await;
    let dir = tempfile::tempdir().unwrap();
    let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
    st.connections
        .upsert(common::plaintext_profile(&bs))
        .unwrap();
    let m = ProduceMessage {
        topic: topic.to_string(),
        partition: None,
        key: PayloadInput {
            format: PayloadFormat::String,
            text: "key-{{seq}}".into(),
        },
        value: PayloadInput {
            format: PayloadFormat::Json,
            text: r#"{"id":"{{uuid}}"}"#.into(),
        },
        headers: vec![],
    };
    let r = produce_impl(&st, "it".into(), m, ProducerSettings::default(), None)
        .await
        .unwrap();
    assert_eq!(r.offset, Some(0));
    let history = st.history.list("it").unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].message.key.text, "key-0");
    assert!(
        !history[0].message.value.text.contains("{{"),
        "в истории — уже подставленные значения"
    );
}
