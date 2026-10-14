#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::Duration;

use kafka_explorer_lib::storage::connections::{ConnectionProfile, Environment, SecurityConfig};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};

/// Интеграционные тесты выполняются, только если задан KE_IT_BOOTSTRAP (см. dev/docker-compose.yml).
pub fn bootstrap() -> Option<String> {
    let v = std::env::var("KE_IT_BOOTSTRAP").ok();
    if v.is_none() {
        eprintln!("skip: KE_IT_BOOTSTRAP не задан");
    }
    v
}

pub fn sasl_bootstrap() -> Option<String> {
    let v = std::env::var("KE_IT_SASL_BOOTSTRAP").ok();
    if v.is_none() {
        eprintln!("skip: KE_IT_SASL_BOOTSTRAP не задан");
    }
    v
}

pub fn plaintext_profile(bs: &str) -> ConnectionProfile {
    ConnectionProfile {
        id: "it".into(),
        name: "it".into(),
        color: "#888888".into(),
        environment: Environment::Local,
        read_only: false,
        bootstrap_servers: bs.into(),
        security: SecurityConfig::Plaintext,
        extra_properties: BTreeMap::new(),
        request_timeout_ms: 10_000,
    }
}

pub fn unique_topic(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
}

pub async fn create_topic(bs: &str, name: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bs)
        .create()
        .unwrap();
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let results = admin
        .create_topics(
            &[NewTopic::new(name, partitions, TopicReplication::Fixed(1))],
            &opts,
        )
        .await
        .unwrap();
    for r in results {
        r.unwrap();
    }
    // даём брокеру распространить метаданные
    tokio::time::sleep(Duration::from_millis(500)).await;
}

pub struct TestMsg<'a> {
    pub partition: i32,
    pub key: Option<&'a str>,
    pub value: &'a str,
    pub headers: &'a [(&'a str, &'a str)],
}

pub async fn produce(bs: &str, topic: &str, msgs: &[TestMsg<'_>]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bs)
        .create()
        .unwrap();
    for m in msgs {
        let mut headers = OwnedHeaders::new();
        for (k, v) in m.headers {
            headers = headers.insert(Header {
                key: k,
                value: Some(*v),
            });
        }
        let mut record: FutureRecord<str, str> = FutureRecord::to(topic)
            .partition(m.partition)
            .payload(m.value)
            .headers(headers);
        if let Some(k) = m.key {
            record = record.key(k);
        }
        producer.send(record, Duration::from_secs(5)).await.unwrap();
    }
}
