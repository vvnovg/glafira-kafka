#![allow(dead_code)]

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Deref;
use std::time::{Duration, Instant};

use kafka_explorer_lib::storage::connections::{ConnectionProfile, Environment, SecurityConfig};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};

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

pub fn admin(bs: &str) -> AdminClient<DefaultClientContext> {
    ClientConfig::new()
        .set("bootstrap.servers", bs)
        .create()
        .unwrap()
}

/// Выполняет future в отдельном потоке со своим runtime: так её можно дождаться и из `Drop`,
/// который вызывается внутри tokio-runtime теста.
pub fn block_on<F>(f: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    std::thread::scope(|s| {
        s.spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(f)
        })
        .join()
        .unwrap()
    })
}

/// Ждёт, пока топик появится в метаданных с нужным числом партиций и у всех есть лидер.
pub fn wait_for_topic_sync(bs: &str, name: &str, partitions: usize) {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bs)
        .create()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(md) = c.fetch_metadata(Some(name), Duration::from_secs(2)) {
            let ready = md.topics().iter().any(|t| {
                t.name() == name
                    && t.error().is_none()
                    && t.partitions().len() == partitions
                    && t.partitions().iter().all(|p| p.leader() >= 0)
            });
            if ready {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "топик {name} не появился в метаданных за 15 с"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

pub async fn create_topic(bs: &str, name: &str, partitions: i32) {
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let results = admin(bs)
        .create_topics(
            &[NewTopic::new(name, partitions, TopicReplication::Fixed(1))],
            &opts,
        )
        .await
        .unwrap();
    for r in results {
        r.unwrap();
    }
    let (bs, name) = (bs.to_string(), name.to_string());
    tokio::task::spawn_blocking(move || wait_for_topic_sync(&bs, &name, partitions as usize))
        .await
        .unwrap();
}

pub fn delete_topics_sync(bs: &str, names: &[String]) {
    let admin = admin(bs);
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    // Ошибки уборки не должны ронять тест (топик мог удалить сам тест).
    let _ = block_on(admin.delete_topics(&refs, &opts));
}

pub fn delete_group_sync(bs: &str, group: &str) {
    let admin = admin(bs);
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let _ = block_on(admin.delete_groups(&[group], &opts));
}

/// Топик теста; удаляется в Drop, даже если тест упал.
pub struct TempTopic {
    bs: String,
    pub name: String,
}

impl TempTopic {
    pub async fn create(bs: &str, prefix: &str, partitions: i32) -> Self {
        let name = unique_topic(prefix);
        create_topic(bs, &name, partitions).await;
        Self {
            bs: bs.to_string(),
            name,
        }
    }

    /// Уборка топика, который создаёт сам тест (например, проверяемой командой create_topic).
    pub fn adopt(bs: &str, name: String) -> Self {
        Self {
            bs: bs.to_string(),
            name,
        }
    }
}

impl Deref for TempTopic {
    type Target = str;
    fn deref(&self) -> &str {
        &self.name
    }
}

impl Drop for TempTopic {
    fn drop(&mut self) {
        delete_topics_sync(&self.bs, std::slice::from_ref(&self.name));
    }
}

/// Consumer group теста; удаляется в Drop.
pub struct TempGroup {
    bs: String,
    pub id: String,
}

impl TempGroup {
    pub fn new(bs: &str, prefix: &str) -> Self {
        Self {
            bs: bs.to_string(),
            id: unique_topic(prefix),
        }
    }
}

impl Deref for TempGroup {
    type Target = str;
    fn deref(&self) -> &str {
        &self.id
    }
}

impl Drop for TempGroup {
    fn drop(&mut self) {
        delete_group_sync(&self.bs, &self.id);
    }
}

/// Коммит от имени группы без вступления в неё: группа остаётся в состоянии Empty.
pub fn commit_group_offsets(bs: &str, group: &str, topic: &str, offsets: &[(i32, i64)]) {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bs)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    for (p, o) in offsets {
        tpl.add_partition_offset(topic, *p, Offset::Offset(*o))
            .unwrap();
    }
    c.commit(&tpl, CommitMode::Sync).unwrap();
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
