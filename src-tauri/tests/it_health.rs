//! Проверка кластера против локальной Kafka (спец. G2 §10): снимок, правила и сохранение.
//! Нужен KE_IT_BOOTSTRAP (см. dev/docker-compose.yml). Dev-стенд — один брокер.

mod common;

use std::sync::Arc;
use std::time::Duration;

use kafka_explorer_lib::health::service::now_ms;
use kafka_explorer_lib::health::store::Trigger;
use kafka_explorer_lib::secrets::MemoryStore;
use kafka_explorer_lib::state::AppState;
use kafka_explorer_lib::storage::connections::Environment;
use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};

#[tokio::test(flavor = "multi_thread")]
async fn check_finds_problems_on_prod() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    // RF = 1 (на одном брокере иначе нельзя) и min.insync.replicas = 2 > RF.
    let plain = common::TempTopic::create(&bs, "it-health", 1).await;
    let strict = common::unique_topic("it-health-isr");
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    for r in common::admin(&bs)
        .create_topics(
            &[NewTopic::new(&strict, 1, TopicReplication::Fixed(1))
                .set("min.insync.replicas", "2")],
            &opts,
        )
        .await
        .unwrap()
    {
        r.unwrap();
    }
    let strict = common::TempTopic::adopt(&bs, strict);
    common::wait_for_topic_sync(&bs, &strict, 1);
    // Пустая группа с лагом: три сообщения, коммит на нуле.
    let msgs: Vec<common::TestMsg> = (0..3)
        .map(|_| common::TestMsg {
            partition: 0,
            key: None,
            value: "x",
            headers: &[],
        })
        .collect();
    common::produce(&bs, &plain, &msgs).await;
    let group = common::TempGroup::new(&bs, "it-health");
    common::commit_group_offsets(&bs, &group, &plain, &[(0, 0)]);

    let dir = tempfile::tempdir().unwrap();
    let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
    let mut p = common::plaintext_profile(&bs);
    p.environment = Environment::Prod;
    st.connections.upsert(p).unwrap();

    let run = st.health.clone().run(&st, "it", Trigger::Ui).await.unwrap();
    let has = |rule: &str, name: Option<&str>| {
        run.findings
            .iter()
            .any(|f| f.rule_id == rule && f.target.name.as_deref() == name)
    };
    assert!(has("cluster.singleBroker", None), "{:#?}", run.findings);
    assert!(has("topic.replicationFactorOne", Some(&plain)));
    assert!(has(
        "topic.replicationFactorOne",
        Some("__consumer_offsets")
    ));
    assert!(has("topic.minIsrUnreachable", Some(&strict)));
    let abandoned = run
        .findings
        .iter()
        .find(|f| f.rule_id == "group.abandoned" && f.target.name.as_deref() == Some(&group))
        .expect("group.abandoned");
    assert_eq!(abandoned.params["lag"], 3);
    assert_eq!(run.scope.brokers, 1);
    assert!(run.duration_ms < 45_000);
    let saved = st
        .health
        .store
        .load("it", now_ms())
        .unwrap()
        .last_check
        .unwrap();
    assert_eq!(saved.run_id, run.run_id);
    assert_eq!(saved.group_lag.get(&*group), Some(&3));
}
