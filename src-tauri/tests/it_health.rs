//! Проверка кластера против локальной Kafka (спец. G2 §10): снимок, правила и сохранение.
//! Нужен KE_IT_BOOTSTRAP (см. dev/docker-compose.yml). Dev-стенд — один брокер.

mod common;

use std::sync::Arc;
use std::time::Duration;

use glafira_mcp::health::Severity;
use glafira_mcp::proposals::{
    ChangeSpec, ConfigChangeSpec, NewProposal, ProposalOp, ProposalOutput,
};
use glafira_mcp::{Backend, SessionView};
use kafka_explorer_lib::commands::topic_ops::{alter_topic_configs_impl, create_topic_impl};
use kafka_explorer_lib::health::proposals::ProposalStatus;
use kafka_explorer_lib::health::service::now_ms;
use kafka_explorer_lib::health::store::Trigger;
use kafka_explorer_lib::kafka::admin::{ConfigChange, NewTopicSpec};
use kafka_explorer_lib::mcp::approvals::Approvals;
use kafka_explorer_lib::mcp::backend::AppBackend;
use kafka_explorer_lib::secrets::MemoryStore;
use kafka_explorer_lib::state::AppState;
use kafka_explorer_lib::storage::connections::{Environment, LlmAccess};
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

/// Предложение LLM (спец. G3 §5–§6): сверка с кластером, применение командой с `proposal_id`
/// и отметка «применено».
#[tokio::test(flavor = "multi_thread")]
async fn proposal_is_checked_against_the_cluster_and_marked_applied() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let strict = common::unique_topic("it-proposal-isr");
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

    let dir = tempfile::tempdir().unwrap();
    let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
    let mut p = common::plaintext_profile(&bs);
    p.environment = Environment::Dev;
    p.llm_access = LlmAccess::Metadata;
    st.connections.upsert(p.clone()).unwrap();
    let backend = AppBackend::new(
        st.clone(),
        Arc::new(Approvals::new(st.settings.clone())),
        Arc::new(|_| {}),
    );
    let claude = SessionView {
        id: "s1".into(),
        client: Some(glafira_mcp::ClientIdent {
            name: "claude-ai".into(),
            version: "1".into(),
        }),
    };
    let propose = |change: ChangeSpec| {
        backend.proposals(ProposalOp::Propose {
            connection_id: "it".into(),
            session: claude.clone(),
            proposal: NewProposal {
                severity: Severity::Warning,
                title: "Fix min.insync.replicas".into(),
                rationale: "RF is 1, so min.insync.replicas = 2 blocks acks=all writes".into(),
                change,
                related_rule_id: Some("topic.minIsrUnreachable".into()),
                related_finding_key: None,
            },
        })
    };
    let alter = |topic: &str, name: &str, value: &str| ChangeSpec::AlterTopicConfigs {
        topic: topic.into(),
        changes: vec![ConfigChangeSpec {
            name: name.into(),
            value: Some(value.into()),
        }],
    };

    // Сверка с кластером: внутренний топик, неизвестный ключ, партиций не больше.
    let e = propose(alter("__consumer_offsets", "min.insync.replicas", "1"))
        .await
        .unwrap_err();
    assert_eq!(e.code, "invalid_argument");
    assert!(e.message.contains("internal"), "{e:?}");
    let e = propose(alter(&strict, "no.such.config", "1"))
        .await
        .unwrap_err();
    assert!(e.message.contains("unknown topic config"), "{e:?}");
    let e = propose(ChangeSpec::AddPartitions {
        topic: strict.to_string(),
        new_total: 1,
    })
    .await
    .unwrap_err();
    assert!(e.message.contains("has 1 partitions"), "{e:?}");
    let e = propose(alter("no-such-topic", "min.insync.replicas", "1"))
        .await
        .unwrap_err();
    assert_eq!(e.code, "not_found");

    // Корректное предложение записано; подключение не read-only.
    let ProposalOutput::Proposed {
        proposal_id,
        created,
        read_only,
    } = propose(alter(&strict, "min.insync.replicas", "1"))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(created && !read_only);

    // Человек применяет формой: команда с proposal_id меняет конфиг и помечает предложение.
    alter_topic_configs_impl(
        &st,
        "it".into(),
        strict.to_string(),
        vec![ConfigChange::Set {
            name: "min.insync.replicas".into(),
            value: "1".into(),
        }],
        None,
        Some(proposal_id.clone()),
    )
    .await
    .unwrap();
    let h = st.handles("it").unwrap();
    let (value, _) = common::topic_config_eventually(&h, &strict, "min.insync.replicas", |v| {
        v.0.as_deref() == Some("1")
    })
    .await;
    assert_eq!(value.as_deref(), Some("1"));
    let f = st.health.store.load("it", now_ms()).unwrap();
    let saved = f.proposals.iter().find(|p| p.id == proposal_id).unwrap();
    assert_eq!(saved.status, ProposalStatus::Applied);
    assert_eq!(saved.applied_as_proposed, Some(true));

    // Создание топика по предложению: значение брокера для фактора репликации (-1) — как в форме.
    let name = common::unique_topic("it-proposal-new");
    let ProposalOutput::Proposed { proposal_id, .. } = propose(ChangeSpec::CreateTopic {
        name: name.clone(),
        partitions: 2,
        replication_factor: None,
        configs: Some([("cleanup.policy".to_string(), "compact".to_string())].into()),
    })
    .await
    .unwrap() else {
        panic!()
    };
    let created_topic = common::TempTopic::adopt(&bs, name.clone());
    create_topic_impl(
        &st,
        "it".into(),
        NewTopicSpec {
            name: name.clone(),
            partitions: 2,
            replication_factor: -1,
            configs: [("cleanup.policy".to_string(), "compact".to_string())].into(),
        },
        None,
        Some(proposal_id.clone()),
    )
    .await
    .unwrap();
    common::wait_for_topic_sync(&bs, &created_topic, 2);
    let f = st.health.store.load("it", now_ms()).unwrap();
    let saved = f.proposals.iter().find(|p| p.id == proposal_id).unwrap();
    assert_eq!(
        (saved.status, saved.applied_as_proposed),
        (ProposalStatus::Applied, Some(true))
    );
    // Теперь топик есть: такое же предложение отвергается.
    let e = propose(ChangeSpec::CreateTopic {
        name: name.clone(),
        partitions: 2,
        replication_factor: None,
        configs: None,
    })
    .await
    .unwrap_err();
    assert!(e.message.contains("already exists"), "{e:?}");

    // Read-only подключение: предложение принимается, модели сообщается readOnly.
    p.read_only = true;
    st.connections.upsert(p).unwrap();
    let ProposalOutput::Proposed { read_only, .. } = propose(ChangeSpec::AddPartitions {
        topic: strict.to_string(),
        new_total: 2,
    })
    .await
    .unwrap() else {
        panic!()
    };
    assert!(read_only);
}
