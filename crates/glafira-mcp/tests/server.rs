//! Сервер целиком: настоящий сокет (pipe на Windows), рукопожатие, MCP-клиент rmcp и
//! фальшивый Backend.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use glafira_mcp::model::*;
use glafira_mcp::{
    AuditLog, Backend, BackendError, BoxFuture, GatewayOptions, McpServerHandle, endpoint,
    handshake,
};
use rmcp::model::{CallToolRequestParams, CallToolResult};
use rmcp::service::RunningService;
use rmcp::{RoleClient, ServiceExt};
use serde_json::{Value, json};

const TOKEN: &str = "test-token";

struct Fake {
    access: Mutex<SessionAccess>,
    seen: Mutex<Vec<SessionView>>,
    closed: Mutex<Vec<String>>,
    groups: Mutex<VecDeque<GroupDescription>>,
    /// Задержка каждого описания группы (медленный кластер).
    describe_delay: Mutex<Duration>,
}

impl Fake {
    fn new(access: SessionAccess) -> Arc<Self> {
        Arc::new(Self {
            access: Mutex::new(access),
            seen: Mutex::new(Vec::new()),
            closed: Mutex::new(Vec::new()),
            groups: Mutex::new(VecDeque::new()),
            describe_delay: Mutex::new(Duration::ZERO),
        })
    }
}

fn member(id: &str, host: &str) -> Member {
    Member {
        member_id: id.into(),
        client_id: "orders-app".into(),
        host: host.into(),
        group_instance_id: Some(format!("pod-{id}")),
        assignment: vec![TopicPartition {
            topic: "orders".into(),
            partition: 0,
        }],
        target_assignment: None,
    }
}

/// 2000 участников с длинными id и хостами: описание группы ~1,4 МБ.
fn crowd() -> Vec<Member> {
    (0..2000)
        .map(|i| member(&format!("m{i}-{}", "x".repeat(300)), &"h".repeat(300)))
        .collect()
}

fn group(state: &str, members: Vec<Member>) -> GroupDescription {
    GroupDescription {
        group_id: "orders-svc".into(),
        state: state.into(),
        group_type: "classic".into(),
        assignor: "range".into(),
        coordinator: Some(Broker {
            id: 1,
            host: "kafka-1".into(),
            port: 9092,
        }),
        members,
        error: None,
        not_found: false,
    }
}

fn config(name: &str, value: &str, is_default: bool) -> ConfigEntry {
    ConfigEntry {
        name: name.into(),
        value: Some(value.into()),
        source: "DYNAMIC_BROKER_CONFIG".into(),
        is_default,
        is_read_only: false,
        is_sensitive: false,
        hidden: false,
    }
}

fn partition(id: i32) -> Partition {
    Partition {
        id,
        leader: 1,
        replicas: vec![1, 2, 3],
        isr: vec![1, 2, 3],
        low: Some(0),
        high: Some(100),
        message_count: Some(100),
        under_replicated: false,
        error: None,
    }
}

impl Backend for Fake {
    fn access(&self, session: &SessionView) -> SessionAccess {
        self.seen.lock().unwrap().push(session.clone());
        *self.access.lock().unwrap()
    }

    fn read(&self, op: ReadOp) -> BoxFuture<'_, Result<ReadOutput, BackendError>> {
        Box::pin(async move {
            if op.connection_id().is_some_and(|c| c != "c1") {
                return Err(BackendError::not_allowed());
            }
            Ok(match op {
                ReadOp::ListConnections => ReadOutput::Connections(vec![Connection {
                    id: "c1".into(),
                    name: "dev".into(),
                    environment: "dev".into(),
                    folder_path: vec!["Team".into()],
                    read_only: false,
                }]),
                ReadOp::ClusterOverview { .. } => ReadOutput::Cluster(ClusterOverview {
                    cluster_id: Some("k1".into()),
                    controller_id: Some(1),
                    brokers: vec![Broker {
                        id: 1,
                        host: "kafka-1".into(),
                        port: 9092,
                    }],
                    topic_count: 3,
                    internal_topic_count: 1,
                    partition_count: 12,
                    under_replicated_partitions: 0,
                    offline_partitions: 0,
                }),
                ReadOp::ListTopics { .. } => ReadOutput::Topics(
                    ["payments", "__consumer_offsets", "orders", "orders.dlq"]
                        .iter()
                        .map(|n| TopicSummary {
                            name: n.to_string(),
                            internal: n.starts_with("__"),
                            partitions: 3,
                            replication_factor: 3,
                            under_replicated: 0,
                        })
                        .collect(),
                ),
                ReadOp::BrokerConfig { .. } => ReadOutput::Configs(vec![
                    config("log.retention.hours", "24", false),
                    config("group.initial.rebalance.delay.ms", "3000", true),
                    config(
                        "listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config",
                        "secret!",
                        false,
                    ),
                ]),
                ReadOp::DescribeTopic { topic, .. } => ReadOutput::Topic(TopicDetails {
                    name: topic.clone(),
                    internal: false,
                    replication_factor: 3,
                    message_count: None,
                    partitions: (0..if topic == "big" { 3000 } else { 3 })
                        .map(partition)
                        .collect(),
                }),
                ReadOp::DescribeGroup { group_id, .. } => {
                    let delay = *self.describe_delay.lock().unwrap();
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    if group_id == "crowd" {
                        ReadOutput::Group(group("Stable", crowd()))
                    } else if group_id == "missing" {
                        let mut g = group("Dead", vec![]);
                        g.not_found = true;
                        ReadOutput::Group(g)
                    } else {
                        let mut q = self.groups.lock().unwrap();
                        let g = if q.len() > 1 {
                            q.pop_front()
                        } else {
                            q.front().cloned()
                        };
                        ReadOutput::Group(
                            g.unwrap_or_else(|| group("Stable", vec![member("a", "10.0.0.1")])),
                        )
                    }
                }
                ReadOp::GroupOffsets { .. } => ReadOutput::Offsets(vec![GroupOffset {
                    topic: "orders".into(),
                    partition: 0,
                    committed: Some(90),
                    log_end: Some(100),
                    lag: Some(10),
                    member_client_id: Some("orders-app".into()),
                    error: None,
                }]),
                ReadOp::GroupsLag { group_ids, .. } => ReadOutput::Lags(
                    group_ids
                        .into_iter()
                        .map(|g| GroupLag {
                            group_id: g,
                            total_lag: Some(10),
                            topics: vec!["orders".into()],
                            error: None,
                        })
                        .collect(),
                ),
                ReadOp::TopicGroups { topic, .. } => ReadOutput::TopicGroups(TopicGroups {
                    groups: (0..if topic == "busy" { 5000 } else { 1 })
                        .map(|i| TopicGroup {
                            group_id: format!("group-{i}-{}", "g".repeat(100)),
                            state: "Stable".into(),
                            lag: Some(i),
                            partitions: 3,
                        })
                        .collect(),
                    incomplete: false,
                }),
                other => {
                    return Err(BackendError::new(
                        "internal",
                        format!("not scripted: {other:?}"),
                    ));
                }
            })
        })
    }

    fn connection_name(&self, id: &str) -> Option<String> {
        (id == "c1").then(|| "dev".to_string())
    }

    fn session_closed(&self, session_id: &str) {
        self.closed.lock().unwrap().push(session_id.to_string());
    }

    fn sessions_changed(&self) {}
}

struct Env {
    _dir: tempfile::TempDir,
    ep: String,
    handle: McpServerHandle,
    audit: Arc<AuditLog>,
}

async fn env(fake: Arc<Fake>) -> Env {
    let dir = tempfile::tempdir().unwrap();
    let ep = endpoint::choose(
        dir.path(),
        &dir.path().file_name().unwrap().to_string_lossy(),
    );
    let audit = Arc::new(AuditLog::new(dir.path()));
    let mut opts = GatewayOptions::new("glafira-kafka", "0.3.0");
    opts.observe_unit = Duration::from_millis(10);
    let handle = glafira_mcp::start(&ep, TOKEN.into(), fake, audit.clone(), opts)
        .await
        .unwrap();
    Env {
        _dir: dir,
        ep,
        handle,
        audit,
    }
}

async fn client(ep: &str) -> RunningService<RoleClient, ()> {
    let mut s = endpoint::connect(&ep.to_string()).await.unwrap();
    handshake::client(&mut s, TOKEN).await.unwrap();
    ().serve(s).await.unwrap()
}

async fn call(c: &RunningService<RoleClient, ()>, tool: &str, args: Value) -> CallToolResult {
    let mut p = CallToolRequestParams::new(tool.to_string());
    if let Value::Object(m) = args {
        p = p.with_arguments(m);
    }
    c.call_tool(p).await.unwrap()
}

fn text(r: &CallToolResult) -> String {
    r.content
        .iter()
        .filter_map(|c| c.as_text().map(|t| t.text.clone()))
        .collect()
}

fn ok(r: CallToolResult) -> Value {
    assert_eq!(r.is_error, Some(false), "{}", text(&r));
    r.structured_content.unwrap()
}

fn err(r: &CallToolResult) -> String {
    assert_eq!(r.is_error, Some(true));
    text(r)
}

#[tokio::test]
async fn exposes_exactly_the_read_only_tools() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = client(&e.ep).await;
    let info = c.peer_info().unwrap();
    assert!(
        info.instructions
            .as_deref()
            .unwrap()
            .contains("read-only gateway")
    );
    let mut tools = c.list_all_tools().await.unwrap();
    tools.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
    assert_eq!(
        names,
        [
            "describe_consumer_group",
            "describe_topic",
            "get_broker_config",
            "get_cluster_overview",
            "get_consumer_groups_lag",
            "get_topic_configs",
            "get_topic_consumer_groups",
            "get_topics_message_counts",
            "list_connections",
            "list_consumer_groups",
            "list_topics",
            "observe_consumer_group",
        ]
    );
    for t in &tools {
        let a = t.annotations.as_ref().unwrap();
        assert_eq!(
            (a.read_only_hint, a.destructive_hint),
            (Some(true), Some(false)),
            "{}",
            t.name
        );
        assert!(
            t.description.as_deref().is_some_and(|d| d.len() > 30),
            "{}",
            t.name
        );
    }
}

#[tokio::test]
async fn topics_are_filtered_paged_and_sorted() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = client(&e.ep).await;
    let v = ok(call(&c, "list_topics", json!({"connectionId": "c1"})).await);
    assert_eq!(v["total"], 3);
    assert_eq!(v["items"][0]["name"], "orders");
    assert_eq!(v["truncated"], false);
    let v = ok(call(
        &c,
        "list_topics",
        json!({"connectionId": "c1", "includeInternal": true, "limit": 2}),
    )
    .await);
    assert_eq!(
        (v["total"].as_u64(), v["nextOffset"].as_u64()),
        (Some(4), Some(2))
    );
    assert_eq!(v["items"][0]["name"], "__consumer_offsets");
    let v = ok(call(
        &c,
        "list_topics",
        json!({"connectionId": "c1", "filter": "ORDERS"}),
    )
    .await);
    assert_eq!(v["total"], 2);
    let msg = err(&call(&c, "list_topics", json!({"connectionId": "c1", "limit": 0})).await);
    assert!(msg.starts_with("invalid_argument:"), "{msg}");
}

#[tokio::test]
async fn broker_config_hides_secrets_and_returns_named_defaults() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = client(&e.ep).await;
    let v = ok(call(
        &c,
        "get_broker_config",
        json!({"connectionId": "c1", "brokerId": 1}),
    )
    .await);
    let configs = v["configs"].as_array().unwrap();
    assert_eq!(configs.len(), 2, "значение по умолчанию отброшено");
    let jaas = configs
        .iter()
        .find(|c| c["name"].as_str().unwrap().contains("jaas"))
        .unwrap();
    assert_eq!(
        (jaas["value"].clone(), jaas["hidden"].clone()),
        (Value::Null, json!(true))
    );
    assert!(
        !text(
            &call(
                &c,
                "get_broker_config",
                json!({"connectionId": "c1", "brokerId": 1})
            )
            .await
        )
        .contains("secret!")
    );
    let v = ok(call(
        &c,
        "get_broker_config",
        json!({"connectionId": "c1", "brokerId": 1, "names": ["group.initial.rebalance.delay.ms"]}),
    )
    .await);
    assert_eq!(v["configs"][0]["value"], "3000");
}

#[tokio::test]
async fn big_topic_is_truncated_to_the_size_limit() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = client(&e.ep).await;
    let r = call(
        &c,
        "describe_topic",
        json!({"connectionId": "c1", "topic": "big"}),
    )
    .await;
    assert!(text(&r).len() <= glafira_mcp::rules::MAX_RESPONSE_BYTES + 200);
    let v = ok(r);
    assert_eq!(v["truncated"], true);
    assert!(v["hint"].as_str().unwrap().contains("100 KB"));
    assert!(v["partitions"].as_array().unwrap().len() < 3000);
}

#[tokio::test]
async fn foreign_connection_is_not_allowed() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = client(&e.ep).await;
    let msg = err(&call(&c, "get_cluster_overview", json!({"connectionId": "prod"})).await);
    assert!(msg.starts_with("not_allowed:"), "{msg}");
}

#[tokio::test]
async fn pending_client_gets_a_hint_then_works_after_approval() {
    let fake = Fake::new(SessionAccess::Pending);
    let e = env(fake.clone()).await;
    let c = client(&e.ep).await;
    let first = fake.seen.lock().unwrap()[0].clone();
    assert_eq!(
        first.client.as_ref().map(|c| c.name.as_str()),
        Some("rmcp"),
        "клиент из initialize"
    );
    let msg = err(&call(&c, "list_connections", json!({})).await);
    assert!(msg.starts_with("client_not_approved:"), "{msg}");
    *fake.access.lock().unwrap() = SessionAccess::Approved;
    let v = ok(call(&c, "list_connections", json!({})).await);
    assert_eq!(v["connections"][0]["folderPath"][0], "Team");
    let sessions = e.handle.sessions();
    assert_eq!(
        (sessions.len(), sessions[0].calls),
        (1, 1),
        "отклонённый вызов не считается"
    );
    assert_eq!(sessions[0].client_name.as_deref(), Some("rmcp"));
}

#[tokio::test]
async fn rejected_client_is_disconnected() {
    let fake = Fake::new(SessionAccess::Rejected);
    let e = env(fake.clone()).await;
    let c = client(&e.ep).await;
    let msg = err(&call(&c, "list_connections", json!({})).await);
    assert!(msg.starts_with("client_rejected:"), "{msg}");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(e.handle.sessions().is_empty());
    assert_eq!(fake.closed.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn calls_are_rate_limited_and_audited() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = client(&e.ep).await;
    for _ in 0..60 {
        ok(call(&c, "list_connections", json!({})).await);
    }
    let msg = err(&call(&c, "list_connections", json!({})).await);
    assert!(msg.starts_with("rate_limited:"), "{msg}");
    let other = client(&e.ep).await;
    ok(call(
        &other,
        "get_cluster_overview",
        json!({"connectionId": "c1"}),
    )
    .await);
    let recent = e.audit.recent(3);
    assert_eq!(recent[0]["event"], "call");
    assert_eq!(recent[0]["tool"], "get_cluster_overview");
    assert_eq!(recent[0]["connection"]["name"], "dev");
    assert!(
        recent.iter().any(|r| r["code"] == "rate_limited"),
        "{recent:?}"
    );
}

#[tokio::test]
async fn group_details_include_static_membership_and_lag() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = client(&e.ep).await;
    let v = ok(call(
        &c,
        "describe_consumer_group",
        json!({"connectionId": "c1", "groupId": "orders-svc"}),
    )
    .await);
    assert_eq!(v["groupType"], "classic");
    assert_eq!(v["coordinator"]["id"], 1);
    assert_eq!(v["members"][0]["groupInstanceId"], "pod-a");
    assert_eq!(v["totalLag"], 10);
    let msg = err(&call(
        &c,
        "describe_consumer_group",
        json!({"connectionId": "c1", "groupId": "missing"}),
    )
    .await);
    assert!(msg.starts_with("not_found:"), "{msg}");
    let msg = err(&call(
        &c,
        "get_consumer_groups_lag",
        json!({"connectionId": "c1", "groupIds": []}),
    )
    .await);
    assert!(msg.starts_with("invalid_argument:"), "{msg}");
}

#[tokio::test]
async fn observation_records_members_leaving_and_joining() {
    let fake = Fake::new(SessionAccess::Approved);
    fake.groups.lock().unwrap().extend([
        group(
            "Stable",
            vec![member("a", "10.0.0.1"), member("b", "10.0.0.2")],
        ),
        group("PreparingRebalance", vec![member("a", "10.0.0.1")]),
        group(
            "Stable",
            vec![member("a", "10.0.0.1"), member("c", "10.0.0.2")],
        ),
    ]);
    let e = env(fake).await;
    let c = client(&e.ep).await;
    let v = ok(call(
        &c,
        "observe_consumer_group",
        json!({"connectionId": "c1", "groupId": "orders-svc", "durationSeconds": 10}),
    )
    .await);
    assert_eq!(v["samples"], 6);
    assert_eq!(v["summary"]["joins"], 1);
    assert_eq!(v["summary"]["leaves"], 1);
    assert_eq!(v["summary"]["stateChanges"], 2);
    assert_eq!(v["lagStart"], 10);
    let left = v["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["kind"] == "memberLeft")
        .unwrap();
    assert_eq!(
        (left["memberId"].as_str(), left["host"].as_str()),
        (Some("b"), Some("10.0.0.2"))
    );
    let msg = err(&call(
        &c,
        "observe_consumer_group",
        json!({"connectionId": "c1", "groupId": "orders-svc", "durationSeconds": 51}),
    )
    .await);
    assert!(msg.starts_with("invalid_argument:"), "{msg}");
}

#[tokio::test]
async fn huge_observation_is_cut_to_the_size_limit() {
    let fake = Fake::new(SessionAccess::Approved);
    let crowd: Vec<Member> = (0..200)
        .map(|i| member(&format!("m{i}-{}", "x".repeat(300)), &"h".repeat(300)))
        .collect();
    {
        let mut q = fake.groups.lock().unwrap();
        for _ in 0..5 {
            q.push_back(group("Stable", vec![member("a", "10.0.0.1")]));
            q.push_back(group("Stable", crowd.clone()));
        }
        q.push_back(group("Stable", vec![member("a", "10.0.0.1")]));
    }
    let e = env(fake).await;
    let c = client(&e.ep).await;
    let r = call(
        &c,
        "observe_consumer_group",
        json!({"connectionId": "c1", "groupId": "orders-svc", "durationSeconds": 20}),
    )
    .await;
    let size = text(&r).len();
    assert!(
        size <= glafira_mcp::rules::MAX_RESPONSE_BYTES + 200,
        "{size}"
    );
    let v = ok(r);
    assert_eq!(v["truncated"], true);
    assert!(
        v["summary"]["joins"].as_u64().unwrap() >= 200,
        "сводка считает все события"
    );
}

#[tokio::test]
async fn long_group_lists_are_truncated_to_the_size_limit() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = client(&e.ep).await;
    let r = call(
        &c,
        "get_topic_consumer_groups",
        json!({"connectionId": "c1", "topic": "busy"}),
    )
    .await;
    assert!(
        text(&r).len() <= glafira_mcp::rules::HARD_LIMIT_BYTES,
        "{}",
        text(&r).len()
    );
    let v = ok(r);
    assert_eq!(v["truncated"], true);
    assert!(v["groups"].as_array().unwrap().len() < 5000);
    let v = ok(call(
        &c,
        "get_topic_consumer_groups",
        json!({"connectionId": "c1", "topic": "orders"}),
    )
    .await);
    assert_eq!(v["truncated"], false);

    let r = call(
        &c,
        "describe_consumer_group",
        json!({"connectionId": "c1", "groupId": "crowd"}),
    )
    .await;
    assert!(
        text(&r).len() <= glafira_mcp::rules::HARD_LIMIT_BYTES,
        "{}",
        text(&r).len()
    );
    let v = ok(r);
    assert_eq!(v["truncated"], true);
    assert!(v["members"].as_array().unwrap().len() < 2000);
    assert_eq!(v["totalLag"], 10, "лаг посчитан до обрезки");
}

#[tokio::test]
async fn response_that_cannot_be_cut_is_refused() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = client(&e.ep).await;
    // initial и final огромной группы не обрезаются: страховка не даёт отправить 2,8 МБ.
    let r = call(
        &c,
        "observe_consumer_group",
        json!({"connectionId": "c1", "groupId": "crowd", "durationSeconds": 10}),
    )
    .await;
    let msg = err(&r);
    assert!(msg.starts_with("response_too_large:"), "{msg}");
    let recent = e.audit.recent(1);
    assert_eq!(recent[0]["code"], "response_too_large");
}

#[tokio::test]
async fn observation_keeps_its_window_when_reads_are_slow() {
    let fake = Fake::new(SessionAccess::Approved);
    // Каждое описание — 80 «секунд» при единице 10 мс; окно — 10 «секунд».
    *fake.describe_delay.lock().unwrap() = Duration::from_millis(80);
    let e = env(fake).await;
    let c = client(&e.ep).await;
    let started = std::time::Instant::now();
    let v = ok(call(
        &c,
        "observe_consumer_group",
        json!({"connectionId": "c1", "groupId": "orders-svc", "durationSeconds": 10}),
    )
    .await);
    // Без расписания от начала: 6 чтений по 80 мс и 5 пауз по 20 мс — больше 580 мс.
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_millis(400), "{elapsed:?}");
    assert!(v["samples"].as_u64().unwrap() < 6, "{v}");
}

#[tokio::test]
async fn one_observation_per_session() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = Arc::new(client(&e.ep).await);
    let long = {
        let c = c.clone();
        tokio::spawn(async move {
            call(
                &c,
                "observe_consumer_group",
                json!({"connectionId": "c1", "groupId": "orders-svc", "durationSeconds": 50}),
            )
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let msg = err(&call(
        &c,
        "observe_consumer_group",
        json!({"connectionId": "c1", "groupId": "orders-svc"}),
    )
    .await);
    assert!(msg.starts_with("busy:"), "{msg}");
    ok(long.await.unwrap());
}

#[tokio::test]
async fn disconnect_and_shutdown_close_sessions() {
    let fake = Fake::new(SessionAccess::Approved);
    let e = env(fake.clone()).await;
    let c = client(&e.ep).await;
    ok(call(&c, "list_connections", json!({})).await);
    let id = e.handle.sessions()[0].id.clone();
    assert!(e.handle.disconnect(&id));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(e.handle.sessions().is_empty());
    assert_eq!(fake.closed.lock().unwrap().as_slice(), [id]);
    let c2 = client(&e.ep).await;
    e.handle.shutdown().await;
    assert!(
        c2.call_tool(CallToolRequestParams::new("list_connections"))
            .await
            .is_err()
    );
    assert!(
        endpoint::connect(&e.ep).await.is_err(),
        "сервер больше не слушает"
    );
}

#[tokio::test]
async fn shutdown_does_not_wait_for_a_silent_handshake() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    // Соединение без строки рукопожатия: сервер ждал бы её до 5 с.
    let _silent = endpoint::connect(&e.ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let started = std::time::Instant::now();
    e.handle.shutdown().await;
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_secs(1), "{elapsed:?}");
}

#[tokio::test]
async fn wrong_token_is_rejected_before_mcp() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let mut s = endpoint::connect(&e.ep).await.unwrap();
    let r = handshake::client(&mut s, "wrong").await;
    assert!(
        matches!(r, Err(handshake::HandshakeError::Rejected { ref code, .. }) if code == "bad_token")
    );
}
