//! Сервер целиком: настоящий сокет (pipe на Windows), рукопожатие, MCP-клиент rmcp и
//! фальшивый Backend.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use glafira_mcp::health::*;
use glafira_mcp::model::*;
use glafira_mcp::proposals::*;
use glafira_mcp::{
    AuditLog, Backend, BackendError, BoxFuture, GatewayOptions, McpServerHandle, endpoint,
    handshake,
};
use rmcp::model::{
    CallToolRequest, CallToolRequestParams, CallToolResult, ClientCapabilities, ClientConfig,
    ClientRequest, Implementation,
};
use rmcp::service::{PeerRequestOptions, RunningService};
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
    /// Задержка обзора кластера и лага групп.
    overview_delay: Mutex<Duration>,
    lag_delay: Mutex<Duration>,
    /// Операций чтения сейчас и наибольшее число одновременных.
    active: AtomicUsize,
    peak: AtomicUsize,
    /// Операции проверки и замечаний, которые дошли до Backend.
    health_ops: Mutex<Vec<HealthOp>>,
    check_delay: Mutex<Duration>,
    /// Находки с длинными пояснениями (предел 100 КБ).
    big_findings: AtomicBool,
    /// Сколько длинных замечаний отдаёт `list_annotations` (0 — одно короткое).
    many_notes: AtomicUsize,
    /// Операции предложений, которые дошли до Backend.
    proposal_ops: Mutex<Vec<ProposalOp>>,
    /// Сколько длинных предложений отдаёт `list_proposals` (0 — одно короткое).
    many_proposals: AtomicUsize,
    /// Подключение только для чтения: `propose_change` отвечает `readOnly: true`.
    read_only: AtomicBool,
}

/// Считает одновременные операции чтения на любом выходе из `read`.
struct InFlight<'a>(&'a Fake);

impl<'a> InFlight<'a> {
    fn new(f: &'a Fake) -> Self {
        let now = f.active.fetch_add(1, Ordering::SeqCst) + 1;
        f.peak.fetch_max(now, Ordering::SeqCst);
        Self(f)
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn pause(d: &Mutex<Duration>) {
    let d = *d.lock().unwrap();
    if !d.is_zero() {
        tokio::time::sleep(d).await;
    }
}

impl Fake {
    fn new(access: SessionAccess) -> Arc<Self> {
        Arc::new(Self {
            access: Mutex::new(access),
            seen: Mutex::new(Vec::new()),
            closed: Mutex::new(Vec::new()),
            groups: Mutex::new(VecDeque::new()),
            describe_delay: Mutex::new(Duration::ZERO),
            overview_delay: Mutex::new(Duration::ZERO),
            lag_delay: Mutex::new(Duration::ZERO),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            health_ops: Mutex::new(Vec::new()),
            check_delay: Mutex::new(Duration::ZERO),
            big_findings: AtomicBool::new(false),
            many_notes: AtomicUsize::new(0),
            proposal_ops: Mutex::new(Vec::new()),
            many_proposals: AtomicUsize::new(0),
            read_only: AtomicBool::new(false),
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

/// Замечание с текстом в 2000 символов; `i` — и номер, и время обновления.
fn long_note(i: i64) -> AnnotationView {
    AnnotationView {
        id: format!("n{i}"),
        target: Target {
            kind: TargetKind::Topic,
            name: Some("orders".into()),
        },
        severity: Severity::Warning,
        title: format!("Note {i}"),
        body: "x".repeat(2000),
        related_rule_id: None,
        client: Some("claude-ai".into()),
        status: "open".into(),
        created_at_ms: i,
        updated_at_ms: i,
    }
}

fn long_proposal(i: i64) -> ProposalView {
    ProposalView {
        id: format!("p{i}"),
        severity: Severity::Warning,
        title: format!("Proposal {i}"),
        rationale: "x".repeat(2000),
        target: Target {
            kind: TargetKind::Topic,
            name: Some("orders".into()),
        },
        change: ChangeSpec::AddPartitions {
            topic: "orders".into(),
            new_total: 6,
        },
        related_rule_id: None,
        related_finding_key: None,
        client: Some("claude-ai".into()),
        status: "open".into(),
        created_at_ms: i,
        updated_at_ms: i,
        closed_at_ms: None,
        applied_as_proposed: None,
        reject_reason: None,
    }
}

fn finding(
    rule: &str,
    severity: Severity,
    kind: TargetKind,
    name: &str,
    ignored: bool,
    details: String,
) -> HealthFinding {
    HealthFinding {
        key: format!("{rule}:{name}"),
        rule_id: rule.into(),
        severity,
        target: Target {
            kind,
            name: Some(name.into()),
        },
        title: format!("{rule} {name}"),
        details,
        params: Default::default(),
        ignored,
    }
}

fn sample_run(big: bool) -> HealthRun {
    let findings = if big {
        (0..300)
            .map(|i| {
                finding(
                    "topic.replicationFactorOne",
                    Severity::Critical,
                    TargetKind::Topic,
                    &format!("t{i}"),
                    false,
                    "x".repeat(1000),
                )
            })
            .collect()
    } else {
        vec![
            finding(
                "topic.replicationFactorOne",
                Severity::Critical,
                TargetKind::Topic,
                "orders",
                false,
                "d".into(),
            ),
            finding(
                "topic.replicationFactorOne",
                Severity::Critical,
                TargetKind::Topic,
                "legacy",
                true,
                "d".into(),
            ),
            finding(
                "group.abandoned",
                Severity::Warning,
                TargetKind::Group,
                "orders-svc",
                false,
                "d".into(),
            ),
            finding(
                "group.rebalancing",
                Severity::Info,
                TargetKind::Group,
                "orders-svc",
                false,
                "d".into(),
            ),
        ]
    };
    HealthRun {
        run_id: "r1".into(),
        started_at_ms: 1,
        duration_ms: 2,
        trigger: json!({ "kind": "ui" }),
        environment: "prod".into(),
        incomplete: false,
        reused: true,
        scope: HealthScope::default(),
        findings,
    }
}

impl Backend for Fake {
    fn access(&self, session: &SessionView) -> SessionAccess {
        self.seen.lock().unwrap().push(session.clone());
        *self.access.lock().unwrap()
    }

    fn read(&self, op: ReadOp) -> BoxFuture<'_, Result<ReadOutput, BackendError>> {
        Box::pin(async move {
            let _in_flight = InFlight::new(self);
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
                ReadOp::ClusterOverview { .. } => ReadOutput::Cluster({
                    pause(&self.overview_delay).await;
                    ClusterOverview {
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
                    }
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
                    pause(&self.describe_delay).await;
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
                ReadOp::GroupsLag { group_ids, .. } => ReadOutput::Lags({
                    pause(&self.lag_delay).await;
                    group_ids
                        .into_iter()
                        .map(|g| GroupLag {
                            group_id: g,
                            total_lag: Some(10),
                            topics: vec!["orders".into()],
                            error: None,
                        })
                        .collect()
                }),
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

    fn health(&self, op: HealthOp) -> BoxFuture<'_, Result<HealthOutput, BackendError>> {
        Box::pin(async move {
            self.health_ops.lock().unwrap().push(op.clone());
            match op {
                HealthOp::Check { run_id, .. } => {
                    pause(&self.check_delay).await;
                    if run_id.as_deref() == Some("old") {
                        return Err(BackendError::not_found(
                            "The check was replaced; run health_check again",
                        ));
                    }
                    Ok(HealthOutput::Run(sample_run(
                        self.big_findings.load(Ordering::SeqCst),
                    )))
                }
                HealthOp::Annotate { note, .. } => {
                    if note.target.name.as_deref() == Some("missing") {
                        return Err(BackendError::not_found("Topic missing not found"));
                    }
                    if note.title == "too many" {
                        return Err(BackendError::new("limit_reached", "At most 200 open notes"));
                    }
                    Ok(HealthOutput::Annotated {
                        annotation_id: "n1".into(),
                        created: true,
                    })
                }
                HealthOp::ListAnnotations { .. } => {
                    let many = self.many_notes.load(Ordering::SeqCst);
                    if many > 0 {
                        // От старых к новым: без сортировки в ответ попали бы самые старые.
                        return Ok(HealthOutput::Annotations(
                            (0..many).map(|i| long_note(i as i64)).collect(),
                        ));
                    }
                    Ok(HealthOutput::Annotations(vec![AnnotationView {
                        id: "n1".into(),
                        target: Target {
                            kind: TargetKind::Topic,
                            name: Some("orders".into()),
                        },
                        severity: Severity::Warning,
                        title: "Lag grows".into(),
                        body: "b".into(),
                        related_rule_id: None,
                        client: Some("claude-ai".into()),
                        status: "open".into(),
                        created_at_ms: 1,
                        updated_at_ms: 1,
                    }]))
                }
                HealthOp::RemoveAnnotation { annotation_id, .. } => {
                    if annotation_id == "foreign" {
                        Err(BackendError::new(
                            "not_allowed",
                            "Only notes created by this client can be removed",
                        ))
                    } else {
                        Ok(HealthOutput::Removed)
                    }
                }
            }
        })
    }

    fn proposals(&self, op: ProposalOp) -> BoxFuture<'_, Result<ProposalOutput, BackendError>> {
        Box::pin(async move {
            self.proposal_ops.lock().unwrap().push(op.clone());
            if op.connection_id() != "c1" {
                return Err(BackendError::not_allowed());
            }
            match op {
                ProposalOp::Propose { proposal, .. } => {
                    if proposal.change.target_name() == "missing" {
                        return Err(BackendError::not_found(
                            "There is no topic missing in this cluster",
                        ));
                    }
                    if proposal.title == "too many" {
                        return Err(BackendError::new(
                            "limit_reached",
                            "At most 50 open proposals",
                        ));
                    }
                    Ok(ProposalOutput::Proposed {
                        proposal_id: "p1".into(),
                        created: proposal.title != "again",
                        read_only: self.read_only.load(Ordering::SeqCst),
                    })
                }
                ProposalOp::List { .. } => {
                    let many = self.many_proposals.load(Ordering::SeqCst);
                    if many > 0 {
                        // От старых к новым: без сортировки в ответ попали бы самые старые.
                        return Ok(ProposalOutput::Proposals(
                            (0..many).map(|i| long_proposal(i as i64)).collect(),
                        ));
                    }
                    let mut applied = long_proposal(1);
                    applied.rationale = "r".into();
                    applied.status = "applied".into();
                    applied.closed_at_ms = Some(2);
                    applied.applied_as_proposed = Some(false);
                    Ok(ProposalOutput::Proposals(vec![applied]))
                }
                ProposalOp::Withdraw { proposal_id, .. } => match proposal_id.as_str() {
                    "foreign" => Err(BackendError::new(
                        "not_allowed",
                        "Only proposals created by this client can be withdrawn",
                    )),
                    "closed" => Err(BackendError::invalid_argument(
                        "The proposal is already applied",
                    )),
                    _ => Ok(ProposalOutput::Withdrawn),
                },
            }
        })
    }
}

struct Env {
    _dir: tempfile::TempDir,
    ep: String,
    handle: McpServerHandle,
    audit: Arc<AuditLog>,
}

async fn env(fake: Arc<Fake>) -> Env {
    env_with(fake, |_| {}).await
}

async fn env_with(fake: Arc<Fake>, tune: impl FnOnce(&mut GatewayOptions)) -> Env {
    let dir = tempfile::tempdir().unwrap();
    let ep = endpoint::choose(
        dir.path(),
        &dir.path().file_name().unwrap().to_string_lossy(),
    );
    let audit = Arc::new(AuditLog::new(dir.path()));
    let mut opts = GatewayOptions::new("glafira-kafka", "0.3.0");
    opts.observe_unit = Duration::from_millis(10);
    tune(&mut opts);
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

/// Клиент с заданными именем и версией в `initialize`.
async fn named_client(
    ep: &str,
    name: &str,
    version: &str,
) -> RunningService<RoleClient, ClientConfig> {
    let mut s = endpoint::connect(&ep.to_string()).await.unwrap();
    handshake::client(&mut s, TOKEN).await.unwrap();
    ClientConfig::new(
        ClientCapabilities::default(),
        Implementation::new(name, version),
    )
    .serve(s)
    .await
    .unwrap()
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
async fn exposes_exactly_the_tools() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = client(&e.ep).await;
    let info = c.peer_info().unwrap();
    let instructions = info.instructions.as_deref().unwrap();
    assert!(instructions.contains("propose_change"));
    assert!(instructions.contains("cannot be proposed"));
    assert!(instructions.contains("start with health_check"));
    // С 0.3.0 платного тарифа нет: ни инструкции, ни описания инструментов не упоминают Pro
    // (слово целиком: «proposal» и «appliedAsProposed» — не тариф).
    let mentions_pro = |s: &str| {
        s.split(|c: char| !c.is_ascii_alphanumeric())
            .any(|w| w == "Pro")
    };
    assert!(!mentions_pro(glafira_mcp::server::INSTRUCTIONS));
    assert!(!mentions_pro(instructions));
    let mut tools = c.list_all_tools().await.unwrap();
    tools.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
    assert_eq!(
        names,
        [
            "annotate",
            "describe_consumer_group",
            "describe_topic",
            "get_broker_config",
            "get_cluster_overview",
            "get_consumer_groups_lag",
            "get_topic_configs",
            "get_topic_consumer_groups",
            "get_topics_message_counts",
            "health_check",
            "list_annotations",
            "list_connections",
            "list_consumer_groups",
            "list_proposals",
            "list_topics",
            "observe_consumer_group",
            "propose_change",
            "remove_annotation",
            "withdraw_proposal",
        ]
    );
    for t in &tools {
        let a = t.annotations.as_ref().unwrap();
        // Замечания и предложения пишутся в Glafira (не в кластер): только они не read-only.
        let writes = matches!(
            t.name.as_ref(),
            "annotate" | "remove_annotation" | "propose_change" | "withdraw_proposal"
        );
        assert_eq!(
            (a.read_only_hint, a.destructive_hint),
            (Some(!writes), Some(false)),
            "{}",
            t.name
        );
        if writes {
            assert_eq!(a.idempotent_hint, Some(true), "{}", t.name);
        }
        assert!(
            t.description.as_deref().is_some_and(|d| d.len() > 30),
            "{}",
            t.name
        );
        assert!(
            !mentions_pro(t.description.as_deref().unwrap_or_default()),
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
async fn client_name_is_trimmed_and_capped() {
    let fake = Fake::new(SessionAccess::Approved);
    let e = env(fake.clone()).await;
    let _blank = named_client(&e.ep, " \t ", "1.0").await;
    let _long = named_client(&e.ep, &format!("  {}", "ж".repeat(300)), &"v".repeat(300)).await;
    let seen = fake.seen.lock().unwrap().clone();
    assert_eq!(seen[0].client, None, "пустое имя — как отсутствующее");
    let c = seen.last().unwrap().client.clone().unwrap();
    assert_eq!(c.name, "ж".repeat(128));
    assert_eq!(c.version, "v".repeat(128));
    let mut names: Vec<Option<String>> = e
        .handle
        .sessions()
        .into_iter()
        .map(|s| s.client_name)
        .collect();
    names.sort();
    assert_eq!(names, [None, Some("ж".repeat(128))]);
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
    assert!(
        msg.contains(&format!(
            "at most {} per minute",
            glafira_mcp::limits::CALLS_PER_MINUTE
        )),
        "{msg}"
    );
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
async fn slow_first_description_and_start_lag_stay_within_the_window() {
    let fake = Fake::new(SessionAccess::Approved);
    // Стартовый лаг — 6 с, то есть 600 «секунд» при единице 10 мс: дольше окна его не ждут.
    *fake.lag_delay.lock().unwrap() = Duration::from_secs(6);
    let e = env(fake.clone()).await;
    let c = client(&e.ep).await;
    let started = std::time::Instant::now();
    let v = ok(call(
        &c,
        "observe_consumer_group",
        json!({"connectionId": "c1", "groupId": "orders-svc", "durationSeconds": 10}),
    )
    .await);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(v["lagStart"], Value::Null);
    assert!(v["samples"].as_u64().unwrap() >= 1, "{v}");

    // Первое описание группы не приходит: ошибка в пределах окна, а не через таймаут бэкенда.
    *fake.lag_delay.lock().unwrap() = Duration::ZERO;
    *fake.describe_delay.lock().unwrap() = Duration::from_secs(6);
    let started = std::time::Instant::now();
    let msg = err(&call(
        &c,
        "observe_consumer_group",
        json!({"connectionId": "c1", "groupId": "orders-svc", "durationSeconds": 10}),
    )
    .await);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    assert!(msg.starts_with("TIMEOUT:"), "{msg}");
}

#[tokio::test]
async fn too_large_group_is_refused_without_waiting_for_the_window() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = client(&e.ep).await;
    let started = std::time::Instant::now();
    // Окно 50 «секунд» = 500 мс: отказ приходит после первого образца.
    let msg = err(&call(
        &c,
        "observe_consumer_group",
        json!({"connectionId": "c1", "groupId": "crowd", "durationSeconds": 50}),
    )
    .await);
    assert!(msg.starts_with("response_too_large:"), "{msg}");
    assert!(
        started.elapsed() < Duration::from_millis(300),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn kafka_operations_are_limited_per_session_and_globally() {
    let fake = Fake::new(SessionAccess::Approved);
    *fake.overview_delay.lock().unwrap() = Duration::from_millis(150);
    let e = env(fake.clone()).await;
    let overview = |c: Arc<RunningService<RoleClient, ()>>| {
        tokio::spawn(async move {
            ok(call(&c, "get_cluster_overview", json!({"connectionId": "c1"})).await)
        })
    };
    let one = Arc::new(client(&e.ep).await);
    let calls: Vec<_> = (0..5).map(|_| overview(one.clone())).collect();
    for h in calls {
        h.await.unwrap();
    }
    assert_eq!(fake.peak.load(Ordering::SeqCst), 2, "не больше 2 на сессию");

    fake.peak.store(0, Ordering::SeqCst);
    let mut clients = vec![one];
    for _ in 0..2 {
        clients.push(Arc::new(client(&e.ep).await));
    }
    let calls: Vec<_> = clients
        .iter()
        .flat_map(|c| (0..3).map(|_| overview(c.clone())).collect::<Vec<_>>())
        .collect();
    for h in calls {
        h.await.unwrap();
    }
    assert_eq!(
        fake.peak.load(Ordering::SeqCst),
        4,
        "не больше 4 на все сессии"
    );
}

#[tokio::test]
async fn cancelled_observation_ends_early_and_frees_the_session() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = client(&e.ep).await;
    let args = json!({"connectionId": "c1", "groupId": "orders-svc", "durationSeconds": 50});
    let Value::Object(args) = args else {
        unreachable!()
    };
    let request = ClientRequest::CallToolRequest(CallToolRequest::new(
        CallToolRequestParams::new("observe_consumer_group").with_arguments(args),
    ));
    let handle = c
        .send_cancellable_request(request, PeerRequestOptions::no_options())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle.cancel(Some("test".into())).await.unwrap();
    // Окно — 500 мс: отменённое наблюдение попадает в журнал раньше.
    let mut logged = None;
    for _ in 0..40 {
        logged = e
            .audit
            .recent(5)
            .into_iter()
            .find(|r| r["tool"] == "observe_consumer_group");
        if logged.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let logged = logged.expect("наблюдение завершилось после отмены");
    assert!(logged["ms"].as_u64().unwrap() < 300, "{logged}");
    // Сессия снова может наблюдать: отметка «идёт наблюдение» снята.
    ok(call(
        &c,
        "observe_consumer_group",
        json!({"connectionId": "c1", "groupId": "orders-svc", "durationSeconds": 10}),
    )
    .await);
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

#[tokio::test]
async fn health_check_summarizes_filters_and_pages() {
    let fake = Fake::new(SessionAccess::Approved);
    let e = env(fake.clone()).await;
    let c = client(&e.ep).await;
    let v = ok(call(
        &c,
        "health_check",
        json!({"connectionId": "c1", "minSeverity": "warning", "includeIgnored": false, "limit": 1}),
    )
    .await);
    assert_eq!(
        v["summary"][0],
        json!({"ruleId": "topic.replicationFactorOne", "severity": "critical", "count": 2})
    );
    assert_eq!(v["summary"].as_array().unwrap().len(), 3);
    // Скрытая и info не попали в страницу.
    assert_eq!(v["findings"]["total"], 2);
    assert_eq!(
        v["findings"]["items"][0]["target"],
        json!({"kind": "topic", "name": "orders"})
    );
    assert_eq!(v["findings"]["nextOffset"], 1);
    assert_eq!(
        (v["reused"].as_bool(), v["truncated"].as_bool()),
        (Some(true), Some(false))
    );
    let ops = fake.health_ops.lock().unwrap().clone();
    let HealthOp::Check {
        connection_id,
        max_age_ms,
        run_id,
        session,
    } = &ops[0]
    else {
        panic!("{ops:?}")
    };
    assert_eq!(
        (connection_id.as_str(), *max_age_ms, run_id.as_deref()),
        ("c1", 300_000, None)
    );
    assert!(!session.id.is_empty());
    for (args, code) in [
        (
            json!({"connectionId": "c1", "maxAgeSeconds": 4000}),
            "invalid_argument",
        ),
        (
            json!({"connectionId": "c1", "limit": 201}),
            "invalid_argument",
        ),
        (json!({"connectionId": "c1", "runId": "old"}), "not_found"),
    ] {
        let msg = err(&call(&c, "health_check", args).await);
        assert!(msg.starts_with(code), "{msg}");
    }
}

#[tokio::test]
async fn health_check_fits_the_size_limit() {
    let fake = Fake::new(SessionAccess::Approved);
    fake.big_findings.store(true, Ordering::SeqCst);
    let e = env(fake).await;
    let c = client(&e.ep).await;
    let v = ok(call(
        &c,
        "health_check",
        json!({"connectionId": "c1", "limit": 200}),
    )
    .await);
    assert_eq!(v["truncated"], true);
    let shown = v["findings"]["items"].as_array().unwrap().len();
    assert!(shown > 0 && shown < 200, "{shown}");
    assert_eq!(v["findings"]["nextOffset"], shown);
    assert!(serde_json::to_vec(&v).unwrap().len() <= 100 * 1024);
}

#[tokio::test]
async fn health_check_gives_up_waiting() {
    let fake = Fake::new(SessionAccess::Approved);
    *fake.check_delay.lock().unwrap() = Duration::from_secs(5);
    let e = env_with(fake, |o| o.check_wait = Duration::from_millis(100)).await;
    let c = client(&e.ep).await;
    let msg = err(&call(&c, "health_check", json!({"connectionId": "c1"})).await);
    assert!(msg.starts_with("check_in_progress_timeout"), "{msg}");
}

#[tokio::test]
async fn paging_by_run_id_does_not_take_kafka_slots() {
    let fake = Fake::new(SessionAccess::Approved);
    *fake.overview_delay.lock().unwrap() = Duration::from_secs(2);
    let e = env_with(fake.clone(), |o| o.check_wait = Duration::from_millis(300)).await;
    let c = Arc::new(client(&e.ep).await);
    // Оба слота сессии заняты медленными чтениями кластера.
    let busy: Vec<_> = (0..2)
        .map(|_| {
            let c = c.clone();
            tokio::spawn(async move {
                ok(call(&c, "get_cluster_overview", json!({"connectionId": "c1"})).await)
            })
        })
        .collect();
    for _ in 0..100 {
        if fake.active.load(Ordering::SeqCst) == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(fake.active.load(Ordering::SeqCst), 2);
    // Страница прошлой проверки читается из файла Glafira — слот Kafka не нужен.
    ok(call(
        &c,
        "health_check",
        json!({"connectionId": "c1", "runId": "r1"}),
    )
    .await);
    // Новая проверка ходит в кластер: ждёт слот и не дожидается.
    let msg = err(&call(&c, "health_check", json!({"connectionId": "c1"})).await);
    assert!(msg.starts_with("check_in_progress_timeout"), "{msg}");
    for h in busy {
        h.await.unwrap();
    }
}

fn note(target: Value, title: &str) -> Value {
    json!({
        "connectionId": "c1",
        "target": target,
        "severity": "warning",
        "title": title,
        "body": "the body of the note",
        "relatedRuleId": "group.abandoned",
    })
}

#[tokio::test]
async fn annotations_need_no_license() {
    let fake = Fake::new(SessionAccess::Approved);
    let e = env(fake.clone()).await;
    let c = client(&e.ep).await;
    ok(call(
        &c,
        "annotate",
        note(json!({"kind": "topic", "name": "orders"}), "t"),
    )
    .await);
    ok(call(&c, "list_annotations", json!({"connectionId": "c1"})).await);
    ok(call(
        &c,
        "remove_annotation",
        json!({"connectionId": "c1", "annotationId": "n1"}),
    )
    .await);
    assert_eq!(fake.health_ops.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn annotate_validates_and_keeps_text_out_of_the_log() {
    let fake = Fake::new(SessionAccess::Approved);
    let e = env(fake.clone()).await;
    let c = client(&e.ep).await;
    for (args, code) in [
        (note(json!({"kind": "group"}), "t"), "invalid_argument"),
        (
            note(json!({"kind": "topic", "name": "orders"}), "  "),
            "invalid_argument",
        ),
        (
            note(json!({"kind": "topic", "name": "missing"}), "t"),
            "not_found",
        ),
        (
            note(json!({"kind": "topic", "name": "orders"}), "too many"),
            "limit_reached",
        ),
    ] {
        let msg = err(&call(&c, "annotate", args).await);
        assert!(msg.starts_with(code), "{msg}");
    }
    let v = ok(call(
        &c,
        "annotate",
        note(json!({"kind": "topic", "name": "orders"}), "Lag grows"),
    )
    .await);
    assert_eq!(v, json!({"annotationId": "n1", "created": true}));
    let ops = fake.health_ops.lock().unwrap().clone();
    let Some(HealthOp::Annotate { note, session, .. }) = ops.last() else {
        panic!("{ops:?}")
    };
    assert_eq!(
        (note.title.as_str(), note.related_rule_id.as_deref()),
        ("Lag grows", Some("group.abandoned"))
    );
    assert!(!session.id.is_empty());
    let line = e.audit.recent(1)[0].to_string();
    assert!(
        line.contains("\"annotate\"") && line.contains("orders"),
        "{line}"
    );
    assert!(
        !line.contains("Lag grows") && !line.contains("the body"),
        "{line}"
    );
}

#[tokio::test]
async fn annotations_are_listed_and_removed() {
    let fake = Fake::new(SessionAccess::Approved);
    let e = env(fake.clone()).await;
    let c = client(&e.ep).await;
    let v = ok(call(
        &c,
        "list_annotations",
        json!({"connectionId": "c1", "status": "all"}),
    )
    .await);
    assert_eq!(
        (
            v["items"][0]["id"].as_str(),
            v["items"][0]["client"].as_str()
        ),
        (Some("n1"), Some("claude-ai"))
    );
    assert!(matches!(
        fake.health_ops.lock().unwrap()[0],
        HealthOp::ListAnnotations {
            status: StatusFilter::All,
            ..
        }
    ));
    let v = ok(call(
        &c,
        "remove_annotation",
        json!({"connectionId": "c1", "annotationId": "n1"}),
    )
    .await);
    assert_eq!(v, json!({"removed": true}));
    let msg = err(&call(
        &c,
        "remove_annotation",
        json!({"connectionId": "c1", "annotationId": "foreign"}),
    )
    .await);
    assert!(msg.starts_with("not_allowed"), "{msg}");
}

#[tokio::test]
async fn long_annotation_list_is_cut_to_the_newest() {
    let fake = Fake::new(SessionAccess::Approved);
    fake.many_notes.store(80, Ordering::SeqCst);
    let e = env(fake).await;
    let c = client(&e.ep).await;
    let v = ok(call(&c, "list_annotations", json!({"connectionId": "c1"})).await);
    assert!(serde_json::to_vec(&v).unwrap().len() <= 100 * 1024);
    assert_eq!(v["truncated"], true);
    let hint = v["hint"].as_str().unwrap();
    assert!(hint.contains("100 KB") && hint.contains("most recently updated"));
    assert!(!hint.contains("offset"), "{hint}");
    let ids: Vec<&str> = v["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["id"].as_str().unwrap())
        .collect();
    assert!(!ids.is_empty() && ids.len() < 80, "{}", ids.len());
    // Остались самые новые, от новых к старым.
    let expected: Vec<String> = (0..80)
        .rev()
        .take(ids.len())
        .map(|i| format!("n{i}"))
        .collect();
    assert_eq!(ids, expected);
}

#[tokio::test]
async fn short_annotation_list_is_not_marked_truncated() {
    let fake = Fake::new(SessionAccess::Approved);
    fake.many_notes.store(3, Ordering::SeqCst);
    let e = env(fake).await;
    let c = client(&e.ep).await;
    let v = ok(call(&c, "list_annotations", json!({"connectionId": "c1"})).await);
    assert_eq!(v["truncated"], false);
    assert!(v.get("hint").is_none(), "{v}");
    // Новые вперёд.
    assert_eq!(v["items"][0]["id"], "n2");
    assert_eq!(v["items"][2]["id"], "n0");
}

fn proposal(topic: &str, title: &str) -> Value {
    json!({
        "connectionId": "c1",
        "severity": "warning",
        "title": title,
        "rationale": "acks=all producers need two in-sync replicas",
        "change": {
            "kind": "alterTopicConfigs",
            "topic": topic,
            "changes": [{ "name": "min.insync.replicas", "value": "2" }]
        },
        "relatedRuleId": "topic.minIsrOne",
        "relatedFindingKey": "topic.minIsrOne:topic:orders",
    })
}

#[tokio::test]
async fn propose_change_validates_and_keeps_text_out_of_the_log() {
    let fake = Fake::new(SessionAccess::Approved);
    let e = env(fake.clone()).await;
    let c = client(&e.ep).await;
    let mut empty_rationale = proposal("orders", "t");
    empty_rationale["rationale"] = json!("  ");
    let mut long_key = proposal("orders", "t");
    long_key["relatedFindingKey"] = json!("k".repeat(201));
    for (args, code) in [
        (proposal("orders", "  "), "invalid_argument"),
        (empty_rationale, "invalid_argument"),
        (long_key, "invalid_argument"),
        (proposal("missing", "t"), "not_found"),
        (proposal("orders", "too many"), "limit_reached"),
    ] {
        let msg = err(&call(&c, "propose_change", args).await);
        assert!(msg.starts_with(code), "{msg}");
    }
    // Неизвестный вид изменения не проходит схему параметров.
    let mut unknown = proposal("orders", "t");
    unknown["change"] = json!({ "kind": "deleteTopic", "topic": "orders" });
    let msg = err(&call(&c, "propose_change", unknown).await);
    assert!(!msg.is_empty());
    assert!(fake.proposal_ops.lock().unwrap().iter().all(|op| !matches!(
        op,
        ProposalOp::Propose { proposal: np, .. } if np.title == "t" && np.change.target_name() == "orders"
    )));

    let v = ok(call(
        &c,
        "propose_change",
        proposal("orders", "Raise min.insync.replicas"),
    )
    .await);
    assert_eq!(
        v,
        json!({ "proposalId": "p1", "created": true, "readOnly": false })
    );
    let ops = fake.proposal_ops.lock().unwrap().clone();
    let Some(ProposalOp::Propose {
        proposal: np,
        session,
        ..
    }) = ops.last()
    else {
        panic!("{ops:?}")
    };
    assert_eq!(np.title, "Raise min.insync.replicas");
    assert_eq!(np.related_rule_id.as_deref(), Some("topic.minIsrOne"));
    assert_eq!(
        np.related_finding_key.as_deref(),
        Some("topic.minIsrOne:topic:orders")
    );
    assert_eq!(
        np.change,
        ChangeSpec::AlterTopicConfigs {
            topic: "orders".into(),
            changes: vec![ConfigChangeSpec {
                name: "min.insync.replicas".into(),
                value: Some("2".into())
            }]
        }
    );
    assert!(!session.id.is_empty());
    let line = e.audit.recent(1)[0].to_string();
    assert!(
        line.contains("\"propose_change\"")
            && line.contains("alterTopicConfigs")
            && line.contains("\"orders\""),
        "{line}"
    );
    assert!(
        !line.contains("Raise min") && !line.contains("acks=all") && !line.contains("\"2\""),
        "{line}"
    );
    // Повтор того же предложения — created: false, как отвечает Backend.
    let v = ok(call(&c, "propose_change", proposal("orders", "again")).await);
    assert_eq!(v["created"], false);
}

#[tokio::test]
async fn read_only_connection_is_reported_to_the_llm() {
    let fake = Fake::new(SessionAccess::Approved);
    fake.read_only.store(true, Ordering::SeqCst);
    let e = env(fake).await;
    let c = client(&e.ep).await;
    let v = ok(call(&c, "propose_change", proposal("orders", "t")).await);
    assert_eq!(v["readOnly"], true);
}

#[tokio::test]
async fn proposals_are_listed_with_decisions_and_withdrawn() {
    let fake = Fake::new(SessionAccess::Approved);
    let e = env(fake.clone()).await;
    let c = client(&e.ep).await;
    let v = ok(call(
        &c,
        "list_proposals",
        json!({ "connectionId": "c1", "status": "closed" }),
    )
    .await);
    let item = &v["items"][0];
    assert_eq!(
        (
            item["id"].as_str(),
            item["status"].as_str(),
            item["appliedAsProposed"].as_bool()
        ),
        (Some("p1"), Some("applied"), Some(false))
    );
    assert_eq!(item["change"]["kind"], "addPartitions");
    assert_eq!(item["change"]["newTotal"], 6);
    assert!(item.get("rejectReason").is_none());
    assert!(matches!(
        fake.proposal_ops.lock().unwrap()[0],
        ProposalOp::List {
            status: StatusFilter::Closed,
            ..
        }
    ));
    let v = ok(call(
        &c,
        "withdraw_proposal",
        json!({ "connectionId": "c1", "proposalId": "p1" }),
    )
    .await);
    assert_eq!(v, json!({ "withdrawn": true }));
    for (id, code) in [("foreign", "not_allowed"), ("closed", "invalid_argument")] {
        let msg = err(&call(
            &c,
            "withdraw_proposal",
            json!({ "connectionId": "c1", "proposalId": id }),
        )
        .await);
        assert!(msg.starts_with(code), "{msg}");
    }
    // Чужое подключение — как у всех инструментов.
    let msg = err(&call(&c, "list_proposals", json!({ "connectionId": "c2" })).await);
    assert!(msg.starts_with("not_allowed"), "{msg}");
}

#[tokio::test]
async fn long_proposal_list_is_cut_to_the_newest() {
    let fake = Fake::new(SessionAccess::Approved);
    fake.many_proposals.store(80, Ordering::SeqCst);
    let e = env(fake).await;
    let c = client(&e.ep).await;
    let v = ok(call(&c, "list_proposals", json!({ "connectionId": "c1" })).await);
    assert!(serde_json::to_vec(&v).unwrap().len() <= 100 * 1024);
    assert_eq!(v["truncated"], true);
    let hint = v["hint"].as_str().unwrap();
    assert!(hint.contains("100 KB") && hint.contains("most recently updated"));
    let ids: Vec<&str> = v["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["id"].as_str().unwrap())
        .collect();
    assert!(!ids.is_empty() && ids.len() < 80, "{}", ids.len());
    let expected: Vec<String> = (0..80)
        .rev()
        .take(ids.len())
        .map(|i| format!("p{i}"))
        .collect();
    assert_eq!(ids, expected);
}
