//! MCP-шлюз целиком против локальной Kafka (спец. G1 §10): контроллер приложения, сокет,
//! рукопожатие, MCP-клиент rmcp. Нужен KE_IT_BOOTSTRAP (см. dev/docker-compose.yml).

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use kafka_explorer_lib::mcp::{McpController, files};
use kafka_explorer_lib::secrets::MemoryStore;
use kafka_explorer_lib::state::AppState;
use kafka_explorer_lib::storage::connections::LlmAccess;
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rmcp::model::{CallToolRequestParams, CallToolResult};
use rmcp::service::RunningService;
use rmcp::{RoleClient, ServiceExt};
use serde_json::{Value, json};

/// Участник группы в своём потоке: опрашивает брокер, пока не попросят остановиться.
struct Member {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Member {
    fn join(bs: &str, group: &str, topic: &str, instance: Option<&str>) -> Self {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", bs)
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .set("session.timeout.ms", "10000");
        if let Some(i) = instance {
            cfg.set("group.instance.id", i);
        }
        let consumer: BaseConsumer = cfg.create().unwrap();
        consumer.subscribe(&[topic]).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let thread = std::thread::spawn(move || {
            while !flag.load(Ordering::Relaxed) {
                let _ = consumer.poll(Duration::from_millis(100));
            }
            consumer.unsubscribe();
        });
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Member {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

async fn gateway(
    bs: &str,
) -> (
    tempfile::TempDir,
    Arc<McpController>,
    RunningService<RoleClient, ()>,
) {
    let dir = tempfile::tempdir().unwrap();
    let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
    let mut p = common::plaintext_profile(bs);
    p.llm_access = LlmAccess::Metadata;
    st.connections.upsert(p).unwrap();
    // Клиент rmcp уже одобрен: диалог подтверждения проверяется в unit-тестах контроллера.
    st.settings.remember_client("rmcp", "test", 0).unwrap();
    let ctl = McpController::new(st, dir.path());
    ctl.set_enabled(true).await.unwrap();
    let f = files::read(dir.path()).unwrap();
    let mut s = glafira_mcp::endpoint::connect(&f.endpoint).await.unwrap();
    glafira_mcp::handshake::client(&mut s, &f.token)
        .await
        .unwrap();
    let client = ().serve(s).await.unwrap();
    (dir, ctl, client)
}

async fn call(c: &RunningService<RoleClient, ()>, tool: &str, args: Value) -> Value {
    let mut p = CallToolRequestParams::new(tool.to_string());
    if let Value::Object(m) = args {
        p = p.with_arguments(m);
    }
    let r: CallToolResult = c.call_tool(p).await.unwrap();
    assert_eq!(r.is_error, Some(false), "{tool}: {:?}", r.content);
    r.structured_content.unwrap()
}

async fn wait_for_members(c: &RunningService<RoleClient, ()>, group: &str, n: usize) -> Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let v = call(
            c,
            "describe_consumer_group",
            json!({"connectionId": "it", "groupId": group}),
        )
        .await;
        if v["state"] == "Stable" && v["members"].as_array().map_or(0, Vec::len) == n {
            return v;
        }
        assert!(
            Instant::now() < deadline,
            "группа не стабилизировалась: {v}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn gateway_reads_a_real_cluster() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-mcp", 2).await;
    let group = common::TempGroup::new(&bs, "it-mcp");
    let (_dir, ctl, c) = gateway(&bs).await;

    let v = call(&c, "list_connections", json!({})).await;
    assert_eq!(v["connections"][0]["id"], "it");
    let v = call(&c, "get_cluster_overview", json!({"connectionId": "it"})).await;
    assert!(!v["brokers"].as_array().unwrap().is_empty());
    let v = call(
        &c,
        "describe_topic",
        json!({"connectionId": "it", "topic": "__consumer_offsets"}),
    )
    .await;
    assert_eq!(v["internal"], true);
    assert!(!v["partitions"].as_array().unwrap().is_empty());
    let v = call(
        &c,
        "list_topics",
        json!({"connectionId": "it", "filter": topic.name}),
    )
    .await;
    assert_eq!(v["items"][0]["partitions"], 2);

    let static_member = Member::join(&bs, &group, &topic, Some("it-static-1"));
    let v = wait_for_members(&c, &group, 1).await;
    assert_eq!(v["members"][0]["groupInstanceId"], "it-static-1");
    assert!(v["coordinator"]["id"].is_number(), "{v}");
    assert_eq!(v["groupType"], "classic");

    // Наблюдение ловит вход второго участника.
    let observe = {
        let c = c.peer().clone();
        let g = group.id.clone();
        tokio::spawn(async move {
            let mut p = CallToolRequestParams::new("observe_consumer_group");
            p = p.with_arguments(
                json!({"connectionId": "it", "groupId": g, "durationSeconds": 12})
                    .as_object()
                    .unwrap()
                    .clone(),
            );
            c.call_tool(p).await.unwrap()
        })
    };
    tokio::time::sleep(Duration::from_secs(3)).await;
    let second = Member::join(&bs, &group, &topic, None);
    let r = observe.await.unwrap();
    assert_eq!(r.is_error, Some(false), "{:?}", r.content);
    let v = r.structured_content.unwrap();
    assert!(v["summary"]["joins"].as_u64().unwrap() >= 1, "{v}");
    assert!(
        v["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["kind"] == "memberJoined"),
        "{v}"
    );

    drop(second);
    drop(static_member);
    ctl.shutdown().await;
}
