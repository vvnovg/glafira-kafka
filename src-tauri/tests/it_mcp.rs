//! MCP-шлюз целиком против локальной Kafka (спец. G1 §10, G4 §10): контроллер приложения, сокет,
//! рукопожатие, MCP-клиент rmcp. Нужен KE_IT_BOOTSTRAP (см. dev/docker-compose.yml).

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use kafka_explorer_lib::kafka::offsets_log;
use kafka_explorer_lib::mcp::{McpController, files};
use kafka_explorer_lib::secrets::MemoryStore;
use kafka_explorer_lib::state::AppState;
use kafka_explorer_lib::storage::connections::LlmAccess;
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
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
    access: LlmAccess,
) -> (
    tempfile::TempDir,
    Arc<McpController>,
    RunningService<RoleClient, ()>,
) {
    let dir = tempfile::tempdir().unwrap();
    let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
    let mut p = common::plaintext_profile(bs);
    p.llm_access = access;
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
    let (_dir, ctl, c) = gateway(&bs, LlmAccess::Metadata).await;

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

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Сообщения с явной меткой CreateTime: (партиция, ключ, значение, метка).
async fn produce_at(bs: &str, topic: &str, msgs: &[(i32, &str, String, i64)]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bs)
        .create()
        .unwrap();
    for (partition, key, value, ts) in msgs {
        let headers = OwnedHeaders::new().insert(Header {
            key: "trace",
            value: Some("t-1"),
        });
        let record = FutureRecord::to(topic)
            .partition(*partition)
            .key(*key)
            .payload(value.as_str())
            .timestamp(*ts)
            .headers(headers);
        producer.send(record, Duration::from_secs(5)).await.unwrap();
    }
}

fn keys(v: &Value) -> Vec<String> {
    v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["key"]["text"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn gateway_reads_messages_and_group_commits() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-mcp-msgs", 3).await;
    let group = common::TempGroup::new(&bs, "it-mcp-msgs");
    // Метки на час назад и на круглой секунде: окно теста не задевает «сейчас».
    let t0 = (now_ms() - 3_600_000) / 1000 * 1000;
    let value = |id: u32| {
        format!(
            r#"{{"orderId":{id},"email":"ann{id}@example.com","note":"card 4111 1111 1111 1111"}}"#
        )
    };
    produce_at(
        &bs,
        &topic,
        &[
            (0, "k1", value(1), t0 + 1_000),
            (1, "k2", value(2), t0 + 2_000),
            (2, "k1", value(3), t0 + 3_000),
            (0, "k3", value(4), t0 + 4_000),
            (1, "k4", value(5), t0 + 60_000), // вне окна
            (0, "k5", value(6), t0 + 2_500),  // метка меньше предыдущей в партиции 0
        ],
    )
    .await;
    let (_dir, ctl, c) = gateway(&bs, LlmAccess::Messages).await;
    let v = call(&c, "list_connections", json!({})).await;
    assert_eq!(v["connections"][0]["llmAccess"], "messages");

    let window = json!({"connectionId": "it", "topic": topic.name, "from": t0, "to": t0 + 10_000});
    let v = call(&c, "read_messages", window.clone()).await;
    assert_eq!(
        (v["matched"].as_u64(), v["complete"].as_bool()),
        (Some(5), Some(true))
    );
    assert_eq!(
        keys(&v),
        ["k1", "k2", "k5", "k1", "k3"],
        "по времени во всех партициях"
    );
    assert_eq!(
        v["messages"][0]["value"]["text"],
        r#"{"orderId":1,"email":"***","note":"card [card]"}"#
    );
    assert_eq!(v["messages"][0]["timestampType"], "create");
    assert_eq!(
        v["messages"][0]["headers"][0],
        json!({"name": "trace", "value": "t-1"})
    );
    assert_eq!(v["masked"], 10);
    assert!(!v.to_string().contains("example.com"));

    // Фильтры видят немаскированные данные.
    let mut by_email = window.clone();
    by_email["value"] = json!({"pattern": "ann2@example.com"});
    assert_eq!(keys(&call(&c, "read_messages", by_email).await), ["k2"]);
    let mut by_path = window.clone();
    by_path["jsonPath"] = json!("$.orderId == 3");
    assert_eq!(keys(&call(&c, "read_messages", by_path).await), ["k1"]);

    // Страницы по курсору.
    let mut seen = Vec::new();
    let mut args = window.clone();
    args["limit"] = json!(2);
    loop {
        let v = call(&c, "read_messages", args.clone()).await;
        seen.extend(keys(&v));
        match v["nextCursor"].as_str() {
            Some(cur) => args["cursor"] = json!(cur),
            None => break,
        }
    }
    assert_eq!(seen, ["k1", "k2", "k5", "k1", "k3"]);

    // Режим «офсеты»: партиция 0 с офсета 1.
    let v = call(
        &c,
        "read_messages",
        json!({"connectionId": "it", "topic": topic.name, "partition": 0, "startOffset": 1}),
    )
    .await;
    assert_eq!(keys(&v), ["k5", "k3"]);
    assert_eq!(
        v["range"],
        json!({"partition": 0, "startOffset": 1, "endOffset": 3})
    );

    let v = call(&c, "summarize_messages", window.clone()).await;
    assert_eq!(
        (v["matched"].as_u64(), v["bucketSeconds"].as_i64()),
        (Some(5), Some(1))
    );
    assert_eq!(v["keys"]["top"][0], json!({"key": "k1", "count": 2}));
    assert_eq!(v["keys"]["distinct"], 4);
    assert_eq!(
        v["outOfOrder"],
        json!({"count": 1, "first": {"partition": 0, "offset": 2}})
    );
    assert_eq!(v["headerNames"], json!([{"name": "trace", "count": 5}]));

    // История коммитов: два одинаковых коммита сворачиваются в один run.
    let from = now_ms() - 5_000;
    common::commit_group_offsets(&bs, &group, &topic, &[(0, 1)]);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    common::commit_group_offsets(&bs, &group, &topic, &[(0, 1)]);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    common::commit_group_offsets(&bs, &group, &topic, &[(0, 3), (1, 1)]);
    let args =
        json!({"connectionId": "it", "groupId": group.id, "from": from, "to": now_ms() + 5_000});
    let v = call(&c, "get_group_commit_history", args).await;
    let offsets = call(
        &c,
        "describe_topic",
        json!({"connectionId": "it", "topic": "__consumer_offsets"}),
    )
    .await;
    let n = offsets["partitions"].as_array().unwrap().len() as i32;
    assert_eq!(
        v["offsetsTopicPartition"],
        offsets_log::partition_for(&group, n)
    );
    let runs = |p: i64| -> Vec<(i64, i64)> {
        v["partitions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|x| x["topic"] == topic.name.as_str() && x["partition"] == p)
            .map(|x| {
                x["runs"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|r| {
                        (
                            r["offset"].as_i64().unwrap(),
                            r["commits"].as_i64().unwrap(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    };
    assert_eq!(runs(0), [(1, 2), (3, 1)], "{v}");
    assert_eq!(runs(1), [(1, 1)], "{v}");
    ctl.shutdown().await;
}
