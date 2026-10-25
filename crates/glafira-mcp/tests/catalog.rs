//! Сервер-каталог для реестров MCP (Glama и др.): тот же список инструментов, что у Glafira,
//! но без приложения и без Kafka — каждый вызов объясняет, что нужна установленная Glafira.

use std::sync::Arc;

use glafira_mcp::catalog::{APP_REQUIRED_CODE, CatalogBackend};
use glafira_mcp::{AuditLog, GatewayOptions, serve_stream};
use rmcp::ServiceExt;
use rmcp::model::{CallToolRequestParams, CallToolResult};
use serde_json::json;

fn text(r: &CallToolResult) -> String {
    r.content
        .iter()
        .filter_map(|c| c.as_text().map(|t| t.text.clone()))
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn catalog_lists_the_tools_and_every_call_points_to_the_desktop_app() {
    let dir = tempfile::tempdir().unwrap();
    let (server_io, client_io) = tokio::io::duplex(64 * 1024);
    let server = tokio::spawn(serve_stream(
        Box::new(server_io),
        Arc::new(CatalogBackend),
        Arc::new(AuditLog::new(dir.path())),
        GatewayOptions::new("glafira-kafka", "catalog"),
    ));

    let c = ().serve(client_io).await.unwrap();
    let info = c.peer_info().expect("initialize answered");
    let server_name = info.server_info.as_ref().map(|i| i.name.as_str());
    assert_eq!(server_name, Some("glafira-kafka"));

    let tools = c.list_all_tools().await.unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
    for expected in [
        "list_connections",
        "describe_consumer_group",
        "health_check",
        "propose_change",
    ] {
        assert!(names.contains(&expected), "{expected} missing in {names:?}");
    }
    for t in &tools {
        let a = t.annotations.as_ref().expect("annotations");
        assert_eq!(a.destructive_hint, Some(false), "{}", t.name);
        assert_eq!(a.open_world_hint, Some(false), "{}", t.name);
    }

    for (tool, args) in [
        ("list_connections", json!({})),
        (
            "describe_topic",
            json!({"connectionId": "c1", "topic": "orders"}),
        ),
        ("health_check", json!({"connectionId": "c1"})),
    ] {
        let mut p = CallToolRequestParams::new(tool.to_string());
        if let serde_json::Value::Object(m) = args {
            p = p.with_arguments(m);
        }
        let r = c.call_tool(p).await.unwrap();
        assert_eq!(r.is_error, Some(true), "{tool}: {}", text(&r));
        let t = text(&r);
        assert!(t.contains(APP_REQUIRED_CODE), "{tool}: {t}");
        assert!(t.contains("glafira-kafka-releases"), "{tool}: {t}");
    }

    c.cancel().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .expect("session ends when the client goes away")
        .unwrap();
}
