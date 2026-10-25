//! Сервер-каталог для реестров MCP: протокол MCP на stdin/stdout, список инструментов Glafira,
//! каждый вызов — ошибка «нужна установленная Glafira» (см. `glafira_mcp::catalog`).
//! Пользователям нужен не он, а `glafira-kafka mcp` из установленного приложения.

use std::sync::Arc;

use glafira_mcp::catalog::CatalogBackend;
use glafira_mcp::{AuditLog, GatewayOptions, serve_stream};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Журнал каталога никому не нужен: временный каталог процесса.
    let dir = std::env::temp_dir().join(format!("glafira-mcp-catalog-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let audit = Arc::new(AuditLog::new(&dir));
    let io = tokio::io::join(tokio::io::stdin(), tokio::io::stdout());
    serve_stream(
        Box::new(io),
        Arc::new(CatalogBackend),
        audit,
        GatewayOptions::new("glafira-kafka", env!("CARGO_PKG_VERSION")),
    )
    .await;
    let _ = std::fs::remove_dir_all(&dir);
}
