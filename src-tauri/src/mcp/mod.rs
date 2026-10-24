//! MCP-шлюз для LLM, этап G1 «только чтение» (спец.
//! `docs/superpowers/specs/2026-10-18-glafira-mcp-gateway-g1-design.md`). Протокол, сессии,
//! инструменты и лимиты — в крейте `glafira-mcp`; здесь — связь с приложением.

pub mod approvals;
pub mod backend;
pub mod controller;
pub mod files;
pub mod messages;
pub mod shim;

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use serde::Serialize;

pub use controller::{McpController, McpStatus, McpUi};

/// Событие статуса для интерфейса (`listen` во фронтенде).
pub const STATUS_EVENT: &str = "mcp://state";

/// Окно Tauri как получатель статуса.
pub struct TauriUi<R: tauri::Runtime>(pub tauri::AppHandle<R>);

impl<R: tauri::Runtime> McpUi for TauriUi<R> {
    fn status_changed(&self, status: &McpStatus) {
        use tauri::Emitter;
        if let Err(e) = self.0.emit(STATUS_EVENT, status) {
            tracing::warn!("mcp status not sent: {e}");
        }
    }

    fn request_attention(&self) {
        use tauri::Manager;
        if let Some(w) = self.0.get_webview_window("main") {
            let _ = w.request_user_attention(Some(tauri::UserAttentionType::Informational));
        }
    }
}

/// Готовые фрагменты конфига клиентов для «Настроек» (спец. G1 §8).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientConfigs {
    pub command: String,
    pub args: Vec<String>,
    pub claude_desktop: String,
    pub claude_code: String,
    /// Команда для любого stdio-клиента: путь в кавычках, если в нём пробелы.
    pub other_client: String,
}

/// Путь для командной строки: в одинарных кавычках на macOS/Linux, в двойных на Windows.
fn shell_quote(s: &str) -> String {
    if cfg!(windows) {
        return format!("\"{s}\"");
    }
    let plain = s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "/._-".contains(c));
    if plain {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', r"'\''"))
    }
}

/// Путь, который запускают клиенты MCP и шим: в AppImage `current_exe()` — временная точка
/// монтирования, исчезающая после выхода, поэтому берётся `$APPIMAGE` (спец. §3, §8).
pub fn launch_path(exe: &Path, appimage: Option<OsString>) -> PathBuf {
    match appimage {
        Some(a) if !a.is_empty() => PathBuf::from(a),
        _ => exe.to_path_buf(),
    }
}

pub fn client_configs(exe: &Path) -> ClientConfigs {
    let command = exe.to_string_lossy().into_owned();
    let key = crate::product::MCP_CLIENT_KEY;
    let desktop = serde_json::json!({
        "mcpServers": { key: { "command": command, "args": ["mcp"] } }
    });
    ClientConfigs {
        claude_desktop: serde_json::to_string_pretty(&desktop).unwrap_or_default(),
        claude_code: format!("claude mcp add {key} -- {} mcp", shell_quote(&command)),
        other_client: format!("{} mcp", shell_quote(&command)),
        args: vec!["mcp".into()],
        command,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn configs_quote_paths_with_spaces() {
        let exe =
            Path::new("/Applications/Glafira for Apache Kafka.app/Contents/MacOS/glafira-kafka");
        let c = client_configs(exe);
        assert_eq!(
            c.claude_code,
            "claude mcp add glafira-kafka -- '/Applications/Glafira for Apache Kafka.app/Contents/MacOS/glafira-kafka' mcp"
        );
        let v: serde_json::Value = serde_json::from_str(&c.claude_desktop).unwrap();
        assert_eq!(
            v["mcpServers"]["glafira-kafka"]["command"],
            exe.to_str().unwrap()
        );
        assert_eq!(v["mcpServers"]["glafira-kafka"]["args"][0], "mcp");
        assert_eq!(
            c.other_client,
            "'/Applications/Glafira for Apache Kafka.app/Contents/MacOS/glafira-kafka' mcp"
        );
        let plain = client_configs(Path::new("/usr/bin/glafira-kafka"));
        assert_eq!(
            plain.claude_code,
            "claude mcp add glafira-kafka -- /usr/bin/glafira-kafka mcp"
        );
        assert_eq!(plain.other_client, "/usr/bin/glafira-kafka mcp");
        assert_eq!(shell_quote("/a/it's"), r"'/a/it'\''s'");
    }

    #[test]
    fn appimage_path_wins_over_the_mount_point() {
        let exe = Path::new("/tmp/.mount_abc/usr/bin/glafira-kafka");
        assert_eq!(
            launch_path(exe, Some("/home/u/Glafira.AppImage".into())),
            PathBuf::from("/home/u/Glafira.AppImage")
        );
        assert_eq!(launch_path(exe, None), exe.to_path_buf());
        assert_eq!(launch_path(exe, Some("".into())), exe.to_path_buf());
    }
}
