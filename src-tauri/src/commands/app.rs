use serde::Serialize;
use tauri::{AppHandle, Manager, State};
use tauri_plugin_updater::UpdaterExt;
use time::format_description::well_known::Rfc3339;

use super::blocking;
use crate::diagnostics::{AppInfo, write_archive};
use crate::error::{AppError, AppResult, ErrorCode};
use crate::policy::PolicyStatus;
use crate::state::AppState;

fn dir(app: &AppHandle, which: &str) -> String {
    let p = match which {
        "data" => app.path().app_data_dir(),
        _ => app.path().app_log_dir(),
    };
    p.map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "—".into())
}

pub fn app_info(app: &AppHandle) -> AppInfo {
    AppInfo {
        name: crate::product::PRODUCT_NAME.into(),
        version: env!("CARGO_PKG_VERSION").into(),
        commit: env!("GIT_COMMIT").into(),
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        data_dir: dir(app, "data"),
        log_dir: dir(app, "log"),
    }
}

#[tauri::command]
pub fn get_app_info(app: AppHandle) -> AppResult<AppInfo> {
    Ok(app_info(&app))
}

#[tauri::command]
pub async fn export_diagnostics(
    app: AppHandle,
    state: State<'_, AppState>,
    path: String,
) -> AppResult<()> {
    if path.trim().is_empty() {
        return Err(AppError::invalid("Не выбран файл для архива"));
    }
    let st = state.inner().clone();
    let info = app_info(&app);
    let log_dir = app
        .path()
        .app_log_dir()
        .map_err(|e| AppError::internal(e.to_string()))?;
    blocking(move || {
        write_archive(
            std::path::Path::new(&path),
            &info,
            &st.connections.list(),
            &st.policy.status,
            &st.license.status(),
            &crate::logging::log_files(&log_dir),
        )
    })
    .await
}

/// Заглушка открытого ключа updater в `tauri.conf.json`, пока владелец не создал ключ.
/// Плагин проверяет ключ только при установке, поэтому сборку с заглушкой отсекаем сами:
/// никаких запросов к серверу обновлений и понятная ошибка вместо ошибки подписи.
const PUBKEY_PLACEHOLDER: &str = "REPLACE_WITH_UPDATER_PUBKEY";

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum UpdateCheck {
    Disabled {
        reason: String,
    },
    UpToDate,
    Available {
        version: String,
        notes: Option<String>,
        date: Option<String>,
    },
}

#[derive(Debug)]
enum UpdatePlan {
    Disabled(String),
    /// None — адрес из tauri.conf.json, Some — зеркало из политики.
    Check(Option<String>),
}

fn update_plan(policy: &PolicyStatus) -> UpdatePlan {
    if !policy.updates_enabled {
        return UpdatePlan::Disabled("Обновления управляются вашей организацией".into());
    }
    UpdatePlan::Check(policy.update_url.clone())
}

fn pubkey_configured(pubkey: Option<&str>) -> bool {
    pubkey.is_some_and(|k| !k.trim().is_empty() && k.trim() != PUBKEY_PLACEHOLDER)
}

fn updater(app: &AppHandle, url: Option<String>) -> AppResult<tauri_plugin_updater::Updater> {
    let pubkey = app
        .config()
        .plugins
        .0
        .get("updater")
        .and_then(|c| c.get("pubkey"))
        .and_then(|k| k.as_str());
    if !pubkey_configured(pubkey) {
        return Err(AppError::invalid("Обновления не настроены в этой сборке"));
    }
    // На Windows `download_and_install` запускает установщик и вызывает `process::exit(0)`,
    // минуя `RunEvent::Exit`: без хука consumer не выйдут из группы, а пакеты оборвутся.
    // Хук заменяет стандартный хук плагина, поэтому `cleanup_before_exit` вызываем сами.
    // На macOS/Linux хук не используется: там после установки `app.restart()` проходит Exit.
    let handle = app.clone();
    let mut b = app.updater_builder().on_before_exit(move || {
        crate::shutdown_sessions(&handle);
        handle.cleanup_before_exit();
    });
    if let Some(u) = url {
        let parsed = u
            .parse()
            .map_err(|_| AppError::invalid(format!("Неверный адрес обновлений в политике: {u}")))?;
        b = b.endpoints(vec![parsed]).map_err(|e| {
            AppError::invalid(format!("Неверный адрес обновлений в политике: {u}"))
                .with_details(e.to_string())
        })?;
    }
    b.build()
        .map_err(|e| AppError::internal("Обновления недоступны").with_details(e.to_string()))
}

fn net_err(e: tauri_plugin_updater::Error) -> AppError {
    AppError::new(
        ErrorCode::ConnectionFailed,
        "Не удалось проверить обновления",
    )
    .with_details(e.to_string())
}

#[tauri::command]
pub async fn check_update(app: AppHandle, state: State<'_, AppState>) -> AppResult<UpdateCheck> {
    // Политика проверяется до сборки updater: при запрете — ни одного запроса к серверу.
    let url = match update_plan(&state.policy.status) {
        UpdatePlan::Disabled(reason) => return Ok(UpdateCheck::Disabled { reason }),
        UpdatePlan::Check(url) => url,
    };
    match updater(&app, url)?.check().await.map_err(net_err)? {
        None => Ok(UpdateCheck::UpToDate),
        Some(u) => Ok(UpdateCheck::Available {
            version: u.version.clone(),
            notes: u.body.clone(),
            date: u.date.and_then(|d| d.format(&Rfc3339).ok()),
        }),
    }
}

/// Без `force` при активных сессиях установка требует подтверждения.
fn confirm_sessions(active: usize, force: bool) -> AppResult<()> {
    if active > 0 && !force {
        return Err(AppError::new(
            ErrorCode::ConfirmationRequired,
            format!(
                "Идёт сессий: {active} (чтение, consumer или пакетная отправка). Обновление их остановит."
            ),
        ));
    }
    Ok(())
}

#[tauri::command]
pub async fn install_update(
    app: AppHandle,
    state: State<'_, AppState>,
    force: bool,
) -> AppResult<()> {
    let url = match update_plan(&state.policy.status) {
        UpdatePlan::Disabled(reason) => return Err(AppError::invalid(reason)),
        UpdatePlan::Check(url) => url,
    };
    confirm_sessions(state.sessions.len(), force)?;
    let Some(update) = updater(&app, url)?.check().await.map_err(net_err)? else {
        return Err(AppError::invalid("Обновлений нет"));
    };
    // Пока шла проверка по сети, пользователь мог запустить сессию: спрашиваем снова.
    confirm_sessions(state.sessions.len(), force)?;
    tracing::info!(version = %update.version, "установка обновления");
    update
        .download_and_install(|_, _| {}, || {})
        .await
        .map_err(|e| {
            AppError::internal("Не удалось установить обновление").with_details(e.to_string())
        })?;
    // RunEvent::Exit корректно завершает сессии перед перезапуском.
    app.restart();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_disables_check_without_network() {
        let st = PolicyStatus {
            managed: true,
            updates_enabled: false,
            ..PolicyStatus::default()
        };
        match update_plan(&st) {
            UpdatePlan::Disabled(reason) => assert!(reason.contains("организац")),
            p => panic!("ожидалось Disabled, получено {p:?}"),
        }
    }

    #[test]
    fn policy_url_overrides_endpoint() {
        let st = PolicyStatus {
            managed: true,
            update_url: Some("https://mirror.corp/latest.json".into()),
            ..PolicyStatus::default()
        };
        match update_plan(&st) {
            UpdatePlan::Check(Some(url)) => assert_eq!(url, "https://mirror.corp/latest.json"),
            p => panic!("ожидалось Check(Some), получено {p:?}"),
        }
        assert!(matches!(
            update_plan(&PolicyStatus::default()),
            UpdatePlan::Check(None)
        ));
    }

    #[test]
    fn update_check_json_shape() {
        let v = serde_json::to_value(UpdateCheck::Available {
            version: "0.3.0".into(),
            notes: None,
            date: None,
        })
        .unwrap();
        assert_eq!(v["status"], "available");
        assert_eq!(v["version"], "0.3.0");
        assert!(v["notes"].is_null());
        assert_eq!(
            serde_json::to_value(UpdateCheck::UpToDate).unwrap()["status"],
            "upToDate"
        );
        let d = serde_json::to_value(UpdateCheck::Disabled { reason: "r".into() }).unwrap();
        assert_eq!(d["status"], "disabled");
        assert_eq!(d["reason"], "r");
    }

    #[test]
    fn active_sessions_require_confirmation_unless_forced() {
        assert!(confirm_sessions(0, false).is_ok());
        assert!(confirm_sessions(2, true).is_ok());
        let e = confirm_sessions(1, false).unwrap_err();
        assert_eq!(e.code, ErrorCode::ConfirmationRequired);
        assert!(e.message.contains("Идёт сессий: 1"));
    }

    #[test]
    fn placeholder_pubkey_is_not_configured() {
        assert!(!pubkey_configured(None));
        assert!(!pubkey_configured(Some("")));
        assert!(!pubkey_configured(Some(PUBKEY_PLACEHOLDER)));
        assert!(pubkey_configured(Some(
            "dW50cnVzdGVkIGNvbW1lbnQ6IG1pbmlzaWdu"
        )));
    }

    #[test]
    fn shipped_config_uses_known_pubkey_and_endpoint() {
        let conf: serde_json::Value =
            serde_json::from_str(include_str!("../../tauri.conf.json")).unwrap();
        let up = &conf["plugins"]["updater"];
        assert!(up["pubkey"].is_string());
        assert_eq!(
            up["endpoints"][0],
            "https://github.com/vvnovg/glafira-kafka-releases/releases/latest/download/latest.json"
        );
        assert_eq!(conf["bundle"]["createUpdaterArtifacts"], true);
    }
}
