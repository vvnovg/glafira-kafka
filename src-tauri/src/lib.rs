pub mod browse;
pub mod commands;
pub mod consume;
pub mod error;
pub mod guard;
pub mod kafka;
pub mod license;
pub mod migrate;
pub mod policy;
pub mod produce;
pub mod product;
pub mod secrets;
pub mod state;
pub mod storage;
pub mod sync;
pub mod tls;

use std::sync::Arc;

use tauri::Manager;
use tracing_subscriber::EnvFilter;

use crate::secrets::KeyringStore;
use crate::state::AppState;

/// Сколько ждать завершения сессий при выходе из приложения (в сумме, не на сессию).
/// Пакет при отмене ждёт подтверждений до 2 с, выход consumer из группы — доли секунды.
const EXIT_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            let dir = app.path().app_data_dir()?;
            // Каталог данных Tauri — `<базовый каталог>/<identifier>`: прежние лежат рядом.
            if let Some(parent) = dir.parent() {
                let report = crate::migrate::migrate(
                    &dir,
                    &crate::migrate::legacy_sources(parent),
                    &KeyringStore::default(),
                );
                if let Some(from) = &report.from {
                    tracing::info!(
                        from,
                        files = report.files.len(),
                        secrets = report.secrets,
                        "миграция данных"
                    );
                }
                for e in &report.errors {
                    tracing::warn!("миграция: {e}");
                }
            }
            let policy = crate::policy::sources::load();
            tracing::info!(
                managed = policy.status.managed,
                updates = policy.status.updates_enabled,
                "политика"
            );
            let st = AppState::new(&dir, Arc::new(KeyringStore::default()))?.with_policy(policy);
            tracing::info!(tier = ?st.license.status().tier, "лицензия");
            app.manage(st);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::connections::list_connections,
            commands::connections::save_connection,
            commands::connections::delete_connection,
            commands::connections::disconnect,
            commands::connections::test_connection,
            commands::cluster::get_cluster_overview,
            commands::cluster::get_broker_config,
            commands::topics::list_topics,
            commands::topics::get_topic_stats,
            commands::topics::get_topic,
            commands::topics::get_topic_configs,
            commands::topic_ops::create_topic,
            commands::topic_ops::alter_topic_configs,
            commands::topic_ops::add_partitions,
            commands::topic_ops::purge_topic,
            commands::topic_ops::delete_topics,
            commands::topic_ops::get_topic_policies,
            commands::groups::list_groups,
            commands::groups::describe_groups,
            commands::groups::get_group_offsets,
            commands::groups::get_groups_lag,
            commands::groups::get_topic_groups,
            commands::groups::preview_offset_reset,
            commands::groups::reset_offsets,
            commands::groups::delete_group,
            commands::produce::produce,
            commands::produce::produce_batch,
            commands::produce::list_history,
            commands::produce::list_templates,
            commands::produce::save_template,
            commands::produce::delete_template,
            commands::browse::start_browse,
            commands::browse::stop_session,
            commands::consume::start_consumer,
            commands::consume::commit_offsets,
            commands::consume::pause_partitions,
            commands::consume::resume_partitions,
            commands::consume::suggest_group_id,
            commands::license::get_policy_status,
            commands::license::get_license_status,
            commands::license::set_license_key,
            commands::license::remove_license_key,
        ])
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|app, event| {
            if let tauri::RunEvent::Exit = event {
                // Процесс завершится сразу после колбэка: даём сессиям выйти из группы
                // (иначе участник висит до session.timeout.ms) и завершить пакеты.
                if !app.state::<AppState>().sessions.shutdown(EXIT_GRACE) {
                    tracing::warn!("сессии не завершились за {EXIT_GRACE:?}: выходим без ожидания");
                }
            }
        });
}
