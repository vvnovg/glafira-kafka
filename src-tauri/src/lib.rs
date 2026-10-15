pub mod browse;
pub mod commands;
pub mod error;
pub mod guard;
pub mod kafka;
pub mod produce;
pub mod secrets;
pub mod state;
pub mod storage;
pub mod sync;

use std::sync::Arc;

use tauri::Manager;
use tracing_subscriber::EnvFilter;

use crate::secrets::KeyringStore;
use crate::state::AppState;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            let dir = app.path().app_data_dir()?;
            app.manage(AppState::new(&dir, Arc::new(KeyringStore))?);
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
            commands::browse::start_browse,
            commands::browse::stop_session,
        ])
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|app, event| {
            if let tauri::RunEvent::Exit = event {
                app.state::<AppState>().sessions.cancel_all();
            }
        });
}
