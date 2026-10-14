pub mod browse;
pub mod commands;
pub mod error;
pub mod kafka;
pub mod secrets;
pub mod state;
pub mod storage;

use std::sync::Arc;

use tauri::Manager;
use tracing_subscriber::EnvFilter;

use crate::secrets::KeyringStore;
use crate::state::AppState;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
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
