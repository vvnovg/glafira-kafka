// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    // Режим шима MCP (спец. G1 §3): без окна и Tauri, stdio ↔ сокет запущенной Glafira.
    if std::env::args_os().nth(1).is_some_and(|a| a == "mcp") {
        std::process::exit(kafka_explorer_lib::mcp::shim::run());
    }
    kafka_explorer_lib::run()
}
