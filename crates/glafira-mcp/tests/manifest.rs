/// Архитектурная гарантия «только чтение» (спец. §2): крейт не может дотянуться до Kafka
/// и до изменяющих функций приложения, потому что не зависит от них.
#[test]
fn crate_cannot_reach_kafka_or_the_app() {
    let manifest =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml")).unwrap();
    let mut section = String::new();
    let mut deps = Vec::new();
    for line in manifest.lines().map(str::trim) {
        if line.starts_with('[') {
            section = line.to_string();
        } else if section.contains("dependencies")
            && !line.starts_with('#')
            && let Some((name, _)) = line.split_once('=')
        {
            deps.push(name.trim().to_string());
        }
    }
    assert!(
        deps.contains(&"rmcp".to_string()),
        "разбор манифеста: {deps:?}"
    );
    for d in &deps {
        for forbidden in ["rdkafka", "kafka-explorer", "kafka_explorer", "tauri"] {
            assert!(!d.contains(forbidden), "запрещённая зависимость {d}");
        }
    }
}
