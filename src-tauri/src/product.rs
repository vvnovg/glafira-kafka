//! Имя продукта и идентификаторы — в одном месте. Переименование меняет только этот файл
//! и `tauri.conf.json`; тест ниже следит, чтобы они совпадали.

pub const PRODUCT_NAME: &str = "Glafira for Apache Kafka";
pub const IDENTIFIER: &str = "io.github.vvnovg.glafira.kafka";
pub const KEYCHAIN_SERVICE: &str = "glafira-kafka";
pub const POLICY_VENDOR: &str = "Glafira";
pub const POLICY_PRODUCT: &str = "Kafka";
/// Префикс имён, видимых в Kafka: client.id и consumer group (ACL, мониторинг).
pub const KAFKA_CLIENT_PREFIX: &str = "glafira-kafka";
/// Имя сервера в MCP (`serverInfo.name`) и в конфигах клиентов.
pub const MCP_SERVER_NAME: &str = "glafira-kafka";
/// Ключ сервера в `mcpServers` клиентов (Claude Desktop, Claude Code).
pub const MCP_CLIENT_KEY: &str = "glafira";
/// Прежние (идентификатор, сервис keychain), от новых к старым. Данные и секреты отсюда
/// переносятся при первом запуске (см. `migrate`).
pub const LEGACY: &[(&str, &str)] = &[("dev.kafkaexplorer.app", "kafka-explorer")];

#[cfg(test)]
mod tests {
    #[test]
    fn matches_tauri_conf() {
        let conf: serde_json::Value =
            serde_json::from_str(include_str!("../tauri.conf.json")).unwrap();
        assert_eq!(conf["identifier"], super::IDENTIFIER);
        assert_eq!(conf["productName"], super::PRODUCT_NAME);
        assert_eq!(conf["app"]["windows"][0]["title"], super::PRODUCT_NAME);
    }

    #[test]
    fn legacy_differs_from_current() {
        for (id, service) in super::LEGACY {
            assert_ne!(*id, super::IDENTIFIER);
            assert_ne!(*service, super::KEYCHAIN_SERVICE);
        }
    }
}
