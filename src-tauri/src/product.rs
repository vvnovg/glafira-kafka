//! Имя продукта и идентификаторы — в одном месте. Переименование (M3b) меняет только этот файл
//! и `tauri.conf.json`; тест ниже следит, чтобы они совпадали.

pub const PRODUCT_NAME: &str = "Kafka Explorer";
pub const IDENTIFIER: &str = "dev.kafkaexplorer.app";
pub const KEYCHAIN_SERVICE: &str = "kafka-explorer";
pub const POLICY_VENDOR: &str = "KafkaExplorer";
pub const POLICY_PRODUCT: &str = "KafkaExplorer";

#[cfg(test)]
mod tests {
    #[test]
    fn matches_tauri_conf() {
        let conf: serde_json::Value =
            serde_json::from_str(include_str!("../tauri.conf.json")).unwrap();
        assert_eq!(conf["identifier"], super::IDENTIFIER);
        assert_eq!(conf["productName"], super::PRODUCT_NAME);
    }
}
