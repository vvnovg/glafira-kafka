mod common;

use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::kafka::config::SASL_PASSWORD;
use kafka_explorer_lib::kafka::connectivity::test_connection;
use kafka_explorer_lib::secrets::{secret_key, MemoryStore, SecretStore};
use kafka_explorer_lib::storage::connections::{SaslConfig, SaslMechanism, SecurityConfig};

#[test]
fn plaintext_succeeds() {
    let Some(bs) = common::bootstrap() else { return };
    let r = test_connection(&common::plaintext_profile(&bs), &MemoryStore::default());
    assert!(r.ok, "{r:?}");
    assert_eq!(r.steps.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(), ["config", "tcp", "kafka"]);
}

fn sasl_profile(bs: &str) -> kafka_explorer_lib::storage::connections::ConnectionProfile {
    let mut p = common::plaintext_profile(bs);
    p.security = SecurityConfig::SaslPlaintext {
        sasl: SaslConfig { mechanism: SaslMechanism::Plain, username: "tester".into() },
    };
    p
}

#[test]
fn sasl_plain_succeeds_with_right_password() {
    let Some(bs) = common::sasl_bootstrap() else { return };
    let p = sasl_profile(&bs);
    let secrets = MemoryStore::default();
    secrets.set(&secret_key(&p.id, SASL_PASSWORD), "tester-secret").unwrap();
    let r = test_connection(&p, &secrets);
    assert!(r.ok, "{r:?}");
}

#[test]
fn sasl_plain_wrong_password_is_auth_failed() {
    let Some(bs) = common::sasl_bootstrap() else { return };
    let p = sasl_profile(&bs);
    let secrets = MemoryStore::default();
    secrets.set(&secret_key(&p.id, SASL_PASSWORD), "wrong").unwrap();
    let r = test_connection(&p, &secrets);
    assert!(!r.ok);
    let step = r.steps.last().unwrap();
    assert_eq!(step.name, "kafka");
    assert_eq!(step.error.as_ref().unwrap().code, ErrorCode::AuthFailed, "{step:?}");
}
