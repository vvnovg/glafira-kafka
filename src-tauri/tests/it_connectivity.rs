mod common;

use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::kafka::config::SASL_PASSWORD;
use kafka_explorer_lib::kafka::connectivity::{TestReport, test_connection};
use kafka_explorer_lib::secrets::{MemoryStore, SecretStore, secret_key};
use kafka_explorer_lib::storage::connections::{SaslConfig, SaslMechanism, SecurityConfig};

#[test]
fn plaintext_succeeds() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let r = test_connection(&common::plaintext_profile(&bs), &MemoryStore::default());
    assert!(r.ok, "{r:?}");
    assert_eq!(
        r.steps.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
        ["config", "tcp", "kafka"]
    );
}

fn sasl_profile(bs: &str) -> kafka_explorer_lib::storage::connections::ConnectionProfile {
    let mut p = common::plaintext_profile(bs);
    p.security = SecurityConfig::SaslPlaintext {
        sasl: SaslConfig {
            mechanism: SaslMechanism::Plain,
            username: "tester".into(),
        },
    };
    p
}

#[test]
fn sasl_plain_succeeds_with_right_password() {
    let Some(bs) = common::sasl_bootstrap() else {
        return;
    };
    let p = sasl_profile(&bs);
    let secrets = MemoryStore::default();
    secrets
        .set(&secret_key(&p.id, SASL_PASSWORD), "tester-secret")
        .unwrap();
    let r = test_connection(&p, &secrets);
    assert!(r.ok, "{r:?}");
}

#[test]
fn sasl_plain_wrong_password_is_auth_failed() {
    let Some(bs) = common::sasl_bootstrap() else {
        return;
    };
    let p = sasl_profile(&bs);
    let secrets = MemoryStore::default();
    secrets
        .set(&secret_key(&p.id, SASL_PASSWORD), "wrong")
        .unwrap();
    let r = test_connection(&p, &secrets);
    assert!(!r.ok);
    let step = r.steps.last().unwrap();
    assert_eq!(step.name, "kafka");
    assert_eq!(
        step.error.as_ref().unwrap().code,
        ErrorCode::AuthFailed,
        "{step:?}"
    );
}

/// Пулы и операции вне Test connection тоже должны показывать причину (SASL), а не «таймаут».
#[tokio::test(flavor = "multi_thread")]
async fn pooled_handles_wrong_password_is_auth_failed() {
    let Some(bs) = common::sasl_bootstrap() else {
        return;
    };
    let mut p = sasl_profile(&bs);
    p.request_timeout_ms = 3_000;
    let secrets = MemoryStore::default();
    secrets
        .set(&secret_key(&p.id, SASL_PASSWORD), "wrong")
        .unwrap();
    let h = std::sync::Arc::new(
        kafka_explorer_lib::kafka::pool::KafkaHandles::create(&p, &secrets).unwrap(),
    );
    let h1 = h.clone();
    let err = tokio::task::spawn_blocking(move || {
        kafka_explorer_lib::kafka::cluster::cluster_overview(&h1)
    })
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::AuthFailed, "{err:?}");
}

fn scram_profile(
    bs: &str,
    mechanism: SaslMechanism,
) -> kafka_explorer_lib::storage::connections::ConnectionProfile {
    let mut p = common::plaintext_profile(bs);
    p.security = SecurityConfig::SaslPlaintext {
        sasl: SaslConfig {
            mechanism,
            username: "scram-user".into(),
        },
    };
    p
}

fn scram_report(mechanism: SaslMechanism, password: &str) -> Option<TestReport> {
    let bs = common::sasl_bootstrap()?;
    let p = scram_profile(&bs, mechanism);
    let secrets = MemoryStore::default();
    secrets
        .set(&secret_key(&p.id, SASL_PASSWORD), password)
        .unwrap();
    Some(test_connection(&p, &secrets))
}

#[test]
fn scram_sha_256_succeeds_with_right_password() {
    let Some(r) = scram_report(SaslMechanism::ScramSha256, "scram-secret") else {
        return;
    };
    assert!(r.ok, "{r:?}");
}

#[test]
fn scram_sha_512_succeeds_with_right_password() {
    let Some(r) = scram_report(SaslMechanism::ScramSha512, "scram-secret") else {
        return;
    };
    assert!(r.ok, "{r:?}");
}

#[test]
fn scram_wrong_password_is_auth_failed() {
    for mechanism in [SaslMechanism::ScramSha256, SaslMechanism::ScramSha512] {
        let Some(r) = scram_report(mechanism, "wrong") else {
            return;
        };
        assert!(!r.ok);
        let step = r.steps.last().unwrap();
        assert_eq!(step.name, "kafka");
        assert_eq!(
            step.error.as_ref().unwrap().code,
            ErrorCode::AuthFailed,
            "{mechanism:?}: {step:?}"
        );
    }
}
