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

fn wrong_password_profile(
    bs: &str,
) -> (
    kafka_explorer_lib::storage::connections::ConnectionProfile,
    MemoryStore,
) {
    let mut p = sasl_profile(bs);
    p.request_timeout_ms = 3_000;
    let secrets = MemoryStore::default();
    secrets
        .set(&secret_key(&p.id, SASL_PASSWORD), "wrong")
        .unwrap();
    (p, secrets)
}

/// librdkafka подавляет повтор той же ошибки брокера 30 с: повторные операции
/// на тех же handles не получают свежего колбэка, но причина не должна теряться.
#[tokio::test(flavor = "multi_thread")]
async fn pooled_handles_repeated_wrong_password_stays_auth_failed() {
    let Some(bs) = common::sasl_bootstrap() else {
        return;
    };
    let (p, secrets) = wrong_password_profile(&bs);
    let h = std::sync::Arc::new(
        kafka_explorer_lib::kafka::pool::KafkaHandles::create(&p, &secrets).unwrap(),
    );
    let h1 = h.clone();
    // Несколько подряд: первые вызовы могут «съесть» ещё не доставленные события из очереди,
    // дальше свежих колбэков нет (подавление) — причина всё равно должна сохраняться.
    let codes = tokio::task::spawn_blocking(move || {
        (0..4)
            .map(|_| {
                kafka_explorer_lib::kafka::cluster::cluster_overview(&h1)
                    .unwrap_err()
                    .code
            })
            .collect::<Vec<_>>()
    })
    .await
    .unwrap();
    assert_eq!(codes, vec![ErrorCode::AuthFailed; 4]);
}

#[tokio::test(flavor = "multi_thread")]
async fn pooled_describe_configs_wrong_password_is_auth_failed() {
    let Some(bs) = common::sasl_bootstrap() else {
        return;
    };
    let (p, secrets) = wrong_password_profile(&bs);
    let h = std::sync::Arc::new(
        kafka_explorer_lib::kafka::pool::KafkaHandles::create(&p, &secrets).unwrap(),
    );
    let mut codes = Vec::new();
    for _ in 0..3 {
        let err = kafka_explorer_lib::kafka::configs::describe_configs(
            &h,
            rdkafka::admin::ResourceSpecifier::Broker(1),
        )
        .await
        .unwrap_err();
        codes.push(err.code);
    }
    assert_eq!(codes, vec![ErrorCode::AuthFailed; 3]);
}
/// Через AppState: ошибка аутентификации вытесняет handles из пула, следующий вызов
/// получает свежий клиент и снова AUTH_FAILED.
#[test]
fn app_state_evicts_handles_on_auth_failure() {
    let Some(bs) = common::sasl_bootstrap() else {
        return;
    };
    let (p, secrets) = wrong_password_profile(&bs);
    let dir = tempfile::tempdir().unwrap();
    let state =
        kafka_explorer_lib::state::AppState::new(dir.path(), std::sync::Arc::new(secrets)).unwrap();
    state.connections.upsert(p.clone()).unwrap();
    let first_handles = state.handles(&p.id).unwrap();
    let first = state
        .with_handles(&p.id, kafka_explorer_lib::kafka::cluster::cluster_overview)
        .unwrap_err();
    assert_eq!(first.code, ErrorCode::AuthFailed, "{first:?}");
    let fresh = state.handles(&p.id).unwrap();
    assert!(
        !std::sync::Arc::ptr_eq(&first_handles, &fresh),
        "handles вытеснены из пула"
    );
    let second = state
        .with_handles(&p.id, kafka_explorer_lib::kafka::cluster::cluster_overview)
        .unwrap_err();
    assert_eq!(second.code, ErrorCode::AuthFailed, "{second:?}");
}
