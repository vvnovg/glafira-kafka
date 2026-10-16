mod common;

use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::kafka::config::{SSL_KEY_PASSWORD, SSL_KEYSTORE_PASSWORD};
use kafka_explorer_lib::kafka::connectivity::test_connection;
use kafka_explorer_lib::secrets::{MemoryStore, SecretStore, secret_key};
use kafka_explorer_lib::storage::connections::{
    CertFormat, ClientCert, ConnectionProfile, SecurityConfig, TlsConfig,
};

fn profile(bs: &str, client: Option<ClientCert>) -> ConnectionProfile {
    let mut p = common::plaintext_profile(bs);
    p.security = SecurityConfig::Ssl {
        tls: TlsConfig {
            ca_location: Some(common::cert_path("ca.pem")),
            ca_format: CertFormat::Pem,
            verify_hostname: true,
            client_cert: client,
        },
    };
    p
}

fn cert(format: CertFormat, file: &str, key: Option<&str>) -> Option<ClientCert> {
    Some(ClientCert {
        format,
        certificate_path: common::cert_path(file),
        key_path: key.map(common::cert_path),
        alias: None,
    })
}

#[test]
fn connects_with_each_certificate_format() {
    let Some(bs) = common::mtls_bootstrap() else {
        return;
    };
    let secrets = MemoryStore::default();
    secrets
        .set(&secret_key("it", SSL_KEYSTORE_PASSWORD), "storepass")
        .unwrap();
    secrets
        .set(&secret_key("it", SSL_KEY_PASSWORD), "keypass")
        .unwrap();
    for client in [
        cert(CertFormat::Pem, "client.pem", Some("client.key")),
        cert(CertFormat::Pem, "client.pem", Some("client-enc.key")),
        cert(CertFormat::Pkcs12, "client.p12", None),
        cert(CertFormat::Jks, "client-keypass.jks", None),
    ] {
        let r = test_connection(&profile(&bs, client.clone()), &secrets);
        assert!(r.ok, "{client:?}: {r:?}");
    }
}

#[test]
fn broker_rejects_missing_client_certificate() {
    let Some(bs) = common::mtls_bootstrap() else {
        return;
    };
    let r = test_connection(&profile(&bs, None), &MemoryStore::default());
    assert!(!r.ok);
    let e = r.steps.last().unwrap().error.clone().unwrap();
    eprintln!("missing client cert → {e:?}");
    // TLS 1.3: отказ брокера приходит алертом после рукопожатия или разрывом соединения.
    assert!(
        matches!(e.code, ErrorCode::TlsError | ErrorCode::ConnectionFailed),
        "{e:?}"
    );
}
