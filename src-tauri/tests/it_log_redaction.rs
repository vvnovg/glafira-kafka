mod common;

use std::io::Write;
use std::sync::{Arc, Mutex};

use kafka_explorer_lib::kafka::config::{SASL_PASSWORD, SSL_KEY_PASSWORD};
use kafka_explorer_lib::kafka::connectivity::test_connection;
use kafka_explorer_lib::secrets::{MemoryStore, SecretStore, secret_key};
use kafka_explorer_lib::storage::connections::{
    CertFormat, ClientCert, SaslConfig, SaslMechanism, SecurityConfig, TlsConfig,
};

#[derive(Clone, Default)]
struct Buf(Arc<Mutex<Vec<u8>>>);

impl Write for Buf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn passwords_and_keys_never_reach_logs() {
    let buf = Buf::default();
    let sink = buf.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(move || sink.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        let secrets = MemoryStore::default();
        secrets
            .set(&secret_key("it", SASL_PASSWORD), "SASL-SECRET-123")
            .unwrap();
        secrets
            .set(&secret_key("it", SSL_KEY_PASSWORD), "keypass")
            .unwrap();
        let mut p = common::plaintext_profile("127.0.0.1:1");
        p.security = SecurityConfig::SaslSsl {
            tls: TlsConfig {
                ca_location: Some(common::cert_path("ca.pem")),
                ca_format: CertFormat::Pem,
                verify_hostname: true,
                client_cert: Some(ClientCert {
                    format: CertFormat::Pem,
                    certificate_path: common::cert_path("client.pem"),
                    key_path: Some(common::cert_path("client-enc.key")),
                    alias: None,
                }),
            },
            sasl: SaslConfig {
                mechanism: SaslMechanism::Plain,
                username: "u".into(),
            },
        };
        let _ = test_connection(&p, &secrets);
    });
    let text = String::from_utf8_lossy(&buf.0.lock().unwrap()).to_string();
    for secret in [
        "SASL-SECRET-123",
        "keypass",
        "PRIVATE KEY",
        "BEGIN CERTIFICATE",
    ] {
        assert!(!text.contains(secret), "в логе найдено «{secret}»:\n{text}");
    }
}
