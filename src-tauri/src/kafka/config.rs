use rdkafka::ClientConfig;

use crate::error::{AppError, AppResult};
use crate::secrets::{SecretStore, secret_key};
use crate::storage::connections::{
    CertFormat, ConnectionProfile, SaslConfig, SecurityConfig, TlsConfig,
};

pub const SASL_PASSWORD: &str = "sasl.password";
pub const SSL_KEY_PASSWORD: &str = "ssl.key.password";
pub const SSL_KEYSTORE_PASSWORD: &str = "ssl.keystore.password";
pub const SSL_TRUSTSTORE_PASSWORD: &str = "ssl.truststore.password";

pub fn build_client_config(
    p: &ConnectionProfile,
    secrets: &dyn SecretStore,
) -> AppResult<ClientConfig> {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", p.bootstrap_servers.trim())
        .set("client.id", "kafka-explorer")
        .set("socket.connection.setup.timeout.ms", "10000")
        .set("security.protocol", protocol_name(&p.security));
    match &p.security {
        SecurityConfig::Plaintext => {}
        SecurityConfig::Ssl { tls } => apply_tls(&mut c, &p.id, tls, secrets)?,
        SecurityConfig::SaslPlaintext { sasl } => apply_sasl(&mut c, &p.id, sasl, secrets)?,
        SecurityConfig::SaslSsl { tls, sasl } => {
            apply_tls(&mut c, &p.id, tls, secrets)?;
            apply_sasl(&mut c, &p.id, sasl, secrets)?;
        }
    }
    // Дополнительные свойства применяются последними и могут переопределить всё выше.
    for (k, v) in &p.extra_properties {
        c.set(k, v);
    }
    Ok(c)
}

fn protocol_name(s: &SecurityConfig) -> &'static str {
    match s {
        SecurityConfig::Plaintext => "plaintext",
        SecurityConfig::Ssl { .. } => "ssl",
        SecurityConfig::SaslPlaintext { .. } => "sasl_plaintext",
        SecurityConfig::SaslSsl { .. } => "sasl_ssl",
    }
}

/// Сертификаты читаются при каждом создании клиента (ротация) и передаются как PEM в памяти.
/// Вызывается только из blocking-контекста: читает файлы и keychain.
fn apply_tls(
    c: &mut ClientConfig,
    id: &str,
    tls: &TlsConfig,
    secrets: &dyn SecretStore,
) -> AppResult<()> {
    // Читаем только секреты, нужные форматам (та же матрица, что в secret_ops при сохранении):
    // лишние обращения к keychain медленны, а его недоступность не должна ломать профили без паролей.
    let secret = |used: bool, field: &str| -> AppResult<Option<String>> {
        if used {
            secrets.get(&secret_key(id, field))
        } else {
            Ok(None)
        }
    };
    match tls
        .ca_location
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        // "probe" — librdkafka сам ищет системные CA (vendored OpenSSL не знает путей ОС).
        None => {
            c.set("ssl.ca.location", "probe");
        }
        Some(path) => {
            let pem = crate::tls::load_ca(
                tls.ca_format,
                path,
                secret(tls.ca_format != CertFormat::Pem, SSL_TRUSTSTORE_PASSWORD)?.as_deref(),
            )?;
            c.set("ssl.ca.pem", pem);
        }
    }
    if let Some(cert) = &tls.client_cert {
        let id_pem = crate::tls::load_identity(
            cert,
            secret(
                matches!(cert.format, CertFormat::Pem | CertFormat::Jks),
                SSL_KEY_PASSWORD,
            )?
            .as_deref(),
            secret(
                matches!(cert.format, CertFormat::Pkcs12 | CertFormat::Jks),
                SSL_KEYSTORE_PASSWORD,
            )?
            .as_deref(),
        )?;
        c.set("ssl.certificate.pem", id_pem.certificate_pem);
        c.set("ssl.key.pem", id_pem.key_pem);
    }
    c.set(
        "ssl.endpoint.identification.algorithm",
        if tls.verify_hostname { "https" } else { "none" },
    );
    Ok(())
}

fn apply_sasl(
    c: &mut ClientConfig,
    id: &str,
    sasl: &SaslConfig,
    secrets: &dyn SecretStore,
) -> AppResult<()> {
    let password = secrets
        .get(&secret_key(id, SASL_PASSWORD))?
        .ok_or_else(|| AppError::invalid("Не задан пароль SASL для подключения"))?;
    c.set("sasl.mechanism", sasl.mechanism.as_str())
        .set("sasl.username", &sasl.username)
        .set("sasl.password", password);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::secrets::{MemoryStore, SecretStore, secret_key};
    use crate::storage::connections::{
        CertFormat, ClientCert, SaslConfig, SaslMechanism, SecurityConfig, TlsConfig,
        sample_profile,
    };

    #[test]
    fn plaintext_with_extra_properties_overriding() {
        let mut p = sample_profile("c1");
        p.extra_properties
            .insert("client.id".into(), "custom".into());
        let c = build_client_config(&p, &MemoryStore::default()).unwrap();
        assert_eq!(c.get("bootstrap.servers"), Some("localhost:9092"));
        assert_eq!(c.get("security.protocol"), Some("plaintext"));
        assert_eq!(c.get("client.id"), Some("custom"));
    }

    fn fixture(name: &str) -> String {
        format!("{}/../dev/certs/out/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    fn tls(ca: Option<String>, client: Option<ClientCert>) -> TlsConfig {
        TlsConfig {
            ca_location: ca,
            ca_format: CertFormat::Pem,
            verify_hostname: true,
            client_cert: client,
        }
    }

    #[test]
    fn ssl_uses_probe_when_no_ca_and_can_disable_hostname_check() {
        let mut p = sample_profile("c1");
        p.security = SecurityConfig::Ssl {
            tls: TlsConfig {
                verify_hostname: false,
                ..tls(None, None)
            },
        };
        let c = build_client_config(&p, &MemoryStore::default()).unwrap();
        assert_eq!(c.get("security.protocol"), Some("ssl"));
        assert_eq!(c.get("ssl.ca.location"), Some("probe"));
        assert_eq!(c.get("ssl.endpoint.identification.algorithm"), Some("none"));
    }

    #[test]
    fn ca_and_client_identity_are_passed_as_pem() {
        let mut p = sample_profile("c1");
        p.security = SecurityConfig::Ssl {
            tls: tls(
                Some(fixture("ca.pem")),
                Some(ClientCert {
                    format: CertFormat::Jks,
                    certificate_path: fixture("client-keypass.jks"),
                    key_path: None,
                    alias: None,
                }),
            ),
        };
        let secrets = MemoryStore::default();
        secrets
            .set(&secret_key("c1", SSL_KEYSTORE_PASSWORD), "storepass")
            .unwrap();
        secrets
            .set(&secret_key("c1", SSL_KEY_PASSWORD), "keypass")
            .unwrap();
        let c = build_client_config(&p, &secrets).unwrap();
        assert!(c.get("ssl.ca.pem").unwrap().contains("BEGIN CERTIFICATE"));
        assert_eq!(c.get("ssl.ca.location"), None);
        assert!(
            c.get("ssl.certificate.pem")
                .unwrap()
                .contains("BEGIN CERTIFICATE")
        );
        assert!(c.get("ssl.key.pem").unwrap().contains("BEGIN PRIVATE KEY"));
    }

    /// Хранилище, которое падает на любом ключе, кроме разрешённых: недоступный keychain
    /// не должен ломать профили, которым эти секреты не нужны.
    struct StrictStore {
        allowed: Vec<String>,
        inner: MemoryStore,
    }

    impl StrictStore {
        fn new(id: &str, allowed: &[&str]) -> Self {
            Self {
                allowed: allowed.iter().map(|f| secret_key(id, f)).collect(),
                inner: MemoryStore::default(),
            }
        }
    }

    impl SecretStore for StrictStore {
        fn get(&self, key: &str) -> AppResult<Option<String>> {
            if !self.allowed.iter().any(|k| k == key) {
                return Err(AppError::internal(format!(
                    "неожиданное чтение секрета {key}"
                )));
            }
            self.inner.get(key)
        }
        fn set(&self, key: &str, value: &str) -> AppResult<()> {
            self.inner.set(key, value)
        }
        fn delete(&self, key: &str) -> AppResult<()> {
            self.inner.delete(key)
        }
    }

    fn client(format: CertFormat, cert: &str, key: Option<&str>) -> Option<ClientCert> {
        Some(ClientCert {
            format,
            certificate_path: fixture(cert),
            key_path: key.map(fixture),
            alias: None,
        })
    }

    #[test]
    fn tls_reads_only_the_secrets_the_formats_need() {
        let ssl = |t: TlsConfig| {
            let mut p = sample_profile("c1");
            p.security = SecurityConfig::Ssl { tls: t };
            p
        };
        // PEM CA + PEM-клиент: только пароль ключа.
        let p = ssl(tls(
            Some(fixture("ca.pem")),
            client(CertFormat::Pem, "client.pem", Some("client.key")),
        ));
        build_client_config(&p, &StrictStore::new("c1", &[SSL_KEY_PASSWORD])).unwrap();

        // PKCS12-клиент без CA: только пароль хранилища.
        let p = ssl(tls(
            None,
            client(CertFormat::Pkcs12, "client-nopass.p12", None),
        ));
        build_client_config(&p, &StrictStore::new("c1", &[SSL_KEYSTORE_PASSWORD])).unwrap();

        // Только PEM CA: секреты не читаются вовсе.
        let p = ssl(tls(Some(fixture("ca.pem")), None));
        build_client_config(&p, &StrictStore::new("c1", &[])).unwrap();

        // CA в PKCS12: только пароль truststore.
        let p = ssl(TlsConfig {
            ca_format: CertFormat::Pkcs12,
            ..tls(Some(fixture("truststore.p12")), None)
        });
        let store = StrictStore::new("c1", &[SSL_TRUSTSTORE_PASSWORD]);
        store
            .set(&secret_key("c1", SSL_TRUSTSTORE_PASSWORD), "storepass")
            .unwrap();
        build_client_config(&p, &store).unwrap();
    }

    #[test]
    fn certificate_problems_are_invalid_input() {
        let mut p = sample_profile("c1");
        p.security = SecurityConfig::Ssl {
            tls: tls(
                None,
                Some(ClientCert {
                    format: CertFormat::Pem,
                    certificate_path: fixture("client.pem"),
                    key_path: Some(fixture("client-enc.key")),
                    alias: None,
                }),
            ),
        };
        let e = build_client_config(&p, &MemoryStore::default())
            .err()
            .unwrap();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert!(e.message.contains("зашифрован"), "{}", e.message);
    }

    #[test]
    fn sasl_ssl_reads_password_from_secrets() {
        let mut p = sample_profile("c1");
        p.security = SecurityConfig::SaslSsl {
            tls: tls(Some(fixture("ca.pem")), None),
            sasl: SaslConfig {
                mechanism: SaslMechanism::ScramSha512,
                username: "alice".into(),
            },
        };
        let secrets = MemoryStore::default();
        secrets.set(&secret_key("c1", SASL_PASSWORD), "pw").unwrap();
        let c = build_client_config(&p, &secrets).unwrap();
        assert_eq!(c.get("security.protocol"), Some("sasl_ssl"));
        assert!(c.get("ssl.ca.pem").unwrap().contains("BEGIN CERTIFICATE"));
        assert_eq!(
            c.get("ssl.endpoint.identification.algorithm"),
            Some("https")
        );
        assert_eq!(c.get("sasl.mechanism"), Some("SCRAM-SHA-512"));
        assert_eq!(c.get("sasl.username"), Some("alice"));
        assert_eq!(c.get("sasl.password"), Some("pw"));
    }

    #[test]
    fn sasl_without_password_is_invalid_input() {
        let mut p = sample_profile("c1");
        p.security = SecurityConfig::SaslPlaintext {
            sasl: SaslConfig {
                mechanism: SaslMechanism::Plain,
                username: "alice".into(),
            },
        };
        let err = build_client_config(&p, &MemoryStore::default()).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidInput);
    }
}
