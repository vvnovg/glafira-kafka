use rdkafka::ClientConfig;

use crate::error::{AppError, AppResult};
use crate::secrets::{SecretStore, secret_key};
use crate::storage::connections::{ConnectionProfile, SaslConfig, SecurityConfig, TlsConfig};

pub const SASL_PASSWORD: &str = "sasl.password";

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
        SecurityConfig::Ssl { tls } => apply_tls(&mut c, tls),
        SecurityConfig::SaslPlaintext { sasl } => apply_sasl(&mut c, &p.id, sasl, secrets)?,
        SecurityConfig::SaslSsl { tls, sasl } => {
            apply_tls(&mut c, tls);
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

fn apply_tls(c: &mut ClientConfig, tls: &TlsConfig) {
    // "probe" — librdkafka сам ищет системные CA (vendored OpenSSL не знает путей ОС).
    let ca = tls
        .ca_location
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("probe");
    c.set("ssl.ca.location", ca);
    c.set(
        "ssl.endpoint.identification.algorithm",
        if tls.verify_hostname { "https" } else { "none" },
    );
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
        SaslConfig, SaslMechanism, SecurityConfig, TlsConfig, sample_profile,
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

    #[test]
    fn ssl_uses_probe_when_no_ca_and_can_disable_hostname_check() {
        let mut p = sample_profile("c1");
        p.security = SecurityConfig::Ssl {
            tls: TlsConfig {
                ca_location: None,
                ca_format: Default::default(),
                verify_hostname: false,
                client_cert: None,
            },
        };
        let c = build_client_config(&p, &MemoryStore::default()).unwrap();
        assert_eq!(c.get("security.protocol"), Some("ssl"));
        assert_eq!(c.get("ssl.ca.location"), Some("probe"));
        assert_eq!(c.get("ssl.endpoint.identification.algorithm"), Some("none"));
    }

    #[test]
    fn sasl_ssl_reads_password_from_secrets() {
        let mut p = sample_profile("c1");
        p.security = SecurityConfig::SaslSsl {
            tls: TlsConfig {
                ca_location: Some("/ca.pem".into()),
                ca_format: Default::default(),
                verify_hostname: true,
                client_cert: None,
            },
            sasl: SaslConfig {
                mechanism: SaslMechanism::ScramSha512,
                username: "alice".into(),
            },
        };
        let secrets = MemoryStore::default();
        secrets.set(&secret_key("c1", SASL_PASSWORD), "pw").unwrap();
        let c = build_client_config(&p, &secrets).unwrap();
        assert_eq!(c.get("security.protocol"), Some("sasl_ssl"));
        assert_eq!(c.get("ssl.ca.location"), Some("/ca.pem"));
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
