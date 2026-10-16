use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::storage::fs::{read_json, write_json};
use crate::sync::RwLockExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Environment {
    Local,
    Dev,
    Test,
    Stage,
    Prod,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SaslMechanism {
    #[serde(rename = "PLAIN")]
    Plain,
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256,
    #[serde(rename = "SCRAM-SHA-512")]
    ScramSha512,
}

impl SaslMechanism {
    pub fn as_str(self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_request_timeout() -> u64 {
    30_000
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CertFormat {
    #[default]
    Pem,
    Pkcs12,
    Jks,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientCert {
    pub format: CertFormat,
    pub certificate_path: String,
    #[serde(default)]
    pub key_path: Option<String>,
    #[serde(default)]
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    #[serde(default)]
    pub ca_location: Option<String>,
    #[serde(default)]
    pub ca_format: CertFormat,
    #[serde(default = "default_true")]
    pub verify_hostname: bool,
    #[serde(default)]
    pub client_cert: Option<ClientCert>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaslConfig {
    pub mechanism: SaslMechanism,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "protocol", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SecurityConfig {
    Plaintext,
    Ssl { tls: TlsConfig },
    SaslPlaintext { sasl: SaslConfig },
    SaslSsl { tls: TlsConfig, sasl: SaslConfig },
}

impl SecurityConfig {
    pub fn uses_sasl(&self) -> bool {
        matches!(
            self,
            SecurityConfig::SaslPlaintext { .. } | SecurityConfig::SaslSsl { .. }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionProfile {
    pub id: String,
    pub name: String,
    pub color: String,
    pub environment: Environment,
    pub read_only: bool,
    pub bootstrap_servers: String,
    pub security: SecurityConfig,
    #[serde(default)]
    pub extra_properties: BTreeMap<String, String>,
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,
}

pub fn parse_bootstrap(s: &str) -> AppResult<Vec<(String, u16)>> {
    let mut out = Vec::new();
    for raw in s.split(',').map(str::trim).filter(|x| !x.is_empty()) {
        let (host, port) = raw.rsplit_once(':').ok_or_else(|| {
            AppError::invalid(format!("Адрес «{raw}» должен быть в формате host:port"))
        })?;
        let port: u16 = port
            .parse()
            .map_err(|_| AppError::invalid(format!("Некорректный порт в адресе «{raw}»")))?;
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if host.is_empty() {
            return Err(AppError::invalid(format!("Пустой хост в адресе «{raw}»")));
        }
        out.push((host.to_string(), port));
    }
    if out.is_empty() {
        return Err(AppError::invalid("Укажите хотя бы один bootstrap-сервер"));
    }
    Ok(out)
}

fn is_secret_property(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    k.contains("password")
        || k.contains("secret")
        || k.contains("sasl.jaas.config")
        || k == "ssl.key.pem"
        || k == "ssl.keystore.key"
}

fn validate_path(path: &str, what: &str) -> AppResult<()> {
    if path.trim().is_empty() {
        return Err(AppError::invalid(format!("Укажите файл: {what}")));
    }
    if !std::path::Path::new(path.trim()).is_absolute() {
        return Err(AppError::invalid(format!(
            "{what}: укажите абсолютный путь (сейчас «{path}»)"
        )));
    }
    Ok(())
}

fn validate_tls(tls: &TlsConfig) -> AppResult<()> {
    if let Some(ca) = tls.ca_location.as_deref().filter(|s| !s.trim().is_empty()) {
        validate_path(ca, "доверенный CA")?;
    }
    if let Some(cc) = &tls.client_cert {
        validate_path(&cc.certificate_path, "клиентский сертификат")?;
        if cc.format == CertFormat::Pem {
            match cc.key_path.as_deref() {
                Some(k) if !k.trim().is_empty() => validate_path(k, "ключ клиента")?,
                _ => return Err(AppError::invalid("Укажите файл: ключ клиента (PEM)")),
            }
        }
    }
    Ok(())
}

pub fn validate_profile(p: &ConnectionProfile) -> AppResult<()> {
    if p.id.trim().is_empty() {
        return Err(AppError::invalid("У подключения нет идентификатора"));
    }
    if p.name.trim().is_empty() {
        return Err(AppError::invalid("Имя подключения не может быть пустым"));
    }
    parse_bootstrap(&p.bootstrap_servers)?;
    if let SecurityConfig::SaslPlaintext { sasl } | SecurityConfig::SaslSsl { sasl, .. } =
        &p.security
        && sasl.username.trim().is_empty()
    {
        return Err(AppError::invalid("Укажите имя пользователя SASL"));
    }
    if let SecurityConfig::Ssl { tls } | SecurityConfig::SaslSsl { tls, .. } = &p.security {
        validate_tls(tls)?;
    }
    // Секреты хранятся только в keychain: connections.json не должен их содержать.
    if let Some(key) = p.extra_properties.keys().find(|k| is_secret_property(k)) {
        return Err(AppError::invalid(format!(
            "Секреты нельзя хранить в доп. свойствах: {key}"
        )));
    }
    if p.request_timeout_ms < 1_000 {
        return Err(AppError::invalid(
            "Таймаут запроса должен быть не меньше 1000 мс",
        ));
    }
    Ok(())
}

const CURRENT_SCHEMA_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConnectionsFile {
    schema_version: u32,
    connections: Vec<ConnectionProfile>,
}

pub struct ConnectionStore {
    path: PathBuf,
    cache: RwLock<Vec<ConnectionProfile>>,
}

impl ConnectionStore {
    pub fn open(dir: &Path) -> AppResult<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("connections.json");
        let connections = match read_json::<ConnectionsFile>(&path)? {
            Some(file) if file.schema_version > CURRENT_SCHEMA_VERSION => {
                return Err(AppError::internal(format!(
                    "connections.json создан более новой версией приложения (schemaVersion {})",
                    file.schema_version
                )));
            }
            Some(file) => file.connections,
            None => Vec::new(),
        };
        Ok(Self {
            path,
            cache: RwLock::new(connections),
        })
    }

    pub fn list(&self) -> Vec<ConnectionProfile> {
        self.cache.read_safe().clone()
    }

    pub fn get(&self, id: &str) -> AppResult<ConnectionProfile> {
        self.cache
            .read_safe()
            .iter()
            .find(|c| c.id == id)
            .cloned()
            .ok_or_else(|| AppError::not_found(format!("Подключение {id} не найдено")))
    }

    pub fn upsert(&self, profile: ConnectionProfile) -> AppResult<ConnectionProfile> {
        validate_profile(&profile)?;
        let mut cache = self.cache.write_safe();
        let mut next = cache.clone();
        match next.iter_mut().find(|c| c.id == profile.id) {
            Some(slot) => *slot = profile.clone(),
            None => next.push(profile.clone()),
        }
        self.persist(&next)?;
        *cache = next;
        Ok(profile)
    }

    pub fn delete(&self, id: &str) -> AppResult<()> {
        let mut cache = self.cache.write_safe();
        if !cache.iter().any(|c| c.id == id) {
            return Err(AppError::not_found(format!("Подключение {id} не найдено")));
        }
        let next: Vec<_> = cache.iter().filter(|c| c.id != id).cloned().collect();
        self.persist(&next)?;
        *cache = next;
        Ok(())
    }

    fn persist(&self, list: &[ConnectionProfile]) -> AppResult<()> {
        write_json(
            &self.path,
            &ConnectionsFile {
                schema_version: CURRENT_SCHEMA_VERSION,
                connections: list.to_vec(),
            },
        )
    }
}

#[cfg(test)]
pub fn sample_profile(id: &str) -> ConnectionProfile {
    ConnectionProfile {
        id: id.to_string(),
        name: format!("conn-{id}"),
        color: "#4f8cff".into(),
        environment: Environment::Dev,
        read_only: false,
        bootstrap_servers: "localhost:9092".into(),
        security: SecurityConfig::Plaintext,
        extra_properties: BTreeMap::new(),
        request_timeout_ms: 30_000,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[test]
    fn old_tls_profiles_get_defaults() {
        let tls: TlsConfig =
            serde_json::from_str(r#"{"caLocation":"/ca.pem","verifyHostname":false}"#).unwrap();
        assert_eq!(tls.ca_format, CertFormat::Pem);
        assert_eq!(tls.client_cert, None);
        assert!(!tls.verify_hostname);
    }

    #[test]
    fn client_cert_json_shape() {
        let tls = TlsConfig {
            ca_location: None,
            ca_format: CertFormat::Jks,
            verify_hostname: true,
            client_cert: Some(ClientCert {
                format: CertFormat::Pkcs12,
                certificate_path: "/c.p12".into(),
                key_path: None,
                alias: None,
            }),
        };
        let v = serde_json::to_value(&tls).unwrap();
        assert_eq!(v["caFormat"], "jks");
        assert_eq!(v["clientCert"]["format"], "pkcs12");
        assert_eq!(v["clientCert"]["certificatePath"], "/c.p12");
    }

    fn tls_profile(tls: TlsConfig) -> ConnectionProfile {
        let mut p = sample_profile("a");
        p.security = SecurityConfig::Ssl { tls };
        p
    }

    #[test]
    fn validation_of_certificate_paths() {
        let base = TlsConfig {
            ca_location: None,
            ca_format: CertFormat::Pem,
            verify_hostname: true,
            client_cert: None,
        };
        let pem = |cert: &str, key: Option<&str>| TlsConfig {
            client_cert: Some(ClientCert {
                format: CertFormat::Pem,
                certificate_path: cert.into(),
                key_path: key.map(String::from),
                alias: None,
            }),
            ..base.clone()
        };
        let abs = if cfg!(windows) {
            "C:\\c\\c.pem"
        } else {
            "/c/c.pem"
        };
        assert!(validate_profile(&tls_profile(pem(abs, Some(abs)))).is_ok());
        for (tls, needle) in [
            (pem("", Some(abs)), "сертификат"),
            (pem("rel/c.pem", Some(abs)), "абсолютный"),
            (pem(abs, None), "ключ"),
            (
                TlsConfig {
                    ca_location: Some("ca.pem".into()),
                    ..base.clone()
                },
                "абсолютный",
            ),
        ] {
            let e = validate_profile(&tls_profile(tls)).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput);
            assert!(e.message.to_lowercase().contains(needle), "{}", e.message);
        }
    }

    #[test]
    fn private_key_material_is_rejected_in_extra_properties() {
        for key in ["ssl.key.pem", "SSL.KEYSTORE.KEY"] {
            let mut p = sample_profile("a");
            p.extra_properties.insert(key.into(), "x".into());
            assert_eq!(
                validate_profile(&p).unwrap_err().code,
                ErrorCode::InvalidInput,
                "{key}"
            );
        }
    }

    #[test]
    fn parse_bootstrap_variants() {
        assert_eq!(
            parse_bootstrap(" a:9092, b:9093 ,").unwrap(),
            vec![("a".to_string(), 9092), ("b".to_string(), 9093)]
        );
        assert_eq!(
            parse_bootstrap("[::1]:9092").unwrap(),
            vec![("::1".to_string(), 9092)]
        );
        assert_eq!(
            parse_bootstrap("host").unwrap_err().code,
            ErrorCode::InvalidInput
        );
        assert_eq!(
            parse_bootstrap("host:abc").unwrap_err().code,
            ErrorCode::InvalidInput
        );
        assert_eq!(
            parse_bootstrap(" , ").unwrap_err().code,
            ErrorCode::InvalidInput
        );
    }

    #[test]
    fn persists_and_reloads() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStore::open(dir.path()).unwrap();
        store.upsert(sample_profile("a")).unwrap();
        let mut b = sample_profile("b");
        b.security = SecurityConfig::SaslSsl {
            tls: TlsConfig {
                ca_location: Some(
                    if cfg!(windows) {
                        "C:\\ca.pem"
                    } else {
                        "/ca.pem"
                    }
                    .into(),
                ),
                ca_format: CertFormat::Pem,
                verify_hostname: false,
                client_cert: None,
            },
            sasl: SaslConfig {
                mechanism: SaslMechanism::ScramSha512,
                username: "alice".into(),
            },
        };
        store.upsert(b.clone()).unwrap();

        let reopened = ConnectionStore::open(dir.path()).unwrap();
        assert_eq!(reopened.list().len(), 2);
        assert_eq!(reopened.get("b").unwrap(), b);
    }

    #[test]
    fn upsert_replaces_and_delete_removes() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStore::open(dir.path()).unwrap();
        store.upsert(sample_profile("a")).unwrap();
        let mut a2 = sample_profile("a");
        a2.name = "renamed".into();
        store.upsert(a2).unwrap();
        assert_eq!(store.list().len(), 1);
        assert_eq!(store.get("a").unwrap().name, "renamed");
        store.delete("a").unwrap();
        assert_eq!(store.get("a").unwrap_err().code, ErrorCode::NotFound);
        assert_eq!(store.delete("a").unwrap_err().code, ErrorCode::NotFound);
    }

    #[test]
    fn validation_rejects_secrets_in_extra_properties() {
        for key in [
            "sasl.password",
            "SSL.Key.Password",
            "my.secret",
            "SASL.JAAS.CONFIG",
        ] {
            let mut p = sample_profile("a");
            p.extra_properties.insert(key.into(), "x".into());
            let e = validate_profile(&p).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput, "{key}");
            assert!(e.message.contains(key), "{}", e.message);
        }
        let mut p = sample_profile("a");
        p.extra_properties
            .insert("fetch.max.bytes".into(), "1".into());
        assert!(validate_profile(&p).is_ok());
    }

    #[test]
    fn validation_rejects_bad_profiles() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStore::open(dir.path()).unwrap();
        let mut p = sample_profile("a");
        p.name = "  ".into();
        assert_eq!(store.upsert(p).unwrap_err().code, ErrorCode::InvalidInput);
        let mut p = sample_profile("a");
        p.bootstrap_servers = "nope".into();
        assert_eq!(store.upsert(p).unwrap_err().code, ErrorCode::InvalidInput);
        assert!(store.list().is_empty());
    }

    #[test]
    fn json_shape_matches_frontend_contract() {
        let mut p = sample_profile("x");
        p.security = SecurityConfig::SaslPlaintext {
            sasl: SaslConfig {
                mechanism: SaslMechanism::ScramSha256,
                username: "u".into(),
            },
        };
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["bootstrapServers"], "localhost:9092");
        assert_eq!(v["environment"], "dev");
        assert_eq!(v["security"]["protocol"], "SASL_PLAINTEXT");
        assert_eq!(v["security"]["sasl"]["mechanism"], "SCRAM-SHA-256");
        assert_eq!(v["requestTimeoutMs"], 30000);
    }

    #[test]
    fn file_contains_schema_version_and_no_password_field() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStore::open(dir.path()).unwrap();
        store.upsert(sample_profile("a")).unwrap();
        let raw = std::fs::read_to_string(dir.path().join("connections.json")).unwrap();
        assert!(raw.contains("\"schemaVersion\": 1"));
        assert!(!raw.to_lowercase().contains("password"));
    }
}
