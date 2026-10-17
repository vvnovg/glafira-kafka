use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult, ErrorCode};
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
    #[serde(default)]
    pub folder_id: Option<String>,
    #[serde(default)]
    pub order: i64,
    /// Поля из более новой версии Glafira: сохраняются при записи как есть.
    /// В `ConnectionStore::upsert` пустой `extra` у существующего профиля означает
    /// «оставить сохранённый»: форма фронтенда может не знать этих полей.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionFolder {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub order: i64,
    /// Поля из более новой версии Glafira: сохраняются при записи как есть.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

pub fn parse_bootstrap(s: &str) -> AppResult<Vec<(String, u16)>> {
    let mut out = Vec::new();
    for raw in s.split(',').map(str::trim).filter(|x| !x.is_empty()) {
        let (host, port) = raw.rsplit_once(':').ok_or_else(|| {
            app_err!(
                ErrorCode::InvalidInput,
                "errors.connections.addressFormat",
                "Address “{{address}}” must be in host:port format",
                address = raw
            )
        })?;
        let port: u16 = port.parse().map_err(|_| {
            app_err!(
                ErrorCode::InvalidInput,
                "errors.connections.badPort",
                "Invalid port in address “{{address}}”",
                address = raw
            )
        })?;
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if host.is_empty() {
            return Err(app_err!(
                ErrorCode::InvalidInput,
                "errors.connections.emptyHost",
                "Empty host in address “{{address}}”",
                address = raw
            ));
        }
        out.push((host.to_string(), port));
    }
    if out.is_empty() {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.connections.noBootstrap",
            "Specify at least one bootstrap server"
        ));
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

/// Файл TLS, путь к которому проверяется: у каждого свои тексты ошибок.
#[derive(Clone, Copy)]
enum TlsFile {
    Ca,
    Certificate,
    Key,
}

fn path_required(what: TlsFile) -> AppError {
    match what {
        TlsFile::Ca => app_err!(
            ErrorCode::InvalidInput,
            "errors.connections.caRequired",
            "Specify a file: trusted CA"
        ),
        TlsFile::Certificate => app_err!(
            ErrorCode::InvalidInput,
            "errors.connections.certificateRequired",
            "Specify a file: client certificate"
        ),
        TlsFile::Key => app_err!(
            ErrorCode::InvalidInput,
            "errors.connections.keyRequired",
            "Specify a file: client key"
        ),
    }
}

fn path_not_absolute(what: TlsFile, path: &str) -> AppError {
    match what {
        TlsFile::Ca => app_err!(
            ErrorCode::InvalidInput,
            "errors.connections.caNotAbsolute",
            "Trusted CA: specify an absolute path (currently “{{path}}”)",
            path = path
        ),
        TlsFile::Certificate => app_err!(
            ErrorCode::InvalidInput,
            "errors.connections.certificateNotAbsolute",
            "Client certificate: specify an absolute path (currently “{{path}}”)",
            path = path
        ),
        TlsFile::Key => app_err!(
            ErrorCode::InvalidInput,
            "errors.connections.keyNotAbsolute",
            "Client key: specify an absolute path (currently “{{path}}”)",
            path = path
        ),
    }
}

fn validate_path(path: &str, what: TlsFile) -> AppResult<()> {
    if path.trim().is_empty() {
        return Err(path_required(what));
    }
    if !std::path::Path::new(path.trim()).is_absolute() {
        return Err(path_not_absolute(what, path));
    }
    Ok(())
}

fn validate_tls(tls: &TlsConfig) -> AppResult<()> {
    if let Some(ca) = tls.ca_location.as_deref().filter(|s| !s.trim().is_empty()) {
        validate_path(ca, TlsFile::Ca)?;
    }
    if let Some(cc) = &tls.client_cert {
        validate_path(&cc.certificate_path, TlsFile::Certificate)?;
        if cc.format == CertFormat::Pem {
            match cc.key_path.as_deref() {
                Some(k) if !k.trim().is_empty() => validate_path(k, TlsFile::Key)?,
                _ => {
                    return Err(app_err!(
                        ErrorCode::InvalidInput,
                        "errors.connections.pemKeyRequired",
                        "Specify a file: client key (PEM)"
                    ));
                }
            }
        }
    }
    Ok(())
}

pub fn validate_profile(p: &ConnectionProfile) -> AppResult<()> {
    if p.id.trim().is_empty() {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.connections.noId",
            "The connection has no ID"
        ));
    }
    if p.name.trim().is_empty() {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.connections.emptyName",
            "Connection name cannot be empty"
        ));
    }
    parse_bootstrap(&p.bootstrap_servers)?;
    if let SecurityConfig::SaslPlaintext { sasl } | SecurityConfig::SaslSsl { sasl, .. } =
        &p.security
        && sasl.username.trim().is_empty()
    {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.connections.saslUsernameRequired",
            "Specify the SASL username"
        ));
    }
    if let SecurityConfig::Ssl { tls } | SecurityConfig::SaslSsl { tls, .. } = &p.security {
        validate_tls(tls)?;
    }
    // Секреты хранятся только в keychain: connections.json не должен их содержать.
    // Неизвестные поля профиля тоже пишутся в файл: секретоподобные не принимаем.
    let secret_extra = p.extra.keys().find(|k| {
        let k = k.to_lowercase();
        k.contains("password") || k.contains("secret")
    });
    if let Some(key) = p
        .extra_properties
        .keys()
        .find(|k| is_secret_property(k))
        .or(secret_extra)
    {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.connections.secretInExtraProperties",
            "Secrets cannot be stored in extra properties: {{key}}",
            key = key
        ));
    }
    if p.request_timeout_ms < 1_000 {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.connections.timeoutTooSmall",
            "Request timeout must be at least 1000 ms"
        ));
    }
    Ok(())
}

fn connection_not_found(id: &str) -> AppError {
    app_err!(
        ErrorCode::NotFound,
        "errors.connections.notFound",
        "Connection {{id}} not found",
        id = id
    )
}

const CURRENT_SCHEMA_VERSION: u32 = 1;
const FOLDER_NAME_MAX_CHARS: usize = 100;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConnectionsFile {
    schema_version: u32,
    #[serde(default)]
    folders: Vec<ConnectionFolder>,
    connections: Vec<ConnectionProfile>,
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}

/// Запись файла без копирования данных кэша.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConnectionsFileRef<'a> {
    schema_version: u32,
    folders: &'a [ConnectionFolder],
    connections: &'a [ConnectionProfile],
    #[serde(flatten)]
    extra: &'a serde_json::Map<String, serde_json::Value>,
}

/// Содержимое `connections.json` в памяти: подключения, папки и неизвестные поля файла.
#[derive(Debug, Clone, Default)]
pub(crate) struct StoreData {
    pub connections: Vec<ConnectionProfile>,
    pub folders: Vec<ConnectionFolder>,
    pub extra: serde_json::Map<String, serde_json::Value>,
}

fn folder_not_found(id: &str) -> AppError {
    app_err!(
        ErrorCode::NotFound,
        "errors.folders.notFound",
        "Folder {{id}} not found",
        id = id
    )
}

/// Проверяет имя папки: непустое после trim и не длиннее 100 символов.
pub(crate) fn validate_folder_name(name: &str) -> AppResult<()> {
    let name = name.trim();
    if name.is_empty() {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.folders.nameEmpty",
            "Enter a folder name"
        ));
    }
    if name.chars().count() > FOLDER_NAME_MAX_CHARS {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.folders.nameTooLong",
            "The folder name is longer than 100 characters"
        ));
    }
    Ok(())
}

fn truncate_chars(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

/// Имя, не совпадающее (без учёта регистра) ни с одним из `taken`: при совпадении добавляется
/// суффикс « (2)», « (3)»…; основа укорачивается, чтобы имя уложилось в 100 символов.
pub(crate) fn unique_name(taken: &[&str], wanted: &str) -> String {
    let is_taken = |name: &str| {
        let name = name.to_lowercase();
        taken.iter().any(|t| t.trim().to_lowercase() == name)
    };
    let wanted = truncate_chars(wanted, FOLDER_NAME_MAX_CHARS);
    if !is_taken(wanted) {
        return wanted.to_string();
    }
    (2..)
        .map(|n| {
            let suffix = format!(" ({n})");
            let base = truncate_chars(wanted, FOLDER_NAME_MAX_CHARS - suffix.chars().count());
            format!("{}{suffix}", base.trim_end())
        })
        .find(|candidate| !is_taken(candidate))
        .expect("suffix sequence is infinite")
}

/// Инварианты дерева: ссылки на папки существуют, циклов нет,
/// имена папок валидны и уникальны среди соседей без учёта регистра.
pub(crate) fn check_tree(data: &StoreData) -> AppResult<()> {
    use std::collections::{HashMap, HashSet};

    let by_id: HashMap<&str, &ConnectionFolder> =
        data.folders.iter().map(|f| (f.id.as_str(), f)).collect();
    for c in &data.connections {
        if let Some(fid) = c.folder_id.as_deref()
            && !by_id.contains_key(fid)
        {
            return Err(folder_not_found(fid));
        }
    }
    for f in &data.folders {
        if let Some(pid) = f.parent_id.as_deref()
            && !by_id.contains_key(pid)
        {
            return Err(folder_not_found(pid));
        }
    }
    for f in &data.folders {
        let mut seen = HashSet::new();
        let mut cur = Some(f);
        while let Some(node) = cur {
            if !seen.insert(node.id.as_str()) {
                return Err(app_err!(
                    ErrorCode::InvalidInput,
                    "errors.folders.cycle",
                    "A folder cannot be moved into itself or its subfolder"
                ));
            }
            cur = node
                .parent_id
                .as_deref()
                .and_then(|p| by_id.get(p).copied());
        }
    }
    let mut names = HashSet::new();
    for f in &data.folders {
        validate_folder_name(&f.name)?;
        let name = f.name.trim();
        if !names.insert((f.parent_id.as_deref(), name.to_lowercase())) {
            return Err(app_err!(
                ErrorCode::InvalidInput,
                "errors.folders.nameTaken",
                "A folder named “{{name}}” already exists here",
                name = name
            ));
        }
    }
    Ok(())
}

/// Приводит прочитанный файл к допустимому виду без ошибок:
/// старый файл получает `order` по порядку списка, висячие ссылки и циклы уходят в корень.
fn repair_on_read(data: &mut StoreData) {
    use std::collections::{HashMap, HashSet};

    if data.connections.len() > 1 && data.connections.iter().all(|c| c.order == 0) {
        for (i, c) in data.connections.iter_mut().enumerate() {
            c.order = i as i64;
        }
    }
    let ids: HashSet<String> = data.folders.iter().map(|f| f.id.clone()).collect();
    for c in &mut data.connections {
        if c.folder_id.as_ref().is_some_and(|id| !ids.contains(id)) {
            c.folder_id = None;
        }
    }
    for f in &mut data.folders {
        if f.parent_id.as_ref().is_some_and(|id| !ids.contains(id)) {
            f.parent_id = None;
        }
    }
    // Цикл из правленного вручную файла: в корень уходит первая папка, лежащая на цикле
    // (путь вверх возвращается к ней самой); папки, лишь ведущие в цикл, остаются на месте.
    for i in 0..data.folders.len() {
        let parents: HashMap<&str, Option<&str>> = data
            .folders
            .iter()
            .map(|f| (f.id.as_str(), f.parent_id.as_deref()))
            .collect();
        let start = data.folders[i].id.as_str();
        let mut seen = HashSet::new();
        let mut cur = parents.get(start).copied().flatten();
        let mut on_cycle = false;
        while let Some(id) = cur {
            if id == start {
                on_cycle = true;
                break;
            }
            if !seen.insert(id) {
                break;
            }
            cur = parents.get(id).copied().flatten();
        }
        if on_cycle {
            data.folders[i].parent_id = None;
        }
    }
    // Имена: trim, пустое — id папки, не длиннее 100 символов, уникальны среди соседей.
    let mut taken: HashMap<Option<String>, Vec<String>> = HashMap::new();
    for f in &mut data.folders {
        let trimmed = f.name.trim();
        let wanted = if trimmed.is_empty() {
            f.id.trim()
        } else {
            trimmed
        };
        let wanted = truncate_chars(wanted, FOLDER_NAME_MAX_CHARS)
            .trim_end()
            .to_string();
        let siblings = taken.entry(f.parent_id.clone()).or_default();
        let refs: Vec<&str> = siblings.iter().map(String::as_str).collect();
        let name = unique_name(&refs, &wanted);
        siblings.push(name.clone());
        f.name = name;
    }
}

pub struct ConnectionStore {
    path: PathBuf,
    cache: RwLock<StoreData>,
}

impl ConnectionStore {
    pub fn open(dir: &Path) -> AppResult<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("connections.json");
        let mut data = match read_json::<ConnectionsFile>(&path)? {
            Some(file) if file.schema_version > CURRENT_SCHEMA_VERSION => {
                return Err(crate::storage::fs::newer_schema_error(
                    &path,
                    file.schema_version,
                ));
            }
            Some(file) => StoreData {
                connections: file.connections,
                folders: file.folders,
                extra: file.extra,
            },
            None => StoreData::default(),
        };
        repair_on_read(&mut data);
        Ok(Self {
            path,
            cache: RwLock::new(data),
        })
    }

    pub fn list(&self) -> Vec<ConnectionProfile> {
        self.cache.read_safe().connections.clone()
    }

    pub fn list_folders(&self) -> Vec<ConnectionFolder> {
        self.cache.read_safe().folders.clone()
    }

    pub fn get(&self, id: &str) -> AppResult<ConnectionProfile> {
        self.cache
            .read_safe()
            .connections
            .iter()
            .find(|c| c.id == id)
            .cloned()
            .ok_or_else(|| connection_not_found(id))
    }

    /// Изменяет данные одной транзакцией: `f` работает с копией, затем проверяются
    /// инварианты дерева и пишется файл; кэш заменяется только после успешной записи.
    pub(crate) fn update<R>(&self, f: impl FnOnce(&mut StoreData) -> AppResult<R>) -> AppResult<R> {
        let mut cache = self.cache.write_safe();
        let mut next = cache.clone();
        let result = f(&mut next)?;
        check_tree(&next)?;
        self.persist(&next)?;
        *cache = next;
        Ok(result)
    }

    /// Создаёт или обновляет подключение и возвращает его в сохранённом виде.
    ///
    /// Новое подключение кладётся в конец папки `folder_id` входящего профиля.
    /// У существующего `folder_id` и `order` берутся из сохранённого, входящие значения
    /// игнорируются: место подключения меняют только команды перемещения. Пустой `extra`
    /// входящего профиля тоже означает «оставить сохранённый».
    pub fn upsert(&self, mut profile: ConnectionProfile) -> AppResult<ConnectionProfile> {
        validate_profile(&profile)?;
        self.update(|data| {
            match data.connections.iter_mut().find(|c| c.id == profile.id) {
                Some(slot) => {
                    // Профиль из формы не знает неизвестных полей: берём их из сохранённого.
                    if profile.extra.is_empty() {
                        profile.extra = std::mem::take(&mut slot.extra);
                    }
                    profile.folder_id = slot.folder_id.take();
                    profile.order = slot.order;
                    *slot = profile.clone();
                }
                None => {
                    profile.order = data
                        .connections
                        .iter()
                        .filter(|c| c.folder_id == profile.folder_id)
                        .map(|c| c.order + 1)
                        .max()
                        .unwrap_or(0);
                    data.connections.push(profile.clone());
                }
            }
            Ok(profile)
        })
    }

    pub fn delete(&self, id: &str) -> AppResult<()> {
        self.update(|data| {
            if !data.connections.iter().any(|c| c.id == id) {
                return Err(connection_not_found(id));
            }
            data.connections.retain(|c| c.id != id);
            Ok(())
        })
    }

    fn persist(&self, data: &StoreData) -> AppResult<()> {
        write_json(
            &self.path,
            &ConnectionsFileRef {
                schema_version: CURRENT_SCHEMA_VERSION,
                folders: &data.folders,
                connections: &data.connections,
                extra: &data.extra,
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
        folder_id: None,
        order: 0,
        extra: Default::default(),
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
        for (tls, key) in [
            (pem("", Some(abs)), "errors.connections.certificateRequired"),
            (
                pem("rel/c.pem", Some(abs)),
                "errors.connections.certificateNotAbsolute",
            ),
            (pem(abs, None), "errors.connections.pemKeyRequired"),
            (pem(abs, Some("k.pem")), "errors.connections.keyNotAbsolute"),
            (
                TlsConfig {
                    ca_location: Some("ca.pem".into()),
                    ..base.clone()
                },
                "errors.connections.caNotAbsolute",
            ),
        ] {
            let e = validate_profile(&tls_profile(tls)).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput);
            assert_eq!(e.key, key, "{}", e.message);
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
        let b = store.upsert(b).unwrap();

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
            assert_eq!(e.key, "errors.connections.secretInExtraProperties");
            assert_eq!(e.params["key"], key);
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

    #[test]
    fn migration_keeps_connections_in_root_with_order() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("connections.json"),
            r##"{"schemaVersion":1,"connections":[
          {"id":"a","name":"A","color":"#fff","environment":"dev","readOnly":false,"bootstrapServers":"h:1","security":{"protocol":"PLAINTEXT"}},
          {"id":"b","name":"B","color":"#fff","environment":"dev","readOnly":false,"bootstrapServers":"h:1","security":{"protocol":"PLAINTEXT"}}]}"##,
        )
        .unwrap();
        let store = ConnectionStore::open(dir.path()).unwrap();
        let list = store.list();
        assert_eq!(
            list.iter()
                .map(|c| (c.id.as_str(), c.folder_id.clone(), c.order))
                .collect::<Vec<_>>(),
            vec![("a", None, 0), ("b", None, 1)]
        );
        assert!(store.list_folders().is_empty());
    }

    #[test]
    fn unknown_fields_survive_write() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("connections.json"),
            r##"{"schemaVersion":1,"future":{"x":1},"folders":[{"id":"f","name":"F","parentId":null,"order":0,"icon":"star"}],
           "connections":[{"id":"a","name":"A","color":"#fff","environment":"dev","readOnly":false,"bootstrapServers":"h:1",
           "security":{"protocol":"PLAINTEXT"},"folderId":"f","order":0,"pinned":true}]}"##,
        )
        .unwrap();
        let store = ConnectionStore::open(dir.path()).unwrap();
        let mut p = store.get("a").unwrap();
        p.name = "A2".into();
        p.extra.clear(); // как профиль из формы фронтенда — без неизвестных полей
        store.upsert(p).unwrap();
        let raw: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("connections.json")).unwrap())
                .unwrap();
        assert_eq!(raw["future"]["x"], 1);
        assert_eq!(raw["folders"][0]["icon"], "star");
        assert_eq!(raw["connections"][0]["pinned"], true);
        assert_eq!(raw["connections"][0]["name"], "A2");
        assert_eq!(raw["connections"][0]["folderId"], "f");
    }

    #[test]
    fn new_connection_goes_to_end_of_its_folder() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStore::open(dir.path()).unwrap();
        store.upsert(sample_profile("a")).unwrap();
        store.upsert(sample_profile("b")).unwrap();
        assert_eq!(store.get("b").unwrap().order, 1);
    }

    #[test]
    fn dangling_folder_reference_is_rejected_on_write() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStore::open(dir.path()).unwrap();
        let mut p = sample_profile("a");
        p.folder_id = Some("missing".into());
        let e = store.upsert(p).unwrap_err();
        assert_eq!(e.key, "errors.folders.notFound");
    }

    fn folder(id: &str, name: &str, parent: Option<&str>) -> ConnectionFolder {
        ConnectionFolder {
            id: id.into(),
            name: name.into(),
            parent_id: parent.map(String::from),
            order: 0,
            extra: Default::default(),
        }
    }

    #[test]
    fn check_tree_rejects_bad_folders() {
        let data = |folders: Vec<ConnectionFolder>| StoreData {
            folders,
            ..Default::default()
        };
        assert!(
            check_tree(&data(vec![
                folder("a", "A", None),
                folder("b", "a", Some("a"))
            ]))
            .is_ok()
        );
        for (folders, key) in [
            (
                vec![folder("a", "A", Some("b")), folder("b", "B", Some("a"))],
                "errors.folders.cycle",
            ),
            (vec![folder("a", "A", Some("a"))], "errors.folders.cycle"),
            (vec![folder("a", "A", Some("x"))], "errors.folders.notFound"),
            (vec![folder("a", "  ", None)], "errors.folders.nameEmpty"),
            (
                vec![folder("a", &"я".repeat(101), None)],
                "errors.folders.nameTooLong",
            ),
            (
                vec![folder("a", "Prod", None), folder("b", " prod ", None)],
                "errors.folders.nameTaken",
            ),
        ] {
            assert_eq!(check_tree(&data(folders)).unwrap_err().key, key);
        }
        assert!(check_tree(&data(vec![folder("a", &"я".repeat(100), None)])).is_ok());
    }

    #[test]
    fn dangling_references_and_cycles_go_to_root_on_read() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("connections.json"),
            r##"{"schemaVersion":1,"folders":[
              {"id":"f","name":"F","parentId":"gone"},
              {"id":"x","name":"X","parentId":"y"},{"id":"y","name":"Y","parentId":"x"}],
              "connections":[{"id":"a","name":"A","color":"#fff","environment":"dev","readOnly":false,
              "bootstrapServers":"h:1","security":{"protocol":"PLAINTEXT"},"folderId":"gone"}]}"##,
        )
        .unwrap();
        let store = ConnectionStore::open(dir.path()).unwrap();
        assert_eq!(store.get("a").unwrap().folder_id, None);
        let folders = store.list_folders();
        assert_eq!(folders[0].parent_id, None);
        check_tree(&store.cache.read_safe()).unwrap();
        store.delete("a").unwrap();
    }

    #[test]
    fn folder_names_are_repaired_on_read() {
        let dir = tempfile::tempdir().unwrap();
        let long = "я".repeat(101);
        std::fs::write(
            dir.path().join("connections.json"),
            format!(
                r##"{{"schemaVersion":1,"folders":[
                  {{"id":"e","name":"   "}},{{"id":"l","name":"{long}"}},
                  {{"id":"p1","name":"Prod"}},{{"id":"p2","name":" prod "}},{{"id":"p3","name":"PROD"}},
                  {{"id":"c","name":"prod","parentId":"p1"}}],
                  "connections":[]}}"##
            ),
        )
        .unwrap();
        let store = ConnectionStore::open(dir.path()).unwrap();
        check_tree(&store.cache.read_safe()).unwrap();
        let names: Vec<_> = store.list_folders().into_iter().map(|f| f.name).collect();
        assert_eq!(names[0], "e");
        assert_eq!(names[1], "я".repeat(100));
        assert_eq!(&names[2..], ["Prod", "prod (2)", "PROD (3)", "prod"]);
        store.upsert(sample_profile("a")).unwrap();
    }

    #[test]
    fn unique_name_adds_suffix_case_insensitively() {
        assert_eq!(unique_name(&[], "A"), "A");
        assert_eq!(unique_name(&["a"], "A"), "A (2)");
        assert_eq!(unique_name(&["A", "a (2)"], "A"), "A (3)");
        let long = "x".repeat(100);
        let n = unique_name(&[long.as_str()], &long);
        assert_eq!(n.chars().count(), 100);
        assert!(n.ends_with(" (2)"));
    }

    #[test]
    fn upsert_of_existing_profile_keeps_its_place() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStore::open(dir.path()).unwrap();
        store
            .update(|d| {
                d.folders.push(folder("f", "F", None));
                Ok(())
            })
            .unwrap();
        store.upsert(sample_profile("x")).unwrap();
        let mut p = sample_profile("a");
        p.folder_id = Some("f".into());
        let saved = store.upsert(p).unwrap();
        assert_eq!(saved.order, 0);
        let mut edited = saved.clone();
        edited.folder_id = None;
        edited.order = 99;
        edited.name = "renamed".into();
        let back = store.upsert(edited).unwrap();
        assert_eq!(back.folder_id.as_deref(), Some("f"));
        assert_eq!(back.order, 0);
        let stored = store.get("a").unwrap();
        assert_eq!(
            (
                stored.folder_id.as_deref(),
                stored.order,
                stored.name.as_str()
            ),
            (Some("f"), 0, "renamed")
        );
    }

    #[test]
    fn cycle_repair_detaches_only_folders_on_the_cycle() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("connections.json"),
            r##"{"schemaVersion":1,"folders":[
              {"id":"z","name":"Z","parentId":"x"},
              {"id":"x","name":"X","parentId":"y"},{"id":"y","name":"Y","parentId":"x"}],
              "connections":[]}"##,
        )
        .unwrap();
        let store = ConnectionStore::open(dir.path()).unwrap();
        let f = store.list_folders();
        assert_eq!(f[0].parent_id.as_deref(), Some("x"));
        let roots = f[1..].iter().filter(|f| f.parent_id.is_none()).count();
        assert_eq!(roots, 1);
        check_tree(&store.cache.read_safe()).unwrap();
    }

    #[test]
    fn secret_like_unknown_fields_are_rejected() {
        for key in ["saslPassword", "clientSecret"] {
            let mut p = sample_profile("a");
            p.extra.insert(key.into(), serde_json::json!("x"));
            let e = validate_profile(&p).unwrap_err();
            assert_eq!(e.key, "errors.connections.secretInExtraProperties", "{key}");
            assert_eq!(e.params["key"], key);
        }
        let mut p = sample_profile("a");
        p.extra.insert("pinned".into(), serde_json::json!(true));
        assert!(validate_profile(&p).is_ok());
    }
}
