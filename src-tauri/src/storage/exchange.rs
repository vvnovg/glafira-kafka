//! Файл обмена подключениями (`glafira-connections`): экспорт без секретов, предпросмотр и
//! применение импорта.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::error::{AppResult, ErrorCode};
use crate::i18n::LocalizedText;
use crate::kafka::config::{
    SASL_PASSWORD, SSL_KEY_PASSWORD, SSL_KEYSTORE_PASSWORD, SSL_TRUSTSTORE_PASSWORD, uses_secret,
};
use crate::secrets::{SecretStore, secret_key};
use crate::storage::connections::{
    ConnectionFolder, ConnectionProfile, ConnectionStore, Environment, LlmAccess, SecurityConfig,
    StoreData, check_tree, folder_not_found, is_secret_property, unique_name, validate_profile,
};
use crate::storage::tree::TreeItemKind;

pub const EXCHANGE_FORMAT: &str = "glafira-connections";
pub const EXCHANGE_VERSION: u32 = 1;

/// Какие пароли были сохранены у подключения при экспорте.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NeedsPassword {
    pub sasl: bool,
    pub truststore: bool,
    pub keystore: bool,
    pub key: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeFileOut<'a> {
    pub format: &'a str,
    pub version: u32,
    pub exported_at: String,
    pub app_version: &'a str,
    pub folders: Vec<ConnectionFolder>,
    pub connections: Vec<ExportedConnection>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ExportedConnection {
    #[serde(flatten)]
    pub profile: ConnectionProfile,
    #[serde(default)]
    pub needs_password: NeedsPassword,
}

/// Итог экспорта для фронтенда.
#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExportSummary {
    pub connections: usize,
    pub folders: usize,
}

/// Флаг поднимается, только если пароль сохранён и профиль его действительно использует
/// (матрица `uses_secret`, та же, что при подключении и сохранении): устаревшие записи keychain
/// не требуют ввода у получателя.
fn needs_password(secrets: &dyn SecretStore, p: &ConnectionProfile) -> AppResult<NeedsPassword> {
    let check = |field: &str| -> AppResult<bool> {
        if !uses_secret(&p.security, field) {
            return Ok(false);
        }
        Ok(secrets
            .get(&secret_key(&p.id, field))?
            .is_some_and(|v| !v.is_empty()))
    };
    Ok(NeedsPassword {
        sasl: check(SASL_PASSWORD)?,
        truststore: check(SSL_TRUSTSTORE_PASSWORD)?,
        keystore: check(SSL_KEYSTORE_PASSWORD)?,
        key: check(SSL_KEY_PASSWORD)?,
    })
}

/// Собирает файл экспорта: всё дерево или поддерево `root` (сама папка становится корнем).
/// Секреты (keychain и секретные `extraProperties`) не попадают в файл; неизвестные локальные
/// поля (`extra`) не выносятся.
pub fn build_export(
    store: &ConnectionStore,
    secrets: &dyn SecretStore,
    root: Option<&str>,
    app_version: &'static str,
    now: &str,
) -> AppResult<ExchangeFileOut<'static>> {
    let all_folders = store.list_folders();
    let included: Option<HashSet<String>> = match root {
        None => None,
        Some(root) => {
            if !all_folders.iter().any(|f| f.id == root) {
                return Err(folder_not_found(root));
            }
            let mut set = HashSet::from([root.to_string()]);
            // Дерево без циклов (инвариант хранилища), поэтому достаточно расширять до неподвижной точки.
            loop {
                let before = set.len();
                for f in &all_folders {
                    if f.parent_id.as_deref().is_some_and(|p| set.contains(p)) {
                        set.insert(f.id.clone());
                    }
                }
                if set.len() == before {
                    break;
                }
            }
            Some(set)
        }
    };
    let in_scope = |folder: Option<&str>| match &included {
        None => true,
        Some(set) => folder.is_some_and(|id| set.contains(id)),
    };

    let folders = all_folders
        .into_iter()
        .filter(|f| in_scope(Some(&f.id)))
        .map(|mut f| {
            if Some(f.id.as_str()) == root {
                f.parent_id = None;
            }
            f.extra.clear();
            f
        })
        .collect();

    let connections = store
        .list()
        .into_iter()
        .filter(|p| in_scope(p.folder_id.as_deref()))
        .map(|mut profile| {
            profile.extra.clear();
            // Доступ LLM — локальное решение получателя (спец. G1 §4): Off в файл не пишется.
            profile.llm_access = LlmAccess::Off;
            // Профили с диска не валидируются: секреты в дополнительных свойствах не выносим.
            profile
                .extra_properties
                .retain(|k, _| !is_secret_property(k));
            let needs_password = needs_password(secrets, &profile)?;
            Ok(ExportedConnection {
                profile,
                needs_password,
            })
        })
        .collect::<AppResult<Vec<_>>>()?;

    Ok(ExchangeFileOut {
        format: EXCHANGE_FORMAT,
        version: EXCHANGE_VERSION,
        exported_at: now.to_string(),
        app_version,
        folders,
        connections,
    })
}

/// Подключение файла, которое не будет импортировано, и причина.
#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Skipped {
    pub name: String,
    pub reason: LocalizedText,
}

/// Прочитанный файл обмена: папки, подключения (с флагами паролей) и пропущенные подключения.
#[derive(Debug, Clone, Default)]
pub struct ParsedExchange {
    pub folders: Vec<ConnectionFolder>,
    pub connections: Vec<(ConnectionProfile, NeedsPassword)>,
    pub skipped: Vec<Skipped>,
}

fn not_glafira_file() -> crate::error::AppError {
    app_err!(
        ErrorCode::InvalidInput,
        "errors.import.notGlafiraFile",
        "This is not a Glafira connections file"
    )
}

fn invalid_json(error: impl std::fmt::Display) -> crate::error::AppError {
    app_err!(
        ErrorCode::InvalidInput,
        "errors.import.invalidJson",
        "The file is damaged or is not JSON: {{error}}",
        error = error
    )
}

/// Имя подключения для списка пропущенных: из файла, иначе id, иначе номер в списке.
fn display_name(v: &serde_json::Value, index: usize) -> String {
    ["name", "id"]
        .iter()
        .filter_map(|k| v.get(*k).and_then(|x| x.as_str()))
        .map(str::trim)
        .find(|s| !s.is_empty())
        .map(String::from)
        .unwrap_or_else(|| format!("#{}", index + 1))
}

/// Читает файл обмена. Ошибка — только для чужого или повреждённого файла и более новой версии;
/// неизвестные поля игнорируются, неподдерживаемое или невалидное подключение попадает в `skipped`.
pub fn parse_exchange(bytes: &[u8]) -> AppResult<ParsedExchange> {
    let root: serde_json::Value = serde_json::from_slice(bytes).map_err(invalid_json)?;
    if root.get("format").and_then(|f| f.as_str()) != Some(EXCHANGE_FORMAT) {
        return Err(not_glafira_file());
    }
    let version = root
        .get("version")
        .and_then(|v| v.as_u64())
        .ok_or_else(not_glafira_file)?;
    if version > u64::from(EXCHANGE_VERSION) {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.import.incompatibleVersion",
            "The file was created by an incompatible version of Glafira — update the app"
        ));
    }
    let list = |key: &str| -> AppResult<Vec<serde_json::Value>> {
        match root.get(key) {
            None | Some(serde_json::Value::Null) => Ok(Vec::new()),
            Some(serde_json::Value::Array(items)) => Ok(items.clone()),
            Some(_) => Err(invalid_json(format!("“{key}” is not a list"))),
        }
    };

    let mut seen = HashSet::new();
    let mut folders = Vec::new();
    for v in list("folders")? {
        let mut f: ConnectionFolder = serde_json::from_value(v).map_err(invalid_json)?;
        // Папка без id пропускается (её подключения попадут в цель); из повторов id берётся первая.
        if f.id.trim().is_empty() || !seen.insert(f.id.clone()) {
            continue;
        }
        f.extra.clear();
        let name = f.name.trim();
        f.name = if name.is_empty() { f.id.trim() } else { name }.to_string();
        folders.push(f);
    }

    let mut seen = HashSet::new();
    let mut connections = Vec::new();
    let mut skipped = Vec::new();
    for (i, v) in list("connections")?.into_iter().enumerate() {
        let name = display_name(&v, i);
        let mut c: ExportedConnection = match serde_json::from_value(v) {
            Ok(c) => c,
            Err(e) => {
                skipped.push(Skipped {
                    name,
                    reason: ltext!(
                        "errors.import.unsupportedConnection",
                        "Not supported by this version of Glafira (update the app): {{error}}",
                        error = e
                    ),
                });
                continue;
            }
        };
        // Неизвестные поля файла не переносятся в хранилище.
        c.profile.extra.clear();
        if let Err(e) = validate_profile(&c.profile) {
            skipped.push(Skipped {
                name,
                reason: e.text(),
            });
            continue;
        }
        if !seen.insert(c.profile.id.clone()) {
            skipped.push(Skipped {
                name,
                reason: ltext!(
                    "errors.import.duplicateId",
                    "Duplicate connection id in the file"
                ),
            });
            continue;
        }
        connections.push((c.profile, c.needs_password));
    }
    Ok(ParsedExchange {
        folders,
        connections,
        skipped,
    })
}

pub struct ImportOptions<'a> {
    /// Папка, под которую попадают корневые элементы файла; `None` — корень.
    pub target: Option<&'a str>,
    pub update_existing: bool,
    pub is_pro: bool,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PreviewItem {
    pub kind: TreeItemKind,
    pub id: String,
    pub name: String,
    /// Имена папок от корня до места элемента (сам элемент не входит).
    pub path: Vec<String>,
}

#[derive(Serialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ImportPreview {
    pub added: Vec<PreviewItem>,
    pub updated: Vec<PreviewItem>,
    pub unchanged: Vec<PreviewItem>,
    pub skipped: Vec<Skipped>,
    pub warnings: Vec<LocalizedText>,
    pub needs_passwords: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ImportPlan {
    pub next: StoreData,
    pub preview: ImportPreview,
}

/// Имена папок от корня до `folder` включительно.
fn path_of(folders: &[ConnectionFolder], folder: Option<&str>) -> Vec<String> {
    let mut path = Vec::new();
    let mut seen = HashSet::new();
    let mut cur = folder;
    while let Some(id) = cur {
        if !seen.insert(id) {
            break;
        }
        let Some(f) = folders.iter().find(|f| f.id == id) else {
            break;
        };
        path.push(f.name.clone());
        cur = f.parent_id.as_deref();
    }
    path.reverse();
    path
}

/// Папки файла в порядке «родитель раньше потомка» (внутри уровня — по `order`).
/// Папка с родителем не из файла — корневая в файле; цикл разрывается на первой оставшейся папке.
fn parents_first(folders: &[ConnectionFolder]) -> Vec<(&ConnectionFolder, bool)> {
    let ids: HashSet<&str> = folders.iter().map(|f| f.id.as_str()).collect();
    let mut rest: Vec<&ConnectionFolder> = folders.iter().collect();
    rest.sort_by_key(|f| f.order);
    let mut done: HashSet<&str> = HashSet::new();
    let mut out = Vec::new();
    while !rest.is_empty() {
        let ready = |f: &&ConnectionFolder| match f.parent_id.as_deref() {
            None => true,
            Some(p) => !ids.contains(p) || done.contains(p),
        };
        let (mut now, later): (Vec<_>, Vec<_>) = rest.into_iter().partition(ready);
        let mut forced_root = None;
        if now.is_empty() {
            // Цикл в файле: первая оставшаяся папка становится корневой.
            forced_root = Some(later[0].id.as_str());
            now.push(later[0]);
        }
        rest = later
            .into_iter()
            .filter(|f| Some(f.id.as_str()) != forced_root)
            .collect();
        for f in now {
            let root = Some(f.id.as_str()) == forced_root
                || f.parent_id.as_deref().is_none_or(|p| !ids.contains(p));
            out.push((f, root));
            done.insert(f.id.as_str());
        }
    }
    out
}

fn tls_of(security: &SecurityConfig) -> Option<&crate::storage::connections::TlsConfig> {
    match security {
        SecurityConfig::Ssl { tls } | SecurityConfig::SaslSsl { tls, .. } => Some(tls),
        _ => None,
    }
}

/// Хотя бы один указанный файл сертификата отсутствует на этом компьютере.
fn misses_files(p: &ConnectionProfile, file_exists: &dyn Fn(&str) -> bool) -> bool {
    let Some(tls) = tls_of(&p.security) else {
        return false;
    };
    let cert = tls.client_cert.as_ref();
    [
        tls.ca_location.as_deref(),
        cert.map(|c| c.certificate_path.as_str()),
        cert.and_then(|c| c.key_path.as_deref()),
    ]
    .into_iter()
    .flatten()
    .map(str::trim)
    .filter(|s| !s.is_empty())
    .any(|path| !file_exists(path))
}

/// Настройки подключения без места в дереве и локальных неизвестных полей.
fn same_settings(a: &ConnectionProfile, b: &ConnectionProfile) -> bool {
    let strip = |p: &ConnectionProfile| ConnectionProfile {
        id: String::new(),
        folder_id: None,
        order: 0,
        llm_access: LlmAccess::Off,
        extra: Default::default(),
        ..p.clone()
    };
    strip(a) == strip(b)
}

/// Строит новое состояние хранилища и предпросмотр импорта. Функция чистая: лицензия и
/// проверка файлов сертификатов приходят снаружи.
pub(crate) fn plan_import(
    data: &StoreData,
    parsed: &ParsedExchange,
    opts: &ImportOptions,
    file_exists: &dyn Fn(&str) -> bool,
) -> AppResult<ImportPlan> {
    let target = opts.target.map(str::to_string);
    if let Some(t) = &target
        && !data.folders.iter().any(|f| &f.id == t)
    {
        return Err(folder_not_found(t));
    }
    let mut next = data.clone();
    let mut preview = ImportPreview {
        skipped: parsed.skipped.clone(),
        ..Default::default()
    };
    // id папок файла совпадают с id в хранилище: существующие берутся как есть, новые создаются с тем же id.
    let file_folders: HashSet<&str> = parsed.folders.iter().map(|f| f.id.as_str()).collect();
    let local_folders: HashSet<&str> = data.folders.iter().map(|f| f.id.as_str()).collect();
    // (вид, id) в порядке появления, чтобы пути в предпросмотре считались по итоговому дереву.
    let mut added = Vec::new();
    let mut updated = Vec::new();
    let mut unchanged = Vec::new();

    for (f, is_root) in parents_first(&parsed.folders) {
        if local_folders.contains(f.id.as_str()) {
            unchanged.push((TreeItemKind::Folder, f.id.clone()));
            continue;
        }
        let parent = if is_root {
            target.clone()
        } else {
            f.parent_id.clone()
        };
        let siblings: Vec<&str> = next
            .folders
            .iter()
            .filter(|s| s.parent_id == parent)
            .map(|s| s.name.as_str())
            .collect();
        let name = unique_name(&siblings, &f.name);
        let order = next
            .folders
            .iter()
            .filter(|s| s.parent_id == parent)
            .map(|s| s.order + 1)
            .max()
            .unwrap_or(0);
        next.folders.push(ConnectionFolder {
            id: f.id.clone(),
            name,
            parent_id: parent,
            order,
            extra: Default::default(),
        });
        added.push((TreeItemKind::Folder, f.id.clone()));
    }

    let mut connections: Vec<&(ConnectionProfile, NeedsPassword)> =
        parsed.connections.iter().collect();
    connections.sort_by_key(|(p, _)| p.order);
    let mut prod_read_only = 0usize;
    let mut missing_files = 0usize;
    for (incoming, needs) in connections {
        let mut profile = incoming.clone();
        let forced_read_only = !opts.is_pro && profile.environment == Environment::Prod;
        if forced_read_only {
            profile.read_only = true;
        }
        if let Some(slot) = next.connections.iter_mut().find(|c| c.id == profile.id) {
            // Во Free совпадение с файлом «как есть» — тоже «без изменений»: иначе локальный
            // prod без readOnly выглядел бы обновлённым при каждом импорте того же файла.
            let unchanged_settings = same_settings(slot, incoming) || same_settings(slot, &profile);
            if unchanged_settings || !opts.update_existing {
                unchanged.push((TreeItemKind::Connection, profile.id.clone()));
                continue;
            }
            // Место в дереве и неизвестные поля — локальные; пароли получателя не трогаются.
            profile.folder_id = slot.folder_id.clone();
            profile.order = slot.order;
            profile.llm_access = slot.llm_access;
            profile.extra = std::mem::take(&mut slot.extra);
            prod_read_only += usize::from(forced_read_only);
            missing_files += usize::from(misses_files(&profile, file_exists));
            updated.push((TreeItemKind::Connection, profile.id.clone()));
            *slot = profile;
            continue;
        }
        let folder = profile
            .folder_id
            .as_deref()
            .filter(|f| file_folders.contains(f))
            .map(str::to_string)
            .or_else(|| target.clone());
        let siblings: Vec<&str> = next
            .connections
            .iter()
            .filter(|c| c.folder_id == folder)
            .map(|c| c.name.as_str())
            .collect();
        let wanted = profile.name.trim();
        let taken = siblings
            .iter()
            .any(|s| s.trim().to_lowercase() == wanted.to_lowercase());
        let name = if taken {
            unique_name(&siblings, wanted)
        } else {
            profile.name.clone()
        };
        profile.order = next
            .connections
            .iter()
            .filter(|c| c.folder_id == folder)
            .map(|c| c.order + 1)
            .max()
            .unwrap_or(0);
        profile.folder_id = folder;
        profile.name = name;
        profile.llm_access = LlmAccess::Off;
        profile.extra = Default::default();
        prod_read_only += usize::from(forced_read_only);
        missing_files += usize::from(misses_files(&profile, file_exists));
        if needs.sasl || needs.truststore || needs.keystore || needs.key {
            preview.needs_passwords += 1;
        }
        added.push((TreeItemKind::Connection, profile.id.clone()));
        next.connections.push(profile);
    }

    if prod_read_only > 0 {
        preview.warnings.push(ltext!(
            "warnings.import.prodReadOnly",
            "{{count}} prod connections will be read-only: changing prod connections requires Pro",
            count = prod_read_only
        ));
    }
    if missing_files > 0 {
        preview.warnings.push(ltext!(
            "warnings.import.missingFiles",
            "Certificate files of {{count}} connections are not found on this computer — check the paths",
            count = missing_files
        ));
    }
    check_tree(&next)?;

    let folders_by_id: HashMap<&str, &ConnectionFolder> =
        next.folders.iter().map(|f| (f.id.as_str(), f)).collect();
    let item = |(kind, id): (TreeItemKind, String)| -> PreviewItem {
        let (name, location) = match kind {
            TreeItemKind::Folder => {
                let f = folders_by_id[id.as_str()];
                (f.name.clone(), f.parent_id.as_deref())
            }
            TreeItemKind::Connection => {
                let c = next
                    .connections
                    .iter()
                    .find(|c| c.id == id)
                    .expect("planned connection exists");
                (c.name.clone(), c.folder_id.as_deref())
            }
        };
        PreviewItem {
            kind,
            path: path_of(&next.folders, location),
            id,
            name,
        }
    };
    preview.added = added.into_iter().map(item).collect();
    preview.updated = updated.into_iter().map(item).collect();
    preview.unchanged = unchanged.into_iter().map(item).collect();
    Ok(ImportPlan { next, preview })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::{
        CertFormat, SaslConfig, SaslMechanism, SecurityConfig, TlsConfig, sample_profile,
    };

    fn store() -> (tempfile::TempDir, ConnectionStore) {
        let d = tempfile::tempdir().unwrap();
        let s = ConnectionStore::open(d.path()).unwrap();
        (d, s)
    }

    fn in_folder(id: &str, folder: Option<&str>) -> ConnectionProfile {
        let mut p = sample_profile(id);
        p.folder_id = folder.map(String::from);
        p
    }

    /// ИФТ/{kafka-1}, ПСИ; подключения: a в ИФТ, b в kafka-1, c в ПСИ, d в корне.
    fn tree(s: &ConnectionStore) -> (String, String, String) {
        let ift = s.create_folder("ИФТ", None).unwrap();
        let k1 = s.create_folder("kafka-1", Some(&ift.id)).unwrap();
        let psi = s.create_folder("ПСИ", None).unwrap();
        s.upsert(in_folder("a", Some(&ift.id))).unwrap();
        s.upsert(in_folder("b", Some(&k1.id))).unwrap();
        s.upsert(in_folder("c", Some(&psi.id))).unwrap();
        s.upsert(in_folder("d", None)).unwrap();
        (ift.id, k1.id, psi.id)
    }

    fn ids<T>(items: &[T], f: impl Fn(&T) -> &str) -> Vec<String> {
        let mut v: Vec<String> = items.iter().map(|i| f(i).to_string()).collect();
        v.sort();
        v
    }

    #[test]
    fn exports_whole_tree() {
        let (_d, s) = store();
        let (ift, k1, psi) = tree(&s);
        let out = build_export(
            &s,
            &MemoryStore::default(),
            None,
            "9.9.9",
            "2026-10-17T12:00:00Z",
        )
        .unwrap();
        assert_eq!(out.format, "glafira-connections");
        assert_eq!(out.version, 1);
        assert_eq!(out.app_version, "9.9.9");
        assert_eq!(out.exported_at, "2026-10-17T12:00:00Z");
        let mut want = vec![ift, k1, psi];
        want.sort();
        assert_eq!(ids(&out.folders, |f| &f.id), want);
        assert_eq!(
            ids(&out.connections, |c| &c.profile.id),
            ["a", "b", "c", "d"]
        );

        let v = serde_json::to_value(&out).unwrap();
        assert_eq!(v["format"], "glafira-connections");
        assert_eq!(v["version"], 1);
        assert!(v["exportedAt"].is_string());
        assert!(v["appVersion"].is_string());
        assert!(v["connections"][0]["needsPassword"]["sasl"].is_boolean());
        assert!(v["connections"][0]["bootstrapServers"].is_string());
    }

    #[test]
    fn exports_subtree_with_root_detached() {
        let (_d, s) = store();
        let (ift, k1, _psi) = tree(&s);
        let out = build_export(&s, &MemoryStore::default(), Some(&ift), "1", "now").unwrap();
        let mut want = vec![ift.clone(), k1.clone()];
        want.sort();
        assert_eq!(ids(&out.folders, |f| &f.id), want);
        let root = out.folders.iter().find(|f| f.id == ift).unwrap();
        assert_eq!(root.parent_id, None);
        let child = out.folders.iter().find(|f| f.id == k1).unwrap();
        assert_eq!(child.parent_id.as_deref(), Some(ift.as_str()));
        assert_eq!(ids(&out.connections, |c| &c.profile.id), ["a", "b"]);
        let v = serde_json::to_value(&out).unwrap();
        let root_json = v["folders"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["id"] == ift.as_str())
            .unwrap();
        assert!(root_json["parentId"].is_null());
    }

    #[test]
    fn secrets_are_never_exported() {
        let (_d, s) = store();
        let mut p = sample_profile("x");
        p.security = SecurityConfig::SaslPlaintext {
            sasl: SaslConfig {
                mechanism: SaslMechanism::Plain,
                username: "user".into(),
            },
        };
        s.upsert(p).unwrap();
        s.upsert(sample_profile("y")).unwrap();
        let secrets = MemoryStore::default();
        secrets
            .set(&secret_key("x", SASL_PASSWORD), "s3cr3t")
            .unwrap();
        // У PLAINTEXT-подключения устаревший пароль в keychain не делает флаг истинным.
        secrets
            .set(&secret_key("y", SASL_PASSWORD), "s3cr3t")
            .unwrap();
        let out = build_export(&s, &secrets, None, "1", "now").unwrap();
        let json = serde_json::to_string(&out).unwrap();
        assert!(!json.contains("s3cr3t"));
        let x = out
            .connections
            .iter()
            .find(|c| c.profile.id == "x")
            .unwrap();
        assert_eq!(
            x.needs_password,
            NeedsPassword {
                sasl: true,
                ..Default::default()
            }
        );
        let y = out
            .connections
            .iter()
            .find(|c| c.profile.id == "y")
            .unwrap();
        assert_eq!(y.needs_password, NeedsPassword::default());
    }

    #[test]
    fn unknown_local_fields_are_not_exported() {
        let (_d, s) = store();
        s.upsert(sample_profile("a")).unwrap();
        s.update(|data| {
            data.connections[0]
                .extra
                .insert("futureField".into(), serde_json::json!("local"));
            Ok(())
        })
        .unwrap();
        let out = build_export(&s, &MemoryStore::default(), None, "1", "now").unwrap();
        assert!(out.connections[0].profile.extra.is_empty());
        assert!(!serde_json::to_string(&out).unwrap().contains("futureField"));
    }

    #[test]
    fn secret_extra_properties_are_not_exported() {
        let (_d, s) = store();
        s.upsert(sample_profile("a")).unwrap();
        // Профили с диска не валидируются: старый или правленый вручную файл может содержать секреты.
        s.update(|data| {
            let props = &mut data.connections[0].extra_properties;
            props.insert(
                "sasl.jaas.config".into(),
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"s3cr3t\";".into(),
            );
            props.insert("ssl.key.password".into(), "s3cr3t".into());
            props.insert("linger.ms".into(), "5".into());
            Ok(())
        })
        .unwrap();
        let out = build_export(&s, &MemoryStore::default(), None, "1", "now").unwrap();
        let json = serde_json::to_string(&out).unwrap();
        assert!(!json.contains("s3cr3t"));
        assert!(!json.contains("sasl.jaas.config"));
        assert!(!json.contains("ssl.key.password"));
        assert_eq!(
            out.connections[0].profile.extra_properties.get("linger.ms"),
            Some(&"5".to_string())
        );
    }

    #[test]
    fn leftover_truststore_password_of_pem_ca_is_not_flagged() {
        let (_d, s) = store();
        let mut p = sample_profile("t");
        p.security = SecurityConfig::Ssl {
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
                verify_hostname: true,
                client_cert: None,
            },
        };
        s.upsert(p).unwrap();
        let secrets = MemoryStore::default();
        for f in [
            SSL_TRUSTSTORE_PASSWORD,
            SSL_KEYSTORE_PASSWORD,
            SSL_KEY_PASSWORD,
        ] {
            secrets.set(&secret_key("t", f), "old").unwrap();
        }
        let out = build_export(&s, &secrets, None, "1", "now").unwrap();
        assert_eq!(out.connections[0].needs_password, NeedsPassword::default());
    }

    #[test]
    fn unknown_root_is_an_error() {
        let (_d, s) = store();
        tree(&s);
        let err = build_export(&s, &MemoryStore::default(), Some("nope"), "1", "now")
            .err()
            .unwrap();
        assert_eq!(err.key, "errors.folders.notFound");
    }

    // ---- импорт ----

    fn conn_json(id: &str, name: &str, folder: Option<&str>) -> serde_json::Value {
        let mut p = sample_profile(id);
        p.name = name.into();
        p.folder_id = folder.map(String::from);
        let mut v = serde_json::to_value(&p).unwrap();
        v["needsPassword"] = serde_json::json!({"sasl": false, "truststore": false, "keystore": false, "key": false});
        v
    }

    fn folder_json(id: &str, name: &str, parent: Option<&str>, order: i64) -> serde_json::Value {
        serde_json::json!({"id": id, "name": name, "parentId": parent, "order": order})
    }

    fn file(folders: Vec<serde_json::Value>, conns: Vec<serde_json::Value>) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "format": "glafira-connections",
            "version": 1,
            "exportedAt": "now",
            "appVersion": "1",
            "folders": folders,
            "connections": conns,
        }))
        .unwrap()
    }

    fn local_folder(id: &str, name: &str, parent: Option<&str>) -> ConnectionFolder {
        ConnectionFolder {
            id: id.into(),
            name: name.into(),
            parent_id: parent.map(String::from),
            order: 0,
            extra: Default::default(),
        }
    }

    fn opts(target: Option<&str>) -> ImportOptions<'_> {
        ImportOptions {
            target,
            update_existing: true,
            is_pro: true,
        }
    }

    fn all_exist(_: &str) -> bool {
        true
    }

    fn plan(data: &StoreData, bytes: &[u8], o: &ImportOptions) -> ImportPlan {
        let parsed = parse_exchange(bytes).unwrap();
        plan_import(data, &parsed, o, &all_exist).unwrap()
    }

    fn conn<'a>(data: &'a StoreData, id: &str) -> &'a ConnectionProfile {
        data.connections.iter().find(|c| c.id == id).unwrap()
    }

    fn folder<'a>(data: &'a StoreData, id: &str) -> &'a ConnectionFolder {
        data.folders.iter().find(|f| f.id == id).unwrap()
    }

    fn preview_ids(items: &[PreviewItem]) -> Vec<&str> {
        items.iter().map(|i| i.id.as_str()).collect()
    }

    #[test]
    fn new_folders_and_connections_go_under_target_keeping_structure() {
        let data = StoreData {
            folders: vec![local_folder("t", "Target", None)],
            ..Default::default()
        };
        let bytes = file(
            vec![
                folder_json("k1", "kafka-1", Some("ift"), 0),
                folder_json("ift", "ИФТ", None, 0),
            ],
            vec![
                conn_json("a", "A", Some("ift")),
                conn_json("b", "B", Some("k1")),
                conn_json("c", "C", None),
            ],
        );
        let p = plan(&data, &bytes, &opts(Some("t")));
        let next = &p.next;
        assert_eq!(folder(next, "ift").parent_id.as_deref(), Some("t"));
        assert_eq!(folder(next, "ift").name, "ИФТ");
        assert_eq!(folder(next, "k1").parent_id.as_deref(), Some("ift"));
        assert_eq!(conn(next, "a").folder_id.as_deref(), Some("ift"));
        assert_eq!(conn(next, "b").folder_id.as_deref(), Some("k1"));
        assert_eq!(conn(next, "c").folder_id.as_deref(), Some("t"));
        assert_eq!(preview_ids(&p.preview.added), ["ift", "k1", "a", "b", "c"]);
        let b = p.preview.added.iter().find(|i| i.id == "b").unwrap();
        assert_eq!(b.kind, TreeItemKind::Connection);
        assert_eq!(b.path, ["Target", "ИФТ", "kafka-1"]);
        let k1 = p.preview.added.iter().find(|i| i.id == "k1").unwrap();
        assert_eq!(k1.kind, TreeItemKind::Folder);
        assert_eq!(k1.path, ["Target", "ИФТ"]);
        assert!(p.preview.updated.is_empty() && p.preview.unchanged.is_empty());
        check_tree(next).unwrap();
    }

    #[test]
    fn unknown_target_is_an_error() {
        let parsed = parse_exchange(&file(vec![], vec![])).unwrap();
        let err = plan_import(
            &StoreData::default(),
            &parsed,
            &opts(Some("nope")),
            &all_exist,
        )
        .err()
        .unwrap();
        assert_eq!(err.key, "errors.folders.notFound");
    }

    #[test]
    fn matched_connection_is_updated_only_with_update_existing() {
        let mut local = sample_profile("a");
        local.folder_id = Some("f".into());
        local.order = 7;
        local.extra.insert("pinned".into(), serde_json::json!(true));
        let data = StoreData {
            folders: vec![local_folder("f", "F", None)],
            connections: vec![local.clone()],
            ..Default::default()
        };
        let mut changed = conn_json("a", "conn-a", None);
        changed["bootstrapServers"] = "other:9092".into();
        let bytes = file(vec![], vec![changed]);

        let p = plan(&data, &bytes, &opts(None));
        let a = conn(&p.next, "a");
        assert_eq!(a.bootstrap_servers, "other:9092");
        assert_eq!(a.folder_id.as_deref(), Some("f"));
        assert_eq!(a.order, 7);
        assert_eq!(a.extra.get("pinned"), Some(&serde_json::json!(true)));
        assert_eq!(preview_ids(&p.preview.updated), ["a"]);
        assert_eq!(p.preview.updated[0].path, ["F"]);

        let p = plan(
            &data,
            &bytes,
            &ImportOptions {
                update_existing: false,
                ..opts(None)
            },
        );
        assert_eq!(conn(&p.next, "a"), &local);
        assert_eq!(preview_ids(&p.preview.unchanged), ["a"]);
        assert!(p.preview.updated.is_empty());

        // Те же настройки — «без изменений» и при update_existing.
        let same = file(vec![], vec![conn_json("a", "conn-a", None)]);
        let p = plan(&data, &same, &opts(None));
        assert_eq!(preview_ids(&p.preview.unchanged), ["a"]);
        assert_eq!(conn(&p.next, "a"), &local);
    }

    #[test]
    fn llm_access_is_never_exported_or_imported() {
        let (_d, s) = store();
        let mut open = sample_profile("a");
        open.llm_access = LlmAccess::Metadata;
        s.upsert(open).unwrap();
        let out = build_export(&s, &MemoryStore::default(), None, "1", "now").unwrap();
        let json = serde_json::to_value(&out).unwrap();
        assert!(json["connections"][0].get("llmAccess").is_none(), "{json}");

        let mut local = sample_profile("a");
        local.llm_access = LlmAccess::Metadata;
        let data = StoreData {
            connections: vec![local],
            ..Default::default()
        };
        let mut incoming_a = conn_json("a", "conn-a", None);
        incoming_a["llmAccess"] = "off".into();
        incoming_a["bootstrapServers"] = "other:9092".into();
        let mut incoming_b = conn_json("b", "conn-b", None);
        incoming_b["llmAccess"] = "metadata".into();
        let p = plan(
            &data,
            &file(vec![], vec![incoming_a, incoming_b]),
            &opts(None),
        );
        assert_eq!(
            conn(&p.next, "a").llm_access,
            LlmAccess::Metadata,
            "своё решение сохраняется"
        );
        assert_eq!(
            conn(&p.next, "b").llm_access,
            LlmAccess::Off,
            "файл не открывает доступ"
        );

        // Отличие только в llmAccess — «без изменений».
        let mut same = conn_json("a", "conn-a", None);
        same["llmAccess"] = "off".into();
        let p = plan(&data, &file(vec![], vec![same]), &opts(None));
        assert_eq!(preview_ids(&p.preview.unchanged), ["a"]);
    }

    #[test]
    fn matched_folder_is_not_renamed_or_moved_and_receives_new_connections() {
        let data = StoreData {
            folders: vec![
                local_folder("p", "Parent", None),
                local_folder("f", "Local name", Some("p")),
            ],
            ..Default::default()
        };
        let bytes = file(
            vec![folder_json("f", "Renamed", None, 0)],
            vec![conn_json("n", "New", Some("f"))],
        );
        let p = plan(&data, &bytes, &opts(None));
        let f = folder(&p.next, "f");
        assert_eq!(f.name, "Local name");
        assert_eq!(f.parent_id.as_deref(), Some("p"));
        assert_eq!(conn(&p.next, "n").folder_id.as_deref(), Some("f"));
        assert_eq!(preview_ids(&p.preview.unchanged), ["f"]);
        assert_eq!(preview_ids(&p.preview.added), ["n"]);
        assert_eq!(p.preview.added[0].path, ["Parent", "Local name"]);
    }

    #[test]
    fn name_conflicts_get_suffixes() {
        let mut x = sample_profile("x");
        x.name = "Kafka".into();
        let mut y = sample_profile("y");
        y.name = "kafka (2)".into();
        let data = StoreData {
            folders: vec![local_folder("l1", "ИФТ", None)],
            connections: vec![x, y],
            ..Default::default()
        };
        let bytes = file(
            vec![folder_json("n1", "ифт", None, 0)],
            vec![conn_json("n", "Kafka", None)],
        );
        let p = plan(&data, &bytes, &opts(None));
        assert_eq!(folder(&p.next, "n1").name, "ифт (2)");
        assert_eq!(conn(&p.next, "n").name, "Kafka (3)");
        assert_eq!(p.preview.added[1].name, "Kafka (3)");
    }

    #[test]
    fn foreign_local_connections_are_untouched() {
        let mut mine = sample_profile("mine");
        mine.order = 3;
        let data = StoreData {
            connections: vec![mine.clone()],
            ..Default::default()
        };
        let p = plan(
            &data,
            &file(vec![], vec![conn_json("a", "A", None)]),
            &opts(None),
        );
        assert_eq!(conn(&p.next, "mine"), &mine);
        assert_eq!(conn(&p.next, "a").order, 4);
    }

    #[test]
    fn prod_becomes_read_only_in_free() {
        let mut prod = conn_json("p", "P", None);
        prod["environment"] = "prod".into();
        let bytes = file(vec![], vec![prod, conn_json("d", "D", None)]);
        let free = ImportOptions {
            is_pro: false,
            ..opts(None)
        };
        let p = plan(&StoreData::default(), &bytes, &free);
        assert!(conn(&p.next, "p").read_only);
        assert!(!conn(&p.next, "d").read_only);
        assert_eq!(p.preview.warnings.len(), 1);
        assert_eq!(p.preview.warnings[0].key, "warnings.import.prodReadOnly");
        assert_eq!(p.preview.warnings[0].params["count"], "1");

        let p = plan(&StoreData::default(), &bytes, &opts(None));
        assert!(!conn(&p.next, "p").read_only);
        assert!(p.preview.warnings.is_empty());

        // Pro: readOnly из файла сохраняется как есть.
        let mut ro = conn_json("r", "R", None);
        ro["environment"] = "prod".into();
        ro["readOnly"] = true.into();
        let p = plan(&StoreData::default(), &file(vec![], vec![ro]), &opts(None));
        assert!(conn(&p.next, "r").read_only);
    }

    #[test]
    fn identical_prod_in_free_is_unchanged_and_keeps_read_only() {
        let mut local = sample_profile("p");
        local.environment = Environment::Prod;
        local.read_only = false;
        let data = StoreData {
            connections: vec![local.clone()],
            ..Default::default()
        };
        let mut same = conn_json("p", "conn-p", None);
        same["environment"] = "prod".into();
        let free = ImportOptions {
            is_pro: false,
            ..opts(None)
        };
        let p = plan(&data, &file(vec![], vec![same]), &free);
        assert_eq!(preview_ids(&p.preview.unchanged), ["p"]);
        assert!(p.preview.updated.is_empty());
        assert!(p.preview.warnings.is_empty());
        assert_eq!(conn(&p.next, "p"), &local);
    }

    #[test]
    fn duplicate_connection_ids_are_skipped_with_reason() {
        let bytes = file(
            vec![],
            vec![
                conn_json("a", "First", None),
                conn_json("a", "Second", None),
            ],
        );
        let parsed = parse_exchange(&bytes).unwrap();
        assert_eq!(parsed.connections.len(), 1);
        assert_eq!(parsed.connections[0].0.name, "First");
        assert_eq!(parsed.skipped.len(), 1);
        assert_eq!(parsed.skipped[0].name, "Second");
        assert_eq!(parsed.skipped[0].reason.key, "errors.import.duplicateId");
    }

    #[test]
    fn secrets_in_extra_properties_are_skipped() {
        let mut leaky = conn_json("s", "Leaky", None);
        leaky["extraProperties"] = serde_json::json!({"sasl.password": "x"});
        let bytes = file(vec![], vec![leaky, conn_json("ok", "OK", None)]);
        let parsed = parse_exchange(&bytes).unwrap();
        assert_eq!(parsed.skipped.len(), 1);
        assert_eq!(parsed.skipped[0].name, "Leaky");
        assert_eq!(
            parsed.skipped[0].reason.key,
            "errors.connections.secretInExtraProperties"
        );
        let p = plan_import(&StoreData::default(), &parsed, &opts(None), &all_exist).unwrap();
        assert!(p.next.connections.iter().all(|c| c.id != "s"));
        assert!(
            !serde_json::to_string(&p.next.connections)
                .unwrap()
                .contains("sasl.password")
        );
    }

    #[test]
    fn missing_certificate_files_are_warned_about() {
        let abs = |f: &str| {
            if cfg!(windows) {
                format!("C:\\certs\\{f}")
            } else {
                format!("/certs/{f}")
            }
        };
        let mut tls = conn_json("t", "T", None);
        tls["security"] = serde_json::json!({
            "protocol": "SSL",
            "tls": {"caLocation": abs("ca.pem"), "caFormat": "pem", "verifyHostname": true,
                    "clientCert": {"format": "pem", "certificatePath": abs("c.pem"), "keyPath": abs("k.pem")}}
        });
        let mut tls2 = tls.clone();
        tls2["id"] = "t2".into();
        let bytes = file(vec![], vec![tls, tls2, conn_json("plain", "Plain", None)]);
        let parsed = parse_exchange(&bytes).unwrap();
        let missing = |p: &str| !p.ends_with("k.pem");
        let p = plan_import(&StoreData::default(), &parsed, &opts(None), &missing).unwrap();
        assert_eq!(p.preview.warnings.len(), 1);
        assert_eq!(p.preview.warnings[0].key, "warnings.import.missingFiles");
        assert_eq!(p.preview.warnings[0].params["count"], "2");
        let p = plan_import(&StoreData::default(), &parsed, &opts(None), &all_exist).unwrap();
        assert!(p.preview.warnings.is_empty());

        // Обновлённое подключение тоже проверяется.
        let data = StoreData {
            connections: vec![sample_profile("t")],
            ..Default::default()
        };
        let only_t = file(
            vec![],
            vec![serde_json::to_value(&parsed.connections[0].0).unwrap()],
        );
        let parsed = parse_exchange(&only_t).unwrap();
        let p = plan_import(&data, &parsed, &opts(None), &missing).unwrap();
        assert_eq!(preview_ids(&p.preview.updated), ["t"]);
        assert_eq!(p.preview.warnings.len(), 1);
        assert_eq!(p.preview.warnings[0].key, "warnings.import.missingFiles");
        assert_eq!(p.preview.warnings[0].params["count"], "1");
    }

    #[test]
    fn needs_passwords_counts_new_connections_only() {
        let mut a = conn_json("a", "A", None);
        a["needsPassword"]["sasl"] = true.into();
        let mut b = conn_json("b", "B", None);
        b["needsPassword"]["key"] = true.into();
        b["bootstrapServers"] = "changed:1".into();
        let data = StoreData {
            connections: vec![sample_profile("b")],
            ..Default::default()
        };
        let p = plan(&data, &file(vec![], vec![a, b]), &opts(None));
        assert_eq!(preview_ids(&p.preview.updated), ["b"]);
        assert_eq!(p.preview.needs_passwords, 1);
    }

    #[test]
    fn parse_exchange_tolerates_unknown_fields_and_skips_unsupported() {
        let mut ok = conn_json("ok", "OK", None);
        ok["futureField"] = serde_json::json!({"x": 1});
        let mut oauth = conn_json("oauth", "OAuth", None);
        oauth["security"] = serde_json::json!({
            "protocol": "SASL_SSL",
            "tls": {"verifyHostname": true},
            "sasl": {"mechanism": "OAUTHBEARER", "username": "u"}
        });
        let mut invalid = conn_json("bad", "Bad", None);
        invalid["bootstrapServers"] = "nope".into();
        let mut v: serde_json::Value =
            serde_json::from_slice(&file(vec![], vec![ok, oauth, invalid])).unwrap();
        v["somethingNew"] = serde_json::json!([1, 2]);
        let parsed = parse_exchange(&serde_json::to_vec(&v).unwrap()).unwrap();
        assert_eq!(parsed.connections.len(), 1);
        assert_eq!(parsed.connections[0].0.id, "ok");
        assert!(parsed.connections[0].0.extra.is_empty());
        assert_eq!(parsed.skipped.len(), 2);
        assert_eq!(parsed.skipped[0].name, "OAuth");
        assert_eq!(
            parsed.skipped[0].reason.key,
            "errors.import.unsupportedConnection"
        );
        assert_eq!(parsed.skipped[1].name, "Bad");
        assert_eq!(
            parsed.skipped[1].reason.key,
            "errors.connections.addressFormat"
        );

        let p = plan_import(&StoreData::default(), &parsed, &opts(None), &all_exist).unwrap();
        assert_eq!(preview_ids(&p.preview.added), ["ok"]);
        assert_eq!(p.preview.skipped.len(), 2);

        v["version"] = 2.into();
        let err = parse_exchange(&serde_json::to_vec(&v).unwrap())
            .err()
            .unwrap();
        assert_eq!(err.key, "errors.import.incompatibleVersion");
        v["version"] = 1.into();
        v["format"] = "other".into();
        let err = parse_exchange(&serde_json::to_vec(&v).unwrap())
            .err()
            .unwrap();
        assert_eq!(err.key, "errors.import.notGlafiraFile");
        let err = parse_exchange(b"{not json").err().unwrap();
        assert_eq!(err.key, "errors.import.invalidJson");
        let err = parse_exchange(b"[1, 2]").err().unwrap();
        assert_eq!(err.key, "errors.import.notGlafiraFile");
    }

    #[test]
    fn export_then_import_into_empty_store_round_trips() {
        let (_d, s) = store();
        let (ift, k1, psi) = tree(&s);
        let mut x = sample_profile("x");
        x.security = SecurityConfig::SaslPlaintext {
            sasl: SaslConfig {
                mechanism: SaslMechanism::Plain,
                username: "user".into(),
            },
        };
        x.folder_id = Some(psi.clone());
        s.upsert(x).unwrap();
        let secrets = MemoryStore::default();
        secrets
            .set(&secret_key("x", SASL_PASSWORD), "s3cr3t")
            .unwrap();
        let out = build_export(&s, &secrets, None, "1", "now").unwrap();
        let bytes = serde_json::to_vec(&out).unwrap();

        let parsed = parse_exchange(&bytes).unwrap();
        assert!(parsed.skipped.is_empty());
        let p = plan_import(&StoreData::default(), &parsed, &opts(None), &all_exist).unwrap();
        assert_eq!(p.preview.needs_passwords, 1);
        let sort_f = |mut v: Vec<ConnectionFolder>| {
            v.sort_by(|a, b| a.id.cmp(&b.id));
            v
        };
        let sort_c = |mut v: Vec<ConnectionProfile>| {
            v.sort_by(|a, b| a.id.cmp(&b.id));
            v
        };
        assert_eq!(sort_f(p.next.folders.clone()), sort_f(s.list_folders()));
        assert_eq!(sort_c(p.next.connections.clone()), sort_c(s.list()));
        let _ = (ift, k1);
    }
}
