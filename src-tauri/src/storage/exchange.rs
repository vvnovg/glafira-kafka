//! Файл обмена подключениями (`glafira-connections`): экспорт без секретов.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::error::AppResult;
use crate::kafka::config::{
    SASL_PASSWORD, SSL_KEY_PASSWORD, SSL_KEYSTORE_PASSWORD, SSL_TRUSTSTORE_PASSWORD,
};
use crate::secrets::{SecretStore, secret_key};
use crate::storage::connections::{
    ConnectionFolder, ConnectionProfile, ConnectionStore, SecurityConfig, folder_not_found,
};

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

fn has_secret(secrets: &dyn SecretStore, id: &str, field: &str) -> AppResult<bool> {
    Ok(secrets
        .get(&secret_key(id, field))?
        .is_some_and(|v| !v.is_empty()))
}

/// Флаги учитывают только пароли, которые профиль использует: устаревшие записи keychain
/// (например, SASL-пароль после перехода на SSL) не требуют ввода у получателя.
fn needs_password(secrets: &dyn SecretStore, p: &ConnectionProfile) -> AppResult<NeedsPassword> {
    let uses_tls = matches!(
        p.security,
        SecurityConfig::Ssl { .. } | SecurityConfig::SaslSsl { .. }
    );
    let check = |used: bool, field: &str| -> AppResult<bool> {
        if used {
            has_secret(secrets, &p.id, field)
        } else {
            Ok(false)
        }
    };
    Ok(NeedsPassword {
        sasl: check(p.security.uses_sasl(), SASL_PASSWORD)?,
        truststore: check(uses_tls, SSL_TRUSTSTORE_PASSWORD)?,
        keystore: check(uses_tls, SSL_KEYSTORE_PASSWORD)?,
        key: check(uses_tls, SSL_KEY_PASSWORD)?,
    })
}

/// Собирает файл экспорта: всё дерево или поддерево `root` (сама папка становится корнем).
/// Секреты не попадают в файл; неизвестные локальные поля (`extra`) не выносятся.
pub fn build_export(
    store: &ConnectionStore,
    secrets: &dyn SecretStore,
    root: Option<&str>,
    app_version: &str,
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
        app_version: leak_version(app_version),
        folders,
        connections,
    })
}

/// `app_version` в рабочем коде — `env!("CARGO_PKG_VERSION")`; для него утечки нет.
fn leak_version(v: &str) -> &'static str {
    if v == env!("CARGO_PKG_VERSION") {
        env!("CARGO_PKG_VERSION")
    } else {
        Box::leak(v.to_string().into_boxed_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::{SaslConfig, SaslMechanism, sample_profile};

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
    fn unknown_root_is_an_error() {
        let (_d, s) = store();
        tree(&s);
        let err = build_export(&s, &MemoryStore::default(), Some("nope"), "1", "now")
            .err()
            .unwrap();
        assert_eq!(err.key, "errors.folders.notFound");
    }
}
