//! Файл обмена подключениями (`glafira-connections`): экспорт без секретов.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::error::AppResult;
use crate::kafka::config::{
    SASL_PASSWORD, SSL_KEY_PASSWORD, SSL_KEYSTORE_PASSWORD, SSL_TRUSTSTORE_PASSWORD, uses_secret,
};
use crate::secrets::{SecretStore, secret_key};
use crate::storage::connections::{
    ConnectionFolder, ConnectionProfile, ConnectionStore, folder_not_found, is_secret_property,
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
}
