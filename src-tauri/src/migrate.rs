//! Однократный перенос данных и секретов со старого идентификатора приложения (переименование).
//! Ничего не удаляет: к старой версии можно вернуться. Значения секретов не пишутся в отчёт и лог.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::secrets::{CONNECTION_SECRET_FIELDS, KeyringStore, SecretStore, secret_key};
use crate::storage::connections::ConnectionStore;

#[derive(Debug, Default)]
pub struct MigrationReport {
    /// Каталог, из которого перенесены данные; `None` — переносить было нечего.
    pub from: Option<String>,
    pub files: Vec<String>,
    pub secrets: usize,
    pub errors: Vec<String>,
}

/// Признак установки: без него каталог не считается ни старой, ни новой установкой.
const MARKER: &str = "connections.json";
/// Файлы, кроме `MARKER`; он копируется последним (см. `migrate`).
const FILES: [&str; 2] = ["templates.json", "license.json"];

/// Каталоги данных прежних идентификаторов рядом с текущим (`<app data>/<identifier>`).
pub fn legacy_sources(data_dir_parent: &Path) -> Vec<(PathBuf, Arc<dyn SecretStore>)> {
    crate::product::LEGACY
        .iter()
        .filter(|(id, _)| *id != crate::product::IDENTIFIER)
        .map(|(id, service)| {
            let store: Arc<dyn SecretStore> = Arc::new(KeyringStore { service });
            (data_dir_parent.join(id), store)
        })
        .collect()
}

/// Переносит данные первого найденного старого каталога в `new_dir`, если в `new_dir` ещё нет
/// `connections.json`. Уже существующие в `new_dir` файлы и секреты не перезаписываются.
///
/// `connections.json` копируется последним: если перенос прервётся, следующий запуск повторит
/// его (уже скопированное пропускается), а не сочтёт выполненным.
pub fn migrate(
    new_dir: &Path,
    legacy: &[(PathBuf, Arc<dyn SecretStore>)],
    target: &dyn SecretStore,
) -> MigrationReport {
    let mut report = MigrationReport::default();
    if new_dir.join(MARKER).exists() {
        return report;
    }
    let Some((old_dir, old_secrets)) = legacy.iter().find(|(d, _)| d.join(MARKER).exists()) else {
        return report;
    };
    report.from = Some(old_dir.to_string_lossy().into_owned());
    if let Err(e) = std::fs::create_dir_all(new_dir) {
        report.errors.push(format!("{}: {e}", new_dir.display()));
        return report;
    }
    for name in FILES {
        copy_file(&old_dir.join(name), &new_dir.join(name), name, &mut report);
    }
    copy_dir(
        &old_dir.join("history"),
        &new_dir.join("history"),
        &mut report,
    );
    copy_secrets(old_dir, old_secrets.as_ref(), target, &mut report);
    copy_file(
        &old_dir.join(MARKER),
        &new_dir.join(MARKER),
        MARKER,
        &mut report,
    );
    report
}

fn copy_secrets(
    old_dir: &Path,
    old_secrets: &dyn SecretStore,
    target: &dyn SecretStore,
    report: &mut MigrationReport,
) {
    // Только чтение: `open` создаёт каталог, но он уже есть (в нём MARKER).
    let connections = match ConnectionStore::open(old_dir) {
        Ok(s) => s.list(),
        Err(e) => {
            report.errors.push(format!("{MARKER}: {}", e.message));
            return;
        }
    };
    for c in connections {
        for field in CONNECTION_SECRET_FIELDS {
            let key = secret_key(&c.id, field);
            let copy = || -> crate::error::AppResult<bool> {
                if target.get(&key)?.is_some() {
                    return Ok(false);
                }
                match old_secrets.get(&key)? {
                    Some(v) => target.set(&key, &v).map(|_| true),
                    None => Ok(false),
                }
            };
            match copy() {
                Ok(true) => report.secrets += 1,
                Ok(false) => {}
                // В ключе только id подключения и имя поля, значение не попадает в отчёт.
                Err(e) => report.errors.push(format!("secret {key}: {}", e.message)),
            }
        }
    }
}

/// Копия через временный файл и rename: в `dst` не бывает половины файла.
/// Отсутствующий `src` и уже существующий `dst` пропускаются. Права файла сохраняются.
fn copy_file(src: &Path, dst: &Path, label: &str, report: &mut MigrationReport) {
    if !src.is_file() || dst.exists() {
        return;
    }
    let mut tmp = dst.as_os_str().to_os_string();
    tmp.push(".migrate.tmp");
    let tmp = PathBuf::from(tmp);
    match std::fs::copy(src, &tmp).and_then(|_| std::fs::rename(&tmp, dst)) {
        Ok(()) => report.files.push(label.to_string()),
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            report.errors.push(format!("{label}: {e}"));
        }
    }
}

fn copy_dir(src: &Path, dst: &Path, report: &mut MigrationReport) {
    let Ok(entries) = std::fs::read_dir(src) else {
        return;
    };
    if let Err(e) = std::fs::create_dir_all(dst) {
        report.errors.push(format!("{}: {e}", dst.display()));
        return;
    }
    for entry in entries.flatten() {
        let name = entry.file_name();
        let label = format!("history/{}", name.to_string_lossy());
        copy_file(&entry.path(), &dst.join(&name), &label, report);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::config::SASL_PASSWORD;
    use crate::secrets::{MemoryStore, secret_key};
    use crate::storage::connections::{ConnectionStore, sample_profile};

    fn old_install(dir: &Path) -> Arc<dyn SecretStore> {
        let store = ConnectionStore::open(dir).unwrap();
        store.upsert(sample_profile("a")).unwrap();
        std::fs::create_dir_all(dir.join("history")).unwrap();
        std::fs::write(dir.join("history").join("a.json"), "{}").unwrap();
        std::fs::write(
            dir.join("templates.json"),
            r#"{"schemaVersion":1,"templates":[]}"#,
        )
        .unwrap();
        let secrets = Arc::new(MemoryStore::default());
        secrets.set(&secret_key("a", SASL_PASSWORD), "pw").unwrap();
        secrets
    }

    #[test]
    fn copies_data_and_secrets_once() {
        let root = tempfile::tempdir().unwrap();
        let old = root.path().join("old");
        let new = root.path().join("new");
        let old_secrets = old_install(&old);
        let target = MemoryStore::default();
        let r = migrate(&new, &[(old.clone(), old_secrets.clone())], &target);
        assert_eq!(r.from.as_deref(), Some(old.to_string_lossy().as_ref()));
        assert_eq!(r.secrets, 1);
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert!(new.join("connections.json").exists());
        assert!(new.join("templates.json").exists());
        assert!(new.join("history").join("a.json").exists());
        assert_eq!(
            target
                .get(&secret_key("a", SASL_PASSWORD))
                .unwrap()
                .as_deref(),
            Some("pw")
        );
        assert!(old.join("connections.json").exists(), "старое не удаляется");
        assert!(old.join("history").join("a.json").exists());
        assert_eq!(
            old_secrets
                .get(&secret_key("a", SASL_PASSWORD))
                .unwrap()
                .as_deref(),
            Some("pw"),
            "старый секрет не удаляется"
        );
        assert_eq!(
            ConnectionStore::open(&new).unwrap().list()[0].id,
            "a",
            "перенесённые подключения читаются"
        );

        // Повторный запуск ничего не делает: новые данные уже есть.
        let again = migrate(&new, &[(old, old_secrets)], &target);
        assert_eq!(again.from, None);
    }

    #[test]
    fn nothing_to_migrate() {
        let root = tempfile::tempdir().unwrap();
        let r = migrate(
            &root.path().join("new"),
            &[(root.path().join("absent"), Arc::new(MemoryStore::default()))],
            &MemoryStore::default(),
        );
        assert_eq!(r.from, None);
        assert!(
            !root.path().join("new").exists(),
            "каталог не создаётся зря"
        );
    }

    #[test]
    fn existing_new_secret_is_not_overwritten() {
        let root = tempfile::tempdir().unwrap();
        let old = root.path().join("old");
        let old_secrets = old_install(&old);
        let target = MemoryStore::default();
        target
            .set(&secret_key("a", SASL_PASSWORD), "newer")
            .unwrap();
        migrate(&root.path().join("new"), &[(old, old_secrets)], &target);
        assert_eq!(
            target
                .get(&secret_key("a", SASL_PASSWORD))
                .unwrap()
                .as_deref(),
            Some("newer")
        );
    }

    #[test]
    fn existing_new_file_is_not_overwritten() {
        // Новая версия уже запускалась без подключений, но с лицензией.
        let root = tempfile::tempdir().unwrap();
        let old = root.path().join("old");
        let new = root.path().join("new");
        let old_secrets = old_install(&old);
        std::fs::write(old.join("license.json"), "old").unwrap();
        std::fs::create_dir_all(&new).unwrap();
        std::fs::write(new.join("license.json"), "new").unwrap();
        let r = migrate(&new, &[(old, old_secrets)], &MemoryStore::default());
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(
            std::fs::read_to_string(new.join("license.json")).unwrap(),
            "new"
        );
        assert!(!r.files.iter().any(|f| f == "license.json"));
        assert!(new.join("connections.json").exists());
    }

    #[test]
    fn first_existing_legacy_wins() {
        let root = tempfile::tempdir().unwrap();
        let older = root.path().join("older");
        let older_secrets = old_install(&older);
        let r = migrate(
            &root.path().join("new"),
            &[
                (root.path().join("absent"), Arc::new(MemoryStore::default())),
                (older.clone(), older_secrets),
            ],
            &MemoryStore::default(),
        );
        assert_eq!(r.from.as_deref(), Some(older.to_string_lossy().as_ref()));
        assert_eq!(r.secrets, 1);
    }

    #[test]
    fn secret_errors_are_reported_without_values() {
        struct Failing;
        impl SecretStore for Failing {
            fn get(&self, _: &str) -> crate::error::AppResult<Option<String>> {
                Err(crate::error::AppError::internal_with("no access"))
            }
            fn set(&self, _: &str, _: &str) -> crate::error::AppResult<()> {
                unreachable!()
            }
            fn delete(&self, _: &str) -> crate::error::AppResult<()> {
                unreachable!()
            }
        }
        let root = tempfile::tempdir().unwrap();
        let old = root.path().join("old");
        old_install(&old);
        let new = root.path().join("new");
        let r = migrate(&new, &[(old, Arc::new(Failing))], &MemoryStore::default());
        assert_eq!(r.secrets, 0);
        assert_eq!(r.errors.len(), CONNECTION_SECRET_FIELDS.len());
        assert!(r.errors.iter().all(|e| !e.contains("pw")));
        assert!(new.join("connections.json").exists(), "данные переносятся");
    }

    #[test]
    fn legacy_sources_are_siblings_of_data_dir() {
        let parent = Path::new("/base");
        let sources = legacy_sources(parent);
        assert_eq!(sources.len(), crate::product::LEGACY.len());
        assert_eq!(sources[0].0, parent.join("dev.kafkaexplorer.app"));
        assert!(
            sources
                .iter()
                .all(|(d, _)| d != &parent.join(crate::product::IDENTIFIER))
        );
    }
}
