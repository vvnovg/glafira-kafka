use std::path::{Path, PathBuf};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::error::{AppResult, ErrorCode};
use crate::kafka::producer::ProducerSettings;
use crate::produce::payload::ProduceMessage;
use crate::storage::fs::{read_json_or_quarantine, write_json};
use crate::sync::RwLockExt;

const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Template {
    #[serde(default)]
    pub id: String,
    pub name: String,
    /// None — глобальный шаблон (виден во всех подключениях).
    #[serde(default)]
    pub connection_id: Option<String>,
    pub message: ProduceMessage,
    #[serde(default)]
    pub settings: ProducerSettings,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TemplatesFile {
    schema_version: u32,
    templates: Vec<Template>,
}

pub struct TemplateStore {
    path: PathBuf,
    /// Err — файл не прочитан (более новая схема, ошибка ввода-вывода): каждая операция
    /// шаблонов возвращает эту ошибку, а остальное приложение работает (fail-closed по вызову).
    cache: RwLock<AppResult<Vec<Template>>>,
}

fn load(path: &Path) -> AppResult<Vec<Template>> {
    match read_json_or_quarantine::<TemplatesFile>(path, SCHEMA_VERSION)? {
        Some(f) if f.schema_version > SCHEMA_VERSION => Err(
            crate::storage::fs::newer_schema_error(path, f.schema_version),
        ),
        Some(f) => Ok(f.templates),
        None => Ok(Vec::new()),
    }
}

impl TemplateStore {
    /// Не падает: ошибка чтения файла сохраняется и возвращается операциями шаблонов,
    /// чтобы непрочитанный templates.json не мешал запуску приложения.
    pub fn open(data_dir: &Path) -> Self {
        let path = data_dir.join("templates.json");
        let cache = load(&path).map_err(|e| {
            tracing::warn!("templates are unavailable: {}", e.message);
            app_err!(
                ErrorCode::Internal,
                "errors.templates.unavailable",
                "Templates are unavailable: {{reason}}",
                reason = e.message
            )
        });
        Self {
            path,
            cache: RwLock::new(cache),
        }
    }

    pub fn list(&self, connection_id: &str) -> AppResult<Vec<Template>> {
        let cache = self.cache.read_safe();
        let mut out: Vec<Template> = cache
            .as_ref()
            .map_err(Clone::clone)?
            .iter()
            .filter(|t| {
                t.connection_id
                    .as_deref()
                    .is_none_or(|c| c == connection_id)
            })
            .cloned()
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    fn persist(&self, list: &[Template]) -> AppResult<()> {
        write_json(
            &self.path,
            &TemplatesFile {
                schema_version: SCHEMA_VERSION,
                templates: list.to_vec(),
            },
        )
    }

    pub fn save(&self, mut t: Template) -> AppResult<Template> {
        t.name = t.name.trim().to_string();
        if t.name.is_empty() {
            return Err(app_err!(
                ErrorCode::InvalidInput,
                "errors.templates.nameRequired",
                "Specify a template name"
            ));
        }
        if t.id.is_empty() {
            t.id = uuid::Uuid::new_v4().to_string();
        }
        let mut cache = self.cache.write_safe();
        let mut next = cache.as_ref().map_err(Clone::clone)?.clone();
        match next.iter_mut().find(|x| x.id == t.id) {
            Some(slot) => *slot = t.clone(),
            None => next.push(t.clone()),
        }
        self.persist(&next)?;
        *cache = Ok(next);
        Ok(t)
    }

    pub fn delete(&self, id: &str) -> AppResult<()> {
        let mut cache = self.cache.write_safe();
        let list = cache.as_ref().map_err(Clone::clone)?;
        if !list.iter().any(|t| t.id == id) {
            return Err(app_err!(
                ErrorCode::NotFound,
                "errors.templates.notFound",
                "Template not found"
            ));
        }
        let next: Vec<Template> = list.iter().filter(|t| t.id != id).cloned().collect();
        self.persist(&next)?;
        *cache = Ok(next);
        Ok(())
    }

    pub fn delete_for_connection(&self, connection_id: &str) -> AppResult<()> {
        let mut cache = self.cache.write_safe();
        let list = cache.as_ref().map_err(Clone::clone)?;
        let next: Vec<Template> = list
            .iter()
            .filter(|t| t.connection_id.as_deref() != Some(connection_id))
            .cloned()
            .collect();
        if next.len() != list.len() {
            self.persist(&next)?;
            *cache = Ok(next);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::produce::payload::{PayloadFormat, PayloadInput};

    fn template(name: &str, conn: Option<&str>) -> Template {
        Template {
            id: String::new(),
            name: name.into(),
            connection_id: conn.map(String::from),
            message: ProduceMessage {
                topic: "orders".into(),
                partition: None,
                key: PayloadInput {
                    format: PayloadFormat::String,
                    text: "{{uuid}}".into(),
                },
                value: PayloadInput {
                    format: PayloadFormat::Json,
                    text: "{\"n\":{{seq}}}".into(),
                },
                headers: vec![],
            },
            settings: ProducerSettings::default(),
        }
    }

    #[test]
    fn save_list_delete() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemplateStore::open(dir.path());
        let g = store.save(template("глобальный", None)).unwrap();
        assert!(!g.id.is_empty());
        let a = store.save(template("a", Some("c1"))).unwrap();
        store.save(template("b", Some("c2"))).unwrap();
        let reopened = TemplateStore::open(dir.path());
        let names: Vec<String> = reopened
            .list("c1")
            .unwrap()
            .into_iter()
            .map(|t| t.name)
            .collect();
        assert_eq!(names, ["a", "глобальный"]);
        let mut renamed = a.clone();
        renamed.name = "a2".into();
        reopened.save(renamed).unwrap();
        assert_eq!(
            reopened.list("c1").unwrap().len(),
            2,
            "сохранение с тем же id заменяет шаблон"
        );
        reopened.delete(&a.id).unwrap();
        assert_eq!(
            reopened.delete(&a.id).unwrap_err().code,
            ErrorCode::NotFound
        );
        reopened.delete_for_connection("c2").unwrap();
        assert_eq!(
            reopened.list("c2").unwrap().len(),
            1,
            "остался только глобальный"
        );
    }

    #[test]
    fn name_is_required() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemplateStore::open(dir.path());
        assert_eq!(
            store.save(template(" ", None)).unwrap_err().code,
            ErrorCode::InvalidInput
        );
    }

    #[test]
    fn corrupt_file_is_quarantined() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("templates.json"), "{ не json").unwrap();
        let store = TemplateStore::open(dir.path());
        assert!(store.list("c").unwrap().is_empty());
        let backups = std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("templates.json.corrupt-")
            })
            .count();
        assert_eq!(backups, 1);
        store.save(template("x", None)).unwrap();
    }

    #[test]
    fn newer_schema_version_fails_only_template_operations() {
        let dir = tempfile::tempdir().unwrap();
        let raw = r#"{"schemaVersion":99,"templates":[]}"#;
        let path = dir.path().join("templates.json");
        std::fs::write(&path, raw).unwrap();
        // Открытие не падает: приложение запускается, ошибку получают только операции шаблонов.
        let store = TemplateStore::open(dir.path());
        let err = store.list("c").unwrap_err();
        assert_eq!(err.key, "errors.templates.unavailable");
        assert!(
            err.params["reason"].contains("newer version"),
            "{}",
            err.message
        );
        assert!(store.save(template("x", None)).is_err());
        assert!(store.delete("x").is_err());
        assert!(store.delete_for_connection("c").is_err());
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            raw,
            "файл не перезаписан"
        );
    }

    #[test]
    fn unreadable_file_fails_only_template_operations() {
        let dir = tempfile::tempdir().unwrap();
        // На месте файла — каталог: прочитать нельзя.
        std::fs::create_dir(dir.path().join("templates.json")).unwrap();
        let store = TemplateStore::open(dir.path());
        assert!(store.list("c").is_err());
        assert!(store.save(template("x", None)).is_err());
    }
}
