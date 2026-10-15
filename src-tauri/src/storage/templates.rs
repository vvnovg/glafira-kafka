use std::path::{Path, PathBuf};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
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
    cache: RwLock<Vec<Template>>,
}

impl TemplateStore {
    pub fn open(data_dir: &Path) -> AppResult<Self> {
        let path = data_dir.join("templates.json");
        let templates = match read_json_or_quarantine::<TemplatesFile>(&path, SCHEMA_VERSION)? {
            Some(f) if f.schema_version > SCHEMA_VERSION => {
                return Err(AppError::internal(format!(
                    "templates.json создан более новой версией приложения (schemaVersion {})",
                    f.schema_version
                )));
            }
            Some(f) => f.templates,
            None => Vec::new(),
        };
        Ok(Self {
            path,
            cache: RwLock::new(templates),
        })
    }

    pub fn list(&self, connection_id: &str) -> Vec<Template> {
        let mut out: Vec<Template> = self
            .cache
            .read_safe()
            .iter()
            .filter(|t| {
                t.connection_id
                    .as_deref()
                    .is_none_or(|c| c == connection_id)
            })
            .cloned()
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
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
            return Err(AppError::invalid("Укажите имя шаблона"));
        }
        if t.id.is_empty() {
            t.id = uuid::Uuid::new_v4().to_string();
        }
        let mut cache = self.cache.write_safe();
        let mut next = cache.clone();
        match next.iter_mut().find(|x| x.id == t.id) {
            Some(slot) => *slot = t.clone(),
            None => next.push(t.clone()),
        }
        self.persist(&next)?;
        *cache = next;
        Ok(t)
    }

    pub fn delete(&self, id: &str) -> AppResult<()> {
        let mut cache = self.cache.write_safe();
        if !cache.iter().any(|t| t.id == id) {
            return Err(AppError::not_found("Шаблон не найден"));
        }
        let next: Vec<Template> = cache.iter().filter(|t| t.id != id).cloned().collect();
        self.persist(&next)?;
        *cache = next;
        Ok(())
    }

    pub fn delete_for_connection(&self, connection_id: &str) -> AppResult<()> {
        let mut cache = self.cache.write_safe();
        let next: Vec<Template> = cache
            .iter()
            .filter(|t| t.connection_id.as_deref() != Some(connection_id))
            .cloned()
            .collect();
        if next.len() != cache.len() {
            self.persist(&next)?;
            *cache = next;
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
        let store = TemplateStore::open(dir.path()).unwrap();
        let g = store.save(template("глобальный", None)).unwrap();
        assert!(!g.id.is_empty());
        let a = store.save(template("a", Some("c1"))).unwrap();
        store.save(template("b", Some("c2"))).unwrap();
        let reopened = TemplateStore::open(dir.path()).unwrap();
        let names: Vec<String> = reopened.list("c1").into_iter().map(|t| t.name).collect();
        assert_eq!(names, ["a", "глобальный"]);
        let mut renamed = a.clone();
        renamed.name = "a2".into();
        reopened.save(renamed).unwrap();
        assert_eq!(
            reopened.list("c1").len(),
            2,
            "сохранение с тем же id заменяет шаблон"
        );
        reopened.delete(&a.id).unwrap();
        assert_eq!(
            reopened.delete(&a.id).unwrap_err().code,
            ErrorCode::NotFound
        );
        reopened.delete_for_connection("c2").unwrap();
        assert_eq!(reopened.list("c2").len(), 1, "остался только глобальный");
    }

    #[test]
    fn name_is_required() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemplateStore::open(dir.path()).unwrap();
        assert_eq!(
            store.save(template(" ", None)).unwrap_err().code,
            ErrorCode::InvalidInput
        );
    }

    #[test]
    fn corrupt_file_is_quarantined() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("templates.json"), "{ не json").unwrap();
        let store = TemplateStore::open(dir.path()).unwrap();
        assert!(store.list("c").is_empty());
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
    fn newer_schema_version_refuses_to_open() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("templates.json"),
            r#"{"schemaVersion":99,"templates":[]}"#,
        )
        .unwrap();
        assert!(TemplateStore::open(dir.path()).is_err());
    }
}
