use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::kafka::producer::{ProduceResult, ProducerSettings};
use crate::produce::payload::ProduceMessage;
use crate::storage::fs::{read_json, write_json};
use crate::sync::MutexExt;

pub const HISTORY_LIMIT: usize = 100;
pub const MAX_HISTORY_TEXT: usize = 64 * 1024;
const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryEntry {
    pub id: String,
    pub sent_at: i64,
    pub message: ProduceMessage,
    pub settings: ProducerSettings,
    pub result: ProduceResult,
    /// Значения усечены до 64 КБ — повторить такую запись нельзя.
    pub truncated: bool,
}

impl HistoryEntry {
    pub fn new(message: ProduceMessage, settings: ProducerSettings, result: ProduceResult) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            sent_at: result.timestamp,
            message,
            settings,
            result,
            truncated: false,
        }
    }
}

fn cut(s: &mut String) -> bool {
    if s.len() <= MAX_HISTORY_TEXT {
        return false;
    }
    let mut at = MAX_HISTORY_TEXT;
    while !s.is_char_boundary(at) {
        at -= 1;
    }
    s.truncate(at);
    true
}

pub fn truncate_for_history(m: &mut ProduceMessage) -> bool {
    let mut any = cut(&mut m.key.text);
    any |= cut(&mut m.value.text);
    for h in &mut m.headers {
        any |= cut(&mut h.value);
    }
    any
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistoryFile {
    schema_version: u32,
    entries: Vec<HistoryEntry>,
}

/// История отправленных сообщений: по файлу на подключение (`history/<id>.json`).
pub struct HistoryStore {
    dir: PathBuf,
    // Сериализует read-modify-write файлов истории.
    lock: Mutex<()>,
}

impl HistoryStore {
    pub fn open(data_dir: &Path) -> AppResult<Self> {
        let dir = data_dir.join("history");
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            lock: Mutex::new(()),
        })
    }

    fn path(&self, connection_id: &str) -> AppResult<PathBuf> {
        let safe = !connection_id.is_empty()
            && connection_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
        if !safe {
            return Err(AppError::invalid("Некорректный id подключения"));
        }
        Ok(self.dir.join(format!("{connection_id}.json")))
    }

    fn read(&self, path: &Path) -> AppResult<Vec<HistoryEntry>> {
        match read_json::<HistoryFile>(path)? {
            Some(f) if f.schema_version > SCHEMA_VERSION => Err(AppError::internal(format!(
                "{} создан более новой версией приложения (schemaVersion {})",
                path.display(),
                f.schema_version
            ))),
            Some(f) => Ok(f.entries),
            None => Ok(Vec::new()),
        }
    }

    pub fn list(&self, connection_id: &str) -> AppResult<Vec<HistoryEntry>> {
        let path = self.path(connection_id)?;
        let _g = self.lock.lock_safe();
        self.read(&path)
    }

    pub fn append(&self, connection_id: &str, mut entry: HistoryEntry) -> AppResult<()> {
        let path = self.path(connection_id)?;
        entry.truncated = truncate_for_history(&mut entry.message);
        let _g = self.lock.lock_safe();
        // Новая schemaVersion → read вернёт ошибку, файл не перезаписывается.
        let mut entries = self.read(&path)?;
        entries.insert(0, entry);
        entries.truncate(HISTORY_LIMIT);
        write_json(
            &path,
            &HistoryFile {
                schema_version: SCHEMA_VERSION,
                entries,
            },
        )
    }

    pub fn delete_connection(&self, connection_id: &str) -> AppResult<()> {
        let path = self.path(connection_id)?;
        let _g = self.lock.lock_safe();
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::kafka::producer::{ProduceResult, ProducerSettings};
    use crate::produce::payload::{PayloadFormat, PayloadInput};

    fn entry(n: i64, value: &str) -> HistoryEntry {
        HistoryEntry::new(
            ProduceMessage {
                topic: "t".into(),
                partition: None,
                key: PayloadInput {
                    format: PayloadFormat::Null,
                    text: String::new(),
                },
                value: PayloadInput {
                    format: PayloadFormat::String,
                    text: value.into(),
                },
                headers: vec![],
            },
            ProducerSettings::default(),
            ProduceResult {
                partition: 0,
                offset: Some(n),
                timestamp: n,
            },
        )
    }

    #[test]
    fn keeps_last_100_newest_first_per_connection() {
        let dir = tempfile::tempdir().unwrap();
        let store = HistoryStore::open(dir.path()).unwrap();
        for n in 0..105 {
            store.append("c1", entry(n, "v")).unwrap();
        }
        store.append("c2", entry(999, "v")).unwrap();
        let reopened = HistoryStore::open(dir.path()).unwrap();
        let list = reopened.list("c1").unwrap();
        assert_eq!(list.len(), HISTORY_LIMIT);
        assert_eq!(list[0].result.offset, Some(104));
        assert_eq!(list[99].result.offset, Some(5));
        assert_eq!(reopened.list("c2").unwrap().len(), 1);
        assert!(reopened.list("none").unwrap().is_empty());
        reopened.delete_connection("c1").unwrap();
        assert!(reopened.list("c1").unwrap().is_empty());
    }

    #[test]
    fn large_values_are_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let store = HistoryStore::open(dir.path()).unwrap();
        store
            .append("c", entry(1, &"я".repeat(MAX_HISTORY_TEXT)))
            .unwrap();
        let e = &store.list("c").unwrap()[0];
        assert!(e.truncated);
        assert!(e.message.value.text.len() <= MAX_HISTORY_TEXT);
        store.append("c", entry(2, "short")).unwrap();
        assert!(!store.list("c").unwrap()[0].truncated);
    }

    #[test]
    fn rejects_unsafe_connection_ids() {
        let dir = tempfile::tempdir().unwrap();
        let store = HistoryStore::open(dir.path()).unwrap();
        assert_eq!(
            store.list("../x").unwrap_err().code,
            ErrorCode::InvalidInput
        );
    }

    #[test]
    fn concurrent_appends_do_not_lose_entries() {
        let dir = tempfile::tempdir().unwrap();
        let store = HistoryStore::open(dir.path()).unwrap();
        std::thread::scope(|s| {
            for t in 0..4 {
                let store = &store;
                s.spawn(move || {
                    for n in 0..10 {
                        store.append("c", entry(t * 100 + n, "v")).unwrap();
                    }
                });
            }
        });
        assert_eq!(store.list("c").unwrap().len(), 40);
    }

    #[test]
    fn newer_schema_version_is_not_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let store = HistoryStore::open(dir.path()).unwrap();
        let p = dir.path().join("history").join("c.json");
        let raw = r#"{"schemaVersion":99,"entries":[]}"#;
        std::fs::write(&p, raw).unwrap();
        assert!(store.append("c", entry(1, "v")).is_err());
        assert_eq!(std::fs::read_to_string(&p).unwrap(), raw);
    }

    #[test]
    fn rejects_unsafe_ids_everywhere() {
        let dir = tempfile::tempdir().unwrap();
        let store = HistoryStore::open(dir.path()).unwrap();
        for bad in ["", "a/b", "..", "a.b", "a\\b"] {
            assert!(store.list(bad).is_err(), "{bad}");
            assert!(store.append(bad, entry(1, "v")).is_err());
            assert!(store.delete_connection(bad).is_err());
        }
    }
}
