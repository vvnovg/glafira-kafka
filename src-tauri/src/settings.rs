//! Пользовательские настройки (пока — язык интерфейса) и выбор действующего языка.

use std::path::{Path, PathBuf};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::error::{AppResult, ErrorCode};
use crate::storage::fs::{read_json_or_quarantine, write_json};
use crate::sync::RwLockExt;

pub const LANGUAGES: [(&str, bool); 8] = [
    ("en", false),
    ("ru", false),
    ("de", true),
    ("fr", true),
    ("es", true),
    ("it", true),
    ("pl", true),
    ("pt-BR", true),
];

fn is_pro(code: &str) -> Option<bool> {
    LANGUAGES.iter().find(|(c, _)| *c == code).map(|(_, p)| *p)
}

pub fn match_language(tag: &str) -> Option<&'static str> {
    let primary = tag
        .split(['-', '_'])
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    if primary == "pt" {
        return Some("pt-BR");
    }
    LANGUAGES
        .iter()
        .map(|(c, _)| *c)
        .find(|c| c.eq_ignore_ascii_case(&primary))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum LanguageReason {
    Policy,
    User,
    System,
    Default,
    ProRequired,
}

/// Политика → выбор → ОС → en; Pro-язык без Pro → en (ProRequired).
pub fn resolve(
    system: Option<&str>,
    chosen: Option<&str>,
    policy: Option<&str>,
    pro: bool,
) -> (String, LanguageReason) {
    let candidate = policy
        .map(|p| (p.to_string(), LanguageReason::Policy))
        .or_else(|| chosen.map(|c| (c.to_string(), LanguageReason::User)))
        .or_else(|| {
            system
                .and_then(match_language)
                .map(|s| (s.to_string(), LanguageReason::System))
        });
    match candidate {
        Some((code, reason)) => match is_pro(&code) {
            Some(true) if !pro => ("en".into(), LanguageReason::ProRequired),
            Some(_) => (code, reason),
            None => ("en".into(), LanguageReason::Default),
        },
        None => ("en".into(), LanguageReason::Default),
    }
}

#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct SettingsFile {
    schema_version: u32,
    #[serde(default)]
    language: Option<String>,
}

pub struct SettingsStore {
    path: PathBuf,
    language: RwLock<Option<String>>,
}

impl SettingsStore {
    pub fn open(dir: &Path) -> Self {
        let path = dir.join("settings.json");
        let language = match read_json_or_quarantine::<SettingsFile>(&path, 1) {
            Ok(f) => f.and_then(|f| f.language).filter(|l| is_pro(l).is_some()),
            Err(e) => {
                tracing::warn!("settings not read: {}", e.message);
                None
            }
        };
        Self {
            path,
            language: RwLock::new(language),
        }
    }

    pub fn language(&self) -> Option<String> {
        self.language.read_safe().clone()
    }

    /// Блокирующий вызов (запись файла). Проверка Pro — в команде (нужна лицензия).
    pub fn set_language(&self, lang: Option<&str>) -> AppResult<()> {
        if let Some(l) = lang
            && is_pro(l).is_none()
        {
            return Err(crate::app_err!(
                ErrorCode::InvalidInput,
                "errors.settings.unknownLanguage",
                "Unknown language “{{code}}”",
                code = l
            ));
        }
        write_json(
            &self.path,
            &SettingsFile {
                schema_version: 1,
                language: lang.map(String::from),
            },
        )?;
        *self.language.write_safe() = lang.map(String::from);
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LanguageSettings {
    pub system: Option<String>,
    pub system_match: Option<String>,
    pub chosen: Option<String>,
    pub policy: Option<String>,
    pub effective: String,
    pub reason: LanguageReason,
    pub pro_languages: Vec<String>,
    pub languages: Vec<String>,
}

pub fn system_locale() -> Option<String> {
    sys_locale::get_locale()
}

pub fn language_settings(
    system: Option<String>,
    chosen: Option<String>,
    policy: Option<String>,
    pro: bool,
) -> LanguageSettings {
    let (effective, reason) = resolve(system.as_deref(), chosen.as_deref(), policy.as_deref(), pro);
    LanguageSettings {
        system_match: system.as_deref().and_then(match_language).map(String::from),
        system,
        chosen,
        policy,
        effective,
        reason,
        pro_languages: LANGUAGES
            .iter()
            .filter(|(_, p)| *p)
            .map(|(c, _)| c.to_string())
            .collect(),
        languages: LANGUAGES.iter().map(|(c, _)| c.to_string()).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_system_tags() {
        assert_eq!(match_language("de-AT"), Some("de"));
        assert_eq!(match_language("ru_RU"), Some("ru"));
        assert_eq!(match_language("pt-PT"), Some("pt-BR"));
        assert_eq!(match_language("pt"), Some("pt-BR"));
        assert_eq!(match_language("en-GB"), Some("en"));
        assert_eq!(match_language("uk-UA"), None);
    }

    #[test]
    fn priority_and_pro() {
        use LanguageReason::*;
        assert_eq!(
            resolve(Some("ru-RU"), None, None, false),
            ("ru".into(), System)
        );
        assert_eq!(
            resolve(Some("de-DE"), None, None, false),
            ("en".into(), ProRequired)
        );
        assert_eq!(
            resolve(Some("de-DE"), None, None, true),
            ("de".into(), System)
        );
        assert_eq!(
            resolve(Some("ru"), Some("en"), None, false),
            ("en".into(), User)
        );
        assert_eq!(
            resolve(Some("ru"), Some("fr"), None, false),
            ("en".into(), ProRequired)
        );
        assert_eq!(
            resolve(Some("ru"), Some("fr"), Some("ru"), true),
            ("ru".into(), Policy)
        );
        assert_eq!(
            resolve(Some("ru"), None, Some("de"), false),
            ("en".into(), ProRequired)
        );
        assert_eq!(
            resolve(Some("uk"), None, None, false),
            ("en".into(), Default)
        );
        assert_eq!(resolve(None, None, None, false), ("en".into(), Default));
    }

    #[test]
    fn store_persists_and_validates() {
        let dir = tempfile::tempdir().unwrap();
        let s = SettingsStore::open(dir.path());
        assert_eq!(s.language(), None);
        s.set_language(Some("ru")).unwrap();
        assert_eq!(
            SettingsStore::open(dir.path()).language().as_deref(),
            Some("ru")
        );
        assert_eq!(
            s.set_language(Some("xx")).unwrap_err().code,
            crate::error::ErrorCode::InvalidInput
        );
        s.set_language(None).unwrap();
        assert_eq!(SettingsStore::open(dir.path()).language(), None);
    }

    #[test]
    fn corrupt_settings_do_not_block() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("settings.json"), "{broken").unwrap();
        assert_eq!(SettingsStore::open(dir.path()).language(), None);
    }
}
