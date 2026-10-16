//! Лицензия Pro (спец. M3 §4): офлайн-ключ из политики организации или введённый пользователем.

use std::path::{Path, PathBuf};
use std::sync::RwLock;

use license_format::{Kind, NaiveDate, VerifyingKey};
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult, ErrorCode};
use crate::storage::fs::{read_json_or_quarantine, write_json};
use crate::sync::RwLockExt;

pub const EXPIRY_WARNING_DAYS: i64 = 14;
const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Free,
    Pro,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LicenseState {
    None,
    Valid,
    Expiring,
    Expired,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LicenseSource {
    Policy,
    User,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LicenseStatus {
    pub tier: Tier,
    pub state: LicenseState,
    pub source: Option<LicenseSource>,
    pub licensee: Option<String>,
    pub kind: Option<Kind>,
    pub expires: Option<NaiveDate>,
    pub days_left: Option<i64>,
    pub error: Option<String>,
}

impl LicenseStatus {
    fn free(state: LicenseState, source: Option<LicenseSource>) -> Self {
        Self {
            tier: Tier::Free,
            state,
            source,
            licensee: None,
            kind: None,
            expires: None,
            days_left: None,
            error: None,
        }
    }
}

pub fn evaluate(
    token: Option<&str>,
    source: Option<LicenseSource>,
    key: &VerifyingKey,
    today: NaiveDate,
) -> LicenseStatus {
    let Some(token) = token.map(str::trim).filter(|t| !t.is_empty()) else {
        return LicenseStatus::free(LicenseState::None, None);
    };
    match license_format::verify(token, key) {
        Err(e) => LicenseStatus {
            error: Some(e.to_string()),
            ..LicenseStatus::free(LicenseState::Invalid, source)
        },
        Ok(p) => {
            let days_left = p.expires.map(|d| (d - today).num_days());
            let state = match days_left {
                Some(d) if d < 0 => LicenseState::Expired,
                Some(d) if d < EXPIRY_WARNING_DAYS => LicenseState::Expiring,
                _ => LicenseState::Valid,
            };
            LicenseStatus {
                tier: if state == LicenseState::Expired {
                    Tier::Free
                } else {
                    Tier::Pro
                },
                state,
                source,
                licensee: Some(p.licensee),
                kind: Some(p.kind),
                expires: p.expires,
                days_left,
                error: None,
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LicenseFile {
    schema_version: u32,
    key: String,
}

pub struct LicenseManager {
    path: PathBuf,
    key: VerifyingKey,
    policy_key: RwLock<Option<String>>,
    user_key: RwLock<Option<String>>,
}

impl LicenseManager {
    /// Открытый ключ из `src-tauri/license-public.key`, вшитый при сборке.
    pub fn embedded_key() -> VerifyingKey {
        license_format::parse_verifying_key(include_str!("../../license-public.key"))
            .expect("license-public.key содержит неверный открытый ключ")
    }

    /// Повреждённый `license.json` не мешает запуску: он переименовывается, лицензия — Free.
    pub fn open(dir: &Path, key: VerifyingKey) -> Self {
        let path = dir.join("license.json");
        let user_key = match read_json_or_quarantine::<LicenseFile>(&path, SCHEMA_VERSION) {
            Ok(f) => f.map(|f| f.key),
            Err(e) => {
                tracing::warn!("лицензия не прочитана: {}", e.message);
                None
            }
        };
        Self {
            path,
            key,
            policy_key: RwLock::new(None),
            user_key: RwLock::new(user_key),
        }
    }

    pub fn set_policy_key(&self, key: Option<String>) {
        *self.policy_key.write_safe() = key;
    }

    pub fn status(&self) -> LicenseStatus {
        self.status_on(chrono::Local::now().date_naive())
    }

    pub fn status_on(&self, today: NaiveDate) -> LicenseStatus {
        if let Some(k) = self.policy_key.read_safe().as_deref() {
            return evaluate(Some(k), Some(LicenseSource::Policy), &self.key, today);
        }
        let user = self.user_key.read_safe();
        evaluate(
            user.as_deref(),
            user.as_ref().map(|_| LicenseSource::User),
            &self.key,
            today,
        )
    }

    fn ensure_not_managed(&self) -> AppResult<()> {
        if self.policy_key.read_safe().is_some() {
            return Err(AppError::invalid(
                "Лицензия управляется вашей организацией (корпоративная политика) — изменить её нельзя",
            ));
        }
        Ok(())
    }

    /// Блокирующий вызов (запись файла) — из blocking-контекста.
    pub fn set_user_key(&self, token: &str) -> AppResult<LicenseStatus> {
        self.ensure_not_managed()?;
        let st = evaluate(
            Some(token),
            Some(LicenseSource::User),
            &self.key,
            chrono::Local::now().date_naive(),
        );
        match st.state {
            LicenseState::Invalid | LicenseState::None => {
                return Err(AppError::invalid(format!(
                    "Ключ недействителен: {}",
                    st.error.as_deref().unwrap_or("пустой ключ")
                )));
            }
            LicenseState::Expired => {
                return Err(AppError::invalid(format!(
                    "Срок действия ключа истёк {}",
                    st.expires.map(|d| d.to_string()).unwrap_or_default()
                )));
            }
            LicenseState::Valid | LicenseState::Expiring => {}
        }
        let token = token.trim().to_string();
        write_json(
            &self.path,
            &LicenseFile {
                schema_version: SCHEMA_VERSION,
                key: token.clone(),
            },
        )?;
        *self.user_key.write_safe() = Some(token);
        Ok(self.status())
    }

    pub fn remove_user_key(&self) -> AppResult<LicenseStatus> {
        self.ensure_not_managed()?;
        match std::fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        *self.user_key.write_safe() = None;
        Ok(self.status())
    }

    /// Проверка в Rust: интерфейс только показывает замки.
    pub fn require_pro(&self) -> AppResult<()> {
        let st = self.status();
        if st.tier == Tier::Pro {
            return Ok(());
        }
        let message = match (st.state, st.expires) {
            (LicenseState::Expired, Some(d)) => {
                format!("Срок лицензии Pro истёк {d} — функция доступна в Pro")
            }
            _ => "Функция доступна в Pro — активируйте лицензию на экране «Лицензия»".to_string(),
        };
        Err(AppError::new(ErrorCode::ProRequired, message))
    }
}

/// Для тестов: менеджер с dev-ключом и бессрочной Pro-лицензией.
#[cfg(test)]
pub fn pro_for_tests(dir: &Path) -> LicenseManager {
    let m = LicenseManager::open(dir, license_format::dev::verifying_key());
    m.set_user_key(&license_format::dev::pro_token("test", None))
        .expect("dev-лицензия");
    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use license_format::dev;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    #[test]
    fn evaluate_states() {
        let key = dev::verifying_key();
        let today = d(2026, 10, 16);
        assert_eq!(evaluate(None, None, &key, today).tier, Tier::Free);
        assert_eq!(evaluate(None, None, &key, today).state, LicenseState::None);

        let forever = dev::pro_token("a@b", None);
        let s = evaluate(Some(&forever), Some(LicenseSource::User), &key, today);
        assert_eq!((s.tier, s.state), (Tier::Pro, LicenseState::Valid));
        assert_eq!(s.licensee.as_deref(), Some("a@b"));

        let soon = dev::pro_token("a@b", Some(d(2026, 10, 29)));
        let s = evaluate(Some(&soon), Some(LicenseSource::User), &key, today);
        assert_eq!(
            (s.tier, s.state, s.days_left),
            (Tier::Pro, LicenseState::Expiring, Some(13))
        );

        // Последний день действия включителен.
        let last_day = dev::pro_token("a@b", Some(today));
        assert_eq!(evaluate(Some(&last_day), None, &key, today).tier, Tier::Pro);

        let past = dev::pro_token("a@b", Some(d(2026, 10, 15)));
        let s = evaluate(Some(&past), None, &key, today);
        assert_eq!((s.tier, s.state), (Tier::Free, LicenseState::Expired));
        assert_eq!(s.expires, Some(d(2026, 10, 15)));

        let s = evaluate(Some("garbage"), None, &key, today);
        assert_eq!((s.tier, s.state), (Tier::Free, LicenseState::Invalid));
        assert!(s.error.unwrap().contains("повреждён"));
    }

    fn manager(dir: &std::path::Path) -> LicenseManager {
        LicenseManager::open(dir, dev::verifying_key())
    }

    #[test]
    fn user_key_is_persisted_and_removable() {
        let dir = tempfile::tempdir().unwrap();
        let m = manager(dir.path());
        assert_eq!(m.status().tier, Tier::Free);
        assert_eq!(m.require_pro().unwrap_err().code, ErrorCode::ProRequired);
        let st = m.set_user_key(&dev::pro_token("a@b", None)).unwrap();
        assert_eq!((st.tier, st.source), (Tier::Pro, Some(LicenseSource::User)));
        assert!(m.require_pro().is_ok());
        assert_eq!(
            manager(dir.path()).status().tier,
            Tier::Pro,
            "ключ сохранён на диск"
        );
        assert_eq!(m.remove_user_key().unwrap().tier, Tier::Free);
        assert_eq!(manager(dir.path()).status().tier, Tier::Free);
    }

    #[test]
    fn invalid_or_expired_keys_are_not_saved() {
        let dir = tempfile::tempdir().unwrap();
        let m = manager(dir.path());
        assert_eq!(
            m.set_user_key("garbage").unwrap_err().code,
            ErrorCode::InvalidInput
        );
        let past = dev::pro_token("a@b", Some(d(2020, 1, 1)));
        let e = m.set_user_key(&past).unwrap_err();
        assert!(e.message.contains("истёк"), "{}", e.message);
        assert!(!dir.path().join("license.json").exists());
    }

    #[test]
    fn policy_key_wins_and_locks_user_changes() {
        let dir = tempfile::tempdir().unwrap();
        let m = manager(dir.path());
        m.set_user_key(&dev::pro_token("user", None)).unwrap();
        m.set_policy_key(Some(dev::pro_token("ACME", None)));
        let st = m.status();
        assert_eq!(st.source, Some(LicenseSource::Policy));
        assert_eq!(st.licensee.as_deref(), Some("ACME"));
        assert_eq!(
            m.set_user_key(&dev::pro_token("x", None)).unwrap_err().code,
            ErrorCode::InvalidInput
        );
        assert_eq!(
            m.remove_user_key().unwrap_err().code,
            ErrorCode::InvalidInput
        );
    }

    #[test]
    fn expired_message_names_date() {
        let dir = tempfile::tempdir().unwrap();
        let m = manager(dir.path());
        m.set_policy_key(Some(dev::pro_token("ACME", Some(d(2020, 1, 1)))));
        let e = m.require_pro().unwrap_err();
        assert_eq!(e.code, ErrorCode::ProRequired);
        assert!(e.message.contains("2020-01-01"), "{}", e.message);
    }

    #[test]
    fn corrupt_license_file_does_not_block() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("license.json"), "{broken").unwrap();
        assert_eq!(manager(dir.path()).status().tier, Tier::Free);
    }

    #[test]
    fn embedded_key_parses() {
        let _ = LicenseManager::embedded_key();
    }

    #[test]
    fn status_json_shape() {
        let key = dev::verifying_key();
        let s = evaluate(
            Some(&dev::pro_token("a@b", Some(d(2026, 10, 20)))),
            Some(LicenseSource::User),
            &key,
            d(2026, 10, 16),
        );
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["tier"], "pro");
        assert_eq!(v["state"], "expiring");
        assert_eq!(v["source"], "user");
        assert_eq!(v["kind"], "personal");
        assert_eq!(v["daysLeft"], 4);
    }
}
