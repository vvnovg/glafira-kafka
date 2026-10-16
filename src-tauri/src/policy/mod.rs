//! Корпоративная политика (спец. M3 §5): читается один раз при запуске, приоритетнее настроек пользователя.

pub mod sources;

use std::collections::BTreeMap;

use serde::Serialize;

pub const KEY_UPDATES_ENABLED: &str = "updatesEnabled";
pub const KEY_UPDATE_URL: &str = "updateUrl";
pub const KEY_LICENSE_KEY: &str = "licenseKey";

#[derive(Debug, Clone, PartialEq)]
pub enum PolicyValue {
    Bool(bool),
    Int(i64),
    Str(String),
    /// Значение неподдерживаемого типа (описание типа).
    Other(String),
}

impl PolicyValue {
    fn describe(&self) -> String {
        match self {
            PolicyValue::Bool(b) => format!("логическое {b}"),
            PolicyValue::Int(i) => format!("число {i}"),
            PolicyValue::Str(s) => format!("строка «{s}»"),
            PolicyValue::Other(t) => format!("значение типа {t}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RawPolicy {
    Absent,
    Values(BTreeMap<String, PolicyValue>),
    Unreadable(String),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyStatus {
    pub managed: bool,
    pub source: Option<String>,
    pub updates_enabled: bool,
    pub update_url: Option<String>,
    pub has_license_key: bool,
    pub errors: Vec<String>,
}

impl Default for PolicyStatus {
    fn default() -> Self {
        Self {
            managed: false,
            source: None,
            updates_enabled: true,
            update_url: None,
            has_license_key: false,
            errors: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Policy {
    pub status: PolicyStatus,
    /// Ключ организации; в интерфейс не отдаётся.
    pub license_key: Option<String>,
}

/// Политика, которую задал администратор, но которую не удалось понять, выключает обновления:
/// раз политика есть, администратор хотел ограничений.
pub fn resolve(source: &str, raw: RawPolicy) -> Policy {
    let map = match raw {
        RawPolicy::Absent => return Policy::default(),
        RawPolicy::Unreadable(e) => {
            return Policy {
                status: PolicyStatus {
                    managed: true,
                    source: Some(source.to_string()),
                    updates_enabled: false,
                    errors: vec![format!("Политика не читается: {e}")],
                    ..PolicyStatus::default()
                },
                license_key: None,
            };
        }
        RawPolicy::Values(map) => map,
    };
    let mut status = PolicyStatus {
        managed: true,
        source: Some(source.to_string()),
        ..PolicyStatus::default()
    };
    let mut license_key = None;
    let mut broken = false;
    for (key, value) in map {
        match (key.as_str(), value) {
            (KEY_UPDATES_ENABLED, PolicyValue::Bool(b)) => status.updates_enabled = b,
            (KEY_UPDATES_ENABLED, PolicyValue::Int(i @ (0 | 1))) => status.updates_enabled = i == 1,
            (KEY_UPDATE_URL, PolicyValue::Str(u))
                if u.starts_with("https://") || u.starts_with("http://") =>
            {
                status.update_url = Some(u)
            }
            (KEY_LICENSE_KEY, PolicyValue::Str(s)) if !s.trim().is_empty() => {
                license_key = Some(s.trim().to_string());
                status.has_license_key = true;
            }
            (KEY_UPDATES_ENABLED | KEY_UPDATE_URL | KEY_LICENSE_KEY, other) => {
                broken = true;
                status
                    .errors
                    .push(format!("{key}: недопустимое {}", other.describe()));
            }
            (unknown, _) => tracing::warn!("неизвестный ключ политики «{unknown}» проигнорирован"),
        }
    }
    if broken {
        status.updates_enabled = false;
    }
    Policy {
        status,
        license_key,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(pairs: &[(&str, PolicyValue)]) -> RawPolicy {
        RawPolicy::Values(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )
    }

    #[test]
    fn absent_policy_is_unmanaged_with_updates() {
        let p = resolve("/etc/x/policy.json", RawPolicy::Absent);
        assert!(!p.status.managed);
        assert!(p.status.updates_enabled);
        assert_eq!(p.license_key, None);
    }

    #[test]
    fn values_are_applied() {
        let p = resolve(
            "src",
            values(&[
                ("updatesEnabled", PolicyValue::Bool(false)),
                (
                    "updateUrl",
                    PolicyValue::Str("https://mirror.corp/latest.json".into()),
                ),
                ("licenseKey", PolicyValue::Str(" abc.def ".into())),
            ]),
        );
        assert!(p.status.managed);
        assert_eq!(p.status.source.as_deref(), Some("src"));
        assert!(!p.status.updates_enabled);
        assert_eq!(
            p.status.update_url.as_deref(),
            Some("https://mirror.corp/latest.json")
        );
        assert!(p.status.has_license_key);
        assert_eq!(p.license_key.as_deref(), Some("abc.def"));
        assert!(p.status.errors.is_empty());
    }

    #[test]
    fn registry_dword_is_bool() {
        let p = resolve("src", values(&[("updatesEnabled", PolicyValue::Int(0))]));
        assert!(!p.status.updates_enabled);
        let p = resolve("src", values(&[("updatesEnabled", PolicyValue::Int(1))]));
        assert!(p.status.updates_enabled);
    }

    #[test]
    fn broken_policy_disables_updates() {
        let p = resolve("src", RawPolicy::Unreadable("plist: unexpected EOF".into()));
        assert!(p.status.managed);
        assert!(!p.status.updates_enabled);
        assert!(p.status.errors[0].contains("unexpected EOF"));
        // Неверный тип или адрес — тоже «безопасно закрыто», даже если updatesEnabled = true.
        for bad in [
            values(&[("updatesEnabled", PolicyValue::Str("yes".into()))]),
            values(&[
                ("updatesEnabled", PolicyValue::Bool(true)),
                ("updateUrl", PolicyValue::Str("ftp://x".into())),
            ]),
        ] {
            let p = resolve("src", bad);
            assert!(!p.status.updates_enabled);
            assert_eq!(p.status.errors.len(), 1, "{:?}", p.status.errors);
        }
    }

    #[test]
    fn unknown_keys_are_ignored() {
        let p = resolve("src", values(&[("futureKey", PolicyValue::Bool(true))]));
        assert!(p.status.managed);
        assert!(p.status.updates_enabled);
        assert!(p.status.errors.is_empty());
    }

    #[test]
    fn license_key_is_not_serialized() {
        let p = resolve(
            "src",
            values(&[("licenseKey", PolicyValue::Str("secret.key".into()))]),
        );
        let json = serde_json::to_string(&p.status).unwrap();
        assert!(!json.contains("secret.key"));
        assert!(json.contains("\"hasLicenseKey\":true"));
    }
}
