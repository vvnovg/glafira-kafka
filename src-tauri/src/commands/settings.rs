use tauri::State;

use super::blocking;
use crate::error::{AppResult, ErrorCode};
use crate::license::Tier;
use crate::settings::{LANGUAGES, LanguageSettings, language_settings, system_locale};
use crate::state::AppState;

fn current(st: &AppState) -> LanguageSettings {
    let pro = st.license.status().tier == Tier::Pro;
    language_settings(
        system_locale(),
        st.settings.language(),
        st.policy.status.language.clone(),
        pro,
    )
}

/// Блокирующая часть `set_language` (запись файла, проверка лицензии).
fn apply_language(st: &AppState, lang: Option<&str>) -> AppResult<LanguageSettings> {
    if st.policy.status.language.is_some() {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.settings.languageByPolicy",
            "The language is set by your organization"
        ));
    }
    if let Some(l) = lang
        && LANGUAGES.iter().any(|(c, p)| *c == l && *p)
    {
        st.license.require_pro()?;
    }
    st.settings.set_language(lang)?;
    Ok(current(st))
}

#[tauri::command]
pub fn get_language_settings(state: State<'_, AppState>) -> AppResult<LanguageSettings> {
    Ok(current(&state))
}

#[tauri::command]
pub async fn set_language(
    state: State<'_, AppState>,
    lang: Option<String>,
) -> AppResult<LanguageSettings> {
    let st = state.inner().clone();
    blocking(move || apply_language(&st, lang.as_deref())).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrets::MemoryStore;
    use crate::settings::LanguageReason;
    use std::sync::Arc;

    fn state(dir: &std::path::Path) -> AppState {
        AppState::new(dir, Arc::new(MemoryStore::default())).unwrap()
    }

    #[test]
    fn free_can_pick_free_language_only() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let r = apply_language(&st, Some("ru")).unwrap();
        assert_eq!(
            (r.effective.as_str(), r.reason),
            ("ru", LanguageReason::User)
        );
        assert_eq!(
            apply_language(&st, Some("de")).unwrap_err().code,
            ErrorCode::ProRequired
        );
        assert_eq!(st.settings.language().as_deref(), Some("ru"));
        let r = apply_language(&st, None).unwrap();
        assert_eq!(r.chosen, None);
    }

    #[test]
    fn pro_can_pick_pro_language() {
        let dir = tempfile::tempdir().unwrap();
        let mut st = state(dir.path());
        st.license = Arc::new(crate::license::pro_for_tests(dir.path()));
        let r = apply_language(&st, Some("de")).unwrap();
        assert_eq!(
            (r.effective.as_str(), r.reason),
            ("de", LanguageReason::User)
        );
    }

    #[test]
    fn policy_language_blocks_change() {
        let dir = tempfile::tempdir().unwrap();
        let mut st = state(dir.path());
        let mut policy = crate::policy::Policy::default();
        policy.status.language = Some("en".into());
        st = st.with_policy(policy);
        assert_eq!(
            apply_language(&st, Some("ru")).unwrap_err().code,
            ErrorCode::InvalidInput
        );
        assert_eq!(st.settings.language(), None);
        let s = current(&st);
        assert_eq!(
            (s.effective.as_str(), s.reason),
            ("en", LanguageReason::Policy)
        );
    }
}
