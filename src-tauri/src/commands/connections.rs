use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;
use tauri::State;

use super::blocking;
use crate::error::AppResult;
use crate::kafka::config::{
    SASL_PASSWORD, SSL_KEY_PASSWORD, SSL_KEYSTORE_PASSWORD, SSL_TRUSTSTORE_PASSWORD,
};
use crate::kafka::connectivity::{self, TestReport};
use crate::secrets::{OverlayStore, SecretStore, secret_key};
use crate::state::AppState;
use crate::storage::connections::{ConnectionProfile, Environment};

/// Пароли TLS из формы. Для каждого: `null`/нет — оставить сохранённый, `""` — удалить, иное — сохранить.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsPasswordsInput {
    #[serde(default)]
    pub key_password: Option<String>,
    #[serde(default)]
    pub keystore_password: Option<String>,
    #[serde(default)]
    pub truststore_password: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInput {
    pub profile: ConnectionProfile,
    /// Пустой или `null` — оставить сохранённый пароль SASL.
    #[serde(default)]
    pub sasl_password: Option<String>,
    #[serde(default)]
    pub tls_passwords: Option<TlsPasswordsInput>,
}

/// Что делать с секретом при сохранении.
enum SecretOp {
    Keep,
    Set(String),
    Delete,
}

/// Секреты профиля: используемые поля — по данным формы, неиспользуемые — удаляются.
fn secret_ops(
    profile: &ConnectionProfile,
    sasl_password: Option<&str>,
    pw: Option<&TlsPasswordsInput>,
) -> Vec<(&'static str, SecretOp)> {
    use crate::storage::connections::{CertFormat, SecurityConfig};
    let tls = match &profile.security {
        SecurityConfig::Ssl { tls } | SecurityConfig::SaslSsl { tls, .. } => Some(tls),
        _ => None,
    };
    let tls_op = |used: bool, v: Option<&String>| match (used, v) {
        (false, _) => SecretOp::Delete,
        (true, None) => SecretOp::Keep,
        (true, Some(s)) if s.is_empty() => SecretOp::Delete,
        (true, Some(s)) => SecretOp::Set(s.clone()),
    };
    let client = tls.and_then(|t| t.client_cert.as_ref()).map(|c| c.format);
    let ca_store = tls.is_some_and(|t| {
        t.ca_format != CertFormat::Pem
            && t.ca_location
                .as_deref()
                .is_some_and(|s| !s.trim().is_empty())
    });
    vec![
        (
            SASL_PASSWORD,
            if !profile.security.uses_sasl() {
                SecretOp::Delete
            } else {
                match sasl_password.filter(|p| !p.is_empty()) {
                    Some(p) => SecretOp::Set(p.to_string()),
                    None => SecretOp::Keep,
                }
            },
        ),
        (
            SSL_KEY_PASSWORD,
            tls_op(
                matches!(client, Some(CertFormat::Pem | CertFormat::Jks)),
                pw.and_then(|p| p.key_password.as_ref()),
            ),
        ),
        (
            SSL_KEYSTORE_PASSWORD,
            tls_op(
                matches!(client, Some(CertFormat::Pkcs12 | CertFormat::Jks)),
                pw.and_then(|p| p.keystore_password.as_ref()),
            ),
        ),
        (
            SSL_TRUSTSTORE_PASSWORD,
            tls_op(ca_store, pw.and_then(|p| p.truststore_password.as_ref())),
        ),
    ]
}

#[tauri::command]
pub fn list_connections(state: State<'_, AppState>) -> AppResult<Vec<ConnectionProfile>> {
    Ok(state.connections.list())
}

#[tauri::command]
pub async fn save_connection(
    state: State<'_, AppState>,
    input: ConnectionInput,
) -> AppResult<ConnectionProfile> {
    let st = state.inner().clone();
    blocking(move || save_connection_blocking(&st, input)).await
}

/// Сохраняет профиль и секрет согласованно: если keychain не принял изменение,
/// профиль возвращается к прежнему состоянию (или удаляется, если он новый).
pub fn save_connection_blocking(
    st: &AppState,
    input: ConnectionInput,
) -> AppResult<ConnectionProfile> {
    let ConnectionInput {
        mut profile,
        sasl_password,
        tls_passwords,
    } = input;
    if profile.id.is_empty() {
        profile.id = uuid::Uuid::new_v4().to_string();
    }
    let previous = st.connections.get(&profile.id).ok();
    // Перевод в prod — функция Pro; уже существующие prod-профили остаются редактируемыми (и защищёнными guard).
    let becomes_prod = profile.environment == Environment::Prod
        && previous.as_ref().map(|p| p.environment) != Some(Environment::Prod);
    if becomes_prod {
        st.license.require_pro()?;
    }
    // upsert валидирует профиль — секрет трогаем только после успешного сохранения.
    let saved = st.connections.upsert(profile)?;
    let ops = secret_ops(&saved, sasl_password.as_deref(), tls_passwords.as_ref());
    let secret_result = ops.into_iter().try_for_each(|(field, op)| {
        let key = secret_key(&saved.id, field);
        match op {
            SecretOp::Keep => Ok(()),
            SecretOp::Set(v) => st.secrets.set(&key, &v),
            SecretOp::Delete => st.secrets.delete(&key),
        }
    });
    if let Err(e) = secret_result {
        let rollback = match previous {
            Some(p) => st.connections.upsert(p).map(|_| ()),
            None => st.connections.delete(&saved.id),
        };
        if let Err(r) = rollback {
            tracing::error!("failed to roll back profile {}: {r}", saved.id);
        }
        return Err(e);
    }
    // Живые сессии (browse, consumer, пакет) работают со старым профилем: адресами, учётными
    // данными, флагом read-only. Останавливаем их — новые запустятся уже с новым профилем.
    st.sessions.cancel_for(&saved.id);
    st.evict_connection(&saved.id);
    Ok(saved)
}

#[tauri::command]
pub async fn delete_connection(state: State<'_, AppState>, id: String) -> AppResult<()> {
    let st = state.inner().clone();
    blocking(move || st.delete_connection_full(&id)).await
}

#[tauri::command]
pub async fn disconnect(state: State<'_, AppState>, connection_id: String) -> AppResult<()> {
    let st = state.inner().clone();
    blocking(move || {
        st.evict_connection(&connection_id);
        Ok(())
    })
    .await
}

/// Проверяет и несохранённый профиль: пароли из формы накладываются поверх keychain
/// (с той же семантикой, что и при сохранении).
#[tauri::command]
pub async fn test_connection(
    state: State<'_, AppState>,
    input: ConnectionInput,
) -> AppResult<TestReport> {
    let id = input.profile.id.clone();
    let mut overrides = HashMap::new();
    if let Some(pw) = input.sasl_password.as_ref().filter(|p| !p.is_empty()) {
        overrides.insert(secret_key(&id, SASL_PASSWORD), pw.clone());
    }
    if let Some(t) = &input.tls_passwords {
        for (field, v) in [
            (SSL_KEY_PASSWORD, &t.key_password),
            (SSL_KEYSTORE_PASSWORD, &t.keystore_password),
            (SSL_TRUSTSTORE_PASSWORD, &t.truststore_password),
        ] {
            if let Some(v) = v {
                overrides.insert(secret_key(&id, field), v.clone());
            }
        }
    }
    let secrets: Arc<dyn SecretStore> = Arc::new(OverlayStore {
        base: state.secrets.clone(),
        overrides,
    });
    let profile = input.profile;
    blocking(move || Ok(connectivity::test_connection(&profile, secrets.as_ref()))).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{AppError, ErrorCode};
    use crate::secrets::MemoryStore;
    use crate::storage::connections::{
        CertFormat, ClientCert, SaslConfig, SaslMechanism, SecurityConfig, TlsConfig,
        sample_profile,
    };

    /// Keychain, в который ничего нельзя записать.
    struct BrokenKeychain;

    impl SecretStore for BrokenKeychain {
        fn get(&self, _: &str) -> AppResult<Option<String>> {
            Ok(None)
        }
        fn set(&self, _: &str, _: &str) -> AppResult<()> {
            Err(AppError::internal_with("keychain unavailable"))
        }
        fn delete(&self, _: &str) -> AppResult<()> {
            Err(AppError::internal_with("keychain unavailable"))
        }
    }

    fn sasl(mut p: ConnectionProfile) -> ConnectionProfile {
        p.security = SecurityConfig::SaslPlaintext {
            sasl: SaslConfig {
                mechanism: SaslMechanism::Plain,
                username: "u".into(),
            },
        };
        p
    }

    fn input(profile: ConnectionProfile, pw: Option<&str>) -> ConnectionInput {
        ConnectionInput {
            profile,
            sasl_password: pw.map(String::from),
            tls_passwords: None,
        }
    }

    fn mtls(
        mut p: ConnectionProfile,
        format: CertFormat,
        ca_format: CertFormat,
    ) -> ConnectionProfile {
        let abs = if cfg!(windows) { "C:\\c\\c" } else { "/c/c" };
        p.security = SecurityConfig::Ssl {
            tls: TlsConfig {
                ca_location: Some(abs.into()),
                ca_format,
                verify_hostname: true,
                client_cert: Some(ClientCert {
                    format,
                    certificate_path: abs.into(),
                    key_path: Some(abs.into()),
                    alias: None,
                }),
            },
        };
        p
    }

    fn tls_input(
        profile: ConnectionProfile,
        key: Option<&str>,
        store: Option<&str>,
        trust: Option<&str>,
    ) -> ConnectionInput {
        ConnectionInput {
            profile,
            sasl_password: None,
            tls_passwords: Some(TlsPasswordsInput {
                key_password: key.map(String::from),
                keystore_password: store.map(String::from),
                truststore_password: trust.map(String::from),
            }),
        }
    }

    #[test]
    fn tls_passwords_set_keep_and_clear() {
        let (_d, st) = state(Arc::new(MemoryStore::default()));
        let p = || mtls(sample_profile("a"), CertFormat::Jks, CertFormat::Jks);
        let get = |st: &AppState, f: &str| st.secrets.get(&secret_key("a", f)).unwrap();
        save_connection_blocking(&st, tls_input(p(), Some("k"), Some("s"), Some("t"))).unwrap();
        assert_eq!(get(&st, SSL_KEY_PASSWORD).as_deref(), Some("k"));
        // null — оставить, "" — удалить.
        save_connection_blocking(&st, tls_input(p(), None, Some(""), None)).unwrap();
        assert_eq!(get(&st, SSL_KEY_PASSWORD).as_deref(), Some("k"));
        assert_eq!(get(&st, SSL_KEYSTORE_PASSWORD), None);
        assert_eq!(get(&st, SSL_TRUSTSTORE_PASSWORD).as_deref(), Some("t"));
    }

    #[test]
    fn unused_tls_passwords_are_deleted() {
        let (_d, st) = state(Arc::new(MemoryStore::default()));
        save_connection_blocking(
            &st,
            tls_input(
                mtls(sample_profile("a"), CertFormat::Jks, CertFormat::Jks),
                Some("k"),
                Some("s"),
                Some("t"),
            ),
        )
        .unwrap();
        // PEM-клиент и PEM-CA: пароль хранилища и truststore больше не нужны.
        save_connection_blocking(
            &st,
            tls_input(
                mtls(sample_profile("a"), CertFormat::Pem, CertFormat::Pem),
                None,
                None,
                None,
            ),
        )
        .unwrap();
        let get = |f: &str| st.secrets.get(&secret_key("a", f)).unwrap();
        assert_eq!(get(SSL_KEY_PASSWORD).as_deref(), Some("k"));
        assert_eq!(get(SSL_KEYSTORE_PASSWORD), None);
        assert_eq!(get(SSL_TRUSTSTORE_PASSWORD), None);
        // PLAINTEXT — удаляются все пароли TLS.
        save_connection_blocking(&st, input(sample_profile("a"), None)).unwrap();
        assert_eq!(get(SSL_KEY_PASSWORD), None);
    }

    #[test]
    fn connection_input_deserializes_tls_passwords() {
        let mut v = serde_json::json!({
            "saslPassword": null,
            "tlsPasswords": {"keyPassword": null, "keystorePassword": "x", "truststorePassword": ""}
        });
        v["profile"] = serde_json::to_value(sample_profile("a")).unwrap();
        let i: ConnectionInput = serde_json::from_value(v).unwrap();
        let t = i.tls_passwords.unwrap();
        assert_eq!(t.key_password, None);
        assert_eq!(t.keystore_password.as_deref(), Some("x"));
        assert_eq!(t.truststore_password.as_deref(), Some(""));
    }

    fn state(secrets: Arc<dyn SecretStore>) -> (tempfile::TempDir, AppState) {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), secrets).unwrap();
        (dir, st)
    }

    #[test]
    fn switching_off_sasl_deletes_secret() {
        let (_d, st) = state(Arc::new(MemoryStore::default()));
        save_connection_blocking(&st, input(sasl(sample_profile("a")), Some("pw"))).unwrap();
        let key = secret_key("a", SASL_PASSWORD);
        assert_eq!(st.secrets.get(&key).unwrap().as_deref(), Some("pw"));
        // Форма может прислать старый пароль и для PLAINTEXT — он не должен сохраниться.
        save_connection_blocking(&st, input(sample_profile("a"), Some("pw"))).unwrap();
        assert_eq!(st.secrets.get(&key).unwrap(), None);
    }

    #[test]
    fn empty_password_keeps_existing_secret() {
        let (_d, st) = state(Arc::new(MemoryStore::default()));
        save_connection_blocking(&st, input(sasl(sample_profile("a")), Some("pw"))).unwrap();
        save_connection_blocking(&st, input(sasl(sample_profile("a")), Some(""))).unwrap();
        let key = secret_key("a", SASL_PASSWORD);
        assert_eq!(st.secrets.get(&key).unwrap().as_deref(), Some("pw"));
    }

    #[test]
    fn keychain_failure_does_not_leave_new_profile() {
        let (_d, st) = state(Arc::new(BrokenKeychain));
        let err = save_connection_blocking(&st, input(sasl(sample_profile("a")), Some("pw")))
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(
            st.connections.get("a").unwrap_err().code,
            ErrorCode::NotFound
        );
    }

    #[test]
    fn keychain_failure_restores_previous_profile() {
        let (_d, st) = state(Arc::new(BrokenKeychain));
        st.connections.upsert(sample_profile("a")).unwrap();
        let mut changed = sasl(sample_profile("a"));
        changed.name = "новое имя".into();
        assert!(save_connection_blocking(&st, input(changed, Some("pw"))).is_err());
        assert_eq!(st.connections.get("a").unwrap(), sample_profile("a"));
    }

    #[test]
    fn saving_connection_cancels_its_live_sessions() {
        use std::sync::atomic::Ordering;
        let (_d, st) = state(Arc::new(MemoryStore::default()));
        st.connections.upsert(sample_profile("a")).unwrap();
        let (_, a) = st.sessions.register("a");
        let (_, b) = st.sessions.register("b");
        // Сессии держат старый профиль (адреса, учётные данные, read-only) — их нужно остановить.
        let mut ro = sample_profile("a");
        ro.read_only = true;
        save_connection_blocking(&st, input(ro, None)).unwrap();
        assert!(
            a.load(Ordering::Relaxed),
            "сессия изменённого подключения отменена"
        );
        assert!(!b.load(Ordering::Relaxed), "чужие сессии не тронуты");
    }

    fn prod(mut p: ConnectionProfile) -> ConnectionProfile {
        p.environment = Environment::Prod;
        p
    }

    #[test]
    fn prod_environment_requires_pro_but_existing_prod_stays_editable() {
        let (_d, st) = state(Arc::new(MemoryStore::default()));
        // Новый prod-профиль без лицензии — нельзя.
        let e = save_connection_blocking(&st, input(prod(sample_profile("a")), None)).unwrap_err();
        assert_eq!(e.code, ErrorCode::ProRequired);
        assert!(st.connections.get("a").is_err());
        // dev → prod — нельзя.
        save_connection_blocking(&st, input(sample_profile("b"), None)).unwrap();
        assert_eq!(
            save_connection_blocking(&st, input(prod(sample_profile("b")), None))
                .unwrap_err()
                .code,
            ErrorCode::ProRequired
        );
        // Существующий prod-профиль (создан раньше) остаётся редактируемым и защищённым.
        st.connections.upsert(prod(sample_profile("c"))).unwrap();
        let mut renamed = prod(sample_profile("c"));
        renamed.name = "новое имя".into();
        save_connection_blocking(&st, input(renamed, None)).unwrap();
    }

    #[test]
    fn prod_environment_with_pro() {
        let (d, mut st) = state(Arc::new(MemoryStore::default()));
        st.license = Arc::new(crate::license::pro_for_tests(d.path()));
        save_connection_blocking(&st, input(prod(sample_profile("a")), None)).unwrap();
    }
}
