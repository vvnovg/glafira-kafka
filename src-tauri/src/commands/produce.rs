use tauri::State;

use super::blocking;
use crate::error::AppResult;
use crate::kafka::producer::{self, ProduceResult, ProducerSettings};
use crate::produce::payload::{ProduceMessage, encode_message};
use crate::produce::template::{RenderContext, render_message, system_env};
use crate::state::AppState;
use crate::storage::history::HistoryEntry;
use crate::storage::templates::Template;

#[tauri::command]
pub async fn produce(
    state: State<'_, AppState>,
    connection_id: String,
    message: ProduceMessage,
    settings: ProducerSettings,
    confirm: Option<String>,
) -> AppResult<ProduceResult> {
    produce_impl(state.inner(), connection_id, message, settings, confirm).await
}

// Мутирующая команда: `guarded` — первым делом, до разбора ввода и сети.
pub async fn produce_impl(
    st: &AppState,
    connection_id: String,
    message: ProduceMessage,
    settings: ProducerSettings,
    confirm: Option<String>,
) -> AppResult<ProduceResult> {
    let profile = st.guarded(&connection_id, confirm.as_deref())?;
    settings.validate()?;
    let mut rng = fastrand::Rng::new();
    let rendered = render_message(
        &message,
        &mut RenderContext {
            seq: 0,
            now: chrono::Utc::now(),
            rng: &mut rng,
            env: &system_env,
        },
    )?;
    let encoded = encode_message(&rendered)?;

    // Создание producer'а читает keychain и стартует потоки librdkafka — на blocking-потоке.
    let st2 = st.clone();
    let h = blocking(move || st2.producers.get(&profile, st2.secrets.as_ref(), &settings)).await?;
    let result = producer::send(&h, &encoded).await;
    // Вытеснение при AUTH/TLS и финальный drop producer'а — тоже на blocking-потоке.
    let (st3, id, err) = (
        st.clone(),
        connection_id.clone(),
        result.as_ref().err().cloned(),
    );
    let _ = blocking(move || {
        if let Some(e) = &err {
            st3.evict_if_client_broken(&id, e);
        }
        drop(h);
        Ok(())
    })
    .await;
    let result = result?;

    let (st4, r) = (st.clone(), result.clone());
    let saved = blocking(move || {
        st4.history
            .append(&connection_id, HistoryEntry::new(rendered, settings, r))
    })
    .await;
    if let Err(e) = saved {
        // Сообщение уже в Kafka: ошибка истории не должна выглядеть как ошибка отправки.
        tracing::warn!("не удалось записать историю отправки: {}", e.message);
    }
    Ok(result)
}

#[tauri::command]
pub async fn list_history(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<Vec<HistoryEntry>> {
    let st = state.inner().clone();
    blocking(move || {
        st.connections.get(&connection_id)?;
        st.history.list(&connection_id)
    })
    .await
}

#[tauri::command]
pub async fn list_templates(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<Vec<Template>> {
    let st = state.inner().clone();
    blocking(move || {
        st.connections.get(&connection_id)?;
        Ok(st.templates.list(&connection_id))
    })
    .await
}

#[tauri::command]
pub async fn save_template(state: State<'_, AppState>, template: Template) -> AppResult<Template> {
    let st = state.inner().clone();
    blocking(move || {
        if let Some(id) = &template.connection_id {
            st.connections.get(id)?;
        }
        st.templates.save(template)
    })
    .await
}

#[tauri::command]
pub async fn delete_template(state: State<'_, AppState>, id: String) -> AppResult<()> {
    let st = state.inner().clone();
    blocking(move || st.templates.delete(&id)).await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::error::ErrorCode;
    use crate::produce::payload::{PayloadFormat, PayloadInput};
    use crate::secrets::MemoryStore;
    use crate::storage::connections::{Environment, sample_profile};

    /// Bootstrap недоступен: если проверка не сработает до сети, код будет не тот.
    fn state() -> (tempfile::TempDir, AppState) {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        for (id, env, ro) in [
            ("ro", Environment::Dev, true),
            ("prod", Environment::Prod, false),
            ("dev", Environment::Dev, false),
        ] {
            let mut p = sample_profile(id);
            p.environment = env;
            p.read_only = ro;
            p.bootstrap_servers = "127.0.0.1:1".into();
            st.connections.upsert(p).unwrap();
        }
        (dir, st)
    }

    fn message(value: &str, format: PayloadFormat) -> ProduceMessage {
        ProduceMessage {
            topic: "t".into(),
            partition: None,
            key: PayloadInput {
                format: PayloadFormat::Null,
                text: String::new(),
            },
            value: PayloadInput {
                format,
                text: value.into(),
            },
            headers: vec![],
        }
    }

    #[tokio::test]
    async fn produce_is_guarded() {
        let (_d, st) = state();
        let ok = || message("v", PayloadFormat::String);
        let e = produce_impl(
            &st,
            "ro".into(),
            ok(),
            ProducerSettings::default(),
            Some("conn-ro".into()),
        )
        .await;
        assert_eq!(e.unwrap_err().code, ErrorCode::ReadOnly);
        let e = produce_impl(&st, "prod".into(), ok(), ProducerSettings::default(), None).await;
        assert_eq!(e.unwrap_err().code, ErrorCode::ConfirmationRequired);
        // Guard — раньше разбора ввода: невалидное сообщение не меняет код.
        let bad = message("{oops", PayloadFormat::Json);
        let e = produce_impl(&st, "ro".into(), bad, ProducerSettings::default(), None).await;
        assert_eq!(e.unwrap_err().code, ErrorCode::ReadOnly);
        let e = produce_impl(&st, "nope".into(), ok(), ProducerSettings::default(), None).await;
        assert_eq!(e.unwrap_err().code, ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn invalid_message_is_rejected_before_network() {
        let (_d, st) = state();
        for m in [
            message("{{nope}}", PayloadFormat::String),
            message("{oops", PayloadFormat::Json),
        ] {
            let e = produce_impl(&st, "dev".into(), m, ProducerSettings::default(), None).await;
            assert_eq!(e.unwrap_err().code, ErrorCode::InvalidInput);
        }
        assert!(st.history.list("dev").unwrap().is_empty());
    }

    #[tokio::test]
    async fn json_is_validated_after_rendering() {
        let (_d, st) = state();
        // Шаблон превращается в невалидный JSON только после подстановки.
        let m = message("{\"a\": {{uuid}}}", PayloadFormat::Json);
        let e = produce_impl(&st, "dev".into(), m, ProducerSettings::default(), None).await;
        assert_eq!(e.unwrap_err().code, ErrorCode::InvalidInput);
    }
}
