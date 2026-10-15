use tauri::State;
use tauri::ipc::Channel;

use super::{RemoveOnDrop, blocking};
use crate::error::{AppError, AppResult};
use crate::kafka::producer::{self, ProduceResult, ProducerSettings};
use crate::produce::batch::{self, BatchEvent, BatchParams, BatchSink};
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

impl BatchSink for Channel<BatchEvent> {
    fn send(&self, event: BatchEvent) -> bool {
        Channel::<BatchEvent>::send(self, event).is_ok()
    }
}

/// Пакетная отправка: сессия в общем реестре (останавливается `stop_session` и удалением
/// подключения), прогресс — событиями в канал. Возвращает id сессии.
#[tauri::command]
pub fn produce_batch(
    state: State<'_, AppState>,
    params: BatchParams,
    on_event: Channel<BatchEvent>,
    confirm: Option<String>,
) -> AppResult<String> {
    produce_batch_impl(state.inner(), params, on_event, confirm)
}

// Мутирующая команда: `guarded` — первым делом, до проверки параметров и открытия файла.
// Producer, файл и keychain — только в отдельном потоке сессии.
pub fn produce_batch_impl(
    st: &AppState,
    params: BatchParams,
    sink: impl BatchSink,
    confirm: Option<String>,
) -> AppResult<String> {
    let profile = st.guarded(&params.connection_id, confirm.as_deref())?;
    batch::validate(&params)?;
    let secrets = st.secrets.clone();
    let (id, cancel) = st.sessions.register(&params.connection_id);
    // Guard создаётся до spawn: при ошибке запуска замыкание дропается вместе с ним.
    let guard = RemoveOnDrop {
        sessions: st.sessions.clone(),
        id: id.clone(),
    };
    std::thread::Builder::new()
        .name(format!("batch-{id}"))
        .spawn(move || {
            let _guard = guard;
            batch::run_batch(&profile, secrets.as_ref(), params, cancel, &sink);
        })
        .map_err(|e| AppError::internal(format!("Не удалось запустить поток отправки: {e}")))?;
    Ok(id)
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

    struct ChanSink(std::sync::Mutex<std::sync::mpsc::Sender<BatchEvent>>);

    impl BatchSink for ChanSink {
        fn send(&self, e: BatchEvent) -> bool {
            self.0.lock().unwrap().send(e).is_ok()
        }
    }

    fn sink() -> (ChanSink, std::sync::mpsc::Receiver<BatchEvent>) {
        let (tx, rx) = std::sync::mpsc::channel();
        (ChanSink(std::sync::Mutex::new(tx)), rx)
    }

    fn batch_params(connection_id: &str, count: u64, rate: Option<u32>) -> BatchParams {
        BatchParams {
            connection_id: connection_id.into(),
            source: crate::produce::batch::BatchSource::Copies {
                count,
                message: message("{{seq}}", PayloadFormat::String),
            },
            settings: ProducerSettings::default(),
            rate_per_sec: rate,
        }
    }

    fn wait_last(rx: &std::sync::mpsc::Receiver<BatchEvent>) -> BatchEvent {
        let mut last = None;
        while let Ok(e) = rx.recv_timeout(std::time::Duration::from_secs(10)) {
            last = Some(e);
        }
        last.expect("ни одного события")
    }

    #[test]
    fn batch_is_guarded_before_validation() {
        let (_d, st) = state();
        let e = produce_batch_impl(
            &st,
            batch_params("ro", 1, None),
            sink().0,
            Some("conn-ro".into()),
        );
        assert_eq!(e.unwrap_err().code, ErrorCode::ReadOnly);
        // Невалидные параметры (0 копий) не меняют код: guard — первым.
        let e = produce_batch_impl(&st, batch_params("ro", 0, None), sink().0, None);
        assert_eq!(e.unwrap_err().code, ErrorCode::ReadOnly);
        let e = produce_batch_impl(&st, batch_params("prod", 0, None), sink().0, None);
        assert_eq!(e.unwrap_err().code, ErrorCode::ConfirmationRequired);
        let e = produce_batch_impl(&st, batch_params("nope", 1, None), sink().0, None);
        assert_eq!(e.unwrap_err().code, ErrorCode::NotFound);
        for bad in [0, crate::produce::batch::MAX_COPIES + 1] {
            let e = produce_batch_impl(&st, batch_params("dev", bad, None), sink().0, None);
            assert_eq!(e.unwrap_err().code, ErrorCode::InvalidInput);
        }
        assert!(
            st.sessions.is_empty(),
            "отклонённый пакет не регистрирует сессию"
        );
    }

    #[test]
    fn batch_missing_file_ends_with_error_event() {
        let (d, st) = state();
        let params = BatchParams {
            source: crate::produce::batch::BatchSource::File {
                path: d.path().join("missing.jsonl").to_string_lossy().into(),
                format: crate::produce::source::FileFormat::Jsonl,
                topic: "t".into(),
            },
            ..batch_params("dev", 1, None)
        };
        let (s, rx) = sink();
        produce_batch_impl(&st, params, s, None).unwrap();
        match wait_last(&rx) {
            BatchEvent::Error { error } => assert_eq!(error.code, ErrorCode::InvalidInput),
            other => panic!("ожидали Error, получили {other:?}"),
        }
        assert!(st.sessions.is_empty(), "сессия снята с учёта");
    }

    #[test]
    fn deleting_connection_cancels_batch() {
        let (_d, st) = state();
        let (s, rx) = sink();
        // Брокер недоступен, 1 сообщение/с: пакет ждёт в ограничителе скорости.
        produce_batch_impl(&st, batch_params("dev", 1000, Some(1)), s, None).unwrap();
        assert_eq!(st.sessions.len(), 1);
        std::thread::sleep(std::time::Duration::from_millis(300));
        let started = std::time::Instant::now();
        assert_eq!(st.sessions.cancel_for("dev"), 1);
        match wait_last(&rx) {
            BatchEvent::Finished {
                cancelled,
                sent,
                failed,
                not_sent,
            } => {
                assert!(cancelled);
                // Первое сообщение было в локальной очереди (вынуто при отмене),
                // второе ждало в ограничителе скорости — оба «отменено до отправки».
                assert_eq!((sent, failed, not_sent), (0, 0, 2));
            }
            other => panic!("ожидали Finished, получили {other:?}"),
        }
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        assert!(st.sessions.is_empty(), "сессия снята с учёта");
    }
}
