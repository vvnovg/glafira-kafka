use std::sync::Arc;
use std::time::Duration;

use tauri::State;
use tauri::ipc::Channel;

use super::blocking;
use crate::browse::registry::SessionRegistry;
use crate::consume::control::{ConsumerCommand, ConsumerControls, reply_timeout, request};
use crate::consume::session::{ConsumeSink, run_consumer};
use crate::consume::types::{
    CommitTarget, ConsumeEvent, ConsumerParams, current_user, default_group_id, validate,
};
use crate::error::{AppError, AppResult};
use crate::kafka::groups::{OffsetTarget, TopicPartitionRef};
use crate::state::AppState;

impl ConsumeSink for Channel<ConsumeEvent> {
    fn send(&self, event: ConsumeEvent) -> bool {
        Channel::<ConsumeEvent>::send(self, event).is_ok()
    }
}

/// Снимает сессию с учёта (реестр и канал команд) при любом выходе из потока.
struct ConsumerGuard {
    sessions: Arc<SessionRegistry>,
    controls: Arc<ConsumerControls>,
    id: String,
}

impl Drop for ConsumerGuard {
    fn drop(&mut self) {
        self.controls.remove(&self.id);
        self.sessions.remove(&self.id);
    }
}

#[tauri::command]
pub fn start_consumer(
    state: State<'_, AppState>,
    params: ConsumerParams,
    on_event: Channel<ConsumeEvent>,
    confirm: Option<String>,
) -> AppResult<String> {
    start_consumer_impl(state.inner(), params, on_event, confirm)
}

pub fn start_consumer_impl<S: ConsumeSink>(
    st: &AppState,
    params: ConsumerParams,
    sink: S,
    confirm: Option<String>,
) -> AppResult<String> {
    st.license.require_pro()?;
    let profile = st.guarded(&params.connection_id, confirm.as_deref())?;
    validate(&params)?;
    let secrets = st.secrets.clone();
    let (id, cancel) = st.sessions.register(&params.connection_id);
    let (tx, rx) = std::sync::mpsc::channel();
    st.consumers.register(&id, &params.connection_id, tx);
    let guard = ConsumerGuard {
        sessions: st.sessions.clone(),
        controls: st.consumers.clone(),
        id: id.clone(),
    };
    std::thread::Builder::new()
        .name(format!("consumer-{id}"))
        .spawn(move || {
            let _guard = guard;
            run_consumer(&profile, secrets.as_ref(), params, cancel, rx, &sink);
        })
        .map_err(|e| {
            AppError::internal(format!("Не удалось запустить поток consumer-сессии: {e}"))
        })?;
    Ok(id)
}

/// Таймаут ожидания ответа сессии (`reply_timeout`): consumer создаётся с
/// `socket.timeout.ms` = таймаут запроса подключения, а синхронный коммит состоит из поиска
/// координатора и OffsetCommit — каждый ограничен socket.timeout, поэтому ждём 2 × это + запас.
/// Команда несёт deadline и после таймаута не выполняется.
fn session_timeout(st: &AppState, connection_id: &str) -> AppResult<Duration> {
    Ok(reply_timeout(st.profile(connection_id)?.request_timeout_ms))
}

#[tauri::command]
pub async fn commit_offsets(
    state: State<'_, AppState>,
    session_id: String,
    target: CommitTarget,
    confirm: Option<String>,
) -> AppResult<Vec<OffsetTarget>> {
    commit_offsets_impl(state.inner(), session_id, target, confirm).await
}

pub async fn commit_offsets_impl(
    st: &AppState,
    session_id: String,
    target: CommitTarget,
    confirm: Option<String>,
) -> AppResult<Vec<OffsetTarget>> {
    st.license.require_pro()?;
    let (connection_id, tx) = st.consumers.get(&session_id)?;
    st.guarded(&connection_id, confirm.as_deref())?;
    let timeout = session_timeout(st, &connection_id)?;
    blocking(move || {
        request(&tx, timeout, |reply, deadline| ConsumerCommand::Commit {
            target,
            reply,
            deadline,
        })
    })
    .await
}

#[tauri::command]
pub async fn pause_partitions(
    state: State<'_, AppState>,
    session_id: String,
    partitions: Vec<TopicPartitionRef>,
) -> AppResult<()> {
    state.license.require_pro()?;
    let (connection_id, tx) = state.consumers.get(&session_id)?;
    let timeout = session_timeout(state.inner(), &connection_id)?;
    blocking(move || {
        request(&tx, timeout, |reply, deadline| ConsumerCommand::Pause {
            partitions,
            reply,
            deadline,
        })
    })
    .await
}

#[tauri::command]
pub async fn resume_partitions(
    state: State<'_, AppState>,
    session_id: String,
    partitions: Vec<TopicPartitionRef>,
) -> AppResult<()> {
    state.license.require_pro()?;
    let (connection_id, tx) = state.consumers.get(&session_id)?;
    let timeout = session_timeout(state.inner(), &connection_id)?;
    blocking(move || {
        request(&tx, timeout, |reply, deadline| ConsumerCommand::Resume {
            partitions,
            reply,
            deadline,
        })
    })
    .await
}

#[tauri::command]
pub fn suggest_group_id() -> String {
    let suffix: String = uuid::Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .take(6)
        .collect();
    default_group_id(current_user().as_deref(), &suffix)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::error::ErrorCode;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::{Environment, sample_profile};

    struct NullSink;
    impl ConsumeSink for NullSink {
        fn send(&self, _: ConsumeEvent) -> bool {
            true
        }
    }

    fn state() -> (tempfile::TempDir, AppState) {
        let dir = tempfile::tempdir().unwrap();
        let mut st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        st.license = Arc::new(crate::license::pro_for_tests(dir.path()));
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

    fn params(conn: &str, group: &str) -> ConsumerParams {
        serde_json::from_value(serde_json::json!({
            "connectionId": conn, "groupId": group, "topics": ["t"],
            "autoOffsetReset": "earliest", "enableAutoCommit": false, "isolationLevel": "readCommitted",
            "keyFormat": "auto", "valueFormat": "auto"
        }))
        .unwrap()
    }

    #[test]
    fn consumer_sessions_are_guarded() {
        let (_d, st) = state();
        let e = start_consumer_impl(&st, params("ro", "g"), NullSink, Some("conn-ro".into()))
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::ReadOnly);
        let e = start_consumer_impl(&st, params("prod", "g"), NullSink, None).unwrap_err();
        assert_eq!(e.code, ErrorCode::ConfirmationRequired);
        let e = start_consumer_impl(&st, params("dev", " "), NullSink, None).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert!(
            st.sessions.is_empty(),
            "отклонённый запуск не регистрирует сессию"
        );
    }

    #[tokio::test]
    async fn commit_for_unknown_session_is_not_found() {
        let (_d, st) = state();
        let e = commit_offsets_impl(&st, "nope".into(), CommitTarget::AllConsumed, None)
            .await
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::NotFound);
    }

    #[test]
    fn suggested_group_id_shape() {
        let id = suggest_group_id();
        assert!(id.starts_with("glafira-kafka-"), "{id}");
        assert_eq!(id.rsplit('-').next().unwrap().len(), 6);
    }

    #[test]
    fn session_is_unregistered_when_thread_ends() {
        let (_d, st) = state();
        let id = start_consumer_impl(&st, params("dev", "g"), NullSink, None).unwrap();
        assert!(st.consumers.get(&id).is_ok());
        st.sessions.cancel_for("dev");
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while !(st.sessions.is_empty() && st.consumers.get(&id).is_err()) {
            assert!(
                std::time::Instant::now() < deadline,
                "сессия не снята с учёта"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    #[test]
    fn consumer_sessions_require_pro() {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        st.connections.upsert(sample_profile("dev")).unwrap();
        let e = start_consumer_impl(&st, params("dev", "g"), NullSink, None).unwrap_err();
        assert_eq!(e.code, ErrorCode::ProRequired);
        assert!(st.sessions.is_empty(), "сессия не зарегистрирована");
    }
}
