pub mod app;
pub mod browse;
pub mod cluster;
pub mod connections;
pub mod consume;
pub mod folders;
pub mod groups;
pub mod license;
pub mod mcp;
pub mod produce;
pub mod settings;
pub mod topic_ops;
pub mod topics;

use std::future::Future;
use std::sync::Arc;

use rdkafka::admin::ResourceSpecifier;

use crate::browse::registry::SessionRegistry;
use crate::error::{AppResult, ErrorCode};
use crate::kafka::configs::{ConfigEntryDto, describe_configs};
use crate::kafka::pool::KafkaHandles;
use crate::state::AppState;

/// Блокирующие вызовы librdkafka не выполняются на async-потоках Tauri.
pub(crate) async fn blocking<T, F>(f: F) -> AppResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> AppResult<T> + Send + 'static,
{
    tauri::async_runtime::spawn_blocking(f).await.map_err(|e| {
        app_err!(
            ErrorCode::Internal,
            "errors.app.taskPanicked",
            "Background task failed: {{error}}",
            error = e
        )
    })?
}

/// Async-операция на pooled-handles подключения: handles берутся (keychain, создание клиента) и
/// освобождаются на blocking-потоке; при AUTH/TLS-ошибке вытесняются из пула
/// (как в `AppState::with_handles`).
pub(crate) async fn with_pooled<T, F, Fut>(
    state: &AppState,
    connection_id: String,
    op: F,
) -> AppResult<T>
where
    F: FnOnce(Arc<KafkaHandles>) -> Fut,
    Fut: Future<Output = AppResult<T>>,
{
    let (st, id) = (state.clone(), connection_id.clone());
    let h = blocking(move || st.handles(&id)).await?;
    let r = op(h.clone()).await;
    let (st, err) = (state.clone(), r.as_ref().err().cloned());
    let _ = blocking(move || {
        if let Some(e) = &err {
            st.evict_if_client_broken(&connection_id, e);
        }
        drop(h);
        Ok(())
    })
    .await;
    r
}

/// DescribeConfigs на pooled-handles.
pub(crate) async fn pooled_configs(
    state: &AppState,
    connection_id: String,
    spec: ResourceSpecifier<'_>,
) -> AppResult<Vec<ConfigEntryDto>> {
    with_pooled(state, connection_id, |h| async move {
        describe_configs(&h, spec).await
    })
    .await
}

/// Снимает сессию с учёта при любом выходе из потока, включая панику.
pub(crate) struct RemoveOnDrop {
    pub(crate) sessions: Arc<SessionRegistry>,
    pub(crate) id: String,
}

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        self.sessions.remove(&self.id);
    }
}
