pub mod browse;
pub mod cluster;
pub mod connections;
pub mod topics;

use rdkafka::admin::ResourceSpecifier;

use crate::error::{AppError, AppResult};
use crate::kafka::configs::{ConfigEntryDto, describe_configs};
use crate::state::AppState;

/// Блокирующие вызовы librdkafka не выполняются на async-потоках Tauri.
pub(crate) async fn blocking<T, F>(f: F) -> AppResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> AppResult<T> + Send + 'static,
{
    tauri::async_runtime::spawn_blocking(f)
        .await
        .map_err(|e| AppError::internal(format!("Фоновая задача завершилась аварийно: {e}")))?
}

/// DescribeConfigs на pooled-handles; при AUTH/TLS-ошибке handles вытесняются (как в
/// `AppState::with_handles`), последняя ссылка освобождается на blocking-потоке.
pub(crate) async fn pooled_configs(
    state: &AppState,
    connection_id: String,
    spec: ResourceSpecifier<'_>,
) -> AppResult<Vec<ConfigEntryDto>> {
    let st = state.clone();
    let id = connection_id.clone();
    let h = blocking(move || st.handles(&id)).await?;
    let r = describe_configs(&h, spec).await;
    if let Err(e) = &r {
        let (st, e) = (state.clone(), e.clone());
        let _ = blocking(move || {
            st.evict_if_client_broken(&connection_id, &e);
            drop(h);
            Ok(())
        })
        .await;
    }
    r
}
