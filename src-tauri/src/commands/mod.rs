pub mod browse;
pub mod cluster;
pub mod connections;
pub mod topics;

use crate::error::{AppError, AppResult};

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
