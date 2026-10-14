use std::sync::Arc;

use tauri::State;
use tauri::ipc::Channel;

use crate::browse::registry::SessionRegistry;
use crate::browse::session::{BrowseParams, EventSink, run_browse};
use crate::browse::types::BrowseEvent;
use crate::error::{AppError, AppResult};
use crate::state::AppState;

impl EventSink for Channel<BrowseEvent> {
    fn send(&self, event: BrowseEvent) -> bool {
        Channel::<BrowseEvent>::send(self, event).is_ok()
    }
}

/// Снимает сессию с учёта при любом выходе из потока, включая панику.
struct RemoveOnDrop {
    sessions: Arc<SessionRegistry>,
    id: String,
}

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        self.sessions.remove(&self.id);
    }
}

#[tauri::command]
pub fn start_browse(
    state: State<'_, AppState>,
    params: BrowseParams,
    on_event: Channel<BrowseEvent>,
) -> AppResult<String> {
    let profile = state.profile(&params.connection_id)?;
    let secrets = state.secrets.clone();
    let (id, cancel) = state.sessions.register(&params.connection_id);
    // Guard создаётся до spawn: при ошибке запуска замыкание дропается вместе с ним.
    let guard = RemoveOnDrop {
        sessions: state.sessions.clone(),
        id: id.clone(),
    };
    std::thread::Builder::new()
        .name(format!("browse-{id}"))
        .spawn(move || {
            let _guard = guard;
            run_browse(&profile, secrets.as_ref(), params, cancel, &on_event);
        })
        .map_err(|e| AppError::internal(format!("Не удалось запустить поток чтения: {e}")))?;
    Ok(id)
}

#[tauri::command]
pub fn stop_session(state: State<'_, AppState>, session_id: String) -> AppResult<bool> {
    Ok(state.sessions.cancel(&session_id))
}
