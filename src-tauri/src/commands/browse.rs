use tauri::ipc::Channel;
use tauri::State;

use crate::browse::session::{run_browse, BrowseParams, EventSink};
use crate::browse::types::BrowseEvent;
use crate::error::{AppError, AppResult};
use crate::state::AppState;

impl EventSink for Channel<BrowseEvent> {
    fn send(&self, event: BrowseEvent) -> bool {
        Channel::<BrowseEvent>::send(self, event).is_ok()
    }
}

#[tauri::command]
pub fn start_browse(state: State<'_, AppState>, params: BrowseParams, on_event: Channel<BrowseEvent>) -> AppResult<String> {
    let profile = state.profile(&params.connection_id)?;
    let secrets = state.secrets.clone();
    let sessions = state.sessions.clone();
    let (id, cancel) = sessions.register();
    let session_id = id.clone();
    std::thread::Builder::new()
        .name(format!("browse-{id}"))
        .spawn(move || {
            run_browse(&profile, secrets.as_ref(), params, cancel, &on_event);
            sessions.remove(&session_id);
        })
        .map_err(|e| AppError::internal(format!("Не удалось запустить поток чтения: {e}")))?;
    Ok(id)
}

#[tauri::command]
pub fn stop_session(state: State<'_, AppState>, session_id: String) -> bool {
    state.sessions.cancel(&session_id)
}
