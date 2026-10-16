use tauri::State;

use crate::error::AppResult;
use crate::policy::PolicyStatus;
use crate::state::AppState;

#[tauri::command]
pub fn get_policy_status(state: State<'_, AppState>) -> AppResult<PolicyStatus> {
    Ok(state.policy.status.clone())
}
