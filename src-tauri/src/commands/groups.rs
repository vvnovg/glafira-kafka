use tauri::State;

use super::{blocking, with_pooled};
use crate::error::AppResult;
use crate::guard::ensure_name_confirmed;
use crate::kafka::group_ops;
use crate::kafka::groups::{
    GroupDescription, GroupLag, GroupList, GroupPartitionOffset, OffsetTarget, ResetPreviewRow,
    ResetRequest, ResetResult, TopicGroups, check_deletable, validate_reset_request,
    validate_targets,
};
use crate::state::AppState;

/// Блокирующая операция на pooled-handles (см. `AppState::with_handles`).
async fn on_handles<T: Send + 'static>(
    st: &AppState,
    connection_id: String,
    op: impl FnOnce(&crate::kafka::pool::KafkaHandles) -> AppResult<T> + Send + 'static,
) -> AppResult<T> {
    let st = st.clone();
    blocking(move || st.with_handles(&connection_id, op)).await
}

#[tauri::command]
pub async fn list_groups(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<GroupList> {
    on_handles(state.inner(), connection_id, group_ops::list_groups).await
}

#[tauri::command]
pub async fn describe_groups(
    state: State<'_, AppState>,
    connection_id: String,
    group_ids: Vec<String>,
) -> AppResult<Vec<GroupDescription>> {
    on_handles(state.inner(), connection_id, move |h| {
        group_ops::describe_groups(h, &group_ids)
    })
    .await
}

#[tauri::command]
pub async fn get_group_offsets(
    state: State<'_, AppState>,
    connection_id: String,
    group_id: String,
) -> AppResult<Vec<GroupPartitionOffset>> {
    on_handles(state.inner(), connection_id, move |h| {
        group_ops::group_offsets(h, &group_id)
    })
    .await
}

#[tauri::command]
pub async fn get_groups_lag(
    state: State<'_, AppState>,
    connection_id: String,
    group_ids: Vec<String>,
) -> AppResult<Vec<GroupLag>> {
    on_handles(state.inner(), connection_id, move |h| {
        group_ops::groups_lag(h, &group_ids)
    })
    .await
}

#[tauri::command]
pub async fn get_topic_groups(
    state: State<'_, AppState>,
    connection_id: String,
    topic: String,
) -> AppResult<TopicGroups> {
    on_handles(state.inner(), connection_id, move |h| {
        group_ops::topic_groups(h, &topic)
    })
    .await
}

#[tauri::command]
pub async fn preview_offset_reset(
    state: State<'_, AppState>,
    connection_id: String,
    request: ResetRequest,
) -> AppResult<Vec<ResetPreviewRow>> {
    validate_reset_request(&request)?;
    on_handles(state.inner(), connection_id, move |h| {
        group_ops::preview_reset(h, &request)
    })
    .await
}

#[tauri::command]
pub async fn reset_offsets(
    state: State<'_, AppState>,
    connection_id: String,
    group_id: String,
    offsets: Vec<OffsetTarget>,
    confirm: Option<String>,
) -> AppResult<Vec<ResetResult>> {
    reset_offsets_impl(state.inner(), connection_id, group_id, offsets, confirm).await
}

pub async fn reset_offsets_impl(
    st: &AppState,
    connection_id: String,
    group_id: String,
    offsets: Vec<OffsetTarget>,
    confirm: Option<String>,
) -> AppResult<Vec<ResetResult>> {
    st.guarded(&connection_id, confirm.as_deref())?;
    validate_targets(&offsets)?;
    on_handles(st, connection_id, move |h| {
        group_ops::reset_offsets(h, &group_id, &offsets)
    })
    .await
}

#[tauri::command]
pub async fn delete_group(
    state: State<'_, AppState>,
    connection_id: String,
    group_id: String,
    confirm: Option<String>,
    typed_name: Option<String>,
) -> AppResult<()> {
    delete_group_impl(state.inner(), connection_id, group_id, confirm, typed_name).await
}

pub async fn delete_group_impl(
    st: &AppState,
    connection_id: String,
    group_id: String,
    confirm: Option<String>,
    typed_name: Option<String>,
) -> AppResult<()> {
    st.guarded(&connection_id, confirm.as_deref())?;
    ensure_name_confirmed(&group_id, typed_name.as_deref())?;
    let g = group_id.clone();
    let desc = on_handles(st, connection_id.clone(), move |h| {
        group_ops::describe_group(h, &g)
    })
    .await?;
    check_deletable(&desc)?;
    with_pooled(st, connection_id, |h| async move {
        group_ops::delete_group(&h, &group_id).await
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::error::ErrorCode;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::{Environment, sample_profile};

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

    fn target() -> Vec<OffsetTarget> {
        vec![OffsetTarget {
            topic: "t".into(),
            partition: 0,
            offset: 0,
        }]
    }

    #[tokio::test]
    async fn group_mutations_are_guarded() {
        let (_d, st) = state();
        let g = || "g".to_string();
        let ro_reset =
            reset_offsets_impl(&st, "ro".into(), g(), target(), Some("conn-ro".into())).await;
        assert_eq!(ro_reset.unwrap_err().code, ErrorCode::ReadOnly);
        let ro_delete =
            delete_group_impl(&st, "ro".into(), g(), Some("conn-ro".into()), Some(g())).await;
        assert_eq!(ro_delete.unwrap_err().code, ErrorCode::ReadOnly);
        let prod_reset = reset_offsets_impl(&st, "prod".into(), g(), target(), None).await;
        assert_eq!(
            prod_reset.unwrap_err().code,
            ErrorCode::ConfirmationRequired
        );
        let prod_delete = delete_group_impl(&st, "prod".into(), g(), None, Some(g())).await;
        assert_eq!(
            prod_delete.unwrap_err().code,
            ErrorCode::ConfirmationRequired
        );
        let wrong_name = delete_group_impl(&st, "dev".into(), g(), None, Some("x".into())).await;
        assert_eq!(
            wrong_name.unwrap_err().code,
            ErrorCode::ConfirmationRequired
        );
        let empty_plan = reset_offsets_impl(&st, "dev".into(), g(), vec![], None).await;
        assert_eq!(empty_plan.unwrap_err().code, ErrorCode::InvalidInput);
    }
}
