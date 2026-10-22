//! Команды проверки кластера и вкладки «Замечания» (спец. G2 §7, G3 §6). Каждое изменение шлёт
//! событие `health://changed`: значки и блоки других вкладок перечитывают данные.

use serde::Serialize;
use tauri::State;

use super::blocking;
use crate::error::AppResult;
use crate::health::proposals::{Applied, Proposal, ProposalStatus};
use crate::health::service::now_ms;
use crate::health::store::{
    Annotation, AnnotationStatus, CheckRun, HealthStore, MarkApplied, Trigger,
};
use crate::state::AppState;

/// Состояние подключения для интерфейса: последняя проверка, скрытые находки, замечания и
/// предложения LLM.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthView {
    pub last_check: Option<CheckRun>,
    pub ignored: Vec<String>,
    pub annotations: Vec<Annotation>,
    pub proposals: Vec<Proposal>,
}

fn view(st: &AppState, connection_id: &str) -> AppResult<HealthView> {
    st.profile(connection_id)?;
    let f = st.health.store.load(connection_id, now_ms())?;
    Ok(HealthView {
        last_check: f.last_check,
        ignored: f.ignored.into_iter().map(|i| i.key).collect(),
        annotations: f.annotations,
        proposals: f.proposals,
    })
}

/// Отметка «применено» после успеха операции в Kafka (спец. G3 §6). Операция уже прошла, поэтому
/// ничто здесь её не отменяет: предложения нет, оно закрыто или про другой объект — отметка
/// пропускается без события; ошибка файла — только в лог. Без `proposal_id` ничего не делает.
pub(crate) async fn mark_applied(
    st: &AppState,
    connection_id: &str,
    proposal_id: Option<String>,
    applied: Applied,
) {
    let Some(pid) = proposal_id else {
        return;
    };
    let (st, id, pid2) = (st.clone(), connection_id.to_string(), pid.clone());
    let r = blocking(move || {
        let generation = st.health.generation(&id);
        st.profile(&id)?;
        let r = st.health.write(&id, generation, |s| {
            s.mark_applied(&id, &pid2, &applied, now_ms())
        })?;
        if matches!(r, MarkApplied::Applied { .. }) {
            st.health.changed(&id);
        }
        Ok(r)
    })
    .await;
    match r {
        Ok(MarkApplied::Applied { .. }) => {}
        Ok(MarkApplied::Skipped(why)) => {
            tracing::warn!(
                "proposal {pid} of connection {connection_id} not marked applied: {why}"
            );
        }
        Err(e) => tracing::warn!(
            "proposal {pid} of connection {connection_id} not marked applied: {}",
            e.message
        ),
    }
}

/// Изменение файла подключения на blocking-потоке, затем событие и свежее состояние. Запись идёт
/// через `HealthService::write`: поколение читается до проверки подключения, и подключение,
/// удалённое в промежутке, файла не получит.
async fn change(
    st: &AppState,
    connection_id: String,
    op: impl FnOnce(&HealthStore, &str) -> AppResult<()> + Send + 'static,
) -> AppResult<HealthView> {
    let st = st.clone();
    blocking(move || {
        let generation = st.health.generation(&connection_id);
        st.profile(&connection_id)?;
        st.health
            .write(&connection_id, generation, |s| op(s, &connection_id))?;
        st.health.changed(&connection_id);
        view(&st, &connection_id)
    })
    .await
}

#[tauri::command]
pub async fn run_health_check(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<HealthView> {
    let st = state.inner().clone();
    st.health
        .clone()
        .run(&st, &connection_id, Trigger::Ui)
        .await?;
    blocking(move || view(&st, &connection_id)).await
}

#[tauri::command]
pub async fn get_health(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<HealthView> {
    let st = state.inner().clone();
    blocking(move || view(&st, &connection_id)).await
}

#[tauri::command]
pub async fn set_finding_ignored(
    state: State<'_, AppState>,
    connection_id: String,
    key: String,
    ignored: bool,
) -> AppResult<HealthView> {
    change(&state, connection_id, move |s, id| {
        s.set_ignored(id, &key, ignored, now_ms())
    })
    .await
}

#[tauri::command]
pub async fn set_annotation_status(
    state: State<'_, AppState>,
    connection_id: String,
    annotation_id: String,
    status: AnnotationStatus,
) -> AppResult<HealthView> {
    change(&state, connection_id, move |s, id| {
        s.set_status(id, &annotation_id, status, now_ms())
    })
    .await
}

#[tauri::command]
pub async fn delete_annotation(
    state: State<'_, AppState>,
    connection_id: String,
    annotation_id: String,
) -> AppResult<HealthView> {
    change(&state, connection_id, move |s, id| {
        s.delete_annotation(id, &annotation_id, now_ms())
    })
    .await
}

#[tauri::command]
pub async fn delete_all_annotations(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<HealthView> {
    change(&state, connection_id, |s, id| {
        s.delete_all_annotations(id, now_ms()).map(|_| ())
    })
    .await
}

/// «Отклонить» (`rejected`, причина необязательна) или «Открыть снова» (`open`).
#[tauri::command]
pub async fn set_proposal_status(
    state: State<'_, AppState>,
    connection_id: String,
    proposal_id: String,
    status: ProposalStatus,
    reason: Option<String>,
) -> AppResult<HealthView> {
    change(&state, connection_id, move |s, id| {
        s.set_proposal_status(id, &proposal_id, status, reason, now_ms())
    })
    .await
}

#[tauri::command]
pub async fn delete_proposal(
    state: State<'_, AppState>,
    connection_id: String,
    proposal_id: String,
) -> AppResult<HealthView> {
    change(&state, connection_id, move |s, id| {
        s.delete_proposal(id, &proposal_id, now_ms())
    })
    .await
}

#[tauri::command]
pub async fn delete_all_proposals(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<HealthView> {
    change(&state, connection_id, |s, id| {
        s.delete_all_proposals(id, now_ms()).map(|_| ())
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::health::model::{Severity, Target};
    use crate::health::proposals::Change;
    use crate::health::store::{Annotated, Author, NewAnnotation, NewProposal, Proposed};
    use crate::secrets::MemoryStore;
    use crate::storage::connections::sample_profile;

    fn state(dir: &std::path::Path) -> AppState {
        let st = AppState::new(dir, Arc::new(MemoryStore::default())).unwrap();
        st.connections.upsert(sample_profile("c1")).unwrap();
        st
    }

    #[tokio::test]
    async fn changes_notify_and_return_fresh_view() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let n = Arc::new(AtomicUsize::new(0));
        let n2 = n.clone();
        st.health.set_notify(Arc::new(move |id| {
            assert_eq!(id, "c1");
            n2.fetch_add(1, Ordering::SeqCst);
        }));
        let v = change(&st, "c1".into(), |s, id| {
            s.set_ignored(id, "k", true, now_ms())
        })
        .await
        .unwrap();
        assert_eq!(v.ignored, ["k"]);
        let Annotated::Created(aid) = st
            .health
            .store
            .annotate(
                "c1",
                NewAnnotation {
                    target: Target::cluster(),
                    severity: Severity::Info,
                    title: "t".into(),
                    body: "b".into(),
                    related_rule_id: None,
                    author: Author {
                        client: Some("x".into()),
                        session_id: "s".into(),
                    },
                },
                now_ms(),
            )
            .unwrap()
        else {
            panic!()
        };
        let v = change(&st, "c1".into(), move |s, id| {
            s.set_status(id, &aid, AnnotationStatus::Resolved, now_ms())
        })
        .await
        .unwrap();
        assert_eq!(v.annotations[0].status, AnnotationStatus::Resolved);
        assert_eq!(n.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn deleted_connection_gets_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        change(&st, "c1".into(), |s, id| {
            s.set_ignored(id, "k", true, now_ms())
        })
        .await
        .unwrap();
        let file = dir.path().join("health").join("c1.json");
        assert!(file.exists());
        st.delete_connection_full("c1").unwrap();
        assert!(!file.exists());
        let e = change(&st, "c1".into(), |s, id| {
            s.set_ignored(id, "k", true, now_ms())
        })
        .await
        .unwrap_err();
        assert_eq!(e.code, crate::error::ErrorCode::NotFound);
        assert!(!file.exists(), "запись удалённого подключения создала файл");
    }

    fn proposal(st: &AppState, topic: &str) -> String {
        match st
            .health
            .store
            .propose(
                "c1",
                NewProposal {
                    severity: Severity::Warning,
                    title: "More partitions".into(),
                    rationale: "r".into(),
                    change: Change::AddPartitions {
                        topic: topic.into(),
                        new_total: 6,
                    },
                    related_rule_id: None,
                    related_finding_key: None,
                    author: Author {
                        client: Some("claude-ai".into()),
                        session_id: "s".into(),
                    },
                },
                now_ms(),
            )
            .unwrap()
        {
            Proposed::Created(id) => id,
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn proposal_commands_change_status_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let n = Arc::new(AtomicUsize::new(0));
        let n2 = n.clone();
        st.health.set_notify(Arc::new(move |_| {
            n2.fetch_add(1, Ordering::SeqCst);
        }));
        let id = proposal(&st, "orders");
        let v = change(&st, "c1".into(), {
            let id = id.clone();
            move |s, c| {
                s.set_proposal_status(
                    c,
                    &id,
                    ProposalStatus::Rejected,
                    Some("later".into()),
                    now_ms(),
                )
            }
        })
        .await
        .unwrap();
        assert_eq!(v.proposals[0].status, ProposalStatus::Rejected);
        assert_eq!(v.proposals[0].reject_reason.as_deref(), Some("later"));
        let v = change(&st, "c1".into(), {
            let id = id.clone();
            move |s, c| s.set_proposal_status(c, &id, ProposalStatus::Open, None, now_ms())
        })
        .await
        .unwrap();
        assert_eq!(v.proposals[0].status, ProposalStatus::Open);
        let v = change(&st, "c1".into(), move |s, c| {
            s.delete_proposal(c, &id, now_ms())
        })
        .await
        .unwrap();
        assert!(v.proposals.is_empty());
        proposal(&st, "a");
        proposal(&st, "b");
        let v = change(&st, "c1".into(), |s, c| {
            s.delete_all_proposals(c, now_ms()).map(|_| ())
        })
        .await
        .unwrap();
        assert!(v.proposals.is_empty());
        assert_eq!(n.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn mark_applied_closes_the_proposal_and_notifies() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let n = Arc::new(AtomicUsize::new(0));
        let n2 = n.clone();
        st.health.set_notify(Arc::new(move |_| {
            n2.fetch_add(1, Ordering::SeqCst);
        }));
        let id = proposal(&st, "orders");
        // Без id предложения — ничего не происходит.
        mark_applied(
            &st,
            "c1",
            None,
            Applied::AddPartitions {
                topic: "orders".into(),
                new_total: 6,
            },
        )
        .await;
        assert_eq!(n.load(Ordering::SeqCst), 0);
        // С правками (8 вместо 6).
        mark_applied(
            &st,
            "c1",
            Some(id.clone()),
            Applied::AddPartitions {
                topic: "orders".into(),
                new_total: 8,
            },
        )
        .await;
        let p = &view(&st, "c1").unwrap().proposals[0];
        assert_eq!(
            (p.status, p.applied_as_proposed),
            (ProposalStatus::Applied, Some(false))
        );
        assert_eq!(n.load(Ordering::SeqCst), 1);
        // Неизвестное предложение и удалённое подключение — не ошибка (операция уже прошла).
        mark_applied(
            &st,
            "c1",
            Some("nope".into()),
            Applied::AddPartitions {
                topic: "orders".into(),
                new_total: 8,
            },
        )
        .await;
        // Пропуск ничего не меняет: прежнее предложение применено, события нет.
        assert_eq!(
            view(&st, "c1").unwrap().proposals[0].status,
            ProposalStatus::Applied
        );
        assert_eq!(n.load(Ordering::SeqCst), 1);
        st.delete_connection_full("c1").unwrap();
        mark_applied(
            &st,
            "c1",
            Some(id),
            Applied::AddPartitions {
                topic: "orders".into(),
                new_total: 8,
            },
        )
        .await;
        assert!(!dir.path().join("health").join("c1.json").exists());
    }

    #[tokio::test]
    async fn unknown_connection_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let e = change(&st, "nope".into(), |_, _| Ok(())).await.unwrap_err();
        assert_eq!(e.code, crate::error::ErrorCode::NotFound);
    }
}
