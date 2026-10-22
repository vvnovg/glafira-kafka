use tauri::State;

use super::health::mark_applied;
use super::{blocking, with_pooled};
use crate::error::{AppResult, ErrorCode};
use crate::guard::ensure_name_confirmed;
use crate::health::proposals::Applied;
use crate::kafka::admin::{self, ConfigChange, NewTopicSpec, PurgedPartition, TopicDeleteResult};
use crate::kafka::configs::{TopicPolicy, describe_topic_policies};
use crate::kafka::metadata::is_internal;
use crate::state::AppState;

// Каждая мутирующая команда первым делом вызывает `AppState::guarded` (read-only / prod),
// затем проверяет ввод и только потом обращается к сети. `proposal_id` — форма открыта из
// предложения LLM: после успеха оно помечается применённым (спец. G3 §6).

#[tauri::command]
pub async fn create_topic(
    state: State<'_, AppState>,
    connection_id: String,
    spec: NewTopicSpec,
    confirm: Option<String>,
    proposal_id: Option<String>,
) -> AppResult<()> {
    create_topic_impl(state.inner(), connection_id, spec, confirm, proposal_id).await
}

pub async fn create_topic_impl(
    st: &AppState,
    connection_id: String,
    spec: NewTopicSpec,
    confirm: Option<String>,
    proposal_id: Option<String>,
) -> AppResult<()> {
    st.guarded(&connection_id, confirm.as_deref())?;
    admin::validate_new_topic(&spec)?;
    let applied = Applied::CreateTopic(spec.clone());
    with_pooled(st, connection_id.clone(), |h| async move {
        admin::create_topic(&h, &spec).await
    })
    .await?;
    mark_applied(st, &connection_id, proposal_id, applied).await;
    Ok(())
}

#[tauri::command]
pub async fn alter_topic_configs(
    state: State<'_, AppState>,
    connection_id: String,
    topic: String,
    changes: Vec<ConfigChange>,
    confirm: Option<String>,
    proposal_id: Option<String>,
) -> AppResult<()> {
    alter_topic_configs_impl(
        state.inner(),
        connection_id,
        topic,
        changes,
        confirm,
        proposal_id,
    )
    .await
}

pub async fn alter_topic_configs_impl(
    st: &AppState,
    connection_id: String,
    topic: String,
    changes: Vec<ConfigChange>,
    confirm: Option<String>,
    proposal_id: Option<String>,
) -> AppResult<()> {
    st.guarded(&connection_id, confirm.as_deref())?;
    admin::validate_config_changes(&changes)?;
    let applied = Applied::AlterTopicConfigs {
        topic: topic.clone(),
        changes: changes.clone(),
    };
    let (st2, id) = (st.clone(), connection_id.clone());
    blocking(move || st2.with_handles(&id, |h| admin::alter_topic_configs(h, &topic, &changes)))
        .await?;
    mark_applied(st, &connection_id, proposal_id, applied).await;
    Ok(())
}

#[tauri::command]
pub async fn add_partitions(
    state: State<'_, AppState>,
    connection_id: String,
    topic: String,
    new_total: i32,
    confirm: Option<String>,
    proposal_id: Option<String>,
) -> AppResult<()> {
    add_partitions_impl(
        state.inner(),
        connection_id,
        topic,
        new_total,
        confirm,
        proposal_id,
    )
    .await
}

pub async fn add_partitions_impl(
    st: &AppState,
    connection_id: String,
    topic: String,
    new_total: i32,
    confirm: Option<String>,
    proposal_id: Option<String>,
) -> AppResult<()> {
    st.guarded(&connection_id, confirm.as_deref())?;
    let current = partition_ids(st, &connection_id, &topic).await?.len();
    admin::validate_partition_increase(current, new_total)?;
    let applied = Applied::AddPartitions {
        topic: topic.clone(),
        new_total,
    };
    with_pooled(st, connection_id.clone(), |h| async move {
        admin::add_partitions(&h, &topic, new_total as usize).await
    })
    .await?;
    mark_applied(st, &connection_id, proposal_id, applied).await;
    Ok(())
}

#[tauri::command]
pub async fn purge_topic(
    state: State<'_, AppState>,
    connection_id: String,
    topic: String,
    partitions: Option<Vec<i32>>,
    confirm: Option<String>,
    typed_name: Option<String>,
) -> AppResult<Vec<PurgedPartition>> {
    purge_topic_impl(
        state.inner(),
        connection_id,
        topic,
        partitions,
        confirm,
        typed_name,
    )
    .await
}

pub async fn purge_topic_impl(
    st: &AppState,
    connection_id: String,
    topic: String,
    partitions: Option<Vec<i32>>,
    confirm: Option<String>,
    typed_name: Option<String>,
) -> AppResult<Vec<PurgedPartition>> {
    st.guarded(&connection_id, confirm.as_deref())?;
    if is_internal(&topic) {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.topics.internalPurge",
            "Internal topic “{{topic}}” cannot be purged",
            topic = topic
        ));
    }
    ensure_name_confirmed(&topic, typed_name.as_deref())?;
    let all = partition_ids(st, &connection_id, &topic).await?;
    let selected = admin::resolve_partitions(&all, partitions.as_deref())?;
    with_pooled(st, connection_id, |h| async move {
        admin::purge_topic(&h, &topic, &selected).await
    })
    .await
}

#[tauri::command]
pub async fn delete_topics(
    state: State<'_, AppState>,
    connection_id: String,
    topics: Vec<String>,
    confirm: Option<String>,
    typed_confirmation: Option<String>,
) -> AppResult<Vec<TopicDeleteResult>> {
    delete_topics_impl(
        state.inner(),
        connection_id,
        topics,
        confirm,
        typed_confirmation,
    )
    .await
}

pub async fn delete_topics_impl(
    st: &AppState,
    connection_id: String,
    topics: Vec<String>,
    confirm: Option<String>,
    typed_confirmation: Option<String>,
) -> AppResult<Vec<TopicDeleteResult>> {
    st.guarded(&connection_id, confirm.as_deref())?;
    // Дубликаты убираются до подтверждения: число в нём — число уникальных топиков.
    let mut seen = std::collections::HashSet::new();
    let topics: Vec<String> = topics
        .into_iter()
        .filter(|t| seen.insert(t.clone()))
        .collect();
    if topics.is_empty() {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.topics.noneSelected",
            "No topics selected"
        ));
    }
    if let Some(t) = topics.iter().find(|t| is_internal(t)) {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.topics.internalDelete",
            "Internal topic “{{topic}}” cannot be deleted",
            topic = t
        ));
    }
    // Один топик подтверждается его именем, несколько — их числом.
    let expected = match topics.as_slice() {
        [one] => one.clone(),
        many => many.len().to_string(),
    };
    ensure_name_confirmed(&expected, typed_confirmation.as_deref())?;
    with_pooled(st, connection_id, |h| async move {
        admin::delete_topics(&h, &topics).await
    })
    .await
}

#[tauri::command]
pub async fn get_topic_policies(
    state: State<'_, AppState>,
    connection_id: String,
    topics: Vec<String>,
) -> AppResult<Vec<TopicPolicy>> {
    with_pooled(state.inner(), connection_id, |h| async move {
        describe_topic_policies(&h, &topics).await
    })
    .await
}

async fn partition_ids(st: &AppState, connection_id: &str, topic: &str) -> AppResult<Vec<i32>> {
    let (st, id, topic) = (st.clone(), connection_id.to_string(), topic.to_string());
    blocking(move || st.with_handles(&id, |h| admin::topic_partition_ids(h, &topic))).await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::error::ErrorCode;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::{Environment, sample_profile};

    /// Подключения: ro (read-only), prod (без read-only), dev. Bootstrap недоступен:
    /// если проверка не сработает до сети, тест увидит CONNECTION_FAILED вместо ожидаемого кода.
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

    fn spec() -> NewTopicSpec {
        NewTopicSpec {
            name: "t".into(),
            partitions: 1,
            replication_factor: 1,
            configs: Default::default(),
        }
    }

    async fn all_mutations(st: &AppState, id: &str, confirm: Option<String>) -> Vec<ErrorCode> {
        let s = |s: &str| Some(s.to_string());
        vec![
            create_topic_impl(st, id.into(), spec(), confirm.clone(), None)
                .await
                .unwrap_err()
                .code,
            alter_topic_configs_impl(
                st,
                id.into(),
                "t".into(),
                vec![ConfigChange::Delete { name: "x".into() }],
                confirm.clone(),
                None,
            )
            .await
            .unwrap_err()
            .code,
            add_partitions_impl(st, id.into(), "t".into(), 5, confirm.clone(), None)
                .await
                .unwrap_err()
                .code,
            purge_topic_impl(st, id.into(), "t".into(), None, confirm.clone(), s("t"))
                .await
                .unwrap_err()
                .code,
            delete_topics_impl(st, id.into(), vec!["t".into()], confirm.clone(), s("t"))
                .await
                .unwrap_err()
                .code,
        ]
    }

    #[tokio::test]
    async fn read_only_blocks_every_topic_mutation() {
        let (_d, st) = state();
        assert_eq!(
            all_mutations(&st, "ro", Some("conn-ro".into())).await,
            vec![ErrorCode::ReadOnly; 5]
        );
    }

    #[tokio::test]
    async fn prod_requires_connection_name() {
        let (_d, st) = state();
        assert_eq!(
            all_mutations(&st, "prod", None).await,
            vec![ErrorCode::ConfirmationRequired; 5]
        );
    }

    #[tokio::test]
    async fn purge_and_delete_require_object_name() {
        let (_d, st) = state();
        let wrong = Some("другое".to_string());
        let purge =
            purge_topic_impl(&st, "dev".into(), "t".into(), None, None, wrong.clone()).await;
        assert_eq!(purge.unwrap_err().code, ErrorCode::ConfirmationRequired);
        let one = delete_topics_impl(&st, "dev".into(), vec!["t".into()], None, wrong).await;
        assert_eq!(one.unwrap_err().code, ErrorCode::ConfirmationRequired);
        // Несколько топиков — подтверждается их число.
        let many = delete_topics_impl(
            &st,
            "dev".into(),
            vec!["a".into(), "b".into()],
            None,
            Some("a".into()),
        )
        .await;
        assert_eq!(many.unwrap_err().code, ErrorCode::ConfirmationRequired);
        // Дубликаты схлопываются: ["a","a"] — это один топик, подтверждается именем.
        let dup = delete_topics_impl(
            &st,
            "dev".into(),
            vec!["a".into(), "a".into()],
            None,
            Some("2".into()),
        )
        .await;
        assert_eq!(dup.unwrap_err().code, ErrorCode::ConfirmationRequired);
    }

    #[tokio::test]
    async fn internal_topics_are_rejected_after_guard() {
        let (_d, st) = state();
        let s = |s: &str| Some(s.to_string());
        let e = purge_topic_impl(
            &st,
            "dev".into(),
            "__consumer_offsets".into(),
            None,
            None,
            s("__consumer_offsets"),
        )
        .await
        .unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert_eq!(e.key, "errors.topics.internalPurge");
        assert_eq!(e.params["topic"], "__consumer_offsets");
        let e = delete_topics_impl(
            &st,
            "dev".into(),
            vec!["a".into(), "_schemas".into()],
            None,
            s("2"),
        )
        .await
        .unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert_eq!(e.key, "errors.topics.internalDelete");
        assert_eq!(e.params["topic"], "_schemas");
        // Guard остаётся первым: read-only отвечает READ_ONLY даже для внутреннего топика.
        let e = delete_topics_impl(
            &st,
            "ro".into(),
            vec!["__consumer_offsets".into()],
            s("conn-ro"),
            s("__consumer_offsets"),
        )
        .await
        .unwrap_err();
        assert_eq!(e.code, ErrorCode::ReadOnly);
    }

    #[tokio::test]
    async fn invalid_input_is_rejected_before_network() {
        let (_d, st) = state();
        let mut bad = spec();
        bad.name = "bad name".into();
        let e = create_topic_impl(&st, "dev".into(), bad, None, None)
            .await
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        let e = alter_topic_configs_impl(&st, "dev".into(), "t".into(), vec![], None, None)
            .await
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        let e = delete_topics_impl(&st, "dev".into(), vec![], None, None)
            .await
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
    }
}
