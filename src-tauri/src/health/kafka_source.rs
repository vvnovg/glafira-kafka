//! `Source` проверки на существующих функциях чтения Kafka (pooled-handles подключения).
//! Только чтение: метаданные, DescribeConfigs, описание групп и их лаг.

use rdkafka::admin::ResourceSpecifier;

use super::collect::{Fut, GroupRead, Metadata, Source};
use super::rules::DRIFT_KEYS;
use super::snapshot::{ConfigMap, ConfigValue, GroupSnapshot, TOPIC_CONFIG_KEYS};
use crate::commands::{blocking, pooled_configs, with_pooled};
use crate::kafka::configs::{ConfigEntryDto, describe_topic_configs};
use crate::kafka::{cluster, group_ops};
use crate::state::AppState;

pub struct KafkaSource {
    state: AppState,
    connection_id: String,
}

impl KafkaSource {
    pub fn new(state: AppState, connection_id: &str) -> Self {
        Self {
            state,
            connection_id: connection_id.to_string(),
        }
    }

    fn parts(&self) -> (AppState, String) {
        (self.state.clone(), self.connection_id.clone())
    }
}

fn to_map(entries: Vec<ConfigEntryDto>, keys: &[&str]) -> ConfigMap {
    entries
        .into_iter()
        .filter(|e| keys.contains(&e.name.as_str()))
        .map(|e| {
            let value = if e.is_sensitive { None } else { e.value };
            (
                e.name,
                ConfigValue {
                    value,
                    source: e.source,
                },
            )
        })
        .collect()
}

impl Source for KafkaSource {
    fn metadata(&self) -> Fut<Metadata> {
        let (st, id) = self.parts();
        Box::pin(
            async move { blocking(move || st.with_handles(&id, cluster::cluster_metadata)).await },
        )
    }

    fn broker_config(&self, broker_id: i32) -> Fut<ConfigMap> {
        let (st, id) = self.parts();
        Box::pin(async move {
            let entries = pooled_configs(&st, id, ResourceSpecifier::Broker(broker_id)).await?;
            Ok(to_map(entries, DRIFT_KEYS))
        })
    }

    fn topic_configs(&self, topics: Vec<String>) -> Fut<Vec<(String, Option<ConfigMap>)>> {
        let (st, id) = self.parts();
        Box::pin(async move {
            let items = with_pooled(&st, id, move |h| async move {
                describe_topic_configs(&h, &topics, TOPIC_CONFIG_KEYS).await
            })
            .await?;
            Ok(items
                .into_iter()
                .map(|(name, entries)| (name, entries.map(|e| to_map(e, TOPIC_CONFIG_KEYS))))
                .collect())
        })
    }

    fn list_groups(&self) -> Fut<Vec<String>> {
        let (st, id) = self.parts();
        Box::pin(async move {
            let l = blocking(move || st.with_handles(&id, group_ops::list_groups)).await?;
            Ok(l.groups.into_iter().map(|g| g.group_id).collect())
        })
    }

    fn describe_groups(&self, ids: Vec<String>) -> Fut<Vec<GroupRead>> {
        let (st, id) = self.parts();
        Box::pin(async move {
            let groups =
                blocking(move || st.with_handles(&id, |h| group_ops::describe_groups(h, &ids)))
                    .await?;
            Ok(groups
                .into_iter()
                .map(|g| {
                    let ok = g.error.is_none() && !g.not_found;
                    let snap = ok.then(|| GroupSnapshot {
                        group_id: g.group_id.clone(),
                        state: g.state.clone(),
                        group_type: g.group_type.clone(),
                        member_partitions: g.members.iter().map(|m| m.assignment.len()).collect(),
                        total_lag: None,
                    });
                    (g.group_id, snap)
                })
                .collect())
        })
    }

    fn groups_lag(&self, ids: Vec<String>) -> Fut<Vec<(String, Option<i64>)>> {
        let (st, id) = self.parts();
        Box::pin(async move {
            let lags =
                blocking(move || st.with_handles(&id, |h| group_ops::groups_lag(h, &ids))).await?;
            Ok(lags
                .into_iter()
                .map(|l| (l.group_id, l.total_lag))
                .collect())
        })
    }
}
