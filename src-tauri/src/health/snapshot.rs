//! Снимок кластера, по которому считаются правила (спец. G2 §3). Только данные: сборка — в
//! `collect.rs`, правила — в `rules/`.

use std::collections::BTreeMap;

use crate::kafka::metadata::{BrokerInfo, TopicMeta};

/// Ключи конфига топика, которые читают правила.
pub const TOPIC_CONFIG_KEYS: &[&str] = &["min.insync.replicas", "unclean.leader.election.enable"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigValue {
    pub value: Option<String>,
    /// Debug-имя `ConfigSource` (как в `ConfigEntryDto`): `DynamicTopic`, `Default`…
    pub source: String,
}

pub type ConfigMap = BTreeMap<String, ConfigValue>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSnapshot {
    pub group_id: String,
    pub state: String,
    /// `classic`, `consumer` или `unknown`.
    pub group_type: String,
    /// Число назначенных партиций у каждого участника.
    pub member_partitions: Vec<usize>,
    /// `None` — лаг не прочитан.
    pub total_lag: Option<i64>,
}

/// Что проверено и что нет: из этого строится `check.partial`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Coverage {
    pub topics_total: usize,
    /// Топики, конфиги которых прочитаны.
    pub topics_checked: usize,
    pub topic_config_errors: usize,
    pub broker_config_errors: usize,
    pub groups_total: usize,
    /// Группы, описание которых прочитано.
    pub groups_checked: usize,
    pub group_errors: usize,
    /// Группы, лаг которых не прочитан (ошибка группы или всего пакета).
    pub lag_errors: usize,
    /// Сбор остановлен общим дедлайном.
    pub timed_out: bool,
}

impl Coverage {
    pub fn partial(&self) -> bool {
        self.timed_out
            || self.topics_checked < self.topics_total
            || self.groups_checked < self.groups_total
            || self.broker_config_errors > 0
            || self.group_errors > 0
            || self.lag_errors > 0
    }
}

/// Лаг групп из прошлой проверки подключения (для `group.lagGrowing`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviousLag {
    pub age_ms: i64,
    pub lag: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClusterSnapshot {
    pub cluster_id: Option<String>,
    pub controller_id: Option<i32>,
    pub brokers: Vec<BrokerInfo>,
    /// Все топики из метаданных, включая внутренние.
    pub topics: Vec<TopicMeta>,
    /// Конфиги брокеров, которые удалось прочитать.
    pub broker_configs: BTreeMap<i32, ConfigMap>,
    /// Конфиги проверенных топиков, только `TOPIC_CONFIG_KEYS`.
    pub topic_configs: BTreeMap<String, ConfigMap>,
    /// Описанные группы.
    pub groups: Vec<GroupSnapshot>,
    pub coverage: Coverage,
    pub previous: Option<PreviousLag>,
}
