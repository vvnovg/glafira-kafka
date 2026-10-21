//! Контракт крейта с приложением: операции чтения, их результаты и трейт [`Backend`].
//! Типы ответов — собственные (стабильный контракт для LLM), а не DTO интерфейса.

use std::future::Future;
use std::pin::Pin;

use serde::Serialize;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientIdent {
    pub name: String,
    pub version: String,
}

/// Сессия глазами приложения: клиент становится известен из `initialize` или из `_meta`
/// первого запроса (протокол 2026-07-28 без `initialize`) и дальше не меняется.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionView {
    pub id: String,
    pub client: Option<ClientIdent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionAccess {
    Approved,
    Pending,
    Rejected,
    /// MCP запрещён (политика) или выключается.
    Disabled,
}

/// Ошибка для LLM: `code` — стабильный код (`not_allowed`, `AUTH_FAILED`…), `message` — по-английски.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendError {
    pub code: String,
    pub message: String,
}

impl BackendError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    /// Нет такого подключения или доступ LLM к нему выключен — не различаем.
    pub fn not_allowed() -> Self {
        Self::new(
            "not_allowed",
            "Unknown connection or LLM access is off for it; call list_connections",
        )
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new("invalid_argument", message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new("not_found", message)
    }

    pub(crate) fn unexpected() -> Self {
        Self::new("internal", "Unexpected backend response")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadOp {
    ListConnections,
    ClusterOverview {
        connection_id: String,
    },
    BrokerConfig {
        connection_id: String,
        broker_id: i32,
    },
    ListTopics {
        connection_id: String,
    },
    DescribeTopic {
        connection_id: String,
        topic: String,
    },
    TopicConfigs {
        connection_id: String,
        topic: String,
    },
    TopicsMessageCounts {
        connection_id: String,
        topics: Vec<String>,
    },
    ListGroups {
        connection_id: String,
    },
    DescribeGroup {
        connection_id: String,
        group_id: String,
    },
    GroupOffsets {
        connection_id: String,
        group_id: String,
    },
    GroupsLag {
        connection_id: String,
        group_ids: Vec<String>,
    },
    TopicGroups {
        connection_id: String,
        topic: String,
    },
}

impl ReadOp {
    pub fn connection_id(&self) -> Option<&str> {
        match self {
            ReadOp::ListConnections => None,
            ReadOp::ClusterOverview { connection_id }
            | ReadOp::BrokerConfig { connection_id, .. }
            | ReadOp::ListTopics { connection_id }
            | ReadOp::DescribeTopic { connection_id, .. }
            | ReadOp::TopicConfigs { connection_id, .. }
            | ReadOp::TopicsMessageCounts { connection_id, .. }
            | ReadOp::ListGroups { connection_id }
            | ReadOp::DescribeGroup { connection_id, .. }
            | ReadOp::GroupOffsets { connection_id, .. }
            | ReadOp::GroupsLag { connection_id, .. }
            | ReadOp::TopicGroups { connection_id, .. } => Some(connection_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReadOutput {
    Connections(Vec<Connection>),
    Cluster(ClusterOverview),
    Configs(Vec<ConfigEntry>),
    Topics(Vec<TopicSummary>),
    Topic(TopicDetails),
    MessageCounts(Vec<MessageCount>),
    Groups(GroupList),
    Group(GroupDescription),
    Offsets(Vec<GroupOffset>),
    Lags(Vec<GroupLag>),
    TopicGroups(TopicGroups),
}

/// Источник данных шлюза. Реализация приложения проверяет `llmAccess` подключения в `read`.
pub trait Backend: Send + Sync + 'static {
    /// Право сессии на вызовы. Вызывается на каждый запрос — должно быть быстрым.
    fn access(&self, session: &SessionView) -> SessionAccess;
    fn read(&self, op: ReadOp) -> BoxFuture<'_, Result<ReadOutput, BackendError>>;
    /// Имя подключения для журнала; `None`, если подключения нет.
    fn connection_name(&self, id: &str) -> Option<String>;
    /// Сессия закрыта (снять ожидающее подтверждение и т. п.).
    fn session_closed(&self, session_id: &str);
    /// Сессии изменились: открыта, закрыта, опознан клиент, начат или закончен вызов.
    fn sessions_changed(&self);
    /// Проверка кластера и замечания (спец. G2 §6). Реализация проверяет `llmAccess` подключения.
    fn health(
        &self,
        op: crate::health::HealthOp,
    ) -> BoxFuture<'_, Result<crate::health::HealthOutput, BackendError>>;
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Connection {
    pub id: String,
    pub name: String,
    pub environment: String,
    pub folder_path: Vec<String>,
    pub read_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Broker {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterOverview {
    pub cluster_id: Option<String>,
    pub controller_id: Option<i32>,
    pub brokers: Vec<Broker>,
    pub topic_count: usize,
    pub internal_topic_count: usize,
    pub partition_count: usize,
    pub under_replicated_partitions: usize,
    pub offline_partitions: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigEntry {
    pub name: String,
    pub value: Option<String>,
    pub source: String,
    pub is_default: bool,
    pub is_read_only: bool,
    pub is_sensitive: bool,
    /// Значение скрыто шлюзом (чувствительное или похоже на секрет).
    pub hidden: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicSummary {
    pub name: String,
    pub internal: bool,
    pub partitions: usize,
    pub replication_factor: usize,
    pub under_replicated: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Partition {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub low: Option<i64>,
    pub high: Option<i64>,
    pub message_count: Option<i64>,
    pub under_replicated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicDetails {
    pub name: String,
    pub internal: bool,
    pub replication_factor: usize,
    pub message_count: Option<i64>,
    pub partitions: Vec<Partition>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageCount {
    pub name: String,
    pub message_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupSummary {
    pub group_id: String,
    pub state: String,
    pub is_simple: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupList {
    pub groups: Vec<GroupSummary>,
    /// Частичные ошибки: список может быть неполным.
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Member {
    pub member_id: String,
    pub client_id: String,
    pub host: String,
    pub group_instance_id: Option<String>,
    pub assignment: Vec<TopicPartition>,
    /// Только у групп нового протокола (KIP-848).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_assignment: Option<Vec<TopicPartition>>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupDescription {
    pub group_id: String,
    pub state: String,
    /// `classic`, `consumer` (KIP-848) или `unknown`.
    pub group_type: String,
    pub assignor: String,
    pub coordinator: Option<Broker>,
    pub members: Vec<Member>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Брокер ответил GROUP_ID_NOT_FOUND.
    #[serde(skip)]
    pub not_found: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupOffset {
    pub topic: String,
    pub partition: i32,
    pub committed: Option<i64>,
    pub log_end: Option<i64>,
    pub lag: Option<i64>,
    pub member_client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupLag {
    pub group_id: String,
    pub total_lag: Option<i64>,
    pub topics: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicGroup {
    pub group_id: String,
    pub state: String,
    pub lag: Option<i64>,
    pub partitions: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicGroups {
    pub groups: Vec<TopicGroup>,
    pub incomplete: bool,
}
