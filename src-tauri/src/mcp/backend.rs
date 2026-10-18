//! Реализация `glafira_mcp::Backend` (спец. G1 §2): существующие функции чтения и проверка
//! `llmAccess` подключения при каждой операции. Изменяющих функций здесь нет и быть не должно.

use std::sync::Arc;

use glafira_mcp::model as m;
use glafira_mcp::{
    Backend, BackendError, BoxFuture, ReadOp, ReadOutput, SessionAccess, SessionView,
};
use rdkafka::admin::ResourceSpecifier;

use super::approvals::Approvals;
use crate::commands::{blocking, pooled_configs};
use crate::error::{AppError, AppResult, ErrorCode};
use crate::kafka::pool::KafkaHandles;
use crate::kafka::{cluster, group_ops, topics};
use crate::state::AppState;

/// Что-то изменилось для интерфейса; `true` — нужен диалог подтверждения (привлечь внимание).
pub type OnChange = Arc<dyn Fn(bool) + Send + Sync>;

pub struct AppBackend {
    state: AppState,
    approvals: Arc<Approvals>,
    on_change: OnChange,
}

/// Код ошибки — как в приложении (`CONNECTION_FAILED`, `TIMEOUT`…). Тексты ошибок настроек и
/// файлов содержат пути к сертификатам (спец. §6) — LLM получает вместо них общий текст.
fn to_backend(e: AppError) -> BackendError {
    let code = serde_json::to_value(e.code)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_else(|| "INTERNAL".into());
    let message = match e.code {
        ErrorCode::InvalidInput | ErrorCode::Internal => {
            "The settings or certificate files of this connection cannot be used; ask the user to check the connection in Glafira".to_string()
        }
        _ => e.message,
    };
    BackendError::new(code, message)
}

impl AppBackend {
    pub fn new(state: AppState, approvals: Arc<Approvals>, on_change: OnChange) -> Self {
        Self {
            state,
            approvals,
            on_change,
        }
    }

    /// Подключения, которые видит LLM.
    pub fn connections(&self) -> Vec<m::Connection> {
        let store = &self.state.connections;
        store
            .list()
            .into_iter()
            .filter(|p| !p.llm_access.is_off())
            .map(|p| m::Connection {
                folder_path: p
                    .folder_id
                    .as_deref()
                    .map(|f| store.folder_path(f))
                    .unwrap_or_default(),
                environment: serde_json::to_value(p.environment)
                    .ok()
                    .and_then(|v| v.as_str().map(String::from))
                    .unwrap_or_default(),
                id: p.id,
                name: p.name,
                read_only: p.read_only,
            })
            .collect()
    }

    /// Нет подключения и доступ выключен — одна и та же ошибка.
    fn allowed(&self, id: &str) -> Result<(), BackendError> {
        match self.state.connections.get(id) {
            Ok(p) if !p.llm_access.is_off() => Ok(()),
            _ => Err(BackendError::not_allowed()),
        }
    }

    /// Блокирующая операция на pooled-handles (как в командах интерфейса).
    async fn on<T: Send + 'static>(
        &self,
        id: String,
        op: impl FnOnce(&KafkaHandles) -> AppResult<T> + Send + 'static,
    ) -> Result<T, BackendError> {
        let st = self.state.clone();
        blocking(move || st.with_handles(&id, op))
            .await
            .map_err(to_backend)
    }

    async fn read_impl(&self, op: ReadOp) -> Result<ReadOutput, BackendError> {
        if let Some(id) = op.connection_id() {
            self.allowed(id)?;
        }
        Ok(match op {
            ReadOp::ListConnections => ReadOutput::Connections(self.connections()),
            ReadOp::ClusterOverview { connection_id } => ReadOutput::Cluster(map_cluster(
                self.on(connection_id, cluster::cluster_overview).await?,
            )),
            ReadOp::BrokerConfig {
                connection_id,
                broker_id,
            } => ReadOutput::Configs(map_configs(
                pooled_configs(
                    &self.state,
                    connection_id,
                    ResourceSpecifier::Broker(broker_id),
                )
                .await
                .map_err(to_backend)?,
            )),
            ReadOp::ListTopics { connection_id } => ReadOutput::Topics(
                self.on(connection_id, topics::list_topics)
                    .await?
                    .into_iter()
                    .map(|t| m::TopicSummary {
                        name: t.name,
                        internal: t.internal,
                        partitions: t.partition_count,
                        replication_factor: t.replication_factor,
                        under_replicated: t.under_replicated,
                    })
                    .collect(),
            ),
            ReadOp::DescribeTopic {
                connection_id,
                topic,
            } => ReadOutput::Topic(map_topic(
                self.on(connection_id, move |h| topics::topic_details(h, &topic))
                    .await?,
            )),
            ReadOp::TopicConfigs {
                connection_id,
                topic,
            } => ReadOutput::Configs(map_configs(
                pooled_configs(&self.state, connection_id, ResourceSpecifier::Topic(&topic))
                    .await
                    .map_err(to_backend)?,
            )),
            ReadOp::TopicsMessageCounts {
                connection_id,
                topics: names,
            } => {
                let wanted = names.clone();
                let stats = self
                    .on(connection_id, move |h| topics::topic_stats(h, &wanted))
                    .await?;
                ReadOutput::MessageCounts(
                    names
                        .into_iter()
                        .map(|n| match stats.iter().find(|s| s.name == n) {
                            Some(s) => m::MessageCount {
                                name: n,
                                message_count: s.message_count,
                                error: None,
                            },
                            None => m::MessageCount {
                                name: n,
                                message_count: None,
                                error: Some("Topic not found".into()),
                            },
                        })
                        .collect(),
                )
            }
            ReadOp::ListGroups { connection_id } => {
                let l = self.on(connection_id, group_ops::list_groups).await?;
                ReadOutput::Groups(m::GroupList {
                    groups: l
                        .groups
                        .into_iter()
                        .map(|g| m::GroupSummary {
                            group_id: g.group_id,
                            state: g.state,
                            is_simple: g.is_simple,
                        })
                        .collect(),
                    errors: l.errors.into_iter().map(|e| e.text).collect(),
                })
            }
            ReadOp::DescribeGroup {
                connection_id,
                group_id,
            } => ReadOutput::Group(map_group(
                self.on(connection_id, move |h| {
                    group_ops::describe_group(h, &group_id)
                })
                .await?,
            )),
            ReadOp::GroupOffsets {
                connection_id,
                group_id,
            } => ReadOutput::Offsets(
                self.on(connection_id, move |h| {
                    group_ops::group_offsets(h, &group_id)
                })
                .await?
                .into_iter()
                .map(|o| m::GroupOffset {
                    topic: o.topic,
                    partition: o.partition,
                    committed: o.committed,
                    log_end: o.log_end,
                    lag: o.lag,
                    member_client_id: o.member_client_id,
                    error: o.error.map(|e| e.text),
                })
                .collect(),
            ),
            ReadOp::GroupsLag {
                connection_id,
                group_ids,
            } => ReadOutput::Lags(
                self.on(connection_id, move |h| group_ops::groups_lag(h, &group_ids))
                    .await?
                    .into_iter()
                    .map(|l| m::GroupLag {
                        group_id: l.group_id,
                        total_lag: l.total_lag,
                        topics: l.topics,
                        error: l.error.map(|e| e.text),
                    })
                    .collect(),
            ),
            ReadOp::TopicGroups {
                connection_id,
                topic,
            } => {
                let g = self
                    .on(connection_id, move |h| group_ops::topic_groups(h, &topic))
                    .await?;
                ReadOutput::TopicGroups(m::TopicGroups {
                    groups: g
                        .groups
                        .into_iter()
                        .map(|t| m::TopicGroup {
                            group_id: t.group_id,
                            state: t.state,
                            lag: t.lag,
                            partitions: t.partitions,
                        })
                        .collect(),
                    incomplete: g.incomplete,
                })
            }
        })
    }
}

fn map_cluster(o: cluster::ClusterOverview) -> m::ClusterOverview {
    m::ClusterOverview {
        cluster_id: o.cluster_id,
        controller_id: o.controller_id,
        brokers: o
            .brokers
            .into_iter()
            .map(|b| m::Broker {
                id: b.id,
                host: b.host,
                port: b.port,
            })
            .collect(),
        topic_count: o.topic_count,
        internal_topic_count: o.internal_topic_count,
        partition_count: o.partition_count,
        under_replicated_partitions: o.under_replicated_partitions,
        offline_partitions: o.offline_partitions,
    }
}

fn map_configs(v: Vec<crate::kafka::configs::ConfigEntryDto>) -> Vec<m::ConfigEntry> {
    v.into_iter()
        .map(|c| m::ConfigEntry {
            name: c.name,
            value: c.value,
            source: c.source,
            is_default: c.is_default,
            is_read_only: c.is_read_only,
            is_sensitive: c.is_sensitive,
            hidden: false,
        })
        .collect()
}

fn map_topic(t: topics::TopicDetails) -> m::TopicDetails {
    m::TopicDetails {
        name: t.name,
        internal: t.internal,
        replication_factor: t.replication_factor,
        message_count: t.message_count,
        partitions: t
            .partitions
            .into_iter()
            .map(|p| m::Partition {
                id: p.id,
                leader: p.leader,
                replicas: p.replicas,
                isr: p.isr,
                low: p.low,
                high: p.high,
                message_count: p.message_count,
                under_replicated: p.under_replicated,
                error: p.offsets_error.map(|e| e.text),
            })
            .collect(),
    }
}

fn map_group(g: crate::kafka::groups::GroupDescription) -> m::GroupDescription {
    let parts = |v: Vec<crate::kafka::groups::TopicPartitionRef>| {
        v.into_iter()
            .map(|p| m::TopicPartition {
                topic: p.topic,
                partition: p.partition,
            })
            .collect::<Vec<_>>()
    };
    m::GroupDescription {
        group_id: g.group_id,
        state: g.state,
        group_type: g.group_type,
        assignor: g.assignor,
        coordinator: g.coordinator.map(|c| m::Broker {
            id: c.id,
            host: c.host,
            port: i32::from(c.port),
        }),
        members: g
            .members
            .into_iter()
            .map(|mm| m::Member {
                member_id: mm.member_id,
                client_id: mm.client_id,
                host: mm.host,
                group_instance_id: mm.group_instance_id,
                assignment: parts(mm.assignment),
                target_assignment: mm.target_assignment.map(parts),
            })
            .collect(),
        error: g.error,
        not_found: g.not_found,
    }
}

impl Backend for AppBackend {
    fn access(&self, session: &SessionView) -> SessionAccess {
        if !self.state.policy.status.mcp_enabled {
            return SessionAccess::Disabled;
        }
        let (access, new_pending) = self.approvals.access(session, glafira_mcp::audit::now_ms());
        if new_pending {
            (self.on_change)(true);
        }
        access
    }

    fn read(&self, op: ReadOp) -> BoxFuture<'_, Result<ReadOutput, BackendError>> {
        Box::pin(self.read_impl(op))
    }

    fn connection_name(&self, id: &str) -> Option<String> {
        self.state.connections.get(id).ok().map(|p| p.name)
    }

    fn session_closed(&self, session_id: &str) {
        self.approvals.closed(session_id);
    }

    fn sessions_changed(&self) {
        (self.on_change)(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::{LlmAccess, sample_profile};

    fn backend() -> (tempfile::TempDir, AppBackend) {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        let mut open = sample_profile("open");
        open.name = "dev".into();
        open.llm_access = LlmAccess::Metadata;
        open.bootstrap_servers = "127.0.0.1:1".into();
        st.connections.upsert(open).unwrap();
        st.connections.upsert(sample_profile("closed")).unwrap();
        let approvals = Arc::new(Approvals::new(st.settings.clone()));
        (dir, AppBackend::new(st, approvals, Arc::new(|_| {})))
    }

    #[test]
    fn only_open_connections_are_listed() {
        let (_d, b) = backend();
        let list = b.connections();
        assert_eq!(list.len(), 1);
        assert_eq!(
            (list[0].id.as_str(), list[0].environment.as_str()),
            ("open", "dev")
        );
        assert_eq!(b.connection_name("open").as_deref(), Some("dev"));
    }

    #[tokio::test]
    async fn closed_and_unknown_connections_are_not_allowed_before_network() {
        let (_d, b) = backend();
        for id in ["closed", "missing"] {
            let e = b
                .read(ReadOp::ClusterOverview {
                    connection_id: id.into(),
                })
                .await
                .unwrap_err();
            assert_eq!(e, BackendError::not_allowed(), "{id}");
        }
    }

    #[tokio::test]
    async fn kafka_errors_keep_their_code() {
        let (_d, b) = backend();
        let e = b
            .read(ReadOp::ListTopics {
                connection_id: "open".into(),
            })
            .await
            .unwrap_err();
        assert!(
            ["CONNECTION_FAILED", "TIMEOUT"].contains(&e.code.as_str()),
            "{e:?}"
        );
    }

    #[test]
    fn policy_disables_every_session() {
        let (_d, b) = backend();
        let mut st = b.state.clone();
        let mut policy = (*st.policy).clone();
        policy.status.mcp_enabled = false;
        st.policy = Arc::new(policy);
        let b = AppBackend::new(st, b.approvals.clone(), Arc::new(|_| {}));
        let v = SessionView {
            id: "s1".into(),
            client: None,
        };
        assert_eq!(b.access(&v), SessionAccess::Disabled);
    }

    #[tokio::test]
    async fn certificate_paths_never_reach_the_llm() {
        let (_d, b) = backend();
        let mut p = sample_profile("tls");
        p.llm_access = LlmAccess::Metadata;
        p.security = crate::storage::connections::SecurityConfig::Ssl {
            tls: crate::storage::connections::TlsConfig {
                ca_location: Some("/nonexistent/secret-path/ca.pem".into()),
                ca_format: crate::storage::connections::CertFormat::Pem,
                verify_hostname: true,
                client_cert: None,
            },
        };
        b.state.connections.upsert(p).unwrap();
        let e = b
            .read(ReadOp::ListTopics {
                connection_id: "tls".into(),
            })
            .await
            .unwrap_err();
        assert_eq!(e.code, "INVALID_INPUT", "{e:?}");
        assert!(!e.message.contains("secret-path"), "{e:?}");
    }
}
