//! Реализация `glafira_mcp::Backend` (спец. G1 §2): существующие функции чтения и проверка
//! `llmAccess` подключения при каждой операции. Изменяющих функций здесь нет и быть не должно.

use std::sync::Arc;

use glafira_mcp::health as mh;
use glafira_mcp::model as m;
use glafira_mcp::{
    Backend, BackendError, BoxFuture, ReadOp, ReadOutput, SessionAccess, SessionView,
};
use rdkafka::admin::ResourceSpecifier;

use super::approvals::Approvals;
use crate::commands::{blocking, pooled_configs};
use crate::error::{AppError, AppResult, ErrorCode};
use crate::health::model::{Finding, Severity, Target, TargetKind};
use crate::health::service::now_ms;
use crate::health::store::{
    Annotated, AnnotationStatus, Author, CheckRun, NewAnnotation, Removed, Trigger,
};
use crate::health::{rules, texts};
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
/// Тексты ошибок Kafka (`errors.kafka.*`) — фиксированный английский без путей и адресов:
/// они остаются, иначе неклассифицированная ошибка брокера (`errors.kafka.generic`) выглядела бы
/// как сломанные сертификаты.
fn to_backend(e: AppError) -> BackendError {
    let code = serde_json::to_value(e.code)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_else(|| "INTERNAL".into());
    let message = match e.code {
        _ if e.key.starts_with("errors.kafka.") => e.message,
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

impl AppBackend {
    /// Автор замечаний и проверок: имя из диалога подтверждения.
    fn author(&self, session: &SessionView) -> Author {
        Author {
            client: self.approvals.shown_name(&session.id),
            session_id: session.id.clone(),
        }
    }

    /// Объект замечания есть в кластере (защита от придуманных имён).
    async fn target_exists(&self, connection_id: &str, t: &mh::Target) -> Result<(), BackendError> {
        let name = t.name.clone().unwrap_or_default();
        let found = match t.kind {
            mh::TargetKind::Cluster => true,
            mh::TargetKind::Broker => {
                let id: i32 = name.trim().parse().map_err(|_| {
                    BackendError::invalid_argument("target.name of a broker is its numeric id")
                })?;
                let md = self
                    .on(connection_id.into(), cluster::cluster_metadata)
                    .await?;
                md.brokers.iter().any(|b| b.id == id)
            }
            mh::TargetKind::Topic => {
                let md = self
                    .on(connection_id.into(), cluster::cluster_metadata)
                    .await?;
                md.topics.iter().any(|x| x.name == name)
            }
            mh::TargetKind::Group => {
                let l = self
                    .on(connection_id.into(), group_ops::list_groups)
                    .await?;
                l.groups.iter().any(|g| g.group_id == name)
            }
        };
        if found {
            Ok(())
        } else {
            Err(BackendError::not_found(format!(
                "There is no {} {name} in this cluster",
                kind_name(t.kind)
            )))
        }
    }

    async fn health_impl(&self, op: mh::HealthOp) -> Result<mh::HealthOutput, BackendError> {
        self.allowed(op.connection_id())?;
        let st = self.state.clone();
        match op {
            mh::HealthOp::Check {
                connection_id,
                max_age_ms,
                run_id,
                session,
            } => {
                let (run, reused) = match run_id {
                    Some(id) => {
                        let (st2, cid) = (st.clone(), connection_id.clone());
                        let last = crate::commands::blocking(move || {
                            Ok(st2.health.store.load(&cid, now_ms())?.last_check)
                        })
                        .await
                        .map_err(to_backend)?;
                        match last {
                            Some(r) if r.run_id == id => (std::sync::Arc::new(r), true),
                            _ => {
                                return Err(BackendError::not_found(
                                    "This check was replaced by a newer one; call health_check without runId",
                                ));
                            }
                        }
                    }
                    None => {
                        let trigger = Trigger::Mcp {
                            client: self.author(&session).client,
                        };
                        st.health
                            .clone()
                            .latest_or_run(&st, &connection_id, trigger, max_age_ms)
                            .await
                            .map_err(to_backend)?
                    }
                };
                let ignored = st
                    .health
                    .store
                    .load(&connection_id, now_ms())
                    .map_err(to_backend)?
                    .ignored;
                Ok(mh::HealthOutput::Run(map_run(&run, reused, |k| {
                    ignored.iter().any(|i| i.key == k)
                })))
            }
            mh::HealthOp::Annotate {
                connection_id,
                session,
                note,
            } => {
                if let Some(r) = &note.related_rule_id
                    && !rules::rule_ids().any(|id| id == r)
                {
                    return Err(BackendError::invalid_argument(format!(
                        "relatedRuleId {r} is not a health_check rule"
                    )));
                }
                self.target_exists(&connection_id, &note.target).await?;
                let new = NewAnnotation {
                    target: to_target(&note.target),
                    severity: to_severity(note.severity),
                    title: note.title,
                    body: note.body,
                    related_rule_id: note.related_rule_id,
                    author: self.author(&session),
                };
                let id = connection_id.clone();
                let st2 = st.clone();
                let r = crate::commands::blocking(move || {
                    st2.health.store.annotate(&id, new, now_ms())
                })
                .await
                .map_err(to_backend)?;
                let (annotation_id, created) = match r {
                    Annotated::Created(id) => (id, true),
                    Annotated::Updated(id) => (id, false),
                    Annotated::LimitReached => {
                        return Err(BackendError::new(
                            "limit_reached",
                            "This connection already has 200 open notes; ask the user to close some in Glafira",
                        ));
                    }
                };
                st.health.changed(&connection_id);
                Ok(mh::HealthOutput::Annotated {
                    annotation_id,
                    created,
                })
            }
            mh::HealthOp::ListAnnotations {
                connection_id,
                status,
            } => {
                let f = st
                    .health
                    .store
                    .load(&connection_id, now_ms())
                    .map_err(to_backend)?;
                let items = f
                    .annotations
                    .into_iter()
                    .filter(|a| match status {
                        mh::StatusFilter::Open => a.status == AnnotationStatus::Open,
                        mh::StatusFilter::Closed => a.status != AnnotationStatus::Open,
                        mh::StatusFilter::All => true,
                    })
                    .map(|a| mh::AnnotationView {
                        id: a.id,
                        target: from_target(&a.target),
                        severity: from_severity(a.severity),
                        title: a.title,
                        body: a.body,
                        related_rule_id: a.related_rule_id,
                        client: a.client,
                        status: status_name(a.status).into(),
                        created_at_ms: a.created_at_ms,
                        updated_at_ms: a.updated_at_ms,
                    })
                    .collect();
                Ok(mh::HealthOutput::Annotations(items))
            }
            mh::HealthOp::RemoveAnnotation {
                connection_id,
                session,
                annotation_id,
            } => {
                let author = self.author(&session);
                let id = connection_id.clone();
                let st2 = st.clone();
                let r = crate::commands::blocking(move || {
                    st2.health
                        .store
                        .remove_own(&id, &annotation_id, &author, now_ms())
                })
                .await
                .map_err(to_backend)?;
                match r {
                    Removed::Removed => {
                        st.health.changed(&connection_id);
                        Ok(mh::HealthOutput::Removed)
                    }
                    Removed::NotFound => Err(BackendError::not_found(
                        "The note was deleted or has expired",
                    )),
                    Removed::NotOwner => Err(BackendError::new(
                        "not_allowed",
                        "Only notes created by this client can be removed",
                    )),
                }
            }
        }
    }
}

fn kind_name(k: mh::TargetKind) -> &'static str {
    match k {
        mh::TargetKind::Cluster => "cluster",
        mh::TargetKind::Broker => "broker",
        mh::TargetKind::Topic => "topic",
        mh::TargetKind::Group => "group",
    }
}

fn to_target(t: &mh::Target) -> Target {
    let kind = match t.kind {
        mh::TargetKind::Cluster => TargetKind::Cluster,
        mh::TargetKind::Broker => TargetKind::Broker,
        mh::TargetKind::Topic => TargetKind::Topic,
        mh::TargetKind::Group => TargetKind::Group,
    };
    Target {
        kind,
        name: match kind {
            TargetKind::Cluster => None,
            _ => t.name.as_ref().map(|n| n.trim().to_string()),
        },
    }
}

fn from_target(t: &Target) -> mh::Target {
    mh::Target {
        kind: match t.kind {
            TargetKind::Cluster => mh::TargetKind::Cluster,
            TargetKind::Broker => mh::TargetKind::Broker,
            TargetKind::Topic => mh::TargetKind::Topic,
            TargetKind::Group => mh::TargetKind::Group,
        },
        name: t.name.clone(),
    }
}

fn to_severity(s: mh::Severity) -> Severity {
    match s {
        mh::Severity::Info => Severity::Info,
        mh::Severity::Warning => Severity::Warning,
        mh::Severity::Critical => Severity::Critical,
    }
}

fn from_severity(s: Severity) -> mh::Severity {
    match s {
        Severity::Info => mh::Severity::Info,
        Severity::Warning => mh::Severity::Warning,
        Severity::Critical => mh::Severity::Critical,
    }
}

fn status_name(s: AnnotationStatus) -> &'static str {
    match s {
        AnnotationStatus::Open => "open",
        AnnotationStatus::Resolved => "resolved",
        AnnotationStatus::Dismissed => "dismissed",
    }
}

fn map_finding(f: &Finding, ignored: bool) -> mh::HealthFinding {
    let (title, details) = texts::english(f);
    mh::HealthFinding {
        key: f.key.clone(),
        rule_id: f.rule_id.clone(),
        severity: from_severity(f.severity),
        target: from_target(&f.target),
        title,
        details,
        params: f.params.clone(),
        ignored,
    }
}

fn map_run(r: &CheckRun, reused: bool, ignored: impl Fn(&str) -> bool) -> mh::HealthRun {
    mh::HealthRun {
        run_id: r.run_id.clone(),
        started_at_ms: r.started_at_ms,
        duration_ms: r.duration_ms,
        trigger: serde_json::to_value(&r.trigger).unwrap_or_default(),
        environment: serde_json::to_value(r.environment)
            .ok()
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_default(),
        incomplete: r.incomplete,
        reused,
        scope: mh::HealthScope {
            brokers: r.scope.brokers,
            topics_checked: r.scope.topics_checked,
            topics_total: r.scope.topics_total,
            groups_checked: r.scope.groups_checked,
            groups_total: r.scope.groups_total,
        },
        findings: r
            .findings
            .iter()
            .map(|f| map_finding(f, ignored(&f.key)))
            .collect(),
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

    fn health(&self, op: mh::HealthOp) -> BoxFuture<'_, Result<mh::HealthOutput, BackendError>> {
        Box::pin(self.health_impl(op))
    }

    fn pro_active(&self) -> bool {
        self.state.license.require_pro().is_ok()
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

    #[test]
    fn unclassified_kafka_error_keeps_its_text() {
        // Опечатка в имени топика у get_topic_configs: AdminOp(UnknownTopicOrPartition) → Internal.
        let e = to_backend(crate::error::from_kafka(
            &rdkafka::error::KafkaError::AdminOp(
                rdkafka::types::RDKafkaErrorCode::UnknownTopicOrPartition,
            ),
            None,
        ));
        assert_eq!(
            (e.code.as_str(), e.message.as_str()),
            ("INTERNAL", "Kafka error")
        );
        let e = to_backend(app_err!(
            ErrorCode::Internal,
            "errors.kafka.emptyResponse",
            "Empty {{request}} response",
            request = "DescribeConfigs"
        ));
        assert_eq!(e.message, "Empty DescribeConfigs response");
    }

    #[test]
    fn file_errors_hide_the_path() {
        let e = to_backend(
            crate::storage::fs::atomic_write(
                std::path::Path::new("/nonexistent/secret-path/ca.pem/x"),
                b"x",
            )
            .unwrap_err(),
        );
        assert_eq!(e.code, "INTERNAL", "{e:?}");
        assert!(!e.message.contains("secret-path"), "{e:?}");
        assert!(e.message.contains("certificate files"), "{e:?}");
    }

    fn view(id: &str, name: Option<&str>) -> SessionView {
        SessionView {
            id: id.into(),
            client: name.map(|n| glafira_mcp::ClientIdent {
                name: n.into(),
                version: "1".into(),
            }),
        }
    }

    fn cluster_note(title: &str, related: Option<&str>) -> mh::NewNote {
        mh::NewNote {
            target: mh::Target {
                kind: mh::TargetKind::Cluster,
                name: Some("ignored".into()),
            },
            severity: mh::Severity::Warning,
            title: title.into(),
            body: "body".into(),
            related_rule_id: related.map(String::from),
        }
    }

    async fn annotate(
        b: &AppBackend,
        session: &SessionView,
        title: &str,
    ) -> Result<mh::HealthOutput, BackendError> {
        b.health(mh::HealthOp::Annotate {
            connection_id: "open".into(),
            session: session.clone(),
            note: cluster_note(title, Some("cluster.singleBroker")),
        })
        .await
    }

    #[tokio::test]
    async fn health_ops_need_llm_access() {
        let (_d, b) = backend();
        let e = b
            .health(mh::HealthOp::ListAnnotations {
                connection_id: "closed".into(),
                status: mh::StatusFilter::All,
            })
            .await
            .unwrap_err();
        assert_eq!(e.code, "not_allowed");
        assert!(!b.pro_active());
    }

    #[tokio::test]
    async fn notes_are_written_listed_and_removed_by_their_author() {
        let (_d, b) = backend();
        let claude = view("s1", Some("claude-ai"));
        let _ = b.approvals.access(&claude, 0);
        let e = b
            .health(mh::HealthOp::Annotate {
                connection_id: "open".into(),
                session: claude.clone(),
                note: cluster_note("t", Some("no.such.rule")),
            })
            .await
            .unwrap_err();
        assert_eq!(e.code, "invalid_argument");
        let mh::HealthOutput::Annotated {
            annotation_id,
            created,
        } = annotate(&b, &claude, "Single broker").await.unwrap()
        else {
            panic!()
        };
        assert!(created);
        let again = annotate(&b, &claude, "single broker").await.unwrap();
        assert_eq!(
            again,
            mh::HealthOutput::Annotated {
                annotation_id: annotation_id.clone(),
                created: false
            }
        );
        let mh::HealthOutput::Annotations(items) = b
            .health(mh::HealthOp::ListAnnotations {
                connection_id: "open".into(),
                status: mh::StatusFilter::Open,
            })
            .await
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].client.as_deref(), Some("claude-ai"));
        // Имя у кластера отбрасывается.
        assert_eq!(items[0].target.name, None);
        let other = view("s2", Some("cursor"));
        let _ = b.approvals.access(&other, 0);
        let remove = |s: &SessionView| {
            b.health(mh::HealthOp::RemoveAnnotation {
                connection_id: "open".into(),
                session: s.clone(),
                annotation_id: annotation_id.clone(),
            })
        };
        assert_eq!(remove(&other).await.unwrap_err().code, "not_allowed");
        assert_eq!(remove(&claude).await.unwrap(), mh::HealthOutput::Removed);
        assert_eq!(remove(&claude).await.unwrap_err().code, "not_found");
    }

    #[tokio::test]
    async fn check_reuses_the_last_run_and_marks_ignored() {
        let (_d, b) = backend();
        let run = CheckRun {
            run_id: "r1".into(),
            started_at_ms: now_ms(),
            duration_ms: 5,
            trigger: Trigger::Ui,
            environment: crate::storage::connections::Environment::Prod,
            incomplete: false,
            scope: Default::default(),
            findings: vec![Finding::new(
                "cluster.singleBroker",
                Severity::Critical,
                Target::cluster(),
                None,
                serde_json::json!({}),
            )],
            group_lag: Default::default(),
        };
        b.state
            .health
            .store
            .save_check("open", run, now_ms())
            .unwrap();
        b.state
            .health
            .store
            .set_ignored("open", "cluster.singleBroker:cluster:", true, now_ms())
            .unwrap();
        let check = |run_id: Option<&str>| {
            b.health(mh::HealthOp::Check {
                connection_id: "open".into(),
                max_age_ms: 300_000,
                run_id: run_id.map(String::from),
                session: view("s1", None),
            })
        };
        let mh::HealthOutput::Run(r) = check(None).await.unwrap() else {
            panic!()
        };
        assert!(r.reused);
        assert_eq!((r.run_id.as_str(), r.environment.as_str()), ("r1", "prod"));
        assert!(r.findings[0].ignored);
        assert_eq!(r.findings[0].title, "Single-broker cluster");
        assert!(matches!(
            check(Some("r1")).await.unwrap(),
            mh::HealthOutput::Run(_)
        ));
        assert_eq!(check(Some("r0")).await.unwrap_err().code, "not_found");
    }
}
