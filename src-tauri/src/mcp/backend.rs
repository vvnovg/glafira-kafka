//! Реализация `glafira_mcp::Backend` (спец. G1 §2): существующие функции чтения и проверка
//! `llmAccess` подключения при каждой операции. Функций, меняющих кластер, здесь нет и быть не
//! должно; замечания и предложения LLM пишутся только в файл Glafira — предложение применяет
//! человек формой (спец. G3 §6).

use std::sync::Arc;

use glafira_mcp::health as mh;
use glafira_mcp::mask::MaskConfig;
use glafira_mcp::messages as mm;
use glafira_mcp::model as m;
use glafira_mcp::proposals as mp;
use glafira_mcp::{
    Backend, BackendError, BoxFuture, ReadOp, ReadOutput, SessionAccess, SessionView,
};
use rdkafka::admin::ResourceSpecifier;
use serde::{Serialize, de::DeserializeOwned};

use super::approvals::Approvals;
use crate::commands::{blocking, pooled_configs};
use crate::error::{AppError, AppResult, ErrorCode};
use crate::health::model::{Finding, Severity, Target, TargetKind};
use crate::health::proposals::{
    Change, Facts, MAX_OPEN_PROPOSALS, MAX_RELATED_KEY_CHARS, Proposal, ProposalError,
    ProposalStatus, TopicFact, validate,
};
use crate::health::service::now_ms;
use crate::health::store::{
    Annotated, AnnotationStatus, Author, CheckRun, MAX_BODY_CHARS, MAX_TITLE_CHARS, NewAnnotation,
    NewProposal, Proposed, Removed, Trigger, Withdrawn, sanitize,
};
use crate::health::{rules, texts};
use crate::kafka::pool::KafkaHandles;
use crate::kafka::{cluster, group_ops, topics};
use crate::state::AppState;
use crate::storage::connections::LlmAccess;

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

/// Текст для LLM при сбое файла `health/<id>.json` (спец. G2 §9): путь к файлу не раскрывается.
const HEALTH_FILE_FAILED: &str = "Glafira could not read or save its data for this connection";
/// Текст для LLM, когда фоновая задача Glafira не завершилась (паника blocking-потока).
const BACKGROUND_FAILED: &str = "A background task in Glafira failed; retry the call";

/// Ключи ошибок файла Glafira: диск, разбор JSON, атомарная запись и версия файла.
const FILE_ERROR_KEYS: [&str; 2] = ["errors.io", "errors.json"];
const FILE_ERROR_PREFIX: &str = "errors.storage.";
const TASK_PANICKED_KEY: &str = "errors.app.taskPanicked";

/// Ошибки операций G2 и G3: сбой файла Glafira (`errors.io`, `errors.json`, `errors.storage.*`) и
/// паника фоновой задачи (`errors.app.taskPanicked`) — код `internal` (строчными, как у других
/// ошибок шлюза) с общим текстом, а не «настройки или сертификаты подключения». Различаем по
/// ключу, а не по коду: проверка (`latest_or_run`) возвращает и ошибки Kafka и настроек
/// подключения — они, как и «не найдено», остаются как в G1.
fn health_to_backend(e: AppError) -> BackendError {
    if FILE_ERROR_KEYS.contains(&e.key.as_str()) || e.key.starts_with(FILE_ERROR_PREFIX) {
        BackendError::new("internal", HEALTH_FILE_FAILED)
    } else if e.key == TASK_PANICKED_KEY {
        BackendError::new("internal", BACKGROUND_FAILED)
    } else {
        to_backend(e)
    }
}

impl AppBackend {
    pub fn new(state: AppState, approvals: Arc<Approvals>, on_change: OnChange) -> Self {
        Self {
            state,
            approvals,
            on_change,
        }
    }

    /// Чтение сообщений разрешено политикой (спец. G4 §9).
    fn messages_allowed(&self) -> bool {
        self.state.policy.status.messages_allowed()
    }

    /// Подключения, которые видит LLM.
    pub fn connections(&self) -> Vec<m::Connection> {
        let store = &self.state.connections;
        let messages_allowed = self.messages_allowed();
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
                llm_access: p.llm_access.effective(messages_allowed).as_str().into(),
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

/// Ошибки чтения сообщений (спец. G4 §4, §7): неверный фильтр и партиция — `invalid_argument`,
/// нет топика — `not_found`, нет права на `__consumer_offsets` — с подсказкой; остальное — как в G1.
fn scan_to_backend(e: AppError, commits: bool) -> BackendError {
    if e.key.starts_with("errors.browse.bad") || e.key == "errors.topics.noSuchPartition" {
        BackendError::invalid_argument(e.message)
    } else if e.code == ErrorCode::NotFound {
        BackendError::not_found(e.message)
    } else if e.key == TASK_PANICKED_KEY {
        BackendError::new("internal", BACKGROUND_FAILED)
    } else if commits && e.code == ErrorCode::NotAuthorized {
        let mut b = to_backend(e);
        b.message = format!(
            "{}. Reading __consumer_offsets requires the Read permission on this internal topic",
            b.message.trim_end_matches('.')
        );
        b
    } else {
        to_backend(e)
    }
}

impl AppBackend {
    /// Чтение сообщений: доступ (спец. G4 §4), затем просмотр на blocking-потоке.
    async fn messages_impl(
        &self,
        op: mm::MessagesOp,
        ctx: mm::ScanCtx,
    ) -> Result<mm::MessagesOutput, BackendError> {
        let profile = self
            .state
            .connections
            .get(op.connection_id())
            .ok()
            .filter(|p| !p.llm_access.is_off())
            .ok_or_else(BackendError::not_allowed)?;
        if !self.messages_allowed() {
            return Err(BackendError::new(
                "messages_disabled",
                "Reading messages is disabled by the organization's policy",
            ));
        }
        if profile.llm_access != LlmAccess::Messages {
            return Err(BackendError::new(
                "messages_not_allowed",
                "LLM access to this connection is limited to metadata. Ask the user to switch it to \"Metadata and messages\" in the connection settings in Glafira",
            ));
        }
        let commits = matches!(op, mm::MessagesOp::CommitHistory { .. });
        let mask = MaskConfig {
            fields: profile.llm_mask_fields.clone(),
        };
        let st = self.state.clone();
        let body = blocking(move || super::messages::scan(&profile, st.secrets.as_ref(), op, &ctx))
            .await
            .map_err(|e| scan_to_backend(e, commits))?;
        Ok(mm::MessagesOutput {
            mask,
            utc_offset_minutes: super::messages::local_utc_offset_minutes(),
            body,
        })
    }

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
                let id = broker_id(&name).ok_or_else(|| {
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
        // Поколение — до проверки доступа: запись в подключение, удалённое за это время, отклоняется.
        let generation = self.state.health.generation(op.connection_id());
        self.allowed(op.connection_id())?;
        let st = self.state.clone();
        match op {
            mh::HealthOp::Check {
                connection_id,
                max_age_ms,
                run_id,
                session,
            } => {
                // Файл подключения читается одним blocking-вызовом на путь.
                let (st2, cid) = (st.clone(), connection_id.clone());
                let (run, reused, ignored) = match run_id {
                    Some(id) => {
                        let file = blocking(move || st2.health.store.load(&cid, now_ms()))
                            .await
                            .map_err(health_to_backend)?;
                        match file.last_check {
                            Some(r) if r.run_id == id => (Arc::new(r), true, file.ignored),
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
                        let (run, reused) = st
                            .health
                            .clone()
                            .latest_or_run(&st, &connection_id, trigger, max_age_ms)
                            .await
                            .map_err(health_to_backend)?;
                        let ignored =
                            blocking(move || Ok(st2.health.store.load(&cid, now_ms())?.ignored))
                                .await
                                .map_err(health_to_backend)?;
                        (run, reused, ignored)
                    }
                };
                Ok(mh::HealthOutput::Run(map_run(&run, reused, |k| {
                    ignored.iter().any(|i| i.key == k)
                })))
            }
            mh::HealthOp::Annotate {
                connection_id,
                session,
                note,
            } => {
                check_note_texts(note.related_rule_id.as_deref(), &note.title)?;
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
                let r = blocking(move || {
                    st2.health
                        .write(&id, generation, |s| s.annotate(&id, new, now_ms()))
                })
                .await
                .map_err(health_to_backend)?;
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
                let f = blocking(move || st.health.store.load(&connection_id, now_ms()))
                    .await
                    .map_err(health_to_backend)?;
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
                let r = blocking(move || {
                    st2.health.write(&id, generation, |s| {
                        s.remove_own(&id, &annotation_id, &author, now_ms())
                    })
                })
                .await
                .map_err(health_to_backend)?;
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

impl AppBackend {
    /// Факты для проверки предложения (спец. G3 §5): метаданные всегда, ключи конфигов топика —
    /// для `alterTopicConfigs` по существующему топику, список групп — для `resetOffsets`.
    async fn facts(&self, connection_id: &str, change: &Change) -> Result<Facts, BackendError> {
        let md = self
            .on(connection_id.into(), cluster::cluster_metadata)
            .await?;
        let mut facts = Facts {
            brokers: md.brokers.len(),
            topics: md
                .topics
                .iter()
                .map(|t| TopicFact {
                    name: t.name.clone(),
                    partitions: t.partitions.len(),
                    internal: t.internal(),
                })
                .collect(),
            config_keys: None,
            groups: None,
        };
        match change {
            Change::AlterTopicConfigs { topic, .. }
                if facts.topics.iter().any(|t| &t.name == topic && !t.internal) =>
            {
                let entries = pooled_configs(
                    &self.state,
                    connection_id.into(),
                    ResourceSpecifier::Topic(topic),
                )
                .await
                .map_err(to_backend)?;
                facts.config_keys = Some(entries.into_iter().map(|e| e.name).collect());
            }
            Change::ResetOffsets { .. } => {
                let l = self
                    .on(connection_id.into(), group_ops::list_groups)
                    .await?;
                facts.groups = Some(l.groups.into_iter().map(|g| g.group_id).collect());
            }
            _ => {}
        }
        Ok(facts)
    }

    async fn proposals_impl(&self, op: mp::ProposalOp) -> Result<mp::ProposalOutput, BackendError> {
        // Поколение — до проверки доступа: запись в подключение, удалённое за это время, отклоняется.
        let generation = self.state.health.generation(op.connection_id());
        self.allowed(op.connection_id())?;
        let st = self.state.clone();
        match op {
            mp::ProposalOp::Propose {
                connection_id,
                session,
                proposal,
            } => {
                check_note_texts(proposal.related_rule_id.as_deref(), &proposal.title)?;
                if sanitize(&proposal.rationale, MAX_BODY_CHARS).is_empty() {
                    return Err(BackendError::invalid_argument(
                        "rationale must not be empty: explain the change to the user",
                    ));
                }
                let change = to_change(&proposal.change)?.trimmed();
                let facts = self.facts(&connection_id, &change).await?;
                validate(&change, &facts).map_err(|e| match e {
                    ProposalError::Invalid(m) => BackendError::invalid_argument(m),
                    ProposalError::NotFound(m) => BackendError::not_found(m),
                })?;
                let read_only = st
                    .connections
                    .get(&connection_id)
                    .map(|p| p.read_only)
                    .unwrap_or(false);
                let new = NewProposal {
                    severity: to_severity(proposal.severity),
                    title: proposal.title,
                    rationale: proposal.rationale,
                    change,
                    related_rule_id: proposal.related_rule_id,
                    related_finding_key: proposal
                        .related_finding_key
                        .map(|k| sanitize(&k, MAX_RELATED_KEY_CHARS))
                        .filter(|k| !k.is_empty()),
                    author: self.author(&session),
                };
                let (st2, id) = (st.clone(), connection_id.clone());
                let r = blocking(move || {
                    st2.health
                        .write(&id, generation, |s| s.propose(&id, new, now_ms()))
                })
                .await
                .map_err(health_to_backend)?;
                let (proposal_id, created) = match r {
                    Proposed::Created(id) => (id, true),
                    Proposed::Updated(id) => (id, false),
                    Proposed::LimitReached => {
                        return Err(BackendError::new(
                            "limit_reached",
                            format!(
                                "This connection already has {MAX_OPEN_PROPOSALS} open proposals; ask the user to apply, reject or delete some in Glafira"
                            ),
                        ));
                    }
                };
                st.health.changed(&connection_id);
                Ok(mp::ProposalOutput::Proposed {
                    proposal_id,
                    created,
                    read_only,
                })
            }
            mp::ProposalOp::List {
                connection_id,
                status,
            } => {
                let f = blocking(move || st.health.store.load(&connection_id, now_ms()))
                    .await
                    .map_err(health_to_backend)?;
                let items = f
                    .proposals
                    .iter()
                    .filter(|p| match status {
                        mh::StatusFilter::Open => p.status == ProposalStatus::Open,
                        mh::StatusFilter::Closed => p.status != ProposalStatus::Open,
                        mh::StatusFilter::All => true,
                    })
                    .map(proposal_view)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(mp::ProposalOutput::Proposals(items))
            }
            mp::ProposalOp::Withdraw {
                connection_id,
                session,
                proposal_id,
            } => {
                let author = self.author(&session);
                let (st2, id) = (st.clone(), connection_id.clone());
                let r = blocking(move || {
                    st2.health.write(&id, generation, |s| {
                        s.withdraw_own(&id, &proposal_id, &author, now_ms())
                    })
                })
                .await
                .map_err(health_to_backend)?;
                match r {
                    Withdrawn::Withdrawn => {
                        st.health.changed(&connection_id);
                        Ok(mp::ProposalOutput::Withdrawn)
                    }
                    Withdrawn::NotFound => Err(BackendError::not_found(
                        "The proposal was deleted or has expired",
                    )),
                    Withdrawn::NotOwner => Err(BackendError::new(
                        "not_allowed",
                        "Only proposals created by this client can be withdrawn",
                    )),
                    Withdrawn::Closed(status) => Err(BackendError::invalid_argument(format!(
                        "The proposal is already {}",
                        status.as_str()
                    ))),
                }
            }
        }
    }
}

/// Вид изменения из крейта в вид приложения: одна и та же JSON-форма (спец. G3 §3).
fn to_change(spec: &mp::ChangeSpec) -> Result<Change, BackendError> {
    reshape(spec).ok_or_else(|| BackendError::invalid_argument("change has an unexpected shape"))
}

/// Перекладывает значение между одинаковыми по JSON типами крейта и приложения.
fn reshape<T: Serialize, U: DeserializeOwned>(t: &T) -> Option<U> {
    serde_json::to_value(t)
        .ok()
        .and_then(|v| serde_json::from_value(v).ok())
}

/// Общие проверки текстов замечания и предложения: правило из каталога, непустой заголовок.
fn check_note_texts(related_rule_id: Option<&str>, title: &str) -> Result<(), BackendError> {
    if let Some(r) = related_rule_id
        && !rules::rule_ids().any(|id| id == r)
    {
        return Err(BackendError::invalid_argument(format!(
            "relatedRuleId {r} is not a health_check rule"
        )));
    }
    // Заголовок из одних управляющих символов после очистки пуст — как пустой заголовок.
    if sanitize(title, MAX_TITLE_CHARS).is_empty() {
        return Err(BackendError::invalid_argument("title must not be empty"));
    }
    Ok(())
}

fn proposal_view(p: &Proposal) -> Result<mp::ProposalView, BackendError> {
    let unexpected = || BackendError::new("internal", "Unexpected stored proposal shape");
    let change = reshape(&p.change).ok_or_else(unexpected)?;
    let applied_change = match &p.applied_change {
        Some(c) => Some(reshape(c).ok_or_else(unexpected)?),
        None => None,
    };
    Ok(mp::ProposalView {
        id: p.id.clone(),
        severity: from_severity(p.severity),
        title: p.title.clone(),
        rationale: p.rationale.clone(),
        target: from_target(&p.target),
        change,
        related_rule_id: p.related_rule_id.clone(),
        related_finding_key: p.related_finding_key.clone(),
        client: p.client.clone(),
        status: p.status.as_str().into(),
        created_at_ms: p.created_at_ms,
        updated_at_ms: p.updated_at_ms,
        closed_at_ms: p.closed_at_ms,
        applied_as_proposed: p.applied_as_proposed,
        reject_reason: p.reject_reason.clone(),
        applied_change,
    })
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
    // Имя сохраняется ровно таким, каким его проверил `target_exists`: id группы может
    // начинаться и кончаться пробелом, поэтому топик и группа не обрезаются; брокер — его
    // числовой id в каноническом виде.
    Target {
        kind,
        name: match kind {
            TargetKind::Cluster => None,
            TargetKind::Broker => t.name.as_deref().map(|n| match broker_id(n) {
                Some(id) => id.to_string(),
                None => n.trim().to_string(),
            }),
            TargetKind::Topic | TargetKind::Group => t.name.clone(),
        },
    }
}

/// Числовой id брокера из `target.name`.
fn broker_id(name: &str) -> Option<i32> {
    name.trim().parse().ok()
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

    fn proposals(
        &self,
        op: mp::ProposalOp,
    ) -> BoxFuture<'_, Result<mp::ProposalOutput, BackendError>> {
        Box::pin(self.proposals_impl(op))
    }

    fn messages(
        &self,
        op: mm::MessagesOp,
        ctx: mm::ScanCtx,
    ) -> BoxFuture<'_, Result<mm::MessagesOutput, BackendError>> {
        Box::pin(self.messages_impl(op, ctx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::{LlmAccess, sample_profile};

    /// Абсолютный путь к несуществующему файлу сертификата. На Windows путь без диска (`/…`)
    /// не абсолютный — проверка подключения отвергла бы его раньше чтения файла.
    fn missing_secret_path() -> &'static str {
        if cfg!(windows) {
            r"C:\nonexistent\secret-path\ca.pem"
        } else {
            "/nonexistent/secret-path/ca.pem"
        }
    }

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
        assert_eq!(list[0].llm_access, "metadata");
        assert_eq!(b.connection_name("open").as_deref(), Some("dev"));
    }

    #[test]
    fn messages_level_is_listed_with_the_policy_applied() {
        let (_d, b) = backend();
        let mut p = sample_profile("msgs");
        p.llm_access = LlmAccess::Messages;
        b.state.connections.upsert(p).unwrap();
        let level = |b: &AppBackend| {
            b.connections()
                .into_iter()
                .find(|c| c.id == "msgs")
                .map(|c| c.llm_access)
        };
        assert_eq!(level(&b).as_deref(), Some("messages"));
        let mut st = b.state.clone();
        let mut policy = (*st.policy).clone();
        policy.status.mcp_messages_enabled = false;
        st.policy = Arc::new(policy);
        let b = AppBackend::new(st, b.approvals.clone(), Arc::new(|_| {}));
        assert_eq!(level(&b).as_deref(), Some("metadata"));
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
                ca_location: Some(missing_secret_path().into()),
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
        // Запись невозможна на любой платформе: на месте каталога `secret-path` обычный файл.
        // (`/nonexistent/…` на Windows — путь на текущем диске, и create_dir_all его создал бы.)
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join("secret-path"), b"x").unwrap();
        let e = to_backend(
            crate::storage::fs::atomic_write(&d.path().join("secret-path").join("ca.pem"), b"x")
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
    async fn title_of_control_characters_is_rejected_before_writing() {
        let (d, b) = backend();
        let claude = view("s1", Some("claude-ai"));
        let _ = b.approvals.access(&claude, 0);
        let e = annotate(&b, &claude, "\u{1}\u{2}").await.unwrap_err();
        assert_eq!(e.code, "invalid_argument", "{e:?}");
        assert_eq!(e.message, "title must not be empty");
        assert!(!d.path().join("health").join("open.json").exists());
        let f = b.state.health.store.load("open", now_ms()).unwrap();
        assert!(f.annotations.is_empty());
    }

    #[tokio::test]
    async fn health_file_errors_are_internal_without_the_path() {
        let (d, b) = backend();
        // Файл `health/open.json` не прочитать и не записать: на его месте каталог. Обычный файл
        // на месте каталога `health/` подошёл бы только на Unix: Windows отвечает на чтение
        // сквозь него NotFound, и хранилище считает, что файла ещё нет.
        std::fs::create_dir_all(d.path().join("health").join("open.json")).unwrap();
        let claude = view("s1", Some("claude-ai"));
        let _ = b.approvals.access(&claude, 0);
        let check = |run_id: Option<&str>| mh::HealthOp::Check {
            connection_id: "open".into(),
            max_age_ms: 300_000,
            run_id: run_id.map(String::from),
            session: claude.clone(),
        };
        let ops = [
            check(None),
            check(Some("r1")),
            mh::HealthOp::ListAnnotations {
                connection_id: "open".into(),
                status: mh::StatusFilter::All,
            },
            mh::HealthOp::Annotate {
                connection_id: "open".into(),
                session: claude.clone(),
                note: cluster_note("t", None),
            },
            mh::HealthOp::RemoveAnnotation {
                connection_id: "open".into(),
                session: claude.clone(),
                annotation_id: "n1".into(),
            },
        ];
        for op in ops {
            let name = format!("{op:?}");
            let e = b.health(op).await.unwrap_err();
            assert_eq!(
                (e.code.as_str(), e.message.as_str()),
                ("internal", HEALTH_FILE_FAILED),
                "{name}"
            );
        }
        // Операции предложений, не ходящие в Kafka, — так же.
        let ops = [
            mp::ProposalOp::List {
                connection_id: "open".into(),
                status: mh::StatusFilter::All,
            },
            mp::ProposalOp::Withdraw {
                connection_id: "open".into(),
                session: claude.clone(),
                proposal_id: "p1".into(),
            },
        ];
        for op in ops {
            let name = format!("{op:?}");
            let e = b.proposals(op).await.unwrap_err();
            assert_eq!(
                (e.code.as_str(), e.message.as_str()),
                ("internal", HEALTH_FILE_FAILED),
                "{name}"
            );
        }
    }

    #[test]
    fn health_errors_are_classified_by_key() {
        // Файловые ошибки и паника фоновой задачи — `internal` строчными с общим текстом;
        // остальное — как в G1.
        let err = |key: &str| {
            let mut e = app_err!(
                ErrorCode::Internal,
                "errors.io",
                "Failed to write {{path}}",
                path = "/secret-path/health/open.json"
            );
            e.key = key.to_string();
            e
        };
        for key in [
            "errors.io",
            "errors.json",
            "errors.storage.writeFailed",
            "errors.storage.newerSchema",
        ] {
            let e = health_to_backend(err(key));
            assert_eq!(
                (e.code.as_str(), e.message.as_str()),
                ("internal", HEALTH_FILE_FAILED),
                "{key}"
            );
        }
        let e = health_to_backend(err(TASK_PANICKED_KEY));
        assert_eq!(
            (e.code.as_str(), e.message.as_str()),
            ("internal", BACKGROUND_FAILED)
        );
        let e = health_to_backend(crate::health::store::connection_not_found("x"));
        assert_eq!(e.code, "NOT_FOUND");
        let e = health_to_backend(app_err!(
            ErrorCode::Internal,
            "errors.kafka.emptyResponse",
            "Empty {{request}} response",
            request = "DescribeConfigs"
        ));
        assert_eq!(
            (e.code.as_str(), e.message.as_str()),
            ("INTERNAL", "Empty DescribeConfigs response")
        );
    }

    #[test]
    fn target_name_is_stored_as_it_was_checked() {
        let target = |kind, name: &str| {
            to_target(&mh::Target {
                kind,
                name: Some(name.into()),
            })
            .name
        };
        // Id группы и имя топика могут начинаться и кончаться пробелом.
        assert_eq!(
            target(mh::TargetKind::Group, " g 1 ").as_deref(),
            Some(" g 1 ")
        );
        assert_eq!(
            target(mh::TargetKind::Topic, "orders ").as_deref(),
            Some("orders ")
        );
        // Брокер — канонический числовой id, как у находок.
        for raw in [" 1 ", "01", "+1"] {
            assert_eq!(target(mh::TargetKind::Broker, raw).as_deref(), Some("1"));
        }
        assert_eq!(
            to_target(&mh::Target {
                kind: mh::TargetKind::Cluster,
                name: Some("x".into())
            })
            .name,
            None
        );
    }

    fn alter_spec(topic: &str) -> mp::ChangeSpec {
        mp::ChangeSpec::AlterTopicConfigs {
            topic: topic.into(),
            changes: vec![mp::ConfigChangeSpec {
                name: "min.insync.replicas".into(),
                value: Some("2".into()),
            }],
        }
    }

    fn new_proposal(title: &str, related: Option<&str>) -> mp::NewProposal {
        mp::NewProposal {
            severity: mh::Severity::Warning,
            title: title.into(),
            rationale: "acks=all needs two in-sync replicas".into(),
            change: alter_spec("orders"),
            related_rule_id: related.map(String::from),
            related_finding_key: Some(" topic.minIsrOne:topic:orders ".into()),
        }
    }

    fn propose_in<'a>(
        b: &'a AppBackend,
        connection_id: &str,
        session: &SessionView,
        proposal: mp::NewProposal,
    ) -> BoxFuture<'a, Result<mp::ProposalOutput, BackendError>> {
        b.proposals(mp::ProposalOp::Propose {
            connection_id: connection_id.into(),
            session: session.clone(),
            proposal,
        })
    }

    fn propose<'a>(
        b: &'a AppBackend,
        session: &SessionView,
        title: &str,
        related: Option<&str>,
    ) -> BoxFuture<'a, Result<mp::ProposalOutput, BackendError>> {
        propose_in(b, "open", session, new_proposal(title, related))
    }

    fn read_op(connection_id: &str, value_regex: Option<&str>) -> mm::MessagesOp {
        mm::MessagesOp::Read {
            connection_id: connection_id.into(),
            topic: "orders".into(),
            range: mm::Range::Window {
                from_ms: 0,
                to_ms: 1,
                partitions: None,
            },
            filter: mm::MessageFilter {
                value: value_regex.map(|p| mm::TextMatch {
                    pattern: p.into(),
                    regex: true,
                    case_sensitive: false,
                }),
                ..Default::default()
            },
            key_format: mm::FormatArg::Auto,
            value_format: mm::FormatArg::Auto,
            limit: 10,
            after: None,
        }
    }

    fn scan_ctx() -> mm::ScanCtx {
        mm::ScanCtx::new(std::time::Duration::from_secs(5))
    }

    #[tokio::test]
    async fn messages_need_the_messages_level_and_the_policy() {
        let (_d, b) = backend();
        for id in ["closed", "missing"] {
            let e = b.messages(read_op(id, None), scan_ctx()).await.unwrap_err();
            assert_eq!(e.code, "not_allowed", "{id}");
        }
        let e = b
            .messages(read_op("open", None), scan_ctx())
            .await
            .unwrap_err();
        assert_eq!(e.code, "messages_not_allowed", "{e:?}");
        assert!(e.message.contains("Metadata and messages"));

        let mut p = sample_profile("msgs");
        p.llm_access = LlmAccess::Messages;
        p.bootstrap_servers = "127.0.0.1:1".into();
        b.state.connections.upsert(p).unwrap();
        // Неверный фильтр — до обращения к кластеру.
        let e = b
            .messages(read_op("msgs", Some("(")), scan_ctx())
            .await
            .unwrap_err();
        assert_eq!(e.code, "invalid_argument", "{e:?}");
        assert!(e.message.contains("regular expression"), "{e:?}");
        let e = b
            .messages(read_op("msgs", None), scan_ctx())
            .await
            .unwrap_err();
        assert!(
            ["CONNECTION_FAILED", "TIMEOUT"].contains(&e.code.as_str()),
            "{e:?}"
        );

        let mut st = b.state.clone();
        let mut policy = (*st.policy).clone();
        policy.status.mcp_messages_enabled = false;
        st.policy = Arc::new(policy);
        let b = AppBackend::new(st, b.approvals.clone(), Arc::new(|_| {}));
        for id in ["msgs", "open"] {
            let e = b.messages(read_op(id, None), scan_ctx()).await.unwrap_err();
            assert_eq!(e.code, "messages_disabled", "{id}: {e:?}");
        }
    }

    #[test]
    fn scan_errors_are_classified() {
        let not_found = crate::kafka::admin::topic_not_found("orders");
        assert_eq!(scan_to_backend(not_found, false).code, "not_found");
        let partition = app_err!(
            ErrorCode::InvalidInput,
            "errors.topics.noSuchPartition",
            "The topic has no partition {{partition}}",
            partition = 9
        );
        let e = scan_to_backend(partition, false);
        assert_eq!(
            (e.code.as_str(), e.message.as_str()),
            ("invalid_argument", "The topic has no partition 9")
        );
        let denied = || {
            app_err!(
                ErrorCode::NotAuthorized,
                "errors.kafka.notAuthorized",
                "Not authorized to perform the operation"
            )
        };
        assert!(
            scan_to_backend(denied(), true)
                .message
                .contains("Read permission on this internal topic")
        );
        assert!(
            !scan_to_backend(denied(), false)
                .message
                .contains("__consumer_offsets")
        );
    }

    #[tokio::test]
    async fn proposals_need_llm_access() {
        let (_d, b) = backend();
        let s = view("s1", Some("claude-ai"));
        for id in ["closed", "missing"] {
            let ops = [
                b.proposals(mp::ProposalOp::List {
                    connection_id: id.into(),
                    status: mh::StatusFilter::All,
                }),
                b.proposals(mp::ProposalOp::Withdraw {
                    connection_id: id.into(),
                    session: s.clone(),
                    proposal_id: "p1".into(),
                }),
                propose_in(&b, id, &s, new_proposal("t", None)),
            ];
            for (n, op) in ["list", "withdraw", "propose"].into_iter().zip(ops) {
                let e = op.await.unwrap_err();
                assert_eq!(e.code, "not_allowed", "{id} {n}: {e:?}");
            }
        }
    }

    #[tokio::test]
    async fn proposal_is_validated_before_the_cluster_is_read() {
        let (d, b) = backend();
        let claude = view("s1", Some("claude-ai"));
        let _ = b.approvals.access(&claude, 0);
        let e = propose(&b, &claude, "t", Some("no.such.rule"))
            .await
            .unwrap_err();
        assert_eq!(e.code, "invalid_argument", "{e:?}");
        let e = propose(&b, &claude, "\u{1}", None).await.unwrap_err();
        assert_eq!(
            (e.code.as_str(), e.message.as_str()),
            ("invalid_argument", "title must not be empty")
        );
        // Обоснование из одних управляющих символов после очистки пусто.
        let mut bad = new_proposal("Raise min.insync.replicas", None);
        bad.rationale = "\u{0}\u{1}".into();
        let e = propose_in(&b, "open", &claude, bad).await.unwrap_err();
        assert_eq!(e.code, "invalid_argument", "{e:?}");
        assert!(e.message.contains("rationale"), "{e:?}");
        assert!(!d.path().join("health").join("open.json").exists());
        // Дальше предложение сверяется с кластером: 127.0.0.1:1 не отвечает.
        let e = propose(
            &b,
            &claude,
            "Raise min.insync.replicas",
            Some("topic.minIsrOne"),
        )
        .await
        .unwrap_err();
        assert!(
            ["CONNECTION_FAILED", "TIMEOUT"].contains(&e.code.as_str()),
            "{e:?}"
        );
        assert!(
            b.state
                .health
                .store
                .load("open", now_ms())
                .unwrap()
                .proposals
                .is_empty()
        );
    }

    #[test]
    fn change_spec_and_change_share_one_json_shape() {
        let specs = [
            mp::ChangeSpec::AlterTopicConfigs {
                topic: "orders".into(),
                changes: vec![
                    mp::ConfigChangeSpec {
                        name: "min.insync.replicas".into(),
                        value: Some("2".into()),
                    },
                    mp::ConfigChangeSpec {
                        name: "retention.ms".into(),
                        value: None,
                    },
                ],
            },
            mp::ChangeSpec::CreateTopic {
                name: "payments".into(),
                partitions: 6,
                replication_factor: Some(-1),
                configs: Some(std::collections::BTreeMap::from([(
                    "cleanup.policy".to_string(),
                    "compact".to_string(),
                )])),
            },
            mp::ChangeSpec::AddPartitions {
                topic: "orders".into(),
                new_total: 12,
            },
            mp::ChangeSpec::ResetOffsets {
                group_id: "billing".into(),
                strategy: mp::ResetStrategySpec::Timestamp {
                    timestamp_ms: 1_700_000_000_000,
                },
                scope: mp::ResetScopeSpec::Partitions {
                    partitions: vec![mp::PartitionRef {
                        topic: "orders".into(),
                        partition: 3,
                    }],
                },
            },
        ];
        for spec in specs {
            let json = serde_json::to_value(&spec).unwrap();
            let change: Change =
                serde_json::from_value(json.clone()).unwrap_or_else(|e| panic!("{json}: {e}"));
            assert_eq!(serde_json::to_value(&change).unwrap(), json);
            assert_eq!(reshape::<_, mp::ChangeSpec>(&change), Some(spec), "{json}");
        }
    }

    #[tokio::test]
    async fn proposals_are_listed_and_withdrawn_by_their_author() {
        let (_d, b) = backend();
        let claude = view("s1", Some("claude-ai"));
        let _ = b.approvals.access(&claude, 0);
        // Предложение записано в файл (сверка с кластером проверяется интеграционным тестом).
        let Proposed::Created(id) = b
            .state
            .health
            .store
            .propose(
                "open",
                NewProposal {
                    severity: Severity::Critical,
                    title: "Raise min.insync.replicas".into(),
                    rationale: "r".into(),
                    change: to_change(&alter_spec("orders")).unwrap(),
                    related_rule_id: Some("topic.minIsrOne".into()),
                    related_finding_key: None,
                    author: b.author(&claude),
                },
                now_ms(),
            )
            .unwrap()
        else {
            panic!()
        };
        let list = |status| {
            b.proposals(mp::ProposalOp::List {
                connection_id: "open".into(),
                status,
            })
        };
        let mp::ProposalOutput::Proposals(items) = list(mh::StatusFilter::Open).await.unwrap()
        else {
            panic!()
        };
        assert_eq!(items.len(), 1);
        let p = &items[0];
        assert_eq!(
            (p.id.as_str(), p.status.as_str(), p.client.as_deref()),
            (id.as_str(), "open", Some("claude-ai"))
        );
        assert_eq!(p.severity, mh::Severity::Critical);
        assert_eq!(p.change, alter_spec("orders"));
        assert_eq!(p.target.name.as_deref(), Some("orders"));
        assert_eq!(p.related_rule_id.as_deref(), Some("topic.minIsrOne"));
        let mp::ProposalOutput::Proposals(closed) = list(mh::StatusFilter::Closed).await.unwrap()
        else {
            panic!()
        };
        assert!(closed.is_empty());

        let other = view("s2", Some("cursor"));
        let _ = b.approvals.access(&other, 0);
        let withdraw = |s: &SessionView, pid: &str| {
            b.proposals(mp::ProposalOp::Withdraw {
                connection_id: "open".into(),
                session: s.clone(),
                proposal_id: pid.into(),
            })
        };
        assert_eq!(withdraw(&other, &id).await.unwrap_err().code, "not_allowed");
        assert_eq!(
            withdraw(&claude, "nope").await.unwrap_err().code,
            "not_found"
        );
        assert_eq!(
            withdraw(&claude, &id).await.unwrap(),
            mp::ProposalOutput::Withdrawn
        );
        let e = withdraw(&claude, &id).await.unwrap_err();
        assert_eq!(
            (e.code.as_str(), e.message.as_str()),
            ("invalid_argument", "The proposal is already withdrawn")
        );
        let mp::ProposalOutput::Proposals(closed) = list(mh::StatusFilter::Closed).await.unwrap()
        else {
            panic!()
        };
        assert_eq!(closed[0].status, "withdrawn");
        assert!(closed[0].closed_at_ms.is_some());
    }

    #[tokio::test]
    async fn proposal_applied_with_edits_is_listed_with_the_applied_change() {
        let (_d, b) = backend();
        let claude = view("s1", Some("claude-ai"));
        let _ = b.approvals.access(&claude, 0);
        let store = &b.state.health.store;
        let Proposed::Created(id) = store
            .propose(
                "open",
                NewProposal {
                    severity: Severity::Warning,
                    title: "Raise min.insync.replicas".into(),
                    rationale: "r".into(),
                    change: to_change(&alter_spec("orders")).unwrap(),
                    related_rule_id: None,
                    related_finding_key: None,
                    author: b.author(&claude),
                },
                now_ms(),
            )
            .unwrap()
        else {
            panic!()
        };
        // Человек поправил значение: 3 вместо предложенных 2.
        let applied = crate::health::proposals::Applied::AlterTopicConfigs {
            topic: "orders".into(),
            changes: vec![crate::kafka::admin::ConfigChange::Set {
                name: "min.insync.replicas".into(),
                value: "3".into(),
            }],
        };
        store.mark_applied("open", &id, &applied, now_ms()).unwrap();
        let mp::ProposalOutput::Proposals(items) = b
            .proposals(mp::ProposalOp::List {
                connection_id: "open".into(),
                status: mh::StatusFilter::Closed,
            })
            .await
            .unwrap()
        else {
            panic!()
        };
        let p = &items[0];
        assert_eq!(p.applied_as_proposed, Some(false));
        assert_eq!(p.change, alter_spec("orders"));
        assert_eq!(
            p.applied_change,
            Some(mp::ChangeSpec::AlterTopicConfigs {
                topic: "orders".into(),
                changes: vec![mp::ConfigChangeSpec {
                    name: "min.insync.replicas".into(),
                    value: Some("3".into()),
                }],
            })
        );
    }

    #[tokio::test]
    async fn check_reuses_the_last_run_and_marks_ignored() {
        let (_d, b) = backend();
        let run = CheckRun {
            run_id: "r1".into(),
            started_at_ms: now_ms(),
            duration_ms: 5,
            trigger: Trigger::Ui,
            environment: crate::storage::connections::Environment::Dev,
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
        assert_eq!((r.run_id.as_str(), r.environment.as_str()), ("r1", "dev"));
        assert!(r.findings[0].ignored);
        assert_eq!(r.findings[0].title, "Single-broker cluster");
        assert!(matches!(
            check(Some("r1")).await.unwrap(),
            mh::HealthOutput::Run(_)
        ));
        assert_eq!(check(Some("r0")).await.unwrap_err().code, "not_found");
    }

    #[tokio::test]
    async fn check_of_another_environment_is_not_reused() {
        let (_d, b) = backend();
        // Свежая проверка на prod, а подключение теперь dev: строгость другая — нужна новая.
        let run = CheckRun {
            run_id: "r1".into(),
            started_at_ms: now_ms(),
            duration_ms: 5,
            trigger: Trigger::Ui,
            environment: crate::storage::connections::Environment::Prod,
            incomplete: false,
            scope: Default::default(),
            findings: vec![],
            group_lag: Default::default(),
        };
        b.state
            .health
            .store
            .save_check("open", run, now_ms())
            .unwrap();
        let e = b
            .health(mh::HealthOp::Check {
                connection_id: "open".into(),
                max_age_ms: 300_000,
                run_id: None,
                session: view("s1", None),
            })
            .await
            .unwrap_err();
        // Новая проверка ушла в Kafka (127.0.0.1:1 не отвечает).
        assert!(
            ["CONNECTION_FAILED", "TIMEOUT"].contains(&e.code.as_str()),
            "{e:?}"
        );
    }
}
