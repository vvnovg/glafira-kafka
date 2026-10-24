//! Инструменты шлюза: параметры (их описания уходят в JSON Schema для LLM), тела ответов и
//! сами инструменты. Все — только чтение.

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ProgressNotificationParam};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{GatewayServer, to_value};
use crate::audit::now_ms;
use crate::model::*;
use crate::observe;
use crate::rules::{self, TRUNCATED_HINT};

/// Ответ со списком, обрезанным под предел размера.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Fitted<T: Serialize> {
    #[serde(flatten)]
    body: T,
    truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<&'static str>,
}

fn wrap<T: Serialize>(body: T, truncated: bool) -> Result<Value, BackendError> {
    to_value(&Fitted {
        body,
        truncated,
        hint: truncated.then_some(TRUNCATED_HINT),
    })
}

fn fitted<T: Serialize, I>(
    mut body: T,
    list: fn(&mut T) -> &mut Vec<I>,
) -> Result<Value, BackendError> {
    let truncated = rules::fit(&mut body, list);
    wrap(body, truncated)
}

macro_rules! expect {
    ($out:expr, $variant:ident) => {
        match $out {
            ReadOutput::$variant(v) => Ok(v),
            _ => Err(BackendError::unexpected()),
        }
    };
}

// ---- Параметры инструментов (описания полей уходят в JSON Schema для LLM) ----

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionParams {
    /// Connection id from list_connections.
    pub connection_id: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BrokerConfigParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Broker id from get_cluster_overview.
    pub broker_id: i32,
    /// Return exactly these config names, including default values.
    #[serde(default)]
    pub names: Option<Vec<String>>,
    /// Include configs that have default values (ignored when names is set). Default: false.
    #[serde(default)]
    pub include_defaults: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListTopicsParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Case-insensitive substring of the topic name.
    #[serde(default)]
    pub filter: Option<String>,
    /// Include internal topics such as __consumer_offsets. Default: false.
    #[serde(default)]
    pub include_internal: Option<bool>,
    /// Skip this many topics (sorted by name). Default: 0.
    #[serde(default)]
    pub offset: Option<u32>,
    /// Page size, 1..=500. Default: 200.
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Topic name; internal topics such as __consumer_offsets are allowed.
    pub topic: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicConfigsParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    pub topic: String,
    /// Return exactly these config names, including default values.
    #[serde(default)]
    pub names: Option<Vec<String>>,
    /// Include configs that have default values (ignored when names is set). Default: false.
    #[serde(default)]
    pub include_defaults: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicsParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Topic names, 1..=100.
    pub topics: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListGroupsParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Case-insensitive substring of the group id.
    #[serde(default)]
    pub filter: Option<String>,
    /// Only groups in this state: Stable, Empty, PreparingRebalance, CompletingRebalance, Dead.
    #[serde(default)]
    pub state: Option<String>,
    /// Skip this many groups (sorted by id). Default: 0.
    #[serde(default)]
    pub offset: Option<u32>,
    /// Page size, 1..=500. Default: 200.
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GroupParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    pub group_id: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GroupsParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Group ids, 1..=50.
    pub group_ids: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ObserveParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    pub group_id: String,
    /// Observation window, 10..=50 seconds. Default: 30.
    #[serde(default)]
    pub duration_seconds: Option<u32>,
    /// Pause between samples, 2..=10 seconds. Default: 2.
    #[serde(default)]
    pub interval_seconds: Option<u32>,
}

pub(super) fn in_range(
    v: Option<u32>,
    default: u32,
    min: u32,
    max: u32,
    name: &str,
) -> Result<u32, BackendError> {
    match v {
        None => Ok(default),
        Some(x) if (min..=max).contains(&x) => Ok(x),
        Some(x) => Err(BackendError::invalid_argument(format!(
            "{name} must be between {min} and {max}, got {x}"
        ))),
    }
}

fn count_in(len: usize, max: usize, name: &str) -> Result<(), BackendError> {
    if (1..=max).contains(&len) {
        Ok(())
    } else {
        Err(BackendError::invalid_argument(format!(
            "{name}: pass 1..={max} items, got {len}"
        )))
    }
}

fn items_of<T>(p: &mut rules::Page<T>) -> &mut Vec<T> {
    &mut p.items
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigsBody {
    configs: Vec<ConfigEntry>,
}

fn configs_of(b: &mut ConfigsBody) -> &mut Vec<ConfigEntry> {
    &mut b.configs
}

fn partitions_of(t: &mut TopicDetails) -> &mut Vec<Partition> {
    &mut t.partitions
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GroupsPage {
    #[serde(flatten)]
    page: rules::Page<GroupSummary>,
    errors: Vec<String>,
}

fn group_items(p: &mut GroupsPage) -> &mut Vec<GroupSummary> {
    &mut p.page.items
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GroupDetails {
    #[serde(flatten)]
    group: GroupDescription,
    offsets: Vec<GroupOffset>,
    total_lag: Option<i64>,
}

fn offsets_of(d: &mut GroupDetails) -> &mut Vec<GroupOffset> {
    &mut d.offsets
}

fn members_of(d: &mut GroupDetails) -> &mut Vec<Member> {
    &mut d.group.members
}

fn topic_groups_of(g: &mut TopicGroups) -> &mut Vec<TopicGroup> {
    &mut g.groups
}

fn events_of(o: &mut observe::Observation) -> &mut Vec<observe::Event> {
    &mut o.events
}

macro_rules! args {
    ($p:expr) => {
        serde_json::to_value(&$p).unwrap_or(Value::Null)
    };
}

impl GatewayServer {
    async fn observe(
        &self,
        p: &ObserveParams,
        ctx: &RequestContext<RoleServer>,
    ) -> Result<Value, BackendError> {
        use observe::*;
        let duration = in_range(
            p.duration_seconds,
            DEFAULT_DURATION_S,
            MIN_DURATION_S,
            MAX_DURATION_S,
            "durationSeconds",
        )?;
        let interval = in_range(
            p.interval_seconds,
            DEFAULT_INTERVAL_S,
            MIN_INTERVAL_S,
            MAX_INTERVAL_S,
            "intervalSeconds",
        )?;
        let _long = self.begin_long()?;
        let describe = || ReadOp::DescribeGroup {
            connection_id: p.connection_id.clone(),
            group_id: p.group_id.clone(),
        };
        // Расписание — от начала наблюдения: время чтений не растягивает окно (спец. §5).
        let (started_at_ms, started) = (now_ms(), tokio::time::Instant::now());
        let unit = self.shared.options.observe_unit;
        let reads_until = started + unit * (duration + READ_GRACE_S);
        let late = || {
            BackendError::new(
                "TIMEOUT",
                "The group description did not arrive before the end of the observation",
            )
        };
        // И первое описание, и стартовый лаг — не дольше окна: вызов укладывается в 60 с.
        let first = tokio::time::timeout_at(reads_until, self.read(describe()))
            .await
            .map_err(|_| late())??;
        let first = expect!(first, Group)?;
        if first.not_found {
            return Err(BackendError::not_found(format!(
                "Group “{}” not found",
                p.group_id
            )));
        }
        if serde_json::to_vec(&GroupState::from(&first)).map_or(0, |b| b.len()) > MAX_STATE_BYTES {
            return Err(BackendError::new(
                "response_too_large",
                "The group is too large to observe: its members and assignments alone do not fit into 100 KB; use describe_consumer_group instead",
            ));
        }
        let lag = || async {
            match self
                .read(ReadOp::GroupsLag {
                    connection_id: p.connection_id.clone(),
                    group_ids: vec![p.group_id.clone()],
                })
                .await
            {
                Ok(ReadOutput::Lags(l)) => l.into_iter().next().and_then(|g| g.total_lag),
                _ => None,
            }
        };
        let lag_start = tokio::time::timeout_at(reads_until, lag())
            .await
            .ok()
            .flatten();
        let total = (duration / interval) as usize + 1;
        let progress = ctx.meta.get_progress_token();
        let mut rec = Recorder::default();
        rec.sample(started.elapsed().as_millis() as u64, &first);
        for i in 1..total {
            let tick = started + unit * (interval * i as u32);
            tokio::select! {
                _ = ctx.ct.cancelled() => break,
                _ = tokio::time::sleep_until(tick) => {}
            }
            let at = started.elapsed().as_millis() as u64;
            match tokio::time::timeout_at(reads_until, self.read(describe())).await {
                Err(_) => {
                    rec.failed(at, late());
                    break;
                }
                Ok(r) => match r.and_then(|o| expect!(o, Group)) {
                    Ok(g) => rec.sample(at, &g),
                    Err(e) => rec.failed(at, e),
                },
            }
            if let Some(t) = &progress {
                let note = ProgressNotificationParam::new(t.clone(), (i + 1) as f64)
                    .with_total(total as f64);
                let _ = ctx.peer.notify_progress(note).await;
            }
        }
        let lag_end = tokio::time::timeout(unit * LAG_WAIT_S, lag())
            .await
            .ok()
            .flatten();
        let mut observation = rec.finish(
            started_at_ms,
            started.elapsed().as_millis() as u64,
            lag_start,
            lag_end,
        );
        // Предел 100 КБ (спец. §6): лишние события отбрасываются, как строки списков у других
        // инструментов; счётчики сводки остаются полными.
        if rules::fit(&mut observation, events_of) {
            observation.truncated = true;
        }
        to_value(&observation)
    }
}

#[tool_router(vis = "pub(crate)")]
impl GatewayServer {
    #[tool(
        description = "List Kafka connections that the user allowed for LLM access: id, name, environment (local/dev/test/stage/prod), folder path, read-only flag. Use the id in every other tool.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn list_connections(
        &self,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .call(&ctx, "list_connections", Value::Null, None, async {
                let list = expect!(self.read(ReadOp::ListConnections).await?, Connections)?;
                to_value(&serde_json::json!({ "connections": list }))
            })
            .await)
    }

    #[tool(
        description = "Cluster overview: cluster id, controller, brokers with host:port, topic and partition counts, under-replicated and offline partitions.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn get_cluster_overview(
        &self,
        Parameters(p): Parameters<ConnectionParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .call(
                &ctx,
                "get_cluster_overview",
                args!(p),
                Some(&p.connection_id),
                async {
                    let o = expect!(
                        self.read(ReadOp::ClusterOverview {
                            connection_id: p.connection_id.clone()
                        })
                        .await?,
                        Cluster
                    )?;
                    to_value(&o)
                },
            )
            .await)
    }

    #[tool(
        description = "Broker config. By default only values that differ from defaults; pass names to get specific keys with their default values (e.g. group.initial.rebalance.delay.ms, auto.leader.rebalance.enable). Secret values are hidden.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn get_broker_config(
        &self,
        Parameters(p): Parameters<BrokerConfigParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .call(
                &ctx,
                "get_broker_config",
                args!(p),
                Some(&p.connection_id),
                async {
                    let all = expect!(
                        self.read(ReadOp::BrokerConfig {
                            connection_id: p.connection_id.clone(),
                            broker_id: p.broker_id
                        })
                        .await?,
                        Configs
                    )?;
                    let configs = rules::select_configs(
                        all,
                        p.names.as_deref(),
                        p.include_defaults.unwrap_or(false),
                    );
                    fitted(ConfigsBody { configs }, configs_of)
                },
            )
            .await)
    }

    #[tool(
        description = "List topics sorted by name: partitions, replication factor, under-replicated partitions. Supports a name filter and paging (offset, limit up to 500); internal topics are hidden unless includeInternal is true.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn list_topics(
        &self,
        Parameters(p): Parameters<ListTopicsParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .call(
                &ctx,
                "list_topics",
                args!(p),
                Some(&p.connection_id),
                async {
                    let limit =
                        rules::checked_limit(p.limit).map_err(BackendError::invalid_argument)?;
                    let mut all = expect!(
                        self.read(ReadOp::ListTopics {
                            connection_id: p.connection_id.clone()
                        })
                        .await?,
                        Topics
                    )?;
                    let internal = p.include_internal.unwrap_or(false);
                    all.retain(|t| {
                        (internal || !t.internal)
                            && rules::matches_filter(&t.name, p.filter.as_deref())
                    });
                    all.sort_by(|a, b| a.name.cmp(&b.name));
                    fitted(
                        rules::page(all, p.offset.unwrap_or(0) as usize, limit),
                        items_of,
                    )
                },
            )
            .await)
    }

    #[tool(
        description = "Topic details: replication factor, approximate message count and every partition with leader, replicas (the first one is the preferred leader), ISR, low/high offsets. Internal topics such as __consumer_offsets are allowed.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn describe_topic(
        &self,
        Parameters(p): Parameters<TopicParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .call(
                &ctx,
                "describe_topic",
                args!(p),
                Some(&p.connection_id),
                async {
                    let t = expect!(
                        self.read(ReadOp::DescribeTopic {
                            connection_id: p.connection_id.clone(),
                            topic: p.topic.clone()
                        })
                        .await?,
                        Topic
                    )?;
                    fitted(t, partitions_of)
                },
            )
            .await)
    }

    #[tool(
        description = "Topic config with the source of each value. By default only values that differ from defaults; pass names to get specific keys with defaults. Secret values are hidden.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn get_topic_configs(
        &self,
        Parameters(p): Parameters<TopicConfigsParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .call(
                &ctx,
                "get_topic_configs",
                args!(p),
                Some(&p.connection_id),
                async {
                    let all = expect!(
                        self.read(ReadOp::TopicConfigs {
                            connection_id: p.connection_id.clone(),
                            topic: p.topic.clone()
                        })
                        .await?,
                        Configs
                    )?;
                    let configs = rules::select_configs(
                        all,
                        p.names.as_deref(),
                        p.include_defaults.unwrap_or(false),
                    );
                    fitted(ConfigsBody { configs }, configs_of)
                },
            )
            .await)
    }

    #[tool(
        description = "Approximate message counts (high minus low offsets) for up to 100 topics: find large and empty topics.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn get_topics_message_counts(
        &self,
        Parameters(p): Parameters<TopicsParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .call(
                &ctx,
                "get_topics_message_counts",
                args!(p),
                Some(&p.connection_id),
                async {
                    count_in(p.topics.len(), rules::MAX_TOPICS_PER_CALL, "topics")?;
                    let counts = expect!(
                        self.read(ReadOp::TopicsMessageCounts {
                            connection_id: p.connection_id.clone(),
                            topics: p.topics.clone()
                        })
                        .await?,
                        MessageCounts
                    )?;
                    to_value(&serde_json::json!({ "topics": counts }))
                },
            )
            .await)
    }

    #[tool(
        description = "List consumer groups sorted by id with state and whether the group is simple. Supports a filter by id, a state filter and paging (offset, limit up to 500).",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn list_consumer_groups(
        &self,
        Parameters(p): Parameters<ListGroupsParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .call(
                &ctx,
                "list_consumer_groups",
                args!(p),
                Some(&p.connection_id),
                async {
                    let limit =
                        rules::checked_limit(p.limit).map_err(BackendError::invalid_argument)?;
                    let GroupList { mut groups, errors } = expect!(
                        self.read(ReadOp::ListGroups {
                            connection_id: p.connection_id.clone()
                        })
                        .await?,
                        Groups
                    )?;
                    groups.retain(|g| {
                        rules::matches_filter(&g.group_id, p.filter.as_deref())
                            && p.state
                                .as_deref()
                                .is_none_or(|s| g.state.eq_ignore_ascii_case(s))
                    });
                    groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
                    let page = rules::page(groups, p.offset.unwrap_or(0) as usize, limit);
                    fitted(GroupsPage { page, errors }, group_items)
                },
            )
            .await)
    }

    #[tool(
        description = "Consumer group details: state, group type (classic or consumer/KIP-848), assignor, coordinator broker, members with client id, host, group.instance.id (static membership) and assignment, committed offsets and lag per partition. Start here when diagnosing lag or rebalances.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn describe_consumer_group(
        &self,
        Parameters(p): Parameters<GroupParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .call(
                &ctx,
                "describe_consumer_group",
                args!(p),
                Some(&p.connection_id),
                async {
                    let group = expect!(
                        self.read(ReadOp::DescribeGroup {
                            connection_id: p.connection_id.clone(),
                            group_id: p.group_id.clone()
                        })
                        .await?,
                        Group
                    )?;
                    if group.not_found {
                        return Err(BackendError::not_found(format!(
                            "Group “{}” not found",
                            p.group_id
                        )));
                    }
                    let offsets = expect!(
                        self.read(ReadOp::GroupOffsets {
                            connection_id: p.connection_id.clone(),
                            group_id: p.group_id.clone()
                        })
                        .await?,
                        Offsets
                    )?;
                    let total_lag = offsets.iter().map(|o| o.lag).sum::<Option<i64>>();
                    let mut details = GroupDetails {
                        group,
                        offsets,
                        total_lag,
                    };
                    // Сначала оффсеты, затем участники: оба списка могут быть большими.
                    let truncated =
                        rules::fit(&mut details, offsets_of) | rules::fit(&mut details, members_of);
                    wrap(details, truncated)
                },
            )
            .await)
    }

    #[tool(
        description = "Total lag for up to 50 consumer groups and the topics each group has commits in.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn get_consumer_groups_lag(
        &self,
        Parameters(p): Parameters<GroupsParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .call(
                &ctx,
                "get_consumer_groups_lag",
                args!(p),
                Some(&p.connection_id),
                async {
                    count_in(p.group_ids.len(), rules::MAX_GROUPS_PER_CALL, "groupIds")?;
                    let lags = expect!(
                        self.read(ReadOp::GroupsLag {
                            connection_id: p.connection_id.clone(),
                            group_ids: p.group_ids.clone()
                        })
                        .await?,
                        Lags
                    )?;
                    to_value(&serde_json::json!({ "groups": lags }))
                },
            )
            .await)
    }

    #[tool(
        description = "Consumer groups that have commits in a topic, with their state and lag on that topic.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn get_topic_consumer_groups(
        &self,
        Parameters(p): Parameters<TopicParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .call(
                &ctx,
                "get_topic_consumer_groups",
                args!(p),
                Some(&p.connection_id),
                async {
                    let g = expect!(
                        self.read(ReadOp::TopicGroups {
                            connection_id: p.connection_id.clone(),
                            topic: p.topic.clone()
                        })
                        .await?,
                        TopicGroups
                    )?;
                    fitted(g, topic_groups_of)
                },
            )
            .await)
    }

    #[tool(
        description = "Watch a consumer group for 10..=50 seconds (default 30), sampling it every 2..=10 seconds (default 2). Returns a timeline: state changes, members joining and leaving (member id, client id, host, group.instance.id), assignment and coordinator changes, lag at start and end, and the time spent rebalancing. Use it when a group keeps rebalancing: who leaves, how often, from which host. For consumer (KIP-848) groups, the Assigning and Reconciling phases may show up as state Unknown and are not counted as rebalancing time, and target assignments are not compared.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn observe_consumer_group(
        &self,
        Parameters(p): Parameters<ObserveParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .call(
                &ctx,
                "observe_consumer_group",
                args!(p),
                Some(&p.connection_id),
                self.observe(&p, &ctx),
            )
            .await)
    }
}
