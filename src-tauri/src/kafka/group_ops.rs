use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::admin::AdminOptions;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaResult;
use rdkafka::{Offset, TopicPartitionList};

use crate::error::{AppError, AppResult};
use crate::kafka::admin::topic_partition_ids;
use crate::kafka::admin_ext;
use crate::kafka::context::CapturingContext;
use crate::kafka::errors::{admin_error, item_error};
use crate::kafka::groups::*;
use crate::kafka::pool::KafkaHandles;
use crate::kafka::topics::{PartitionBoundsMap, fetch_bounds};

type Key = (String, i32);

pub fn list_groups(h: &KafkaHandles) -> AppResult<GroupList> {
    let started = Instant::now();
    let (groups, errors) = admin_ext::list_consumer_groups(&h.admin, h.timeout)
        .map_err(|e| h.kafka_err(started, e))?;
    let mut groups: Vec<GroupSummary> = groups
        .into_iter()
        .map(|g| GroupSummary {
            group_id: g.group_id,
            state: g.state,
            is_simple: g.is_simple,
        })
        .collect();
    groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
    Ok(GroupList {
        groups,
        errors: errors
            .into_iter()
            .map(|e| format!("{}: {}", e.code, e.message))
            .collect(),
    })
}

pub fn describe_groups(h: &KafkaHandles, ids: &[String]) -> AppResult<Vec<GroupDescription>> {
    let started = Instant::now();
    let raw = admin_ext::describe_consumer_groups(&h.admin, h.timeout, ids)
        .map_err(|e| h.kafka_err(started, e))?;
    Ok(raw.into_iter().map(GroupDescription::from).collect())
}

pub fn describe_group(h: &KafkaHandles, id: &str) -> AppResult<GroupDescription> {
    describe_groups(h, &[id.to_string()])?
        .into_iter()
        .next()
        .ok_or_else(|| AppError::internal("Пустой ответ DescribeConsumerGroups"))
}

fn committed(h: &KafkaHandles, group: &str) -> AppResult<CommittedMap> {
    let started = Instant::now();
    match admin_ext::list_consumer_group_offsets(&h.admin, h.timeout, group) {
        Ok(Ok(raw)) => Ok(committed_from_raw(&raw)),
        Ok(Err(item)) => Err(item_error(
            item.code,
            &item.message,
            "Не удалось получить оффсеты группы",
        )),
        Err(e) => Err(h.kafka_err(started, e)),
    }
}

fn bounds(h: &KafkaHandles, keys: &[Key]) -> AppResult<PartitionBoundsMap> {
    let started = Instant::now();
    fetch_bounds(&h.consumer, keys, h.timeout).map_err(|e| h.kafka_err(started, e))
}

pub fn group_offsets(h: &KafkaHandles, group: &str) -> AppResult<Vec<GroupPartitionOffset>> {
    let desc = describe_group(h, group)?;
    let committed = committed(h, group)?;
    let assigned: HashMap<Key, String> = desc
        .members
        .iter()
        .flat_map(|m| {
            m.assignment
                .iter()
                .map(move |a| ((a.topic.clone(), a.partition), m.client_id.clone()))
        })
        .collect();
    let keys: Vec<Key> = committed
        .keys()
        .chain(assigned.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let b = bounds(h, &keys)?;
    Ok(build_partition_offsets(&committed, &assigned, &b))
}

pub fn groups_lag(h: &KafkaHandles, ids: &[String]) -> AppResult<Vec<GroupLag>> {
    let per_group: Vec<(String, AppResult<CommittedMap>)> =
        ids.iter().map(|g| (g.clone(), committed(h, g))).collect();
    let keys: Vec<Key> = per_group
        .iter()
        .filter_map(|(_, c)| c.as_ref().ok())
        .flat_map(|c| c.keys().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let b = bounds(h, &keys)?;
    Ok(per_group
        .into_iter()
        .map(|(group_id, c)| match c {
            Ok(c) => {
                let rows = build_partition_offsets(&c, &HashMap::new(), &b);
                let topics: BTreeSet<String> = rows.iter().map(|r| r.topic.clone()).collect();
                GroupLag {
                    group_id,
                    total_lag: total_lag(&rows),
                    topics: topics.into_iter().collect(),
                    error: None,
                }
            }
            Err(e) => GroupLag {
                group_id,
                total_lag: None,
                topics: Vec::new(),
                error: Some(e.message),
            },
        })
        .collect())
}

/// Группы с коммитами в топик и их лаг по нему. Перебор групп ограничен таймаутом запроса.
pub fn topic_groups(h: &KafkaHandles, topic: &str) -> AppResult<TopicGroups> {
    let deadline = Instant::now() + h.timeout;
    let list = list_groups(h)?;
    let mut found: Vec<(GroupSummary, CommittedMap)> = Vec::new();
    let mut incomplete = !list.errors.is_empty();
    for g in list.groups {
        if Instant::now() >= deadline {
            incomplete = true;
            break;
        }
        // Ошибка одной группы (например, нет прав) не мешает остальным.
        let Ok(c) = committed(h, &g.group_id) else {
            incomplete = true;
            continue;
        };
        let mine: CommittedMap = c.into_iter().filter(|((t, _), _)| t == topic).collect();
        if !mine.is_empty() {
            found.push((g, mine));
        }
    }
    let keys: Vec<Key> = found
        .iter()
        .flat_map(|(_, c)| c.keys().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let b = bounds(h, &keys)?;
    Ok(TopicGroups {
        groups: found
            .into_iter()
            .map(|(g, c)| {
                let rows = build_partition_offsets(&c, &HashMap::new(), &b);
                TopicGroupLag {
                    group_id: g.group_id,
                    state: g.state,
                    lag: total_lag(&rows),
                    partitions: rows.len(),
                }
            })
            .collect(),
        incomplete,
    })
}

fn scope_keys(
    h: &KafkaHandles,
    scope: &ResetScope,
    committed: &CommittedMap,
) -> AppResult<Vec<Key>> {
    let keys: BTreeSet<Key> = match scope {
        ResetScope::All => {
            if committed.is_empty() {
                return Err(AppError::invalid(
                    "У группы нет закоммиченных оффсетов — выберите топики для сброса",
                ));
            }
            committed.keys().cloned().collect()
        }
        ResetScope::Topics { topics } => {
            let mut out = BTreeSet::new();
            for t in topics {
                for p in topic_partition_ids(h, t)? {
                    out.insert((t.clone(), p));
                }
            }
            out
        }
        ResetScope::Partitions { partitions } => partitions
            .iter()
            .map(|p| (p.topic.clone(), p.partition))
            .collect(),
    };
    Ok(keys.into_iter().collect())
}

/// Оффсеты по времени для каждой партиции: Ok(None) — сообщений новее метки нет.
fn offsets_for_time(
    c: &BaseConsumer<CapturingContext>,
    keys: &[Key],
    ts: i64,
    timeout: Duration,
) -> KafkaResult<TimeLookup> {
    let mut tpl = TopicPartitionList::with_capacity(keys.len());
    for (t, p) in keys {
        tpl.add_partition_offset(t, *p, Offset::Offset(ts))?;
    }
    let res = c.offsets_for_times(tpl, timeout)?;
    Ok(res
        .elements()
        .iter()
        .map(|e| {
            let v = match (e.error(), e.offset()) {
                (Err(err), _) => Err(err.to_string()),
                (Ok(()), Offset::Offset(o)) if o >= 0 => Ok(Some(o)),
                (Ok(()), _) => Ok(None),
            };
            ((e.topic().to_string(), e.partition()), v)
        })
        .collect())
}

pub fn preview_reset(h: &KafkaHandles, req: &ResetRequest) -> AppResult<Vec<ResetPreviewRow>> {
    validate_reset_request(req)?;
    let committed = committed(h, &req.group_id)?;
    let keys = scope_keys(h, &req.scope, &committed)?;
    let b = bounds(h, &keys)?;
    let by_time = match req.strategy {
        ResetStrategy::Timestamp { timestamp_ms } => {
            let started = Instant::now();
            offsets_for_time(&h.consumer, &keys, timestamp_ms, h.timeout)
                .map_err(|e| h.kafka_err(started, e))?
        }
        _ => TimeLookup::new(),
    };
    let states: Vec<PartitionState> = keys
        .iter()
        .map(|k| PartitionState {
            topic: k.0.clone(),
            partition: k.1,
            current: committed.get(k).and_then(|c| c.clone().ok().flatten()),
            bounds: b
                .get(k)
                .cloned()
                .unwrap_or_else(|| Err("нет ответа брокера".to_string())),
        })
        .collect();
    Ok(plan_reset(&states, &req.strategy, &by_time))
}

/// Применяет ровно переданный план (тот, что пользователь видел в предпросмотре).
pub fn reset_offsets(
    h: &KafkaHandles,
    group: &str,
    targets: &[OffsetTarget],
) -> AppResult<Vec<ResetResult>> {
    validate_targets(targets)?;
    check_resettable(&describe_group(h, group)?)?;
    let raw: Vec<(String, i32, i64)> = targets
        .iter()
        .map(|t| (t.topic.clone(), t.partition, t.offset))
        .collect();
    let started = Instant::now();
    let applied = match admin_ext::alter_consumer_group_offsets(&h.admin, h.timeout, group, &raw) {
        Ok(Ok(parts)) => parts,
        Ok(Err(item)) => {
            return Err(item_error(
                item.code,
                &item.message,
                "Не удалось сбросить оффсеты",
            ));
        }
        Err(e) => return Err(h.kafka_err(started, e)),
    };
    let by_key: BTreeMap<Key, Option<String>> = applied
        .into_iter()
        .map(|p| ((p.topic, p.partition), p.error.map(|e| e.message)))
        .collect();
    Ok(targets
        .iter()
        .map(|t| ResetResult {
            topic: t.topic.clone(),
            partition: t.partition,
            offset: t.offset,
            error: by_key
                .get(&(t.topic.clone(), t.partition))
                .cloned()
                .unwrap_or_else(|| Some("брокер не подтвердил партицию".to_string())),
        })
        .collect())
}

/// Удаление группы. Состояние Empty проверяет вызывающий (`check_deletable`); если за это время
/// в группу кто-то вошёл, брокер ответит NonEmptyGroup → GROUP_NOT_EMPTY.
pub async fn delete_group(h: &Arc<KafkaHandles>, group: &str) -> AppResult<()> {
    let opts = AdminOptions::new()
        .request_timeout(Some(h.timeout))
        .operation_timeout(Some(h.timeout));
    let started = Instant::now();
    let results = match h.admin.delete_groups(&[group], &opts).await {
        Ok(r) => r,
        Err(e) => return Err(admin_error(h, started, e).await),
    };
    for r in results {
        r.map_err(|(name, code)| item_error(code, &name, "Не удалось удалить группу"))?;
    }
    Ok(())
}
