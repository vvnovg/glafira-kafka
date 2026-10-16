use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::admin::AdminOptions;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaResult;
use rdkafka::{Offset, TopicPartitionList};

use crate::error::{AppResult, ErrorCode};
use crate::i18n::LocalizedText;
use crate::kafka::admin::topic_partition_ids;
use crate::kafka::admin_ext;
use crate::kafka::context::CapturingContext;
use crate::kafka::errors::{admin_error, item_error};
use crate::kafka::groups::*;
use crate::kafka::pool::KafkaHandles;
use crate::kafka::topics::{PartitionBoundsMap, fetch_bounds};

type Key = (String, i32);

/// Меньше этого не имеет смысла начинать сетевой запрос.
const MIN_STEP: Duration = Duration::from_millis(50);

/// Оставшееся до дедлайна время; None, если дедлайн прошёл (новые запросы не начинаем).
fn remaining(deadline: Instant) -> Option<Duration> {
    let left = deadline.saturating_duration_since(Instant::now());
    (left >= MIN_STEP).then_some(left)
}

pub fn list_groups(h: &KafkaHandles) -> AppResult<GroupList> {
    list_groups_within(h, h.timeout)
}

fn list_groups_within(h: &KafkaHandles, timeout: Duration) -> AppResult<GroupList> {
    let started = Instant::now();
    let (groups, errors) =
        admin_ext::list_consumer_groups(&h.admin, timeout).map_err(|e| h.kafka_err(started, e))?;
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
            .map(|e| LocalizedText::raw(format!("{}: {}", e.code, e.message)))
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
        .ok_or_else(|| {
            app_err!(
                ErrorCode::Internal,
                "errors.kafka.emptyResponse",
                "Empty {{request}} response",
                request = "DescribeConsumerGroups"
            )
        })
}

fn committed(h: &KafkaHandles, group: &str) -> AppResult<CommittedMap> {
    committed_within(h, group, h.timeout)
}

fn committed_within(h: &KafkaHandles, group: &str, timeout: Duration) -> AppResult<CommittedMap> {
    let started = Instant::now();
    match admin_ext::list_consumer_group_offsets(&h.admin, timeout, group) {
        Ok(Ok(raw)) => Ok(committed_from_raw(&raw)),
        Ok(Err(item)) => Err(item_error(
            item.code,
            &item.message,
            ltext!(
                "errors.groups.offsetsFailed",
                "Failed to get the group offsets"
            ),
        )),
        Err(e) => Err(h.kafka_err(started, e)),
    }
}

fn bounds(h: &KafkaHandles, keys: &[Key]) -> AppResult<PartitionBoundsMap> {
    bounds_within(h, keys, h.timeout)
}

fn bounds_within(
    h: &KafkaHandles,
    keys: &[Key],
    timeout: Duration,
) -> AppResult<PartitionBoundsMap> {
    let started = Instant::now();
    fetch_bounds(&h.consumer, keys, timeout).map_err(|e| h.kafka_err(started, e))
}

/// Границы под общим дедлайном; при неудаче/таймауте — пустая карта (лаг станет None)
/// и признак неполноты.
fn bounds_or_incomplete(
    h: &KafkaHandles,
    keys: &[Key],
    deadline: Instant,
) -> (PartitionBoundsMap, bool) {
    if keys.is_empty() {
        return (PartitionBoundsMap::new(), false);
    }
    match remaining(deadline).map(|t| bounds_within(h, keys, t)) {
        Some(Ok(b)) => (b, false),
        _ => (PartitionBoundsMap::new(), true),
    }
}

pub fn group_offsets(h: &KafkaHandles, group: &str) -> AppResult<Vec<GroupPartitionOffset>> {
    let desc = describe_group(h, group)?;
    if let (Some(e), false) = (&desc.error, desc.not_found) {
        return Err(app_err!(
            ErrorCode::Internal,
            "errors.groups.membersFailed",
            "Failed to get the members of group “{{group}}”",
            group = group
        )
        .with_details(e.clone()));
    }
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
    // Общий дедлайн на весь пакет: N групп не должны занимать N×timeout.
    let deadline = Instant::now() + h.timeout;
    let per_group: Vec<(String, AppResult<CommittedMap>)> = ids
        .iter()
        .map(|g| {
            let c = match remaining(deadline) {
                Some(t) => committed_within(h, g, t),
                None => Err(app_err!(
                    ErrorCode::Timeout,
                    "errors.groups.batchTimeout",
                    "The group batch timed out"
                )),
            };
            (g.clone(), c)
        })
        .collect();
    let keys: Vec<Key> = per_group
        .iter()
        .filter_map(|(_, c)| c.as_ref().ok())
        .flat_map(|c| c.keys().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let (b, bounds_failed) = bounds_or_incomplete(h, &keys, deadline);
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
                    error: bounds_failed.then(|| {
                        ltext!(
                            "errors.groups.boundsTimeout",
                            "Failed to get partition bounds in time"
                        )
                    }),
                }
            }
            Err(e) => GroupLag {
                group_id,
                total_lag: None,
                topics: Vec::new(),
                error: Some(e.text()),
            },
        })
        .collect())
}

/// Группы с коммитами в топик и их лаг по нему. Перебор групп ограничен таймаутом запроса.
pub fn topic_groups(h: &KafkaHandles, topic: &str) -> AppResult<TopicGroups> {
    let deadline = Instant::now() + h.timeout;
    let list = list_groups_within(h, h.timeout)?;
    let mut found: Vec<(GroupSummary, CommittedMap)> = Vec::new();
    let mut incomplete = !list.errors.is_empty();
    for g in list.groups {
        let Some(left) = remaining(deadline) else {
            incomplete = true;
            break;
        };
        // Ошибка одной группы (например, нет прав) не мешает остальным.
        let Ok(c) = committed_within(h, &g.group_id, left) else {
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
    let (b, bounds_failed) = bounds_or_incomplete(h, &keys, deadline);
    incomplete |= bounds_failed;
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
                return Err(app_err!(
                    ErrorCode::InvalidInput,
                    "errors.groups.noCommittedOffsets",
                    "The group has no committed offsets — select topics to reset"
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
            bounds: match b.get(k) {
                Some(Ok(v)) => Ok(*v),
                Some(Err(e)) => Err(e.text()),
                None => Err(ltext!(
                    "errors.groups.noBrokerAnswer",
                    "no response from the broker"
                )),
            },
        })
        .collect();
    let mut rows = plan_reset(&states, &req.strategy, &by_time);
    // Ошибка чтения коммита — не «коммита нет»: показываем её в строке.
    for row in &mut rows {
        if let Some(Err(e)) = committed.get(&(row.topic.clone(), row.partition)) {
            row.target = None;
            // «Нет коммита для сдвига» — следствие той же ошибки коммита, его не повторяем.
            row.error = Some(match row.error.take() {
                Some(prev) if prev.key != no_committed_offset().key => ltext!(
                    "errors.groups.errorAndCommitError",
                    "{{previous}}; commit: {{error}}",
                    previous = prev.text,
                    error = e
                ),
                _ => ltext!("errors.groups.commitError", "commit: {{error}}", error = e),
            });
        }
    }
    Ok(rows)
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
                ltext!("errors.groups.resetFailed", "Failed to reset offsets"),
            ));
        }
        Err(e) => return Err(h.kafka_err(started, e)),
    };
    let by_key: BTreeMap<Key, Option<LocalizedText>> = applied
        .into_iter()
        .map(|p| {
            let error = p.error.map(|e| LocalizedText::raw(e.message));
            ((p.topic, p.partition), error)
        })
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
                .unwrap_or_else(|| {
                    Some(ltext!(
                        "errors.groups.partitionNotConfirmed",
                        "the broker did not confirm the partition"
                    ))
                }),
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
    let context = ltext!("errors.groups.deleteFailed", "Failed to delete the group");
    for r in results {
        r.map_err(|(_name, code)| item_error(code, "", context.clone()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remaining_is_none_after_deadline() {
        assert!(remaining(Instant::now() - Duration::from_secs(1)).is_none());
        assert!(remaining(Instant::now()).is_none());
        let left = remaining(Instant::now() + Duration::from_secs(5)).unwrap();
        assert!(left <= Duration::from_secs(5) && left > Duration::from_secs(4));
    }
}
