use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult, ErrorCode};
use crate::i18n::LocalizedText;
use crate::kafka::admin_ext::{GroupDescriptionRaw, RawPartition};
use crate::kafka::topics::PartitionBoundsMap;
use rdkafka::types::RDKafkaErrorCode;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicPartitionRef {
    pub topic: String,
    pub partition: i32,
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
    /// Частичные ошибки (например, один из брокеров не ответил) — список может быть неполным.
    pub errors: Vec<LocalizedText>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupMember {
    pub member_id: String,
    pub client_id: String,
    pub host: String,
    pub assignment: Vec<TopicPartitionRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupDescription {
    pub group_id: String,
    pub state: String,
    pub assignor: String,
    pub members: Vec<GroupMember>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Брокер ответил GROUP_ID_NOT_FOUND: группы не существует (во фронтенд не уходит).
    #[serde(skip)]
    pub not_found: bool,
}

impl From<GroupDescriptionRaw> for GroupDescription {
    fn from(r: GroupDescriptionRaw) -> Self {
        Self {
            group_id: r.group_id,
            state: r.state,
            assignor: r.assignor,
            members: r
                .members
                .into_iter()
                .map(|m| {
                    let mut assignment: Vec<TopicPartitionRef> = m
                        .assignment
                        .into_iter()
                        .map(|(topic, partition)| TopicPartitionRef { topic, partition })
                        .collect();
                    assignment.sort();
                    GroupMember {
                        member_id: m.member_id,
                        client_id: m.client_id,
                        host: m.host,
                        assignment,
                    }
                })
                .collect(),
            not_found: r
                .error
                .as_ref()
                .is_some_and(|e| e.code == RDKafkaErrorCode::GroupIdNotFound),
            error: r.error.map(|e| format!("{}: {}", e.code, e.message)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupPartitionOffset {
    pub topic: String,
    pub partition: i32,
    /// None — коммита нет (или см. error).
    pub committed: Option<i64>,
    pub log_end: Option<i64>,
    pub lag: Option<i64>,
    pub member_client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<LocalizedText>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupLag {
    pub group_id: String,
    pub total_lag: Option<i64>,
    pub topics: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<LocalizedText>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicGroupLag {
    pub group_id: String,
    pub state: String,
    pub lag: Option<i64>,
    /// Партиций топика с коммитами этой группы.
    pub partitions: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicGroups {
    pub groups: Vec<TopicGroupLag>,
    /// Не все группы проверены за таймаут запроса.
    pub incomplete: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ResetStrategy {
    Earliest,
    Latest,
    Timestamp { timestamp_ms: i64 },
    Offset { offset: i64 },
    Shift { delta: i64 },
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ResetScope {
    /// Все партиции, в которые у группы есть коммиты.
    All,
    Topics {
        topics: Vec<String>,
    },
    Partitions {
        partitions: Vec<TopicPartitionRef>,
    },
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetRequest {
    pub group_id: String,
    pub strategy: ResetStrategy,
    pub scope: ResetScope,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetPreviewRow {
    pub topic: String,
    pub partition: i32,
    pub current: Option<i64>,
    pub target: Option<i64>,
    pub low: Option<i64>,
    pub high: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<LocalizedText>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OffsetTarget {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetResult {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<LocalizedText>,
}

/// Ошибки в картах — технический текст брокера/librdkafka (не переводится).
pub type CommittedMap = BTreeMap<(String, i32), Result<Option<i64>, String>>;
pub type TimeLookup = HashMap<(String, i32), Result<Option<i64>, String>>;

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionState {
    pub topic: String,
    pub partition: i32,
    pub current: Option<i64>,
    pub bounds: Result<(i64, i64), LocalizedText>,
}

pub fn lag(committed: Option<i64>, log_end: Option<i64>) -> Option<i64> {
    Some((log_end? - committed?).max(0))
}

pub fn committed_from_raw(raw: &[RawPartition]) -> CommittedMap {
    raw.iter()
        .map(|p| {
            let value = match &p.error {
                Some(e) => Err(format!("{}: {}", e.code, e.message)),
                None => Ok((p.offset >= 0).then_some(p.offset)),
            };
            ((p.topic.clone(), p.partition), value)
        })
        .collect()
}

pub fn build_partition_offsets(
    committed: &CommittedMap,
    assigned: &HashMap<(String, i32), String>,
    bounds: &PartitionBoundsMap,
) -> Vec<GroupPartitionOffset> {
    let keys: BTreeSet<&(String, i32)> = committed.keys().chain(assigned.keys()).collect();
    keys.into_iter()
        .map(|k| {
            let (committed, commit_err) = match committed.get(k) {
                Some(Ok(c)) => (*c, None),
                Some(Err(e)) => (None, Some(e)),
                None => (None, None),
            };
            // Some(None) — брокер не вернул границы партиции.
            let (log_end, bounds_err) = match bounds.get(k) {
                Some(Ok((_, high))) => (Some(*high), None),
                Some(Err(e)) => (None, Some(Some(e))),
                None => (None, Some(None)),
            };
            let error = partition_error(commit_err, bounds_err);
            GroupPartitionOffset {
                topic: k.0.clone(),
                partition: k.1,
                committed,
                log_end,
                lag: lag(committed, log_end),
                member_client_id: assigned.get(k).cloned(),
                error,
            }
        })
        .collect()
}

fn partition_error(
    commit: Option<&String>,
    bounds: Option<Option<&String>>,
) -> Option<LocalizedText> {
    Some(match (commit, bounds) {
        (None, None) => return None,
        (Some(c), None) => ltext!("errors.groups.commitError", "commit: {{error}}", error = c),
        (None, Some(Some(b))) => ltext!(
            "errors.groups.offsetsError",
            "offsets: {{error}}",
            error = b
        ),
        (None, Some(None)) => ltext!(
            "errors.groups.offsetsNoAnswer",
            "offsets: no response from the broker"
        ),
        (Some(c), Some(Some(b))) => ltext!(
            "errors.groups.commitAndOffsetsError",
            "commit: {{commit}}; offsets: {{offsets}}",
            commit = c,
            offsets = b
        ),
        (Some(c), Some(None)) => ltext!(
            "errors.groups.commitAndOffsetsNoAnswer",
            "commit: {{commit}}; offsets: no response from the broker",
            commit = c
        ),
    })
}

/// Сумма лагов партиций с коммитом; None, если хоть одна партиция с ошибкой
/// или коммитов нет вовсе (лаг неизвестен, а не 0).
pub fn total_lag(rows: &[GroupPartitionOffset]) -> Option<i64> {
    if rows.iter().any(|r| r.error.is_some()) || rows.iter().all(|r| r.committed.is_none()) {
        return None;
    }
    Some(rows.iter().filter_map(|r| r.lag).sum())
}

pub fn validate_reset_request(req: &ResetRequest) -> AppResult<()> {
    if req.group_id.trim().is_empty() {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.groups.groupMissing",
            "No group specified"
        ));
    }
    match req.strategy {
        ResetStrategy::Offset { offset } if offset < 0 => {
            return Err(app_err!(
                ErrorCode::InvalidInput,
                "errors.groups.offsetNegative",
                "Offset cannot be negative"
            ));
        }
        ResetStrategy::Timestamp { timestamp_ms } if timestamp_ms < 0 => {
            return Err(app_err!(
                ErrorCode::InvalidInput,
                "errors.groups.timestampNegative",
                "Timestamp cannot be negative"
            ));
        }
        _ => {}
    }
    match &req.scope {
        ResetScope::Topics { topics } if topics.is_empty() => Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.topics.noneSelected",
            "No topics selected"
        )),
        ResetScope::Partitions { partitions } if partitions.is_empty() => Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.topics.noPartitionsSelected",
            "No partitions selected"
        )),
        _ => Ok(()),
    }
}

pub fn plan_reset(
    states: &[PartitionState],
    strategy: &ResetStrategy,
    by_time: &TimeLookup,
) -> Vec<ResetPreviewRow> {
    let mut rows: Vec<ResetPreviewRow> = states
        .iter()
        .map(|s| {
            let row = |target: Result<i64, LocalizedText>, low: Option<i64>, high: Option<i64>| {
                let (target, error) = match target {
                    Ok(t) => (Some(t), None),
                    Err(e) => (None, Some(e)),
                };
                ResetPreviewRow {
                    topic: s.topic.clone(),
                    partition: s.partition,
                    current: s.current,
                    target,
                    low,
                    high,
                    error,
                }
            };
            let (low, high) = match &s.bounds {
                Ok(b) => *b,
                Err(e) => return row(Err(e.clone()), None, None),
            };
            let clamp = |o: i64| o.clamp(low, high.max(low));
            let target = match strategy {
                ResetStrategy::Earliest => Ok(low),
                ResetStrategy::Latest => Ok(high),
                ResetStrategy::Offset { offset } => Ok(clamp(*offset)),
                ResetStrategy::Shift { delta } => s
                    .current
                    .map(|c| clamp(c.saturating_add(*delta)))
                    .ok_or_else(no_committed_offset),
                ResetStrategy::Timestamp { .. } => {
                    match by_time.get(&(s.topic.clone(), s.partition)) {
                        Some(Ok(Some(o))) if *o >= 0 => Ok(clamp(*o)),
                        Some(Ok(Some(o))) => Err(ltext!(
                            "errors.groups.badTimestampOffset",
                            "Invalid offset for the timestamp: {{offset}}",
                            offset = o
                        )),
                        // Нет сообщений новее метки — встаём в конец.
                        Some(Ok(None)) => Ok(high),
                        None => Err(ltext!(
                            "errors.groups.noTimestampAnswer",
                            "No response from the broker for the timestamp"
                        )),
                        Some(Err(e)) => Err(LocalizedText::raw(e)),
                    }
                }
            };
            row(target, Some(low), Some(high))
        })
        .collect();
    rows.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
    rows
}

pub fn no_committed_offset() -> LocalizedText {
    ltext!(
        "errors.groups.noCommittedOffset",
        "No committed offset — cannot shift"
    )
}

pub fn validate_targets(targets: &[OffsetTarget]) -> AppResult<()> {
    if targets.is_empty() {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.groups.emptyPlan",
            "Nothing to apply: the reset plan is empty"
        ));
    }
    let mut seen = HashSet::new();
    for t in targets {
        if t.offset < 0 {
            return Err(app_err!(
                ErrorCode::InvalidInput,
                "errors.groups.targetNegative",
                "Negative offset for {{topic}}/{{partition}}",
                topic = t.topic,
                partition = t.partition
            ));
        }
        if !seen.insert((&t.topic, t.partition)) {
            return Err(app_err!(
                ErrorCode::InvalidInput,
                "errors.groups.targetDuplicate",
                "Partition {{topic}}/{{partition}} is specified twice",
                topic = t.topic,
                partition = t.partition
            ));
        }
    }
    Ok(())
}

/// Действие над группой, которому мешают активные участники.
#[derive(Debug, Clone, Copy)]
enum GroupAction {
    Reset,
    Delete,
}

fn not_empty_error(d: &GroupDescription, action: GroupAction) -> AppError {
    let members = d
        .members
        .iter()
        .map(|m| format!("{} ({})", m.client_id, m.host))
        .collect::<Vec<_>>()
        .join(", ");
    let err = match action {
        GroupAction::Reset => app_err!(
            ErrorCode::GroupNotEmpty,
            "errors.groups.resetNotEmpty",
            "Offsets can be reset only for a group without members (current state: {{state}})",
            state = d.state
        ),
        GroupAction::Delete => app_err!(
            ErrorCode::GroupNotEmpty,
            "errors.groups.deleteNotEmpty",
            "Only a group without members can be deleted (current state: {{state}})",
            state = d.state
        ),
    };
    // details — технический текст, не переводится.
    err.with_details(format!("Active members: {members}"))
}

fn description_ok(d: &GroupDescription) -> AppResult<()> {
    match d.error.as_ref().filter(|_| !d.not_found) {
        Some(e) => Err(app_err!(
            ErrorCode::Internal,
            "errors.groups.stateFailed",
            "Failed to get the state of group “{{group}}”",
            group = d.group_id
        )
        .with_details(e.clone())),
        None => Ok(()),
    }
}

pub fn check_resettable(d: &GroupDescription) -> AppResult<()> {
    description_ok(d)?;
    if d.not_found {
        // Несуществующая группа: коммиты создадут её.
        return Ok(());
    }
    match d.state.as_str() {
        "Empty" | "Dead" => Ok(()),
        _ => Err(not_empty_error(d, GroupAction::Reset)),
    }
}

pub fn check_deletable(d: &GroupDescription) -> AppResult<()> {
    description_ok(d)?;
    if d.not_found {
        return Err(group_not_found(&d.group_id));
    }
    match d.state.as_str() {
        "Empty" => Ok(()),
        "Dead" => Err(group_not_found(&d.group_id)),
        _ => Err(not_empty_error(d, GroupAction::Delete)),
    }
}

fn group_not_found(group: &str) -> AppError {
    app_err!(
        ErrorCode::NotFound,
        "errors.groups.notFound",
        "Group “{{group}}” not found",
        group = group
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::kafka::admin_ext::{ItemError, RawPartition};
    use rdkafka::types::RDKafkaErrorCode;

    fn key(t: &str, p: i32) -> (String, i32) {
        (t.to_string(), p)
    }

    #[test]
    fn lag_is_unknown_without_both_sides_and_never_negative() {
        assert_eq!(lag(Some(3), Some(10)), Some(7));
        assert_eq!(lag(Some(12), Some(10)), Some(0));
        assert_eq!(lag(None, Some(10)), None);
        assert_eq!(lag(Some(3), None), None);
    }

    #[test]
    fn committed_from_raw_marks_missing_and_errors() {
        let raw = vec![
            RawPartition {
                topic: "t".into(),
                partition: 0,
                offset: 5,
                error: None,
            },
            RawPartition {
                topic: "t".into(),
                partition: 1,
                offset: -1001,
                error: None,
            },
            RawPartition {
                topic: "t".into(),
                partition: 2,
                offset: -1001,
                error: Some(ItemError {
                    code: RDKafkaErrorCode::UnknownTopicOrPartition,
                    message: "нет".into(),
                }),
            },
        ];
        let m = committed_from_raw(&raw);
        assert_eq!(m[&key("t", 0)], Ok(Some(5)));
        assert_eq!(m[&key("t", 1)], Ok(None));
        assert!(m[&key("t", 2)].is_err());
    }

    #[test]
    fn partition_offsets_union_committed_and_assigned() {
        let committed: CommittedMap =
            BTreeMap::from([(key("t", 0), Ok(Some(3))), (key("t", 2), Ok(Some(1)))]);
        let assigned = HashMap::from([
            (key("t", 1), "client-a".to_string()),
            (key("t", 0), "client-a".to_string()),
        ]);
        let bounds: PartitionBoundsMap = HashMap::from([
            (key("t", 0), Ok((0, 10))),
            (key("t", 1), Ok((0, 4))),
            (key("t", 2), Err("leader not available".to_string())),
        ]);
        let rows = build_partition_offsets(&committed, &assigned, &bounds);
        assert_eq!(rows.len(), 3);
        assert_eq!(
            (rows[0].committed, rows[0].log_end, rows[0].lag),
            (Some(3), Some(10), Some(7))
        );
        assert_eq!(rows[0].member_client_id.as_deref(), Some("client-a"));
        // назначена, но без коммита: лаг неизвестен, но это не ошибка
        assert_eq!(
            (rows[1].committed, rows[1].lag, rows[1].error.as_ref()),
            (None, None, None)
        );
        // ошибка оффсетов не превращается в 0
        assert_eq!((rows[2].log_end, rows[2].lag), (None, None));
        let e = rows[2].error.as_ref().unwrap();
        assert_eq!(e.key, "errors.groups.offsetsError");
        assert_eq!(e.params["error"], "leader not available");
        assert_eq!(
            total_lag(&rows),
            None,
            "ошибка в любой партиции → итог неизвестен"
        );
        assert_eq!(
            total_lag(&rows[..2]),
            Some(7),
            "партиция без коммита не входит в сумму"
        );
        assert_eq!(
            total_lag(&rows[1..2]),
            None,
            "нет ни одного коммита → лаг неизвестен, а не 0"
        );
        assert_eq!(total_lag(&[]), None);
    }

    fn state(
        p: i32,
        current: Option<i64>,
        bounds: Result<(i64, i64), LocalizedText>,
    ) -> PartitionState {
        PartitionState {
            topic: "t".into(),
            partition: p,
            current,
            bounds,
        }
    }

    fn targets(rows: &[ResetPreviewRow]) -> Vec<Option<i64>> {
        rows.iter().map(|r| r.target).collect()
    }

    #[test]
    fn plan_reset_strategies() {
        let states = vec![state(0, Some(5), Ok((2, 10))), state(1, None, Ok((0, 4)))];
        let none = TimeLookup::new();
        assert_eq!(
            targets(&plan_reset(&states, &ResetStrategy::Earliest, &none)),
            [Some(2), Some(0)]
        );
        assert_eq!(
            targets(&plan_reset(&states, &ResetStrategy::Latest, &none)),
            [Some(10), Some(4)]
        );
        assert_eq!(
            targets(&plan_reset(
                &states,
                &ResetStrategy::Offset { offset: 7 },
                &none
            )),
            [Some(7), Some(4)]
        );
        assert_eq!(
            targets(&plan_reset(
                &states,
                &ResetStrategy::Offset { offset: 0 },
                &none
            )),
            [Some(2), Some(0)]
        );

        let shifted = plan_reset(&states, &ResetStrategy::Shift { delta: -4 }, &none);
        assert_eq!(shifted[0].target, Some(2), "5 − 4 = 1 → прижато к low = 2");
        assert_eq!(shifted[1].target, None);
        assert_eq!(
            shifted[1].error.as_ref().unwrap().key,
            "errors.groups.noCommittedOffset"
        );
        let forward = plan_reset(&states, &ResetStrategy::Shift { delta: 100 }, &none);
        assert_eq!(forward[0].target, Some(10));

        let by_time = TimeLookup::from([(key("t", 0), Ok(Some(6))), (key("t", 1), Ok(None))]);
        let ts = plan_reset(
            &states,
            &ResetStrategy::Timestamp { timestamp_ms: 1 },
            &by_time,
        );
        assert_eq!(
            targets(&ts),
            [Some(6), Some(4)],
            "нет сообщений новее метки → high"
        );
    }

    #[test]
    fn plan_reset_timestamp_error_branches() {
        let states = vec![state(0, Some(5), Ok((2, 10)))];
        let ts = ResetStrategy::Timestamp { timestamp_ms: 1 };
        // нет записи в lookup → ошибка, не high
        let rows = plan_reset(&states, &ts, &TimeLookup::new());
        assert_eq!(rows[0].target, None);
        assert_eq!(
            rows[0].error.as_ref().unwrap().key,
            "errors.groups.noTimestampAnswer"
        );
        // отрицательный оффсет → ошибка, не low
        let neg = TimeLookup::from([(key("t", 0), Ok(Some(-1)))]);
        let rows = plan_reset(&states, &ts, &neg);
        assert_eq!(rows[0].target, None);
        assert!(rows[0].error.is_some());
        // Ok(None) → high
        let none = TimeLookup::from([(key("t", 0), Ok(None))]);
        let rows = plan_reset(&states, &ts, &none);
        assert_eq!((rows[0].target, rows[0].error.as_ref()), (Some(10), None));
    }

    #[test]
    fn plan_reset_reports_partition_errors() {
        let states = vec![state(0, Some(1), Err(LocalizedText::raw("нет лидера")))];
        let rows = plan_reset(&states, &ResetStrategy::Earliest, &TimeLookup::new());
        assert_eq!(
            (rows[0].target, rows[0].low, rows[0].high),
            (None, None, None)
        );
        assert_eq!(rows[0].current, Some(1));
        assert_eq!(rows[0].error, Some(LocalizedText::raw("нет лидера")));
        let by_time = TimeLookup::from([(key("t", 0), Err("timeout".to_string()))]);
        let ok_bounds = vec![state(0, Some(1), Ok((0, 5)))];
        let rows = plan_reset(
            &ok_bounds,
            &ResetStrategy::Timestamp { timestamp_ms: 1 },
            &by_time,
        );
        assert_eq!(rows[0].target, None);
        assert_eq!(rows[0].error, Some(LocalizedText::raw("timeout")));
    }

    #[test]
    fn reset_request_json_and_validation() {
        let r: ResetRequest = serde_json::from_value(serde_json::json!({
            "groupId": "g",
            "strategy": {"type": "shift", "delta": -5},
            "scope": {"type": "partitions", "partitions": [{"topic": "t", "partition": 1}]}
        }))
        .unwrap();
        assert!(matches!(r.strategy, ResetStrategy::Shift { delta: -5 }));
        assert!(validate_reset_request(&r).is_ok());
        let all: ResetRequest = serde_json::from_value(serde_json::json!({
            "groupId": "g", "strategy": {"type": "timestamp", "timestampMs": 5}, "scope": {"type": "all"}
        }))
        .unwrap();
        assert!(validate_reset_request(&all).is_ok());

        let bad = |strategy, scope| ResetRequest {
            group_id: "g".into(),
            strategy,
            scope,
        };
        for r in [
            ResetRequest {
                group_id: " ".into(),
                strategy: ResetStrategy::Earliest,
                scope: ResetScope::All,
            },
            bad(ResetStrategy::Offset { offset: -1 }, ResetScope::All),
            bad(
                ResetStrategy::Timestamp { timestamp_ms: -1 },
                ResetScope::All,
            ),
            bad(
                ResetStrategy::Earliest,
                ResetScope::Topics { topics: vec![] },
            ),
            bad(
                ResetStrategy::Earliest,
                ResetScope::Partitions { partitions: vec![] },
            ),
        ] {
            assert_eq!(
                validate_reset_request(&r).unwrap_err().code,
                ErrorCode::InvalidInput
            );
        }
    }

    #[test]
    fn targets_validation() {
        let t = |p: i32, o: i64| OffsetTarget {
            topic: "t".into(),
            partition: p,
            offset: o,
        };
        assert!(validate_targets(&[t(0, 0), t(1, 5)]).is_ok());
        assert!(validate_targets(&[]).is_err());
        assert!(validate_targets(&[t(0, -1)]).is_err());
        assert!(validate_targets(&[t(0, 1), t(0, 2)]).is_err());
    }

    fn desc(state: &str, members: usize) -> GroupDescription {
        GroupDescription {
            group_id: "g".into(),
            state: state.into(),
            assignor: "range".into(),
            members: (0..members)
                .map(|i| GroupMember {
                    member_id: format!("m{i}"),
                    client_id: format!("client-{i}"),
                    host: "/10.0.0.1".into(),
                    assignment: vec![],
                })
                .collect(),
            error: None,
            not_found: false,
        }
    }

    #[test]
    fn group_id_not_found_means_nonexistent() {
        let mut d = desc("Dead", 0);
        d.error = Some("GroupIdNotFound (Broker: Group ID not found): x".into());
        d.not_found = true;
        assert!(check_resettable(&d).is_ok());
        let e = check_deletable(&d).unwrap_err();
        assert_eq!(e.code, ErrorCode::NotFound);
        assert_eq!(e.key, "errors.groups.notFound");
        // Прочие ошибки describe остаются ошибками.
        d.not_found = false;
        assert_eq!(check_resettable(&d).unwrap_err().code, ErrorCode::Internal);
        assert_eq!(check_deletable(&d).unwrap_err().code, ErrorCode::Internal);
    }

    #[test]
    fn resettable_and_deletable_states() {
        assert!(check_resettable(&desc("Empty", 0)).is_ok());
        assert!(
            check_resettable(&desc("Dead", 0)).is_ok(),
            "новая группа — сбросить можно"
        );
        let e = check_resettable(&desc("Stable", 2)).unwrap_err();
        assert_eq!(e.code, ErrorCode::GroupNotEmpty);
        assert!(e.details.as_deref().unwrap().contains("client-1"));
        assert!(check_deletable(&desc("Empty", 0)).is_ok());
        assert_eq!(
            check_deletable(&desc("Dead", 0)).unwrap_err().code,
            ErrorCode::NotFound
        );
        assert_eq!(
            check_deletable(&desc("PreparingRebalance", 1))
                .unwrap_err()
                .code,
            ErrorCode::GroupNotEmpty
        );
    }

    #[test]
    fn description_error_blocks_checks() {
        let mut d = desc("Empty", 0);
        d.error = Some("нет прав".into());
        assert!(check_resettable(&d).is_err());
    }
}
