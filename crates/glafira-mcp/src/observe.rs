//! Хронология группы для `observe_consumer_group` (спец. §5): сравнение соседних образцов.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::model::{BackendError, Broker, GroupDescription, Member, TopicPartition};

pub const MIN_DURATION_S: u32 = 10;
/// Ниже таймаута запроса 60 с, принятого по умолчанию в клиентах на TypeScript SDK.
pub const MAX_DURATION_S: u32 = 50;
pub const DEFAULT_DURATION_S: u32 = 30;
pub const MIN_INTERVAL_S: u32 = 2;
pub const MAX_INTERVAL_S: u32 = 10;
pub const DEFAULT_INTERVAL_S: u32 = 2;
pub const MAX_EVENTS: usize = 500;
/// Сколько сверх окна ждать последнее описание группы и итоговый лаг: вместе с
/// MAX_DURATION_S вызов укладывается в 60 с.
pub const READ_GRACE_S: u32 = 3;
pub const LAG_WAIT_S: u32 = 3;

pub fn is_rebalancing(state: &str) -> bool {
    matches!(state, "PreparingRebalance" | "CompletingRebalance")
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupState {
    pub state: String,
    pub coordinator: Option<Broker>,
    pub members: Vec<Member>,
}

impl From<&GroupDescription> for GroupState {
    fn from(g: &GroupDescription) -> Self {
        Self {
            state: g.state.clone(),
            coordinator: g.coordinator.clone(),
            members: g.members.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemberRef {
    pub member_id: String,
    pub client_id: String,
    pub host: String,
    pub group_instance_id: Option<String>,
}

impl From<&Member> for MemberRef {
    fn from(m: &Member) -> Self {
        Self {
            member_id: m.member_id.clone(),
            client_id: m.client_id.clone(),
            host: m.host.clone(),
            group_instance_id: m.group_instance_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum EventKind {
    #[serde(rename_all = "camelCase")]
    StateChanged {
        from: String,
        to: String,
    },
    MemberJoined(MemberRef),
    MemberLeft(MemberRef),
    #[serde(rename_all = "camelCase")]
    AssignmentChanged {
        member_id: String,
        added: Vec<TopicPartition>,
        removed: Vec<TopicPartition>,
    },
    #[serde(rename_all = "camelCase")]
    CoordinatorChanged {
        from: Option<i32>,
        to: Option<i32>,
    },
    #[serde(rename_all = "camelCase")]
    SampleFailed {
        code: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub at_ms: u64,
    #[serde(flatten)]
    pub kind: EventKind,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    pub state_changes: usize,
    pub joins: usize,
    pub leaves: usize,
    /// Время в PreparingRebalance / CompletingRebalance по образцам: интервал до следующего удачного образца; неудачные образцы состояние не меняют.
    pub rebalancing_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Observation {
    pub started_at_ms: u64,
    pub duration_ms: u64,
    pub samples: usize,
    pub initial: Option<GroupState>,
    #[serde(rename = "final")]
    pub last: Option<GroupState>,
    pub lag_start: Option<i64>,
    pub lag_end: Option<i64>,
    pub events: Vec<Event>,
    pub truncated: bool,
    pub summary: Summary,
}

#[derive(Debug, Default)]
pub struct Recorder {
    prev: Option<(GroupState, u64)>,
    initial: Option<GroupState>,
    events: Vec<Event>,
    truncated: bool,
    samples: usize,
    summary: Summary,
}

fn by_id(members: &[Member]) -> BTreeMap<&str, &Member> {
    members.iter().map(|m| (m.member_id.as_str(), m)).collect()
}

fn sorted(v: &[TopicPartition]) -> Vec<TopicPartition> {
    let mut v = v.to_vec();
    v.sort();
    v
}

impl Recorder {
    fn push(&mut self, at_ms: u64, kind: EventKind) {
        match &kind {
            EventKind::StateChanged { .. } => self.summary.state_changes += 1,
            EventKind::MemberJoined(_) => self.summary.joins += 1,
            EventKind::MemberLeft(_) => self.summary.leaves += 1,
            _ => {}
        }
        if self.events.len() >= MAX_EVENTS {
            self.truncated = true;
        } else {
            self.events.push(Event { at_ms, kind });
        }
    }

    pub fn sample(&mut self, at_ms: u64, g: &GroupDescription) {
        // Ошибка брокера (координатор переезжает и т. п.) — не снимок группы: иначе в хронологии
        // появились бы ложные выходы и входы всех участников.
        if let Some(message) = &g.error {
            return self.failed(at_ms, BackendError::new("describe_error", message.clone()));
        }
        let next = GroupState::from(g);
        self.samples += 1;
        let Some((prev, prev_at)) = self.prev.take() else {
            self.initial = Some(next.clone());
            self.prev = Some((next, at_ms));
            return;
        };
        if is_rebalancing(&prev.state) {
            self.summary.rebalancing_ms += at_ms.saturating_sub(prev_at);
        }
        if prev.state != next.state {
            self.push(
                at_ms,
                EventKind::StateChanged {
                    from: prev.state.clone(),
                    to: next.state.clone(),
                },
            );
        }
        let (from, to) = (
            prev.coordinator.as_ref().map(|b| b.id),
            next.coordinator.as_ref().map(|b| b.id),
        );
        if from != to {
            self.push(at_ms, EventKind::CoordinatorChanged { from, to });
        }
        let (old, new) = (by_id(&prev.members), by_id(&next.members));
        let mut kinds = Vec::new();
        for (id, m) in &old {
            if !new.contains_key(id) {
                kinds.push(EventKind::MemberLeft(MemberRef::from(*m)));
            }
        }
        for (id, m) in &new {
            match old.get(id) {
                None => kinds.push(EventKind::MemberJoined(MemberRef::from(*m))),
                Some(o) => {
                    let (a, b) = (sorted(&o.assignment), sorted(&m.assignment));
                    if a != b {
                        kinds.push(EventKind::AssignmentChanged {
                            member_id: id.to_string(),
                            added: b.iter().filter(|p| !a.contains(p)).cloned().collect(),
                            removed: a.iter().filter(|p| !b.contains(p)).cloned().collect(),
                        });
                    }
                }
            }
        }
        for k in kinds {
            self.push(at_ms, k);
        }
        self.prev = Some((next, at_ms));
    }

    pub fn failed(&mut self, at_ms: u64, e: BackendError) {
        self.samples += 1;
        self.push(
            at_ms,
            EventKind::SampleFailed {
                code: e.code,
                message: e.message,
            },
        );
    }

    pub fn finish(
        self,
        started_at_ms: u64,
        duration_ms: u64,
        lag_start: Option<i64>,
        lag_end: Option<i64>,
    ) -> Observation {
        Observation {
            started_at_ms,
            duration_ms,
            samples: self.samples,
            initial: self.initial,
            last: self.prev.map(|(s, _)| s),
            lag_start,
            lag_end,
            events: self.events,
            truncated: self.truncated,
            summary: self.summary,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn member(id: &str, host: &str, parts: &[i32]) -> Member {
        Member {
            member_id: id.into(),
            client_id: "app".into(),
            host: host.into(),
            group_instance_id: None,
            assignment: parts
                .iter()
                .map(|&p| TopicPartition {
                    topic: "orders".into(),
                    partition: p,
                })
                .collect(),
            target_assignment: None,
        }
    }

    pub(crate) fn group(state: &str, coordinator: i32, members: Vec<Member>) -> GroupDescription {
        GroupDescription {
            group_id: "g".into(),
            state: state.into(),
            group_type: "classic".into(),
            assignor: "range".into(),
            coordinator: Some(Broker {
                id: coordinator,
                host: "b".into(),
                port: 9092,
            }),
            members,
            error: None,
            not_found: false,
        }
    }

    #[test]
    fn timeline_of_a_rebalance() {
        let mut r = Recorder::default();
        r.sample(
            0,
            &group(
                "Stable",
                1,
                vec![member("a", "h1", &[0, 1]), member("b", "h2", &[2])],
            ),
        );
        r.sample(
            2000,
            &group("PreparingRebalance", 1, vec![member("a", "h1", &[0, 1])]),
        );
        r.sample(
            4000,
            &group(
                "CompletingRebalance",
                2,
                vec![member("a", "h1", &[0, 1]), member("c", "h2", &[])],
            ),
        );
        r.sample(
            6000,
            &group(
                "Stable",
                2,
                vec![member("a", "h1", &[0]), member("c", "h2", &[1, 2])],
            ),
        );
        r.failed(8000, BackendError::new("TIMEOUT", "slow"));
        let o = r.finish(1_000, 8000, Some(10), Some(3));
        assert_eq!(o.samples, 5);
        assert_eq!(o.initial.as_ref().unwrap().state, "Stable");
        assert_eq!(o.last.as_ref().unwrap().members.len(), 2);
        assert_eq!(
            o.summary,
            Summary {
                state_changes: 3,
                joins: 1,
                leaves: 1,
                rebalancing_ms: 4000
            }
        );
        let kinds: Vec<String> = o
            .events
            .iter()
            .map(|e| {
                serde_json::to_value(e).unwrap()["kind"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(
            kinds,
            [
                "stateChanged",
                "memberLeft",
                "stateChanged",
                "coordinatorChanged",
                "memberJoined",
                "stateChanged",
                "assignmentChanged",
                "assignmentChanged",
                "sampleFailed"
            ]
        );
        let json = serde_json::to_value(&o).unwrap();
        assert_eq!(json["events"][1]["memberId"], "b");
        assert_eq!(json["events"][1]["host"], "h2");
        assert_eq!(json["events"][6]["removed"][0]["partition"], 1);
        assert_eq!(json["final"]["state"], "Stable");
    }

    #[test]
    fn events_are_capped_but_counted() {
        let mut r = Recorder::default();
        r.sample(0, &group("Stable", 1, vec![]));
        for i in 0..(MAX_EVENTS as u64 + 20) {
            let members = if i % 2 == 0 {
                vec![member(&format!("m{i}"), "h", &[])]
            } else {
                vec![]
            };
            r.sample(i + 1, &group("Stable", 1, members));
        }
        let o = r.finish(0, 0, None, None);
        assert_eq!(o.events.len(), MAX_EVENTS);
        assert!(o.truncated);
        assert_eq!(o.summary.joins + o.summary.leaves, MAX_EVENTS + 20);
    }

    #[test]
    fn broker_error_is_a_failed_sample_not_an_empty_group() {
        let mut r = Recorder::default();
        r.sample(
            0,
            &group(
                "Stable",
                1,
                vec![member("a", "h1", &[0]), member("b", "h2", &[1])],
            ),
        );
        let mut broken = group("Unknown", 2, vec![]);
        broken.error = Some("COORDINATOR_NOT_AVAILABLE: The coordinator is not available".into());
        r.sample(2000, &broken);
        r.sample(
            4000,
            &group(
                "Stable",
                1,
                vec![member("a", "h1", &[0]), member("b", "h2", &[1])],
            ),
        );
        let o = r.finish(0, 4000, None, None);
        assert_eq!(o.samples, 3);
        assert_eq!(o.events.len(), 1, "{:?}", o.events);
        let e = serde_json::to_value(&o.events[0]).unwrap();
        assert_eq!(
            (e["kind"].as_str(), e["code"].as_str()),
            (Some("sampleFailed"), Some("describe_error"))
        );
        assert_eq!(o.summary, Summary::default());
        assert_eq!(o.last.unwrap().members.len(), 2);
    }
}
