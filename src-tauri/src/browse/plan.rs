use std::collections::{HashMap, HashSet};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum StartPosition {
    Earliest,
    /// Последние `count` сообщений на всю выборку, поровну по партициям.
    Latest {
        count: i64,
    },
    Offset {
        offset: i64,
    },
    Timestamp {
        timestamp_ms: i64,
    },
    LastMinutes {
        minutes: i64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionBounds {
    pub partition: i32,
    pub low: i64,
    pub high: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionPlan {
    pub partition: i32,
    pub start: i64,
    /// High watermark на момент старта (не включительно).
    pub end: i64,
}

fn plan_with(
    bounds: &[PartitionBounds],
    start: impl Fn(&PartitionBounds) -> i64,
) -> Vec<PartitionPlan> {
    bounds
        .iter()
        .map(|b| PartitionPlan {
            partition: b.partition,
            start: start(b).clamp(b.low, b.high.max(b.low)),
            end: b.high,
        })
        .collect()
}

pub fn plan_earliest(bounds: &[PartitionBounds]) -> Vec<PartitionPlan> {
    plan_with(bounds, |b| b.low)
}

pub fn plan_latest(bounds: &[PartitionBounds], count: i64) -> Vec<PartitionPlan> {
    let n = bounds.len().max(1) as i64;
    let per = (count + n - 1) / n;
    plan_with(bounds, |b| b.high - per)
}

pub fn plan_offset(bounds: &[PartitionBounds], offset: i64) -> Vec<PartitionPlan> {
    plan_with(bounds, |_| offset)
}

/// `resolved` — результат offsets_for_times; если для партиции оффсета нет
/// (нет сообщений новее метки), читаем с конца.
pub fn plan_from_offsets(
    bounds: &[PartitionBounds],
    resolved: &HashMap<i32, i64>,
) -> Vec<PartitionPlan> {
    plan_with(bounds, |b| {
        resolved.get(&b.partition).copied().unwrap_or(b.high)
    })
}

pub struct EndTracker {
    ends: HashMap<i32, i64>,
    done: HashSet<i32>,
}

impl EndTracker {
    pub fn new(plans: &[PartitionPlan]) -> Self {
        Self {
            ends: plans.iter().map(|p| (p.partition, p.end)).collect(),
            done: plans
                .iter()
                .filter(|p| p.start >= p.end)
                .map(|p| p.partition)
                .collect(),
        }
    }

    /// Возвращает true, если сообщение входит в снимок (offset < end).
    pub fn observe(&mut self, partition: i32, offset: i64) -> bool {
        let Some(&end) = self.ends.get(&partition) else {
            return false;
        };
        if offset >= end {
            self.done.insert(partition);
            return false;
        }
        if offset + 1 >= end {
            self.done.insert(partition);
        }
        true
    }

    pub fn mark_done(&mut self, partition: i32) {
        if self.ends.contains_key(&partition) {
            self.done.insert(partition);
        }
    }

    pub fn all_done(&self) -> bool {
        self.done.len() == self.ends.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(partition: i32, low: i64, high: i64) -> PartitionBounds {
        PartitionBounds {
            partition,
            low,
            high,
        }
    }

    fn starts(plans: &[PartitionPlan]) -> Vec<i64> {
        plans.iter().map(|p| p.start).collect()
    }

    #[test]
    fn latest_splits_count_across_partitions() {
        let bounds = [b(0, 0, 10), b(1, 5, 7), b(2, 0, 0)];
        let plans = plan_latest(&bounds, 6); // по 2 на партицию
        assert_eq!(starts(&plans), vec![8, 5, 0]);
        assert_eq!(
            plans.iter().map(|p| p.end).collect::<Vec<_>>(),
            vec![10, 7, 0]
        );
        assert_eq!(starts(&plan_latest(&bounds, 7)), vec![7, 5, 0]); // ceil(7/3) = 3
    }

    #[test]
    fn earliest_offset_and_resolved() {
        let bounds = [b(0, 5, 10), b(1, 0, 3)];
        assert_eq!(starts(&plan_earliest(&bounds)), vec![5, 0]);
        assert_eq!(starts(&plan_offset(&bounds, 2)), vec![5, 2]);
        assert_eq!(starts(&plan_offset(&bounds, 100)), vec![10, 3]);
        let resolved = HashMap::from([(0, 7)]);
        assert_eq!(starts(&plan_from_offsets(&bounds, &resolved)), vec![7, 3]); // нет оффсета → high
    }

    #[test]
    fn tracker_detects_end() {
        let plans = [
            PartitionPlan {
                partition: 0,
                start: 0,
                end: 2,
            },
            PartitionPlan {
                partition: 1,
                start: 4,
                end: 4,
            },
        ];
        let mut t = EndTracker::new(&plans);
        assert!(!t.all_done()); // партиция 1 пустая и уже done, партиция 0 — нет
        assert!(t.observe(0, 0));
        assert!(!t.all_done());
        assert!(t.observe(0, 1));
        assert!(t.all_done());
        assert!(!t.observe(0, 2)); // за пределами снимка
        assert!(!t.observe(9, 0)); // неизвестная партиция
    }

    #[test]
    fn tracker_mark_done_handles_gaps() {
        let plans = [PartitionPlan {
            partition: 0,
            start: 0,
            end: 10,
        }];
        let mut t = EndTracker::new(&plans);
        t.observe(0, 7); // offsets 8..9 — транзакционные маркеры, не придут
        t.mark_done(0); // PartitionEOF
        assert!(t.all_done());
    }

    #[test]
    fn start_position_json() {
        let p: StartPosition =
            serde_json::from_str(r#"{"type":"timestamp","timestampMs":5}"#).unwrap();
        assert!(matches!(p, StartPosition::Timestamp { timestamp_ms: 5 }));
        let p: StartPosition =
            serde_json::from_str(r#"{"type":"lastMinutes","minutes":15}"#).unwrap();
        assert!(matches!(p, StartPosition::LastMinutes { minutes: 15 }));
    }
}
