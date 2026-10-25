//! История коммитов группы (спец. G4 §3.4): записи `__consumer_offsets`, расшифрованные
//! приложением, сворачиваются в `runs` — подряд идущие коммиты одного офсета.

use std::collections::BTreeMap;

use crate::messages::StopReason;

/// Не больше стольких `runs` на партицию; остаются последние.
pub const MAX_RUNS: usize = 500;
/// Строка метаданных коммита в ответе — не длиннее (после маскировки).
pub const MAX_METADATA_TEXT: usize = 256;

/// Коммит офсета группы; `offset: None` — tombstone (офсет удалён).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: Option<i64>,
    pub at_ms: i64,
    pub metadata: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub offset: i64,
    pub at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    pub offset: i64,
    pub first_at_ms: i64,
    pub last_at_ms: i64,
    pub commits: u64,
    /// Метаданные последнего коммита `run` (без маскировки).
    pub metadata: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionHistory {
    pub topic: String,
    pub partition: i32,
    pub before: Option<Commit>,
    pub runs: Vec<Run>,
    pub runs_truncated: bool,
    pub deleted_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitStats {
    /// Партиция `__consumer_offsets`, где координатор хранит группу.
    pub offsets_partition: i32,
    pub scanned: u64,
    /// Записи коммитов неизвестной версии или битые.
    pub skipped_records: u64,
    pub stopped_by: Option<StopReason>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitHistory {
    pub stats: CommitStats,
    /// По топику и номеру партиции.
    pub partitions: Vec<PartitionHistory>,
}

#[derive(Default)]
struct PartState {
    before: Option<Commit>,
    runs: Vec<Run>,
    /// Старые `runs` уже отброшены при просмотре.
    runs_dropped: bool,
    /// Последний tombstone окна: следующий коммит начнёт новый `run`.
    after_delete: bool,
    /// Время последнего удаления в окне, даже если после него были коммиты.
    deleted_at_ms: Option<i64>,
}

/// Записи приходят в порядке журнала (одна партиция `__consumer_offsets`).
pub struct CommitHistoryBuilder {
    from_ms: i64,
    to_ms: i64,
    topic: Option<String>,
    parts: BTreeMap<(String, i32), PartState>,
}

impl CommitHistoryBuilder {
    pub fn new(from_ms: i64, to_ms: i64, topic: Option<String>) -> Self {
        Self {
            from_ms,
            to_ms,
            topic,
            parts: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, r: CommitRecord) {
        if self.topic.as_ref().is_some_and(|t| *t != r.topic) || r.at_ms > self.to_ms {
            return;
        }
        let p = self.parts.entry((r.topic, r.partition)).or_default();
        if r.at_ms < self.from_ms {
            // До окна: последнее состояние на его начало; удаление его снимает.
            p.before = r.offset.map(|offset| Commit {
                offset,
                at_ms: r.at_ms,
            });
            return;
        }
        let Some(offset) = r.offset else {
            p.deleted_at_ms = Some(r.at_ms);
            p.after_delete = true;
            return;
        };
        match p.runs.last_mut() {
            Some(last) if last.offset == offset && !p.after_delete => {
                last.last_at_ms = r.at_ms;
                last.commits += 1;
                last.metadata = r.metadata;
            }
            _ => {
                p.runs.push(Run {
                    offset,
                    first_at_ms: r.at_ms,
                    last_at_ms: r.at_ms,
                    commits: 1,
                    metadata: r.metadata,
                });
                // Память ограничена: при удвоенном пределе отбрасываем старые до предела.
                if p.runs.len() >= 2 * MAX_RUNS {
                    p.runs.drain(..p.runs.len().saturating_sub(MAX_RUNS));
                    p.runs_dropped = true;
                }
            }
        }
        p.after_delete = false;
    }

    pub fn finish(self, stats: CommitStats) -> CommitHistory {
        CommitHistory {
            stats,
            partitions: self
                .parts
                .into_iter()
                // Партиция, у которой были только коммиты до отступа окна и которую удалили, пуста.
                .filter(|(_, p)| {
                    p.before.is_some() || !p.runs.is_empty() || p.deleted_at_ms.is_some()
                })
                .map(|((topic, partition), mut p)| {
                    let runs_truncated = p.runs_dropped || p.runs.len() > MAX_RUNS;
                    if runs_truncated {
                        // После отбрасывания при просмотре `runs` остаётся не меньше предела; saturating_sub не даёт
                        // этому инварианту стать паникой.
                        p.runs.drain(..p.runs.len().saturating_sub(MAX_RUNS));
                    }
                    PartitionHistory {
                        topic,
                        partition,
                        before: p.before,
                        runs: p.runs,
                        runs_truncated,
                        deleted_at_ms: p.deleted_at_ms,
                    }
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(topic: &str, partition: i32, offset: Option<i64>, at_ms: i64) -> CommitRecord {
        CommitRecord {
            topic: topic.into(),
            partition,
            offset,
            at_ms,
            metadata: String::new(),
        }
    }

    #[test]
    fn repeated_commits_fold_into_runs() {
        let mut b = CommitHistoryBuilder::new(1_000, 9_000, None);
        b.add(c("orders", 0, Some(5), 100));
        b.add(c("orders", 0, Some(7), 900));
        b.add(c("orders", 0, Some(7), 1_000));
        b.add(c("orders", 0, Some(7), 1_500));
        b.add(c("orders", 0, Some(12), 2_000));
        b.add(c("orders", 1, Some(3), 2_500));
        b.add(c("orders", 0, Some(12), 9_500)); // после окна
        let h = b.finish(CommitStats::default());
        assert_eq!(h.partitions.len(), 2);
        let p0 = &h.partitions[0];
        assert_eq!(
            p0.before,
            Some(Commit {
                offset: 7,
                at_ms: 900
            })
        );
        assert_eq!(
            p0.runs
                .iter()
                .map(|r| (r.offset, r.first_at_ms, r.last_at_ms, r.commits))
                .collect::<Vec<_>>(),
            [(7, 1_000, 1_500, 2), (12, 2_000, 2_000, 1)]
        );
        assert_eq!(
            (h.partitions[1].partition, h.partitions[1].before.clone()),
            (1, None)
        );
    }

    #[test]
    fn tombstones_and_topic_filter() {
        let mut b = CommitHistoryBuilder::new(1_000, 9_000, Some("orders".into()));
        b.add(c("payments", 0, Some(1), 2_000));
        b.add(c("orders", 0, Some(4), 500));
        b.add(c("orders", 0, None, 700)); // удалён до окна: состояния на начало нет
        b.add(c("orders", 1, Some(9), 1_200));
        b.add(c("orders", 1, None, 3_000));
        let h = b.finish(CommitStats::default());
        assert_eq!(
            h.partitions.len(),
            1,
            "партиция 0 пуста, payments отфильтрован"
        );
        let p = &h.partitions[0];
        assert_eq!(
            (p.partition, p.deleted_at_ms, p.runs.len()),
            (1, Some(3_000), 1)
        );
    }

    #[test]
    fn a_commit_after_a_tombstone_starts_a_new_run() {
        let mut b = CommitHistoryBuilder::new(1_000, 9_000, None);
        b.add(c("t", 0, Some(7), 1_500));
        b.add(c("t", 0, None, 2_000));
        b.add(c("t", 0, Some(7), 3_000)); // тот же офсет после удаления
        let p = &b.finish(CommitStats::default()).partitions[0];
        assert_eq!(
            p.runs
                .iter()
                .map(|r| (r.offset, r.first_at_ms, r.commits))
                .collect::<Vec<_>>(),
            [(7, 1_500, 1), (7, 3_000, 1)]
        );
        assert_eq!(p.deleted_at_ms, Some(2_000), "коммиты после не стирают");

        // Последнее удаление в окне, и новые коммиты после него сворачиваются как обычно.
        let mut b = CommitHistoryBuilder::new(1_000, 9_000, None);
        b.add(c("t", 0, None, 2_000));
        b.add(c("t", 0, Some(8), 3_000));
        b.add(c("t", 0, Some(8), 3_500));
        b.add(c("t", 0, None, 4_000));
        let p = &b.finish(CommitStats::default()).partitions[0];
        assert_eq!(p.runs.len(), 1);
        assert_eq!((p.runs[0].commits, p.deleted_at_ms), (2, Some(4_000)));
    }

    #[test]
    fn runs_are_capped_during_the_scan() {
        let total = 3 * MAX_RUNS as i64 + 7;
        let mut b = CommitHistoryBuilder::new(0, i64::MAX, None);
        for i in 0..total {
            b.add(c("t", 0, Some(i), i));
            let held = b.parts[&("t".to_string(), 0)].runs.len();
            assert!(held < 2 * MAX_RUNS, "{held} runs after commit {i}");
        }
        let p = &b.finish(CommitStats::default()).partitions[0];
        assert!(p.runs_truncated);
        assert_eq!(p.runs.len(), MAX_RUNS);
        assert_eq!(p.runs[0].offset, total - MAX_RUNS as i64);
        assert_eq!(p.runs[MAX_RUNS - 1].offset, total - 1);
    }

    #[test]
    fn only_the_last_runs_are_kept() {
        let mut b = CommitHistoryBuilder::new(0, i64::MAX, None);
        for i in 0..(MAX_RUNS as i64 + 10) {
            b.add(c("t", 0, Some(i), i));
        }
        let p = &b.finish(CommitStats::default()).partitions[0];
        assert!(p.runs_truncated);
        assert_eq!(p.runs.len(), MAX_RUNS);
        assert_eq!(p.runs[0].offset, 10);
    }
}
