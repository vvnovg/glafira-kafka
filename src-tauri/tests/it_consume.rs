mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use common::TestMsg;
use kafka_explorer_lib::browse::filter::FilterSpec;
use kafka_explorer_lib::browse::format::DataFormat;
use kafka_explorer_lib::consume::control::{ConsumerCommand, request};
use kafka_explorer_lib::consume::session::{ConsumeSink, run_consumer};
use kafka_explorer_lib::consume::types::{
    CommitTarget, ConsumeEvent, ConsumerParams, IsolationLevel, OffsetReset,
};
use kafka_explorer_lib::kafka::context::RebalanceRecord;
use kafka_explorer_lib::kafka::group_ops::group_offsets;
use kafka_explorer_lib::kafka::pool::KafkaHandles;
use kafka_explorer_lib::secrets::MemoryStore;

#[derive(Clone, Default)]
struct VecSink(Arc<Mutex<Vec<ConsumeEvent>>>);

impl ConsumeSink for VecSink {
    fn send(&self, e: ConsumeEvent) -> bool {
        self.0.lock().unwrap().push(e);
        true
    }
}

impl VecSink {
    fn messages(&self) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|e| match e {
                ConsumeEvent::Messages { messages } => messages.len(),
                _ => 0,
            })
            .sum()
    }

    /// Последнее назначение (число партиций), если оно было.
    fn last_assigned(&self) -> Option<usize> {
        self.0.lock().unwrap().iter().rev().find_map(|e| match e {
            ConsumeEvent::Rebalance {
                record: RebalanceRecord::Assigned { partitions, .. },
            } => Some(partitions.len()),
            _ => None,
        })
    }

    fn revoked(&self) -> bool {
        self.0.lock().unwrap().iter().any(|e| {
            matches!(
                e,
                ConsumeEvent::Rebalance {
                    record: RebalanceRecord::Revoked { .. }
                }
            )
        })
    }

    fn finished(&self) -> bool {
        matches!(
            self.0.lock().unwrap().last(),
            Some(ConsumeEvent::Finished { .. })
        )
    }
}

struct Session {
    sink: VecSink,
    tx: Sender<ConsumerCommand>,
    cancel: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

fn start(bs: &str, group: &str, topic: &str) -> Session {
    let params = ConsumerParams {
        connection_id: "it".into(),
        group_id: group.into(),
        topics: vec![topic.into()],
        topic_pattern: None,
        auto_offset_reset: OffsetReset::Earliest,
        enable_auto_commit: false,
        isolation_level: IsolationLevel::ReadCommitted,
        key_format: DataFormat::Auto,
        value_format: DataFormat::Auto,
        filter: FilterSpec::default(),
    };
    let (sink, cancel) = (VecSink::default(), Arc::new(AtomicBool::new(false)));
    let (tx, rx) = mpsc::channel();
    let (s, c, profile) = (sink.clone(), cancel.clone(), common::plaintext_profile(bs));
    let handle = std::thread::spawn(move || {
        run_consumer(&profile, &MemoryStore::default(), params, c, rx, &s)
    });
    Session {
        sink,
        tx,
        cancel,
        handle,
    }
}

fn wait_until(what: &str, f: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !f() {
        assert!(Instant::now() < deadline, "не дождались: {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consume_commit_and_rebalance() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-consume", 2).await;
    let msgs: Vec<TestMsg> = (0..10)
        .map(|i| TestMsg {
            partition: i % 2,
            key: None,
            value: "v",
            headers: &[],
        })
        .collect();
    common::produce(&bs, &topic, &msgs).await;
    let group = common::TempGroup::new(&bs, "it-consume");
    let (bs2, t, g) = (bs.clone(), topic.to_string(), group.to_string());

    tokio::task::spawn_blocking(move || {
        let a = start(&bs2, &g, &t);
        wait_until("A получила 10 сообщений", || {
            a.sink.messages() == 10
        });
        assert_eq!(a.sink.last_assigned(), Some(2));

        let committed = request(&a.tx, Duration::from_secs(10), |reply| {
            ConsumerCommand::Commit {
                target: CommitTarget::AllConsumed,
                reply,
            }
        })
        .unwrap();
        let mut offs: Vec<(i32, i64)> = committed.iter().map(|o| (o.partition, o.offset)).collect();
        offs.sort();
        assert_eq!(offs, [(0, 5), (1, 5)]);

        let selected = request(&a.tx, Duration::from_secs(10), |reply| {
            ConsumerCommand::Commit {
                target: CommitTarget::Selected {
                    topic: t.clone(),
                    partition: 0,
                    offset: 1,
                },
                reply,
            }
        })
        .unwrap();
        assert_eq!(
            selected[0].offset, 2,
            "до выбранного включительно = offset + 1"
        );
        let h = KafkaHandles::create(&common::plaintext_profile(&bs2), &MemoryStore::default())
            .unwrap();
        let rows = group_offsets(&h, &g).unwrap();
        assert_eq!(
            rows.iter().map(|r| r.committed).collect::<Vec<_>>(),
            [Some(2), Some(5)]
        );

        let b = start(&bs2, &g, &t);
        wait_until("ребаланс: A отдала партицию", || {
            a.sink.revoked() && a.sink.last_assigned() == Some(1)
        });
        wait_until("B получила партицию", || {
            b.sink.last_assigned() == Some(1)
        });

        for s in [&a, &b] {
            s.cancel.store(true, Ordering::Relaxed);
        }
        let started = Instant::now();
        for s in [a, b] {
            s.handle.join().unwrap();
            assert!(s.sink.finished());
        }
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "остановка с выходом из группы не зависает"
        );
    })
    .await
    .unwrap();
}
