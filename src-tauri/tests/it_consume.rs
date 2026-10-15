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
use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::kafka::context::RebalanceRecord;
use kafka_explorer_lib::kafka::group_ops::group_offsets;
use kafka_explorer_lib::kafka::groups::TopicPartitionRef;
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

        let committed = request(&a.tx, Duration::from_secs(10), |reply, deadline| {
            ConsumerCommand::Commit {
                target: CommitTarget::AllConsumed,
                reply,
                deadline,
            }
        })
        .unwrap();
        let mut offs: Vec<(i32, i64)> = committed.iter().map(|o| (o.partition, o.offset)).collect();
        offs.sort();
        assert_eq!(offs, [(0, 5), (1, 5)]);

        let selected = request(&a.tx, Duration::from_secs(10), |reply, deadline| {
            ConsumerCommand::Commit {
                target: CommitTarget::Selected {
                    topic: t.clone(),
                    partition: 0,
                    offset: 1,
                },
                reply,
                deadline,
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

#[tokio::test(flavor = "multi_thread")]
async fn pause_resume_and_unassigned_partition_error() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-consume-pause", 1).await;
    let msg = |v| TestMsg {
        partition: 0,
        key: None,
        value: v,
        headers: &[],
    };
    common::produce(&bs, &topic, &[msg("a"), msg("b"), msg("c")]).await;
    let group = common::TempGroup::new(&bs, "it-consume-pause");
    let (bs2, t, g) = (bs.clone(), topic.to_string(), group.to_string());

    tokio::task::spawn_blocking(move || {
        let s = start(&bs2, &g, &t);
        wait_until("3 сообщения", || s.sink.messages() == 3);
        let part = |topic: &str, partition| {
            vec![TopicPartitionRef {
                topic: topic.to_string(),
                partition,
            }]
        };

        let e = request(&s.tx, Duration::from_secs(10), |reply, deadline| {
            ConsumerCommand::Pause {
                partitions: part(&t, 7),
                reply,
                deadline,
            }
        })
        .unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput, "непринадлежащая партиция");
        let e = request(&s.tx, Duration::from_secs(10), |reply, deadline| {
            ConsumerCommand::Resume {
                partitions: part("other-topic", 0),
                reply,
                deadline,
            }
        })
        .unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);

        request(&s.tx, Duration::from_secs(10), |reply, deadline| {
            ConsumerCommand::Pause {
                partitions: part(&t, 0),
                reply,
                deadline,
            }
        })
        .unwrap();
        let (bs3, t3) = (bs2.clone(), t.clone());
        common::block_on(async move { common::produce(&bs3, &t3, &[msg("d"), msg("e")]).await });
        std::thread::sleep(Duration::from_secs(2));
        assert_eq!(s.sink.messages(), 3, "на паузе новые сообщения не приходят");

        request(&s.tx, Duration::from_secs(10), |reply, deadline| {
            ConsumerCommand::Resume {
                partitions: part(&t, 0),
                reply,
                deadline,
            }
        })
        .unwrap();
        wait_until("после resume дошли 2 сообщения", || {
            s.sink.messages() == 5
        });

        // Просроченная команда не выполняется и не порождает событий.
        let before = s.sink.0.lock().unwrap().len();
        let (reply, rx) = mpsc::channel();
        s.tx.send(ConsumerCommand::Pause {
            partitions: part(&t, 0),
            reply,
            deadline: Instant::now() - Duration::from_secs(1),
        })
        .unwrap();
        assert!(rx.recv_timeout(Duration::from_secs(5)).is_err());
        let paused = s
            .sink
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|e| matches!(e, ConsumeEvent::Paused { .. }));
        assert!(paused, "реальная пауза выше эмитила Paused");
        let events = s.sink.0.lock().unwrap();
        let pauses = events
            .iter()
            .skip(before)
            .filter(|e| matches!(e, ConsumeEvent::Paused { .. }))
            .count();
        assert_eq!(pauses, 0, "просроченная команда не эмитит события");
        drop(events);

        s.cancel.store(true, Ordering::Relaxed);
        s.handle.join().unwrap();
    })
    .await
    .unwrap();
}

/// Выход из приложения (`SessionRegistry::shutdown`): сессия успевает выйти из группы,
/// участник не висит до session.timeout.ms.
#[tokio::test(flavor = "multi_thread")]
async fn app_shutdown_waits_for_session_to_leave_group() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-consume-exit", 1).await;
    let group = common::TempGroup::new(&bs, "it-consume-exit");
    let (t, g) = (topic.to_string(), group.to_string());
    tokio::task::spawn_blocking(move || {
        let dir = tempfile::tempdir().unwrap();
        let st =
            kafka_explorer_lib::state::AppState::new(dir.path(), Arc::new(MemoryStore::default()))
                .unwrap();
        st.connections
            .upsert(common::plaintext_profile(&bs))
            .unwrap();
        let params = ConsumerParams {
            connection_id: "it".into(),
            group_id: g.clone(),
            topics: vec![t],
            topic_pattern: None,
            auto_offset_reset: OffsetReset::Earliest,
            enable_auto_commit: false,
            isolation_level: IsolationLevel::ReadCommitted,
            key_format: DataFormat::Auto,
            value_format: DataFormat::Auto,
            filter: FilterSpec::default(),
        };
        let sink = VecSink::default();
        kafka_explorer_lib::commands::consume::start_consumer_impl(&st, params, sink.clone(), None)
            .unwrap();
        wait_until("сессия получила партицию", || {
            sink.last_assigned() == Some(1)
        });
        let started = Instant::now();
        assert!(st.sessions.shutdown(Duration::from_secs(5)));
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(sink.finished());
        let h =
            KafkaHandles::create(&common::plaintext_profile(&bs), &MemoryStore::default()).unwrap();
        let d = kafka_explorer_lib::kafka::group_ops::describe_group(&h, &g).unwrap();
        assert!(d.members.is_empty(), "участник вышел из группы: {d:?}");
    })
    .await
    .unwrap();
}
