mod common;

use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kafka_explorer_lib::kafka::pool::KafkaHandles;
use kafka_explorer_lib::kafka::producer::ProducerSettings;
use kafka_explorer_lib::kafka::topics::topic_details;
use kafka_explorer_lib::produce::batch::{
    BatchEvent, BatchParams, BatchSink, BatchSource, run_batch,
};
use kafka_explorer_lib::produce::payload::{PayloadFormat, PayloadInput, ProduceMessage};
use kafka_explorer_lib::produce::source::FileFormat;
use kafka_explorer_lib::secrets::MemoryStore;

#[derive(Clone, Default)]
struct VecSink(Arc<Mutex<Vec<BatchEvent>>>);

impl BatchSink for VecSink {
    fn send(&self, e: BatchEvent) -> bool {
        self.0.lock().unwrap().push(e);
        true
    }
}

impl VecSink {
    /// (sent, failed, notSent, cancelled)
    fn finished(&self) -> (u64, u64, u64, bool) {
        match self.0.lock().unwrap().last().cloned() {
            Some(BatchEvent::Finished {
                sent,
                failed,
                not_sent,
                cancelled,
            }) => (sent, failed, not_sent, cancelled),
            other => panic!("ожидали Finished, получили {other:?}"),
        }
    }

    fn errors(&self) -> Vec<(Option<u64>, String)> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find_map(|e| match e {
                BatchEvent::Errors { errors } => {
                    Some(errors.iter().map(|x| (x.line, x.message.clone())).collect())
                }
                _ => None,
            })
            .unwrap_or_default()
    }
}

fn run(bs: &str, params: BatchParams, cancel: Arc<AtomicBool>) -> VecSink {
    let sink = VecSink::default();
    run_batch(
        &common::plaintext_profile(bs),
        &MemoryStore::default(),
        params,
        cancel,
        &sink,
    );
    sink
}

fn copies(topic: &str, count: u64, rate: Option<u32>) -> BatchParams {
    BatchParams {
        connection_id: "it".into(),
        source: BatchSource::Copies {
            count,
            message: ProduceMessage {
                topic: topic.into(),
                partition: None,
                key: PayloadInput {
                    format: PayloadFormat::String,
                    text: "k{{seq}}".into(),
                },
                value: PayloadInput {
                    format: PayloadFormat::Json,
                    text: r#"{"n":{{seq}}}"#.into(),
                },
                headers: vec![],
            },
        },
        settings: ProducerSettings::default(),
        rate_per_sec: rate,
    }
}

async fn message_count(bs: &str, topic: &str) -> Option<i64> {
    let h = KafkaHandles::create(&common::plaintext_profile(bs), &MemoryStore::default()).unwrap();
    let t = topic.to_string();
    tokio::task::spawn_blocking(move || topic_details(&h, &t).unwrap().message_count)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn thousand_copies() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-batch", 3).await;
    let (bs2, t) = (bs.clone(), topic.to_string());
    let sink = tokio::task::spawn_blocking(move || {
        run(
            &bs2,
            copies(&t, 1000, None),
            Arc::new(AtomicBool::new(false)),
        )
    })
    .await
    .unwrap();
    assert_eq!(sink.finished(), (1000, 0, 0, false));
    assert_eq!(message_count(&bs, &topic).await, Some(1000));
}

#[tokio::test(flavor = "multi_thread")]
async fn jsonl_file_with_bad_line() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-batch-file", 1).await;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    writeln!(
        f,
        r#"{{"key":"a","value":{{"n":1}},"headers":{{"src":"file"}}}}"#
    )
    .unwrap();
    writeln!(f, "{{oops").unwrap();
    writeln!(f, r#"{{"value":"plain","partition":0}}"#).unwrap();
    let params = BatchParams {
        connection_id: "it".into(),
        source: BatchSource::File {
            path: f.path().to_string_lossy().into(),
            format: FileFormat::Jsonl,
            topic: topic.to_string(),
        },
        settings: ProducerSettings::default(),
        rate_per_sec: None,
    };
    let bs2 = bs.clone();
    let sink =
        tokio::task::spawn_blocking(move || run(&bs2, params, Arc::new(AtomicBool::new(false))))
            .await
            .unwrap();
    assert_eq!(sink.finished(), (2, 1, 0, false));
    let errors = sink.errors();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].0, Some(2));
    assert_eq!(message_count(&bs, &topic).await, Some(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_stops_rate_limited_batch() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-batch-cancel", 1).await;
    let cancel = Arc::new(AtomicBool::new(false));
    let (bs2, t, c2) = (bs.clone(), topic.to_string(), cancel.clone());
    let task = tokio::task::spawn_blocking(move || run(&bs2, copies(&t, 100_000, Some(200)), c2));
    tokio::time::sleep(Duration::from_millis(500)).await;
    cancel.store(true, Ordering::Relaxed);
    let started = std::time::Instant::now();
    let sink = task.await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "отмена срабатывает быстро"
    );
    let (sent, failed, not_sent, cancelled) = sink.finished();
    assert!(cancelled);
    assert_eq!(failed, 0);
    // Ждавшее в ограничителе скорости + не ушедшее из локальной очереди (linger) — единицы.
    assert!(not_sent < 20, "отменено до отправки: {not_sent}");
    assert!(sent > 0 && sent < 1000, "отправлено {sent}");
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_of_unthrottled_batch_does_not_count_queued_as_failed() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-batch-cancel-fast", 1).await;
    let cancel = Arc::new(AtomicBool::new(false));
    let (bs2, t, c2) = (bs.clone(), topic.to_string(), cancel.clone());
    let task = tokio::task::spawn_blocking(move || run(&bs2, copies(&t, 1_000_000, None), c2));
    tokio::time::sleep(Duration::from_millis(500)).await;
    cancel.store(true, Ordering::Relaxed);
    let started = std::time::Instant::now();
    let sink = task.await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "отмена срабатывает быстро"
    );
    let (sent, failed, not_sent, cancelled) = sink.finished();
    assert!(cancelled);
    assert_eq!(
        failed, 0,
        "вынутое из локальной очереди — не ошибки брокера"
    );
    assert!(not_sent > 0, "часть пакета отменена до отправки");
    assert!(
        sent + not_sent < 1_000_000,
        "отправлено {sent}, отменено {not_sent}"
    );
    assert!(sink.errors().is_empty());
    // Вынутое из очереди в Kafka не попало.
    assert_eq!(message_count(&bs, &topic).await, Some(sent as i64));
}
