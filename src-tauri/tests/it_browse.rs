mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::TestMsg;
use kafka_explorer_lib::browse::filter::{FilterSpec, HeaderMatch, TextMatch};
use kafka_explorer_lib::browse::format::DataFormat;
use kafka_explorer_lib::browse::plan::StartPosition;
use kafka_explorer_lib::browse::session::{BrowseParams, EventSink, run_browse};
use kafka_explorer_lib::browse::types::{BrowseEvent, BrowsedMessage, FinishReason};
use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::secrets::MemoryStore;

/// Снимает зависание тест-бинарника: при панике в ассертах выставляет cancel.
struct CancelOnDrop(Arc<AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[derive(Clone, Default)]
struct VecSink(Arc<Mutex<Vec<BrowseEvent>>>);

impl EventSink for VecSink {
    fn send(&self, event: BrowseEvent) -> bool {
        self.0.lock().unwrap().push(event);
        true
    }
}

impl VecSink {
    fn messages(&self) -> Vec<BrowsedMessage> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .flat_map(|e| match e {
                BrowseEvent::Messages { messages } => messages.clone(),
                _ => vec![],
            })
            .collect()
    }

    fn last(&self) -> BrowseEvent {
        self.0.lock().unwrap().last().cloned().unwrap()
    }

    fn has_reached_end(&self) -> bool {
        self.0
            .lock()
            .unwrap()
            .iter()
            .any(|e| matches!(e, BrowseEvent::ReachedEnd))
    }
}

fn params(topic: &str, start: StartPosition, limit: usize, filter: FilterSpec) -> BrowseParams {
    BrowseParams {
        connection_id: "it".into(),
        topic: topic.into(),
        partitions: None,
        start,
        limit,
        live: false,
        key_format: DataFormat::Auto,
        value_format: DataFormat::Auto,
        filter,
    }
}

async fn browse(bs: &str, p: BrowseParams) -> VecSink {
    let sink = VecSink::default();
    let (s, profile) = (sink.clone(), common::plaintext_profile(bs));
    tokio::task::spawn_blocking(move || {
        run_browse(
            &profile,
            &MemoryStore::default(),
            p,
            Arc::new(AtomicBool::new(false)),
            &s,
        )
    })
    .await
    .unwrap();
    sink
}

/// 30 сообщений по 3 партициям: key k{i}, value {"n":i,"status":"OK"|"FAILED"(i=7)}, header trace=t{i}.
async fn seeded_topic(bs: &str) -> String {
    let topic = common::unique_topic("it-browse");
    common::create_topic(bs, &topic, 3).await;
    let keys: Vec<String> = (0..30).map(|i| format!("k{i}")).collect();
    let values: Vec<String> = (0..30)
        .map(|i| {
            format!(
                r#"{{"n":{i},"status":"{}"}}"#,
                if i == 7 { "FAILED" } else { "OK" }
            )
        })
        .collect();
    let traces: Vec<String> = (0..30).map(|i| format!("t{i}")).collect();
    let headers: Vec<[(&str, &str); 1]> = traces.iter().map(|t| [("trace", t.as_str())]).collect();
    let msgs: Vec<TestMsg> = (0..30)
        .map(|i| TestMsg {
            partition: (i % 3) as i32,
            key: Some(keys[i].as_str()),
            value: &values[i],
            headers: &headers[i],
        })
        .collect();
    common::produce(bs, &topic, &msgs).await;
    topic
}

#[tokio::test(flavor = "multi_thread")]
async fn earliest_reads_everything_and_stops_at_end() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = seeded_topic(&bs).await;
    let sink = browse(
        &bs,
        params(&topic, StartPosition::Earliest, 1000, FilterSpec::default()),
    )
    .await;
    let msgs = sink.messages();
    assert_eq!(msgs.len(), 30);
    assert_eq!(msgs[0].value.as_ref().unwrap().format, DataFormat::Json);
    assert_eq!(msgs[0].headers[0].name, "trace");
    assert!(matches!(
        sink.last(),
        BrowseEvent::Finished {
            reason: FinishReason::EndOfTopic,
            scanned: 30,
            matched: 30
        }
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn latest_count_takes_tail_of_each_partition() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = seeded_topic(&bs).await;
    let msgs = browse(
        &bs,
        params(
            &topic,
            StartPosition::Latest { count: 6 },
            1000,
            FilterSpec::default(),
        ),
    )
    .await
    .messages();
    assert_eq!(msgs.len(), 6);
    assert!(msgs.iter().all(|m| m.offset >= 8)); // в каждой партиции 10 сообщений → offsets 8, 9
}

#[tokio::test(flavor = "multi_thread")]
async fn limit_stops_early() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = seeded_topic(&bs).await;
    let sink = browse(
        &bs,
        params(&topic, StartPosition::Earliest, 5, FilterSpec::default()),
    )
    .await;
    assert_eq!(sink.messages().len(), 5);
    assert!(matches!(
        sink.last(),
        BrowseEvent::Finished {
            reason: FinishReason::Limit,
            ..
        }
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn filters_by_json_and_header() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = seeded_topic(&bs).await;
    let json = FilterSpec {
        json_path: Some(r#"$.status == "FAILED""#.into()),
        ..Default::default()
    };
    let msgs = browse(&bs, params(&topic, StartPosition::Earliest, 1000, json))
        .await
        .messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].key.as_ref().unwrap().text, "k7");

    let header = FilterSpec {
        header: Some(HeaderMatch {
            name: "trace".into(),
            value: Some(TextMatch {
                pattern: "^t3$".into(),
                regex: true,
                case_sensitive: true,
            }),
        }),
        ..Default::default()
    };
    let msgs = browse(&bs, params(&topic, StartPosition::Earliest, 1000, header))
        .await
        .messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].key.as_ref().unwrap().text, "k3");
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_topic_reports_not_found() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let sink = browse(
        &bs,
        params(
            &common::unique_topic("it-missing"),
            StartPosition::Earliest,
            10,
            FilterSpec::default(),
        ),
    )
    .await;
    match sink.last() {
        BrowseEvent::Error { error } => assert_eq!(error.code, ErrorCode::NotFound),
        other => panic!("ожидали Error, получили {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn live_tail_receives_new_messages_until_cancelled() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::unique_topic("it-live");
    common::create_topic(&bs, &topic, 1).await;
    common::produce(
        &bs,
        &topic,
        &[TestMsg {
            partition: 0,
            key: None,
            value: "old",
            headers: &[],
        }],
    )
    .await;

    let sink = VecSink::default();
    let cancel = Arc::new(AtomicBool::new(false));
    let _guard = CancelOnDrop(cancel.clone());
    let mut p = params(
        &topic,
        StartPosition::Latest { count: 1 },
        1000,
        FilterSpec::default(),
    );
    p.live = true;
    let (s, c, profile) = (sink.clone(), cancel.clone(), common::plaintext_profile(&bs));
    let handle = tokio::task::spawn_blocking(move || {
        run_browse(&profile, &MemoryStore::default(), p, c, &s)
    });

    let deadline = Instant::now() + Duration::from_secs(15);
    while !sink.has_reached_end() {
        assert!(Instant::now() < deadline, "не дождались ReachedEnd");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let new: Vec<TestMsg> = (0..3)
        .map(|_| TestMsg {
            partition: 0,
            key: None,
            value: "new",
            headers: &[],
        })
        .collect();
    common::produce(&bs, &topic, &new).await;
    while sink.messages().len() < 4 {
        assert!(Instant::now() < deadline, "не дождались новых сообщений");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cancel.store(true, Ordering::Relaxed);
    handle.await.unwrap();
    assert!(matches!(
        sink.last(),
        BrowseEvent::Finished {
            reason: FinishReason::Cancelled,
            ..
        }
    ));
}
