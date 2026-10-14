mod common;

use std::time::{Duration, Instant};

use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::kafka::cluster::cluster_overview;
use kafka_explorer_lib::kafka::pool::KafkaHandles;
use kafka_explorer_lib::kafka::topics::{list_topics, topic_details};
use kafka_explorer_lib::secrets::MemoryStore;

/// Порог заметно меньше таймаута запроса (30 с): брокер явно недоступен — ждать нечего.
const FAST: Duration = Duration::from_secs(8);

/// Брокер не слушает порт (connection refused): ошибка CONNECTION_FAILED должна приходить
/// за секунды, а не через полный таймаут запроса — в том числе на повторных операциях,
/// когда librdkafka подавляет повтор одинаковой ошибки.
#[test]
fn unreachable_broker_fails_fast() {
    let mut p = common::plaintext_profile("127.0.0.1:1");
    p.request_timeout_ms = 30_000;
    let h = KafkaHandles::create(&p, &MemoryStore::default()).unwrap();

    for attempt in 0..2 {
        let t = Instant::now();
        let err = list_topics(&h).unwrap_err();
        assert_eq!(
            err.code,
            ErrorCode::ConnectionFailed,
            "попытка {attempt}: {err:?}"
        );
        assert!(t.elapsed() < FAST, "попытка {attempt}: {:?}", t.elapsed());
    }

    let t = Instant::now();
    let err = cluster_overview(&h).unwrap_err();
    assert_eq!(err.code, ErrorCode::ConnectionFailed, "{err:?}");
    assert!(t.elapsed() < FAST, "{:?}", t.elapsed());

    let t = Instant::now();
    let err = topic_details(&h, "any").unwrap_err();
    assert_eq!(err.code, ErrorCode::ConnectionFailed, "{err:?}");
    assert!(t.elapsed() < FAST, "{:?}", t.elapsed());
}
