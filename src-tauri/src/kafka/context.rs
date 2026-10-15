use std::sync::{Arc, Mutex};
use std::time::Instant;

use rdkafka::TopicPartitionList;
use rdkafka::client::ClientContext;
use rdkafka::consumer::{BaseConsumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;
use serde::Serialize;

use crate::kafka::groups::TopicPartitionRef;

/// Запись журнала ребалансов consumer-сессии (время — мс от эпохи).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum RebalanceRecord {
    Assigned {
        partitions: Vec<TopicPartitionRef>,
        at: i64,
    },
    Revoked {
        partitions: Vec<TopicPartitionRef>,
        at: i64,
    },
    Error {
        message: String,
        at: i64,
    },
}

fn refs(tpl: &TopicPartitionList) -> Vec<TopicPartitionRef> {
    let mut v: Vec<TopicPartitionRef> = tpl
        .elements()
        .iter()
        .map(|e| TopicPartitionRef {
            topic: e.topic().to_string(),
            partition: e.partition(),
        })
        .collect();
    v.sort();
    v
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Запоминает последнюю ошибку клиента (TLS, SASL, транспорт) вместе со временем её получения,
/// чтобы показать пользователю реальную причину, а не просто «таймаут».
/// Колбэк вызывается только во время poll очереди клиента (см. `kafka::errors::op_error`).
#[derive(Clone, Default)]
pub struct CapturingContext {
    last_error: Arc<Mutex<Option<(Instant, String)>>>,
    rebalances: Arc<Mutex<Vec<RebalanceRecord>>>,
}

impl CapturingContext {
    pub fn last_error(&self) -> Option<String> {
        self.lock().as_ref().map(|(_, e)| e.clone())
    }

    /// Последняя ошибка, полученная не раньше `t` (старые «залипшие» ошибки не возвращаются).
    pub fn last_error_since(&self, t: Instant) -> Option<String> {
        self.lock()
            .as_ref()
            .filter(|(at, _)| *at >= t)
            .map(|(_, e)| e.clone())
    }

    pub fn clear(&self) {
        *self.lock() = None;
    }

    /// Ребалансы с прошлого вызова (заполняется только у consumer'ов с subscribe).
    pub fn take_rebalances(&self) -> Vec<RebalanceRecord> {
        std::mem::take(&mut *self.rebalances.lock().unwrap_or_else(|p| p.into_inner()))
    }

    fn record_rebalance(&self, r: RebalanceRecord) {
        // Вызывается из FFI-колбэка: отравленный мьютекс не должен приводить к панике.
        self.rebalances
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(r);
    }

    fn record(&self, at: Instant, message: String) {
        *self.lock() = Some((at, message));
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<(Instant, String)>> {
        // Вызывается и из FFI-колбэка: отравленный мьютекс не должен приводить к панике.
        self.last_error.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl ClientContext for CapturingContext {
    fn error(&self, error: KafkaError, reason: &str) {
        // Конец партиции при enable.partition.eof — штатное событие, а не ошибка клиента.
        if error.rdkafka_error_code() == Some(RDKafkaErrorCode::PartitionEOF) {
            tracing::debug!(reason, "partition EOF");
            return;
        }
        tracing::warn!(%error, reason, "kafka client error");
        self.record(Instant::now(), format!("{error}: {reason}"));
    }
}

impl ConsumerContext for CapturingContext {
    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let at = now_ms();
        self.record_rebalance(match rebalance {
            Rebalance::Assign(tpl) => RebalanceRecord::Assigned {
                partitions: refs(tpl),
                at,
            },
            Rebalance::Revoke(tpl) => RebalanceRecord::Revoked {
                partitions: refs(tpl),
                at,
            },
            Rebalance::Error(e) => RebalanceRecord::Error {
                message: e.to_string(),
                at,
            },
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn last_error_since_filters_older_errors() {
        let ctx = CapturingContext::default();
        let t0 = Instant::now();
        assert_eq!(ctx.last_error_since(t0), None);
        ctx.record(t0, "old".into());
        let t1 = t0 + Duration::from_millis(10);
        assert_eq!(ctx.last_error_since(t0).as_deref(), Some("old"));
        assert_eq!(
            ctx.last_error_since(t1),
            None,
            "ошибка старше t1 не возвращается"
        );
        assert_eq!(ctx.last_error().as_deref(), Some("old"));
        ctx.record(t1, "new".into());
        assert_eq!(ctx.last_error_since(t1).as_deref(), Some("new"));
        ctx.clear();
        assert_eq!(ctx.last_error(), None);
    }

    #[test]
    fn error_callback_records_message() {
        let ctx = CapturingContext::default();
        let before = Instant::now();
        ctx.error(
            KafkaError::Global(rdkafka::types::RDKafkaErrorCode::Authentication),
            "SASL authentication error",
        );
        let e = ctx.last_error_since(before).unwrap();
        assert!(e.contains("SASL authentication error"), "{e}");
    }

    #[test]
    fn rebalance_log_is_taken_once() {
        let ctx = CapturingContext::default();
        assert!(ctx.take_rebalances().is_empty());
        ctx.record_rebalance(RebalanceRecord::Error {
            message: "x".into(),
            at: 1,
        });
        ctx.record_rebalance(RebalanceRecord::Assigned {
            partitions: vec![TopicPartitionRef {
                topic: "t".into(),
                partition: 0,
            }],
            at: 2,
        });
        let taken = ctx.take_rebalances();
        assert_eq!(taken.len(), 2);
        assert!(ctx.take_rebalances().is_empty());
        assert_eq!(
            serde_json::to_value(&taken[1]).unwrap(),
            serde_json::json!({"kind": "assigned", "partitions": [{"topic": "t", "partition": 0}], "at": 2})
        );
    }

    #[test]
    fn partition_eof_is_not_an_error() {
        let ctx = CapturingContext::default();
        ctx.error(
            KafkaError::Global(rdkafka::types::RDKafkaErrorCode::PartitionEOF),
            "Fetch from broker 1 reached end of partition at offset 20",
        );
        assert_eq!(
            ctx.last_error(),
            None,
            "конец партиции — штатное событие, не причина ошибок"
        );
    }
}
