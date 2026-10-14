use std::sync::{Arc, Mutex};

use rdkafka::client::ClientContext;
use rdkafka::consumer::ConsumerContext;
use rdkafka::error::KafkaError;

/// Запоминает последнюю ошибку клиента (TLS, SASL, транспорт),
/// чтобы показать пользователю реальную причину, а не просто «таймаут».
#[derive(Clone, Default)]
pub struct CapturingContext {
    last_error: Arc<Mutex<Option<String>>>,
}

impl CapturingContext {
    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().unwrap().clone()
    }

    pub fn clear(&self) {
        *self.last_error.lock().unwrap() = None;
    }
}

impl ClientContext for CapturingContext {
    fn error(&self, error: KafkaError, reason: &str) {
        tracing::warn!(%error, reason, "kafka client error");
        *self.last_error.lock().unwrap() = Some(format!("{error}: {reason}"));
    }
}

impl ConsumerContext for CapturingContext {}
