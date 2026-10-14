use std::sync::{Arc, Mutex};
use std::time::Instant;

use rdkafka::client::ClientContext;
use rdkafka::consumer::ConsumerContext;
use rdkafka::error::KafkaError;

/// Запоминает последнюю ошибку клиента (TLS, SASL, транспорт) вместе со временем её получения,
/// чтобы показать пользователю реальную причину, а не просто «таймаут».
/// Колбэк вызывается только во время poll очереди клиента (см. `kafka::errors::op_error`).
#[derive(Clone, Default)]
pub struct CapturingContext {
    last_error: Arc<Mutex<Option<(Instant, String)>>>,
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
        tracing::warn!(%error, reason, "kafka client error");
        self.record(Instant::now(), format!("{error}: {reason}"));
    }
}

impl ConsumerContext for CapturingContext {}

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
}
