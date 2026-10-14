use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;

use crate::error::{AppError, ErrorCode, classify_message, from_kafka};
use crate::kafka::context::CapturingContext;

/// Сколько максимум ждём доставки отложенных колбэков ошибок клиента.
const ERROR_DRAIN: Duration = Duration::from_millis(1500);
const DRAIN_STEP: Duration = Duration::from_millis(100);

/// Превращает ошибку операции в AppError с реальной причиной (TLS/SASL).
///
/// В rust-rdkafka `ClientContext::error` вызывается только во время poll очереди клиента,
/// поэтому перед классификацией консьюмер коротко опрашивается, пока не придёт ошибка,
/// полученная не раньше `started` (момента начала операции), или не истечёт ERROR_DRAIN.
/// Сообщения, если они придут, игнорируются: у pooled/metadata-консьюмеров нет assignment,
/// а browse-сессия на этом пути и так завершается ошибкой.
pub fn op_error(
    consumer: &BaseConsumer<CapturingContext>,
    started: Instant,
    err: KafkaError,
) -> AppError {
    let ctx = consumer.context();
    if needs_client_error(&err) {
        let deadline = Instant::now() + ERROR_DRAIN;
        while ctx.last_error_since(started).is_none() && Instant::now() < deadline {
            let _ = consumer.poll(DRAIN_STEP);
        }
    }
    from_kafka(&err, ctx.last_error_since(started).as_deref())
}

/// Ошибка операции сама по себе не объясняет причину (таймаут/транспорт/неизвестно) —
/// стоит дождаться колбэка ошибок клиента. Остальные (например, нет прав) не задерживаем.
fn needs_client_error(err: &KafkaError) -> bool {
    matches!(
        classify_message(&err.to_string()),
        None | Some(ErrorCode::Timeout) | Some(ErrorCode::ConnectionFailed)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::types::RDKafkaErrorCode as C;

    #[test]
    fn drains_only_for_ambiguous_errors() {
        assert!(needs_client_error(&KafkaError::MetadataFetch(
            C::BrokerTransportFailure
        )));
        assert!(needs_client_error(&KafkaError::MetadataFetch(
            C::OperationTimedOut
        )));
        assert!(!needs_client_error(&KafkaError::AdminOp(
            C::TopicAuthorizationFailed
        )));
    }
}
