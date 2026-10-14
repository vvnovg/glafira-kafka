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
    let cause = pick_client_error(&err, ctx.last_error_since(started), ctx.last_error());
    from_kafka(&err, cause.as_deref())
}

/// Причина для классификации: свежая ошибка клиента, а если её нет — более старая, но только
/// AUTH/TLS. librdkafka подавляет повтор одинаковой ошибки брокера 30 с, поэтому повторная
/// операция может не получить колбэка; устаревшая транспортная ошибка при этом не должна
/// перекрывать собственную ошибку операции.
fn pick_client_error(
    err: &KafkaError,
    fresh: Option<String>,
    stale: Option<String>,
) -> Option<String> {
    if fresh.is_some() || !needs_client_error(err) {
        return fresh;
    }
    stale.filter(|s| {
        matches!(
            classify_message(s),
            Some(ErrorCode::AuthFailed) | Some(ErrorCode::TlsError)
        )
    })
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
    fn stale_error_used_only_for_auth_or_tls() {
        let timeout = KafkaError::MetadataFetch(C::OperationTimedOut);
        let auth = Some("SASL authentication error: Authentication failed".to_string());
        let tls = Some("SSL handshake failed".to_string());
        let transport = Some("Broker transport failure: Connection refused".to_string());
        let fresh = Some("Local: All broker connections are down".to_string());
        // свежая ошибка всегда побеждает
        assert_eq!(
            pick_client_error(&timeout, fresh.clone(), auth.clone()),
            fresh
        );
        // нет свежей: устаревшая AUTH/TLS используется
        assert_eq!(pick_client_error(&timeout, None, auth.clone()), auth);
        assert_eq!(pick_client_error(&timeout, None, tls.clone()), tls);
        // устаревшая транспортная — нет
        assert_eq!(pick_client_error(&timeout, None, transport), None);
        // ошибка операции сама по себе однозначна — устаревшая не подставляется
        let denied = KafkaError::AdminOp(C::TopicAuthorizationFailed);
        assert_eq!(pick_client_error(&denied, None, auth), None);
    }

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
