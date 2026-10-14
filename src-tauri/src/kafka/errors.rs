use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::metadata::Metadata;

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

/// Первая порция ожидания метаданных: если за это время ответа нет, а клиент уже сообщил,
/// что брокеры недоступны (или отказали в аутентификации/TLS), полный таймаут не ждём.
const FIRST_METADATA_SLICE: Duration = Duration::from_secs(2);
/// librdkafka подавляет повтор одинаковой ошибки брокера 30 с: в этом окне последняя
/// ошибка клиента остаётся актуальной, даже если на текущий запрос колбэка не пришло.
const SUPPRESSION_WINDOW: Duration = Duration::from_secs(30);

/// `fetch_metadata` с быстрым отказом при явно недоступном кластере.
///
/// Сначала ждём не дольше FIRST_METADATA_SLICE; при таймауте доставляем колбэки ошибок
/// клиента и, если недавняя ошибка объясняет отсутствие ответа (транспорт, AUTH, TLS),
/// сразу возвращаем её. Иначе (например, большой кластер отвечает медленно) повторяем
/// запрос с оставшимся временем — второй запрос уже не дробится.
pub fn fetch_metadata_fast_fail(
    consumer: &BaseConsumer<CapturingContext>,
    topic: Option<&str>,
    timeout: Duration,
) -> Result<Metadata, AppError> {
    let started = Instant::now();
    let first = timeout.min(FIRST_METADATA_SLICE);
    let err = match consumer.fetch_metadata(topic, first) {
        Ok(md) => return Ok(md),
        Err(e) => e,
    };
    let remaining = timeout.saturating_sub(started.elapsed());
    if !is_timeout(&err) || remaining.is_zero() {
        return Err(op_error(consumer, started, err));
    }
    // Колбэки ошибок клиента доставляются только во время poll.
    let _ = consumer.poll(Duration::ZERO);
    let recent = started.checked_sub(SUPPRESSION_WINDOW).unwrap_or(started);
    if let Some(cause) = consumer
        .context()
        .last_error_since(recent)
        .filter(|c| explains_no_answer(c))
    {
        return Err(from_kafka(&err, Some(&cause)));
    }
    consumer
        .fetch_metadata(topic, remaining)
        .map_err(|e| op_error(consumer, started, e))
}

fn is_timeout(err: &KafkaError) -> bool {
    classify_message(&err.to_string()) == Some(ErrorCode::Timeout)
}

/// Ошибка клиента, которая объясняет, почему брокер не отвечает: ждать дальше бессмысленно.
fn explains_no_answer(cause: &str) -> bool {
    matches!(
        classify_message(cause),
        Some(ErrorCode::ConnectionFailed) | Some(ErrorCode::AuthFailed) | Some(ErrorCode::TlsError)
    )
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
    fn only_transport_auth_tls_causes_stop_waiting() {
        assert!(explains_no_answer(
            "sasl_plaintext://localhost:9094/1: Connect to ipv4#127.0.0.1:9094 failed: Connection refused"
        ));
        assert!(explains_no_answer("Local: All broker connections are down"));
        assert!(explains_no_answer(
            "SASL authentication error: Authentication failed"
        ));
        assert!(explains_no_answer("SSL handshake failed"));
        assert!(!explains_no_answer("Broker: Topic authorization failed"));
        assert!(!explains_no_answer("something unrelated"));
        assert!(is_timeout(&KafkaError::MetadataFetch(C::OperationTimedOut)));
        assert!(!is_timeout(&KafkaError::MetadataFetch(
            C::BrokerTransportFailure
        )));
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
