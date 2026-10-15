use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::metadata::Metadata;
use rdkafka::types::RDKafkaErrorCode;

use crate::error::{AppError, ErrorCode, classify_message, from_kafka};
use crate::kafka::context::CapturingContext;
use crate::kafka::pool::KafkaHandles;

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

/// Ошибка отдельного элемента admin-ответа (топика, ресурса, группы, партиции).
/// `message` — контекст для пользователя, в `details` — код и текст брокера.
pub fn item_error(code: RDKafkaErrorCode, message: &str, context: &str) -> AppError {
    use RDKafkaErrorCode as C;
    let app_code = match code {
        C::TopicAlreadyExists
        | C::InvalidConfig
        | C::InvalidRequest
        | C::PolicyViolation
        | C::InvalidPartitions
        | C::InvalidReplicationFactor
        | C::InvalidReplicaAssignment
        | C::InvalidTopic
        | C::OffsetOutOfRange => ErrorCode::InvalidInput,
        C::TopicAuthorizationFailed
        | C::GroupAuthorizationFailed
        | C::ClusterAuthorizationFailed => ErrorCode::NotAuthorized,
        C::UnknownTopicOrPartition | C::GroupIdNotFound => ErrorCode::NotFound,
        C::NonEmptyGroup | C::UnknownMemberId | C::RebalanceInProgress => ErrorCode::GroupNotEmpty,
        C::RequestTimedOut | C::OperationTimedOut => ErrorCode::Timeout,
        _ => classify_message(message).unwrap_or(ErrorCode::Internal),
    };
    let details = if message.is_empty() {
        code.to_string()
    } else {
        format!("{code}: {message}")
    };
    AppError::new(app_code, context).with_details(details)
}

/// Колбэки ошибок admin-клиента в rust-rdkafka не доставляются, поэтому причину (TLS/SASL)
/// берём у pooled-консьюмера того же подключения; его опрос блокирующий — уводим с async-потока.
pub async fn admin_error(h: &Arc<KafkaHandles>, started: Instant, e: KafkaError) -> AppError {
    let h = h.clone();
    let fallback = e.clone();
    tauri::async_runtime::spawn_blocking(move || h.kafka_err(started, e))
        .await
        .unwrap_or_else(|_| from_kafka(&fallback, None))
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
    let res = fast_fail_flow(
        timeout,
        |slice| consumer.fetch_metadata(topic, slice),
        std::thread::sleep,
        || started.elapsed(),
        || {
            // Колбэки ошибок клиента доставляются только во время poll.
            let _ = consumer.poll(Duration::ZERO);
            let recent = started.checked_sub(SUPPRESSION_WINDOW).unwrap_or(started);
            consumer
                .context()
                .last_error_since(recent)
                .filter(|c| explains_no_answer(c))
        },
    );
    res.map_err(|f| match f {
        FastFail::Op(e) => op_error(consumer, started, e),
        FastFail::NoAnswer(e, cause) => from_kafka(&e, Some(&cause)),
    })
}

/// Исход `fast_fail_flow` при ошибке.
#[derive(Debug)]
enum FastFail {
    /// Ошибка операции (причину клиента ещё предстоит выяснить).
    Op(KafkaError),
    /// Таймаут, который объясняется недавней ошибкой клиента (транспорт/AUTH/TLS).
    NoAnswer(KafkaError, String),
}

/// Логика `fetch_metadata_fast_fail` без привязки к клиенту (для тестов).
/// Транзитные ошибки брокера повторяются в пределах всего `timeout`, в том числе
/// за границей первой порции; быстрый отказ `explain` проверяется только при таймауте.
fn fast_fail_flow<T>(
    timeout: Duration,
    mut fetch: impl FnMut(Duration) -> Result<T, KafkaError>,
    mut sleep: impl FnMut(Duration),
    elapsed: impl Fn() -> Duration,
    mut explain: impl FnMut() -> Option<String>,
) -> Result<T, FastFail> {
    let first = timeout.min(FIRST_METADATA_SLICE);
    let err = match retry_loop(first, &mut fetch, &mut sleep, &elapsed) {
        Ok(v) => return Ok(v),
        Err(e) => e,
    };
    let remaining = timeout.saturating_sub(elapsed());
    let timed_out = is_timeout(&err);
    if remaining.is_zero() || !(timed_out || is_retriable_metadata_error(&err)) {
        return Err(FastFail::Op(err));
    }
    if timed_out && let Some(cause) = explain() {
        return Err(FastFail::NoAnswer(err, cause));
    }
    retry_loop(remaining, &mut fetch, &mut sleep, &elapsed).map_err(FastFail::Op)
}

/// Ошибки метаданных, которые брокер возвращает кратковременно (смена лидера после
/// изменения топика, выборы): повтор через короткую паузу обычно проходит.
fn is_retriable_metadata_error(err: &KafkaError) -> bool {
    use RDKafkaErrorCode as C;
    matches!(
        err,
        KafkaError::MetadataFetch(
            C::NotLeaderForPartition
                | C::LeaderNotAvailable
                | C::BrokerNotAvailable
                | C::ReplicaNotAvailable
                | C::UnknownLeaderEpoch
                | C::FencedLeaderEpoch
        )
    )
}

/// Пауза перед повтором номер `attempt` (с нуля): 200, 400, 800, 1600 мс и далее 1600 мс.
fn retry_backoff(attempt: u32) -> Duration {
    Duration::from_millis(200u64 << attempt.min(3))
}

/// Повторяет `fetch` при транзитных ошибках брокера в пределах `budget`.
fn retry_loop<T>(
    budget: Duration,
    fetch: &mut impl FnMut(Duration) -> Result<T, KafkaError>,
    sleep: &mut impl FnMut(Duration),
    elapsed: &impl Fn() -> Duration,
) -> Result<T, KafkaError> {
    let start = elapsed();
    let spent = || elapsed().saturating_sub(start);
    let mut attempt = 0;
    loop {
        match fetch(budget.saturating_sub(spent())) {
            Err(e) if is_retriable_metadata_error(&e) => {
                let pause = retry_backoff(attempt);
                if budget.saturating_sub(spent()) <= pause {
                    return Err(e);
                }
                sleep(pause);
                attempt += 1;
            }
            other => return other,
        }
    }
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
    fn item_errors_map_to_app_codes() {
        use crate::error::ErrorCode as E;
        let cases = [
            (C::TopicAlreadyExists, E::InvalidInput),
            (C::InvalidConfig, E::InvalidInput),
            (C::InvalidPartitions, E::InvalidInput),
            (C::InvalidReplicationFactor, E::InvalidInput),
            (C::PolicyViolation, E::InvalidInput),
            (C::TopicAuthorizationFailed, E::NotAuthorized),
            (C::GroupAuthorizationFailed, E::NotAuthorized),
            (C::UnknownTopicOrPartition, E::NotFound),
            (C::GroupIdNotFound, E::NotFound),
            (C::NonEmptyGroup, E::GroupNotEmpty),
            (C::UnknownMemberId, E::GroupNotEmpty),
            (C::RebalanceInProgress, E::GroupNotEmpty),
            (C::RequestTimedOut, E::Timeout),
            (C::Unknown, E::Internal),
        ];
        for (code, expected) in cases {
            assert_eq!(item_error(code, "", "ctx").code, expected, "{code:?}");
        }
        let e = item_error(
            C::InvalidConfig,
            "Invalid value abc for configuration retention.ms",
            "Не удалось изменить конфигурацию топика",
        );
        assert_eq!(e.message, "Не удалось изменить конфигурацию топика");
        assert!(e.details.unwrap().contains("retention.ms"));
    }

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

    #[test]
    fn retriable_metadata_errors() {
        for c in [
            C::NotLeaderForPartition,
            C::LeaderNotAvailable,
            C::BrokerNotAvailable,
            C::ReplicaNotAvailable,
            C::UnknownLeaderEpoch,
            C::FencedLeaderEpoch,
        ] {
            assert!(
                is_retriable_metadata_error(&KafkaError::MetadataFetch(c)),
                "{c:?}"
            );
        }
        for c in [
            C::OperationTimedOut,
            C::BrokerTransportFailure,
            C::TopicAuthorizationFailed,
            C::UnknownTopicOrPartition,
        ] {
            assert!(
                !is_retriable_metadata_error(&KafkaError::MetadataFetch(c)),
                "{c:?}"
            );
        }
        assert!(!is_retriable_metadata_error(&KafkaError::AdminOp(
            C::NotLeaderForPartition
        )));
    }

    #[test]
    fn retry_backoff_doubles_and_caps() {
        let ms = |n| retry_backoff(n).as_millis();
        assert_eq!((ms(0), ms(1), ms(2)), (200, 400, 800));
        assert_eq!(ms(10), 1600);
    }

    /// Модельные часы: sleep сдвигает время, fetch тоже может тратить время.
    struct Sim {
        now: std::cell::Cell<Duration>,
        calls: std::cell::Cell<u32>,
    }
    impl Sim {
        fn new() -> Self {
            Sim {
                now: Default::default(),
                calls: Default::default(),
            }
        }
        fn run(
            &self,
            timeout: Duration,
            mut results: Vec<Result<u32, KafkaError>>,
            per_call: Duration,
        ) -> Result<u32, FastFail> {
            results.reverse();
            fast_fail_flow(
                timeout,
                |slice| {
                    self.calls.set(self.calls.get() + 1);
                    assert!(slice <= timeout);
                    self.now.set(self.now.get() + per_call);
                    results
                        .pop()
                        .unwrap_or(Err(KafkaError::MetadataFetch(C::NotLeaderForPartition)))
                },
                |d| self.now.set(self.now.get() + d),
                || self.now.get(),
                || None,
            )
        }
    }
    fn nl() -> Result<u32, KafkaError> {
        Err(KafkaError::MetadataFetch(C::NotLeaderForPartition))
    }

    #[test]
    fn retriable_errors_retry_across_first_slice_until_success() {
        let sim = Sim::new();
        // каждый вызов "тратит" 1 с: успех приходит после границы первых 2 с
        let r = sim.run(
            Duration::from_secs(10),
            vec![nl(), nl(), nl(), nl(), Ok(7)],
            Duration::from_secs(1),
        );
        assert!(matches!(r, Ok(7)));
        assert_eq!(sim.calls.get(), 5);
        assert!(sim.now.get() > FIRST_METADATA_SLICE);
    }

    #[test]
    fn retriable_errors_stop_when_timeout_exhausted() {
        let sim = Sim::new();
        let r = sim.run(Duration::from_secs(3), vec![], Duration::from_millis(500));
        assert!(matches!(
            r,
            Err(FastFail::Op(KafkaError::MetadataFetch(
                C::NotLeaderForPartition
            )))
        ));
        assert!(sim.now.get() <= Duration::from_secs(4));
        assert!(sim.calls.get() > 2);
    }

    #[test]
    fn non_retriable_non_timeout_error_returns_immediately() {
        let sim = Sim::new();
        let r = sim.run(
            Duration::from_secs(10),
            vec![Err(KafkaError::MetadataFetch(C::TopicAuthorizationFailed))],
            Duration::from_millis(10),
        );
        assert!(matches!(r, Err(FastFail::Op(_))));
        assert_eq!(sim.calls.get(), 1);
    }
}
