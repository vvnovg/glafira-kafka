use std::collections::BTreeMap;

use rdkafka::error::KafkaError;
use serde::Serialize;

use crate::i18n::LocalizedText;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    ConnectionFailed,
    TlsError,
    AuthFailed,
    NotAuthorized,
    Timeout,
    InvalidInput,
    NotFound,
    ReadOnly,
    ConfirmationRequired,
    GroupNotEmpty,
    Internal,
}

#[derive(Debug, Clone, Serialize, thiserror::Error)]
#[serde(rename_all = "camelCase")]
#[error("{message}")]
pub struct AppError {
    pub code: ErrorCode,
    /// Ключ словаря интерфейса (`src/locales/*.json`).
    pub key: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, String>,
    /// Английский текст (для логов и как запасной вариант перевода).
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
    pub retryable: bool,
}

pub type AppResult<T> = Result<T, AppError>;

impl AppError {
    pub fn localized(code: ErrorCode, text: crate::i18n::LocalizedText) -> Self {
        Self {
            code,
            key: text.key,
            params: text.params,
            message: text.text,
            details: None,
            retryable: Self::retryable_code(code),
        }
    }

    fn retryable_code(code: ErrorCode) -> bool {
        matches!(code, ErrorCode::ConnectionFailed | ErrorCode::Timeout)
    }

    /// Текст ошибки для полей-сообщений (ключ, параметры, английский текст).
    pub fn text(&self) -> LocalizedText {
        LocalizedText {
            key: self.key.clone(),
            params: self.params.clone(),
            text: self.message.clone(),
        }
    }

    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }

    /// Неожиданная внутренняя ошибка: общий текст «Internal error», подробности — в `details`
    /// (технический текст, не переводится).
    pub fn internal_with(details: impl Into<String>) -> Self {
        app_err!(ErrorCode::Internal, "errors.internal", "Internal error").with_details(details)
    }
}

impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self {
        app_err!(
            ErrorCode::Internal,
            "errors.io",
            "I/O error: {{error}}",
            error = e
        )
    }
}

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        app_err!(
            ErrorCode::Internal,
            "errors.json",
            "JSON error: {{error}}",
            error = e
        )
    }
}

/// Классифицирует текст ошибки librdkafka по ключевым словам.
/// URL брокеров (`sasl_ssl://...`) отбрасываются, иначе протокол в адресе
/// путает SSL и SASL. "authoriz" проверяется раньше "authenticat".
pub fn classify_message(text: &str) -> Option<ErrorCode> {
    let t = text
        .split_whitespace()
        .filter(|w| !w.contains("://"))
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase();
    if t.contains("authoriz") {
        Some(ErrorCode::NotAuthorized)
    } else if t.contains("ssl") || t.contains("certificate") || t.contains("handshake") {
        Some(ErrorCode::TlsError)
    } else if t.contains("sasl") || t.contains("authenticat") {
        Some(ErrorCode::AuthFailed)
    } else if t.contains("timed out") || t.contains("timeout") {
        Some(ErrorCode::Timeout)
    } else if t.contains("transport")
        || t.contains("all brokers down")
        || t.contains("resolve")
        || t.contains("connect")
    {
        Some(ErrorCode::ConnectionFailed)
    } else {
        None
    }
}

/// Уточнение TLS-ошибки по тексту librdkafka/OpenSSL. Имя хоста проверяется раньше общего
/// «certificate verify failed», в который оно вложено.
fn tls_message(text: &str) -> LocalizedText {
    let t = text.to_lowercase();
    if t.contains("plaintext broker listener") {
        ltext!(
            "errors.kafka.plaintextListener",
            "The broker accepts connections without TLS on this address — check the port or choose a protocol without SSL"
        )
    } else if t.contains("hostname mismatch") || t.contains("ip address mismatch") {
        ltext!(
            "errors.kafka.hostnameMismatch",
            "The broker host name does not match its certificate"
        )
    } else if t.contains("certificate verify failed") {
        ltext!(
            "errors.kafka.caUntrusted",
            "The broker certificate is not signed by a trusted CA — check “Trusted CA”"
        )
    } else if t.contains("certificate required")
        || t.contains("bad certificate")
        || t.contains("unknown ca")
        || t.contains("certificate unknown")
    {
        ltext!(
            "errors.kafka.clientCertRejected",
            "The broker rejected the client certificate"
        )
    } else {
        ltext!("errors.kafka.tlsError", "TLS connection error")
    }
}

/// `client_error` — последняя ошибка из `ClientContext::error`. Она точнее
/// ошибки операции: при проблеме с TLS сама операция падает просто по таймауту.
pub fn from_kafka(err: &KafkaError, client_error: Option<&str>) -> AppError {
    let op = err.to_string();
    let code = client_error
        .and_then(classify_message)
        .or_else(|| classify_message(&op))
        .unwrap_or(ErrorCode::Internal);
    let details = match client_error {
        Some(c) => format!("{op}\n{c}"),
        None => op,
    };
    let text = match code {
        ErrorCode::ConnectionFailed => ltext!(
            "errors.kafka.connectionFailed",
            "Could not connect to the brokers"
        ),
        ErrorCode::TlsError => tls_message(&details),
        ErrorCode::AuthFailed => ltext!("errors.kafka.authFailed", "Authentication failed"),
        ErrorCode::NotAuthorized => ltext!(
            "errors.kafka.notAuthorized",
            "Not authorized to perform the operation"
        ),
        ErrorCode::Timeout => ltext!("errors.kafka.timeout", "The operation timed out"),
        _ => ltext!("errors.kafka.generic", "Kafka error"),
    };
    AppError::localized(code, text).with_details(details)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::types::RDKafkaErrorCode;

    #[test]
    fn authorization_wins_over_authentication() {
        assert_eq!(
            classify_message("Broker: Topic authorization failed"),
            Some(ErrorCode::NotAuthorized)
        );
    }

    #[test]
    fn ssl_errors_are_tls() {
        assert_eq!(
            classify_message(
                "sasl_ssl://localhost:9094/bootstrap: SSL handshake failed: certificate verify failed"
            ),
            Some(ErrorCode::TlsError)
        );
    }

    #[test]
    fn sasl_errors_are_auth_even_with_sasl_ssl_url() {
        assert_eq!(
            classify_message(
                "sasl_ssl://localhost:9094/bootstrap: SASL authentication error: Authentication failed"
            ),
            Some(ErrorCode::AuthFailed)
        );
    }

    #[test]
    fn timeouts_and_transport() {
        assert_eq!(
            classify_message("Local: Timed out"),
            Some(ErrorCode::Timeout)
        );
        assert_eq!(
            classify_message("Local: Broker transport failure"),
            Some(ErrorCode::ConnectionFailed)
        );
        assert_eq!(classify_message("something else"), None);
    }

    #[test]
    fn client_error_is_more_precise_than_operation_error() {
        let err = KafkaError::MetadataFetch(RDKafkaErrorCode::OperationTimedOut);
        let app = from_kafka(&err, Some("ssl://h:9093/bootstrap: SSL handshake failed"));
        assert_eq!(app.code, ErrorCode::TlsError);
        assert!(app.details.unwrap().contains("SSL handshake failed"));
    }

    #[test]
    fn tls_errors_get_specific_messages() {
        let err = KafkaError::MetadataFetch(RDKafkaErrorCode::OperationTimedOut);
        for (client, expected) in [
            (
                "ssl://localhost:9092/bootstrap: SSL handshake failed: Disconnected: SSL connection closed by peer: connecting to a PLAINTEXT broker listener? (after 17ms in state SSL_HANDSHAKE)",
                "errors.kafka.plaintextListener",
            ),
            (
                "ssl://h:9095/bootstrap: SSL handshake failed: error:0A000086:SSL routines::certificate verify failed: broker certificate could not be verified",
                "errors.kafka.caUntrusted",
            ),
            (
                "ssl://h:9095/bootstrap: SSL handshake failed: certificate verify failed: Hostname mismatch",
                "errors.kafka.hostnameMismatch",
            ),
            (
                "ssl://h:9095/bootstrap: SSL handshake failed: error:0A00045C:SSL routines::tlsv13 alert certificate required",
                "errors.kafka.clientCertRejected",
            ),
            (
                "ssl://h:9095/bootstrap: SSL handshake failed: sslv3 alert bad certificate",
                "errors.kafka.clientCertRejected",
            ),
            (
                "ssl://h:9095/bootstrap: SSL handshake failed: something new",
                "errors.kafka.tlsError",
            ),
        ] {
            let app = from_kafka(&err, Some(client));
            assert_eq!(app.code, ErrorCode::TlsError, "{client}");
            assert_eq!(app.key, expected, "{client}");
        }
    }

    #[test]
    fn serializes_for_frontend() {
        let json = serde_json::to_value(app_err!(
            ErrorCode::InvalidInput,
            "errors.topics.notFound",
            "Topic “{{topic}}” not found",
            topic = "t"
        ))
        .unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "code": "INVALID_INPUT",
                "key": "errors.topics.notFound",
                "params": {"topic": "t"},
                "message": "Topic “t” not found",
                "retryable": false
            })
        );
        let json = serde_json::to_value(AppError::internal_with("boom")).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "code": "INTERNAL",
                "key": "errors.internal",
                "message": "Internal error",
                "details": "boom",
                "retryable": false
            })
        );
    }

    #[test]
    fn mutation_codes_serialize_and_are_not_retryable() {
        for (code, name) in [
            (ErrorCode::ReadOnly, "READ_ONLY"),
            (ErrorCode::ConfirmationRequired, "CONFIRMATION_REQUIRED"),
            (ErrorCode::GroupNotEmpty, "GROUP_NOT_EMPTY"),
        ] {
            assert_eq!(serde_json::to_value(code).unwrap(), name);
            assert!(!AppError::localized(code, LocalizedText::raw("x")).retryable);
        }
    }
}
