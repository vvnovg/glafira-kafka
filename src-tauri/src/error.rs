use std::collections::BTreeMap;

use rdkafka::error::KafkaError;
use serde::Serialize;

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
    ProRequired,
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
    /// Старый конструктор (до переноса на ключи): текст показывается как есть через `errors.generic`.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        let message = message.into();
        let mut params = BTreeMap::new();
        params.insert("message".to_string(), message.clone());
        Self {
            code,
            key: "errors.generic".to_string(),
            params,
            message,
            details: None,
            retryable: Self::retryable_code(code),
        }
    }

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

    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidInput, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }
}

impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self {
        AppError::internal(format!("Ошибка ввода-вывода: {e}"))
    }
}

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        AppError::internal(format!("Ошибка JSON: {e}"))
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
fn tls_message(text: &str) -> &'static str {
    let t = text.to_lowercase();
    if t.contains("plaintext broker listener") {
        "На этом адресе брокер принимает соединения без TLS — проверьте порт или выберите протокол без SSL"
    } else if t.contains("hostname mismatch") || t.contains("ip address mismatch") {
        "Имя хоста брокера не совпадает с его сертификатом"
    } else if t.contains("certificate verify failed") {
        "Сертификат брокера не подписан доверенным CA — проверьте «Доверенный CA»"
    } else if t.contains("certificate required")
        || t.contains("bad certificate")
        || t.contains("unknown ca")
        || t.contains("certificate unknown")
    {
        "Брокер не принял клиентский сертификат"
    } else {
        "Ошибка TLS-соединения"
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
    let message = match code {
        ErrorCode::ConnectionFailed => "Не удалось подключиться к брокерам",
        ErrorCode::TlsError => tls_message(&details),
        ErrorCode::AuthFailed => "Ошибка аутентификации",
        ErrorCode::NotAuthorized => "Недостаточно прав для операции",
        ErrorCode::Timeout => "Операция не уложилась в таймаут",
        _ => "Ошибка Kafka",
    };
    AppError::new(code, message).with_details(details)
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
                "без TLS",
            ),
            (
                "ssl://h:9095/bootstrap: SSL handshake failed: error:0A000086:SSL routines::certificate verify failed: broker certificate could not be verified",
                "не подписан доверенным CA",
            ),
            (
                "ssl://h:9095/bootstrap: SSL handshake failed: certificate verify failed: Hostname mismatch",
                "Имя хоста",
            ),
            (
                "ssl://h:9095/bootstrap: SSL handshake failed: error:0A00045C:SSL routines::tlsv13 alert certificate required",
                "не принял клиентский сертификат",
            ),
            (
                "ssl://h:9095/bootstrap: SSL handshake failed: sslv3 alert bad certificate",
                "не принял клиентский сертификат",
            ),
            (
                "ssl://h:9095/bootstrap: SSL handshake failed: something new",
                "Ошибка TLS-соединения",
            ),
        ] {
            let app = from_kafka(&err, Some(client));
            assert_eq!(app.code, ErrorCode::TlsError, "{client}");
            assert!(app.message.contains(expected), "{client} → {}", app.message);
        }
    }

    #[test]
    fn serializes_for_frontend() {
        let json = serde_json::to_value(AppError::invalid("плохо")).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "code": "INVALID_INPUT",
                "key": "errors.generic",
                "params": {"message": "плохо"},
                "message": "плохо",
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
            (ErrorCode::ProRequired, "PRO_REQUIRED"),
        ] {
            assert_eq!(serde_json::to_value(code).unwrap(), name);
            assert!(!AppError::new(code, "x").retryable);
        }
    }
}
