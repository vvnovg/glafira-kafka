use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use rdkafka::consumer::Consumer;
use serde::Serialize;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::kafka::pool::KafkaHandles;
use crate::secrets::SecretStore;
use crate::storage::connections::{parse_bootstrap, ConnectionProfile};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestStep {
    pub name: String,
    pub ok: bool,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<AppError>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestReport {
    pub ok: bool,
    pub steps: Vec<TestStep>,
}

const TCP_TIMEOUT: Duration = Duration::from_secs(5);
const KAFKA_TIMEOUT: Duration = Duration::from_secs(10);

/// Пошаговая проверка: конфигурация → DNS+TCP → Kafka (TLS + SASL + metadata).
/// Останавливается на первом неуспешном шаге.
pub fn test_connection(p: &ConnectionProfile, secrets: &dyn SecretStore) -> TestReport {
    let mut steps = Vec::new();

    let servers = match parse_bootstrap(&p.bootstrap_servers) {
        Ok(s) => s,
        Err(e) => return failed(steps, "config", e),
    };
    steps.push(passed("config", "Конфигурация корректна"));

    match tcp_probe(&servers) {
        Ok(addr) => steps.push(passed("tcp", format!("TCP-соединение с {addr} установлено"))),
        Err(e) => return failed(steps, "tcp", e),
    }

    let handles = match KafkaHandles::create(p, secrets) {
        Ok(h) => h,
        Err(e) => return failed(steps, "kafka", e),
    };
    match handles.consumer.fetch_metadata(None, KAFKA_TIMEOUT) {
        Ok(md) => steps.push(passed(
            "kafka",
            format!("Брокеров: {}, топиков: {}", md.brokers().len(), md.topics().len()),
        )),
        Err(e) => {
            // Колбэк ошибок клиента вызывается только из poll: даём librdkafka доставить причину (SASL/TLS).
            let _ = handles.consumer.poll(Duration::from_millis(1500));
            return failed(steps, "kafka", handles.kafka_err(e));
        }
    }

    TestReport { ok: true, steps }
}

fn passed(name: &str, message: impl Into<String>) -> TestStep {
    TestStep { name: name.into(), ok: true, message: message.into(), error: None }
}

fn failed(mut steps: Vec<TestStep>, name: &str, error: AppError) -> TestReport {
    steps.push(TestStep { name: name.into(), ok: false, message: error.message.clone(), error: Some(error) });
    TestReport { ok: false, steps }
}

fn tcp_probe(servers: &[(String, u16)]) -> AppResult<String> {
    let mut last = String::new();
    for (host, port) in servers {
        let addrs = match (host.as_str(), *port).to_socket_addrs() {
            Ok(a) => a,
            Err(e) => {
                last = format!("{host}: DNS: {e}");
                continue;
            }
        };
        for addr in addrs {
            match TcpStream::connect_timeout(&addr, TCP_TIMEOUT) {
                Ok(_) => return Ok(format!("{host}:{port}")),
                Err(e) => last = format!("{addr}: {e}"),
            }
        }
    }
    Err(AppError::new(ErrorCode::ConnectionFailed, "Ни один bootstrap-сервер не доступен по TCP").with_details(last))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::sample_profile;

    #[test]
    fn invalid_bootstrap_fails_on_config_step() {
        let mut p = sample_profile("c");
        p.bootstrap_servers = "no-port".into();
        let r = test_connection(&p, &MemoryStore::default());
        assert!(!r.ok);
        assert_eq!(r.steps.len(), 1);
        assert_eq!(r.steps[0].name, "config");
        assert_eq!(r.steps[0].error.as_ref().unwrap().code, ErrorCode::InvalidInput);
    }

    #[test]
    fn closed_port_fails_on_tcp_step() {
        let mut p = sample_profile("c");
        p.bootstrap_servers = "127.0.0.1:1".into();
        let r = test_connection(&p, &MemoryStore::default());
        assert!(!r.ok);
        let last = r.steps.last().unwrap();
        assert_eq!(last.name, "tcp");
        assert_eq!(last.error.as_ref().unwrap().code, ErrorCode::ConnectionFailed);
    }
}
