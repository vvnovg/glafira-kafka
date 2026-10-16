use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use rdkafka::consumer::Consumer;
use serde::Serialize;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::i18n::LocalizedText;
use crate::kafka::pool::KafkaHandles;
use crate::secrets::SecretStore;
use crate::storage::connections::{ConnectionProfile, parse_bootstrap};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestStep {
    pub name: String,
    pub ok: bool,
    pub message: LocalizedText,
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
    // Файлы сертификатов, пароли и SASL проверяются до сети: ошибка относится к конфигурации.
    if let Err(e) = crate::kafka::config::build_client_config(p, secrets) {
        return failed(steps, "config", e);
    }
    steps.push(passed(
        "config",
        ltext!("messages.connectionTest.configOk", "Configuration is valid"),
    ));

    match tcp_probe(&servers) {
        Ok(addr) => steps.push(passed(
            "tcp",
            ltext!(
                "messages.connectionTest.tcpOk",
                "TCP connection to {{address}} established",
                address = addr
            ),
        )),
        Err(e) => return failed(steps, "tcp", e),
    }

    let handles = match KafkaHandles::create(p, secrets) {
        Ok(h) => h,
        Err(e) => return failed(steps, "kafka", e),
    };
    let started = Instant::now();
    match handles.consumer.fetch_metadata(None, KAFKA_TIMEOUT) {
        Ok(md) => steps.push(passed(
            "kafka",
            ltext!(
                "messages.connectionTest.kafkaOk",
                "Brokers: {{brokers}}, topics: {{topics}}",
                brokers = md.brokers().len(),
                topics = md.topics().len()
            ),
        )),
        Err(e) => return failed(steps, "kafka", handles.kafka_err(started, e)),
    }

    TestReport { ok: true, steps }
}

fn passed(name: &str, message: LocalizedText) -> TestStep {
    TestStep {
        name: name.into(),
        ok: true,
        message,
        error: None,
    }
}

fn failed(mut steps: Vec<TestStep>, name: &str, error: AppError) -> TestReport {
    steps.push(TestStep {
        name: name.into(),
        ok: false,
        message: error.text(),
        error: Some(error),
    });
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
    Err(app_err!(
        ErrorCode::ConnectionFailed,
        "errors.kafka.tcpUnreachable",
        "None of the bootstrap servers is reachable over TCP"
    )
    .with_details(last))
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
        assert_eq!(
            r.steps[0].error.as_ref().unwrap().code,
            ErrorCode::InvalidInput
        );
    }

    #[test]
    fn closed_port_fails_on_tcp_step() {
        let mut p = sample_profile("c");
        p.bootstrap_servers = "127.0.0.1:1".into();
        let r = test_connection(&p, &MemoryStore::default());
        assert!(!r.ok);
        let last = r.steps.last().unwrap();
        assert_eq!(last.name, "tcp");
        assert_eq!(
            last.error.as_ref().unwrap().code,
            ErrorCode::ConnectionFailed
        );
    }

    #[test]
    fn certificate_errors_fail_config_step() {
        use crate::storage::connections::{CertFormat, ClientCert, SecurityConfig, TlsConfig};
        let mut p = sample_profile("a");
        p.security = SecurityConfig::Ssl {
            tls: TlsConfig {
                ca_location: None,
                ca_format: CertFormat::Pem,
                verify_hostname: true,
                client_cert: Some(ClientCert {
                    format: CertFormat::Pem,
                    certificate_path: if cfg!(windows) {
                        "C:\\nope.pem".into()
                    } else {
                        "/nope.pem".into()
                    },
                    key_path: Some(if cfg!(windows) {
                        "C:\\nope.key".into()
                    } else {
                        "/nope.key".into()
                    }),
                    alias: None,
                }),
            },
        };
        let r = test_connection(&p, &MemoryStore::default());
        assert!(!r.ok);
        let last = r.steps.last().unwrap();
        assert_eq!(last.name, "config");
        assert_eq!(last.error.as_ref().unwrap().code, ErrorCode::InvalidInput);
    }
}
