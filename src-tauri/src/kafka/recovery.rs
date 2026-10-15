use std::time::{Duration, Instant};

use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;

/// Сколько librdkafka даём на самостоятельное восстановление соединения, прежде чем сессия считается failed.
pub const RECOVERY_WINDOW: Duration = Duration::from_secs(60);
/// Как часто во время «деградации» проверять брокер пробой метаданных.
pub const PROBE_INTERVAL: Duration = Duration::from_secs(5);
/// Проба метаданных во время деградации. Короткая: цикл сессии проверяет отмену между пробами,
/// а частота проб задаётся PROBE_INTERVAL, так что длинный таймаут ничего не даёт.
pub const PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// Информационные ошибки клиента, из которых librdkafka восстанавливается сама (реконнект).
pub fn is_transient(err: &KafkaError) -> bool {
    matches!(
        err,
        KafkaError::MessageConsumption(
            RDKafkaErrorCode::BrokerTransportFailure
                | RDKafkaErrorCode::AllBrokersDown
                | RDKafkaErrorCode::Resolve
                | RDKafkaErrorCode::OperationTimedOut
        )
    )
}

/// Состояние после временной ошибки: восстановление подтверждается успешной пробой метаданных
/// или прогрессом чтения; без восстановления дольше RECOVERY_WINDOW — сессия завершается ошибкой.
struct Degraded {
    since: Instant,
    last_probe: Instant,
    error: KafkaError,
}

#[derive(Default)]
pub struct RecoveryMonitor {
    state: Option<Degraded>,
}

impl RecoveryMonitor {
    /// true — сессия только что перешла в деградацию (первая временная ошибка).
    pub fn on_transient(&mut self, now: Instant, err: KafkaError) -> bool {
        match &mut self.state {
            Some(d) => {
                d.error = err;
                false
            }
            None => {
                self.state = Some(Degraded {
                    since: now,
                    last_probe: now,
                    error: err,
                });
                true
            }
        }
    }

    /// true — деградация снята (восстановление).
    pub fn on_progress(&mut self) -> bool {
        self.state.take().is_some()
    }

    pub fn should_probe(&self, now: Instant) -> bool {
        self.state
            .as_ref()
            .is_some_and(|d| now.saturating_duration_since(d.last_probe) >= PROBE_INTERVAL)
    }

    /// true — успешная проба сняла деградацию.
    pub fn on_probe(&mut self, now: Instant, ok: bool) -> bool {
        if ok {
            return self.state.take().is_some();
        }
        if let Some(d) = &mut self.state {
            d.last_probe = now;
        }
        false
    }

    /// Окно восстановления истекло: (начало деградации, последняя ошибка).
    pub fn expired(&self, now: Instant) -> Option<(Instant, &KafkaError)> {
        self.state
            .as_ref()
            .filter(|d| now.saturating_duration_since(d.since) >= RECOVERY_WINDOW)
            .map(|d| (d.since, &d.error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::types::RDKafkaErrorCode;

    #[test]
    fn transient_errors_are_classified() {
        use rdkafka::types::RDKafkaErrorCode as C;
        for c in [
            C::BrokerTransportFailure,
            C::AllBrokersDown,
            C::Resolve,
            C::OperationTimedOut,
        ] {
            assert!(is_transient(&KafkaError::MessageConsumption(c)), "{c:?}");
        }
        assert!(!is_transient(&KafkaError::MessageConsumption(
            C::TopicAuthorizationFailed
        )));
        assert!(!is_transient(&KafkaError::MessageConsumptionFatal(
            C::BrokerTransportFailure
        )));
        assert!(!is_transient(&KafkaError::PartitionEOF(0)));
    }

    fn transient() -> KafkaError {
        KafkaError::MessageConsumption(RDKafkaErrorCode::AllBrokersDown)
    }

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn monitor_idle_when_not_degraded() {
        let m = RecoveryMonitor::default();
        let t = Instant::now();
        assert!(!m.should_probe(t + secs(1000)));
        assert!(m.expired(t + secs(1000)).is_none());
    }

    #[test]
    fn monitor_keeps_original_since_on_repeated_transient() {
        let t = Instant::now();
        let mut m = RecoveryMonitor::default();
        m.on_transient(t, transient());
        m.on_transient(
            t + secs(30),
            KafkaError::MessageConsumption(RDKafkaErrorCode::Resolve),
        );
        assert!(m.expired(t + secs(59)).is_none());
        let (since, e) = m
            .expired(t + secs(60))
            .expect("окно истекло от первой ошибки");
        assert_eq!(since, t);
        assert!(
            matches!(e, KafkaError::MessageConsumption(RDKafkaErrorCode::Resolve)),
            "хранится последняя ошибка"
        );
    }

    #[test]
    fn monitor_reports_degraded_and_recovered_transitions() {
        let t = Instant::now();
        let mut m = RecoveryMonitor::default();
        assert!(!m.on_progress(), "без деградации восстанавливаться нечему");
        assert!(
            m.on_transient(t, transient()),
            "первая ошибка — вход в деградацию"
        );
        assert!(!m.on_transient(t + secs(1), transient()), "повторная — нет");
        assert!(!m.on_probe(t + secs(5), false));
        assert!(
            m.on_probe(t + secs(10), true),
            "успешная проба — восстановление"
        );
        assert!(!m.on_probe(t + secs(15), true));
        assert!(m.on_transient(t + secs(20), transient()));
        assert!(m.on_progress(), "прогресс — восстановление");
    }

    #[test]
    fn monitor_progress_clears() {
        let t = Instant::now();
        let mut m = RecoveryMonitor::default();
        m.on_transient(t, transient());
        m.on_progress();
        assert!(m.expired(t + secs(100)).is_none());
        assert!(!m.should_probe(t + secs(100)));
    }

    #[test]
    fn monitor_successful_probe_clears_and_next_blip_starts_fresh_window() {
        let t = Instant::now();
        let mut m = RecoveryMonitor::default();
        m.on_transient(t, transient());
        m.on_probe(t + secs(5), true);
        // тихий живой топик: сообщений нет, но восстановление подтверждено пробой
        let later = t + secs(300);
        m.on_transient(later, transient());
        assert!(
            m.expired(later).is_none(),
            "новая ошибка не должна истекать сразу"
        );
        assert!(m.expired(later + secs(59)).is_none());
        assert!(m.expired(later + secs(60)).is_some());
    }

    #[test]
    fn monitor_failed_probes_until_window_expires() {
        let t = Instant::now();
        let mut m = RecoveryMonitor::default();
        m.on_transient(t, transient());
        for i in 1..12 {
            let now = t + secs(5 * i);
            assert!(m.expired(now).is_none());
            m.on_probe(now, false);
        }
        assert!(m.expired(t + secs(60)).is_some());
    }

    #[test]
    fn monitor_probe_respects_interval() {
        let t = Instant::now();
        let mut m = RecoveryMonitor::default();
        m.on_transient(t, transient());
        assert!(!m.should_probe(t + secs(4)));
        assert!(m.should_probe(t + secs(5)));
        m.on_probe(t + secs(5), false);
        assert!(!m.should_probe(t + secs(9)));
        assert!(m.should_probe(t + secs(10)));
    }

    #[test]
    fn probe_is_short_enough_for_responsive_cancel() {
        // Цикл сессии проверяет отмену между пробами: проба не должна держать его дольше 0,5 с.
        assert!(PROBE_TIMEOUT <= Duration::from_millis(500));
        assert!(PROBE_TIMEOUT < PROBE_INTERVAL);
    }
}
