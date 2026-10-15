use std::time::{Duration, Instant};

/// Равномерное ограничение скорости: k-е сообщение не раньше started + k / rate.
pub struct RateLimiter {
    per_sec: Option<u32>,
    started: Instant,
    sent: u64,
}

impl RateLimiter {
    pub fn new(per_sec: Option<u32>, now: Instant) -> Self {
        Self {
            per_sec,
            started: now,
            sent: 0,
        }
    }

    /// Сколько ждать перед следующим сообщением.
    pub fn wait_time(&self, now: Instant) -> Duration {
        match self.per_sec {
            None | Some(0) => Duration::ZERO,
            Some(rate) => {
                let due =
                    self.started + Duration::from_secs_f64(self.sent as f64 / f64::from(rate));
                due.saturating_duration_since(now)
            }
        }
    }

    pub fn record(&mut self) {
        self.sent += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlimited_never_waits() {
        let t = Instant::now();
        let mut r = RateLimiter::new(None, t);
        for _ in 0..1000 {
            r.record();
        }
        assert_eq!(r.wait_time(t), Duration::ZERO);
        assert_eq!(RateLimiter::new(Some(0), t).wait_time(t), Duration::ZERO);
    }

    #[test]
    fn spreads_messages_over_time() {
        let t = Instant::now();
        let mut r = RateLimiter::new(Some(10), t);
        assert_eq!(r.wait_time(t), Duration::ZERO, "первое — сразу");
        r.record();
        assert_eq!(r.wait_time(t), Duration::from_millis(100));
        assert_eq!(r.wait_time(t + Duration::from_millis(100)), Duration::ZERO);
        for _ in 0..9 {
            r.record();
        }
        assert_eq!(
            r.wait_time(t + Duration::from_millis(500)),
            Duration::from_millis(500)
        );
    }
}
