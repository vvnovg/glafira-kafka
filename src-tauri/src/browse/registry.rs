use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::sync::MutexExt;

/// Длительные сессии (browse, позже — consumer-сессии) по id; отмена через флаг.
#[derive(Default)]
pub struct SessionRegistry {
    sessions: Mutex<HashMap<String, Entry>>,
    /// Сигнал об удалении сессии — для `shutdown`.
    removed: Condvar,
}

struct Entry {
    connection_id: String,
    cancel: Arc<AtomicBool>,
}

impl SessionRegistry {
    pub fn register(&self, connection_id: &str) -> (String, Arc<AtomicBool>) {
        let id = uuid::Uuid::new_v4().to_string();
        let flag = Arc::new(AtomicBool::new(false));
        self.sessions.lock_safe().insert(
            id.clone(),
            Entry {
                connection_id: connection_id.to_string(),
                cancel: flag.clone(),
            },
        );
        (id, flag)
    }

    pub fn cancel(&self, id: &str) -> bool {
        match self.sessions.lock_safe().get(id) {
            Some(e) => {
                e.cancel.store(true, Ordering::Relaxed);
                true
            }
            None => false,
        }
    }

    /// Отменяет все сессии подключения (например, при его удалении); возвращает их число.
    pub fn cancel_for(&self, connection_id: &str) -> usize {
        let sessions = self.sessions.lock_safe();
        let mut n = 0;
        for e in sessions
            .values()
            .filter(|e| e.connection_id == connection_id)
        {
            e.cancel.store(true, Ordering::Relaxed);
            n += 1;
        }
        n
    }

    pub fn remove(&self, id: &str) {
        self.sessions.lock_safe().remove(id);
        self.removed.notify_all();
    }

    pub fn cancel_all(&self) {
        for e in self.sessions.lock_safe().values() {
            e.cancel.store(true, Ordering::Relaxed);
        }
    }

    /// Выход из приложения: отменяет все сессии и ждёт не дольше `timeout`, пока их потоки
    /// завершатся (consumer — выход из группы, пакет — purge и ожидание подтверждений).
    /// true — все сессии завершились. Блокирующий вызов.
    pub fn shutdown(&self, timeout: Duration) -> bool {
        self.cancel_all();
        let guard = self.sessions.lock_safe();
        let (guard, _) = self
            .removed
            .wait_timeout_while(guard, timeout, |s| !s.is_empty())
            .unwrap_or_else(|e| e.into_inner());
        guard.is_empty()
    }

    pub fn len(&self) -> usize {
        self.sessions.lock_safe().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn register_cancel_remove() {
        let r = SessionRegistry::default();
        let (id, flag) = r.register("c");
        assert_eq!(r.len(), 1);
        assert!(r.cancel(&id));
        assert!(flag.load(Ordering::Relaxed));
        r.remove(&id);
        assert!(!r.cancel(&id));
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn cancel_all_sets_every_flag() {
        let r = SessionRegistry::default();
        let (_, a) = r.register("c");
        let (_, b) = r.register("c");
        r.cancel_all();
        assert!(a.load(Ordering::Relaxed) && b.load(Ordering::Relaxed));
    }

    #[test]
    fn cancel_for_only_touches_sessions_of_that_connection() {
        let r = SessionRegistry::default();
        let (_, a1) = r.register("a");
        let (_, a2) = r.register("a");
        let (_, b) = r.register("b");
        assert_eq!(r.cancel_for("a"), 2);
        assert!(a1.load(Ordering::Relaxed) && a2.load(Ordering::Relaxed));
        assert!(!b.load(Ordering::Relaxed));
        assert_eq!(r.cancel_for("missing"), 0);
    }

    #[test]
    fn shutdown_cancels_and_waits_for_sessions_to_finish() {
        let r = Arc::new(SessionRegistry::default());
        let (id, flag) = r.register("c");
        let r2 = r.clone();
        let worker = std::thread::spawn(move || {
            // Сессия завершается (выход из группы, purge) только после отмены.
            while !flag.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(5));
            }
            std::thread::sleep(Duration::from_millis(100));
            r2.remove(&id);
        });
        let started = Instant::now();
        assert!(r.shutdown(Duration::from_secs(5)));
        assert!(
            started.elapsed() >= Duration::from_millis(100),
            "дождались сессию"
        );
        assert!(r.is_empty());
        worker.join().unwrap();
    }

    #[test]
    fn shutdown_gives_up_after_timeout() {
        let r = SessionRegistry::default();
        let (_, flag) = r.register("c");
        let started = Instant::now();
        assert!(!r.shutdown(Duration::from_millis(150)));
        assert!(flag.load(Ordering::Relaxed));
        let waited = started.elapsed();
        assert!(waited >= Duration::from_millis(150) && waited < Duration::from_secs(2));
        assert!(SessionRegistry::default().shutdown(Duration::from_secs(5)));
    }
}
