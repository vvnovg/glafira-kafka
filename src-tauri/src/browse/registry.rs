use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Длительные сессии (browse, позже — consumer-сессии) по id; отмена через флаг.
#[derive(Default)]
pub struct SessionRegistry {
    sessions: Mutex<HashMap<String, Entry>>,
}

struct Entry {
    connection_id: String,
    cancel: Arc<AtomicBool>,
}

impl SessionRegistry {
    pub fn register(&self, connection_id: &str) -> (String, Arc<AtomicBool>) {
        let id = uuid::Uuid::new_v4().to_string();
        let flag = Arc::new(AtomicBool::new(false));
        self.sessions.lock().unwrap().insert(
            id.clone(),
            Entry {
                connection_id: connection_id.to_string(),
                cancel: flag.clone(),
            },
        );
        (id, flag)
    }

    pub fn cancel(&self, id: &str) -> bool {
        match self.sessions.lock().unwrap().get(id) {
            Some(e) => {
                e.cancel.store(true, Ordering::Relaxed);
                true
            }
            None => false,
        }
    }

    /// Отменяет все сессии подключения (например, при его удалении); возвращает их число.
    pub fn cancel_for(&self, connection_id: &str) -> usize {
        let sessions = self.sessions.lock().unwrap();
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
        self.sessions.lock().unwrap().remove(id);
    }

    pub fn cancel_all(&self) {
        for e in self.sessions.lock().unwrap().values() {
            e.cancel.store(true, Ordering::Relaxed);
        }
    }

    pub fn len(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
