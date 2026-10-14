use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Длительные сессии (browse, позже — consumer-сессии) по id; отмена через флаг.
#[derive(Default)]
pub struct SessionRegistry {
    sessions: Mutex<HashMap<String, Arc<AtomicBool>>>,
}

impl SessionRegistry {
    pub fn register(&self) -> (String, Arc<AtomicBool>) {
        let id = uuid::Uuid::new_v4().to_string();
        let flag = Arc::new(AtomicBool::new(false));
        self.sessions
            .lock()
            .unwrap()
            .insert(id.clone(), flag.clone());
        (id, flag)
    }

    pub fn cancel(&self, id: &str) -> bool {
        match self.sessions.lock().unwrap().get(id) {
            Some(flag) => {
                flag.store(true, Ordering::Relaxed);
                true
            }
            None => false,
        }
    }

    pub fn remove(&self, id: &str) {
        self.sessions.lock().unwrap().remove(id);
    }

    pub fn cancel_all(&self) {
        for flag in self.sessions.lock().unwrap().values() {
            flag.store(true, Ordering::Relaxed);
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
        let (id, flag) = r.register();
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
        let (_, a) = r.register();
        let (_, b) = r.register();
        r.cancel_all();
        assert!(a.load(Ordering::Relaxed) && b.load(Ordering::Relaxed));
    }
}
