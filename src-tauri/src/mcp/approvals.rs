//! Подтверждение MCP-клиентов (спец. G1 §4): решение принимается на сессию; одобренные по
//! имени клиенты запоминаются в настройках. Клиент без имени (протокол 2026-07-28 без
//! `clientInfo`) одобряется только на текущую сессию.

use std::sync::{Arc, Mutex};

use glafira_mcp::{ClientIdent, SessionAccess, SessionView};
use serde::Serialize;

use crate::error::AppResult;
use crate::settings::SettingsStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Decision {
    Pending,
    Approved,
    Rejected,
}

#[derive(Debug, Clone)]
struct Entry {
    session_id: String,
    client: Option<ClientIdent>,
    decision: Decision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalRequest {
    pub session_id: String,
    pub client_name: Option<String>,
    pub client_version: Option<String>,
}

pub struct Approvals {
    settings: Arc<SettingsStore>,
    entries: Mutex<Vec<Entry>>,
}

impl Approvals {
    pub fn new(settings: Arc<SettingsStore>) -> Self {
        Self {
            settings,
            entries: Mutex::new(Vec::new()),
        }
    }

    fn entries(&self) -> std::sync::MutexGuard<'_, Vec<Entry>> {
        self.entries.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Право сессии. Второе значение — true, если сессия только что стала ожидающей
    /// (показать диалог и привлечь внимание). Запоминание клиента пишет `settings.json`.
    pub fn access(&self, s: &SessionView, now_ms: u64) -> (SessionAccess, bool) {
        let mut entries = self.entries();
        if let Some(e) = entries.iter_mut().find(|e| e.session_id == s.id) {
            if e.client.is_none() {
                e.client = s.client.clone();
            }
            return (to_access(e.decision), false);
        }
        if let Some(c) = &s.client
            && self.settings.is_client_approved(&c.name)
        {
            if let Err(e) = self.settings.remember_client(&c.name, &c.version, now_ms) {
                tracing::warn!("mcp client not remembered: {}", e.message);
            }
            entries.push(Entry {
                session_id: s.id.clone(),
                client: s.client.clone(),
                decision: Decision::Approved,
            });
            return (SessionAccess::Approved, false);
        }
        entries.push(Entry {
            session_id: s.id.clone(),
            client: s.client.clone(),
            decision: Decision::Pending,
        });
        (SessionAccess::Pending, true)
    }

    pub fn pending(&self) -> Vec<ApprovalRequest> {
        self.entries()
            .iter()
            .filter(|e| e.decision == Decision::Pending)
            .map(|e| ApprovalRequest {
                session_id: e.session_id.clone(),
                client_name: e.client.as_ref().map(|c| c.name.clone()),
                client_version: e.client.as_ref().map(|c| c.version.clone()),
            })
            .collect()
    }

    pub fn is_approved(&self, session_id: &str) -> bool {
        self.entries()
            .iter()
            .any(|e| e.session_id == session_id && e.decision == Decision::Approved)
    }

    /// Ответ пользователя. `Ok(None)` — сессия уже не ждёт ответа (закрыта или решена);
    /// `Ok(Some(client))` — решение принято. Блокирующий вызов (запись настроек).
    pub fn answer(
        &self,
        session_id: &str,
        approve: bool,
        now_ms: u64,
    ) -> AppResult<Option<Option<ClientIdent>>> {
        let mut entries = self.entries();
        let Some(e) = entries
            .iter_mut()
            .find(|e| e.session_id == session_id && e.decision == Decision::Pending)
        else {
            return Ok(None);
        };
        if approve && let Some(c) = &e.client {
            self.settings.remember_client(&c.name, &c.version, now_ms)?;
        }
        e.decision = if approve {
            Decision::Approved
        } else {
            Decision::Rejected
        };
        Ok(Some(e.client.clone()))
    }

    /// Убирает клиента из запомненных; его открытые сессии теряют право сразу.
    pub fn revoke(&self, name: &str) -> AppResult<bool> {
        let removed = self.settings.revoke_client(name)?;
        for e in self.entries().iter_mut() {
            if e.client.as_ref().is_some_and(|c| c.name == name) {
                e.decision = Decision::Rejected;
            }
        }
        Ok(removed)
    }

    pub fn closed(&self, session_id: &str) {
        self.entries().retain(|e| e.session_id != session_id);
    }
}

fn to_access(d: Decision) -> SessionAccess {
    match d {
        Decision::Pending => SessionAccess::Pending,
        Decision::Approved => SessionAccess::Approved,
        Decision::Rejected => SessionAccess::Rejected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(id: &str, client: Option<&str>) -> SessionView {
        SessionView {
            id: id.into(),
            client: client.map(|n| ClientIdent {
                name: n.into(),
                version: "1.0".into(),
            }),
        }
    }

    fn approvals() -> (tempfile::TempDir, Arc<SettingsStore>, Approvals) {
        let dir = tempfile::tempdir().unwrap();
        let settings = Arc::new(SettingsStore::open(dir.path()));
        let a = Approvals::new(settings.clone());
        (dir, settings, a)
    }

    #[test]
    fn new_client_waits_then_is_remembered() {
        let (_d, settings, a) = approvals();
        assert_eq!(
            a.access(&view("s1", Some("claude-ai")), 1),
            (SessionAccess::Pending, true)
        );
        assert_eq!(
            a.access(&view("s1", Some("claude-ai")), 2),
            (SessionAccess::Pending, false)
        );
        assert_eq!(a.pending()[0].client_name.as_deref(), Some("claude-ai"));
        let client = a.answer("s1", true, 3).unwrap().unwrap().unwrap();
        assert_eq!(client.name, "claude-ai");
        assert!(a.pending().is_empty());
        assert!(a.is_approved("s1"));
        assert!(settings.is_client_approved("claude-ai"));
        assert_eq!(
            a.answer("s1", true, 4).unwrap(),
            None,
            "повторный ответ не нужен"
        );
        // Новая сессия того же клиента — без вопроса, lastSeen обновлён.
        assert_eq!(
            a.access(&view("s2", Some("claude-ai")), 9),
            (SessionAccess::Approved, false)
        );
        assert_eq!(settings.mcp().approved_clients[0].last_seen_ms, 9);
    }

    #[test]
    fn rejection_is_not_remembered() {
        let (_d, settings, a) = approvals();
        a.access(&view("s1", Some("cursor")), 1);
        a.answer("s1", false, 2).unwrap();
        assert_eq!(
            a.access(&view("s1", Some("cursor")), 3).0,
            SessionAccess::Rejected
        );
        assert!(!settings.is_client_approved("cursor"));
        assert_eq!(
            a.access(&view("s2", Some("cursor")), 4),
            (SessionAccess::Pending, true)
        );
    }

    #[test]
    fn unnamed_client_is_approved_only_for_the_session() {
        let (_d, settings, a) = approvals();
        a.access(&view("s1", None), 1);
        assert_eq!(a.pending()[0].client_name, None);
        assert_eq!(a.answer("s1", true, 2).unwrap(), Some(None));
        assert!(settings.mcp().approved_clients.is_empty());
        assert_eq!(
            a.access(&view("s2", None), 3),
            (SessionAccess::Pending, true)
        );
    }

    #[test]
    fn revoke_cuts_open_sessions_and_closed_sessions_are_forgotten() {
        let (_d, settings, a) = approvals();
        settings.remember_client("claude-ai", "1", 0).unwrap();
        assert_eq!(
            a.access(&view("s1", Some("claude-ai")), 1).0,
            SessionAccess::Approved
        );
        assert!(a.revoke("claude-ai").unwrap());
        assert_eq!(
            a.access(&view("s1", Some("claude-ai")), 2).0,
            SessionAccess::Rejected
        );
        a.access(&view("s3", Some("x")), 3);
        a.closed("s3");
        assert!(a.pending().is_empty());
    }
}
