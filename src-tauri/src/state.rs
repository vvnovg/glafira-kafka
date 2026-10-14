use std::path::Path;
use std::sync::Arc;

use crate::browse::registry::SessionRegistry;
use crate::error::AppResult;
use crate::kafka::pool::{ClientPool, KafkaHandles};
use crate::secrets::SecretStore;
use crate::storage::connections::{ConnectionProfile, ConnectionStore};

pub struct AppState {
    pub connections: ConnectionStore,
    pub secrets: Arc<dyn SecretStore>,
    pub pool: ClientPool,
    pub sessions: Arc<SessionRegistry>,
}

impl AppState {
    pub fn new(data_dir: &Path, secrets: Arc<dyn SecretStore>) -> AppResult<Self> {
        Ok(Self {
            connections: ConnectionStore::open(data_dir)?,
            secrets,
            pool: ClientPool::default(),
            sessions: Arc::new(SessionRegistry::default()),
        })
    }

    pub fn profile(&self, id: &str) -> AppResult<ConnectionProfile> {
        self.connections.get(id)
    }

    pub fn handles(&self, id: &str) -> AppResult<Arc<KafkaHandles>> {
        let profile = self.connections.get(id)?;
        self.pool.get(&profile, self.secrets.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::sample_profile;

    #[test]
    fn handles_for_unknown_connection_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        assert_eq!(state.handles("nope").err().unwrap().code, ErrorCode::NotFound);
        state.connections.upsert(sample_profile("a")).unwrap();
        assert!(state.handles("a").is_ok());
    }
}
