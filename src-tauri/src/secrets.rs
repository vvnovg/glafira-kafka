use std::collections::HashMap;
use std::sync::Mutex;

use crate::error::{AppError, AppResult};

const SERVICE: &str = "kafka-explorer";

pub trait SecretStore: Send + Sync {
    fn get(&self, key: &str) -> AppResult<Option<String>>;
    fn set(&self, key: &str, value: &str) -> AppResult<()>;
    fn delete(&self, key: &str) -> AppResult<()>;
}

pub fn secret_key(connection_id: &str, field: &str) -> String {
    format!("{connection_id}/{field}")
}

/// OS keychain: macOS Keychain, Windows Credential Manager, Secret Service.
pub struct KeyringStore;

fn keyring_err(e: keyring::Error) -> AppError {
    AppError::internal("Ошибка доступа к системному хранилищу секретов").with_details(e.to_string())
}

impl SecretStore for KeyringStore {
    fn get(&self, key: &str) -> AppResult<Option<String>> {
        let entry = keyring::Entry::new(SERVICE, key).map_err(keyring_err)?;
        match entry.get_password() {
            Ok(v) => Ok(Some(v)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(keyring_err(e)),
        }
    }

    fn set(&self, key: &str, value: &str) -> AppResult<()> {
        keyring::Entry::new(SERVICE, key)
            .and_then(|e| e.set_password(value))
            .map_err(keyring_err)
    }

    fn delete(&self, key: &str) -> AppResult<()> {
        let entry = keyring::Entry::new(SERVICE, key).map_err(keyring_err)?;
        match entry.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(keyring_err(e)),
        }
    }
}

#[derive(Default)]
pub struct MemoryStore {
    values: Mutex<HashMap<String, String>>,
}

impl SecretStore for MemoryStore {
    fn get(&self, key: &str) -> AppResult<Option<String>> {
        Ok(self.values.lock().unwrap().get(key).cloned())
    }

    fn set(&self, key: &str, value: &str) -> AppResult<()> {
        self.values
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }

    fn delete(&self, key: &str) -> AppResult<()> {
        self.values.lock().unwrap().remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_store_roundtrip() {
        let s = MemoryStore::default();
        assert_eq!(s.get("a").unwrap(), None);
        s.set("a", "1").unwrap();
        assert_eq!(s.get("a").unwrap(), Some("1".to_string()));
        s.delete("a").unwrap();
        s.delete("a").unwrap(); // повторное удаление — не ошибка
        assert_eq!(s.get("a").unwrap(), None);
    }

    #[test]
    fn key_format() {
        assert_eq!(secret_key("c1", "sasl.password"), "c1/sasl.password");
    }
}
