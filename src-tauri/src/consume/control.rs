use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::time::Duration;

use crate::consume::types::CommitTarget;
use crate::error::{AppError, AppResult, ErrorCode};
use crate::kafka::groups::{OffsetTarget, TopicPartitionRef};
use crate::sync::MutexExt;

/// Команды в поток consumer-сессии (consumer не Sync — обращаться к нему можно только из его потока).
pub enum ConsumerCommand {
    Commit {
        target: CommitTarget,
        reply: Sender<AppResult<Vec<OffsetTarget>>>,
    },
    Pause {
        partitions: Vec<TopicPartitionRef>,
        reply: Sender<AppResult<()>>,
    },
    Resume {
        partitions: Vec<TopicPartitionRef>,
        reply: Sender<AppResult<()>>,
    },
}

struct Control {
    connection_id: String,
    tx: Sender<ConsumerCommand>,
}

/// Каналы команд активных consumer-сессий: session_id → (connection_id, Sender).
/// connection_id нужен командам для guard'а (read-only, подтверждение prod) до отправки команды.
#[derive(Default)]
pub struct ConsumerControls {
    inner: Mutex<HashMap<String, Control>>,
}

fn session_gone() -> AppError {
    AppError::not_found("Consumer-сессия не найдена или уже завершена")
}

impl ConsumerControls {
    pub fn register(&self, session_id: &str, connection_id: &str, tx: Sender<ConsumerCommand>) {
        self.inner.lock_safe().insert(
            session_id.to_string(),
            Control {
                connection_id: connection_id.to_string(),
                tx,
            },
        );
    }

    pub fn remove(&self, session_id: &str) {
        self.inner.lock_safe().remove(session_id);
    }

    pub fn get(&self, session_id: &str) -> AppResult<(String, Sender<ConsumerCommand>)> {
        self.inner
            .lock_safe()
            .get(session_id)
            .map(|c| (c.connection_id.clone(), c.tx.clone()))
            .ok_or_else(session_gone)
    }
}

/// Отправляет команду в поток сессии и ждёт ответа не дольше `timeout`. Блокирующая:
/// из async-кода вызывать через spawn_blocking.
pub fn request<T>(
    tx: &Sender<ConsumerCommand>,
    timeout: Duration,
    make: impl FnOnce(Sender<AppResult<T>>) -> ConsumerCommand,
) -> AppResult<T> {
    let (reply, rx) = mpsc::channel();
    tx.send(make(reply)).map_err(|_| session_gone())?;
    match rx.recv_timeout(timeout) {
        Ok(r) => r,
        Err(RecvTimeoutError::Timeout) => Err(AppError::new(
            ErrorCode::Timeout,
            "Consumer-сессия не ответила вовремя",
        )),
        Err(RecvTimeoutError::Disconnected) => Err(session_gone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use std::sync::mpsc;

    #[test]
    fn request_roundtrip_timeout_and_closed_session() {
        let (tx, rx) = mpsc::channel::<ConsumerCommand>();
        let worker = std::thread::spawn(move || {
            if let Ok(ConsumerCommand::Pause { reply, .. }) = rx.recv() {
                reply.send(Ok(())).unwrap();
            }
            // Второй запрос не обслуживаем — ждёт таймаута; потом канал закрывается.
            let _second = rx.recv();
            std::thread::sleep(Duration::from_millis(300));
        });
        let ok = request(&tx, Duration::from_secs(2), |reply| {
            ConsumerCommand::Pause {
                partitions: vec![],
                reply,
            }
        });
        assert!(ok.is_ok());
        let slow = request(&tx, Duration::from_millis(100), |reply| {
            ConsumerCommand::Resume {
                partitions: vec![],
                reply,
            }
        });
        assert_eq!(slow.unwrap_err().code, ErrorCode::Timeout);
        worker.join().unwrap();
        let closed = request(&tx, Duration::from_millis(100), |reply| {
            ConsumerCommand::Resume {
                partitions: vec![],
                reply,
            }
        });
        assert_eq!(closed.unwrap_err().code, ErrorCode::NotFound);
    }

    #[test]
    fn controls_registry() {
        let c = ConsumerControls::default();
        let (tx, _rx) = mpsc::channel();
        c.register("s1", "conn", tx);
        assert_eq!(c.get("s1").unwrap().0, "conn");
        c.remove("s1");
        assert_eq!(c.get("s1").unwrap_err().code, ErrorCode::NotFound);
    }
}
