use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::time::{Duration, Instant};

use crate::consume::types::CommitTarget;
use crate::error::{AppError, AppResult, ErrorCode};
use crate::kafka::groups::{OffsetTarget, TopicPartitionRef};
use crate::sync::MutexExt;

/// Верхняя граница `socket.timeout.ms` consumer'а: берётся из таймаута запроса подключения.
pub fn socket_timeout_ms(request_timeout_ms: u64) -> u64 {
    request_timeout_ms.clamp(1_000, 300_000)
}

/// Сколько вызывающий ждёт ответа сессии. Синхронный коммит — это поиск координатора плюс сам
/// OffsetCommit, каждый ограничен `socket.timeout.ms` (см. `socket_timeout_ms`), поэтому
/// берём 2 × socket.timeout + 2 с запаса на опрос команд между poll'ами.
pub fn reply_timeout(request_timeout_ms: u64) -> Duration {
    Duration::from_millis(2 * socket_timeout_ms(request_timeout_ms)) + Duration::from_secs(2)
}

/// Команды в поток consumer-сессии (consumer не Sync — обращаться к нему можно только из его потока).
pub enum ConsumerCommand {
    Commit {
        target: CommitTarget,
        /// После этого момента вызывающий уже получил Timeout — команду выполнять нельзя.
        deadline: Instant,
        reply: Sender<AppResult<Vec<OffsetTarget>>>,
    },
    Pause {
        partitions: Vec<TopicPartitionRef>,
        /// После этого момента вызывающий уже получил Timeout — команду выполнять нельзя.
        deadline: Instant,
        reply: Sender<AppResult<()>>,
    },
    Resume {
        partitions: Vec<TopicPartitionRef>,
        /// После этого момента вызывающий уже получил Timeout — команду выполнять нельзя.
        deadline: Instant,
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
    app_err!(
        ErrorCode::NotFound,
        "errors.consume.sessionGone",
        "The consumer session was not found or has already ended"
    )
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
    make: impl FnOnce(Sender<AppResult<T>>, Instant) -> ConsumerCommand,
) -> AppResult<T> {
    let (reply, rx) = mpsc::channel();
    tx.send(make(reply, Instant::now() + timeout))
        .map_err(|_| session_gone())?;
    match rx.recv_timeout(timeout) {
        Ok(r) => r,
        Err(RecvTimeoutError::Timeout) => Err(app_err!(
            ErrorCode::Timeout,
            "errors.consume.sessionTimeout",
            "The consumer session did not respond in time"
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
        let ok = request(&tx, Duration::from_secs(2), |reply, deadline| {
            ConsumerCommand::Pause {
                partitions: vec![],
                reply,
                deadline,
            }
        });
        assert!(ok.is_ok());
        let slow = request(&tx, Duration::from_millis(100), |reply, deadline| {
            ConsumerCommand::Resume {
                partitions: vec![],
                reply,
                deadline,
            }
        });
        let e = slow.unwrap_err();
        assert_eq!(e.code, ErrorCode::Timeout);
        assert_eq!(e.key, "errors.consume.sessionTimeout");
        worker.join().unwrap();
        let closed = request(&tx, Duration::from_millis(100), |reply, deadline| {
            ConsumerCommand::Resume {
                partitions: vec![],
                reply,
                deadline,
            }
        });
        let e = closed.unwrap_err();
        assert_eq!(e.code, ErrorCode::NotFound);
        assert_eq!(e.key, "errors.consume.sessionGone");
    }

    #[test]
    fn reply_timeout_covers_socket_bound() {
        for ms in [0, 500, 30_000, 10_000_000] {
            let socket = Duration::from_millis(socket_timeout_ms(ms));
            assert!(reply_timeout(ms) >= 2 * socket + Duration::from_secs(2));
        }
        assert_eq!(socket_timeout_ms(30_000), 30_000);
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
