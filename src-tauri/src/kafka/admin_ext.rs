//! FFI-обёртки над Admin API librdkafka, которых нет в rust-rdkafka 0.37:
//! IncrementalAlterConfigs, ListConsumerGroups, DescribeConsumerGroups,
//! ListConsumerGroupOffsets, AlterConsumerGroupOffsets.
//!
//! Весь новый `unsafe` M2 — только здесь; наружу выходят безопасные функции с Rust-типами.
//! Сырые указатели не покидают модуль: RAII-владельцы приватны, результаты копируются
//! в Rust-типы до уничтожения события.
//! Все функции **блокирующие** (ждут результат в собственной очереди событий) — вызывать
//! только из blocking-контекста. Ошибка запроса целиком — `KafkaError`; ошибка отдельного
//! ресурса, группы или партиции — `ItemError`.

use std::ffi::{CStr, CString, c_char};
use std::time::Duration;

use rdkafka::admin::AdminClient;
use rdkafka::bindings as rd;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::types::RDKafkaErrorCode;

use crate::kafka::context::CapturingContext;

type Admin = AdminClient<CapturingContext>;

const NO_ERROR: rd::rd_kafka_resp_err_t = rd::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR;
/// Запас сверх таймаута запроса на доставку события результата в очередь.
const POLL_MARGIN_MS: i32 = 5_000;

#[derive(Debug, Clone, PartialEq)]
pub struct ItemError {
    pub code: RDKafkaErrorCode,
    pub message: String,
}

pub type ItemResult<T> = Result<T, ItemError>;

/// Партиция из ответа librdkafka. `offset` — как вернул брокер (отрицательный = нет значения).
#[derive(Debug, Clone, PartialEq)]
pub struct RawPartition {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub error: Option<ItemError>,
}

// ---- RAII-владельцы объектов librdkafka ----
// Каждая обёртка создаётся только из ненулевого указателя, которым владеем мы, и
// освобождает его ровно один раз в Drop. Типы не Clone/Copy, поэтому двойного Drop нет.

struct Queue(*mut rd::rd_kafka_queue_t);
impl Drop for Queue {
    fn drop(&mut self) {
        // SAFETY: ненулевой указатель из rd_kafka_queue_new, владеем им единолично.
        // librdkafka держит собственную ссылку на очередь у незавершённого запроса,
        // поэтому уничтожение после таймаута poll безопасно (refcount внутри rd_kafka_q).
        unsafe { rd::rd_kafka_queue_destroy(self.0) }
    }
}

struct Options(*mut rd::rd_kafka_AdminOptions_t);
impl Drop for Options {
    fn drop(&mut self) {
        // SAFETY: ненулевой указатель из rd_kafka_AdminOptions_new; admin-вызов
        // копирует опции в свой op, владение остаётся у нас.
        unsafe { rd::rd_kafka_AdminOptions_destroy(self.0) }
    }
}

struct Event(*mut rd::rd_kafka_event_t);
impl Drop for Event {
    fn drop(&mut self) {
        // SAFETY: ненулевое событие из rd_kafka_queue_poll, владеем им. Объекты результата,
        // прочитанные из него (они принадлежат событию), к этому моменту уже скопированы
        // в Rust-типы и больше не используются.
        unsafe { rd::rd_kafka_event_destroy(self.0) }
    }
}

struct ConfigResource(*mut rd::rd_kafka_ConfigResource_t);
impl Drop for ConfigResource {
    fn drop(&mut self) {
        // SAFETY: ненулевой указатель из rd_kafka_ConfigResource_new.
        // rd_kafka_IncrementalAlterConfigs копирует ресурс (rd_kafka_ConfigResource_copy),
        // владение исходным остаётся у нас.
        unsafe { rd::rd_kafka_ConfigResource_destroy(self.0) }
    }
}

// ---- вспомогательные функции ----

fn cstring(s: &str) -> KafkaResult<CString> {
    CString::new(s)
        .map_err(|_| KafkaError::AdminOpCreation(format!("Недопустимый символ NUL в «{s}»")))
}

fn null_result(what: &str) -> KafkaError {
    KafkaError::AdminOpCreation(format!("librdkafka не создала {what}"))
}

/// # Safety
/// `p` — NULL или указатель на NUL-терминированную C-строку, живую на время вызова.
unsafe fn string(p: *const c_char) -> String {
    if p.is_null() {
        String::new()
    } else {
        // SAFETY: см. контракт функции; строка копируется до возврата.
        unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
    }
}

/// # Safety
/// `p` — NULL или массив из `n` инициализированных элементов, живой, пока используется срез.
unsafe fn slice<'a, T>(p: *const T, n: usize) -> &'a [T] {
    if p.is_null() || n == 0 {
        &[]
    } else {
        // SAFETY: см. контракт функции.
        unsafe { std::slice::from_raw_parts(p, n) }
    }
}

/// # Safety
/// `e` — NULL или живой `rd_kafka_error_t`, принадлежащий событию результата (не освобождается).
#[allow(dead_code)] // используется функциями consumer-групп (Task 7)
unsafe fn error_obj(e: *const rd::rd_kafka_error_t) -> Option<ItemError> {
    if e.is_null() {
        return None;
    }
    // SAFETY: см. контракт функции.
    let code = unsafe { rd::rd_kafka_error_code(e) };
    if code == NO_ERROR {
        return None;
    }
    Some(ItemError {
        code: code.into(),
        // SAFETY: строка принадлежит живому объекту ошибки и копируется сразу.
        message: unsafe { string(rd::rd_kafka_error_string(e)) },
    })
}

fn unexpected_event() -> KafkaError {
    KafkaError::AdminOpCreation("librdkafka вернула событие неожиданного типа".into())
}

fn timeout_ms(timeout: Duration) -> i32 {
    i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX)
}

/// Выполняет admin-запрос: своя очередь и опции с таймаутом, вызов `call`, ожидание события.
/// Блокирует до таймаута запроса + POLL_MARGIN_MS. Указатели, переданные в `call`, живы
/// только на время вызова `call`.
fn run(
    admin: &Admin,
    op: rd::rd_kafka_admin_op_t,
    timeout: Duration,
    call: impl FnOnce(
        *mut rd::rd_kafka_t,
        *const rd::rd_kafka_AdminOptions_t,
        *mut rd::rd_kafka_queue_t,
    ),
) -> KafkaResult<Event> {
    let rk = admin.inner().native_ptr();
    let ms = timeout_ms(timeout);
    // SAFETY: rk — живой клиент: `admin` заимствован на всё время вызова.
    let queue = unsafe { rd::rd_kafka_queue_new(rk) };
    if queue.is_null() {
        return Err(null_result("очередь событий"));
    }
    let queue = Queue(queue);
    // SAFETY: rk жив (см. выше); op — валидное значение перечисления.
    let opts = unsafe { rd::rd_kafka_AdminOptions_new(rk, op) };
    if opts.is_null() {
        return Err(KafkaError::AdminOpCreation(
            "Операция не поддерживается этой версией librdkafka".into(),
        ));
    }
    let opts = Options(opts);
    let mut errstr = [0 as c_char; 512];
    // SAFETY: opts жив; буфер errstr живёт до конца вызова, его размер передан.
    let err = unsafe {
        rd::rd_kafka_AdminOptions_set_request_timeout(opts.0, ms, errstr.as_mut_ptr(), errstr.len())
    };
    if err != NO_ERROR {
        // SAFETY: буфер инициализирован нулями, librdkafka пишет в него NUL-терминированную
        // строку не длиннее errstr_size, поэтому чтение остаётся в пределах массива.
        return Err(KafkaError::AdminOpCreation(unsafe {
            string(errstr.as_ptr())
        }));
    }
    call(rk, opts.0, queue.0);
    // SAFETY: очередь жива; NULL означает, что событие не пришло за отведённое время.
    let ev = unsafe { rd::rd_kafka_queue_poll(queue.0, ms.saturating_add(POLL_MARGIN_MS)) };
    if ev.is_null() {
        return Err(KafkaError::AdminOp(RDKafkaErrorCode::OperationTimedOut));
    }
    let ev = Event(ev);
    // SAFETY: событие живо до Drop.
    let code = unsafe { rd::rd_kafka_event_error(ev.0) };
    if code != NO_ERROR {
        // SAFETY: строка принадлежит живому событию и копируется сразу.
        let text = unsafe { string(rd::rd_kafka_event_error_string(ev.0)) };
        tracing::warn!("admin-запрос завершился ошибкой: {text}");
        return Err(KafkaError::AdminOp(code.into()));
    }
    Ok(ev)
}

// ---- IncrementalAlterConfigs ----

/// Изменяет конфиги топика: `Some(value)` — SET, `None` — DELETE (возврат к default).
/// Остальные параметры топика не затрагиваются (в отличие от устаревшего AlterConfigs).
pub fn incremental_alter_topic_configs(
    admin: &Admin,
    timeout: Duration,
    topic: &str,
    changes: &[(String, Option<String>)],
) -> KafkaResult<ItemResult<()>> {
    let name = cstring(topic)?;
    // SAFETY: name — живая C-строка на время вызова; librdkafka копирует имя.
    let resource = unsafe {
        rd::rd_kafka_ConfigResource_new(
            rd::rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TOPIC,
            name.as_ptr(),
        )
    };
    if resource.is_null() {
        return Err(null_result("ConfigResource"));
    }
    let resource = ConfigResource(resource);
    for (key, value) in changes {
        let key = cstring(key)?;
        let value = value.as_deref().map(cstring).transpose()?;
        let (op, value_ptr) = match &value {
            Some(v) => (
                rd::rd_kafka_AlterConfigOpType_t::RD_KAFKA_ALTER_CONFIG_OP_TYPE_SET,
                v.as_ptr(),
            ),
            None => (
                rd::rd_kafka_AlterConfigOpType_t::RD_KAFKA_ALTER_CONFIG_OP_TYPE_DELETE,
                std::ptr::null(),
            ),
        };
        // SAFETY: resource жив; key и value (если есть) живут до конца итерации, librdkafka
        // копирует их. NULL-значение допустимо только для DELETE — так и передаём.
        let err = unsafe {
            rd::rd_kafka_ConfigResource_add_incremental_config(
                resource.0,
                key.as_ptr(),
                op,
                value_ptr,
            )
        };
        if !err.is_null() {
            // SAFETY: err — живой объект ошибки; по rdkafka.h им владеет вызывающий.
            let message = unsafe { string(rd::rd_kafka_error_string(err)) };
            // SAFETY: освобождаем принадлежащий нам объект ошибки ровно один раз;
            // строка выше уже скопирована.
            unsafe { rd::rd_kafka_error_destroy(err) };
            return Err(KafkaError::AdminOpCreation(message));
        }
    }
    let ev = run(
        admin,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_INCREMENTALALTERCONFIGS,
        timeout,
        |rk, opts, queue| {
            let mut arr = [resource.0];
            // SAFETY: rk, opts, queue живы на время вызова (гарантия `run`); arr — массив
            // из одного живого ресурса. librdkafka копирует ресурсы и опции в свой op,
            // поэтому после возврата массив и ресурс ей не нужны.
            unsafe { rd::rd_kafka_IncrementalAlterConfigs(rk, arr.as_mut_ptr(), 1, opts, queue) }
        },
    )?;
    // SAFETY: событие живо; результат (или NULL при другом типе события) принадлежит событию.
    let result = unsafe { rd::rd_kafka_event_IncrementalAlterConfigs_result(ev.0) };
    if result.is_null() {
        return Err(unexpected_event());
    }
    let mut n = 0usize;
    // SAFETY: result жив, пока жив ev; n — валидный выходной параметр.
    let resources =
        unsafe { rd::rd_kafka_IncrementalAlterConfigs_result_resources(result, &mut n) };
    // SAFETY: массив из n ресурсов, принадлежащих событию; срез не переживает ev.
    for r in unsafe { slice(resources, n) } {
        // SAFETY: r — живой ресурс результата (принадлежит событию).
        let code = unsafe { rd::rd_kafka_ConfigResource_error(*r) };
        if code != NO_ERROR {
            return Ok(Err(ItemError {
                code: code.into(),
                // SAFETY: строка принадлежит ресурсу результата и копируется сразу.
                message: unsafe { string(rd::rd_kafka_ConfigResource_error_string(*r)) },
            }));
        }
    }
    Ok(Ok(()))
}
