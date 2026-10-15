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

// ---- consumer groups ----

#[derive(Debug, Clone, PartialEq)]
pub struct GroupListing {
    pub group_id: String,
    /// Stable, Empty, PreparingRebalance, CompletingRebalance, Dead, Unknown.
    pub state: String,
    pub is_simple: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemberRaw {
    pub member_id: String,
    pub client_id: String,
    pub host: String,
    pub assignment: Vec<(String, i32)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupDescriptionRaw {
    pub group_id: String,
    pub state: String,
    pub assignor: String,
    pub members: Vec<MemberRaw>,
    pub error: Option<ItemError>,
}

struct ListOffsetsRequest(*mut rd::rd_kafka_ListConsumerGroupOffsets_t);
impl Drop for ListOffsetsRequest {
    fn drop(&mut self) {
        // SAFETY: ненулевой указатель из rd_kafka_ListConsumerGroupOffsets_new, владеем им.
        // rd_kafka_ListConsumerGroupOffsets кладёт в свой op копию запроса
        // (rd_kafka_ListConsumerGroupOffsets_copy), исходный остаётся нашим.
        unsafe { rd::rd_kafka_ListConsumerGroupOffsets_destroy(self.0) }
    }
}

struct AlterOffsetsRequest(*mut rd::rd_kafka_AlterConsumerGroupOffsets_t);
impl Drop for AlterOffsetsRequest {
    fn drop(&mut self) {
        // SAFETY: ненулевой указатель из rd_kafka_AlterConsumerGroupOffsets_new, владеем им.
        // rd_kafka_AlterConsumerGroupOffsets кладёт в свой op копию запроса
        // (rd_kafka_AlterConsumerGroupOffsets_copy), исходный остаётся нашим.
        unsafe { rd::rd_kafka_AlterConsumerGroupOffsets_destroy(self.0) }
    }
}

struct PartitionList(*mut rd::rd_kafka_topic_partition_list_t);
impl Drop for PartitionList {
    fn drop(&mut self) {
        // SAFETY: ненулевой указатель из rd_kafka_topic_partition_list_new, владеем им.
        // rd_kafka_AlterConsumerGroupOffsets_new копирует список
        // (rd_kafka_topic_partition_list_copy), поэтому исходный освобождаем сами.
        unsafe { rd::rd_kafka_topic_partition_list_destroy(self.0) }
    }
}

fn state_name(s: rd::rd_kafka_consumer_group_state_t) -> String {
    // SAFETY: rd_kafka_consumer_group_state_name принимает любое значение перечисления и
    // возвращает указатель на статическую строку; string() обрабатывает NULL.
    unsafe { string(rd::rd_kafka_consumer_group_state_name(s)) }
}

/// # Safety
/// `list` — NULL или живой список партиций, принадлежащий событию результата и
/// не изменяемый на время вызова.
unsafe fn read_partitions(list: *const rd::rd_kafka_topic_partition_list_t) -> Vec<RawPartition> {
    if list.is_null() {
        return Vec::new();
    }
    // SAFETY: см. контракт функции — указатель ненулевой и указывает на живой список.
    let l = unsafe { &*list };
    let cnt = usize::try_from(l.cnt).unwrap_or(0);
    // SAFETY: elems — массив как минимум из cnt инициализированных элементов списка.
    unsafe { slice(l.elems, cnt) }
        .iter()
        .map(|e| RawPartition {
            // SAFETY: имя топика принадлежит элементу списка и копируется сразу.
            topic: unsafe { string(e.topic) },
            partition: e.partition,
            offset: e.offset,
            error: (e.err != NO_ERROR).then(|| {
                let code: RDKafkaErrorCode = e.err.into();
                ItemError {
                    message: code.to_string(),
                    code,
                }
            }),
        })
        .collect()
}

fn partition_list(items: &[(String, i32, i64)]) -> KafkaResult<PartitionList> {
    let size = i32::try_from(items.len())
        .map_err(|_| KafkaError::AdminOpCreation("Слишком много партиций в запросе".into()))?;
    // SAFETY: создаём пустой список нужной ёмкости; владение переходит к PartitionList.
    let list = unsafe { rd::rd_kafka_topic_partition_list_new(size) };
    if list.is_null() {
        return Err(null_result("список партиций"));
    }
    let list = PartitionList(list);
    for (topic, partition, offset) in items {
        let t = cstring(topic)?;
        // SAFETY: список жив; t живёт до конца итерации, librdkafka копирует имя топика.
        let e = unsafe { rd::rd_kafka_topic_partition_list_add(list.0, t.as_ptr(), *partition) };
        if e.is_null() {
            return Err(null_result("элемент списка партиций"));
        }
        // SAFETY: e — ненулевой указатель на только что добавленный элемент списка; он
        // действителен до следующего add (возможен realloc), а пишем мы до него.
        unsafe { (*e).offset = *offset };
    }
    Ok(list)
}

/// Результат единственной группы из ответа ListConsumerGroupOffsets/AlterConsumerGroupOffsets.
///
/// # Safety
/// `groups` — NULL или массив из `n` живых group_result, принадлежащих событию,
/// которое переживает вызов.
unsafe fn single_group_result(
    groups: *const *const rd::rd_kafka_group_result_t,
    n: usize,
) -> KafkaResult<ItemResult<Vec<RawPartition>>> {
    // SAFETY: см. контракт функции.
    let Some(g) = unsafe { slice(groups, n) }.first().copied() else {
        return Err(unexpected_event());
    };
    if g.is_null() {
        return Err(unexpected_event());
    }
    // SAFETY: g — живой group_result; объект ошибки (или NULL) принадлежит ему.
    if let Some(e) = unsafe { error_obj(rd::rd_kafka_group_result_error(g)) } {
        return Ok(Err(e));
    }
    // SAFETY: g жив; список партиций (или NULL) принадлежит group_result.
    Ok(Ok(unsafe {
        read_partitions(rd::rd_kafka_group_result_partitions(g))
    }))
}

/// Все consumer groups кластера. Второй элемент — частичные ошибки (например, брокер,
/// не ответивший на ListGroups); список групп при этом может быть неполным.
pub fn list_consumer_groups(
    admin: &Admin,
    timeout: Duration,
) -> KafkaResult<(Vec<GroupListing>, Vec<ItemError>)> {
    let ev = run(
        admin,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS,
        timeout,
        // SAFETY: rk, opts, queue живы на время вызова (гарантия `run`); librdkafka
        // копирует опции в свой op.
        |rk, opts, queue| unsafe { rd::rd_kafka_ListConsumerGroups(rk, opts, queue) },
    )?;
    // SAFETY: событие живо; результат (или NULL при другом типе события) принадлежит событию.
    let result = unsafe { rd::rd_kafka_event_ListConsumerGroups_result(ev.0) };
    if result.is_null() {
        return Err(unexpected_event());
    }
    let (mut n, mut m) = (0usize, 0usize);
    // SAFETY: result жив, пока жив ev; n — валидный выходной параметр.
    let valid = unsafe { rd::rd_kafka_ListConsumerGroups_result_valid(result, &mut n) };
    // SAFETY: массив из n листингов, принадлежащих событию; срез не переживает ev.
    let groups = unsafe { slice(valid, n) }
        .iter()
        .filter(|g| !g.is_null())
        .map(|&g| {
            // SAFETY: g — ненулевой живой листинг события; строка копируется сразу.
            let group_id = unsafe { string(rd::rd_kafka_ConsumerGroupListing_group_id(g)) };
            // SAFETY: то же.
            let state = unsafe { rd::rd_kafka_ConsumerGroupListing_state(g) };
            // SAFETY: то же.
            let simple = unsafe { rd::rd_kafka_ConsumerGroupListing_is_simple_consumer_group(g) };
            GroupListing {
                group_id,
                state: state_name(state),
                is_simple: simple != 0,
            }
        })
        .collect();
    // SAFETY: result жив, пока жив ev; m — валидный выходной параметр.
    let errs = unsafe { rd::rd_kafka_ListConsumerGroups_result_errors(result, &mut m) };
    // SAFETY: массив из m объектов ошибок, принадлежащих событию; срез не переживает ev.
    let errors = unsafe { slice(errs, m) }
        .iter()
        // SAFETY: e — NULL или живой объект ошибки события (не освобождается).
        .filter_map(|&e| unsafe { error_obj(e) })
        .collect();
    Ok((groups, errors))
}

/// # Safety
/// `m` — ненулевое живое описание участника, принадлежащее событию.
unsafe fn read_member(m: *const rd::rd_kafka_MemberDescription_t) -> MemberRaw {
    // SAFETY: см. контракт; назначение (или NULL) принадлежит участнику.
    let assignment = unsafe { rd::rd_kafka_MemberDescription_assignment(m) };
    let parts = if assignment.is_null() {
        Vec::new()
    } else {
        // SAFETY: assignment ненулевой и жив; список (или NULL) принадлежит назначению.
        unsafe { read_partitions(rd::rd_kafka_MemberAssignment_partitions(assignment)) }
    };
    MemberRaw {
        // SAFETY: строки принадлежат живому участнику и копируются сразу.
        member_id: unsafe { string(rd::rd_kafka_MemberDescription_consumer_id(m)) },
        // SAFETY: то же.
        client_id: unsafe { string(rd::rd_kafka_MemberDescription_client_id(m)) },
        // SAFETY: то же.
        host: unsafe { string(rd::rd_kafka_MemberDescription_host(m)) },
        assignment: parts.into_iter().map(|p| (p.topic, p.partition)).collect(),
    }
}

/// # Safety
/// `g` — ненулевое живое описание группы, принадлежащее событию.
unsafe fn read_group_description(
    g: *const rd::rd_kafka_ConsumerGroupDescription_t,
) -> GroupDescriptionRaw {
    // SAFETY: см. контракт.
    let count = unsafe { rd::rd_kafka_ConsumerGroupDescription_member_count(g) };
    let members = (0..count)
        // SAFETY: g жив; при индексе вне диапазона возвращается NULL (отфильтровываем).
        .map(|i| unsafe { rd::rd_kafka_ConsumerGroupDescription_member(g, i) })
        .filter(|m| !m.is_null())
        // SAFETY: m — ненулевой живой участник, принадлежащий описанию группы.
        .map(|m| unsafe { read_member(m) })
        .collect();
    GroupDescriptionRaw {
        // SAFETY: строки и объекты принадлежат живому описанию и копируются сразу.
        group_id: unsafe { string(rd::rd_kafka_ConsumerGroupDescription_group_id(g)) },
        // SAFETY: то же.
        state: state_name(unsafe { rd::rd_kafka_ConsumerGroupDescription_state(g) }),
        // SAFETY: то же.
        assignor: unsafe { string(rd::rd_kafka_ConsumerGroupDescription_partition_assignor(g)) },
        members,
        // SAFETY: объект ошибки (или NULL) принадлежит описанию группы, не освобождается.
        error: unsafe { error_obj(rd::rd_kafka_ConsumerGroupDescription_error(g)) },
    }
}

/// Описания групп в порядке `ids`. Несуществующая группа приходит со state `Dead`
/// (и, в зависимости от версии брокера, с ошибкой GROUP_ID_NOT_FOUND в `error`).
pub fn describe_consumer_groups(
    admin: &Admin,
    timeout: Duration,
    ids: &[String],
) -> KafkaResult<Vec<GroupDescriptionRaw>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let names = ids
        .iter()
        .map(|s| cstring(s))
        .collect::<KafkaResult<Vec<_>>>()?;
    let mut ptrs: Vec<*const c_char> = names.iter().map(|c| c.as_ptr()).collect();
    let ev = run(
        admin,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS,
        timeout,
        |rk, opts, queue| {
            // SAFETY: rk, opts, queue живы на время вызова (гарантия `run`); ptrs — массив
            // из ptrs.len() указателей на живые CString из names. librdkafka копирует
            // имена (rd_strdup) и опции, после возврата массив ей не нужен.
            unsafe {
                rd::rd_kafka_DescribeConsumerGroups(rk, ptrs.as_mut_ptr(), ptrs.len(), opts, queue)
            }
        },
    )?;
    // SAFETY: событие живо; результат (или NULL при другом типе события) принадлежит событию.
    let result = unsafe { rd::rd_kafka_event_DescribeConsumerGroups_result(ev.0) };
    if result.is_null() {
        return Err(unexpected_event());
    }
    let mut n = 0usize;
    // SAFETY: result жив, пока жив ev; n — валидный выходной параметр.
    let arr = unsafe { rd::rd_kafka_DescribeConsumerGroups_result_groups(result, &mut n) };
    // SAFETY: массив из n описаний, принадлежащих событию; срез не переживает ev.
    let out = unsafe { slice(arr, n) }
        .iter()
        .filter(|g| !g.is_null())
        // SAFETY: g — ненулевое живое описание группы, принадлежащее событию.
        .map(|&g| unsafe { read_group_description(g) })
        .collect();
    Ok(out)
}

/// Все закоммиченные партиции группы (список партиций запроса — NULL).
pub fn list_consumer_group_offsets(
    admin: &Admin,
    timeout: Duration,
    group: &str,
) -> KafkaResult<ItemResult<Vec<RawPartition>>> {
    let g = cstring(group)?;
    // SAFETY: g — живая C-строка на время вызова, librdkafka копирует её; NULL-список
    // партиций допустим и означает «все партиции группы».
    let req = unsafe { rd::rd_kafka_ListConsumerGroupOffsets_new(g.as_ptr(), std::ptr::null()) };
    if req.is_null() {
        return Err(null_result("запрос ListConsumerGroupOffsets"));
    }
    let req = ListOffsetsRequest(req);
    let ev = run(
        admin,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS,
        timeout,
        |rk, opts, queue| {
            let mut arr = [req.0];
            // SAFETY: rk, opts, queue живы на время вызова (гарантия `run`); arr — массив
            // ровно из одного живого запроса (требование API). librdkafka копирует запрос
            // и опции, владение req остаётся у нас.
            unsafe { rd::rd_kafka_ListConsumerGroupOffsets(rk, arr.as_mut_ptr(), 1, opts, queue) }
        },
    )?;
    // SAFETY: событие живо; результат (или NULL при другом типе события) принадлежит событию.
    let result = unsafe { rd::rd_kafka_event_ListConsumerGroupOffsets_result(ev.0) };
    if result.is_null() {
        return Err(unexpected_event());
    }
    let mut n = 0usize;
    // SAFETY: result жив, пока жив ev; n — валидный выходной параметр.
    let groups = unsafe { rd::rd_kafka_ListConsumerGroupOffsets_result_groups(result, &mut n) };
    // SAFETY: массив из n group_result, принадлежащих событию, которое живо до конца функции.
    unsafe { single_group_result(groups, n) }
}

/// Задаёт оффсеты группы; результат — по каждой партиции. Брокер принимает изменение
/// только для неактивной группы. Пустой список или отрицательный оффсет librdkafka
/// отклоняет целиком (`KafkaError` с кодом InvalidArgument).
pub fn alter_consumer_group_offsets(
    admin: &Admin,
    timeout: Duration,
    group: &str,
    offsets: &[(String, i32, i64)],
) -> KafkaResult<ItemResult<Vec<RawPartition>>> {
    let g = cstring(group)?;
    let list = partition_list(offsets)?;
    // SAFETY: g и list живы на время вызова (оба ненулевые — требование rd_assert);
    // librdkafka копирует имя группы и список партиций.
    let req = unsafe { rd::rd_kafka_AlterConsumerGroupOffsets_new(g.as_ptr(), list.0) };
    if req.is_null() {
        return Err(null_result("запрос AlterConsumerGroupOffsets"));
    }
    let req = AlterOffsetsRequest(req);
    let ev = run(
        admin,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS,
        timeout,
        |rk, opts, queue| {
            let mut arr = [req.0];
            // SAFETY: rk, opts, queue живы на время вызова (гарантия `run`); arr — массив
            // ровно из одного живого запроса (требование API). librdkafka копирует запрос
            // и опции, владение req остаётся у нас.
            unsafe { rd::rd_kafka_AlterConsumerGroupOffsets(rk, arr.as_mut_ptr(), 1, opts, queue) }
        },
    )?;
    // SAFETY: событие живо; результат (или NULL при другом типе события) принадлежит событию.
    let result = unsafe { rd::rd_kafka_event_AlterConsumerGroupOffsets_result(ev.0) };
    if result.is_null() {
        return Err(unexpected_event());
    }
    let mut n = 0usize;
    // SAFETY: result жив, пока жив ev; n — валидный выходной параметр.
    let groups = unsafe { rd::rd_kafka_AlterConsumerGroupOffsets_result_groups(result, &mut n) };
    // SAFETY: массив из n group_result, принадлежащих событию, которое живо до конца функции.
    unsafe { single_group_result(groups, n) }
}
