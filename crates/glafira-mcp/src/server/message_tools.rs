//! Чтение сообщений (спец. G4 §3): `read_messages`, `summarize_messages`,
//! `get_group_commit_history`. Приложение читает Kafka и отдаёт данные без маскировки; здесь они
//! маскируются ([`Masker`]) и собираются в ответ — к LLM содержимое попадает только отсюда.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ProgressNotificationParam};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::tools::in_range;
use super::{GatewayServer, to_value};
use crate::commits::{CommitHistory, MAX_METADATA_TEXT};
use crate::mask::{DataFormat, MaskedData, Masker, cut_utf8};
use crate::messages::*;
use crate::model::BackendError;
use crate::rules::{self, MAX_RESPONSE_BYTES};
use crate::summary::{
    self, DEFAULT_TOP_KEYS, KEY_PREFIX_BYTES, MAX_BUCKET_SECONDS, MAX_BUCKETS, MAX_TOP_KEYS, Sizes,
    Summary,
};

const INCOMPLETE_HINT: &str = "The scan stopped at the time budget or the record limit (1,000,000): the results cover only the scanned part (see scannedTo). Narrow the window, add filters or select partitions.";
/// `topic` здесь не поможет: партиция `__consumer_offsets` читается целиком (спец. §6.3).
const HISTORY_INCOMPLETE_HINT: &str = "The scan of __consumer_offsets stopped at the time budget or the record limit (1,000,000): later commits of the window may be missing. Narrow the window.";
const PAGE_CUT_HINT: &str =
    "The page was cut to 100 KB: continue with nextCursor or lower maxValueBytes.";
const SUMMARY_CUT_HINT: &str =
    "The response was cut to 100 KB: headerNames and keys.top were shortened.";
const RUNS_CUT_HINT: &str = "The response was cut to 100 KB: the oldest runs were dropped; narrow the window or pass topic.";
const HISTORY_TOO_LARGE: &str = "The commit history does not fit into 100 KB even without commit runs; narrow it: pass topic or a shorter window";
const NO_COMMITS_HINT: &str = "No commits of this group in the window: the group may not exist, may store offsets elsewhere (assign() with external storage), or the history was compacted.";

// ---- Параметры (описания полей уходят в JSON Schema для LLM) ----

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReadMessagesParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Topic name; internal topics are allowed.
    pub topic: String,
    /// Window start, inclusive: RFC 3339 with a time zone or epoch milliseconds. Use together with to.
    #[serde(default)]
    pub from: Option<TimeArg>,
    /// Window end, inclusive: RFC 3339 with a time zone or epoch milliseconds.
    #[serde(default)]
    pub to: Option<TimeArg>,
    /// Window mode only: read just these partitions. Default: all.
    #[serde(default)]
    pub partitions: Option<Vec<i32>>,
    /// Offsets mode: one partition, used with startOffset instead of from/to.
    #[serde(default)]
    pub partition: Option<i32>,
    /// Offsets mode: first offset, inclusive.
    #[serde(default)]
    pub start_offset: Option<i64>,
    /// Offsets mode: end offset, exclusive. Default: the end of the partition.
    #[serde(default)]
    pub end_offset: Option<i64>,
    /// Key filter.
    #[serde(default)]
    pub key: Option<TextMatch>,
    /// Value filter.
    #[serde(default)]
    pub value: Option<TextMatch>,
    /// Header filter.
    #[serde(default)]
    pub header: Option<HeaderMatch>,
    /// JSONPath condition on the value: $.status == 'FAILED', $.items[?(@.qty > 1)].
    #[serde(default)]
    pub json_path: Option<String>,
    /// Key decoding. Default: auto.
    #[serde(default)]
    pub key_format: Option<FormatArg>,
    /// Value decoding. Default: auto.
    #[serde(default)]
    pub value_format: Option<FormatArg>,
    /// Messages per page, 1..=200. Default: 50.
    #[serde(default)]
    pub limit: Option<u32>,
    /// Value text limit after masking, 256..=16384 bytes. Default: 2048.
    #[serde(default)]
    pub max_value_bytes: Option<u32>,
    /// nextCursor of the previous page, with the same other parameters.
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SummarizeMessagesParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Topic name; internal topics are allowed.
    pub topic: String,
    /// Window start, inclusive: RFC 3339 with a time zone or epoch milliseconds.
    pub from: TimeArg,
    /// Window end, inclusive: RFC 3339 with a time zone or epoch milliseconds.
    pub to: TimeArg,
    /// Only these partitions. Default: all.
    #[serde(default)]
    pub partitions: Option<Vec<i32>>,
    /// Key filter.
    #[serde(default)]
    pub key: Option<TextMatch>,
    /// Value filter.
    #[serde(default)]
    pub value: Option<TextMatch>,
    /// Header filter.
    #[serde(default)]
    pub header: Option<HeaderMatch>,
    /// JSONPath condition on the value.
    #[serde(default)]
    pub json_path: Option<String>,
    /// Key decoding. Default: auto.
    #[serde(default)]
    pub key_format: Option<FormatArg>,
    /// Value decoding. Default: auto.
    #[serde(default)]
    pub value_format: Option<FormatArg>,
    /// Bucket size of the timeline, 1..=86400 seconds. Default: a round step giving about 60 buckets.
    #[serde(default)]
    pub bucket_seconds: Option<u32>,
    /// How many most frequent keys to return, 0..=100. Default: 20.
    #[serde(default)]
    pub top_keys: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CommitHistoryParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Consumer group id.
    pub group_id: String,
    /// Window start, inclusive: RFC 3339 with a time zone or epoch milliseconds.
    pub from: TimeArg,
    /// Window end, inclusive: RFC 3339 with a time zone or epoch milliseconds.
    pub to: TimeArg,
    /// Only commits for this topic.
    #[serde(default)]
    pub topic: Option<String>,
}

fn window(from: &TimeArg, to: &TimeArg) -> Result<(i64, i64), BackendError> {
    let from = from.to_ms("from").map_err(BackendError::invalid_argument)?;
    let to = to.to_ms("to").map_err(BackendError::invalid_argument)?;
    if from > to {
        return Err(BackendError::invalid_argument(
            "from must not be later than to",
        ));
    }
    Ok((from, to))
}

fn partition_list(p: &Option<Vec<i32>>) -> Result<Option<Vec<i32>>, BackendError> {
    let Some(list) = p.as_ref().filter(|l| !l.is_empty()) else {
        return Ok(None);
    };
    if list.iter().any(|x| *x < 0) {
        return Err(BackendError::invalid_argument(
            "partitions must not be negative",
        ));
    }
    let mut list = list.clone();
    list.sort_unstable();
    list.dedup();
    Ok(Some(list))
}

fn filter(
    key: &Option<TextMatch>,
    value: &Option<TextMatch>,
    header: &Option<HeaderMatch>,
    json_path: &Option<String>,
) -> MessageFilter {
    let text = |m: &Option<TextMatch>| m.clone().filter(|m| !m.pattern.is_empty());
    MessageFilter {
        key: text(key),
        value: text(value),
        header: header.clone().filter(|h| !h.name.trim().is_empty()),
        json_path: json_path.clone().filter(|j| !j.trim().is_empty()),
    }
}

/// Проверенные параметры `read_messages`: операция, хеш отбора (курсор) и предел текста значения.
struct ReadRequest {
    op: MessagesOp,
    hash: u64,
    max_value: usize,
}

fn read_request(p: &ReadMessagesParams) -> Result<ReadRequest, BackendError> {
    let time = p.from.is_some() || p.to.is_some();
    let offsets = p.partition.is_some() || p.start_offset.is_some() || p.end_offset.is_some();
    let range = match (time, offsets) {
        (true, true) => {
            return Err(BackendError::invalid_argument(
                "pass either from and to (a time window) or partition and startOffset (an offset range), not both",
            ));
        }
        (false, false) => {
            return Err(BackendError::invalid_argument(
                "pass from and to (a time window) or partition and startOffset (an offset range)",
            ));
        }
        (true, false) => {
            let (Some(from), Some(to)) = (&p.from, &p.to) else {
                return Err(BackendError::invalid_argument("pass both from and to"));
            };
            let (from_ms, to_ms) = window(from, to)?;
            Range::Window {
                from_ms,
                to_ms,
                partitions: partition_list(&p.partitions)?,
            }
        }
        (false, true) => {
            let (Some(partition), Some(start)) = (p.partition, p.start_offset) else {
                return Err(BackendError::invalid_argument(
                    "offsets mode needs partition and startOffset",
                ));
            };
            if p.partitions.as_ref().is_some_and(|l| !l.is_empty()) {
                return Err(BackendError::invalid_argument(
                    "partitions is for the window mode; in the offsets mode pass one partition",
                ));
            }
            if partition < 0 || start < 0 {
                return Err(BackendError::invalid_argument(
                    "partition and startOffset must not be negative",
                ));
            }
            if p.end_offset.is_some_and(|e| e <= start) {
                return Err(BackendError::invalid_argument(
                    "endOffset must be greater than startOffset",
                ));
            }
            Range::Offsets {
                partition,
                start,
                end: p.end_offset,
            }
        }
    };
    let limit = in_range(p.limit, DEFAULT_LIMIT, 1, MAX_LIMIT, "limit")?;
    let max_value = in_range(
        p.max_value_bytes,
        DEFAULT_MAX_VALUE_BYTES,
        MIN_MAX_VALUE_BYTES,
        MAX_MAX_VALUE_BYTES,
        "maxValueBytes",
    )?;
    let filter = filter(&p.key, &p.value, &p.header, &p.json_path);
    let (key_format, value_format) = (
        p.key_format.unwrap_or_default(),
        p.value_format.unwrap_or_default(),
    );
    let hash = selection_hash(&(
        &p.connection_id,
        &p.topic,
        &range,
        &filter,
        key_format,
        value_format,
    ));
    let after = p
        .cursor
        .as_deref()
        .map(|c| decode_cursor(c, hash))
        .transpose()
        .map_err(BackendError::invalid_argument)?;
    Ok(ReadRequest {
        op: MessagesOp::Read {
            connection_id: p.connection_id.clone(),
            topic: p.topic.clone(),
            range,
            filter,
            key_format,
            value_format,
            limit: limit as usize,
            after,
        },
        hash,
        max_value: max_value as usize,
    })
}

fn summarize_op(p: &SummarizeMessagesParams) -> Result<MessagesOp, BackendError> {
    let (from_ms, to_ms) = window(&p.from, &p.to)?;
    let bucket_ms = match p.bucket_seconds {
        None => summary::default_bucket_ms(from_ms, to_ms),
        Some(s) => {
            i64::from(in_range(
                Some(s),
                s,
                1,
                MAX_BUCKET_SECONDS,
                "bucketSeconds",
            )?) * 1000
        }
    };
    let buckets = summary::bucket_count(from_ms, to_ms, bucket_ms);
    if buckets > MAX_BUCKETS {
        let fix = if bucket_ms < i64::from(MAX_BUCKET_SECONDS) * 1000 {
            "pass a larger bucketSeconds".to_string()
        } else {
            format!(
                "bucketSeconds is already the largest ({MAX_BUCKET_SECONDS}), narrow the window"
            )
        };
        return Err(BackendError::invalid_argument(format!(
            "the window gives {buckets} buckets, at most {MAX_BUCKETS}: {fix}"
        )));
    }
    let top_keys = in_range(p.top_keys, DEFAULT_TOP_KEYS, 0, MAX_TOP_KEYS, "topKeys")?;
    Ok(MessagesOp::Summarize {
        connection_id: p.connection_id.clone(),
        topic: p.topic.clone(),
        from_ms,
        to_ms,
        partitions: partition_list(&p.partitions)?,
        filter: filter(&p.key, &p.value, &p.header, &p.json_path),
        key_format: p.key_format.unwrap_or_default(),
        value_format: p.value_format.unwrap_or_default(),
        bucket_ms,
        top_keys: top_keys as usize,
    })
}

fn history_op(p: &CommitHistoryParams) -> Result<MessagesOp, BackendError> {
    let (from_ms, to_ms) = window(&p.from, &p.to)?;
    if p.group_id.trim().is_empty() {
        return Err(BackendError::invalid_argument("groupId must not be empty"));
    }
    Ok(MessagesOp::CommitHistory {
        connection_id: p.connection_id.clone(),
        group_id: p.group_id.clone(),
        from_ms,
        to_ms,
        topic: p.topic.clone().filter(|t| !t.trim().is_empty()),
    })
}

// ---- Ответы ----

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TimeZone {
    utc_offset: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WindowOut {
    from: String,
    to: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RangeOut {
    partition: i32,
    start_offset: i64,
    end_offset: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HeaderOut {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MessageOut {
    partition: i32,
    offset: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<String>,
    timestamp_type: TimestampType,
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<MaskedData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<MaskedData>,
    headers: Vec<HeaderOut>,
    #[serde(skip_serializing_if = "is_false")]
    headers_truncated: bool,
    #[serde(skip_serializing_if = "is_zero")]
    masked: usize,
    #[serde(skip_serializing_if = "is_false")]
    masking_limited: bool,
    /// Ключ порядка для курсора после обрезки страницы — LLM не нужен.
    #[serde(skip)]
    sort_key: SortKey,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReadBody {
    topic: String,
    mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    window: Option<WindowOut>,
    #[serde(skip_serializing_if = "Option::is_none")]
    range: Option<RangeOut>,
    time_zone: TimeZone,
    partitions: Vec<PartitionRange>,
    scanned: u64,
    matched: u64,
    complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    stopped_by: Option<StopReason>,
    messages: Vec<MessageOut>,
    masked: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
    #[serde(skip_serializing_if = "is_false")]
    truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<String>,
}

fn messages_of(b: &mut ReadBody) -> &mut Vec<MessageOut> {
    &mut b.messages
}

fn mask_message(m: RawMessage, masker: &Masker, max_value: usize) -> MessageOut {
    let mut n = 0;
    let binary = |d: &Option<RawData>| {
        d.as_ref()
            .is_some_and(|d| matches!(d.format, DataFormat::Hex | DataFormat::Base64))
    };
    let masking_limited = binary(&m.key) || binary(&m.value);
    let sort_key = m.sort_key();
    let key = m
        .key
        .map(|d| masker.data(&d.bytes, d.size, d.format, MAX_KEY_TEXT, &mut n));
    let value = m
        .value
        .map(|d| masker.data(&d.bytes, d.size, d.format, max_value, &mut n));
    let headers = m
        .headers
        .into_iter()
        .map(|(name, value)| HeaderOut {
            value: value.map(|v| cut_utf8(masker.header(&name, &v, &mut n), MAX_HEADER_TEXT).0),
            name,
        })
        .collect();
    MessageOut {
        partition: m.partition,
        offset: m.offset,
        timestamp: m.timestamp.map(iso),
        timestamp_type: m.timestamp_type,
        key,
        value,
        headers,
        headers_truncated: m.headers_truncated,
        masked: n,
        masking_limited,
        sort_key,
    }
}

fn hints(parts: &[Option<&str>]) -> Option<String> {
    let v: Vec<&str> = parts.iter().flatten().copied().collect();
    (!v.is_empty()).then(|| v.join(" "))
}

fn read_body(req: &ReadRequest, out: MessagesOutput) -> Result<Value, BackendError> {
    let MessagesBody::Read {
        stats,
        messages,
        more,
    } = out.body
    else {
        return Err(BackendError::unexpected());
    };
    let MessagesOp::Read { topic, range, .. } = &req.op else {
        return Err(BackendError::unexpected());
    };
    let masker = Masker::new(&out.mask);
    let messages: Vec<MessageOut> = messages
        .into_iter()
        .map(|m| mask_message(m, &masker, req.max_value))
        .collect();
    let (mode, window, range) = match range {
        Range::Window { from_ms, to_ms, .. } => (
            "window",
            Some(WindowOut {
                from: iso(*from_ms),
                to: iso(*to_ms),
            }),
            None,
        ),
        Range::Offsets {
            partition,
            start,
            end,
        } => (
            "offsets",
            None,
            Some(RangeOut {
                partition: *partition,
                start_offset: stats.partitions.first().map_or(*start, |p| p.start_offset),
                end_offset: stats
                    .partitions
                    .first()
                    .map_or(end.unwrap_or(*start), |p| p.end_offset),
            }),
        ),
    };
    let mut body = ReadBody {
        topic: topic.clone(),
        mode,
        window,
        range,
        time_zone: TimeZone {
            utc_offset: utc_offset(out.utc_offset_minutes),
        },
        complete: stats.stopped_by.is_none(),
        stopped_by: stats.stopped_by,
        partitions: stats.partitions,
        scanned: stats.scanned,
        matched: stats.matched,
        masked: 0,
        messages,
        next_cursor: None,
        truncated: false,
        hint: None,
    };
    let cut = rules::fit(&mut body, messages_of);
    body.masked = body.messages.iter().map(|m| m.masked).sum();
    if (more || cut)
        && let Some(last) = body.messages.last()
    {
        body.next_cursor = Some(encode_cursor(req.hash, last.sort_key));
    }
    body.truncated = cut;
    body.hint = hints(&[
        (!body.complete).then_some(INCOMPLETE_HINT),
        cut.then_some(PAGE_CUT_HINT),
    ]);
    to_value(&body)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SummaryPartition {
    partition: i32,
    start_offset: i64,
    end_offset: i64,
    matched: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    first_timestamp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_timestamp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scanned_to: Option<i64>,
}

#[derive(Serialize)]
struct Bucket {
    start: String,
    count: u64,
}

#[derive(Serialize)]
struct KeyCount {
    key: String,
    count: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Keys {
    distinct: u64,
    #[serde(skip_serializing_if = "is_false")]
    distinct_capped: bool,
    null_keys: u64,
    top: Vec<KeyCount>,
}

#[derive(Serialize)]
struct PartitionOffset {
    partition: i32,
    offset: i64,
}

#[derive(Serialize)]
struct OutOfOrder {
    count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    first: Option<PartitionOffset>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TimestampTypes {
    create: u64,
    log_append: u64,
}

#[derive(Serialize)]
struct NameCount {
    name: String,
    count: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SummaryBody {
    topic: String,
    window: WindowOut,
    time_zone: TimeZone,
    scanned: u64,
    matched: u64,
    complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    stopped_by: Option<StopReason>,
    partitions: Vec<SummaryPartition>,
    bucket_seconds: i64,
    buckets: Vec<Bucket>,
    keys: Keys,
    #[serde(skip_serializing_if = "Option::is_none")]
    sizes: Option<Sizes>,
    out_of_order: OutOfOrder,
    timestamp_types: TimestampTypes,
    header_names: Vec<NameCount>,
    masked: usize,
    #[serde(skip_serializing_if = "is_false")]
    truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<String>,
}

fn header_names_of(b: &mut SummaryBody) -> &mut Vec<NameCount> {
    &mut b.header_names
}

fn top_keys_of(b: &mut SummaryBody) -> &mut Vec<KeyCount> {
    &mut b.keys.top
}

fn summary_body(op: &MessagesOp, out: MessagesOutput) -> Result<Value, BackendError> {
    let (
        MessagesOp::Summarize {
            topic,
            from_ms,
            to_ms,
            ..
        },
        MessagesBody::Summarize(s),
    ) = (op, out.body)
    else {
        return Err(BackendError::unexpected());
    };
    let Summary {
        stats,
        partitions,
        bucket_ms,
        buckets,
        distinct_keys,
        distinct_capped,
        null_keys,
        top_keys,
        sizes,
        out_of_order,
        first_out_of_order,
        create,
        log_append,
        header_names,
    } = s;
    let masker = Masker::new(&out.mask);
    let mut masked = 0;
    let top = top_keys
        .into_iter()
        .map(|k| KeyCount {
            key: masker
                .data(&k.bytes, k.size, k.format, KEY_PREFIX_BYTES, &mut masked)
                .text,
            count: k.count,
        })
        .collect();
    let partitions = stats
        .partitions
        .iter()
        .map(|r| {
            let s = partitions.iter().find(|p| p.partition == r.partition);
            SummaryPartition {
                partition: r.partition,
                start_offset: r.start_offset,
                end_offset: r.end_offset,
                matched: s.map_or(0, |s| s.matched),
                first_timestamp: s.and_then(|s| s.first_ts).map(iso),
                last_timestamp: s.and_then(|s| s.last_ts).map(iso),
                scanned_to: r.scanned_to,
            }
        })
        .collect();
    let mut body = SummaryBody {
        topic: topic.clone(),
        window: WindowOut {
            from: iso(*from_ms),
            to: iso(*to_ms),
        },
        time_zone: TimeZone {
            utc_offset: utc_offset(out.utc_offset_minutes),
        },
        scanned: stats.scanned,
        matched: stats.matched,
        complete: stats.stopped_by.is_none(),
        stopped_by: stats.stopped_by,
        partitions,
        bucket_seconds: bucket_ms / 1000,
        buckets: buckets
            .into_iter()
            .map(|(start, count)| Bucket {
                start: iso(start),
                count,
            })
            .collect(),
        keys: Keys {
            distinct: distinct_keys,
            distinct_capped,
            null_keys,
            top,
        },
        sizes,
        out_of_order: OutOfOrder {
            count: out_of_order,
            first: first_out_of_order
                .map(|(partition, offset)| PartitionOffset { partition, offset }),
        },
        timestamp_types: TimestampTypes { create, log_append },
        header_names: header_names
            .into_iter()
            .map(|(name, count)| NameCount { name, count })
            .collect(),
        masked,
        truncated: false,
        hint: None,
    };
    let cut = rules::fit(&mut body, header_names_of) | rules::fit(&mut body, top_keys_of);
    body.truncated = cut;
    body.hint = hints(&[
        (!body.complete).then_some(INCOMPLETE_HINT),
        cut.then_some(SUMMARY_CUT_HINT),
    ]);
    to_value(&body)
}

#[derive(Serialize)]
struct CommitOut {
    offset: i64,
    at: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RunOut {
    offset: i64,
    first_at: String,
    last_at: String,
    commits: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HistoryPartition {
    topic: String,
    partition: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    before: Option<CommitOut>,
    runs: Vec<RunOut>,
    #[serde(skip_serializing_if = "is_false")]
    runs_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    deleted_at: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HistoryBody {
    group_id: String,
    window: WindowOut,
    time_zone: TimeZone,
    offsets_topic_partition: i32,
    scanned: u64,
    skipped_records: u64,
    complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    stopped_by: Option<StopReason>,
    partitions: Vec<HistoryPartition>,
    masked: usize,
    #[serde(skip_serializing_if = "is_false")]
    truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<String>,
}

fn fits(body: &HistoryBody) -> bool {
    serde_json::to_vec(body).map_or(usize::MAX, |v| v.len()) <= MAX_RESPONSE_BYTES
}

/// Оставляет у каждой партиции не больше `k` самых новых `runs`; отброшенные (самые старые)
/// копятся в `dropped` и возвращаются, если `k` растёт. `capped` — `runsTruncated` до обрезки.
fn keep_newest(body: &mut HistoryBody, dropped: &mut [Vec<RunOut>], capped: &[bool], k: usize) {
    for ((p, old), capped) in body.partitions.iter_mut().zip(dropped).zip(capped) {
        old.append(&mut p.runs);
        p.runs = old.split_off(old.len().saturating_sub(k));
        p.runs_truncated = *capped || !old.is_empty();
    }
}

/// Укладывает ответ в 100 КБ пропорционально: у каждой партиции остаются не больше `k` самых
/// новых `runs` с наибольшим подходящим `k` (двоичный поиск, ~10 сериализаций), поэтому старые
/// `runs` первыми теряют самые длинные партиции. true — что-то отброшено.
fn fit_runs(body: &mut HistoryBody) -> bool {
    let longest = body.partitions.iter().map(|p| p.runs.len()).max();
    let Some(longest) = longest.filter(|n| *n > 0 && !fits(body)) else {
        return false;
    };
    let capped: Vec<bool> = body.partitions.iter().map(|p| p.runs_truncated).collect();
    let mut dropped: Vec<Vec<RunOut>> = body.partitions.iter().map(|_| Vec::new()).collect();
    // Наибольший `k` в 0..longest, при котором ответ помещается; не помещается и без `runs` —
    // остаётся 0, а `fit_history` ответит response_too_large.
    let (mut lo, mut hi) = (0, longest - 1);
    while lo < hi {
        let mid = (lo + hi).div_ceil(2);
        keep_newest(body, &mut dropped, &capped, mid);
        if fits(body) {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    keep_newest(body, &mut dropped, &capped, lo);
    true
}

/// Укладывает историю в 100 КБ ([`fit_runs`]); не помещается и без `runs` — `response_too_large`,
/// а не история, похожая на «коммитов не было». `Ok(true)` — что-то отброшено.
fn fit_history(body: &mut HistoryBody) -> Result<bool, BackendError> {
    let cut = fit_runs(body);
    if fits(body) {
        Ok(cut)
    } else {
        Err(BackendError::new("response_too_large", HISTORY_TOO_LARGE))
    }
}

fn history_body(op: &MessagesOp, out: MessagesOutput) -> Result<Value, BackendError> {
    let (
        MessagesOp::CommitHistory {
            group_id,
            from_ms,
            to_ms,
            ..
        },
        MessagesBody::CommitHistory(CommitHistory { stats, partitions }),
    ) = (op, out.body)
    else {
        return Err(BackendError::unexpected());
    };
    let masker = Masker::new(&out.mask);
    let mut masked = 0;
    let partitions = partitions
        .into_iter()
        .map(|p| HistoryPartition {
            topic: p.topic,
            partition: p.partition,
            before: p.before.map(|c| CommitOut {
                offset: c.offset,
                at: iso(c.at_ms),
            }),
            runs: p
                .runs
                .into_iter()
                .map(|r| RunOut {
                    offset: r.offset,
                    first_at: iso(r.first_at_ms),
                    last_at: iso(r.last_at_ms),
                    commits: r.commits,
                    metadata: (!r.metadata.is_empty()).then(|| {
                        cut_utf8(masker.text(&r.metadata, &mut masked), MAX_METADATA_TEXT).0
                    }),
                })
                .collect(),
            runs_truncated: p.runs_truncated,
            deleted_at: p.deleted_at_ms.map(iso),
        })
        .collect::<Vec<_>>();
    let empty = partitions.is_empty();
    let mut body = HistoryBody {
        group_id: group_id.clone(),
        window: WindowOut {
            from: iso(*from_ms),
            to: iso(*to_ms),
        },
        time_zone: TimeZone {
            utc_offset: utc_offset(out.utc_offset_minutes),
        },
        offsets_topic_partition: stats.offsets_partition,
        scanned: stats.scanned,
        skipped_records: stats.skipped_records,
        complete: stats.stopped_by.is_none(),
        stopped_by: stats.stopped_by,
        partitions,
        masked,
        truncated: false,
        hint: None,
    };
    let cut = fit_history(&mut body)?;
    body.truncated = cut;
    body.hint = hints(&[
        (!body.complete).then_some(HISTORY_INCOMPLETE_HINT),
        empty.then_some(NO_COMMITS_HINT),
        cut.then_some(RUNS_CUT_HINT),
    ]);
    to_value(&body)
}

/// Поля журнала у трёх инструментов: просмотрено, отдано (сообщений, корзин или `runs`),
/// замаскировано. Содержимое ответа в журнал не попадает.
pub(super) fn audit_counts(tool: &str, v: &Value) -> Option<(u64, u64, u64)> {
    let len = |x: &Value| x.as_array().map_or(0, |a| a.len() as u64);
    let returned = match tool {
        "read_messages" => len(&v["messages"]),
        "summarize_messages" => len(&v["buckets"]),
        "get_group_commit_history" => v["partitions"]
            .as_array()
            .map_or(0, |ps| ps.iter().map(|p| len(&p["runs"])).sum()),
        _ => return None,
    };
    Some((
        v["scanned"].as_u64().unwrap_or(0),
        returned,
        v["masked"].as_u64().unwrap_or(0),
    ))
}

impl GatewayServer {
    /// Просмотр в приложении: одна длительная операция на сессию, слоты Kafka, бюджет, ход и отмена.
    /// Дедлайн и страховочный таймаут отсчитываются от начала вызова, включая ожидание слотов.
    async fn scan(
        &self,
        op: MessagesOp,
        ctx: &RequestContext<RoleServer>,
    ) -> Result<MessagesOutput, BackendError> {
        let started = Instant::now();
        let budget = self.shared.options.scan_deadline;
        let safety =
            tokio::time::Instant::from_std(started + budget + self.shared.options.scan_grace);
        let timeout = || BackendError::new("TIMEOUT", "Reading messages did not finish in time");
        let _long = self.begin_long()?;
        let _slots = tokio::time::timeout_at(safety, self.slots())
            .await
            .map_err(|_| timeout())??;
        let mut scan = ScanCtx::new(budget);
        scan.deadline = started + budget;
        let cancel = scan.cancel.clone();
        let (tx, mut rx) = tokio::sync::watch::channel((0u64, 0u64));
        scan.progress = Arc::new(move |done, total| {
            let _ = tx.send((done, total));
        });
        let forward = ctx.meta.get_progress_token().map(|token| {
            let peer = ctx.peer.clone();
            tokio::spawn(async move {
                while rx.changed().await.is_ok() {
                    let (done, total) = *rx.borrow_and_update();
                    let note = ProgressNotificationParam::new(token.clone(), done as f64)
                        .with_total(total as f64);
                    let _ = peer.notify_progress(note).await;
                }
            })
        });
        let watcher = {
            let (cancel, ct) = (cancel.clone(), ctx.ct.clone());
            tokio::spawn(async move {
                ct.cancelled().await;
                cancel.store(true, Ordering::Relaxed);
            })
        };
        let result = tokio::time::timeout_at(safety, self.shared.backend.messages(op, scan)).await;
        watcher.abort();
        if let Some(f) = forward {
            f.abort();
        }
        result.map_err(|_| {
            // Приложение может читать в своём потоке и после того, как его future отброшена.
            cancel.store(true, Ordering::Relaxed);
            timeout()
        })?
    }
}

#[tool_router(router = message_router, vis = "pub(crate)")]
impl GatewayServer {
    #[tool(
        description = "Read messages of a topic for debugging: everything written in a time window (from/to) or an offset range of one partition (partition, startOffset, endOffset). Times need a time zone: if the user's logs have none, ask which one they use. Filters work as in Glafira's message browser and see unmasked data, so you can find a message by a value from a log: key, value (substring or regex), header, jsonPath. Messages come ordered by timestamp across partitions; page with nextCursor. Values matching the connection's masking rules come back as ***, [email], [card], [jwt] or [token] — do not try to restore them. A message whose timestamp is inside the window but whose offset lies outside the window's offset range (producer clocks out of order) is not found. Use summarize_messages first for a large window. Available only for connections with llmAccess messages.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn read_messages(
        &self,
        Parameters(p): Parameters<ReadMessagesParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = to_value(&p).unwrap_or(Value::Null);
        Ok(self
            .call(&ctx, "read_messages", args, Some(&p.connection_id), async {
                let req = read_request(&p)?;
                let out = self.scan(req.op.clone(), &ctx).await?;
                read_body(&req, out)
            })
            .await)
    }

    #[tool(
        description = "Overview of a topic's time window without message contents: message counts per partition and per time bucket, the most frequent keys (keys with count > 1 are repeated), value sizes, timestamps going backwards within a partition (outOfOrder) and header names. Use it to find spikes, gaps and duplicates before reading messages. Same filters as read_messages. Available only for connections with llmAccess messages.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn summarize_messages(
        &self,
        Parameters(p): Parameters<SummarizeMessagesParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = to_value(&p).unwrap_or(Value::Null);
        Ok(self
            .call(
                &ctx,
                "summarize_messages",
                args,
                Some(&p.connection_id),
                async {
                    let op = summarize_op(&p)?;
                    let out = self.scan(op.clone(), &ctx).await?;
                    summary_body(&op, out)
                },
            )
            .await)
    }

    #[tool(
        description = "Which offsets a consumer group committed and when, from the __consumer_offsets topic: per partition the last commit before the window (before) and runs of repeated commits of the same offset inside it. The messages the group processed between two commits can be read with read_messages in the offsets mode. Needs the Read permission on __consumer_offsets; compaction removes older commits (recent ones are usually there). Available only for connections with llmAccess messages.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn get_group_commit_history(
        &self,
        Parameters(p): Parameters<CommitHistoryParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = to_value(&p).unwrap_or(Value::Null);
        Ok(self
            .call(
                &ctx,
                "get_group_commit_history",
                args,
                Some(&p.connection_id),
                async {
                    let op = history_op(&p)?;
                    let out = self.scan(op.clone(), &ctx).await?;
                    history_body(&op, out)
                },
            )
            .await)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::commits::MAX_RUNS;

    fn history(partitions: i32, runs: usize) -> HistoryBody {
        let run = |offset: i64| RunOut {
            offset,
            first_at: iso(offset),
            last_at: iso(offset),
            commits: 1,
            metadata: None,
        };
        HistoryBody {
            group_id: "g".into(),
            window: WindowOut {
                from: iso(0),
                to: iso(1),
            },
            time_zone: TimeZone {
                utc_offset: utc_offset(0),
            },
            offsets_topic_partition: 0,
            scanned: 0,
            skipped_records: 0,
            complete: true,
            stopped_by: None,
            partitions: (0..partitions)
                .map(|partition| HistoryPartition {
                    topic: "orders".into(),
                    partition,
                    before: None,
                    runs: (0..runs as i64).map(run).collect(),
                    runs_truncated: false,
                    deleted_at: None,
                })
                .collect(),
            masked: 0,
            truncated: false,
            hint: None,
        }
    }

    #[test]
    fn runs_are_cut_evenly_and_fast_keeping_the_newest() {
        let mut body = history(60, MAX_RUNS);
        body.partitions[0].runs.truncate(3);
        let started = Instant::now();
        assert!(fit_runs(&mut body));
        let elapsed = started.elapsed();
        // Обрезка по десятой части самой длинной партиции сериализует ответ в мегабайты тысячи раз (минуты в debug);
        // двоичный поиск — около десятка сериализаций. Граница с запасом на медленный раннер CI.
        assert!(elapsed < Duration::from_secs(10), "{elapsed:?}");
        let size = serde_json::to_vec(&body).unwrap().len();
        assert!(
            size <= MAX_RESPONSE_BYTES && size > MAX_RESPONSE_BYTES * 9 / 10,
            "{size}"
        );
        let short = &body.partitions[0];
        assert_eq!(short.runs.len(), 3, "короткая партиция не теряет ничего");
        assert!(!short.runs_truncated);
        let k = body.partitions[1].runs.len();
        assert!(k > 0 && k < MAX_RUNS, "{k}");
        for p in &body.partitions[1..] {
            assert_eq!(p.runs.len(), k, "длинные партиции — поровну");
            assert!(p.runs_truncated);
            assert_eq!(
                p.runs[0].offset,
                (MAX_RUNS - k) as i64,
                "остались самые новые"
            );
            assert_eq!(p.runs[k - 1].offset, MAX_RUNS as i64 - 1);
        }
    }

    #[test]
    fn fitting_history_is_left_as_is() {
        let mut body = history(2, 10);
        assert!(!fit_runs(&mut body));
        assert!(
            body.partitions
                .iter()
                .all(|p| p.runs.len() == 10 && !p.runs_truncated)
        );
    }

    #[test]
    fn history_that_does_not_fit_even_without_runs_is_too_large() {
        let mut body = history(3000, 1);
        let e = fit_history(&mut body).unwrap_err();
        assert_eq!(e.code, "response_too_large");
        assert_eq!(e.message, HISTORY_TOO_LARGE);
        // Без `runs` помещается — ответ с обрезкой, как раньше.
        let mut body = history(1200, 2);
        assert_eq!(fit_history(&mut body).ok(), Some(true));
        assert!(
            body.partitions
                .iter()
                .all(|p| p.runs.is_empty() && p.runs_truncated)
        );
    }
}
