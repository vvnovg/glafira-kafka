//! Чтение сообщений для LLM (спец. G4 §3): операции [`MessagesOp`], которые выполняет приложение,
//! сырые записи, которые оно возвращает, разбор времени и курсор страниц. Маскирует и собирает
//! ответ крейт (`server/message_tools.rs`): приложение отдаёт данные без маскировки.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use base64::Engine;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::commits::CommitHistory;
use crate::mask::{DataFormat, MaskConfig};
use crate::summary::Summary;

/// Бюджет одного просмотра: время и число записей (спец. §3.1).
pub const SCAN_DEADLINE: Duration = Duration::from_secs(40);
pub const SCAN_RECORD_LIMIT: u64 = 1_000_000;
/// Запас сверх бюджета просмотра: приложение закрывает консьюмер и возвращает собранное.
pub const SCAN_GRACE: Duration = Duration::from_secs(5);
/// Отступ до начала окна в истории коммитов: состояние группы на начало окна.
pub const COMMIT_LOOKBACK_MS: i64 = 10 * 60 * 1000;
pub const DEFAULT_LIMIT: u32 = 50;
pub const MAX_LIMIT: u32 = 200;
pub const DEFAULT_MAX_VALUE_BYTES: u32 = 2048;
pub const MIN_MAX_VALUE_BYTES: u32 = 256;
pub const MAX_MAX_VALUE_BYTES: u32 = 16384;
pub const MAX_KEY_TEXT: usize = 512;
pub const MAX_HEADER_TEXT: usize = 512;
pub const MAX_HEADERS: usize = 32;

/// Text filter: a substring or a regular expression.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TextMatch {
    /// Substring or regular expression.
    pub pattern: String,
    /// Treat pattern as a regular expression. Default: false.
    #[serde(default)]
    pub regex: bool,
    /// Case-sensitive match. Default: false.
    #[serde(default)]
    pub case_sensitive: bool,
}

/// Header filter: a header with this name, optionally with a matching value.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HeaderMatch {
    /// Header name, exact.
    pub name: String,
    /// Match of the header value.
    #[serde(default)]
    pub value: Option<TextMatch>,
}

/// How to decode keys and values: auto detects JSON, text or binary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum FormatArg {
    #[default]
    Auto,
    String,
    Json,
    Hex,
    Base64,
}

/// Time as an RFC 3339 string with a time zone (YYYY-MM-DDThh:mm:ss±hh:mm, the date and time come from the user's request or logs) or epoch milliseconds.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum TimeArg {
    Millis(i64),
    Text(String),
}

impl TimeArg {
    /// Epoch ms. Строка без зоны — отдельная ошибка: модель должна спросить часовой пояс логов.
    pub fn to_ms(&self, name: &str) -> Result<i64, String> {
        let ms = match self {
            TimeArg::Millis(ms) => *ms,
            TimeArg::Text(s) => {
                let s = s.trim();
                match DateTime::parse_from_rfc3339(s) {
                    Ok(t) => t.timestamp_millis(),
                    Err(_) if has_no_zone(s) => {
                        return Err(format!(
                            "{name}: timestamp has no time zone; ask the user which time zone their logs use and pass it as YYYY-MM-DDThh:mm:ss±hh:mm"
                        ));
                    }
                    Err(e) => {
                        return Err(format!(
                            "{name}: expected RFC 3339 time with a time zone or epoch milliseconds: {e}"
                        ));
                    }
                }
            }
        };
        if ms < 0 {
            return Err(format!("{name} must not be before 1970-01-01"));
        }
        Ok(ms)
    }
}

/// Дата и время без зоны в любой из привычных записей: пробел или `T`/`t` между датой и временем,
/// запятая (log4j) или точка в дробной части, время до минут, только дата.
fn has_no_zone(s: &str) -> bool {
    let s = s.replacen('t', "T", 1).replace(',', ".");
    NaiveDate::parse_from_str(&s, "%Y-%m-%d").is_ok()
        || [
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M",
        ]
        .iter()
        .any(|f| NaiveDateTime::parse_from_str(&s, f).is_ok())
}

/// ISO 8601 UTC с миллисекундами: `2026-10-24T11:32:05.123Z`.
pub fn iso(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .map(|t| t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        .unwrap_or_else(|| ms.to_string())
}

/// Окно `read_messages` (по времени) или диапазон офсетов одной партиции.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(
    tag = "mode",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Range {
    Window {
        from_ms: i64,
        to_ms: i64,
        partitions: Option<Vec<i32>>,
    },
    Offsets {
        partition: i32,
        start: i64,
        /// Не включительно; `None` — до high watermark.
        end: Option<i64>,
    },
}

/// Фильтры экрана «Сообщения»; приложение применяет их к немаскированным данным (спец. §1).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageFilter {
    pub key: Option<TextMatch>,
    pub value: Option<TextMatch>,
    pub header: Option<HeaderMatch>,
    pub json_path: Option<String>,
}

/// Порядок сообщений в ответе: (метка времени, партиция, офсет); без метки — `i64::MIN`.
pub type SortKey = (i64, i32, i64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessagesOp {
    Read {
        connection_id: String,
        topic: String,
        range: Range,
        filter: MessageFilter,
        key_format: FormatArg,
        value_format: FormatArg,
        limit: usize,
        /// Курсор: только ключи больше этого.
        after: Option<SortKey>,
    },
    Summarize {
        connection_id: String,
        topic: String,
        from_ms: i64,
        to_ms: i64,
        partitions: Option<Vec<i32>>,
        filter: MessageFilter,
        key_format: FormatArg,
        value_format: FormatArg,
        bucket_ms: i64,
        top_keys: usize,
    },
    CommitHistory {
        connection_id: String,
        group_id: String,
        from_ms: i64,
        to_ms: i64,
        topic: Option<String>,
    },
}

impl MessagesOp {
    pub fn connection_id(&self) -> &str {
        match self {
            MessagesOp::Read { connection_id, .. }
            | MessagesOp::Summarize { connection_id, .. }
            | MessagesOp::CommitHistory { connection_id, .. } => connection_id,
        }
    }
}

/// Ход просмотра: просмотрено и всего записей в диапазонах.
pub type Progress = Arc<dyn Fn(u64, u64) + Send + Sync>;

/// Дедлайн, отмена и ход просмотра — приложение проверяет их в цикле чтения.
#[derive(Clone)]
pub struct ScanCtx {
    pub deadline: Instant,
    pub cancel: Arc<AtomicBool>,
    pub progress: Progress,
}

impl ScanCtx {
    pub fn new(budget: Duration) -> Self {
        Self {
            deadline: Instant::now() + budget,
            cancel: Arc::new(AtomicBool::new(false)),
            progress: Arc::new(|_, _| {}),
        }
    }

    pub fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::Relaxed)
    }

    pub fn expired(&self) -> bool {
        Instant::now() >= self.deadline
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum TimestampType {
    Create,
    LogAppend,
    Unknown,
}

/// Байты ключа или значения: первые [`crate::mask::MASK_PARSE_LIMIT`], исходный размер и формат.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawData {
    pub bytes: Vec<u8>,
    pub size: usize,
    pub format: DataFormat,
}

/// Сообщение без маскировки — маскирует крейт.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawMessage {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub timestamp_type: TimestampType,
    pub key: Option<RawData>,
    pub value: Option<RawData>,
    /// Не больше [`MAX_HEADERS`]; значение — текст (`browse::format::header_text`).
    pub headers: Vec<(String, Option<String>)>,
    pub headers_truncated: bool,
}

impl RawMessage {
    pub fn sort_key(&self) -> SortKey {
        (
            self.timestamp.unwrap_or(i64::MIN),
            self.partition,
            self.offset,
        )
    }
}

/// Диапазон офсетов партиции; `scanned_to` — первый непросмотренный, если бюджет кончился.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionRange {
    pub partition: i32,
    pub start_offset: i64,
    pub end_offset: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scanned_to: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum StopReason {
    TimeBudget,
    ScanLimit,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanStats {
    pub partitions: Vec<PartitionRange>,
    pub scanned: u64,
    pub matched: u64,
    pub stopped_by: Option<StopReason>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MessagesBody {
    Read {
        stats: ScanStats,
        /// По возрастанию [`SortKey`], не больше `limit`.
        messages: Vec<RawMessage>,
        /// После последнего отданного остались прошедшие фильтры.
        more: bool,
    },
    Summarize(Summary),
    CommitHistory(CommitHistory),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessagesOutput {
    pub mask: MaskConfig,
    /// Текущий сдвиг машины Glafira от UTC в минутах (для `timeZone`).
    pub utc_offset_minutes: i32,
    pub body: MessagesBody,
}

/// `+03:00`, `-05:30`, `+00:00`.
pub fn utc_offset(minutes: i32) -> String {
    let sign = if minutes < 0 { '-' } else { '+' };
    let m = minutes.unsigned_abs();
    format!("{sign}{:02}:{:02}", m / 60, m % 60)
}

/// FNV-1a: стабильный хеш параметров для курсора (не зависит от версии std).
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
    })
}

#[derive(Serialize, Deserialize)]
struct CursorBody {
    v: u8,
    h: u64,
    after: SortKey,
}

/// Хеш параметров отбора: курсор от другого запроса не подходит (спец. §3.2).
pub fn selection_hash<T: Serialize>(selection: &T) -> u64 {
    fnv1a(&serde_json::to_vec(selection).unwrap_or_default())
}

pub fn encode_cursor(hash: u64, after: SortKey) -> String {
    let body = serde_json::to_vec(&CursorBody {
        v: 1,
        h: hash,
        after,
    })
    .unwrap_or_default();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(body)
}

pub fn decode_cursor(cursor: &str, hash: u64) -> Result<SortKey, String> {
    let bad = || "cursor does not match the request parameters".to_string();
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor.trim())
        .map_err(|_| bad())?;
    let body: CursorBody = serde_json::from_slice(&bytes).map_err(|_| bad())?;
    if body.v != 1 || body.h != hash {
        return Err(bad());
    }
    Ok(body.after)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_need_a_zone() {
        let t = |s: &str| TimeArg::Text(s.into()).to_ms("from");
        assert_eq!(t("2026-10-24T14:32:00+03:00"), Ok(1_792_841_520_000));
        assert_eq!(t("2026-10-24T11:32:00.250Z"), Ok(1_792_841_520_250));
        assert_eq!(t(" 2026-10-24 11:32:00Z "), Ok(1_792_841_520_000));
        assert_eq!(TimeArg::Millis(5).to_ms("from"), Ok(5));
        let no_zone = t("2026-10-24T14:32:00").unwrap_err();
        assert!(
            no_zone.starts_with("from: timestamp has no time zone"),
            "{no_zone}"
        );
        assert!(
            t("2026-10-24 14:32")
                .unwrap_err()
                .starts_with("from: timestamp has no time zone")
        );
        assert!(t("yesterday").unwrap_err().contains("RFC 3339"));
        assert!(
            TimeArg::Millis(-1)
                .to_ms("to")
                .unwrap_err()
                .contains("1970")
        );
        let parsed: TimeArg = serde_json::from_str("1792841520000").unwrap();
        assert_eq!(parsed, TimeArg::Millis(1_792_841_520_000));
    }

    #[test]
    fn every_zone_less_form_asks_for_a_zone() {
        let t = |s: &str| TimeArg::Text(s.into()).to_ms("to");
        for s in [
            "2026-10-24 14:32",
            "2026-10-24T14:32",
            "2026-10-24",
            "2026-10-24 14:32:00",
            "2026-10-24 14:32:00.123",
            // log4j: запятая в дробной части
            "2026-10-24 14:32:00,123",
            "2026-10-24T14:32:00,123",
            // строчная t
            "2026-10-24t14:32:00",
            " 2026-10-24 14:32:00,123456789 ",
        ] {
            let e = t(s).unwrap_err();
            assert!(
                e.starts_with("to: timestamp has no time zone"),
                "{s:?}: {e}"
            );
        }
        // С зоной и мусор — прежние ответы.
        assert_eq!(t("2026-10-24 14:32:00.123+03:00"), Ok(1_792_841_520_123));
        for s in ["yesterday", "14:32", "2026-13-45", "2026-10-24 25:00", ""] {
            let e = t(s).unwrap_err();
            assert!(e.contains("RFC 3339"), "{s:?}: {e}");
        }
    }

    #[test]
    fn iso_and_offsets() {
        assert_eq!(iso(1_792_841_525_123), "2026-10-24T11:32:05.123Z");
        assert_eq!(iso(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(utc_offset(180), "+03:00");
        assert_eq!(utc_offset(-330), "-05:30");
        assert_eq!(utc_offset(0), "+00:00");
    }

    #[test]
    fn cursor_round_trips_only_for_the_same_selection() {
        let a = selection_hash(&("orders", 1, 2));
        let b = selection_hash(&("orders", 1, 3));
        assert_ne!(a, b);
        let c = encode_cursor(a, (i64::MIN, 2, 40));
        assert_eq!(decode_cursor(&c, a), Ok((i64::MIN, 2, 40)));
        assert!(decode_cursor(&c, b).unwrap_err().contains("does not match"));
        assert!(decode_cursor("not base64!", a).is_err());
        let other_version = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&CursorBody {
                v: 2,
                h: a,
                after: (1, 2, 40),
            })
            .unwrap(),
        );
        assert!(
            decode_cursor(&other_version, a)
                .unwrap_err()
                .contains("does not match"),
            "курсор другой версии не принимается"
        );
        assert_eq!(
            selection_hash(&("orders", 1, 2)),
            a,
            "стабилен между вызовами"
        );
    }

    #[test]
    fn scan_ctx_reports_cancel_and_deadline() {
        let ctx = ScanCtx::new(Duration::from_secs(60));
        assert!(!ctx.cancelled() && !ctx.expired());
        ctx.cancel.store(true, Ordering::Relaxed);
        assert!(ctx.cancelled());
        assert!(ScanCtx::new(Duration::ZERO).expired());
    }

    #[test]
    fn range_serializes_with_mode() {
        let w = Range::Window {
            from_ms: 1,
            to_ms: 2,
            partitions: None,
        };
        assert_eq!(
            serde_json::to_value(&w).unwrap(),
            serde_json::json!({"mode": "window", "fromMs": 1, "toMs": 2, "partitions": null})
        );
    }
}
