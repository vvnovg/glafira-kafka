use base64::Engine;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataFormat {
    Auto,
    String,
    Json,
    Hex,
    Base64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DecodedData {
    /// Фактический формат (для Auto — определённый автоматически).
    pub format: DataFormat,
    pub text: String,
    /// Размер исходных байтов.
    pub size: usize,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub const MAX_TEXT_BYTES: usize = 1_048_576;

pub fn decode(bytes: &[u8], format: DataFormat) -> DecodedData {
    let resolved = if format == DataFormat::Auto {
        detect(bytes)
    } else {
        format
    };
    let (text, truncated, error) = match resolved {
        DataFormat::String => {
            let (t, cut) = text_prefix(bytes, MAX_TEXT_BYTES);
            (t, cut, None)
        }
        DataFormat::Json => {
            // Валидность — по всему входу (без копирования), в text — только видимая часть.
            let error = serde_json::from_slice::<serde::de::IgnoredAny>(bytes)
                .err()
                .map(|e| format!("Невалидный JSON: {e}"));
            let (t, cut) = text_prefix(bytes, MAX_TEXT_BYTES);
            (t, cut, error)
        }
        DataFormat::Hex => {
            let take = bytes.len().min(MAX_TEXT_BYTES / 2);
            (hex::encode(&bytes[..take]), take < bytes.len(), None)
        }
        DataFormat::Base64 => {
            let take = bytes.len().min(MAX_TEXT_BYTES / 4 * 3);
            (
                base64::engine::general_purpose::STANDARD.encode(&bytes[..take]),
                take < bytes.len(),
                None,
            )
        }
        DataFormat::Auto => unreachable!("detect никогда не возвращает Auto"),
    };
    DecodedData {
        format: resolved,
        text,
        size: bytes.len(),
        truncated,
        error,
    }
}

/// Текст из первых `max` байтов: граница не режет UTF-8-символ, невалидные байты — `�`.
fn text_prefix(bytes: &[u8], max: usize) -> (String, bool) {
    if bytes.len() <= max {
        return (String::from_utf8_lossy(bytes).into_owned(), false);
    }
    let mut cut = max;
    // Не начинать хвост с продолжающего байта UTF-8 (10xxxxxx): откатываемся к началу символа.
    while cut > 0 && (bytes[cut] & 0b1100_0000) == 0b1000_0000 {
        cut -= 1;
    }
    (String::from_utf8_lossy(&bytes[..cut]).into_owned(), true)
}

fn detect(bytes: &[u8]) -> DataFormat {
    let Ok(s) = std::str::from_utf8(bytes) else {
        return DataFormat::Hex;
    };
    let trimmed = s.trim_start();
    if (trimmed.starts_with('{') || trimmed.starts_with('['))
        && serde_json::from_str::<serde_json::Value>(s).is_ok()
    {
        return DataFormat::Json;
    }
    if s.chars()
        .any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t'))
    {
        return DataFormat::Hex;
    }
    DataFormat::String
}

pub fn header_text(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => format!("0x{}", hex::encode(bytes)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_detects_json_string_and_binary() {
        assert_eq!(
            decode(br#" {"a":1}"#, DataFormat::Auto).format,
            DataFormat::Json
        );
        assert_eq!(decode(b"[1,2]", DataFormat::Auto).format, DataFormat::Json);
        assert_eq!(
            decode(b"{not json", DataFormat::Auto).format,
            DataFormat::String
        );
        assert_eq!(
            decode("привет\n".as_bytes(), DataFormat::Auto).format,
            DataFormat::String
        );
        let bin = decode(&[0x00, 0x01, 0xff], DataFormat::Auto);
        assert_eq!(bin.format, DataFormat::Hex);
        assert_eq!(bin.text, "0001ff");
        assert_eq!(
            decode(&[0x01, b'a'], DataFormat::Auto).format,
            DataFormat::Hex
        ); // управляющий символ
    }

    #[test]
    fn explicit_json_reports_error_but_keeps_text() {
        let d = decode(b"oops", DataFormat::Json);
        assert_eq!(d.format, DataFormat::Json);
        assert_eq!(d.text, "oops");
        assert!(d.error.unwrap().starts_with("Невалидный JSON"));
    }

    #[test]
    fn hex_and_base64() {
        assert_eq!(decode(b"AB", DataFormat::Hex).text, "4142");
        assert_eq!(decode(b"AB", DataFormat::Base64).text, "QUI=");
        assert_eq!(decode(b"AB", DataFormat::String).size, 2);
    }

    #[test]
    fn truncates_on_char_boundary() {
        let s = "я".repeat(MAX_TEXT_BYTES); // 2 байта на символ
        let d = decode(s.as_bytes(), DataFormat::String);
        assert!(d.truncated);
        assert!(d.text.len() <= MAX_TEXT_BYTES);
        assert_eq!(d.size, MAX_TEXT_BYTES * 2);
    }

    #[test]
    fn hex_and_base64_encode_only_the_visible_prefix() {
        let big = vec![0xabu8; MAX_TEXT_BYTES * 3];
        let h = decode(&big, DataFormat::Hex);
        assert!(h.truncated);
        assert!(h.text.len() <= MAX_TEXT_BYTES);
        assert_eq!(h.text.len() % 2, 0, "hex не режется посреди байта");
        assert_eq!(h.size, big.len());
        let b = decode(&big, DataFormat::Base64);
        assert!(b.truncated);
        assert!(b.text.len() <= MAX_TEXT_BYTES);
        assert_eq!(b.text.len() % 4, 0, "base64 — целыми группами");
    }

    #[test]
    fn json_validity_is_checked_on_whole_input() {
        let mut v = b"{\"a\":\"".to_vec();
        v.extend(std::iter::repeat_n(b'x', MAX_TEXT_BYTES * 2));
        v.extend(b"\"}");
        let d = decode(&v, DataFormat::Json);
        assert!(d.truncated);
        assert!(d.error.is_none(), "весь вход — валидный JSON");
        assert!(d.text.len() <= MAX_TEXT_BYTES);
    }

    #[test]
    fn invalid_utf8_tail_is_cut_cleanly() {
        let mut v = vec![b'a'; MAX_TEXT_BYTES - 1];
        v.extend("я".as_bytes()); // 2-байтовый символ пересекает границу
        v.extend(vec![b'b'; 10]);
        let d = decode(&v, DataFormat::String);
        assert!(d.truncated);
        assert_eq!(d.text.len(), MAX_TEXT_BYTES - 1);
        assert!(
            !d.text.contains('\u{FFFD}'),
            "символ на границе не превращается в �"
        );
    }

    #[test]
    fn header_text_utf8_or_hex() {
        assert_eq!(header_text(b"abc"), "abc");
        assert_eq!(header_text(&[0xff, 0x00]), "0xff00");
    }
}
