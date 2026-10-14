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
    let resolved = if format == DataFormat::Auto { detect(bytes) } else { format };
    let (text, error) = match resolved {
        DataFormat::String => (String::from_utf8_lossy(bytes).into_owned(), None),
        DataFormat::Json => {
            let text = String::from_utf8_lossy(bytes).into_owned();
            match serde_json::from_slice::<serde_json::Value>(bytes) {
                Ok(_) => (text, None),
                Err(e) => (text, Some(format!("Невалидный JSON: {e}"))),
            }
        }
        DataFormat::Hex => (hex::encode(bytes), None),
        DataFormat::Base64 => (base64::engine::general_purpose::STANDARD.encode(bytes), None),
        DataFormat::Auto => unreachable!("detect никогда не возвращает Auto"),
    };
    let (text, truncated) = truncate(text, MAX_TEXT_BYTES);
    DecodedData { format: resolved, text, size: bytes.len(), truncated, error }
}

fn detect(bytes: &[u8]) -> DataFormat {
    let Ok(s) = std::str::from_utf8(bytes) else { return DataFormat::Hex };
    let trimmed = s.trim_start();
    if (trimmed.starts_with('{') || trimmed.starts_with('[')) && serde_json::from_str::<serde_json::Value>(s).is_ok() {
        return DataFormat::Json;
    }
    if s.chars().any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t')) {
        return DataFormat::Hex;
    }
    DataFormat::String
}

fn truncate(mut s: String, max: usize) -> (String, bool) {
    if s.len() <= max {
        return (s, false);
    }
    let mut cut = max;
    while !s.is_char_boundary(cut) {
        cut -= 1;
    }
    s.truncate(cut);
    (s, true)
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
        assert_eq!(decode(br#" {"a":1}"#, DataFormat::Auto).format, DataFormat::Json);
        assert_eq!(decode(b"[1,2]", DataFormat::Auto).format, DataFormat::Json);
        assert_eq!(decode(b"{not json", DataFormat::Auto).format, DataFormat::String);
        assert_eq!(decode("привет\n".as_bytes(), DataFormat::Auto).format, DataFormat::String);
        let bin = decode(&[0x00, 0x01, 0xff], DataFormat::Auto);
        assert_eq!(bin.format, DataFormat::Hex);
        assert_eq!(bin.text, "0001ff");
        assert_eq!(decode(&[0x01, b'a'], DataFormat::Auto).format, DataFormat::Hex); // управляющий символ
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
    fn header_text_utf8_or_hex() {
        assert_eq!(header_text(b"abc"), "abc");
        assert_eq!(header_text(&[0xff, 0x00]), "0xff00");
    }
}
