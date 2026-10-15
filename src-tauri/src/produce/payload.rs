use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PayloadFormat {
    String,
    Json,
    Hex,
    Base64,
    Null,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PayloadInput {
    pub format: PayloadFormat,
    #[serde(default)]
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HeaderFormat {
    #[default]
    String,
    Hex,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeaderInput {
    pub name: String,
    pub value: String,
    #[serde(default)]
    pub format: HeaderFormat,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProduceMessage {
    pub topic: String,
    /// None — партицию выбирает партиционер librdkafka.
    #[serde(default)]
    pub partition: Option<i32>,
    pub key: PayloadInput,
    pub value: PayloadInput,
    #[serde(default)]
    pub headers: Vec<HeaderInput>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EncodedMessage {
    pub topic: String,
    pub partition: Option<i32>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
}

fn decode_hex(text: &str) -> Result<Vec<u8>, String> {
    let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    hex::decode(&compact).map_err(|e| e.to_string())
}

pub fn encode_payload(p: &PayloadInput, what: &str) -> AppResult<Option<Vec<u8>>> {
    let bad = |e: String| AppError::invalid(format!("Не удалось закодировать {what}: {e}"));
    Ok(match p.format {
        PayloadFormat::Null => None,
        PayloadFormat::String => Some(p.text.as_bytes().to_vec()),
        PayloadFormat::Json => {
            serde_json::from_str::<serde::de::IgnoredAny>(&p.text)
                .map_err(|e| bad(format!("невалидный JSON: {e}")))?;
            Some(p.text.as_bytes().to_vec())
        }
        PayloadFormat::Hex => {
            Some(decode_hex(&p.text).map_err(|e| bad(format!("невалидный hex: {e}")))?)
        }
        PayloadFormat::Base64 => Some(
            base64::engine::general_purpose::STANDARD
                .decode(p.text.trim())
                .map_err(|e| bad(format!("невалидный base64: {e}")))?,
        ),
    })
}

pub fn encode_message(m: &ProduceMessage) -> AppResult<EncodedMessage> {
    if m.topic.trim().is_empty() {
        return Err(AppError::invalid("Не указан топик"));
    }
    if let Some(p) = m.partition
        && p < 0
    {
        return Err(AppError::invalid(
            "Номер партиции не может быть отрицательным",
        ));
    }
    let mut headers = Vec::with_capacity(m.headers.len());
    for h in &m.headers {
        if h.name.trim().is_empty() {
            return Err(AppError::invalid("Пустое имя заголовка"));
        }
        let value = match h.format {
            HeaderFormat::String => h.value.as_bytes().to_vec(),
            HeaderFormat::Hex => decode_hex(&h.value).map_err(|e| {
                AppError::invalid(format!("Заголовок «{}»: невалидный hex: {e}", h.name))
            })?,
        };
        headers.push((h.name.clone(), value));
    }
    Ok(EncodedMessage {
        topic: m.topic.trim().to_string(),
        partition: m.partition,
        key: encode_payload(&m.key, "ключ")?,
        value: encode_payload(&m.value, "значение")?,
        headers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    fn p(format: PayloadFormat, text: &str) -> PayloadInput {
        PayloadInput {
            format,
            text: text.into(),
        }
    }

    #[test]
    fn encodes_each_format() {
        assert_eq!(
            encode_payload(&p(PayloadFormat::String, "привет"), "значение").unwrap(),
            Some("привет".as_bytes().to_vec())
        );
        assert_eq!(
            encode_payload(&p(PayloadFormat::Json, " {\"a\": 1} "), "значение").unwrap(),
            Some(b" {\"a\": 1} ".to_vec())
        );
        assert_eq!(
            encode_payload(&p(PayloadFormat::Hex, "de ad\nBE EF"), "ключ").unwrap(),
            Some(vec![0xde, 0xad, 0xbe, 0xef])
        );
        assert_eq!(
            encode_payload(&p(PayloadFormat::Base64, " QUI= "), "ключ").unwrap(),
            Some(b"AB".to_vec())
        );
        assert_eq!(
            encode_payload(&p(PayloadFormat::Null, "ignored"), "ключ").unwrap(),
            None
        );
        assert_eq!(
            encode_payload(&p(PayloadFormat::String, ""), "ключ").unwrap(),
            Some(vec![])
        );
    }

    #[test]
    fn invalid_payloads_are_invalid_input() {
        for (f, t) in [
            (PayloadFormat::Json, "{oops"),
            (PayloadFormat::Hex, "abc"),
            (PayloadFormat::Hex, "zz"),
            (PayloadFormat::Base64, "%%%"),
        ] {
            let e = encode_payload(&p(f, t), "значение").unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput, "{f:?} {t}");
            assert!(e.message.contains("значение"), "{}", e.message);
        }
    }

    fn msg() -> ProduceMessage {
        ProduceMessage {
            topic: "t".into(),
            partition: None,
            key: p(PayloadFormat::String, "k"),
            value: p(PayloadFormat::Json, "{}"),
            headers: vec![
                HeaderInput {
                    name: "trace".into(),
                    value: "abc".into(),
                    format: HeaderFormat::String,
                },
                HeaderInput {
                    name: "bin".into(),
                    value: "00ff".into(),
                    format: HeaderFormat::Hex,
                },
            ],
        }
    }

    #[test]
    fn encodes_message_with_headers() {
        let e = encode_message(&msg()).unwrap();
        assert_eq!(e.key.as_deref(), Some(&b"k"[..]));
        assert_eq!(
            e.headers,
            vec![
                ("trace".to_string(), b"abc".to_vec()),
                ("bin".to_string(), vec![0x00, 0xff])
            ]
        );
    }

    #[test]
    fn message_validation() {
        let mut m = msg();
        m.topic = " ".into();
        assert_eq!(
            encode_message(&m).unwrap_err().code,
            ErrorCode::InvalidInput
        );
        let mut m = msg();
        m.partition = Some(-1);
        assert_eq!(
            encode_message(&m).unwrap_err().code,
            ErrorCode::InvalidInput
        );
        let mut m = msg();
        m.headers[0].name = "".into();
        assert_eq!(
            encode_message(&m).unwrap_err().code,
            ErrorCode::InvalidInput
        );
    }

    #[test]
    fn json_shape() {
        let m: ProduceMessage = serde_json::from_value(serde_json::json!({
            "topic": "t",
            "key": {"format": "null"},
            "value": {"format": "string", "text": "v"},
            "headers": [{"name": "h", "value": "1"}]
        }))
        .unwrap();
        assert_eq!(m.partition, None);
        assert_eq!(m.key.format, PayloadFormat::Null);
        assert_eq!(m.headers[0].format, HeaderFormat::String);
        assert_eq!(
            serde_json::to_value(PayloadFormat::Base64).unwrap(),
            "base64"
        );
    }
}
