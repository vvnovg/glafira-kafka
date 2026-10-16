//! Формат лицензионного ключа: base64url(JSON) "." base64url(подпись Ed25519).
//! Общий для приложения (проверка) и утилиты issue-license (выдача).

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
pub use chrono::NaiveDate;
use ed25519_dalek::{Signature, Signer, Verifier};
pub use ed25519_dalek::{SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};

pub const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Pro,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Personal,
    Organization,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Payload {
    pub v: u32,
    pub id: String,
    pub tier: Tier,
    pub kind: Kind,
    pub licensee: String,
    pub issued: NaiveDate,
    pub expires: Option<NaiveDate>,
    pub seats: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LicenseError {
    Malformed(String),
    BadSignature,
    UnsupportedVersion(u32),
}

impl std::fmt::Display for LicenseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LicenseError::Malformed(m) => write!(f, "ключ повреждён ({m})"),
            LicenseError::BadSignature => write!(f, "подпись ключа не подтверждена — ключ выдан не нами или изменён"),
            LicenseError::UnsupportedVersion(v) => {
                write!(f, "ключ версии {v} требует более новой версии приложения")
            }
        }
    }
}

impl std::error::Error for LicenseError {}

fn malformed(m: &str) -> LicenseError {
    LicenseError::Malformed(m.to_string())
}

pub fn sign(payload: &Payload, key: &SigningKey) -> String {
    let json = serde_json::to_vec(payload).expect("Payload сериализуется");
    let sig = key.sign(&json);
    format!("{}.{}", B64.encode(&json), B64.encode(sig.to_bytes()))
}

pub fn verify(token: &str, key: &VerifyingKey) -> Result<Payload, LicenseError> {
    let (p, s) = token.trim().split_once('.').ok_or_else(|| malformed("нет разделителя «.»"))?;
    let json = B64.decode(p).map_err(|_| malformed("данные не в base64url"))?;
    let sig = B64.decode(s).map_err(|_| malformed("подпись не в base64url"))?;
    let sig = Signature::from_slice(&sig).map_err(|_| malformed("подпись неверной длины"))?;
    key.verify(&json, &sig).map_err(|_| LicenseError::BadSignature)?;
    let value: serde_json::Value = serde_json::from_slice(&json).map_err(|_| malformed("данные не JSON"))?;
    let v = value.get("v").and_then(serde_json::Value::as_u64).ok_or_else(|| malformed("нет версии"))?;
    if v != u64::from(FORMAT_VERSION) {
        return Err(LicenseError::UnsupportedVersion(v as u32));
    }
    serde_json::from_value(value).map_err(|e| LicenseError::Malformed(e.to_string()))
}

fn key_bytes(s: &str) -> Result<[u8; 32], LicenseError> {
    B64.decode(s.trim())
        .ok()
        .and_then(|b| <[u8; 32]>::try_from(b).ok())
        .ok_or_else(|| malformed("ключ должен быть base64url от 32 байт"))
}

pub fn encode_verifying_key(k: &VerifyingKey) -> String {
    B64.encode(k.as_bytes())
}

pub fn parse_verifying_key(s: &str) -> Result<VerifyingKey, LicenseError> {
    VerifyingKey::from_bytes(&key_bytes(s)?).map_err(|_| malformed("неверный открытый ключ"))
}

pub fn encode_signing_key(k: &SigningKey) -> String {
    B64.encode(k.to_bytes())
}

pub fn parse_signing_key(s: &str) -> Result<SigningKey, LicenseError> {
    Ok(SigningKey::from_bytes(&key_bytes(s)?))
}

/// Ключи разработки: подписывают тестовые лицензии. В сборку приложения не попадают
/// (feature включается только в dev-dependencies).
#[cfg(feature = "dev-keys")]
pub mod dev {
    use super::*;

    const SIGNING: &str = include_str!("../testdata/dev-signing.key");
    const PUBLIC: &str = include_str!("../testdata/dev-public.key");

    pub fn signing_key() -> SigningKey {
        parse_signing_key(SIGNING).expect("dev-signing.key")
    }

    pub fn verifying_key() -> VerifyingKey {
        parse_verifying_key(PUBLIC).expect("dev-public.key")
    }

    pub fn pro_token(licensee: &str, expires: Option<NaiveDate>) -> String {
        sign(
            &Payload {
                v: FORMAT_VERSION,
                id: "dev".into(),
                tier: Tier::Pro,
                kind: Kind::Personal,
                licensee: licensee.into(),
                issued: NaiveDate::from_ymd_opt(2026, 1, 1).expect("дата"),
                expires,
                seats: None,
            },
            &signing_key(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn payload() -> Payload {
        Payload {
            v: FORMAT_VERSION,
            id: "lic-1".into(),
            tier: Tier::Pro,
            kind: Kind::Organization,
            licensee: "ACME".into(),
            issued: NaiveDate::from_ymd_opt(2026, 10, 1).unwrap(),
            expires: NaiveDate::from_ymd_opt(2027, 3, 31),
            seats: Some(500),
        }
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let k = key(1);
        let token = sign(&payload(), &k);
        assert_eq!(token.matches('.').count(), 1);
        assert!(!token.contains('='));
        assert_eq!(verify(&token, &k.verifying_key()).unwrap(), payload());
        assert_eq!(verify(&format!("  {token}\n"), &k.verifying_key()).unwrap(), payload());
    }

    #[test]
    fn foreign_key_and_tampering_fail() {
        let token = sign(&payload(), &key(1));
        assert_eq!(verify(&token, &key(2).verifying_key()), Err(LicenseError::BadSignature));
        let (p, s) = token.split_once('.').unwrap();
        let mut json = String::from_utf8(B64.decode(p).unwrap()).unwrap();
        json = json.replace("ACME", "EVIL");
        let forged = format!("{}.{s}", B64.encode(json));
        assert_eq!(verify(&forged, &key(1).verifying_key()), Err(LicenseError::BadSignature));
    }

    #[test]
    fn malformed_tokens() {
        let vk = key(1).verifying_key();
        for t in ["", "nodot", "@@@.@@@", "e30.AAAA"] {
            assert!(matches!(verify(t, &vk), Err(LicenseError::Malformed(_))), "{t}");
        }
    }

    #[test]
    fn newer_format_version_is_reported() {
        let k = key(1);
        let json = br#"{"v":2,"id":"x"}"#;
        let token = format!("{}.{}", B64.encode(json), B64.encode(k.sign(json).to_bytes()));
        assert_eq!(verify(&token, &k.verifying_key()), Err(LicenseError::UnsupportedVersion(2)));
    }

    #[test]
    fn keys_roundtrip_through_text() {
        let k = key(7);
        let sk = parse_signing_key(&format!("{}\n", encode_signing_key(&k))).unwrap();
        assert_eq!(sk.to_bytes(), k.to_bytes());
        let vk = parse_verifying_key(&encode_verifying_key(&k.verifying_key())).unwrap();
        assert_eq!(vk, k.verifying_key());
        assert!(parse_verifying_key("short").is_err());
    }

    #[test]
    fn payload_json_shape() {
        let v = serde_json::to_value(payload()).unwrap();
        assert_eq!(v["tier"], "pro");
        assert_eq!(v["kind"], "organization");
        assert_eq!(v["expires"], "2027-03-31");
    }
}
