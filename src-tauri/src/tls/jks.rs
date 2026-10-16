//! Хранилища Java KeyStore (JKS). Только чтение: записи с ключом и доверенные сертификаты.

use openssl::pkey::{PKey, Private};
use openssl::sha::Sha1;
use openssl::x509::X509;

use super::der::read_tlv;
use crate::error::{AppError, AppResult, ErrorCode};

const JKS_MAGIC: [u8; 4] = [0xFE, 0xED, 0xFE, 0xED];
const JCEKS_MAGIC: [u8; 4] = [0xCE, 0xCE, 0xCE, 0xCE];
const WHITENER: &[u8] = b"Mighty Aphrodite";
/// 1.3.6.1.4.1.42.2.17.1.1 — Sun KeyProtector.
const KEY_PROTECTOR_OID: [u8; 10] = [0x2B, 0x06, 0x01, 0x04, 0x01, 0x2A, 0x02, 0x11, 0x01, 0x01];

pub fn is_jks(data: &[u8]) -> bool {
    data.starts_with(&JKS_MAGIC)
}

struct KeyEntry {
    alias: String,
    protected_key: Vec<u8>,
    chain: Vec<Vec<u8>>,
}

struct Store {
    keys: Vec<KeyEntry>,
    trusted: Vec<Vec<u8>>,
}

fn damaged() -> AppError {
    app_err!(
        ErrorCode::InvalidInput,
        "errors.tls.jksDamaged",
        "The JKS store is damaged or truncated"
    )
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> AppResult<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|e| *e <= self.data.len())
            .ok_or_else(damaged)?;
        let s = &self.data[self.pos..end];
        self.pos = end;
        Ok(s)
    }
    fn u16(&mut self) -> AppResult<u16> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> AppResult<u32> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    /// Java writeUTF: длина u16 + modified UTF-8 (для обычных alias совпадает с UTF-8).
    fn utf(&mut self) -> AppResult<String> {
        let n = self.u16()? as usize;
        Ok(String::from_utf8_lossy(self.take(n)?).into_owned())
    }
    fn blob(&mut self) -> AppResult<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
}

fn utf16be(s: &str) -> Vec<u8> {
    s.encode_utf16().flat_map(u16::to_be_bytes).collect()
}

fn parse(data: &[u8], store_password: Option<&str>) -> AppResult<Store> {
    if data.starts_with(&JCEKS_MAGIC) {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.tls.jceksUnsupported",
            "JCEKS stores are not supported — convert it to PKCS#12: \
             keytool -importkeystore -srcstoretype JCEKS -srckeystore <file> -deststoretype PKCS12 -destkeystore <file>.p12"
        ));
    }
    let mut r = Reader { data, pos: 0 };
    if r.take(4)? != JKS_MAGIC {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.tls.notJks",
            "The file is not a JKS store"
        ));
    }
    let version = r.u32()?;
    if version != 1 && version != 2 {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.tls.jksVersion",
            "Unsupported JKS version: {{version}}",
            version = version
        ));
    }
    let count = r.u32()?;
    let mut store = Store {
        keys: Vec::new(),
        trusted: Vec::new(),
    };
    for _ in 0..count {
        let tag = r.u32()?;
        let alias = r.utf()?;
        r.take(8)?; // время создания записи
        match tag {
            1 => {
                let protected_key = r.blob()?.to_vec();
                let n = r.u32()?;
                let mut chain = Vec::new();
                for _ in 0..n {
                    if version == 2 {
                        r.utf()?; // тип сертификата, «X.509»
                    }
                    chain.push(r.blob()?.to_vec());
                }
                store.keys.push(KeyEntry {
                    alias,
                    protected_key,
                    chain,
                });
            }
            2 => {
                if version == 2 {
                    r.utf()?;
                }
                store.trusted.push(r.blob()?.to_vec());
            }
            other => {
                return Err(app_err!(
                    ErrorCode::InvalidInput,
                    "errors.tls.jksEntryType",
                    "Unknown JKS entry type: {{tag}}",
                    tag = other
                ));
            }
        }
    }
    let body = r.pos;
    let digest = r.take(20)?;
    if let Some(pw) = store_password.filter(|p| !p.is_empty()) {
        let mut h = Sha1::new();
        h.update(&utf16be(pw));
        h.update(WHITENER);
        h.update(&data[..body]);
        if h.finish() != digest {
            return Err(app_err!(
                ErrorCode::InvalidInput,
                "errors.tls.jksBadStorePassword",
                "Wrong JKS store password (or the file is damaged)"
            ));
        }
    }
    Ok(store)
}

fn protected_data(info: &[u8]) -> AppResult<&[u8]> {
    let bad = jks_bad_key_entry;
    let (t, seq, _) = read_tlv(info).ok_or_else(bad)?;
    if t != 0x30 {
        return Err(bad());
    }
    let (t, alg, rest) = read_tlv(seq).ok_or_else(bad)?;
    if t != 0x30 {
        return Err(bad());
    }
    let (t, oid, _) = read_tlv(alg).ok_or_else(bad)?;
    if t != 0x06 || oid != KEY_PROTECTOR_OID {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.tls.jksKeyAlgorithm",
            "JKS store: the key is protected with an unsupported algorithm — convert the store to PKCS#12"
        ));
    }
    let (t, data, _) = read_tlv(rest).ok_or_else(bad)?;
    if t != 0x04 {
        return Err(bad());
    }
    Ok(data)
}

fn jks_bad_key_entry() -> AppError {
    app_err!(
        ErrorCode::InvalidInput,
        "errors.tls.jksBadKeyEntry",
        "JKS store: damaged key entry"
    )
}

/// Расшифровка Sun KeyProtector → PKCS#8 DER.
fn decrypt_key(info: &[u8], password: &str) -> AppResult<Vec<u8>> {
    let data = protected_data(info)?;
    if data.len() < 40 {
        return Err(jks_bad_key_entry());
    }
    let (salt, rest) = data.split_at(20);
    let (encrypted, check) = rest.split_at(rest.len() - 20);
    let pw = utf16be(password);
    let mut plain = Vec::with_capacity(encrypted.len());
    let mut digest = [0u8; 20];
    digest.copy_from_slice(salt);
    for chunk in encrypted.chunks(20) {
        let mut h = Sha1::new();
        h.update(&pw);
        h.update(&digest);
        digest = h.finish();
        plain.extend(chunk.iter().zip(digest.iter()).map(|(a, b)| a ^ b));
    }
    let mut h = Sha1::new();
    h.update(&pw);
    h.update(&plain);
    if h.finish() != check {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.tls.jksBadKeyPassword",
            "Wrong key password in the JKS store — specify the key password (if it differs from the store password)"
        ));
    }
    Ok(plain)
}

fn x509(der: &[u8]) -> AppResult<X509> {
    X509::from_der(der).map_err(|e| {
        app_err!(
            ErrorCode::InvalidInput,
            "errors.tls.jksBadCertificate",
            "JKS store: damaged certificate"
        )
        .with_details(e.to_string())
    })
}

pub fn trusted_certificates(data: &[u8], store_password: Option<&str>) -> AppResult<Vec<X509>> {
    let store = parse(data, store_password)?;
    if store.trusted.is_empty() {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.tls.jksNoTrusted",
            "The JKS store has no trusted certificates (trustedCertEntry entries)"
        ));
    }
    store.trusted.iter().map(|d| x509(d)).collect()
}

pub fn identity(
    data: &[u8],
    alias: Option<&str>,
    store_password: Option<&str>,
    key_password: Option<&str>,
) -> AppResult<(PKey<Private>, Vec<X509>)> {
    let store = parse(data, store_password)?;
    let names = || {
        store
            .keys
            .iter()
            .map(|k| k.alias.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    };
    let entry = match alias.map(str::trim).filter(|a| !a.is_empty()) {
        // keytool хранит alias в нижнем регистре.
        Some(a) => store
            .keys
            .iter()
            .find(|k| k.alias.eq_ignore_ascii_case(a))
            .ok_or_else(|| {
                app_err!(
                    ErrorCode::InvalidInput,
                    "errors.tls.jksAliasNotFound",
                    "The JKS store has no key “{{alias}}”. Available: {{available}}",
                    alias = a,
                    available = names()
                )
            })?,
        None => match store.keys.as_slice() {
            [] => {
                return Err(app_err!(
                    ErrorCode::InvalidInput,
                    "errors.tls.jksNoPrivateKey",
                    "The JKS store has no private key"
                ));
            }
            [one] => one,
            _ => {
                return Err(app_err!(
                    ErrorCode::InvalidInput,
                    "errors.tls.jksMultipleKeys",
                    "The JKS store has several keys — specify the alias: {{available}}",
                    available = names()
                ));
            }
        },
    };
    let pw = key_password
        .filter(|p| !p.is_empty())
        .or(store_password)
        .unwrap_or("");
    let pkcs8 = decrypt_key(&entry.protected_key, pw)?;
    let key = PKey::private_key_from_pkcs8(&pkcs8).map_err(|e| {
        app_err!(
            ErrorCode::InvalidInput,
            "errors.tls.jksKeyReadFailed",
            "JKS store: could not read the key"
        )
        .with_details(e.to_string())
    })?;
    let chain = entry
        .chain
        .iter()
        .map(|d| x509(d))
        .collect::<AppResult<Vec<_>>>()?;
    Ok((key, chain))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> Vec<u8> {
        std::fs::read(format!(
            "{}/../dev/certs/out/{name}",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap()
    }

    #[test]
    fn detects_magic() {
        assert!(is_jks(&fixture("client.jks")));
        assert!(!is_jks(&fixture("client.p12")));
        assert!(!is_jks(b"\xFE\xED"));
    }

    #[test]
    fn identity_with_store_password_as_key_password() {
        let (key, chain) = identity(&fixture("client.jks"), None, Some("storepass"), None).unwrap();
        assert!(chain[0].public_key().unwrap().public_eq(&key));
        assert!(!chain.is_empty());
    }

    #[test]
    fn separate_key_password() {
        let data = fixture("client-keypass.jks");
        let e = identity(&data, None, Some("storepass"), None).unwrap_err();
        assert_eq!(e.key, "errors.tls.jksBadKeyPassword", "{}", e.message);
        identity(&data, Some("CLIENT"), Some("storepass"), Some("keypass")).unwrap();
    }

    #[test]
    fn wrong_store_password_and_skipped_integrity_check() {
        let data = fixture("client.jks");
        let e = identity(&data, None, Some("nope"), Some("storepass")).unwrap_err();
        assert_eq!(e.key, "errors.tls.jksBadStorePassword", "{}", e.message);
        // Без пароля хранилища проверка целостности пропускается, ключ расшифровывается своим паролем.
        identity(&data, None, None, Some("storepass")).unwrap();
    }

    #[test]
    fn unknown_alias_lists_available() {
        let e = identity(
            &fixture("client.jks"),
            Some("other"),
            Some("storepass"),
            None,
        )
        .unwrap_err();
        assert!(e.message.contains("client"), "{}", e.message);
    }

    #[test]
    fn truststore_certificates() {
        let certs = trusted_certificates(&fixture("truststore.jks"), Some("storepass")).unwrap();
        assert_eq!(certs.len(), 1);
        let e = trusted_certificates(&fixture("client.jks"), Some("storepass")).unwrap_err();
        assert_eq!(e.key, "errors.tls.jksNoTrusted", "{}", e.message);
    }

    #[test]
    fn jceks_and_garbage_are_rejected_with_reason() {
        let e = identity(b"\xCE\xCE\xCE\xCE\0\0\0\x02\0\0\0\0", None, None, None).unwrap_err();
        assert_eq!(e.key, "errors.tls.jceksUnsupported", "{}", e.message);
        let mut truncated = fixture("client.jks");
        truncated.truncate(40);
        let e = identity(&truncated, None, None, Some("storepass")).unwrap_err();
        assert_eq!(e.key, "errors.tls.jksDamaged", "{}", e.message);
    }

    #[test]
    fn via_load_identity_and_load_ca() {
        use crate::storage::connections::{CertFormat, ClientCert};
        let path = format!("{}/../dev/certs/out/client.jks", env!("CARGO_MANIFEST_DIR"));
        let cc = ClientCert {
            format: CertFormat::Jks,
            certificate_path: path,
            key_path: None,
            alias: None,
        };
        crate::tls::load_identity(&cc, None, Some("storepass")).unwrap();
        let ts = format!(
            "{}/../dev/certs/out/truststore.jks",
            env!("CARGO_MANIFEST_DIR")
        );
        assert!(
            crate::tls::load_ca(CertFormat::Jks, &ts, Some("storepass"))
                .unwrap()
                .contains("BEGIN CERTIFICATE")
        );
    }
}
