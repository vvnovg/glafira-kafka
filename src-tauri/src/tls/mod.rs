//! Сертификаты TLS → PEM в памяти для librdkafka (`ssl.ca.pem`, `ssl.certificate.pem`, `ssl.key.pem`).
//! Файлы читаются при каждом создании клиента, чтобы подхватывать ротацию; временных файлов нет.

mod der;
pub mod jks;

use std::cmp::Ordering;
use std::path::Path;
use std::sync::OnceLock;

use openssl::asn1::Asn1Time;
use openssl::error::ErrorStack;
use openssl::pkcs12::{ParsedPkcs12_2, Pkcs12};
use openssl::pkey::{PKey, Private};
use openssl::provider::Provider;
use openssl::x509::X509;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::storage::connections::{CertFormat, ClientCert};

pub struct ClientIdentity {
    pub certificate_pem: String,
    pub key_pem: String,
}

impl std::fmt::Debug for ClientIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientIdentity")
            .field("key_pem", &"<hidden>")
            .finish_non_exhaustive()
    }
}

/// Какой файл TLS читается: от этого зависит начало текста ошибки («Доверенный CA: …»).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Part {
    /// Клиентский сертификат (или хранилище с ним).
    Client,
    /// Ключ клиента (PEM).
    Key,
    /// Доверенный CA.
    Ca,
}

fn invalid(text: crate::i18n::LocalizedText) -> AppError {
    AppError::localized(ErrorCode::InvalidInput, text)
}

fn read_file(path: &str, part: Part) -> AppResult<Vec<u8>> {
    let p = Path::new(path.trim());
    if !p.is_absolute() {
        return Err(invalid(match part {
            Part::Ca => ltext!(
                "errors.tls.caAbsolutePath",
                "Trusted CA: specify an absolute path (currently “{{path}}”)",
                path = path
            ),
            Part::Client => ltext!(
                "errors.tls.clientAbsolutePath",
                "Client certificate: specify an absolute path (currently “{{path}}”)",
                path = path
            ),
            Part::Key => ltext!(
                "errors.tls.keyAbsolutePath",
                "Client key: specify an absolute path (currently “{{path}}”)",
                path = path
            ),
        }));
    }
    std::fs::read(p).map_err(|e| {
        invalid(match part {
            Part::Ca => ltext!(
                "errors.tls.caReadFailed",
                "Trusted CA: could not read “{{path}}”: {{error}}",
                path = path,
                error = e
            ),
            Part::Client => ltext!(
                "errors.tls.clientReadFailed",
                "Client certificate: could not read “{{path}}”: {{error}}",
                path = path,
                error = e
            ),
            Part::Key => ltext!(
                "errors.tls.keyReadFailed",
                "Client key: could not read “{{path}}”: {{error}}",
                path = path,
                error = e
            ),
        })
    })
}

/// Ошибка с текстом для интерфейса и исходным текстом OpenSSL в `details`.
fn ssl_err(text: crate::i18n::LocalizedText, e: ErrorStack) -> AppError {
    invalid(text).with_details(e.to_string())
}

fn non_empty(p: Option<&str>) -> Option<&str> {
    p.filter(|s| !s.is_empty())
}

/// Сертификаты из PEM (цепочка) или, если PEM-блоков нет, один сертификат DER (частый случай `.cer`).
fn pem_or_der_certs(data: &[u8], path: &str, part: Part) -> AppResult<Vec<X509>> {
    let pem_err = match X509::stack_from_pem(data) {
        Ok(certs) if !certs.is_empty() => return Ok(certs),
        Ok(_) => None,
        Err(e) => Some(e),
    };
    X509::from_der(data).map(|c| vec![c]).map_err(|der_err| {
        let details = match pem_err {
            Some(p) => format!("PEM: {p}; DER: {der_err}"),
            None => format!("DER: {der_err}"),
        };
        invalid(match part {
            Part::Ca => ltext!(
                "errors.tls.caNoPemOrDer",
                "Trusted CA: “{{path}}” contains no PEM or DER certificates",
                path = path
            ),
            Part::Client | Part::Key => ltext!(
                "errors.tls.clientNoPemOrDer",
                "Client certificate: “{{path}}” contains no PEM or DER certificates",
                path = path
            ),
        })
        .with_details(details)
    })
}

/// Доверенный CA в PEM (цепочка сертификатов).
pub fn load_ca(format: CertFormat, path: &str, password: Option<&str>) -> AppResult<String> {
    let data = read_file(path, Part::Ca)?;
    let certs: Vec<X509> = match format {
        CertFormat::Pem => pem_or_der_certs(&data, path, Part::Ca)?,
        CertFormat::Jks if jks::is_jks(&data) => jks::trusted_certificates(&data, password)?,
        // Начиная с Java 9 keytool по умолчанию создаёт PKCS#12, даже если файл назван .jks.
        CertFormat::Pkcs12 | CertFormat::Jks => {
            let p = parse_pkcs12(&data, password, Part::Ca)?;
            p.cert
                .into_iter()
                .chain(p.ca.into_iter().flatten())
                .collect()
        }
    };
    if certs.is_empty() {
        return Err(invalid(ltext!(
            "errors.tls.caNoCertificates",
            "Trusted CA: “{{path}}” contains no certificates",
            path = path
        )));
    }
    to_pem_chain(&certs)
}

/// Клиентский сертификат и ключ в PEM. Проверяет пароль, соответствие ключа сертификату и срок действия.
pub fn load_identity(
    cert: &ClientCert,
    key_password: Option<&str>,
    store_password: Option<&str>,
) -> AppResult<ClientIdentity> {
    let (key, chain) = match cert.format {
        CertFormat::Pem => {
            let chain = pem_or_der_certs(
                &read_file(&cert.certificate_path, Part::Client)?,
                &cert.certificate_path,
                Part::Client,
            )?;
            let key_path = cert
                .key_path
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| {
                    invalid(ltext!(
                        "errors.tls.keyFileMissing",
                        "Client key: specify the key file"
                    ))
                })?;
            (
                load_pem_key(&read_file(key_path, Part::Key)?, key_password)?,
                chain,
            )
        }
        CertFormat::Jks => {
            let data = read_file(&cert.certificate_path, Part::Client)?;
            if jks::is_jks(&data) {
                jks::identity(&data, cert.alias.as_deref(), store_password, key_password)?
            } else {
                pkcs12_identity(&data, store_password)?
            }
        }
        CertFormat::Pkcs12 => pkcs12_identity(
            &read_file(&cert.certificate_path, Part::Client)?,
            store_password,
        )?,
    };
    finish_identity(key, chain)
}

fn key_not_pem() -> crate::i18n::LocalizedText {
    ltext!(
        "errors.tls.keyNotPem",
        "Client key: the file does not contain a PEM key"
    )
}

fn load_pem_key(data: &[u8], password: Option<&str>) -> AppResult<PKey<Private>> {
    let encrypted = String::from_utf8_lossy(data).contains("ENCRYPTED");
    match non_empty(password) {
        None if encrypted => Err(invalid(ltext!(
            "errors.tls.keyEncrypted",
            "Client key: the key is encrypted — specify the key password"
        ))),
        None => PKey::private_key_from_pem(data).map_err(|e| ssl_err(key_not_pem(), e)),
        Some(pw) => PKey::private_key_from_pem_passphrase(data, pw.as_bytes()).map_err(|e| {
            let text = if encrypted {
                ltext!(
                    "errors.tls.keyBadPassword",
                    "Client key: wrong key password"
                )
            } else {
                key_not_pem()
            };
            ssl_err(text, e)
        }),
    }
}

fn pkcs12_identity(data: &[u8], password: Option<&str>) -> AppResult<(PKey<Private>, Vec<X509>)> {
    let p = parse_pkcs12(data, password, Part::Client)?;
    let key = p.pkey.ok_or_else(|| {
        invalid(ltext!(
            "errors.tls.clientNoPrivateKey",
            "Client certificate: the store has no private key"
        ))
    })?;
    let cert = p.cert.ok_or_else(|| {
        invalid(ltext!(
            "errors.tls.clientNoCertificate",
            "Client certificate: the store has no certificate"
        ))
    })?;
    Ok((
        key,
        std::iter::once(cert)
            .chain(p.ca.into_iter().flatten())
            .collect(),
    ))
}

static LEGACY: OnceLock<Option<Provider>> = OnceLock::new();

/// Старые PKCS#12 (RC2/3DES, например созданные LibreSSL или Java 8) требуют провайдер legacy OpenSSL 3.
fn load_legacy_provider() -> bool {
    LEGACY
        .get_or_init(|| Provider::try_load(None, "legacy", true).ok())
        .is_some()
}

fn is_unsupported(e: &ErrorStack) -> bool {
    let t = e.to_string().to_lowercase();
    t.contains("unsupported") || t.contains("rc2") || t.contains("fetch")
}

fn parse_pkcs12(data: &[u8], password: Option<&str>, part: Part) -> AppResult<ParsedPkcs12_2> {
    let p12 = Pkcs12::from_der(data).map_err(|e| {
        let text = match part {
            Part::Ca => ltext!(
                "errors.tls.caNotPkcs12",
                "Trusted CA: the file is not a PKCS#12 store"
            ),
            Part::Client | Part::Key => ltext!(
                "errors.tls.clientNotPkcs12",
                "Client certificate: the file is not a PKCS#12 store"
            ),
        };
        ssl_err(text, e)
    })?;
    let pw = password.unwrap_or("");
    match p12.parse2(pw) {
        Ok(p) => Ok(p),
        Err(e) if is_unsupported(&e) && load_legacy_provider() => {
            p12.parse2(pw).map_err(|e| pkcs12_err(part, e))
        }
        Err(e) => Err(pkcs12_err(part, e)),
    }
}

fn pkcs12_err(part: Part, e: ErrorStack) -> AppError {
    let t = e.to_string().to_lowercase();
    let ca = part == Part::Ca;
    let text = if t.contains("mac verify") || t.contains("bad decrypt") {
        if ca {
            ltext!(
                "errors.tls.caPkcs12BadPassword",
                "Trusted CA: wrong PKCS#12 store password"
            )
        } else {
            ltext!(
                "errors.tls.clientPkcs12BadPassword",
                "Client certificate: wrong PKCS#12 store password"
            )
        }
    } else if is_unsupported(&e) {
        if ca {
            ltext!(
                "errors.tls.caPkcs12Legacy",
                "Trusted CA: the PKCS#12 store is encrypted with a legacy algorithm (RC2/3DES) that is not available. \
                 Recreate it with AES: openssl pkcs12 -export -keypbe AES-256-CBC -certpbe AES-256-CBC …"
            )
        } else {
            ltext!(
                "errors.tls.clientPkcs12Legacy",
                "Client certificate: the PKCS#12 store is encrypted with a legacy algorithm (RC2/3DES) that is not available. \
                 Recreate it with AES: openssl pkcs12 -export -keypbe AES-256-CBC -certpbe AES-256-CBC …"
            )
        }
    } else if ca {
        ltext!(
            "errors.tls.caPkcs12ReadFailed",
            "Trusted CA: could not read the PKCS#12 store"
        )
    } else {
        ltext!(
            "errors.tls.clientPkcs12ReadFailed",
            "Client certificate: could not read the PKCS#12 store"
        )
    };
    ssl_err(text, e)
}

fn finish_identity(key: PKey<Private>, chain: Vec<X509>) -> AppResult<ClientIdentity> {
    let leaf = chain.first().ok_or_else(|| {
        invalid(ltext!(
            "errors.tls.clientNoCertificates",
            "Client certificate: the file contains no certificates"
        ))
    })?;
    let public = leaf.public_key().map_err(|e| {
        ssl_err(
            ltext!(
                "errors.tls.clientPublicKey",
                "Client certificate: could not read the public key"
            ),
            e,
        )
    })?;
    if !public.public_eq(&key) {
        return Err(invalid(ltext!(
            "errors.tls.keyMismatch",
            "The client key does not match the client certificate"
        )));
    }
    let now = Asn1Time::days_from_now(0).map_err(|e| {
        ssl_err(
            ltext!("errors.tls.currentTime", "Could not get the current time"),
            e,
        )
    })?;
    // Ошибка сравнения — ошибка, а не «действителен».
    let cmp = |t: &openssl::asn1::Asn1TimeRef| {
        t.compare(&now).map_err(|e| {
            ssl_err(
                ltext!(
                    "errors.tls.clientValidityCheck",
                    "Client certificate: could not check the validity period"
                ),
                e,
            )
        })
    };
    if cmp(leaf.not_before())? == Ordering::Greater {
        return Err(invalid(ltext!(
            "errors.tls.clientNotYetValid",
            "The client certificate is not valid yet (valid from {{date}})",
            date = leaf.not_before()
        )));
    }
    if cmp(leaf.not_after())? == Ordering::Less {
        return Err(invalid(ltext!(
            "errors.tls.clientExpired",
            "The client certificate expired on {{date}}",
            date = leaf.not_after()
        )));
    }
    let key_pem = key.private_key_to_pem_pkcs8().map_err(|e| {
        ssl_err(
            ltext!(
                "errors.tls.keyToPem",
                "Client key: could not convert to PEM"
            ),
            e,
        )
    })?;
    Ok(ClientIdentity {
        certificate_pem: to_pem_chain(&chain)?,
        key_pem: String::from_utf8(key_pem).map_err(|_| {
            app_err!(
                ErrorCode::Internal,
                "errors.tls.pemNotUtf8",
                "PEM is not UTF-8"
            )
        })?,
    })
}

fn to_pem_chain(certs: &[X509]) -> AppResult<String> {
    let mut out = String::new();
    for c in certs {
        let pem = c.to_pem().map_err(|e| {
            ssl_err(
                ltext!(
                    "errors.tls.certToPem",
                    "Could not convert the certificate to PEM"
                ),
                e,
            )
        })?;
        out.push_str(&String::from_utf8_lossy(&pem));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openssl::asn1::Asn1Time;
    use openssl::ec::{EcGroup, EcKey};
    use openssl::hash::MessageDigest;
    use openssl::nid::Nid;
    use openssl::pkcs12::Pkcs12;
    use openssl::x509::{X509Builder, X509NameBuilder};

    fn fixture(name: &str) -> String {
        format!("{}/../dev/certs/out/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    fn pem_cert(format: CertFormat, cert: &str, key: Option<&str>) -> ClientCert {
        ClientCert {
            format,
            certificate_path: fixture(cert),
            key_path: key.map(fixture),
            alias: None,
        }
    }

    /// Сертификат из фикстуры, сохранённый в DER (типичный `.cer`).
    fn der_copy(dir: &tempfile::TempDir, name: &str) -> String {
        let pem = std::fs::read(fixture(name)).unwrap();
        let der = X509::from_pem(&pem).unwrap().to_der().unwrap();
        let path = dir.path().join(format!("{name}.cer"));
        std::fs::write(&path, der).unwrap();
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn der_certificates_are_accepted_in_pem_format() {
        let dir = tempfile::tempdir().unwrap();
        let ca = load_ca(CertFormat::Pem, &der_copy(&dir, "ca.pem"), None).unwrap();
        assert!(ca.contains("BEGIN CERTIFICATE"));
        let cert = ClientCert {
            format: CertFormat::Pem,
            certificate_path: der_copy(&dir, "client.pem"),
            key_path: Some(fixture("client.key")),
            alias: None,
        };
        let id = load_identity(&cert, None, None).unwrap();
        assert!(id.certificate_pem.contains("BEGIN CERTIFICATE"));
    }

    #[test]
    fn garbage_certificate_error_mentions_pem_and_der() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.cer");
        std::fs::write(&path, b"not a certificate").unwrap();
        let e = load_ca(CertFormat::Pem, &path.to_string_lossy(), None).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert!(
            e.message.contains("PEM") && e.message.contains("DER"),
            "{}",
            e.message
        );
    }

    /// Самоподписанный сертификат со сроком [now+from_days, now+to_days].
    fn self_signed(from_days: i64, to_days: i64) -> (PKey<Private>, X509) {
        let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
        let key = PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap();
        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_text("CN", "t").unwrap();
        let name = name.build();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let mut b = X509Builder::new().unwrap();
        b.set_version(2).unwrap();
        b.set_subject_name(&name).unwrap();
        b.set_issuer_name(&name).unwrap();
        b.set_pubkey(&key).unwrap();
        b.set_not_before(&Asn1Time::from_unix(now + from_days * 86_400).unwrap())
            .unwrap();
        b.set_not_after(&Asn1Time::from_unix(now + to_days * 86_400).unwrap())
            .unwrap();
        b.sign(&key, MessageDigest::sha256()).unwrap();
        (key, b.build())
    }

    fn write_pair(dir: &std::path::Path, key: &PKey<Private>, cert: &X509) -> ClientCert {
        let c = dir.join("c.pem");
        let k = dir.join("k.pem");
        std::fs::write(&c, cert.to_pem().unwrap()).unwrap();
        std::fs::write(&k, key.private_key_to_pem_pkcs8().unwrap()).unwrap();
        ClientCert {
            format: CertFormat::Pem,
            certificate_path: c.to_string_lossy().into(),
            key_path: Some(k.to_string_lossy().into()),
            alias: None,
        }
    }

    #[test]
    fn pem_identity_without_password() {
        let id = load_identity(
            &pem_cert(CertFormat::Pem, "client.pem", Some("client.key")),
            None,
            None,
        )
        .unwrap();
        assert!(id.certificate_pem.contains("BEGIN CERTIFICATE"));
        assert!(id.key_pem.contains("BEGIN PRIVATE KEY"));
    }

    #[test]
    fn encrypted_pem_key_needs_password() {
        let cc = pem_cert(CertFormat::Pem, "client.pem", Some("client-enc.key"));
        let e = load_identity(&cc, None, None).unwrap_err();
        assert_eq!(e.key, "errors.tls.keyEncrypted", "{}", e.message);
        let e = load_identity(&cc, Some("wrong"), None).unwrap_err();
        assert_eq!(e.key, "errors.tls.keyBadPassword", "{}", e.message);
        let id = load_identity(&cc, Some("keypass"), None).unwrap();
        assert!(
            !id.key_pem.contains("ENCRYPTED"),
            "в librdkafka уходит расшифрованный ключ"
        );
    }

    #[test]
    fn pkcs12_identity_with_and_without_password() {
        let id = load_identity(
            &pem_cert(CertFormat::Pkcs12, "client.p12", None),
            None,
            Some("storepass"),
        )
        .unwrap();
        assert!(id.certificate_pem.contains("BEGIN CERTIFICATE"));
        let e = load_identity(
            &pem_cert(CertFormat::Pkcs12, "client.p12", None),
            None,
            Some("nope"),
        )
        .unwrap_err();
        assert_eq!(e.key, "errors.tls.clientPkcs12BadPassword", "{}", e.message);
        load_identity(
            &pem_cert(CertFormat::Pkcs12, "client-nopass.p12", None),
            None,
            None,
        )
        .unwrap();
    }

    #[test]
    fn legacy_pkcs12_is_loaded_or_clearly_rejected() {
        let cc = pem_cert(CertFormat::Pkcs12, "client-legacy.p12", None);
        match load_identity(&cc, None, Some("storepass")) {
            Ok(id) => assert!(id.certificate_pem.contains("BEGIN CERTIFICATE")),
            Err(e) => assert_eq!(e.key, "errors.tls.clientPkcs12Legacy", "{}", e.message),
        }
    }

    #[test]
    fn modern_aes_pkcs12_is_supported() {
        let dir = tempfile::tempdir().unwrap();
        let (key, cert) = self_signed(-1, 30);
        let p12 = Pkcs12::builder()
            .name("x")
            .pkey(&key)
            .cert(&cert)
            .build2("pw")
            .unwrap();
        let path = dir.path().join("a.p12");
        std::fs::write(&path, p12.to_der().unwrap()).unwrap();
        let cc = ClientCert {
            format: CertFormat::Pkcs12,
            certificate_path: path.to_string_lossy().into(),
            key_path: None,
            alias: None,
        };
        load_identity(&cc, None, Some("pw")).unwrap();
    }

    #[test]
    fn key_must_match_certificate() {
        let dir = tempfile::tempdir().unwrap();
        let (_, cert) = self_signed(-1, 30);
        let (other_key, _) = self_signed(-1, 30);
        let cc = write_pair(dir.path(), &other_key, &cert);
        let e = load_identity(&cc, None, None).unwrap_err();
        assert_eq!(e.key, "errors.tls.keyMismatch", "{}", e.message);
    }

    #[test]
    fn expired_and_not_yet_valid_certificates() {
        let dir = tempfile::tempdir().unwrap();
        let (key, cert) = self_signed(-30, -1);
        let e = load_identity(&write_pair(dir.path(), &key, &cert), None, None).unwrap_err();
        assert_eq!(e.key, "errors.tls.clientExpired", "{}", e.message);
        let (key, cert) = self_signed(2, 30);
        let e = load_identity(&write_pair(dir.path(), &key, &cert), None, None).unwrap_err();
        assert_eq!(e.key, "errors.tls.clientNotYetValid", "{}", e.message);
    }

    #[test]
    fn missing_file_names_the_path() {
        let cc = pem_cert(CertFormat::Pem, "nope.pem", Some("client.key"));
        let e = load_identity(&cc, None, None).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert!(e.message.contains("nope.pem"), "{}", e.message);
    }

    #[test]
    fn ca_from_pem_pkcs12_and_jks_truststores() {
        for (format, file, pw) in [
            (CertFormat::Pem, "ca.pem", None),
            (CertFormat::Pkcs12, "truststore.p12", Some("storepass")),
        ] {
            let pem = load_ca(format, &fixture(file), pw).unwrap();
            assert!(pem.contains("BEGIN CERTIFICATE"), "{file}");
        }
        let e = load_ca(CertFormat::Pem, &fixture("client.key"), None).unwrap_err();
        assert_eq!(e.key, "errors.tls.caNoPemOrDer", "{}", e.message);
    }
}
