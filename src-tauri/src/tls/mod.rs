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

#[cfg(test)]
use crate::error::ErrorCode;
use crate::error::{AppError, AppResult};
use crate::storage::connections::{CertFormat, ClientCert};

pub struct ClientIdentity {
    pub certificate_pem: String,
    pub key_pem: String,
}

impl std::fmt::Debug for ClientIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientIdentity")
            .field("key_pem", &"<скрыто>")
            .finish_non_exhaustive()
    }
}

const CLIENT: &str = "Клиентский сертификат";
const KEY: &str = "Ключ клиента";
const CA: &str = "Доверенный CA";

fn read_file(path: &str, what: &str) -> AppResult<Vec<u8>> {
    let p = Path::new(path.trim());
    if !p.is_absolute() {
        return Err(AppError::invalid(format!(
            "{what}: укажите абсолютный путь (сейчас «{path}»)"
        )));
    }
    std::fs::read(p)
        .map_err(|e| AppError::invalid(format!("{what}: не удалось прочитать «{path}»: {e}")))
}

fn ssl_err(message: String, e: ErrorStack) -> AppError {
    AppError::invalid(message).with_details(e.to_string())
}

fn non_empty(p: Option<&str>) -> Option<&str> {
    p.filter(|s| !s.is_empty())
}

/// Доверенный CA в PEM (цепочка сертификатов).
pub fn load_ca(format: CertFormat, path: &str, password: Option<&str>) -> AppResult<String> {
    let data = read_file(path, CA)?;
    let certs: Vec<X509> = match format {
        CertFormat::Pem => X509::stack_from_pem(&data)
            .map_err(|e| ssl_err(format!("{CA}: «{path}» не содержит PEM-сертификатов"), e))?,
        CertFormat::Jks if jks::is_jks(&data) => jks::trusted_certificates(&data, password)?,
        // Начиная с Java 9 keytool по умолчанию создаёт PKCS#12, даже если файл назван .jks.
        CertFormat::Pkcs12 | CertFormat::Jks => {
            let p = parse_pkcs12(&data, password, CA)?;
            p.cert
                .into_iter()
                .chain(p.ca.into_iter().flatten())
                .collect()
        }
    };
    if certs.is_empty() {
        return Err(AppError::invalid(format!(
            "{CA}: в «{path}» нет сертификатов"
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
            let chain =
                X509::stack_from_pem(&read_file(&cert.certificate_path, CLIENT)?).map_err(|e| {
                    ssl_err(
                        format!(
                            "{CLIENT}: «{}» не содержит PEM-сертификатов",
                            cert.certificate_path
                        ),
                        e,
                    )
                })?;
            let key_path = cert
                .key_path
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| AppError::invalid(format!("{KEY}: укажите файл ключа")))?;
            (
                load_pem_key(&read_file(key_path, KEY)?, key_password)?,
                chain,
            )
        }
        CertFormat::Jks => {
            let data = read_file(&cert.certificate_path, CLIENT)?;
            if jks::is_jks(&data) {
                jks::identity(&data, cert.alias.as_deref(), store_password, key_password)?
            } else {
                pkcs12_identity(&data, store_password)?
            }
        }
        CertFormat::Pkcs12 => {
            pkcs12_identity(&read_file(&cert.certificate_path, CLIENT)?, store_password)?
        }
    };
    finish_identity(key, chain)
}

fn load_pem_key(data: &[u8], password: Option<&str>) -> AppResult<PKey<Private>> {
    let encrypted = String::from_utf8_lossy(data).contains("ENCRYPTED");
    match non_empty(password) {
        None if encrypted => Err(AppError::invalid(format!(
            "{KEY}: ключ зашифрован — укажите пароль ключа"
        ))),
        None => PKey::private_key_from_pem(data)
            .map_err(|e| ssl_err(format!("{KEY}: файл не содержит PEM-ключа"), e)),
        Some(pw) => PKey::private_key_from_pem_passphrase(data, pw.as_bytes()).map_err(|e| {
            let msg = if encrypted {
                format!("{KEY}: неверный пароль ключа")
            } else {
                format!("{KEY}: файл не содержит PEM-ключа")
            };
            ssl_err(msg, e)
        }),
    }
}

fn pkcs12_identity(data: &[u8], password: Option<&str>) -> AppResult<(PKey<Private>, Vec<X509>)> {
    let p = parse_pkcs12(data, password, CLIENT)?;
    let key = p
        .pkey
        .ok_or_else(|| AppError::invalid(format!("{CLIENT}: в хранилище нет закрытого ключа")))?;
    let cert = p
        .cert
        .ok_or_else(|| AppError::invalid(format!("{CLIENT}: в хранилище нет сертификата")))?;
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

fn parse_pkcs12(data: &[u8], password: Option<&str>, what: &str) -> AppResult<ParsedPkcs12_2> {
    let p12 = Pkcs12::from_der(data)
        .map_err(|e| ssl_err(format!("{what}: файл не является хранилищем PKCS#12"), e))?;
    let pw = password.unwrap_or("");
    match p12.parse2(pw) {
        Ok(p) => Ok(p),
        Err(e) if is_unsupported(&e) && load_legacy_provider() => {
            p12.parse2(pw).map_err(|e| pkcs12_err(what, e))
        }
        Err(e) => Err(pkcs12_err(what, e)),
    }
}

fn pkcs12_err(what: &str, e: ErrorStack) -> AppError {
    let t = e.to_string().to_lowercase();
    let msg = if t.contains("mac verify") || t.contains("bad decrypt") {
        format!("{what}: неверный пароль хранилища PKCS#12")
    } else if is_unsupported(&e) {
        format!(
            "{what}: хранилище PKCS#12 зашифровано устаревшим алгоритмом (RC2/3DES), который недоступен. \
             Пересоздайте его с AES: openssl pkcs12 -export -keypbe AES-256-CBC -certpbe AES-256-CBC …"
        )
    } else {
        format!("{what}: не удалось прочитать хранилище PKCS#12")
    };
    ssl_err(msg, e)
}

fn finish_identity(key: PKey<Private>, chain: Vec<X509>) -> AppResult<ClientIdentity> {
    let leaf = chain
        .first()
        .ok_or_else(|| AppError::invalid(format!("{CLIENT}: файл не содержит сертификатов")))?;
    let public = leaf
        .public_key()
        .map_err(|e| ssl_err(format!("{CLIENT}: не удалось прочитать открытый ключ"), e))?;
    if !public.public_eq(&key) {
        return Err(AppError::invalid(format!(
            "{KEY} не соответствует клиентскому сертификату"
        )));
    }
    let now = Asn1Time::days_from_now(0)
        .map_err(|e| ssl_err("Не удалось получить текущее время".into(), e))?;
    // Ошибка сравнения — ошибка, а не «действителен».
    let cmp = |t: &openssl::asn1::Asn1TimeRef| {
        t.compare(&now)
            .map_err(|e| ssl_err(format!("{CLIENT}: не удалось проверить срок действия"), e))
    };
    if cmp(leaf.not_before())? == Ordering::Greater {
        return Err(AppError::invalid(format!(
            "{CLIENT} ещё не действует (действует с {})",
            leaf.not_before()
        )));
    }
    if cmp(leaf.not_after())? == Ordering::Less {
        return Err(AppError::invalid(format!(
            "Срок действия: {CLIENT} истёк {}",
            leaf.not_after()
        )));
    }
    let key_pem = key
        .private_key_to_pem_pkcs8()
        .map_err(|e| ssl_err(format!("{KEY}: не удалось преобразовать в PEM"), e))?;
    Ok(ClientIdentity {
        certificate_pem: to_pem_chain(&chain)?,
        key_pem: String::from_utf8(key_pem).map_err(|_| AppError::internal("PEM не в UTF-8"))?,
    })
}

fn to_pem_chain(certs: &[X509]) -> AppResult<String> {
    let mut out = String::new();
    for c in certs {
        let pem = c
            .to_pem()
            .map_err(|e| ssl_err("Не удалось преобразовать сертификат в PEM".into(), e))?;
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
        assert!(e.message.contains("зашифрован"), "{}", e.message);
        let e = load_identity(&cc, Some("wrong"), None).unwrap_err();
        assert!(e.message.contains("неверный пароль"), "{}", e.message);
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
        assert!(e.message.contains("неверный пароль"), "{}", e.message);
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
            Err(e) => assert!(e.message.contains("устаревшим алгоритмом"), "{}", e.message),
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
        assert!(e.message.contains("не соответствует"), "{}", e.message);
    }

    #[test]
    fn expired_and_not_yet_valid_certificates() {
        let dir = tempfile::tempdir().unwrap();
        let (key, cert) = self_signed(-30, -1);
        let e = load_identity(&write_pair(dir.path(), &key, &cert), None, None).unwrap_err();
        assert!(e.message.contains("истёк"), "{}", e.message);
        let (key, cert) = self_signed(2, 30);
        let e = load_identity(&write_pair(dir.path(), &key, &cert), None, None).unwrap_err();
        assert!(e.message.contains("ещё не действует"), "{}", e.message);
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
        assert!(e.message.contains("сертификат"), "{}", e.message);
    }
}
