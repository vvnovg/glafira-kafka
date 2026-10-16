use openssl::pkey::{PKey, Private};
use openssl::x509::X509;

use crate::error::{AppError, AppResult};

const JKS_MAGIC: [u8; 4] = [0xFE, 0xED, 0xFE, 0xED];

pub fn is_jks(data: &[u8]) -> bool {
    data.starts_with(&JKS_MAGIC)
}

pub fn trusted_certificates(_data: &[u8], _password: Option<&str>) -> AppResult<Vec<X509>> {
    Err(AppError::invalid("Формат JKS пока не поддерживается"))
}

pub fn identity(
    _data: &[u8],
    _alias: Option<&str>,
    _store_password: Option<&str>,
    _key_password: Option<&str>,
) -> AppResult<(PKey<Private>, Vec<X509>)> {
    Err(AppError::invalid("Формат JKS пока не поддерживается"))
}
