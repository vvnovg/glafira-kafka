//! Защита мутирующих операций (спец. §5): проверяется в Rust, а не только в UI.

use crate::error::{AppError, AppResult, ErrorCode};
use crate::storage::connections::{ConnectionProfile, Environment};

/// read-only запрещает любые изменения; prod без read-only требует ввести имя подключения.
pub fn ensure_mutation_allowed(p: &ConnectionProfile, confirm: Option<&str>) -> AppResult<()> {
    if p.read_only {
        return Err(AppError::new(
            ErrorCode::ReadOnly,
            format!("Подключение «{}» открыто только для чтения", p.name),
        ));
    }
    if p.environment == Environment::Prod && confirm.map(str::trim) != Some(p.name.trim()) {
        return Err(AppError::new(
            ErrorCode::ConfirmationRequired,
            format!(
                "Для изменений в prod-подключении введите его имя «{}»",
                p.name
            ),
        ));
    }
    Ok(())
}

/// Удаление и очистка требуют ввести имя объекта (при удалении нескольких топиков — их число).
pub fn ensure_name_confirmed(expected: &str, typed: Option<&str>) -> AppResult<()> {
    if typed.map(str::trim) == Some(expected.trim()) {
        Ok(())
    } else {
        Err(AppError::new(
            ErrorCode::ConfirmationRequired,
            format!("Для подтверждения введите «{expected}»"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::storage::connections::{Environment, sample_profile};

    #[test]
    fn read_only_blocks_even_with_confirmation() {
        let mut p = sample_profile("a");
        p.read_only = true;
        let e = ensure_mutation_allowed(&p, Some(&p.name.clone())).unwrap_err();
        assert_eq!(e.code, ErrorCode::ReadOnly);
    }

    #[test]
    fn prod_requires_connection_name() {
        let mut p = sample_profile("a");
        p.environment = Environment::Prod;
        p.name = "kafka-prod".into();
        for confirm in [None, Some(""), Some("kafka-pro"), Some("KAFKA-PROD")] {
            let e = ensure_mutation_allowed(&p, confirm).unwrap_err();
            assert_eq!(e.code, ErrorCode::ConfirmationRequired, "{confirm:?}");
            assert!(e.message.contains("kafka-prod"), "{}", e.message);
        }
        assert!(ensure_mutation_allowed(&p, Some("kafka-prod")).is_ok());
        assert!(ensure_mutation_allowed(&p, Some(" kafka-prod ")).is_ok());
    }

    #[test]
    fn other_environments_need_no_confirmation() {
        for env in [
            Environment::Local,
            Environment::Dev,
            Environment::Test,
            Environment::Stage,
        ] {
            let mut p = sample_profile("a");
            p.environment = env;
            assert!(ensure_mutation_allowed(&p, None).is_ok(), "{env:?}");
        }
    }

    #[test]
    fn object_name_must_match_exactly() {
        assert!(ensure_name_confirmed("orders", Some("orders")).is_ok());
        assert!(ensure_name_confirmed("orders", Some(" orders ")).is_ok());
        // Сравнение симметрично: id с пробелом на конце подтверждается (UI отправляет trim).
        assert!(ensure_name_confirmed("grp ", Some("grp")).is_ok());
        assert!(ensure_name_confirmed("grp ", Some("grp ")).is_ok());
        for typed in [None, Some("order"), Some("ORDERS")] {
            let e = ensure_name_confirmed("orders", typed).unwrap_err();
            assert_eq!(e.code, ErrorCode::ConfirmationRequired, "{typed:?}");
        }
    }
}
