use chrono::{DateTime, SecondsFormat, Utc};

use crate::error::{AppError, AppResult};
use crate::produce::payload::{PayloadFormat, ProduceMessage};

const MAX_RANDOM_STRING: usize = 100_000;

pub struct RenderContext<'a> {
    /// Номер сообщения в пакете (0 для одиночной отправки).
    pub seq: u64,
    pub now: DateTime<Utc>,
    pub rng: &'a mut fastrand::Rng,
    pub env: &'a dyn Fn(&str) -> Option<String>,
}

pub fn system_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

fn invalid(msg: String) -> AppError {
    AppError::invalid(msg)
}

fn eval(expr: &str, ctx: &mut RenderContext) -> AppResult<String> {
    let mut parts = expr.split_whitespace();
    let name = parts.next().unwrap_or("");
    let args: Vec<&str> = parts.collect();
    let arity = |n: usize| -> AppResult<()> {
        if args.len() == n {
            Ok(())
        } else {
            Err(invalid(format!(
                "{{{{{expr}}}}}: ожидается аргументов — {n}"
            )))
        }
    };
    match name {
        "uuid" => {
            arity(0)?;
            let mut bytes = [0u8; 16];
            ctx.rng.fill(&mut bytes);
            Ok(uuid::Builder::from_random_bytes(bytes)
                .into_uuid()
                .to_string())
        }
        "now" => {
            arity(0)?;
            Ok(ctx.now.to_rfc3339_opts(SecondsFormat::Millis, true))
        }
        "nowMillis" => {
            arity(0)?;
            Ok(ctx.now.timestamp_millis().to_string())
        }
        "seq" => {
            arity(0)?;
            Ok(ctx.seq.to_string())
        }
        "randomInt" => {
            arity(2)?;
            let parse = |s: &str| {
                s.parse::<i64>()
                    .map_err(|_| invalid(format!("{{{{{expr}}}}}: «{s}» — не целое число")))
            };
            let (min, max) = (parse(args[0])?, parse(args[1])?);
            if min > max {
                return Err(invalid(format!("{{{{{expr}}}}}: min больше max")));
            }
            Ok(ctx.rng.i64(min..=max).to_string())
        }
        "randomString" => {
            arity(1)?;
            let n: usize = args[0]
                .parse()
                .ok()
                .filter(|n| *n <= MAX_RANDOM_STRING)
                .ok_or_else(|| {
                    invalid(format!(
                        "{{{{{expr}}}}}: длина — число от 0 до {MAX_RANDOM_STRING}"
                    ))
                })?;
            Ok((0..n).map(|_| ctx.rng.alphanumeric()).collect())
        }
        "env" => {
            arity(1)?;
            (ctx.env)(args[0])
                .ok_or_else(|| invalid(format!("Переменная окружения {} не задана", args[0])))
        }
        _ => Err(invalid(format!("Неизвестный плейсхолдер {{{{{expr}}}}}"))),
    }
}

/// Подставляет плейсхолдеры `{{…}}`. Экранирования нет: литерал `{{` в тексте недопустим.
pub fn render(template: &str, ctx: &mut RenderContext) -> AppResult<String> {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find("}}")
            .ok_or_else(|| invalid("Незакрытый плейсхолдер: нет «}}»".to_string()))?;
        out.push_str(&eval(after[..end].trim(), ctx)?);
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    Ok(out)
}

pub fn render_message(m: &ProduceMessage, ctx: &mut RenderContext) -> AppResult<ProduceMessage> {
    let mut r = m.clone();
    if r.key.format != PayloadFormat::Null {
        r.key.text = render(&m.key.text, ctx)?;
    }
    if r.value.format != PayloadFormat::Null {
        r.value.text = render(&m.value.text, ctx)?;
    }
    for h in &mut r.headers {
        h.value = render(&h.value, ctx)?;
    }
    Ok(r)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::produce::payload::{HeaderFormat, HeaderInput, PayloadFormat, PayloadInput};
    use chrono::TimeZone;

    fn env(name: &str) -> Option<String> {
        (name == "TEAM").then(|| "qa".to_string())
    }

    fn with_ctx<T>(seq: u64, f: impl FnOnce(&mut RenderContext) -> T) -> T {
        let mut rng = fastrand::Rng::with_seed(42);
        let mut ctx = RenderContext {
            seq,
            now: chrono::Utc
                .with_ymd_and_hms(2026, 10, 15, 12, 30, 0)
                .unwrap(),
            rng: &mut rng,
            env: &env,
        };
        f(&mut ctx)
    }

    #[test]
    fn simple_placeholders() {
        let out = with_ctx(7, |c| {
            render("#{{seq}} at {{now}} ({{nowMillis}}) team={{env TEAM}}", c)
        })
        .unwrap();
        assert_eq!(
            out,
            "#7 at 2026-10-15T12:30:00.000Z (1792067400000) team=qa"
        );
        assert_eq!(
            with_ctx(0, |c| render("без плейсхолдеров", c)).unwrap(),
            "без плейсхолдеров"
        );
        assert_eq!(
            with_ctx(0, |c| render("{{ seq }}", c)).unwrap(),
            "0",
            "пробелы внутри скобок допустимы"
        );
    }

    #[test]
    fn random_placeholders_respect_arguments() {
        for _ in 0..50 {
            let n: i64 = with_ctx(0, |c| render("{{randomInt -5 5}}", c))
                .unwrap()
                .parse()
                .unwrap();
            assert!((-5..=5).contains(&n));
        }
        let s = with_ctx(0, |c| render("{{randomString 12}}", c)).unwrap();
        assert_eq!(s.len(), 12);
        assert!(s.chars().all(|c| c.is_ascii_alphanumeric()));
        let u = with_ctx(0, |c| render("{{uuid}}", c)).unwrap();
        assert_eq!(uuid::Uuid::parse_str(&u).unwrap().get_version_num(), 4);
        // Один и тот же seed — одинаковый результат (детерминированность тестов и предпросмотра).
        assert_eq!(u, with_ctx(0, |c| render("{{uuid}}", c)).unwrap());
    }

    #[test]
    fn errors_are_invalid_input() {
        for bad in [
            "{{unknown}}",
            "{{randomInt 5}}",
            "{{randomInt 9 1}}",
            "{{randomInt a b}}",
            "{{randomString}}",
            "{{randomString 100001}}",
            "{{env}}",
            "{{env MISSING_VAR}}",
            "{{seq",
        ] {
            let e = with_ctx(0, |c| render(bad, c)).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput, "{bad}");
        }
    }

    #[test]
    fn message_rendering_shares_seq_and_skips_null() {
        let m = ProduceMessage {
            topic: "t".into(),
            partition: None,
            key: PayloadInput {
                format: PayloadFormat::String,
                text: "k-{{seq}}".into(),
            },
            value: PayloadInput {
                format: PayloadFormat::Null,
                text: "{{unknown}}".into(),
            },
            headers: vec![HeaderInput {
                name: "n".into(),
                value: "{{seq}}".into(),
                format: HeaderFormat::String,
            }],
        };
        let r = with_ctx(3, |c| render_message(&m, c)).unwrap();
        assert_eq!(r.key.text, "k-3");
        assert_eq!(r.headers[0].value, "3");
        assert_eq!(r.value.text, "{{unknown}}", "null-значение не рендерится");
    }
}
