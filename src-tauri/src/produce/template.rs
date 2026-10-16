use chrono::{DateTime, SecondsFormat, Utc};

use crate::error::{AppError, AppResult, ErrorCode};
use crate::produce::payload::{PayloadFormat, ProduceMessage};

const MAX_RANDOM_STRING: usize = 100_000;
/// Подставляются параметрами, чтобы `{{` не путался с плейсхолдерами словаря.
const OPEN: &str = "{{";
const ESCAPED: &str = "\\{{";

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

/// Ошибка шаблона; текст для интерфейса — `to_app_error` (с именем заголовка или без).
#[derive(Debug, Clone, PartialEq)]
enum TemplateError {
    Arity { placeholder: String, count: usize },
    NotInteger { placeholder: String, value: String },
    MinGreaterThanMax { placeholder: String },
    StringLength { placeholder: String },
    EnvMissing { name: String },
    Unknown { placeholder: String },
    Unclosed,
}

impl TemplateError {
    fn to_app_error(&self, header: Option<&str>) -> AppError {
        use TemplateError as E;
        let text = match (self, header) {
            (E::Arity { placeholder, count }, None) => ltext!(
                "errors.template.arity",
                "{{placeholder}}: expected number of arguments — {{count}}",
                placeholder = placeholder,
                count = count
            ),
            (E::Arity { placeholder, count }, Some(h)) => ltext!(
                "errors.template.headerArity",
                "Header “{{header}}”: {{placeholder}}: expected number of arguments — {{count}}",
                header = h,
                placeholder = placeholder,
                count = count
            ),
            (E::NotInteger { placeholder, value }, None) => ltext!(
                "errors.template.notInteger",
                "{{placeholder}}: “{{value}}” is not an integer",
                placeholder = placeholder,
                value = value
            ),
            (E::NotInteger { placeholder, value }, Some(h)) => ltext!(
                "errors.template.headerNotInteger",
                "Header “{{header}}”: {{placeholder}}: “{{value}}” is not an integer",
                header = h,
                placeholder = placeholder,
                value = value
            ),
            (E::MinGreaterThanMax { placeholder }, None) => ltext!(
                "errors.template.minGreaterThanMax",
                "{{placeholder}}: min is greater than max",
                placeholder = placeholder
            ),
            (E::MinGreaterThanMax { placeholder }, Some(h)) => ltext!(
                "errors.template.headerMinGreaterThanMax",
                "Header “{{header}}”: {{placeholder}}: min is greater than max",
                header = h,
                placeholder = placeholder
            ),
            (E::StringLength { placeholder }, None) => ltext!(
                "errors.template.stringLength",
                "{{placeholder}}: the length must be a number from 0 to {{max}}",
                placeholder = placeholder,
                max = MAX_RANDOM_STRING
            ),
            (E::StringLength { placeholder }, Some(h)) => ltext!(
                "errors.template.headerStringLength",
                "Header “{{header}}”: {{placeholder}}: the length must be a number from 0 to {{max}}",
                header = h,
                placeholder = placeholder,
                max = MAX_RANDOM_STRING
            ),
            (E::EnvMissing { name }, None) => ltext!(
                "errors.template.envMissing",
                "Environment variable {{name}} is not set",
                name = name
            ),
            (E::EnvMissing { name }, Some(h)) => ltext!(
                "errors.template.headerEnvMissing",
                "Header “{{header}}”: environment variable {{name}} is not set",
                header = h,
                name = name
            ),
            (E::Unknown { placeholder }, None) => ltext!(
                "errors.template.unknown",
                "Unknown placeholder “{{placeholder}}” (to send “{{open}}” literally, write “{{escaped}}”)",
                placeholder = placeholder,
                open = OPEN,
                escaped = ESCAPED
            ),
            (E::Unknown { placeholder }, Some(h)) => ltext!(
                "errors.template.headerUnknown",
                "Header “{{header}}”: unknown placeholder “{{placeholder}}” (to send “{{open}}” literally, write “{{escaped}}”)",
                header = h,
                placeholder = placeholder,
                open = OPEN,
                escaped = ESCAPED
            ),
            (E::Unclosed, None) => ltext!(
                "errors.template.unclosed",
                "Unclosed placeholder: “}}” is missing (to send “{{open}}” literally, write “{{escaped}}”)",
                open = OPEN,
                escaped = ESCAPED
            ),
            (E::Unclosed, Some(h)) => ltext!(
                "errors.template.headerUnclosed",
                "Header “{{header}}”: unclosed placeholder: “}}” is missing (to send “{{open}}” literally, write “{{escaped}}”)",
                header = h,
                open = OPEN,
                escaped = ESCAPED
            ),
        };
        AppError::localized(ErrorCode::InvalidInput, text)
    }
}

fn eval(expr: &str, ctx: &mut RenderContext) -> Result<String, TemplateError> {
    let mut parts = expr.split_whitespace();
    let name = parts.next().unwrap_or("");
    let args: Vec<&str> = parts.collect();
    let placeholder = || format!("{OPEN}{expr}}}}}");
    let arity = |n: usize| -> Result<(), TemplateError> {
        if args.len() == n {
            Ok(())
        } else {
            Err(TemplateError::Arity {
                placeholder: placeholder(),
                count: n,
            })
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
                s.parse::<i64>().map_err(|_| TemplateError::NotInteger {
                    placeholder: placeholder(),
                    value: s.to_string(),
                })
            };
            let (min, max) = (parse(args[0])?, parse(args[1])?);
            if min > max {
                return Err(TemplateError::MinGreaterThanMax {
                    placeholder: placeholder(),
                });
            }
            Ok(ctx.rng.i64(min..=max).to_string())
        }
        "randomString" => {
            arity(1)?;
            let n: usize = args[0]
                .parse()
                .ok()
                .filter(|n| *n <= MAX_RANDOM_STRING)
                .ok_or_else(|| TemplateError::StringLength {
                    placeholder: placeholder(),
                })?;
            Ok((0..n).map(|_| ctx.rng.alphanumeric()).collect())
        }
        "env" => {
            arity(1)?;
            (ctx.env)(args[0]).ok_or_else(|| TemplateError::EnvMissing {
                name: args[0].to_string(),
            })
        }
        _ => Err(TemplateError::Unknown {
            placeholder: placeholder(),
        }),
    }
}

/// Подставляет плейсхолдеры `{{…}}`.
/// Экранирование: `\{{` выводится как литерал `{{` и не разбирается как плейсхолдер.
pub fn render(template: &str, ctx: &mut RenderContext) -> AppResult<String> {
    render_inner(template, ctx).map_err(|e| e.to_app_error(None))
}

fn render_inner(template: &str, ctx: &mut RenderContext) -> Result<String, TemplateError> {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        if rest[..start].ends_with('\\') {
            out.push_str(&rest[..start - 1]);
            out.push_str("{{");
            rest = &rest[start + 2..];
            continue;
        }
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after.find("}}").ok_or(TemplateError::Unclosed)?;
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
        h.value = render_inner(&h.value, ctx).map_err(|e| e.to_app_error(Some(&h.name)))?;
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
        for (bad, key) in [
            ("{{unknown}}", "errors.template.unknown"),
            ("{{randomInt 5}}", "errors.template.arity"),
            ("{{randomInt 9 1}}", "errors.template.minGreaterThanMax"),
            ("{{randomInt a b}}", "errors.template.notInteger"),
            ("{{randomString}}", "errors.template.arity"),
            ("{{randomString 100001}}", "errors.template.stringLength"),
            ("{{env}}", "errors.template.arity"),
            ("{{env MISSING_VAR}}", "errors.template.envMissing"),
            ("{{seq", "errors.template.unclosed"),
        ] {
            let e = with_ctx(0, |c| render(bad, c)).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput, "{bad}");
            assert_eq!(e.key, key, "{bad}");
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

    #[test]
    fn random_int_covers_range_with_one_rng() {
        let mut rng = fastrand::Rng::with_seed(1);
        let mut ctx = RenderContext {
            seq: 0,
            now: chrono::Utc
                .with_ymd_and_hms(2026, 10, 15, 12, 30, 0)
                .unwrap(),
            rng: &mut rng,
            env: &env,
        };
        let mut seen = std::collections::HashSet::new();
        for _ in 0..500 {
            let n: i64 = render("{{randomInt -2 2}}", &mut ctx)
                .unwrap()
                .parse()
                .unwrap();
            assert!((-2..=2).contains(&n));
            seen.insert(n);
        }
        assert_eq!(seen.len(), 5, "достижимы обе границы и всё между ними");
    }

    #[test]
    fn escape_renders_literal_braces() {
        assert_eq!(
            with_ctx(0, |c| render(r"\{{name}}", c)).unwrap(),
            "{{name}}"
        );
        let out = with_ctx(0, |c| render(r#"{"t":"\{{x}}"}"#, c)).unwrap();
        assert_eq!(out, r#"{"t":"{{x}}"}"#);
        serde_json::from_str::<serde_json::Value>(&out).unwrap();
        assert_eq!(with_ctx(5, |c| render(r"\{{ {{seq}}", c)).unwrap(), "{{ 5");
    }

    #[test]
    fn error_mentions_escape() {
        let e = with_ctx(0, |c| render("{{x}}", c)).unwrap_err();
        assert_eq!(e.key, "errors.template.unknown");
        assert_eq!(e.params["placeholder"], "{{x}}");
        assert!(e.message.contains(r"\{{"), "{}", e.message);
        let e = with_ctx(0, |c| render("{{seq", c)).unwrap_err();
        assert_eq!(e.key, "errors.template.unclosed");
        assert!(e.message.contains(r"\{{"), "{}", e.message);
    }

    #[test]
    fn edge_cases() {
        assert_eq!(with_ctx(2, |c| render("{{seq}}{{seq}}", c)).unwrap(), "22");
        assert_eq!(with_ctx(0, |c| render("a}}b", c)).unwrap(), "a}}b");
        assert_eq!(
            with_ctx(0, |c| render("{{randomInt 5 5}}", c)).unwrap(),
            "5"
        );
        assert_eq!(
            with_ctx(0, |c| render("{{randomString 0}}", c)).unwrap(),
            ""
        );
        for bad in ["{{}}", "{{randomInt 99999999999999999999 1}}"] {
            let e = with_ctx(0, |c| render(bad, c)).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput, "{bad}");
        }
        let full = "{{randomInt -9223372036854775808 9223372036854775807}}";
        with_ctx(0, |c| render(full, c))
            .unwrap()
            .parse::<i64>()
            .unwrap();
    }

    #[test]
    fn header_error_names_header() {
        let m = ProduceMessage {
            topic: "t".into(),
            partition: None,
            key: PayloadInput {
                format: PayloadFormat::Null,
                text: String::new(),
            },
            value: PayloadInput {
                format: PayloadFormat::Null,
                text: String::new(),
            },
            headers: vec![HeaderInput {
                name: "trace".into(),
                value: "{{nope}}".into(),
                format: HeaderFormat::String,
            }],
        };
        let e = with_ctx(0, |c| render_message(&m, c)).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert_eq!(e.key, "errors.template.headerUnknown");
        assert_eq!(e.params["header"], "trace");
        assert_eq!(e.params["placeholder"], "{{nope}}");
    }

    /// Контракт с UI (`literalText` в fromMessage.ts): замена каждого `{{` на `\{{` даёт текст,
    /// который `render` возвращает без изменений — «Отправить повторно» и «Повторить» шлют
    /// точную копию, `{{env …}}` из прочитанного сообщения не раскрывается.
    #[test]
    fn escaping_every_open_brace_renders_verbatim() {
        for original in [
            "{{uuid}}",
            "{{env TEAM}}",
            r"a\{{b",
            r"\\{{x}}",
            "{{{",
            "{{{{",
            "}} {{nope",
            "без шаблона",
        ] {
            let escaped = original.replace("{{", r"\{{");
            let out = with_ctx(1, |c| render(&escaped, c)).unwrap();
            assert_eq!(out, original, "{escaped}");
        }
    }
}
