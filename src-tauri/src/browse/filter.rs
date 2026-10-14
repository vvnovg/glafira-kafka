use regex::{Regex, RegexBuilder};
use serde::Deserialize;
use serde_json::Value;
use serde_json_path::JsonPath;

use crate::browse::types::{BrowsedMessage, HeaderView};
use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TextMatch {
    pub pattern: String,
    #[serde(default)]
    pub regex: bool,
    #[serde(default)]
    pub case_sensitive: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeaderMatch {
    pub name: String,
    #[serde(default)]
    pub value: Option<TextMatch>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FilterSpec {
    #[serde(default)]
    pub key: Option<TextMatch>,
    #[serde(default)]
    pub value: Option<TextMatch>,
    #[serde(default)]
    pub header: Option<HeaderMatch>,
    #[serde(default)]
    pub json_path: Option<String>,
    #[serde(default)]
    pub from_timestamp: Option<i64>,
    #[serde(default)]
    pub to_timestamp: Option<i64>,
}

pub struct MessageView<'a> {
    pub key: Option<&'a str>,
    pub value: Option<&'a str>,
    pub headers: &'a [HeaderView],
    pub timestamp: Option<i64>,
}

impl BrowsedMessage {
    pub fn view(&self) -> MessageView<'_> {
        MessageView {
            key: self.key.as_ref().map(|d| d.text.as_str()),
            value: self.value.as_ref().map(|d| d.text.as_str()),
            headers: &self.headers,
            timestamp: self.timestamp,
        }
    }
}

enum Matcher {
    Substring { needle: String, case_sensitive: bool },
    Regex(Regex),
}

impl Matcher {
    fn compile(m: &TextMatch) -> AppResult<Option<Self>> {
        if m.pattern.is_empty() {
            return Ok(None);
        }
        if m.regex {
            let re = RegexBuilder::new(&m.pattern)
                .case_insensitive(!m.case_sensitive)
                .build()
                .map_err(|e| AppError::invalid(format!("Некорректное регулярное выражение «{}»: {e}", m.pattern)))?;
            return Ok(Some(Matcher::Regex(re)));
        }
        let needle = if m.case_sensitive { m.pattern.clone() } else { m.pattern.to_lowercase() };
        Ok(Some(Matcher::Substring { needle, case_sensitive: m.case_sensitive }))
    }

    fn is_match(&self, text: &str) -> bool {
        match self {
            Matcher::Substring { needle, case_sensitive: true } => text.contains(needle.as_str()),
            Matcher::Substring { needle, case_sensitive: false } => text.to_lowercase().contains(needle.as_str()),
            Matcher::Regex(re) => re.is_match(text),
        }
    }
}

enum CmpOp {
    Eq,
    Ne,
}

struct JsonCondition {
    path: JsonPath,
    cmp: Option<(CmpOp, Value)>,
}

impl JsonCondition {
    /// `<JSONPath> [== | != <литерал>]`. Делим по последнему оператору; если левая часть
    /// не парсится как JSONPath (оператор был внутри фильтра `[?...]`), всё выражение — путь.
    fn parse(s: &str) -> AppResult<Self> {
        let s = s.trim();
        for (op_str, op) in [("!=", CmpOp::Ne), ("==", CmpOp::Eq)] {
            if let Some((left, right)) = s.rsplit_once(op_str)
                && let Ok(path) = JsonPath::parse(left.trim()) {
                    return Ok(Self { path, cmp: Some((op, parse_literal(right))) });
                }
        }
        let path = JsonPath::parse(s).map_err(|e| AppError::invalid(format!("Некорректный JSONPath «{s}»: {e}")))?;
        Ok(Self { path, cmp: None })
    }

    fn matches(&self, doc: &Value) -> bool {
        let nodes = self.path.query(doc).all();
        match &self.cmp {
            None => !nodes.is_empty(),
            Some((CmpOp::Eq, lit)) => nodes.contains(&lit),
            Some((CmpOp::Ne, lit)) => !nodes.is_empty() && nodes.iter().all(|n| *n != lit),
        }
    }
}

fn parse_literal(raw: &str) -> Value {
    let t = raw.trim();
    if t.len() >= 2 && t.starts_with('\'') && t.ends_with('\'') {
        return Value::String(t[1..t.len() - 1].to_string());
    }
    serde_json::from_str(t).unwrap_or_else(|_| Value::String(t.to_string()))
}

pub struct CompiledFilter {
    key: Option<Matcher>,
    value: Option<Matcher>,
    header: Option<(String, Option<Matcher>)>,
    json: Option<JsonCondition>,
    from: Option<i64>,
    to: Option<i64>,
}

impl CompiledFilter {
    pub fn compile(spec: &FilterSpec) -> AppResult<Self> {
        let header = match &spec.header {
            Some(h) if !h.name.is_empty() => {
                Some((h.name.clone(), h.value.as_ref().map(Matcher::compile).transpose()?.flatten()))
            }
            _ => None,
        };
        let json = spec
            .json_path
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .map(JsonCondition::parse)
            .transpose()?;
        Ok(Self {
            key: spec.key.as_ref().map(Matcher::compile).transpose()?.flatten(),
            value: spec.value.as_ref().map(Matcher::compile).transpose()?.flatten(),
            header,
            json,
            from: spec.from_timestamp,
            to: spec.to_timestamp,
        })
    }

    pub fn matches(&self, m: &MessageView) -> bool {
        if self.from.is_some() || self.to.is_some() {
            let Some(ts) = m.timestamp else { return false };
            if self.from.is_some_and(|f| ts < f) || self.to.is_some_and(|t| ts > t) {
                return false;
            }
        }
        if let Some(k) = &self.key
            && !m.key.is_some_and(|t| k.is_match(t)) {
                return false;
            }
        if let Some(v) = &self.value
            && !m.value.is_some_and(|t| v.is_match(t)) {
                return false;
            }
        if let Some((name, vm)) = &self.header {
            let found = m.headers.iter().any(|h| {
                h.name == *name
                    && match vm {
                        None => true,
                        Some(vm) => h.value.as_deref().is_some_and(|v| vm.is_match(v)),
                    }
            });
            if !found {
                return false;
            }
        }
        if let Some(cond) = &self.json {
            let Some(text) = m.value else { return false };
            let Ok(doc) = serde_json::from_str::<Value>(text) else { return false };
            if !cond.matches(&doc) {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    fn tm(p: &str) -> Option<TextMatch> {
        Some(TextMatch { pattern: p.into(), regex: false, case_sensitive: false })
    }

    fn view<'a>(key: Option<&'a str>, value: Option<&'a str>, headers: &'a [HeaderView], ts: Option<i64>) -> MessageView<'a> {
        MessageView { key, value, headers, timestamp: ts }
    }

    #[test]
    fn empty_spec_matches_everything() {
        let f = CompiledFilter::compile(&FilterSpec::default()).unwrap();
        assert!(f.matches(&view(None, None, &[], None)));
    }

    #[test]
    fn substring_is_case_insensitive_by_default() {
        let f = CompiledFilter::compile(&FilterSpec { value: tm("order-7"), ..Default::default() }).unwrap();
        assert!(f.matches(&view(None, Some("id=ORDER-7"), &[], None)));
        assert!(!f.matches(&view(None, Some("order-8"), &[], None)));
        assert!(!f.matches(&view(None, None, &[], None)));
    }

    #[test]
    fn case_sensitive_and_regex() {
        let cs = FilterSpec { key: Some(TextMatch { pattern: "Abc".into(), regex: false, case_sensitive: true }), ..Default::default() };
        let f = CompiledFilter::compile(&cs).unwrap();
        assert!(!f.matches(&view(Some("abc"), None, &[], None)));
        let re = FilterSpec { key: Some(TextMatch { pattern: r"^user-\d+$".into(), regex: true, case_sensitive: false }), ..Default::default() };
        let f = CompiledFilter::compile(&re).unwrap();
        assert!(f.matches(&view(Some("USER-42"), None, &[], None)));
        assert!(!f.matches(&view(Some("user-x"), None, &[], None)));
    }

    #[test]
    fn invalid_regex_is_invalid_input() {
        let spec = FilterSpec { value: Some(TextMatch { pattern: "(".into(), regex: true, case_sensitive: false }), ..Default::default() };
        assert_eq!(CompiledFilter::compile(&spec).err().unwrap().code, ErrorCode::InvalidInput);
    }

    #[test]
    fn header_by_name_and_value() {
        let headers = vec![HeaderView { name: "trace".into(), value: Some("t-3".into()) }, HeaderView { name: "empty".into(), value: None }];
        let by_name = FilterSpec { header: Some(HeaderMatch { name: "empty".into(), value: None }), ..Default::default() };
        assert!(CompiledFilter::compile(&by_name).unwrap().matches(&view(None, None, &headers, None)));
        let by_value = FilterSpec { header: Some(HeaderMatch { name: "trace".into(), value: tm("t-3") }), ..Default::default() };
        assert!(CompiledFilter::compile(&by_value).unwrap().matches(&view(None, None, &headers, None)));
        let wrong = FilterSpec { header: Some(HeaderMatch { name: "trace".into(), value: tm("t-4") }), ..Default::default() };
        assert!(!CompiledFilter::compile(&wrong).unwrap().matches(&view(None, None, &headers, None)));
    }

    #[test]
    fn json_conditions() {
        let doc = r#"{"order":{"id":7,"status":"FAILED"},"items":[{"qty":1},{"qty":3}]}"#;
        let check = |cond: &str| {
            let f = CompiledFilter::compile(&FilterSpec { json_path: Some(cond.into()), ..Default::default() }).unwrap();
            f.matches(&view(None, Some(doc), &[], None))
        };
        assert!(check(r#"$.order.status == "FAILED""#));
        assert!(check("$.order.status == 'FAILED'"));
        assert!(check("$.order.status == FAILED"));
        assert!(!check("$.order.status == 'NEW'"));
        assert!(check("$.order.status != 'NEW'"));
        assert!(check("$.order.id == 7"));
        assert!(check("$.order.id"));
        assert!(!check("$.missing"));
        assert!(check("$.items[?@.qty > 2]"));
        let f = CompiledFilter::compile(&FilterSpec { json_path: Some("$.a".into()), ..Default::default() }).unwrap();
        assert!(!f.matches(&view(None, Some("not json"), &[], None)));
        let bad = CompiledFilter::compile(&FilterSpec { json_path: Some("$$$".into()), ..Default::default() });
        assert_eq!(bad.err().unwrap().code, ErrorCode::InvalidInput);
    }

    #[test]
    fn time_range() {
        let f = CompiledFilter::compile(&FilterSpec { from_timestamp: Some(100), to_timestamp: Some(200), ..Default::default() }).unwrap();
        assert!(f.matches(&view(None, None, &[], Some(150))));
        assert!(!f.matches(&view(None, None, &[], Some(99))));
        assert!(!f.matches(&view(None, None, &[], Some(201))));
        assert!(!f.matches(&view(None, None, &[], None)));
    }
}
