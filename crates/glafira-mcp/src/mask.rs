//! Маскировка содержимого сообщений перед ответом LLM (спец. G4 §5): список имён полей
//! подключения и встроенные шаблоны (email, номер карты, JWT, Bearer-токен). Чистые функции:
//! всё, что уходит модели из содержимого кластера, проходит через [`Masker`].

use std::fmt;
use std::sync::LazyLock;

use base64::Engine;
use regex::{Captures, Regex};
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};

/// Замена значения поля из списка.
pub const MASK: &str = "***";
/// Сколько первых байтов значения разбирается как JSON; длиннее — текстовый режим.
pub const MASK_PARSE_LIMIT: usize = 256 * 1024;
/// Пределы списка полей подключения (проверяет приложение при сохранении).
pub const MAX_MASK_FIELDS: usize = 200;
pub const MAX_MASK_FIELD_CHARS: usize = 64;
/// Печатный фрагмент двоичных данных, к которому применяются шаблоны.
pub const MIN_PRINTABLE_RUN: usize = 6;

/// Стандартный список (`llmMaskFields = None`): обновляется вместе с Glafira.
pub const STANDARD_FIELDS: [&str; 21] = [
    "*password*",
    "passwd",
    "pwd",
    "*secret*",
    "*token*",
    "apikey",
    "authorization",
    "cookie",
    "setcookie",
    "*email*",
    "*phone*",
    "card*",
    "pan",
    "cvv",
    "cvc",
    "iban",
    "passport*",
    "ssn",
    "inn",
    "snils",
    "*birth*",
];

/// Формат текста в ответе — определённый приложением (`auto` уже разрешён).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DataFormat {
    String,
    Json,
    Hex,
    Base64,
}

/// Ключ или значение сообщения в ответе LLM: замаскированный и обрезанный текст.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MaskedData {
    pub format: DataFormat,
    pub text: String,
    /// Исходный размер в байтах.
    pub size: usize,
    pub truncated: bool,
}

/// Список полей подключения: `None` — стандартный.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MaskConfig {
    pub fields: Option<Vec<String>>,
}

/// Имя без регистра и разделителей: `api_key`, `apiKey` и `API-KEY` дают `apikey`.
pub fn normalize(name: &str) -> String {
    name.chars()
        .filter(|c| !matches!(c, '_' | '-' | '.') && !c.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect()
}

/// `*` — любая последовательность символов; совпадать должно имя целиком.
fn glob(pattern: &[char], name: &[char]) -> bool {
    let (mut p, mut n) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while n < name.len() {
        if p < pattern.len() && pattern[p] == '*' {
            star = Some((p, n));
            p += 1;
        } else if p < pattern.len() && pattern[p] == name[n] {
            p += 1;
            n += 1;
        } else if let Some((sp, sn)) = star {
            p = sp + 1;
            n = sn + 1;
            star = Some((sp, sn + 1));
        } else {
            return false;
        }
    }
    pattern[p..].iter().all(|c| *c == '*')
}

static JWT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*").expect("jwt regex")
});
static BEARER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{8,}").expect("bearer regex"));
static EMAIL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}").expect("email regex")
});
/// Серия групп цифр (кандидаты в номер карты): между группами одиночный пробел или дефис.
static DIGIT_RUN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b[0-9]+(?:[ -][0-9]+)*\b").expect("digit run regex"));
/// Имя пары `name=value`, `name: value`, `"name": value` в тексте вместе с разделителем.
static PAIR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?:"(?P<n1>[^"\\]{1,64})"|'(?P<n2>[^'\\]{1,64})'|(?P<n3>[A-Za-z_][A-Za-z0-9_.\-]{0,63}))\s*[:=]\s*"#,
    )
    .expect("pair regex")
});
/// Значение пары: строка в кавычках целиком или до `,`, `;`, `&`, `}`, `]` или пробела.
static VALUE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"^(?:"(?:[^"\\]|\\.)*"|'[^']*'|[^\s,;&}\]]+)"#).expect("value regex")
});
/// Учётные данные после схемы авторизации — в той же строке.
static CREDENTIAL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[ \t]+[^\s,;&}\]]+").expect("credential regex"));

/// Значение пары — схема авторизации, и за ней берутся учётные данные (`Authorization: Negotiate
/// YII…`, иначе они остались бы за `***`): у имён, оканчивающихся на `authorization`, — любая
/// схема (токен RFC 7235), у остальных — только `Bearer`, `Basic`, `Digest`, `Token`.
fn takes_credential(name: &str, value: &str) -> bool {
    if normalize(name).ends_with("authorization") {
        value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
    } else {
        ["bearer", "basic", "digest", "token"]
            .iter()
            .any(|s| value.eq_ignore_ascii_case(s))
    }
}

/// Цифры номера карты: проверка Луна и первая цифра 2–6 (Мир, Visa, Mastercard, Amex,
/// Discover). Метки времени в миллисекундах до 2033 года начинаются с 1 — они не номера карт.
fn is_card(candidate: &str) -> bool {
    let digits: Vec<u32> = candidate.chars().filter_map(|c| c.to_digit(10)).collect();
    if !(13..=19).contains(&digits.len()) || !(2..=6).contains(&digits[0]) {
        return false;
    }
    let sum: u32 = digits
        .iter()
        .rev()
        .enumerate()
        .map(|(i, &d)| {
            if i % 2 == 1 {
                let x = d * 2;
                if x > 9 { x - 9 } else { x }
            } else {
                d
            }
        })
        .sum();
    sum.is_multiple_of(10)
}

/// Номера карт (диапазоны байтов) среди групп цифр: в каждой серии — окно из подряд идущих целых
/// групп с 13–19 цифрами, прошедшее [`is_card`], с самым левым началом, при нём самое длинное;
/// дальше поиск идёт со следующей за окном группы. Соседние числа (`… 12/25`, `qty 2 …`) не
/// мешают, а группа не делится: 23 цифры подряд — не карта.
fn card_spans(text: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    for run in DIGIT_RUN.find_iter(text) {
        let mut groups = Vec::new();
        let mut start = run.start();
        for (i, b) in run.as_str().bytes().enumerate() {
            if !b.is_ascii_digit() {
                groups.push((start, run.start() + i));
                start = run.start() + i + 1;
            }
        }
        groups.push((start, run.end()));
        let mut i = 0;
        while i < groups.len() {
            let mut digits = 0;
            let mut found = None;
            for (j, &(s, e)) in groups.iter().enumerate().skip(i) {
                digits += e - s;
                if digits > 19 {
                    break;
                }
                if digits >= 13 && is_card(&text[groups[i].0..e]) {
                    found = Some(j);
                }
            }
            match found {
                Some(j) => {
                    spans.push((groups[i].0, groups[j].1));
                    i = j + 1;
                }
                None => i += 1,
            }
        }
    }
    spans
}

/// Шаблоны в тексте: метки без данных.
fn patterns(text: &str, count: &mut usize) -> String {
    let mut n = 0;
    let s = JWT.replace_all(text, |_: &Captures| {
        n += 1;
        "[jwt]"
    });
    let s = BEARER.replace_all(&s, |_: &Captures| {
        n += 1;
        "Bearer [token]"
    });
    let s = EMAIL.replace_all(&s, |_: &Captures| {
        n += 1;
        "[email]"
    });
    let mut out = String::with_capacity(s.len());
    let mut last = 0;
    for (start, end) in card_spans(&s) {
        out.push_str(&s[last..start]);
        out.push_str("[card]");
        last = end;
        n += 1;
    }
    out.push_str(&s[last..]);
    *count += n;
    out
}

/// Шаблоны в печатном фрагменте двоичных данных: совпавшие байты становятся `*` той же длины.
fn patterns_in_place(run: &mut [u8], count: &mut usize) {
    let Ok(text) = std::str::from_utf8(run) else {
        return;
    };
    let mut spans: Vec<(usize, usize)> = Vec::new();
    for re in [&*JWT, &*BEARER, &*EMAIL] {
        spans.extend(re.find_iter(text).map(|m| (m.start(), m.end())));
    }
    spans.extend(card_spans(text));
    spans.sort_unstable();
    let mut covered = 0;
    for (start, end) in spans {
        if end <= covered {
            continue;
        }
        *count += 1;
        for b in &mut run[start.max(covered)..end] {
            *b = b'*';
        }
        covered = end;
    }
}

/// Обрезает текст до `max` байтов по границе символа.
fn cut(mut s: String, max: usize) -> (String, bool) {
    if s.len() <= max {
        return (s, false);
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s.truncate(end);
    (s, true)
}

/// Скомпилированные правила маскировки одного подключения.
#[derive(Debug, Clone)]
pub struct Masker {
    fields: Vec<Vec<char>>,
}

impl Masker {
    pub fn new(config: &MaskConfig) -> Self {
        let names: Vec<String> = match &config.fields {
            Some(list) => list.clone(),
            None => STANDARD_FIELDS.iter().map(|s| s.to_string()).collect(),
        };
        Self {
            fields: names
                .iter()
                .map(|n| normalize(n))
                .filter(|n| !n.is_empty())
                .map(|n| n.chars().collect())
                .collect(),
        }
    }

    /// Имя поля или заголовка совпадает со списком.
    pub fn field_matches(&self, name: &str) -> bool {
        let n: Vec<char> = normalize(name).chars().collect();
        !n.is_empty() && self.fields.iter().any(|p| glob(p, &n))
    }

    /// Текстовый режим: значения пар `name=value` из списка → `***`, затем шаблоны. Значение пары
    /// с именем не из списка просматривается дальше — в нём бывают свои пары
    /// (`url=…?password=x`, `note: token=y`).
    pub fn text(&self, s: &str, count: &mut usize) -> String {
        let mut out = String::with_capacity(s.len());
        let mut pos = 0;
        while let Some(c) = PAIR.captures_at(s, pos) {
            let head = c.get(0).expect("match").end();
            out.push_str(&s[pos..head]);
            pos = head;
            let name = c
                .name("n1")
                .or_else(|| c.name("n2"))
                .or_else(|| c.name("n3"))
                .map_or("", |m| m.as_str());
            if !self.field_matches(name) {
                continue;
            }
            let Some(v) = VALUE.find(&s[head..]) else {
                continue;
            };
            *count += 1;
            pos = head + v.end();
            let quote = v
                .as_str()
                .chars()
                .next()
                .filter(|q| *q == '"' || *q == '\'');
            match quote {
                Some(q) => {
                    out.push(q);
                    out.push_str(MASK);
                    out.push(q);
                }
                None => {
                    out.push_str(MASK);
                    if takes_credential(name, v.as_str())
                        && let Some(cred) = CREDENTIAL.find(&s[pos..])
                    {
                        pos += cred.end();
                    }
                }
            }
        }
        out.push_str(&s[pos..]);
        patterns(&out, count)
    }

    /// JSON-режим: значение члена объекта из списка (любого типа) → `"***"`, остальные строки —
    /// текстовый режим, имена членов — шаблоны, числа-карты → `"[card]"`. `None`, если это не JSON.
    pub fn json(&self, bytes: &[u8], count: &mut usize) -> Option<String> {
        let mut node: Node = serde_json::from_slice(bytes).ok()?;
        self.node(&mut node, count);
        serde_json::to_string(&node).ok()
    }

    fn node(&self, node: &mut Node, count: &mut usize) {
        match node {
            Node::String(s) => *s = self.text(s, count),
            Node::Number(n) => {
                let digits = n.to_string();
                if digits.bytes().all(|b| b.is_ascii_digit()) && is_card(&digits) {
                    *count += 1;
                    *node = Node::String("[card]".into());
                }
            }
            Node::Array(items) => items.iter_mut().for_each(|i| self.node(i, count)),
            Node::Object(members) => {
                for (name, value) in members.iter_mut() {
                    if self.field_matches(name) {
                        *count += 1;
                        *value = Node::String(MASK.into());
                    } else {
                        self.node(value, count);
                    }
                    *name = patterns(name, count);
                }
            }
            Node::Null | Node::Bool(_) => {}
        }
    }

    /// Двоичный режим: шаблоны в печатных ASCII-фрагментах длиной от [`MIN_PRINTABLE_RUN`].
    pub fn binary(&self, bytes: &[u8], count: &mut usize) -> Vec<u8> {
        let mut out = bytes.to_vec();
        let mut i = 0;
        while i < out.len() {
            if !(0x20..=0x7e).contains(&out[i]) {
                i += 1;
                continue;
            }
            let start = i;
            while i < out.len() && (0x20..=0x7e).contains(&out[i]) {
                i += 1;
            }
            if i - start >= MIN_PRINTABLE_RUN {
                patterns_in_place(&mut out[start..i], count);
            }
        }
        out
    }

    /// Ключ или значение сообщения: `bytes` — первые байты (до [`MASK_PARSE_LIMIT`]), `size` —
    /// исходный размер. Маскировка до обрезки до `max_text` байтов текста; у `hex` и `base64`
    /// маскируются все переданные байты, потом кодируется видимая часть.
    pub fn data(
        &self,
        bytes: &[u8],
        size: usize,
        format: DataFormat,
        max_text: usize,
        count: &mut usize,
    ) -> MaskedData {
        let partial = bytes.len() < size;
        let (text, truncated) = match format {
            DataFormat::Json | DataFormat::String => {
                let json =
                    (format == DataFormat::Json && !partial && bytes.len() <= MASK_PARSE_LIMIT)
                        .then(|| self.json(bytes, count))
                        .flatten();
                let text =
                    json.unwrap_or_else(|| self.text(&String::from_utf8_lossy(bytes), count));
                let (text, cut_now) = cut(text, max_text);
                (text, cut_now || partial)
            }
            DataFormat::Hex => {
                let take = bytes.len().min(max_text / 2);
                let masked = self.binary(bytes, count);
                (hex::encode(&masked[..take]), take < size)
            }
            DataFormat::Base64 => {
                let take = bytes.len().min(max_text / 4 * 3);
                let masked = self.binary(bytes, count);
                (
                    base64::engine::general_purpose::STANDARD.encode(&masked[..take]),
                    take < size,
                )
            }
        };
        MaskedData {
            format,
            text,
            size,
            truncated,
        }
    }

    /// Значение заголовка: имя из списка — `***`, иначе текстовый режим.
    pub fn header(&self, name: &str, value: &str, count: &mut usize) -> String {
        if self.field_matches(name) {
            *count += 1;
            MASK.to_string()
        } else {
            self.text(value, count)
        }
    }
}

/// JSON с порядком членов объекта как в сообщении (у `serde_json::Value` без `preserve_order`
/// ключи сортируются, а включить фичу — значит поменять её и для приложения).
#[derive(Debug, Clone, PartialEq)]
enum Node {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<Node>),
    Object(Vec<(String, Node)>),
}

impl<'de> Deserialize<'de> for Node {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Node;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("JSON")
            }
            fn visit_unit<E>(self) -> Result<Node, E> {
                Ok(Node::Null)
            }
            fn visit_bool<E>(self, b: bool) -> Result<Node, E> {
                Ok(Node::Bool(b))
            }
            fn visit_i64<E>(self, n: i64) -> Result<Node, E> {
                Ok(Node::Number(n.into()))
            }
            fn visit_u64<E>(self, n: u64) -> Result<Node, E> {
                Ok(Node::Number(n.into()))
            }
            fn visit_f64<E: de::Error>(self, n: f64) -> Result<Node, E> {
                serde_json::Number::from_f64(n)
                    .map(Node::Number)
                    .ok_or_else(|| E::custom("not a finite number"))
            }
            fn visit_str<E>(self, s: &str) -> Result<Node, E> {
                Ok(Node::String(s.to_string()))
            }
            fn visit_string<E>(self, s: String) -> Result<Node, E> {
                Ok(Node::String(s))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Node, A::Error> {
                let mut items = Vec::new();
                while let Some(x) = a.next_element()? {
                    items.push(x);
                }
                Ok(Node::Array(items))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Node, A::Error> {
                let mut members = Vec::new();
                while let Some((k, v)) = a.next_entry()? {
                    members.push((k, v));
                }
                Ok(Node::Object(members))
            }
        }
        d.deserialize_any(V)
    }
}

impl Serialize for Node {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Node::Null => s.serialize_unit(),
            Node::Bool(b) => s.serialize_bool(*b),
            Node::Number(n) => n.serialize(s),
            Node::String(x) => s.serialize_str(x),
            Node::Array(items) => {
                let mut seq = s.serialize_seq(Some(items.len()))?;
                for i in items {
                    seq.serialize_element(i)?;
                }
                seq.end()
            }
            Node::Object(members) => {
                let mut map = s.serialize_map(Some(members.len()))?;
                for (k, v) in members {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn standard() -> Masker {
        Masker::new(&MaskConfig::default())
    }

    fn json(m: &Masker, s: &str) -> (String, usize) {
        let mut n = 0;
        let out = m.json(s.as_bytes(), &mut n).expect("valid JSON");
        (out, n)
    }

    fn text(m: &Masker, s: &str) -> (String, usize) {
        let mut n = 0;
        (m.text(s, &mut n), n)
    }

    /// Текстовый режим для набора случаев: в сообщении об ошибке — каждый несовпавший.
    fn texts(m: &Masker, cases: &[(&str, &str, usize)]) {
        let wrong: Vec<String> = cases
            .iter()
            .filter_map(|&(input, want, want_n)| {
                let (got, n) = text(m, input);
                (got != want || n != want_n)
                    .then(|| format!("{input:?} → {got:?} ({n}), ждали {want:?} ({want_n})"))
            })
            .collect();
        assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
    }

    #[test]
    fn names_are_normalized_and_matched_whole() {
        let m = standard();
        for name in [
            "api_key",
            "apiKey",
            "API-KEY",
            "userEmail",
            "e_mail",
            "cardNumber",
            "Set-Cookie",
            "dateOfBirth",
            "x.password.hash",
        ] {
            assert!(m.field_matches(name), "{name}");
        }
        for name in ["author", "discard", "panel", "id", "", "___"] {
            assert!(!m.field_matches(name), "{name}");
        }
        assert_eq!(normalize(" Api_Key-2.x "), "apikey2x");
        let own = Masker::new(&MaskConfig {
            fields: Some(vec!["order*id".into()]),
        });
        assert!(own.field_matches("order_customer_id"));
        assert!(
            !own.field_matches("email"),
            "свой список заменяет стандартный"
        );
    }

    #[test]
    fn glob_handles_stars_anywhere() {
        let g = |p: &str, n: &str| {
            glob(
                &p.chars().collect::<Vec<_>>(),
                &n.chars().collect::<Vec<_>>(),
            )
        };
        assert!(g("*", ""));
        assert!(g("a*b*c", "axxbyyc"));
        assert!(g("*token*", "token"));
        assert!(!g("a*b", "acb_"));
        assert!(!g("pan", "pans"));
    }

    #[test]
    fn json_fields_are_masked_at_any_depth_in_original_order() {
        let (out, n) = json(
            &standard(),
            r#"{"user":{"name":"Ann","email":"a@b.com"},"items":[{"cardNumber":"4111111111111111"}],"note":"write to x@y.org","password":{"a":1},"z":null}"#,
        );
        assert_eq!(
            out,
            r#"{"user":{"name":"Ann","email":"***"},"items":[{"cardNumber":"***"}],"note":"write to [email]","password":"***","z":null}"#
        );
        assert_eq!(n, 4);
        let (out, _) = json(&standard(), r#"{ "b" : 1 , "a" : [ true , 2.5 ] }"#);
        assert_eq!(
            out, r#"{"b":1,"a":[true,2.5]}"#,
            "компактно, порядок как в сообщении"
        );
        let (out, n) = json(&standard(), r#"{"ann@example.com":{"visits":3}}"#);
        assert_eq!((out.as_str(), n), (r#"{"[email]":{"visits":3}}"#, 1));
    }

    #[test]
    fn card_numbers_need_luhn_and_a_card_prefix() {
        let (out, n) = json(
            &standard(),
            r#"{"n":4111111111111111,"ts":1729771925123,"id":5555555555554444}"#,
        );
        assert_eq!(out, r#"{"n":"[card]","ts":1729771925123,"id":"[card]"}"#);
        assert_eq!(n, 2);
        let (out, n) = text(
            &standard(),
            "a 4111-1111-1111-1111 b 4111 1111 1111 1112 c 1729771925123 d 41111111111111111111111",
        );
        assert_eq!(
            out,
            "a [card] b 4111 1111 1111 1112 c 1729771925123 d 41111111111111111111111"
        );
        assert_eq!(n, 1);
    }

    #[test]
    fn jwt_and_bearer_tokens_are_replaced() {
        let (out, n) = text(
            &standard(),
            "got eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln then bearer abcdefgh12345",
        );
        assert_eq!(out, "got [jwt] then Bearer [token]");
        assert_eq!(n, 2);
        let (out, n) = text(&standard(), "Authorization: Bearer abcdefgh12345\nok");
        assert_eq!((out.as_str(), n), ("Authorization: ***\nok", 1));
    }

    #[test]
    fn text_pairs_from_the_list_are_masked() {
        let (out, n) = text(&standard(), "user=ann&password=hunter2&next=/home");
        assert_eq!((out.as_str(), n), ("user=ann&password=***&next=/home", 1));
        let (out, _) = text(&standard(), r#"{"password": "x y", "phone":'123', broken"#);
        assert_eq!(out, r#"{"password": "***", "phone":'***', broken"#);
        let (out, n) = text(&standard(), "Error: timeout at https://host:9092");
        assert_eq!(
            (out.as_str(), n),
            ("Error: timeout at https://host:9092", 0)
        );
    }

    #[test]
    fn binary_runs_keep_their_length() {
        let mut bytes = vec![0x00, 0x02];
        bytes.extend_from_slice(b"mail=ann@example.com");
        bytes.extend_from_slice(&[0xff, b'a', b'@', b'b']);
        let mut n = 0;
        let out = standard().binary(&bytes, &mut n);
        assert_eq!(out.len(), bytes.len());
        assert_eq!(&out[2..22], b"mail=***************");
        assert_eq!(
            &out[22..],
            &[0xff, b'a', b'@', b'b'],
            "короткий фрагмент не трогается"
        );
        assert_eq!(n, 1);
    }

    #[test]
    fn data_masks_before_truncation() {
        let m = standard();
        let mut n = 0;
        let d = m.data(
            b"ann@example.com wrote a long message",
            36,
            DataFormat::String,
            10,
            &mut n,
        );
        assert_eq!(
            (d.text.as_str(), d.truncated, d.size),
            ("[email] wr", true, 36)
        );
        assert_eq!(n, 1);
        let d = m.data(br#"{"a": 1}"#, 8, DataFormat::Json, 2048, &mut n);
        assert_eq!((d.text.as_str(), d.truncated), (r#"{"a":1}"#, false));
        // Значение длиннее разобранной части — текстовый режим, пары всё равно маскируются.
        let d = m.data(
            br#"{"password": "x", "tail": "#,
            100_000,
            DataFormat::Json,
            2048,
            &mut n,
        );
        assert_eq!(d.text, r#"{"password": "***", "tail": "#);
        assert!(d.truncated);
        let d = m.data(b"oops", 4, DataFormat::Json, 2048, &mut n);
        assert_eq!((d.text.as_str(), d.format), ("oops", DataFormat::Json));
    }

    #[test]
    fn hex_and_base64_encode_the_visible_prefix() {
        let m = standard();
        let mut n = 0;
        let d = m.data(b"ABCDEFGH", 8, DataFormat::Hex, 8, &mut n);
        assert_eq!((d.text.as_str(), d.truncated), ("41424344", true));
        let d = m.data(b"AB", 2, DataFormat::Base64, 2048, &mut n);
        assert_eq!((d.text.as_str(), d.truncated), ("QUI=", false));
        let d = m.data(b"to:ann@example.com", 18, DataFormat::Hex, 2048, &mut n);
        assert_eq!(hex::decode(&d.text).unwrap(), b"to:***************");
        // Маскируются исходные байты, потом обрезка: секрет на границе видимой части не виден.
        let jwt: &[u8] = b"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln";
        let cases: [(&[u8], DataFormat, usize, &[u8]); 3] = [
            (
                b"\x00\x01to:ann@example.com",
                DataFormat::Hex,
                22,
                b"\x00\x01to:******",
            ),
            (
                b"\x00\x01card=4111111111111111;",
                DataFormat::Hex,
                32,
                b"\x00\x01card=*********",
            ),
            (jwt, DataFormat::Base64, 40, &[b'*'; 30]),
        ];
        let wrong: Vec<String> = cases
            .iter()
            .filter_map(|&(bytes, format, max_text, want)| {
                let mut n = 0;
                let d = m.data(bytes, bytes.len(), format, max_text, &mut n);
                let got = match format {
                    DataFormat::Hex => hex::decode(&d.text).unwrap(),
                    _ => base64::engine::general_purpose::STANDARD
                        .decode(&d.text)
                        .unwrap(),
                };
                (got != want || !d.truncated || n != 1).then(|| {
                    format!(
                        "{:?} → {:?} ({n}, {})",
                        String::from_utf8_lossy(bytes),
                        String::from_utf8_lossy(&got),
                        d.truncated
                    )
                })
            })
            .collect();
        assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
    }

    #[test]
    fn cards_are_found_among_neighbouring_digit_groups() {
        texts(
            &standard(),
            &[
                ("4111 1111 1111 1111 12/25", "[card] 12/25", 1),
                ("4111111111111111 123", "[card] 123", 1),
                ("qty 2 4111111111111111", "qty 2 [card]", 1),
                ("1 4111111111111111 2025", "1 [card] 2025", 1),
                // Самое длинное окно с верной суммой при самом левом начале.
                ("4111 1111 1111 1111 003", "[card]", 1),
                ("5555555555554444 4111111111111111", "[card] [card]", 2),
                (
                    "2026-10-24 12:00 +7 912 345-67-89",
                    "2026-10-24 12:00 +7 912 345-67-89",
                    0,
                ),
            ],
        );
        let mut n = 0;
        let out = standard().binary(b"\x00pay 1 4111111111111111 2025\x00", &mut n);
        assert_eq!(
            (String::from_utf8_lossy(&out), n),
            ("\0pay 1 **************** 2025\0".into(), 1)
        );
    }

    #[test]
    fn pairs_inside_values_of_other_pairs_are_found() {
        texts(
            &standard(),
            &[
                ("note: password=hunter2", "note: password=***", 1),
                (
                    "url=https://x/?password=hunter2&a=1",
                    "url=https://x/?password=***&a=1",
                    1,
                ),
                ("redirect=/cb?token=abc123", "redirect=/cb?token=***", 1),
                // Значение без кавычек идёт до разделителя — закрывающая кавычка уходит с ним.
                (
                    r#"msg="see password=hunter2" ok"#,
                    r#"msg="see password=*** ok"#,
                    1,
                ),
            ],
        );
    }

    #[test]
    fn any_authorization_scheme_is_masked_with_its_credential() {
        texts(
            &standard(),
            &[
                (
                    "Authorization: Negotiate YIIabcdefSECRET",
                    "Authorization: ***",
                    1,
                ),
                (
                    "authorization=ApiKey s3cr3tKEY, next",
                    "authorization=***, next",
                    1,
                ),
                // Только в той же строке; у других имён следующее слово остаётся.
                (
                    "Authorization: Negotiate\nYIIabc",
                    "Authorization: ***\nYIIabc",
                    1,
                ),
                ("password=hunter2 user=ann", "password=*** user=ann", 1),
                ("X-Auth-Token: Bearer abcdefgh12345", "X-Auth-Token: ***", 1),
            ],
        );
        let own = Masker::new(&MaskConfig {
            fields: Some(vec!["*authorization".into()]),
        });
        texts(
            &own,
            &[(
                "Proxy-Authorization: Custom abc123\nok",
                "Proxy-Authorization: ***\nok",
                1,
            )],
        );
    }

    #[test]
    fn json_string_values_go_through_text_mode() {
        let (out, n) = json(
            &standard(),
            r#"{"log":"login user=ann password=hunter2","n":["token: abc"]}"#,
        );
        assert_eq!(
            (out.as_str(), n),
            (
                r#"{"log":"login user=ann password=***","n":["token: ***"]}"#,
                2
            )
        );
        // Имена членов — только шаблоны.
        let (out, n) = json(&standard(), r#"{"note=ann pwd=x":1}"#);
        assert_eq!((out.as_str(), n), (r#"{"note=ann pwd=x":1}"#, 0));
    }

    #[test]
    fn headers_by_name_or_text() {
        let m = standard();
        let mut n = 0;
        assert_eq!(
            m.header("Authorization", "Basic YWxhZGRpbjpvcGVu", &mut n),
            MASK
        );
        assert_eq!(
            m.header("x-trace", "mail me at a@b.com", &mut n),
            "mail me at [email]"
        );
        assert_eq!(n, 2);
    }

    #[test]
    fn empty_own_list_keeps_only_patterns() {
        let m = Masker::new(&MaskConfig {
            fields: Some(vec![]),
        });
        let (out, n) = json(&m, r#"{"password":"x","mail":"a@b.com"}"#);
        assert_eq!(
            (out.as_str(), n),
            (r#"{"password":"x","mail":"[email]"}"#, 1)
        );
    }
}
