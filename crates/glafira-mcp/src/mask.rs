//! Маскировка содержимого сообщений перед ответом LLM (спец. G4 §5): список имён полей
//! подключения и встроенные шаблоны (email, номер карты, JWT, Bearer-токен). Чистые функции:
//! всё, что уходит модели из содержимого кластера, проходит через [`Masker`].

use std::fmt;
use std::sync::LazyLock;

use base64::Engine;
use regex::{Captures, Match, Regex};
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};

/// Замена значения поля из списка.
pub const MASK: &str = "***";
/// Сколько первых байтов значения разбирается как JSON; длиннее — текстовый режим.
pub const MASK_PARSE_LIMIT: usize = 256 * 1024;
/// Сколько первых байтов ключа приложение передаёт на маскировку: ключ в ответе короткий
/// (`MAX_KEY_TEXT`), а ключ длиннее маскируется как частичный, текстом.
pub const KEY_PARSE_LIMIT: usize = 16 * 1024;
/// Пределы списка полей подключения (проверяет приложение при сохранении).
pub const MAX_MASK_FIELDS: usize = 200;
pub const MAX_MASK_FIELD_CHARS: usize = 64;
/// Печатный фрагмент двоичных данных, к которому применяются шаблоны.
pub const MIN_PRINTABLE_RUN: usize = 6;

/// Стандартный список (`llmMaskFields = None`): обновляется вместе с Glafira. Форма показывает тот же список
/// (`STANDARD_MASK_FIELDS` во фронтенде), совпадение проверяет тест приложения.
pub const STANDARD_FIELDS: [&str; 22] = [
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
    "proxyauthorization",
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
/// Серия групп цифр (кандидаты в номер карты): между группами одиночный пробел или дефис; по
/// краям — не цифра (буква рядом не мешает: `id4111…`).
static DIGIT_RUN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[0-9]+(?:[ -][0-9]+)*").expect("digit run regex"));
/// Серия групп цифр по границам слова, как до G4.1. Число, приклеенное к слову (`ab12`), входит в
/// серию [`DIGIT_RUN`], и карта в редкой группировке рядом с ним по всей серии уже не находится;
/// здесь такое число в серию не входит (`ab12 41111 11111 111111`). Что маскировалось до G4.1,
/// маскируется и теперь.
static WORD_DIGIT_RUN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b[0-9]+(?:[ -][0-9]+)*\b").expect("word digit run regex"));
/// Имя пары `name=value`, `name: value`, `"name": value` в тексте вместе с разделителем.
static PAIR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?:"(?P<n1>[^"\\]{1,64})"|'(?P<n2>[^'\\]{1,64})'|(?P<n3>[\p{L}_][\p{L}\p{N}_.\-]*))\s*[:=]\s*"#,
    )
    .expect("pair regex")
});
/// Значение пары: строка в кавычках целиком или до `,`, `;`, `&`, `}`, `]` или пробела.
static VALUE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"^(?:"(?:[^"\\]|\\.)*"|'[^']*'|[^\s,;&}\]]+)"#).expect("value regex")
});
/// Слово учётных данных после схемы авторизации — и на следующей строке.
static CREDENTIAL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\s+[^\s,;&}\]]+").expect("credential regex"));
/// Разделитель пары сразу за словом: слово — имя следующей пары.
static NEXT_PAIR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\s*[:=]").expect("next pair regex"));
/// Слово начинается с пары `name=value`.
static GLUED_PAIR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[\p{L}_][\p{L}\p{N}_.\-]*=[^=]").expect("glued pair regex"));
/// Слово — имя пары с `=` в конце (`password=`).
static OPEN_PAIR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[\p{L}_][\p{L}\p{N}_.\-]*=$").expect("open pair regex"));
/// За словом в той же строке после пробелов стоит значение.
static SPACED_VALUE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[ \t]+[^\s]").expect("spaced value regex"));

/// Схемы авторизации, после которых учётные данные берутся следующим словом.
const SCHEMES: [&str; 4] = ["bearer", "basic", "digest", "token"];

fn is_scheme(value: &str) -> bool {
    SCHEMES.iter().any(|s| value.eq_ignore_ascii_case(s))
}

/// Имена, значение которых без кавычек маскируется до конца строки: куки и заголовки
/// авторизации (`Cookie: a=1; b=2`, `Authorization: Digest username="u", response="…"`).
fn masks_line(name: &str) -> bool {
    let n = normalize(name);
    n == "cookie" || n == "setcookie" || n.ends_with("authorization")
}

/// Длина строки `s` до `\n` или `\r` (или до конца).
fn line_len(s: &str) -> usize {
    s.find(['\n', '\r']).unwrap_or(s.len())
}

/// Сколько байтов `rest` занимают учётные данные, если значение пары — схема `Bearer`, `Basic`,
/// `Digest` или `Token` (иначе они остались бы за `***`); токен может быть и на следующей строке.
/// Учётные данные — слово целиком (`user:p4ss`, `abc@x`, `a=b`), но не имя следующей пары: слово
/// с `:` в конце или перед `:`/`=` (`access_token: x`), а на следующей строке и слово,
/// начинающееся с `name=value` (`user=ann`), и слово `name=` со значением после пробелов
/// (`password= x`); в той же строке такие слова — параметр схемы или base64 с `=`.
fn credential_len(value: &str, rest: &str) -> usize {
    if !is_scheme(value) {
        return 0;
    }
    let Some(cred) = CREDENTIAL.find(rest) else {
        return 0;
    };
    let word = cred.as_str().trim_start();
    let after = &rest[cred.end()..];
    let next_line = !cred.as_str()[..cred.len() - word.len()]
        .bytes()
        .all(|b| b == b' ' || b == b'\t');
    let next_pair =
        GLUED_PAIR.is_match(word) || (OPEN_PAIR.is_match(word) && SPACED_VALUE.is_match(after));
    if word.ends_with(':') || NEXT_PAIR.is_match(after) || (next_line && next_pair) {
        0
    } else {
        cred.end()
    }
}

/// ASCII-цифры номера карты: 13–19 цифр, проверка Луна и первая цифра 2–6 (Мир, Visa,
/// Mastercard, Amex, Discover). Метки времени в миллисекундах до 2033 года начинаются с 1 — они
/// не номера карт.
fn is_card(digits: &[u8]) -> bool {
    if !(13..=19).contains(&digits.len()) || !(b'2'..=b'6').contains(&digits[0]) {
        return false;
    }
    let sum: u32 = digits
        .iter()
        .rev()
        .enumerate()
        .map(|(i, &b)| {
            let d = u32::from(b - b'0');
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

/// Длины групп окна в форме номера карты: одна группа из 13–19 цифр, 4-4-4-4 с необязательной
/// последней группой из 1–3 цифр, 4-6-5 (Amex), 4-6-4 (Diners), 4-4-4-3, 4-3-3-3, 6-13, 8-8.
fn card_shaped(lens: &[usize]) -> bool {
    match lens {
        [n] => (13..=19).contains(n),
        [4, 4, 4, 4] | [4, 6, 5] | [4, 6, 4] | [4, 4, 4, 3] | [4, 3, 3, 3] | [6, 13] | [8, 8] => {
            true
        }
        [4, 4, 4, 4, n] => (1..=3).contains(n),
        _ => false,
    }
}

/// Номера карт (диапазоны байтов) в одной серии групп цифр: вся серия из 13–19 цифр при любой
/// группировке и все окна из подряд идущих целых групп в форме номера карты ([`card_shaped`]),
/// прошедшие [`is_card`]. Диапазоны могут пересекаться.
fn series_spans(run: Match<'_>) -> Vec<(usize, usize)> {
    let mut spans: Vec<(usize, usize)> = Vec::new();
    if run.len() < 13 {
        return spans;
    }
    let digits: Vec<u8> = run.as_str().bytes().filter(u8::is_ascii_digit).collect();
    // Группы: начало и конец в тексте, начало в `digits`.
    let mut groups: Vec<(usize, usize, usize)> = Vec::new();
    let mut start = run.start();
    for (i, b) in run.as_str().bytes().enumerate() {
        if !b.is_ascii_digit() {
            groups.push((start, run.start() + i, start - run.start() - groups.len()));
            start = run.start() + i + 1;
        }
    }
    groups.push((start, run.end(), start - run.start() - groups.len()));
    let lens: Vec<usize> = groups.iter().map(|&(s, e, _)| e - s).collect();
    // Вся серия из 13–19 цифр — при любой группировке.
    if is_card(&digits) {
        spans.push((run.start(), run.end()));
    }
    for (i, &(first, _, from)) in groups.iter().enumerate() {
        for (j, &(_, end, at)) in groups.iter().enumerate().skip(i).take(5) {
            if card_shaped(&lens[i..=j]) && is_card(&digits[from..at + lens[j]]) {
                spans.push((first, end));
            }
        }
    }
    spans
}

/// Номера карт (диапазоны байтов) среди групп цифр: [`series_spans`] в сериях по границам цифр
/// ([`DIGIT_RUN`]) и по границам слова ([`WORD_DIGIT_RUN`]); пересекающиеся диапазоны
/// объединяются в один. Соседние числа (`… 12/25`, `qty 2 …`, `630000 4111 …`) остаются на виду и
/// не мешают, списки чисел (серии длиннее) картой не считаются, а группа не делится: 23 цифры
/// подряд — не карта.
fn card_spans(text: &str) -> Vec<(usize, usize)> {
    let mut found: Vec<(usize, usize)> = DIGIT_RUN
        .find_iter(text)
        .chain(WORD_DIGIT_RUN.find_iter(text))
        .flat_map(series_spans)
        .collect();
    found.sort_unstable();
    let mut spans: Vec<(usize, usize)> = Vec::new();
    for (start, end) in found {
        // По возрастанию начала: пересекается только с последним диапазоном.
        match spans.last_mut() {
            Some((_, last)) if *last > start => *last = (*last).max(end),
            _ => spans.push((start, end)),
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

/// Цифры модуля целого значения числа JSON: у отрицательного — без знака; у числа с дробной
/// частью или экспонентой — если значение целое и по модулю меньше 10¹⁹.
fn integer_digits(n: &serde_json::Number) -> Option<String> {
    if let Some(u) = n.as_u64() {
        return Some(u.to_string());
    }
    if let Some(i) = n.as_i64() {
        return Some(i.unsigned_abs().to_string());
    }
    let f = n.as_f64()?.abs();
    (f.fract() == 0.0 && f < 1e19).then(|| (f as u64).to_string())
}

/// Обрезает текст до `max` байтов по границе символа; true — обрезан.
pub(crate) fn cut_utf8(mut s: String, max: usize) -> (String, bool) {
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

    /// Текстовый режим: значения пар `name=value` из списка → `***`, затем шаблоны. У кук и
    /// заголовков авторизации ([`masks_line`]) значение без кавычек — до конца строки. Значение
    /// пары с именем не из списка просматривается дальше — в нём бывают свои пары
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
                None if masks_line(name) => {
                    out.push_str(MASK);
                    let end = head + line_len(&s[head..]);
                    let rest_blank = s[pos..end].bytes().all(|b| b == b' ' || b == b'\t');
                    pos = end;
                    if rest_blank {
                        pos += credential_len(v.as_str(), &s[pos..]);
                    }
                }
                None => {
                    out.push_str(MASK);
                    pos += credential_len(v.as_str(), &s[pos..]);
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
                if integer_digits(n).is_some_and(|d| is_card(d.as_bytes())) {
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
                let (text, cut_now) = cut_utf8(text, max_text);
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
                // Число перед картой не оставляет на виду её хвост и само не маскируется.
                ("6 4111 1111 1111 1111", "6 [card]", 1),
                ("22 4111 1111 1111 1111", "22 [card]", 1),
                ("30 4111 1111 1111 1111", "30 [card]", 1),
                ("20000 4111 1111 1111 1111", "20000 [card]", 1),
                ("20004 4111 1111 1111 1111", "20004 [card]", 1),
                ("630000 4111 1111 1111 1111", "630000 [card]", 1),
                // Окна только в форме номера карты: Amex 4-6-5, Diners 4-6-4; списки чисел — не карты.
                ("amex 3782 822463 10005", "amex [card]", 1),
                ("diners 3056 930902 5904", "diners [card]", 1),
                // Отдельная серия из 13–19 цифр — карта при любой группировке.
                ("4111 1111 1111 116", "[card]", 1),
                ("4222 222 222 222", "[card]", 1),
                ("622126 1234567890129", "[card]", 1),
                ("41 11 11 11 11 11 11 11", "[card]", 1),
                ("411 111 111 111 1111", "[card]", 1),
                ("4111 1111 11111111", "[card]", 1),
                ("41111111 11111111", "[card]", 1),
                (
                    "scores 3 1 4 1 5 9 2 6 5 3 5 8 9 7 9 3 2 3 8 4 6",
                    "scores 3 1 4 1 5 9 2 6 5 3 5 8 9 7 9 3 2 3 8 4 6",
                    0,
                ),
                (
                    "ids 304 512 618 220 431 559 602 377 248 316",
                    "ids 304 512 618 220 431 559 602 377 248 316",
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
        let mut n = 0;
        let out = standard().binary(b"\x00630000 4111 1111 1111 1111\x00", &mut n);
        assert_eq!(
            (String::from_utf8_lossy(&out), n),
            (format!("\0630000 {}\0", "*".repeat(19)).into(), 1)
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
                // У `…authorization` — до конца строки, вместе с соседними парами.
                (
                    "authorization=ApiKey s3cr3tKEY, next",
                    "authorization=***",
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
                // Известная схема — токен берётся и со следующей строки, как раньше.
                (
                    "Authorization: Bearer\nabcdefgh12345\nok",
                    "Authorization: ***\nok",
                    1,
                ),
                ("token: Bearer\nabcdefgh12345", "token: ***", 1),
                (
                    "Authorization: Basic dXNlcjpwYXNzd29yZA==\nok",
                    "Authorization: ***\nok",
                    1,
                ),
                // Имя следующей пары — не токен: её значение маскируется само.
                (
                    "token_type: Bearer\naccess_token: abc123def456opaque",
                    "token_type: ***\naccess_token: ***",
                    2,
                ),
                (
                    "token_type: Bearer\nrefresh_token: tGzv3JOkF0XG5Qx2TlKWIA",
                    "token_type: ***\nrefresh_token: ***",
                    2,
                ),
                (
                    "X-Token-Type: Bearer\nAuthorization: s3cr3tvalue",
                    "X-Token-Type: ***\nAuthorization: ***",
                    2,
                ),
                (
                    "pwd: basic\npassword: hunter2",
                    "pwd: ***\npassword: ***",
                    2,
                ),
                (
                    "authorization=abc password: hunter2",
                    "authorization=***",
                    1,
                ),
                ("token: Bearer\nuser=ann", "token: ***\nuser=ann", 1),
                // Токен — слово целиком: с `:`, `@` и `=` внутри.
                (
                    "Authorization: Basic dXNlcg== from 10.0.0.1",
                    "Authorization: ***",
                    1,
                ),
                (
                    "Authorization: Basic user:p4ssw0rd",
                    "Authorization: ***",
                    1,
                ),
                (
                    "Authorization: Basic alice:hunter2 from 10.0.0.1",
                    "Authorization: ***",
                    1,
                ),
                ("token: Token id42:s3cr3tKEY", "token: ***", 1),
                (
                    "Authorization: ApiKey key-id:s3cr3t",
                    "Authorization: ***",
                    1,
                ),
                (
                    "Authorization: Bearer abcdefgh=ijklmnop",
                    "Authorization: ***",
                    1,
                ),
                (
                    "Authorization: Bearer abc@d3fs3cr3t",
                    "Authorization: ***",
                    1,
                ),
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
    fn cookies_and_authorization_are_masked_to_the_end_of_the_line() {
        texts(
            &standard(),
            &[
                ("Cookie: a=1; sessionid=xyz", "Cookie: ***", 1),
                ("cookie: a=1\nuser=ann", "cookie: ***\nuser=ann", 1),
                (
                    r#"Authorization: Digest username="bob", realm="r", response="abc""#,
                    "Authorization: ***",
                    1,
                ),
                (
                    "Authorization: AWS4-HMAC-SHA256 Credential=AKIA/2026/s3, SignedHeaders=host, Signature=abc",
                    "Authorization: ***",
                    1,
                ),
                ("Authorization: Bearer abc:", "Authorization: ***", 1),
                (
                    "Authorization: Bearer\n  abcdef12345",
                    "Authorization: ***",
                    1,
                ),
                // Значение в кавычках — по-прежнему в пределах кавычек.
                (
                    r#"Cookie: "a=1; b=2" user=ann"#,
                    r#"Cookie: "***" user=ann"#,
                    1,
                ),
                (
                    "set-cookie: sid=1; Path=/\r\nok",
                    "set-cookie: ***\r\nok",
                    1,
                ),
            ],
        );
        let (out, n) = json(&standard(), r#"{"h":"Set-Cookie: sid=1; Path=/"}"#);
        assert_eq!((out.as_str(), n), (r#"{"h":"Set-Cookie: ***"}"#, 1));
    }

    #[test]
    fn next_line_pair_with_a_spaced_value_is_not_a_credential() {
        texts(
            &standard(),
            &[
                (
                    "pwd: basic\npassword= hunter2",
                    "pwd: ***\npassword= ***",
                    2,
                ),
                // В той же строке слово с `=` в конце — учётные данные (base64 с выравниванием).
                ("X-Auth-Token: Basic dXNlcjpwYXM=", "X-Auth-Token: ***", 1),
                // На следующей строке без значения после `=` — тоже учётные данные.
                ("token: Basic\ndXNlcjpwYXM=", "token: ***", 1),
            ],
        );
    }

    #[test]
    fn pair_names_may_be_in_any_script() {
        let own = Masker::new(&MaskConfig {
            fields: Some(vec!["пароль".into(), "token".into()]),
        });
        texts(
            &own,
            &[
                ("пароль=x", "пароль=***", 1),
                ("логин=ann Пароль: секрет", "логин=ann Пароль: ***", 1),
                ("token: Basic\nпароль= x", "token: ***\nпароль= ***", 2),
            ],
        );
    }

    #[test]
    fn cards_glued_to_words_and_rare_groupings_are_found() {
        texts(
            &standard(),
            &[
                ("id4111111111111111", "id[card]", 1),
                ("x_4111111111111111", "x_[card]", 1),
                ("4111111111111111abc", "[card]abc", 1),
                // Окна новых форм рядом с числом: 4-4-4-3, 4-3-3-3, 6-13, 8-8.
                ("qty 2 4111 1111 1111 116", "qty 2 [card]", 1),
                ("qty 2 4222 222 222 222", "qty 2 [card]", 1),
                ("qty 2 411111 1111111111110", "qty 2 [card]", 1),
                ("qty 2 41111111 11111111", "qty 2 [card]", 1),
                // Без верной суммы Луна приклеенные цифры остаются.
                ("id4111111111111112", "id4111111111111112", 0),
                // Число, приклеенное к слову, входит в серию по границам цифр; карта в редкой
                // группировке рядом с ним находится по границам слова, как до G4.1.
                ("ab12 41111 11111 111111", "ab12 [card]", 1),
                ("v2 4111-111111111111", "v2 [card]", 1),
                ("A7-411111111 1111111", "A7-[card]", 1),
                ("41111 11111 111111 9x", "[card] 9x", 1),
            ],
        );
    }

    #[test]
    fn negative_and_float_json_card_numbers_are_masked() {
        let m = standard();
        for (input, want, want_n) in [
            (r#"{"n":-4111111111111111}"#, r#"{"n":"[card]"}"#, 1),
            (r#"{"n":4.111111111111111e15}"#, r#"{"n":"[card]"}"#, 1),
            (r#"{"n":4111111111111111.0}"#, r#"{"n":"[card]"}"#, 1),
            (r#"{"n":-12,"m":1.5}"#, r#"{"n":-12,"m":1.5}"#, 0),
        ] {
            assert_eq!(json(&m, input), (want.to_string(), want_n), "{input}");
        }
    }

    #[test]
    fn json_longer_than_the_parse_limit_is_masked_as_text() {
        let m = standard();
        let head = br#"{"password":1,"pad":""#;
        let mut whole = head.to_vec();
        whole.resize(MASK_PARSE_LIMIT + 10, b'a');
        whole.extend(br#""}"#);
        // Целиком передано, но длиннее предела разбора.
        let mut n = 0;
        let d = m.data(&whole, whole.len(), DataFormat::Json, 64, &mut n);
        assert!(
            d.text.starts_with(r#"{"password":***,"pad":"aaa"#),
            "{}",
            d.text
        );
        assert_eq!((d.truncated, d.size, n), (true, whole.len(), 1));
        // Передан только предел разбора.
        let mut n = 0;
        let d = m.data(
            &whole[..MASK_PARSE_LIMIT],
            whole.len(),
            DataFormat::Json,
            64,
            &mut n,
        );
        assert!(
            d.text.starts_with(r#"{"password":***,"pad":"aaa"#),
            "{}",
            d.text
        );
        assert_eq!((d.truncated, n), (true, 1));
    }

    #[test]
    fn proxy_authorization_is_in_the_standard_list() {
        let m = standard();
        texts(
            &m,
            &[(
                "Proxy-Authorization: Basic dXNlcjpwNHNz\nok",
                "Proxy-Authorization: ***\nok",
                1,
            )],
        );
        let (out, n) = json(
            &m,
            r#"{"Proxy-Authorization":"Basic dXNlcjpwNHNz","via":"proxy"}"#,
        );
        assert_eq!(
            (out.as_str(), n),
            (r#"{"Proxy-Authorization":"***","via":"proxy"}"#, 1)
        );
        let mut n = 0;
        assert_eq!(
            m.header("Proxy-Authorization", "Basic dXNlcjpwNHNz", &mut n),
            MASK
        );
        assert_eq!(n, 1);
        // Отдельное имя, а не `*authorization`: другие имена с этим окончанием в список не попадают.
        assert!(!m.field_matches("preauthorization"));
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
