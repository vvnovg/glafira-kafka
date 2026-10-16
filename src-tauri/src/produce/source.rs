use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use serde::Deserialize;
use serde_json::value::RawValue;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::i18n::LocalizedText;
use crate::produce::payload::EncodedMessage;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    Jsonl,
    Csv,
}

#[derive(Debug)]
pub struct SourceItem {
    pub line: u64,
    pub message: Result<EncodedMessage, LocalizedText>,
}

/// Максимальный размер одной строки/записи входного файла.
const MAX_LINE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Deserialize)]
struct JsonlRecord {
    #[serde(default)]
    key: Option<Box<RawValue>>,
    #[serde(default)]
    value: Option<Box<RawValue>>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    partition: Option<i32>,
}

/// Строка — её содержимое; `null`/нет поля — ничего; остальное — исходный текст из файла как есть.
fn invalid_json(error: impl std::fmt::Display) -> LocalizedText {
    ltext!(
        "errors.source.invalidJson",
        "invalid JSON: {{error}}",
        error = error
    )
}

fn json_bytes(v: Option<Box<RawValue>>) -> Result<Option<Vec<u8>>, LocalizedText> {
    let Some(raw) = v else { return Ok(None) };
    let text = raw.get();
    if text == "null" {
        Ok(None)
    } else if text.starts_with('"') {
        let s: String = serde_json::from_str(text).map_err(invalid_json)?;
        Ok(Some(s.into_bytes()))
    } else {
        Ok(Some(text.as_bytes().to_vec()))
    }
}

fn check_partition(p: Option<i32>) -> Result<Option<i32>, LocalizedText> {
    match p {
        Some(n) if n < 0 => Err(ltext!(
            "errors.source.partitionNegative",
            "partition {{partition}} is a negative number",
            partition = n
        )),
        other => Ok(other),
    }
}

pub fn parse_jsonl_line(line: &str, topic: &str) -> Result<EncodedMessage, LocalizedText> {
    if !line.trim_start().starts_with('{') {
        return Err(ltext!(
            "errors.source.objectExpected",
            "invalid JSON: an object was expected"
        ));
    }
    let r: JsonlRecord = serde_json::from_str(line).map_err(invalid_json)?;
    Ok(EncodedMessage {
        topic: topic.to_string(),
        partition: check_partition(r.partition)?,
        key: json_bytes(r.key)?,
        value: json_bytes(r.value)?,
        headers: r
            .headers
            .into_iter()
            .map(|(k, v)| (k, v.into_bytes()))
            .collect(),
    })
}

enum RawLine {
    Line(Vec<u8>),
    TooLong,
}

/// Читает строку, не накапливая больше `cap` байт; хвост слишком длинной строки отбрасывается.
fn read_line_capped<R: BufRead>(r: &mut R, cap: usize) -> std::io::Result<Option<RawLine>> {
    let mut buf = Vec::new();
    let mut too_long = false;
    let mut any = false;
    loop {
        let chunk = r.fill_buf()?;
        if chunk.is_empty() {
            break;
        }
        any = true;
        let (take, done) = match chunk.iter().position(|&b| b == b'\n') {
            Some(i) => (i + 1, true),
            None => (chunk.len(), false),
        };
        if !too_long {
            if buf.len() + take > cap + 1 {
                too_long = true;
                buf = Vec::new();
            } else {
                buf.extend_from_slice(&chunk[..take]);
            }
        }
        r.consume(take);
        if done {
            break;
        }
    }
    if !any {
        return Ok(None);
    }
    if too_long {
        return Ok(Some(RawLine::TooLong));
    }
    if buf.last() == Some(&b'\n') {
        buf.pop();
    }
    Ok(Some(RawLine::Line(buf)))
}

fn too_long_msg(line: u64) -> LocalizedText {
    ltext!(
        "errors.source.lineTooLong",
        "Line {{line}} is longer than 16 MiB",
        line = line
    )
}

pub fn read_jsonl<R: BufRead + Send + 'static>(
    reader: R,
    topic: String,
) -> impl Iterator<Item = SourceItem> + Send {
    read_jsonl_capped(reader, topic, MAX_LINE_BYTES)
}

fn read_jsonl_capped<R: BufRead + Send + 'static>(
    mut reader: R,
    topic: String,
    cap: usize,
) -> impl Iterator<Item = SourceItem> + Send {
    let mut line_no = 0u64;
    let mut failed = false;
    std::iter::from_fn(move || {
        loop {
            if failed {
                return None;
            }
            line_no += 1;
            let message = match read_line_capped(&mut reader, cap) {
                Ok(None) => return None,
                Err(e) => {
                    failed = true;
                    Err(ltext!(
                        "errors.source.readFailed",
                        "read error: {{error}}",
                        error = e
                    ))
                }
                Ok(Some(RawLine::TooLong)) => Err(too_long_msg(line_no)),
                Ok(Some(RawLine::Line(mut bytes))) => {
                    if line_no == 1 && bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
                        bytes.drain(..3);
                    }
                    match std::str::from_utf8(&bytes) {
                        Ok(l) if l.trim().is_empty() => continue,
                        Ok(l) => parse_jsonl_line(l, &topic),
                        Err(_) => Err(ltext!(
                            "errors.source.notUtf8",
                            "the line is not UTF-8 encoded"
                        )),
                    }
                }
            };
            return Some(SourceItem {
                line: line_no,
                message,
            });
        }
    })
}

/// Колонки: обязательная `value`; необязательные `key`, `partition`, `header.<имя>`.
/// Пустое значение `value` — пустая нагрузка (не tombstone): tombstone через CSV отправить нельзя.
pub fn read_csv<R: Read + Send + 'static>(
    reader: R,
    topic: String,
) -> Result<impl Iterator<Item = SourceItem> + Send, LocalizedText> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(reader);
    let headers = rdr.headers().map_err(csv_error)?.clone();
    let col = |name: &str| headers.iter().position(|h| h.trim() == name);
    let value_col = col("value").ok_or_else(|| {
        ltext!(
            "errors.source.csvNoValue",
            "CSV: the required value column is missing"
        )
    })?;
    for name in ["key", "value", "partition"] {
        if headers.iter().filter(|h| h.trim() == name).count() > 1 {
            return Err(ltext!(
                "errors.source.csvDuplicateColumn",
                "CSV: column {{column}} is specified more than once",
                column = name
            ));
        }
    }
    let key_col = col("key");
    let partition_col = col("partition");
    let mut header_cols: Vec<(usize, String)> = Vec::new();
    for (i, h) in headers.iter().enumerate() {
        if let Some(n) = h.trim().strip_prefix("header.") {
            if n.is_empty() {
                return Err(ltext!(
                    "errors.source.csvEmptyHeader",
                    "CSV: empty header name in the header. column"
                ));
            }
            if header_cols.iter().any(|(_, x)| x == n) {
                return Err(ltext!(
                    "errors.source.csvDuplicateHeader",
                    "CSV: header header.{{name}} is specified more than once",
                    name = n
                ));
            }
            header_cols.push((i, n.to_string()));
        }
    }
    let mut records = rdr.into_records();
    let mut failed = false;
    Ok(std::iter::from_fn(move || {
        if failed {
            return None;
        }
        let rec = records.next()?;
        let rec = match rec {
            Ok(r) if r.as_slice().len() > MAX_LINE_BYTES => {
                let line = r.position().map_or(0, |p| p.line());
                return Some(SourceItem {
                    line,
                    message: Err(too_long_msg(line)),
                });
            }
            Ok(r) => r,
            Err(e) => {
                failed = e.is_io_error();
                let line = e.position().map_or(0, |p| p.line());
                return Some(SourceItem {
                    line,
                    message: Err(csv_error(e)),
                });
            }
        };
        let line = rec.position().map_or(0, |p| p.line());
        let field = |i: usize| rec.get(i).unwrap_or("");
        let message = (|| {
            let partition = match partition_col.map(field).map(str::trim) {
                None | Some("") => None,
                Some(s) => Some(s.parse::<i32>().map_err(|_| {
                    ltext!(
                        "errors.source.partitionNotNumber",
                        "partition “{{value}}” is not a number",
                        value = s
                    )
                })?),
            };
            Ok(EncodedMessage {
                topic: topic.clone(),
                partition: check_partition(partition)?,
                key: key_col
                    .map(field)
                    .filter(|k| !k.is_empty())
                    .map(|k| k.as_bytes().to_vec()),
                value: Some(field(value_col).as_bytes().to_vec()),
                headers: header_cols
                    .iter()
                    .filter(|(i, _)| !field(*i).is_empty())
                    .map(|(i, n)| (n.clone(), field(*i).as_bytes().to_vec()))
                    .collect(),
            })
        })();
        Some(SourceItem { line, message })
    }))
}

fn csv_error(e: csv::Error) -> LocalizedText {
    ltext!("errors.source.csv", "CSV: {{error}}", error = e)
}

pub fn open_file(
    path: &Path,
    format: FileFormat,
    topic: &str,
) -> AppResult<Box<dyn Iterator<Item = SourceItem> + Send>> {
    let file = File::open(path).map_err(|e| {
        app_err!(
            ErrorCode::InvalidInput,
            "errors.source.openFailed",
            "Failed to open {{path}}: {{error}}",
            path = path.display(),
            error = e
        )
    })?;
    Ok(match format {
        FileFormat::Jsonl => Box::new(read_jsonl(BufReader::new(file), topic.to_string())),
        FileFormat::Csv => Box::new(
            read_csv(BufReader::new(file), topic.to_string())
                .map_err(|t| AppError::localized(ErrorCode::InvalidInput, t))?,
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn jsonl_line_variants() {
        let m = parse_jsonl_line(
            r#"{"key":"k1","value":{"n":1},"headers":{"trace":"t1"},"partition":2}"#,
            "t",
        )
        .unwrap();
        assert_eq!(m.topic, "t");
        assert_eq!(m.key.as_deref(), Some(&b"k1"[..]));
        assert_eq!(m.value.as_deref(), Some(&br#"{"n":1}"#[..]));
        assert_eq!(m.headers, vec![("trace".to_string(), b"t1".to_vec())]);
        assert_eq!(m.partition, Some(2));
        let m = parse_jsonl_line(r#"{"value":"plain"}"#, "t").unwrap();
        assert_eq!(
            (m.key, m.value.as_deref(), m.partition),
            (None, Some(&b"plain"[..]), None)
        );
        let m = parse_jsonl_line(r#"{"key":null,"value":42}"#, "t").unwrap();
        assert_eq!(m.value.as_deref(), Some(&b"42"[..]));
        assert_eq!(
            parse_jsonl_line("{oops", "t").unwrap_err().key,
            "errors.source.invalidJson"
        );
        assert_eq!(
            parse_jsonl_line(r#"{"value":"v","partition":-1}"#, "t")
                .unwrap_err()
                .key,
            "errors.source.partitionNegative"
        );
        assert_eq!(
            parse_jsonl_line(r#"[1,2]"#, "t").unwrap_err().key,
            "errors.source.objectExpected"
        );
    }

    #[test]
    fn jsonl_reader_numbers_lines_and_skips_blank() {
        let text = "{\"value\":\"a\"}\n\n{oops\n{\"value\":\"b\"}\n";
        let items: Vec<SourceItem> =
            read_jsonl(Cursor::new(text.to_string()), "t".into()).collect();
        assert_eq!(items.iter().map(|i| i.line).collect::<Vec<_>>(), [1, 3, 4]);
        assert!(items[0].message.is_ok() && items[1].message.is_err() && items[2].message.is_ok());
    }

    #[test]
    fn csv_reader_columns() {
        let text = "key,value,partition,header.trace\nk1,v1,0,t1\n,v2,,\n";
        let items: Vec<SourceItem> = read_csv(Cursor::new(text.to_string()), "t".into())
            .unwrap()
            .collect();
        assert_eq!(items.len(), 2);
        let a = items[0].message.as_ref().unwrap();
        assert_eq!(items[0].line, 2);
        assert_eq!(
            (a.key.as_deref(), a.value.as_deref(), a.partition),
            (Some(&b"k1"[..]), Some(&b"v1"[..]), Some(0))
        );
        assert_eq!(a.headers, vec![("trace".to_string(), b"t1".to_vec())]);
        let b = items[1].message.as_ref().unwrap();
        assert_eq!((b.key.as_deref(), b.partition), (None, None));
        assert!(
            b.headers.is_empty(),
            "пустое значение заголовка не добавляется"
        );
    }

    #[test]
    fn bom_and_quoting() {
        let items: Vec<SourceItem> = read_jsonl(
            Cursor::new("\u{feff}{\"value\":\"a\"}\n".to_string()),
            "t".into(),
        )
        .collect();
        assert!(items[0].message.is_ok());
        let text = "\u{feff}key,value\n\"a,b\",\"x\"\"y\nz\"\n";
        let items: Vec<SourceItem> = read_csv(Cursor::new(text.to_string()), "t".into())
            .unwrap()
            .collect();
        let m = items[0].message.as_ref().unwrap();
        assert_eq!(
            (m.key.as_deref(), m.value.as_deref()),
            (Some(&b"a,b"[..]), Some(&b"x\"y\nz"[..]))
        );
    }

    #[test]
    fn open_file_reads_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.jsonl");
        std::fs::write(&p, "{\"value\":\"a\"}\n").unwrap();
        assert_eq!(open_file(&p, FileFormat::Jsonl, "t").unwrap().count(), 1);
        let e = open_file(&dir.path().join("none"), FileFormat::Csv, "t")
            .err()
            .unwrap();
        assert_eq!(e.key, "errors.source.openFailed");
    }

    struct ErrReader;
    impl Read for ErrReader {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("boom"))
        }
    }
    impl BufRead for ErrReader {
        fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
            Err(std::io::Error::other("boom"))
        }
        fn consume(&mut self, _: usize) {}
    }

    #[test]
    fn io_error_ends_iteration() {
        let mut it = read_jsonl(ErrReader, "t".into());
        assert!(it.next().unwrap().message.is_err());
        assert!(it.next().is_none());
        let r = Cursor::new(b"value\nv\n".to_vec()).chain(ErrReader);
        let items: Vec<SourceItem> = read_csv(r, "t".into()).unwrap().collect();
        assert_eq!(items.len(), 2);
        assert!(items[0].message.is_ok() && items[1].message.is_err());
    }

    #[test]
    fn raw_json_passthrough() {
        let m = parse_jsonl_line(r#"{"key":[3, 1],"value":{"b":1,"a":2.50}}"#, "t").unwrap();
        assert_eq!(m.value.as_deref(), Some(&br#"{"b":1,"a":2.50}"#[..]));
        assert_eq!(m.key.as_deref(), Some(&b"[3, 1]"[..]));
        let m = parse_jsonl_line(r#"{"value":"a\nb"}"#, "t").unwrap();
        assert_eq!(m.value.as_deref(), Some(&b"a\nb"[..]));
    }

    #[test]
    fn long_line_is_per_line_error() {
        let text = "{\"value\":\"aaaaaaaaaaaaaaaaaaaa\"}\n{\"value\":\"b\"}\n";
        let items: Vec<SourceItem> =
            read_jsonl_capped(Cursor::new(text.to_string()), "t".into(), 16).collect();
        assert_eq!(items.len(), 2);
        let e = items[0].message.as_ref().unwrap_err();
        assert_eq!(e.key, "errors.source.lineTooLong");
        assert_eq!(e.params["line"], "1");
        assert!(items[1].message.is_ok());
        assert_eq!(items[1].line, 2);
    }

    #[test]
    fn csv_header_validation() {
        let e = |t: &str| {
            read_csv(Cursor::new(t.to_string()), "t".into())
                .err()
                .unwrap()
        };
        let dup = |t: &str, column: &str| {
            let e = e(t);
            assert_eq!(e.key, "errors.source.csvDuplicateColumn", "{t}");
            assert_eq!(e.params["column"], column, "{t}");
        };
        dup("value,value\n", "value");
        dup("key,value,key\n", "key");
        dup("value,partition,partition\n", "partition");
        let h = e("value,header.a,header.a\n");
        assert_eq!(h.key, "errors.source.csvDuplicateHeader");
        assert_eq!(h.params["name"], "a");
        assert_eq!(e("value,header.\n").key, "errors.source.csvEmptyHeader");
    }

    #[test]
    fn csv_errors() {
        assert_eq!(
            read_csv(Cursor::new("key\nk".to_string()), "t".into())
                .err()
                .unwrap()
                .key,
            "errors.source.csvNoValue"
        );
        let items: Vec<SourceItem> = read_csv(
            Cursor::new("value,partition\nv,abc\n".to_string()),
            "t".into(),
        )
        .unwrap()
        .collect();
        let e = items[0].message.as_ref().unwrap_err();
        assert_eq!(e.key, "errors.source.partitionNotNumber");
        assert_eq!(e.params["value"], "abc");
    }
}
