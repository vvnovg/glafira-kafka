use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use serde::Deserialize;
use serde_json::Value;

use crate::error::{AppError, AppResult};
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
    pub message: Result<EncodedMessage, String>,
}

#[derive(Deserialize)]
struct JsonlRecord {
    #[serde(default)]
    key: Option<Value>,
    #[serde(default)]
    value: Option<Value>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    partition: Option<i32>,
}

fn json_bytes(v: Option<Value>) -> Option<Vec<u8>> {
    match v {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.into_bytes()),
        Some(other) => Some(other.to_string().into_bytes()),
    }
}

fn check_partition(p: Option<i32>) -> Result<Option<i32>, String> {
    match p {
        Some(n) if n < 0 => Err(format!("partition {n} — отрицательный номер")),
        other => Ok(other),
    }
}

pub fn parse_jsonl_line(line: &str, topic: &str) -> Result<EncodedMessage, String> {
    let v: Value = serde_json::from_str(line).map_err(|e| format!("невалидный JSON: {e}"))?;
    if !v.is_object() {
        return Err("невалидный JSON: ожидался объект".to_string());
    }
    let r: JsonlRecord = serde_json::from_value(v).map_err(|e| format!("невалидный JSON: {e}"))?;
    Ok(EncodedMessage {
        topic: topic.to_string(),
        partition: check_partition(r.partition)?,
        key: json_bytes(r.key),
        value: json_bytes(r.value),
        headers: r
            .headers
            .into_iter()
            .map(|(k, v)| (k, v.into_bytes()))
            .collect(),
    })
}

pub fn read_jsonl<R: BufRead + Send + 'static>(
    reader: R,
    topic: String,
) -> impl Iterator<Item = SourceItem> + Send {
    reader.lines().enumerate().filter_map(move |(i, line)| {
        let line_no = i as u64 + 1;
        let line = line.map(|l| match l.strip_prefix('\u{feff}') {
            Some(rest) if i == 0 => rest.to_string(),
            _ => l,
        });
        match line {
            Ok(l) if l.trim().is_empty() => None,
            Ok(l) => Some(SourceItem {
                line: line_no,
                message: parse_jsonl_line(&l, &topic),
            }),
            Err(e) => Some(SourceItem {
                line: line_no,
                message: Err(format!("ошибка чтения: {e}")),
            }),
        }
    })
}

pub fn read_csv<R: Read + Send + 'static>(
    reader: R,
    topic: String,
) -> Result<impl Iterator<Item = SourceItem> + Send, String> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(reader);
    let headers = rdr.headers().map_err(|e| format!("CSV: {e}"))?.clone();
    let col = |name: &str| headers.iter().position(|h| h.trim() == name);
    let value_col =
        col("value").ok_or_else(|| "CSV: нет обязательной колонки value".to_string())?;
    let key_col = col("key");
    let partition_col = col("partition");
    let header_cols: Vec<(usize, String)> = headers
        .iter()
        .enumerate()
        .filter_map(|(i, h)| h.trim().strip_prefix("header.").map(|n| (i, n.to_string())))
        .collect();
    Ok(rdr.into_records().map(move |rec| {
        let rec = match rec {
            Ok(r) => r,
            Err(e) => {
                let line = e.position().map_or(0, |p| p.line());
                return SourceItem {
                    line,
                    message: Err(format!("CSV: {e}")),
                };
            }
        };
        let line = rec.position().map_or(0, |p| p.line());
        let field = |i: usize| rec.get(i).unwrap_or("");
        let message = (|| {
            let partition = match partition_col.map(field).map(str::trim) {
                None | Some("") => None,
                Some(s) => Some(
                    s.parse::<i32>()
                        .map_err(|_| format!("partition «{s}» — не число"))?,
                ),
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
        SourceItem { line, message }
    }))
}

pub fn open_file(
    path: &Path,
    format: FileFormat,
    topic: &str,
) -> AppResult<Box<dyn Iterator<Item = SourceItem> + Send>> {
    let file = File::open(path)
        .map_err(|e| AppError::invalid(format!("Не удалось открыть {}: {e}", path.display())))?;
    Ok(match format {
        FileFormat::Jsonl => Box::new(read_jsonl(BufReader::new(file), topic.to_string())),
        FileFormat::Csv => {
            Box::new(read_csv(BufReader::new(file), topic.to_string()).map_err(AppError::invalid)?)
        }
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
        assert!(parse_jsonl_line("{oops", "t").unwrap_err().contains("JSON"));
        assert!(parse_jsonl_line(r#"{"value":"v","partition":-1}"#, "t").is_err());
        assert!(parse_jsonl_line(r#"[1,2]"#, "t").is_err());
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
        assert!(open_file(&dir.path().join("none"), FileFormat::Csv, "t").is_err());
    }

    #[test]
    fn csv_errors() {
        assert!(
            read_csv(Cursor::new("key\nk".to_string()), "t".into())
                .err()
                .unwrap()
                .contains("value")
        );
        let items: Vec<SourceItem> = read_csv(
            Cursor::new("value,partition\nv,abc\n".to_string()),
            "t".into(),
        )
        .unwrap()
        .collect();
        assert!(items[0].message.as_ref().unwrap_err().contains("partition"));
    }
}
