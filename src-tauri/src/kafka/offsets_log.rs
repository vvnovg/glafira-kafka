//! Записи `__consumer_offsets` (спец. G4 §6.3): коммиты офсетов групп и партиция, где координатор
//! хранит группу. Чистые функции над байтами — формат описан в `OffsetCommitKey.json` и
//! `OffsetCommitValue.json` брокера Kafka.

pub const OFFSETS_TOPIC: &str = "__consumer_offsets";

/// Значение коммита офсета.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitValue {
    pub offset: i64,
    pub metadata: String,
    pub commit_timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OffsetsRecord {
    /// Коммит офсета (ключ версии 0 или 1); `value: None` — tombstone, офсет удалён.
    Commit {
        group: String,
        topic: String,
        partition: i32,
        value: Option<CommitValue>,
    },
    /// Прочие записи координатора: метаданные групп (ключ 2), записи протокола KIP-848 (3+).
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    Truncated,
    BadString,
    UnknownVersion(i16),
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.pos.checked_add(n).ok_or(DecodeError::Truncated)?;
        let s = self.buf.get(self.pos..end).ok_or(DecodeError::Truncated)?;
        self.pos = end;
        Ok(s)
    }

    fn i16(&mut self) -> Result<i16, DecodeError> {
        Ok(i16::from_be_bytes(
            self.take(2)?.try_into().expect("2 bytes"),
        ))
    }

    fn i32(&mut self) -> Result<i32, DecodeError> {
        Ok(i32::from_be_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_be_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    fn uvarint(&mut self) -> Result<u64, DecodeError> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.take(1)?[0];
            v |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err(DecodeError::Truncated)
    }

    fn utf8(&mut self, len: usize) -> Result<String, DecodeError> {
        String::from_utf8(self.take(len)?.to_vec()).map_err(|_| DecodeError::BadString)
    }

    /// Строка с длиной `int16`; `-1` (null) — пустая.
    fn string(&mut self) -> Result<String, DecodeError> {
        match self.i16()? {
            n if n < 0 => Ok(String::new()),
            n => self.utf8(n as usize),
        }
    }

    /// Compact string: unsigned varint длина + 1; `0` (null) — пустая.
    fn compact_string(&mut self) -> Result<String, DecodeError> {
        match self.uvarint()? {
            0 => Ok(String::new()),
            n => self.utf8(usize::try_from(n - 1).map_err(|_| DecodeError::Truncated)?),
        }
    }

    /// Tagged fields flexible-версий: пропускаются.
    fn skip_tagged_fields(&mut self) -> Result<(), DecodeError> {
        for _ in 0..self.uvarint()? {
            self.uvarint()?;
            let size = usize::try_from(self.uvarint()?).map_err(|_| DecodeError::Truncated)?;
            self.take(size)?;
        }
        Ok(())
    }
}

fn decode_value(value: &[u8]) -> Result<CommitValue, DecodeError> {
    let mut r = Reader::new(value);
    let version = r.i16()?;
    let offset = r.i64()?;
    let (metadata, commit_timestamp) = match version {
        0 | 2 => (r.string()?, r.i64()?),
        1 => {
            let v = (r.string()?, r.i64()?);
            r.i64()?; // expireTimestamp
            v
        }
        3 => {
            r.i32()?; // leaderEpoch
            (r.string()?, r.i64()?)
        }
        4 => {
            r.i32()?;
            let v = (r.compact_string()?, r.i64()?);
            r.skip_tagged_fields()?;
            v
        }
        v => return Err(DecodeError::UnknownVersion(v)),
    };
    Ok(CommitValue {
        offset,
        metadata,
        commit_timestamp,
    })
}

/// Запись `__consumer_offsets`: ключ и значение (`None` — tombstone).
pub fn decode(key: &[u8], value: Option<&[u8]>) -> Result<OffsetsRecord, DecodeError> {
    let mut r = Reader::new(key);
    match r.i16()? {
        0 | 1 => {
            let group = r.string()?;
            let topic = r.string()?;
            let partition = r.i32()?;
            Ok(OffsetsRecord::Commit {
                group,
                topic,
                partition,
                value: value.map(decode_value).transpose()?,
            })
        }
        v if v >= 2 => Ok(OffsetsRecord::Other),
        v => Err(DecodeError::UnknownVersion(v)),
    }
}

/// Java `String.hashCode`: по кодовым единицам UTF-16, с переполнением `i32`.
fn java_hash(s: &str) -> i32 {
    s.encode_utf16()
        .fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(i32::from(c)))
}

/// Партиция `__consumer_offsets` группы — как `Utils.abs(groupId.hashCode()) % n` в Kafka.
pub fn partition_for(group: &str, partitions: i32) -> i32 {
    let h = java_hash(group);
    let abs = if h == i32::MIN { 0 } else { h.abs() };
    abs % partitions.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s16(s: &str) -> Vec<u8> {
        let mut v = (s.len() as i16).to_be_bytes().to_vec();
        v.extend_from_slice(s.as_bytes());
        v
    }

    fn key(version: i16, group: &str, topic: &str, partition: i32) -> Vec<u8> {
        let mut v = version.to_be_bytes().to_vec();
        v.extend(s16(group));
        v.extend(s16(topic));
        v.extend(partition.to_be_bytes());
        v
    }

    /// Unsigned varint, как в compact-строках Kafka.
    fn uvarint(mut n: u64) -> Vec<u8> {
        let mut v = Vec::new();
        while n >= 0x80 {
            v.push((n as u8 & 0x7f) | 0x80);
            n >>= 7;
        }
        v.push(n as u8);
        v
    }

    fn value(version: i16, offset: i64, metadata: &str, ts: i64) -> Vec<u8> {
        let mut v = version.to_be_bytes().to_vec();
        v.extend(offset.to_be_bytes());
        match version {
            0 | 2 => {
                v.extend(s16(metadata));
                v.extend(ts.to_be_bytes());
            }
            1 => {
                v.extend(s16(metadata));
                v.extend(ts.to_be_bytes());
                v.extend((-1i64).to_be_bytes());
            }
            3 => {
                v.extend(7i32.to_be_bytes());
                v.extend(s16(metadata));
                v.extend(ts.to_be_bytes());
            }
            4 => {
                v.extend(7i32.to_be_bytes());
                v.extend(uvarint(metadata.len() as u64 + 1));
                v.extend_from_slice(metadata.as_bytes());
                v.extend(ts.to_be_bytes());
                // Один tagged field (topicId, tag 0): 16 байтов UUID.
                v.extend([1, 0, 16]);
                v.extend([0xab; 16]);
            }
            _ => unreachable!(),
        }
        v
    }

    #[test]
    fn every_value_version_is_decoded() {
        for version in 0..=4 {
            let r = decode(
                &key(1, "g", "orders", 3),
                Some(&value(version, 42, "m", 1_000)),
            )
            .unwrap();
            assert_eq!(
                r,
                OffsetsRecord::Commit {
                    group: "g".into(),
                    topic: "orders".into(),
                    partition: 3,
                    value: Some(CommitValue {
                        offset: 42,
                        metadata: "m".into(),
                        commit_timestamp: 1_000
                    }),
                },
                "v{version}"
            );
        }
        let r = decode(&key(0, "g", "t", 0), Some(&value(4, 1, "", 5))).unwrap();
        assert!(
            matches!(r, OffsetsRecord::Commit { value: Some(CommitValue { ref metadata, .. }), .. } if metadata.is_empty())
        );
    }

    #[test]
    fn v4_metadata_length_can_take_several_varint_bytes() {
        let metadata = "m".repeat(200);
        let v = value(4, 42, &metadata, 1_000);
        // Длина compact-строки 200 + 1 = 201 — два байта varint после version, offset и leaderEpoch.
        assert_eq!(v[14..16], [0xC9, 0x01]);
        assert_eq!(
            decode(&key(1, "g", "t", 0), Some(&v)).unwrap(),
            OffsetsRecord::Commit {
                group: "g".into(),
                topic: "t".into(),
                partition: 0,
                value: Some(CommitValue {
                    offset: 42,
                    metadata,
                    commit_timestamp: 1_000
                }),
            }
        );
    }

    #[test]
    fn tombstones_other_records_and_errors() {
        assert_eq!(
            decode(&key(1, "g", "t", 0), None).unwrap(),
            OffsetsRecord::Commit {
                group: "g".into(),
                topic: "t".into(),
                partition: 0,
                value: None
            }
        );
        let mut group_key = 2i16.to_be_bytes().to_vec();
        group_key.extend(s16("g"));
        assert_eq!(
            decode(&group_key, Some(b"anything")).unwrap(),
            OffsetsRecord::Other
        );
        assert_eq!(decode(&[0, 5, 1], None).unwrap(), OffsetsRecord::Other);
        assert_eq!(
            decode(&[0xff, 0xff], None),
            Err(DecodeError::UnknownVersion(-1))
        );
        let mut bad = value(3, 1, "m", 1);
        bad[1] = 9;
        assert_eq!(
            decode(&key(1, "g", "t", 0), Some(&bad)),
            Err(DecodeError::UnknownVersion(9))
        );
        let cut = value(3, 1, "meta", 1);
        assert_eq!(
            decode(&key(1, "g", "t", 0), Some(&cut[..cut.len() - 3])),
            Err(DecodeError::Truncated)
        );
        // v4, срез внутри 16 байтов tagged field.
        let cut = value(4, 1, "meta", 1);
        assert_eq!(
            decode(&key(1, "g", "t", 0), Some(&cut[..cut.len() - 5])),
            Err(DecodeError::Truncated)
        );
        assert_eq!(decode(&[0, 1, 0], None), Err(DecodeError::Truncated));
        let mut not_utf8 = 1i16.to_be_bytes().to_vec();
        not_utf8.extend([0, 1, 0xff]);
        assert_eq!(decode(&not_utf8, None), Err(DecodeError::BadString));
    }

    #[test]
    fn partition_matches_kafka() {
        // Значения Java String.hashCode, посчитанные независимо.
        assert_eq!(java_hash("hello"), 99_162_322);
        assert_eq!(java_hash("группа"), 1_749_938_458);
        assert_eq!(java_hash("g😀"), 1_871_882);
        assert_eq!(partition_for("hello", 50), 22);
        assert_eq!(partition_for("группа", 50), 8);
        assert_eq!(partition_for("order-service", 50), 26);
        // Отрицательный hashCode: модуль до деления (остаток со знаком дал бы −12, rem_euclid — 38).
        assert_eq!(java_hash("my-group"), -1_906_497_762);
        assert_eq!(partition_for("my-group", 50), 12);
        // hashCode == Integer.MIN_VALUE: Utils.abs даёт 0.
        assert_eq!(java_hash("polygenelubricants"), i32::MIN);
        assert_eq!(partition_for("polygenelubricants", 50), 0);
        assert_eq!(partition_for("", 50), 0);
        assert_eq!(
            partition_for("hello", 0),
            0,
            "без партиций не делим на ноль"
        );
    }
}
