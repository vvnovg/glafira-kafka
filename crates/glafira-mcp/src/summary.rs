//! Обзор окна без содержимого (спец. G4 §3.3): корзины по времени, частые ключи, размеры,
//! метки не по порядку. Приложение наполняет [`SummaryBuilder`] во время просмотра; ключи в ответе
//! маскирует крейт.

use std::collections::{BTreeMap, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};

use serde::Serialize;

use crate::mask::DataFormat;
use crate::messages::{ScanStats, TimestampType};

pub const DEFAULT_TOP_KEYS: u32 = 20;
pub const MAX_TOP_KEYS: u32 = 100;
/// Шаг корзин по умолчанию — наименьший, при котором `to − from` не больше `TARGET_BUCKETS` шагов
/// (окно включительное: «последний час» — шаг минута; с круглым началом корзин выходит на одну
/// больше).
pub const TARGET_BUCKETS: i64 = 60;
pub const MAX_BUCKETS: i64 = 1000;
pub const BUCKET_STEPS_S: [i64; 11] = [1, 5, 10, 30, 60, 300, 900, 1800, 3600, 21600, 86400];
pub const MAX_BUCKET_SECONDS: u32 = 86400;
/// Сколько байтов ключа хранится для показа в `top`.
pub const KEY_PREFIX_BYTES: usize = 256;
/// Сколько разных ключей считается точно; дальше `distinctCapped: true` (память ограничена).
pub const MAX_TRACKED_KEYS: usize = 100_000;
pub const MAX_HEADER_NAMES: usize = 50;
const MAX_TRACKED_HEADER_NAMES: usize = 1000;

/// Начало первой корзины: круглое время не позже `from_ms`.
pub fn bucket_origin(from_ms: i64, bucket_ms: i64) -> i64 {
    from_ms - from_ms.rem_euclid(bucket_ms)
}

pub fn bucket_count(from_ms: i64, to_ms: i64, bucket_ms: i64) -> i64 {
    (to_ms - bucket_origin(from_ms, bucket_ms)) / bucket_ms + 1
}

pub fn default_bucket_ms(from_ms: i64, to_ms: i64) -> i64 {
    BUCKET_STEPS_S
        .iter()
        .map(|s| s * 1000)
        .find(|ms| to_ms - from_ms <= TARGET_BUCKETS * ms)
        .unwrap_or(86_400_000)
}

/// Прошедшее фильтры сообщение глазами обзора: без значения.
pub struct SummaryRecord<'a> {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub timestamp_type: TimestampType,
    /// Ключ целиком и его формат; `None` — ключа нет.
    pub key: Option<(&'a [u8], DataFormat)>,
    pub value_size: usize,
    pub header_names: &'a [&'a str],
}

struct KeyStat {
    count: u64,
    prefix: Vec<u8>,
    size: usize,
    format: DataFormat,
    first: u64,
}

#[derive(Default)]
struct PartStat {
    matched: u64,
    first_ts: Option<i64>,
    last_ts: Option<i64>,
    /// Метка предыдущей записи партиции — для `outOfOrder`.
    prev_ts: Option<i64>,
}

pub struct SummaryBuilder {
    origin: i64,
    bucket_ms: i64,
    top_keys: usize,
    buckets: Vec<u64>,
    partitions: BTreeMap<i32, PartStat>,
    keys: HashMap<u64, KeyStat>,
    distinct_capped: bool,
    null_keys: u64,
    seen: u64,
    size_min: Option<usize>,
    size_max: usize,
    size_sum: u128,
    out_of_order: u64,
    first_out_of_order: Option<(i32, i64)>,
    create: u64,
    log_append: u64,
    header_names: HashMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopKey {
    /// Первые [`KEY_PREFIX_BYTES`] байтов ключа.
    pub bytes: Vec<u8>,
    pub size: usize,
    pub format: DataFormat,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSummary {
    pub partition: i32,
    pub matched: u64,
    pub first_ts: Option<i64>,
    pub last_ts: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Sizes {
    pub min_bytes: usize,
    pub avg_bytes: usize,
    pub max_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    pub stats: ScanStats,
    pub partitions: Vec<PartitionSummary>,
    pub bucket_ms: i64,
    /// (начало корзины, число сообщений) — все корзины окна, включая пустые.
    pub buckets: Vec<(i64, u64)>,
    pub distinct_keys: u64,
    pub distinct_capped: bool,
    pub null_keys: u64,
    pub top_keys: Vec<TopKey>,
    pub sizes: Option<Sizes>,
    pub out_of_order: u64,
    pub first_out_of_order: Option<(i32, i64)>,
    pub create: u64,
    pub log_append: u64,
    pub header_names: Vec<(String, u64)>,
}

fn hash_key(bytes: &[u8]) -> u64 {
    let mut h = DefaultHasher::new();
    bytes.hash(&mut h);
    h.finish()
}

impl SummaryBuilder {
    pub fn new(from_ms: i64, to_ms: i64, bucket_ms: i64, top_keys: usize) -> Self {
        let n = bucket_count(from_ms, to_ms, bucket_ms).clamp(1, MAX_BUCKETS) as usize;
        Self {
            origin: bucket_origin(from_ms, bucket_ms),
            bucket_ms,
            top_keys,
            buckets: vec![0; n],
            partitions: BTreeMap::new(),
            keys: HashMap::new(),
            distinct_capped: false,
            null_keys: 0,
            seen: 0,
            size_min: None,
            size_max: 0,
            size_sum: 0,
            out_of_order: 0,
            first_out_of_order: None,
            create: 0,
            log_append: 0,
            header_names: HashMap::new(),
        }
    }

    /// Записи одной партиции приходят по возрастанию офсета.
    pub fn add(&mut self, r: &SummaryRecord<'_>) {
        self.seen += 1;
        let p = self.partitions.entry(r.partition).or_default();
        p.matched += 1;
        if let Some(ts) = r.timestamp {
            if p.prev_ts.is_some_and(|prev| ts < prev) {
                self.out_of_order += 1;
                self.first_out_of_order
                    .get_or_insert((r.partition, r.offset));
            }
            p.prev_ts = Some(ts);
            p.first_ts = Some(p.first_ts.map_or(ts, |f| f.min(ts)));
            p.last_ts = Some(p.last_ts.map_or(ts, |l| l.max(ts)));
            let i = (ts - self.origin).div_euclid(self.bucket_ms);
            if let Some(b) = usize::try_from(i)
                .ok()
                .and_then(|i| self.buckets.get_mut(i))
            {
                *b += 1;
            }
        }
        match r.timestamp_type {
            TimestampType::Create => self.create += 1,
            TimestampType::LogAppend => self.log_append += 1,
            TimestampType::Unknown => {}
        }
        match r.key {
            None => self.null_keys += 1,
            Some((bytes, format)) => {
                let h = hash_key(bytes);
                let seen = self.seen;
                if let Some(k) = self.keys.get_mut(&h) {
                    k.count += 1;
                } else if self.keys.len() < MAX_TRACKED_KEYS {
                    self.keys.insert(
                        h,
                        KeyStat {
                            count: 1,
                            prefix: bytes[..bytes.len().min(KEY_PREFIX_BYTES)].to_vec(),
                            size: bytes.len(),
                            format,
                            first: seen,
                        },
                    );
                } else {
                    self.distinct_capped = true;
                }
            }
        }
        self.size_min = Some(self.size_min.map_or(r.value_size, |m| m.min(r.value_size)));
        self.size_max = self.size_max.max(r.value_size);
        self.size_sum += r.value_size as u128;
        for name in r.header_names {
            if let Some(c) = self.header_names.get_mut(*name) {
                *c += 1;
            } else if self.header_names.len() < MAX_TRACKED_HEADER_NAMES {
                self.header_names.insert((*name).to_string(), 1);
            }
        }
    }

    pub fn finish(self, stats: ScanStats) -> Summary {
        let mut keys: Vec<KeyStat> = self.keys.into_values().collect();
        keys.sort_by(|a, b| b.count.cmp(&a.count).then(a.first.cmp(&b.first)));
        let distinct_keys = keys.len() as u64;
        let top_keys = keys
            .into_iter()
            .take(self.top_keys)
            .map(|k| TopKey {
                bytes: k.prefix,
                size: k.size,
                format: k.format,
                count: k.count,
            })
            .collect();
        let mut header_names: Vec<(String, u64)> = self.header_names.into_iter().collect();
        header_names.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        header_names.truncate(MAX_HEADER_NAMES);
        Summary {
            stats,
            partitions: self
                .partitions
                .into_iter()
                .map(|(partition, p)| PartitionSummary {
                    partition,
                    matched: p.matched,
                    first_ts: p.first_ts,
                    last_ts: p.last_ts,
                })
                .collect(),
            bucket_ms: self.bucket_ms,
            buckets: self
                .buckets
                .iter()
                .enumerate()
                .map(|(i, c)| (self.origin + i as i64 * self.bucket_ms, *c))
                .collect(),
            distinct_keys,
            distinct_capped: self.distinct_capped,
            null_keys: self.null_keys,
            top_keys,
            sizes: self.size_min.map(|min| Sizes {
                min_bytes: min,
                avg_bytes: (self.size_sum / u128::from(self.seen.max(1))) as usize,
                max_bytes: self.size_max,
            }),
            out_of_order: self.out_of_order,
            first_out_of_order: self.first_out_of_order,
            create: self.create,
            log_append: self.log_append,
            header_names,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec<'a>(
        partition: i32,
        offset: i64,
        ts: i64,
        key: Option<&'a [u8]>,
        size: usize,
        headers: &'a [&'a str],
    ) -> SummaryRecord<'a> {
        SummaryRecord {
            partition,
            offset,
            timestamp: Some(ts),
            timestamp_type: TimestampType::Create,
            key: key.map(|k| (k, DataFormat::String)),
            value_size: size,
            header_names: headers,
        }
    }

    #[test]
    fn bucket_step_is_round_and_fits() {
        // 10 минут → шаг 10 с (60 корзин), начало — круглое.
        let from = 1_729_771_925_123;
        let to = from + 600_000 - 1;
        assert_eq!(default_bucket_ms(from, to), 10_000);
        assert_eq!(bucket_origin(from, 10_000), 1_729_771_920_000);
        assert_eq!(bucket_count(from, to, 10_000), 61);
        assert_eq!(default_bucket_ms(0, 59_999), 1000);
        assert_eq!(
            default_bucket_ms(0, 400 * 86_400_000),
            86_400_000,
            "дальше суток не растёт"
        );
    }

    #[test]
    fn inclusive_window_fits_the_default_step() {
        // «Последний час»: to − from = 3 600 000 → шаг 60 с, а не 300 с.
        assert_eq!(default_bucket_ms(0, 3_600_000), 60_000);
        assert_eq!(bucket_count(0, 3_600_000, 60_000), 61);
        // На шаг больше — следующий шаг.
        assert_eq!(default_bucket_ms(0, 3_600_001), 300_000);
        assert_eq!(default_bucket_ms(0, 60_000), 1000);
    }

    #[test]
    fn out_of_order_is_compared_with_the_previous_record() {
        let mut b = SummaryBuilder::new(0, 29_999, 10_000, 2);
        // Одна запись «из будущего» не делает неупорядоченными все следующие.
        for (offset, ts) in [(20, 10_000), (21, 5_000), (22, 6_000), (23, 7_000)] {
            b.add(&rec(0, offset, ts, None, 1, &[]));
        }
        // Равная метка — не нарушение; другая партиция считается отдельно.
        b.add(&rec(1, 0, 9_000, None, 1, &[]));
        b.add(&rec(1, 1, 9_000, None, 1, &[]));
        b.add(&rec(1, 2, 9_500, None, 1, &[]));
        let s = b.finish(ScanStats::default());
        assert_eq!(s.out_of_order, 1);
        assert_eq!(s.first_out_of_order, Some((0, 21)));
        assert_eq!(
            (s.partitions[0].first_ts, s.partitions[0].last_ts),
            (Some(5_000), Some(10_000))
        );
    }

    #[test]
    fn counts_keys_buckets_sizes_and_disorder() {
        let mut b = SummaryBuilder::new(0, 29_999, 10_000, 2);
        let h: &[&str] = &["trace-id"];
        b.add(&rec(0, 10, 1_000, Some(b"k1"), 10, h));
        b.add(&rec(0, 11, 12_000, Some(b"k2"), 30, h));
        b.add(&rec(0, 12, 5_000, Some(b"k1"), 20, &[]));
        b.add(&rec(1, 3, 25_000, None, 0, h));
        b.add(&rec(1, 4, 26_000, Some(b"k3"), 40, &["trace-id", "type"]));
        let s = b.finish(ScanStats {
            matched: 5,
            ..Default::default()
        });
        assert_eq!(s.buckets, [(0, 2), (10_000, 1), (20_000, 2)]);
        assert_eq!(
            (s.distinct_keys, s.null_keys, s.distinct_capped),
            (3, 1, false)
        );
        assert_eq!(
            s.top_keys
                .iter()
                .map(|k| (k.bytes.as_slice(), k.count))
                .collect::<Vec<_>>(),
            [(&b"k1"[..], 2), (&b"k2"[..], 1)]
        );
        assert_eq!(s.out_of_order, 1);
        assert_eq!(s.first_out_of_order, Some((0, 12)));
        assert_eq!(
            s.sizes,
            Some(Sizes {
                min_bytes: 0,
                avg_bytes: 20,
                max_bytes: 40
            })
        );
        assert_eq!(
            s.header_names,
            [("trace-id".to_string(), 4), ("type".to_string(), 1)]
        );
        assert_eq!(
            s.partitions,
            [
                PartitionSummary {
                    partition: 0,
                    matched: 3,
                    first_ts: Some(1_000),
                    last_ts: Some(12_000)
                },
                PartitionSummary {
                    partition: 1,
                    matched: 2,
                    first_ts: Some(25_000),
                    last_ts: Some(26_000)
                },
            ]
        );
        assert_eq!(s.create, 5);
    }

    #[test]
    fn long_keys_keep_a_prefix_and_count_by_the_whole_key() {
        let mut b = SummaryBuilder::new(0, 999, 1000, 5);
        let a = vec![b'a'; 300];
        let mut c = a.clone();
        c[299] = b'c';
        b.add(&rec(0, 0, 1, Some(&a), 1, &[]));
        b.add(&rec(0, 1, 2, Some(&c), 1, &[]));
        let s = b.finish(ScanStats::default());
        assert_eq!(s.distinct_keys, 2, "различаются по полному ключу");
        assert_eq!(s.top_keys[0].bytes.len(), KEY_PREFIX_BYTES);
        assert_eq!(s.top_keys[0].size, 300);
    }

    #[test]
    fn empty_window_has_zero_buckets_and_no_sizes() {
        let s = SummaryBuilder::new(0, 2_999, 1000, 5).finish(ScanStats::default());
        assert_eq!(s.buckets, [(0, 0), (1000, 0), (2000, 0)]);
        assert_eq!(s.sizes, None);
        assert!(s.top_keys.is_empty());
    }
}
