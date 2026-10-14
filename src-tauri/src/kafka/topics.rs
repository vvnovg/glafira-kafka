use std::collections::{HashMap, HashSet};
use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaResult;
use rdkafka::{Offset, TopicPartitionList};
use serde::Serialize;

use crate::error::{AppError, AppResult};
use crate::kafka::context::CapturingContext;
use crate::kafka::metadata::{convert, TopicMeta};
use crate::kafka::pool::KafkaHandles;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicSummary {
    pub name: String,
    pub internal: bool,
    pub partition_count: usize,
    pub replication_factor: usize,
    pub under_replicated: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicStats {
    pub name: String,
    pub message_count: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionDetails {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub low: i64,
    pub high: i64,
    pub message_count: i64,
    pub under_replicated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicDetails {
    pub name: String,
    pub internal: bool,
    pub replication_factor: usize,
    pub message_count: i64,
    pub partitions: Vec<PartitionDetails>,
}

pub fn summarize_topics(topics: Vec<TopicMeta>) -> Vec<TopicSummary> {
    let mut out: Vec<TopicSummary> = topics
        .into_iter()
        .map(|t| TopicSummary {
            internal: t.internal(),
            partition_count: t.partitions.len(),
            replication_factor: t.replication_factor(),
            under_replicated: t.partitions.iter().filter(|p| p.under_replicated()).count(),
            name: t.name,
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

pub fn list_topics(h: &KafkaHandles) -> AppResult<Vec<TopicSummary>> {
    let md = h.consumer.fetch_metadata(None, h.timeout).map_err(|e| h.kafka_err(e))?;
    Ok(summarize_topics(convert(&md).1))
}

/// Приблизительное число сообщений: high − low (компакция и транзакционные
/// маркеры делают реальное число меньше).
pub fn message_count(low: i64, high: i64) -> i64 {
    (high - low).max(0)
}

/// (low, high) watermark по каждой партиции двумя пакетными ListOffsets-запросами
/// (offsets_for_times со спецзначениями -2 = earliest, -1 = latest).
pub fn fetch_bounds(
    c: &BaseConsumer<CapturingContext>,
    parts: &[(String, i32)],
    timeout: Duration,
) -> KafkaResult<HashMap<(String, i32), (i64, i64)>> {
    if parts.is_empty() {
        return Ok(HashMap::new());
    }
    let low = list_offsets(c, parts, Offset::Beginning, timeout)?;
    let high = list_offsets(c, parts, Offset::End, timeout)?;
    Ok(parts
        .iter()
        .map(|k| {
            let l = low.get(k).copied().unwrap_or(0);
            let h = high.get(k).copied().unwrap_or(l);
            (k.clone(), (l, h))
        })
        .collect())
}

fn list_offsets(
    c: &BaseConsumer<CapturingContext>,
    parts: &[(String, i32)],
    which: Offset,
    timeout: Duration,
) -> KafkaResult<HashMap<(String, i32), i64>> {
    let mut tpl = TopicPartitionList::with_capacity(parts.len());
    for (t, p) in parts {
        tpl.add_partition_offset(t, *p, which)?;
    }
    let res = c.offsets_for_times(tpl, timeout)?;
    Ok(res
        .elements()
        .iter()
        .filter_map(|e| match e.offset() {
            Offset::Offset(o) => Some(((e.topic().to_string(), e.partition()), o)),
            _ => None,
        })
        .collect())
}

pub fn topic_stats(h: &KafkaHandles, names: &[String]) -> AppResult<Vec<TopicStats>> {
    let md = h.consumer.fetch_metadata(None, h.timeout).map_err(|e| h.kafka_err(e))?;
    let (_, topics) = convert(&md);
    let wanted: HashSet<&str> = names.iter().map(String::as_str).collect();
    let parts: Vec<(String, i32)> = topics
        .iter()
        .filter(|t| wanted.contains(t.name.as_str()))
        .flat_map(|t| t.partitions.iter().map(move |p| (t.name.clone(), p.id)))
        .collect();
    let bounds = fetch_bounds(&h.consumer, &parts, h.timeout).map_err(|e| h.kafka_err(e))?;
    let mut counts: HashMap<&str, i64> = HashMap::new();
    for ((t, _), (l, hi)) in &bounds {
        *counts.entry(t.as_str()).or_default() += message_count(*l, *hi);
    }
    Ok(names
        .iter()
        .filter_map(|n| counts.get(n.as_str()).map(|c| TopicStats { name: n.clone(), message_count: *c }))
        .collect())
}

pub fn topic_details(h: &KafkaHandles, name: &str) -> AppResult<TopicDetails> {
    let md = h.consumer.fetch_metadata(Some(name), h.timeout).map_err(|e| h.kafka_err(e))?;
    let (_, topics) = convert(&md);
    let t = topics
        .into_iter()
        .find(|t| t.name == name && !t.partitions.is_empty())
        .ok_or_else(|| AppError::not_found(format!("Топик «{name}» не найден")))?;
    let keys: Vec<(String, i32)> = t.partitions.iter().map(|p| (t.name.clone(), p.id)).collect();
    let bounds = fetch_bounds(&h.consumer, &keys, h.timeout).map_err(|e| h.kafka_err(e))?;
    let mut partitions: Vec<PartitionDetails> = t
        .partitions
        .iter()
        .map(|p| {
            let (low, high) = bounds.get(&(t.name.clone(), p.id)).copied().unwrap_or((0, 0));
            PartitionDetails {
                id: p.id,
                leader: p.leader,
                replicas: p.replicas.clone(),
                isr: p.isr.clone(),
                low,
                high,
                message_count: message_count(low, high),
                under_replicated: p.under_replicated(),
            }
        })
        .collect();
    partitions.sort_by_key(|p| p.id);
    Ok(TopicDetails {
        internal: t.internal(),
        replication_factor: t.replication_factor(),
        message_count: partitions.iter().map(|p| p.message_count).sum(),
        name: t.name,
        partitions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::metadata::{PartitionMeta, TopicMeta};

    fn topic(name: &str, rf: usize, parts: i32, isr_short: bool) -> TopicMeta {
        let replicas: Vec<i32> = (1..=rf as i32).collect();
        TopicMeta {
            name: name.into(),
            partitions: (0..parts)
                .map(|id| PartitionMeta {
                    id,
                    leader: 1,
                    replicas: replicas.clone(),
                    isr: if isr_short && id == 0 { vec![1] } else { replicas.clone() },
                })
                .collect(),
        }
    }

    #[test]
    fn summaries_sorted_with_counts() {
        let s = summarize_topics(vec![topic("b", 3, 2, true), topic("a", 1, 4, false), topic("__x", 1, 1, false)]);
        assert_eq!(s.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(), ["__x", "a", "b"]);
        assert!(s[0].internal);
        assert_eq!(s[1].partition_count, 4);
        assert_eq!(s[2].replication_factor, 3);
        assert_eq!(s[2].under_replicated, 1);
    }

    #[test]
    fn message_count_never_negative() {
        assert_eq!(message_count(5, 12), 7);
        assert_eq!(message_count(5, 5), 0);
        assert_eq!(message_count(9, 3), 0);
    }
}
