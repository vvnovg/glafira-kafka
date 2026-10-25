//! Лучшие N записей по [`SortKey`] за один проход по окну (спец. G4 §3.2): страница хронологична
//! сразу по всем партициям, память — не больше `limit` записей.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::messages::SortKey;

struct Entry<T> {
    key: SortKey,
    item: T,
}

impl<T> PartialEq for Entry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<T> Eq for Entry<T> {}

impl<T> PartialOrd for Entry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Entry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

/// Куча с наибольшим ключом наверху: новый меньший ключ вытесняет его.
pub struct TopN<T> {
    limit: usize,
    after: Option<SortKey>,
    heap: BinaryHeap<Entry<T>>,
    considered: u64,
}

impl<T> TopN<T> {
    /// `after` — курсор предыдущей страницы: ключи не больше него пропускаются.
    pub fn new(limit: usize, after: Option<SortKey>) -> Self {
        Self {
            limit,
            after,
            heap: BinaryHeap::with_capacity(limit.min(1024) + 1),
            considered: 0,
        }
    }

    fn after_cursor(&self, key: SortKey) -> bool {
        self.after.is_none_or(|a| key > a)
    }

    /// Запись попадёт в страницу: строить её стоит только тогда (копия байтов значения).
    pub fn wants(&self, key: SortKey) -> bool {
        self.after_cursor(key)
            && (self.heap.len() < self.limit || self.heap.peek().is_some_and(|top| key < top.key))
    }

    /// Учитывает прошедшую фильтры запись; `make` вызывается, только если она нужна.
    pub fn offer(&mut self, key: SortKey, make: impl FnOnce() -> T) {
        if !self.after_cursor(key) {
            return;
        }
        self.considered += 1;
        if self.limit == 0 {
            return;
        }
        if self.heap.len() < self.limit {
            self.heap.push(Entry { key, item: make() });
        } else if self.heap.peek().is_some_and(|top| key < top.key) {
            self.heap.pop();
            self.heap.push(Entry { key, item: make() });
        }
    }

    /// После последней записи страницы есть ещё прошедшие фильтры.
    pub fn more(&self) -> bool {
        self.considered > self.heap.len() as u64
    }

    /// По возрастанию ключа.
    pub fn into_sorted(self) -> Vec<(SortKey, T)> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|e| (e.key, e.item))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(keys: &[SortKey], limit: usize, after: Option<SortKey>) -> (Vec<SortKey>, bool) {
        let mut t = TopN::new(limit, after);
        for k in keys {
            t.offer(*k, || *k);
        }
        let more = t.more();
        (t.into_sorted().into_iter().map(|(k, _)| k).collect(), more)
    }

    #[test]
    fn keeps_the_smallest_keys_in_order() {
        let keys = [(5, 0, 1), (1, 1, 7), (3, 0, 0), (1, 0, 9), (9, 2, 2)];
        let (first, more) = page(&keys, 3, None);
        assert_eq!(first, [(1, 0, 9), (1, 1, 7), (3, 0, 0)]);
        assert!(more);
        let (all, more) = page(&keys, 10, None);
        assert_eq!(all.len(), 5);
        assert!(!more);
    }

    #[test]
    fn pages_by_cursor_have_no_gaps_or_duplicates() {
        // Равные метки времени в разных партициях и офсетах.
        let keys: Vec<SortKey> = (0..23).map(|i| (i / 4, (i % 3) as i32, i)).collect();
        let mut seen = Vec::new();
        let mut after = None;
        loop {
            let (got, more) = page(&keys, 5, after);
            seen.extend(got.iter().copied());
            if !more {
                break;
            }
            after = got.last().copied();
        }
        let mut expected = keys.clone();
        expected.sort();
        assert_eq!(seen, expected);
    }

    #[test]
    fn records_are_built_only_when_needed() {
        let mut t = TopN::new(2, Some((0, 0, 0)));
        let mut built = 0;
        for k in [(0, 0, 0), (5, 0, 1), (4, 0, 2), (9, 0, 3), (1, 0, 4)] {
            if t.wants(k) {
                built += 1;
            }
            t.offer(k, || k);
        }
        // (0,0,0) — до курсора; (9,0,3) — хуже двух уже взятых.
        assert_eq!(built, 3);
        assert_eq!(
            t.into_sorted()
                .into_iter()
                .map(|(k, _)| k)
                .collect::<Vec<_>>(),
            [(1, 0, 4), (4, 0, 2)]
        );
    }
}
