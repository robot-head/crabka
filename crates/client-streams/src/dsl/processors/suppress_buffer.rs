//! In-memory time-ordered buffer for `suppress` (KIP final-results). Holds at most
//! one entry per key (replace-by-key), ordered by `(buffer_time, seq)` so eviction
//! drains the earliest-closing windows first. Unbounded in Slice A (no size cap);
//! Slice B adds record/byte accounting + overflow, Slice D adds serialization for
//! the changelog.
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

#[allow(dead_code)] // fields consumed via move-out in evict_while; read by suppress.rs (Task 3)
struct Entry<K, V> {
    key: K,
    value: V,
    record_ts: i64,
}

/// Time-ordered, replace-by-key buffer. `K` must be `Eq + Hash + Clone` (the
/// suppress key is `Windowed<KInner>`).
#[allow(dead_code)] // used by KTableSuppressProcessor (suppress.rs, Task 3)
pub(crate) struct TimeOrderedKeyValueBuffer<K, V> {
    /// Ordered by `(buffer_time, seq)`; `seq` disambiguates equal buffer times.
    entries: BTreeMap<(i64, u64), Entry<K, V>>,
    /// Locate-and-replace the slot currently held by a key.
    index: HashMap<K, (i64, u64)>,
    seq: u64,
}

#[allow(dead_code)] // new/put/evict_while used by KTableSuppressProcessor (suppress.rs, Task 3)
impl<K: Eq + Hash + Clone, V> TimeOrderedKeyValueBuffer<K, V> {
    pub(crate) fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            index: HashMap::new(),
            seq: 0,
        }
    }

    /// Insert or replace the entry for `key`. A re-put removes the key's prior slot
    /// (so there is always exactly one entry per key) and inserts a fresh slot at
    /// `(buffer_time, seq)`.
    pub(crate) fn put(&mut self, key: K, buffer_time: i64, value: V, record_ts: i64) {
        if let Some(old_slot) = self.index.remove(&key) {
            self.entries.remove(&old_slot);
        }
        let slot = (buffer_time, self.seq);
        self.seq += 1;
        self.index.insert(key.clone(), slot);
        self.entries.insert(
            slot,
            Entry {
                key,
                value,
                record_ts,
            },
        );
    }

    /// Pop and return every entry whose `buffer_time <= threshold`, in
    /// `(buffer_time, seq)` order, as `(key, value, record_ts)`.
    pub(crate) fn evict_while(&mut self, threshold: i64) -> Vec<(K, V, i64)> {
        let mut out = Vec::new();
        while let Some((&slot, _)) = self.entries.iter().next() {
            if slot.0 > threshold {
                break;
            }
            let entry = self.entries.remove(&slot).expect("slot present");
            self.index.remove(&entry.key);
            out.push((entry.key, entry.value, entry.record_ts));
        }
        out
    }

    #[allow(dead_code)] // used by tests (and Slice B accounting)
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replace_by_key_keeps_one_entry() {
        let mut b = TimeOrderedKeyValueBuffer::<String, i64>::new();
        b.put("k".into(), 10, 1, 5);
        b.put("k".into(), 10, 2, 7); // same key + buffer_time → replace
        assert_eq!(b.len(), 1);
        let out = b.evict_while(10);
        assert_eq!(out, vec![("k".into(), 2, 7)]);
        assert_eq!(b.len(), 0);
    }

    #[test]
    fn evicts_in_buffer_time_order_up_to_threshold() {
        let mut b = TimeOrderedKeyValueBuffer::<String, i64>::new();
        b.put("a".into(), 30, 1, 30);
        b.put("b".into(), 10, 2, 10);
        b.put("c".into(), 20, 3, 20);
        // threshold 20 → evict b(10), c(20); a(30) stays.
        let out = b.evict_while(20);
        assert_eq!(out, vec![("b".into(), 2, 10), ("c".into(), 3, 20)]);
        assert_eq!(b.len(), 1);
        // raising the threshold drains the rest.
        assert_eq!(b.evict_while(100), vec![("a".into(), 1, 30)]);
    }

    #[test]
    fn evict_below_threshold_returns_empty() {
        let mut b = TimeOrderedKeyValueBuffer::<String, i64>::new();
        b.put("a".into(), 50, 1, 50);
        assert_eq!(b.evict_while(49), vec![]);
        assert_eq!(b.len(), 1);
    }
}
