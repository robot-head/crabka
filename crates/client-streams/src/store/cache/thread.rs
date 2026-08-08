//! Thread-wide record cache: a byte budget shared across the `NamedCache`s.
//!
//! This module ports Kafka `ThreadCache`. There is one cache per task, and its
//! budget is `statestore.cache.max.bytes` divided by `task_count`.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use crabka_units::prelude::*;

use crate::store::cache::{entry::LruCacheEntry, named::NamedCache};

pub(crate) struct ThreadCache {
    caches: HashMap<String, Arc<Mutex<NamedCache>>>,
    max_bytes: ByteSize,
}

impl ThreadCache {
    pub fn new(max_bytes: ByteSize) -> Self {
        Self {
            caches: HashMap::new(),
            max_bytes,
        }
    }

    /// Caching is active only when a positive byte budget is configured.
    pub fn enabled(&self) -> bool {
        self.max_bytes > ByteSize::ZERO
    }

    /// Return the named cache for `name`, and create it when it is absent.
    pub fn register(&mut self, name: &str) -> Arc<Mutex<NamedCache>> {
        Arc::clone(
            self.caches
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(NamedCache::new(name.to_string())))),
        )
    }

    /// Total held across all named caches.
    pub fn total_bytes(&self) -> ByteSize {
        self.caches
            .values()
            .map(|c| c.lock().expect("named cache poisoned").size_bytes())
            .fold(ByteSize::ZERO, |total, size| total + size)
    }

    /// While the cache is over budget, evict the LRU entry from a non-empty
    /// cache. Route each evicted dirty entry through
    /// `listener(cache_name, key, entry)`.
    ///
    /// The cross-cache policy visits the cache names in sorted (lexicographic)
    /// order and evicts one entry from the first non-empty cache each round. This
    /// is deterministic and it terminates. Every round either frees bytes or, when
    /// all the caches are empty, breaks. Within one cache the eviction targets
    /// that cache's own LRU (head) entry, which matches `NamedCache::evict`.
    pub fn maybe_evict(&mut self, listener: &mut impl FnMut(&str, &Bytes, &LruCacheEntry)) {
        let mut names: Vec<String> = self.caches.keys().cloned().collect();
        names.sort();

        while self.total_bytes() > self.max_bytes {
            let mut evicted_any = false;
            for name in &names {
                let cache = &self.caches[name];
                let mut guard = cache.lock().expect("named cache poisoned");
                if guard.len() == 0 {
                    continue;
                }
                let mut inner = |key: &Bytes, entry: &LruCacheEntry| listener(name, key, entry);
                guard.evict(&mut inner);
                evicted_any = true;
                break;
            }
            if !evicted_any {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::processor::record::RecordContext;

    fn ctx() -> RecordContext {
        RecordContext {
            topic: "t".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        }
    }

    fn dirty_entry(value: &'static [u8]) -> LruCacheEntry {
        LruCacheEntry::new(Some(Bytes::from_static(value)), true, ctx())
    }

    fn key(b: &'static [u8]) -> Bytes {
        Bytes::from_static(b)
    }

    #[test]
    fn zero_budget_not_enabled() {
        check!(!ThreadCache::new(ByteSize::ZERO).enabled());
    }

    #[test]
    fn over_budget_evicts_lru() {
        // Each entry: key.len(1) + value(1) + 21 context = 23 bytes.
        let max_bytes = bytes(50);
        let mut tc = ThreadCache::new(max_bytes);

        let ca = tc.register("a");
        let cb = tc.register("b");

        // Insert oldest -> newest across two caches so total > 50.
        // a: A(0), C(2) ; b: B(1), D(3) — relative LRU order within each cache.
        ca.lock().unwrap().put(key(b"A"), dirty_entry(b"0")); // 23
        cb.lock().unwrap().put(key(b"B"), dirty_entry(b"1")); // 23 -> 46
        ca.lock().unwrap().put(key(b"C"), dirty_entry(b"2")); // 23 -> 69
        cb.lock().unwrap().put(key(b"D"), dirty_entry(b"3")); // 23 -> 92

        check!(tc.total_bytes() == bytes(92));

        let mut evicted: Vec<(String, Bytes)> = Vec::new();
        {
            let mut listener = |name: &str, k: &Bytes, _: &LruCacheEntry| {
                evicted.push((name.to_string(), k.clone()));
            };
            tc.maybe_evict(&mut listener);
        }

        check!(tc.total_bytes() <= max_bytes);

        // 92 over budget 50. Each round restarts from the sorted name list and
        // evicts the first non-empty cache's LRU head, so cache "a" drains first:
        // evict A -> 69 (still over), evict C -> 46 (<= 50, stop). Both are
        // cache "a"'s LRU heads in turn (A was inserted before C).
        check!(evicted == vec![("a".to_string(), key(b"A")), ("a".to_string(), key(b"C"))]);
        // Remaining: B and D (23 + 23 = 46 <= 50).
        check!(tc.total_bytes() == bytes(46));
    }

    #[test]
    fn over_budget_evicts_across_caches() {
        // Each entry is 23 bytes. Cache "a" holds a single entry; cache "b" holds
        // three. Budget 30 forces eviction to fully drain "a" and then CROSS into
        // "b" — proving the cross-cache traversal in maybe_evict, not just a
        // single-cache drain.
        let max_bytes = bytes(30);
        let mut tc = ThreadCache::new(max_bytes);

        let ca = tc.register("a");
        let cb = tc.register("b");

        ca.lock().unwrap().put(key(b"A"), dirty_entry(b"0")); // a: 23
        cb.lock().unwrap().put(key(b"B"), dirty_entry(b"1")); // b: 23 -> 46
        cb.lock().unwrap().put(key(b"C"), dirty_entry(b"2")); // b: 46 -> 69
        cb.lock().unwrap().put(key(b"D"), dirty_entry(b"3")); // b: 69 -> 92

        check!(tc.total_bytes() == bytes(92));

        let mut evicted: Vec<(String, Bytes)> = Vec::new();
        {
            let mut listener = |name: &str, k: &Bytes, _: &LruCacheEntry| {
                evicted.push((name.to_string(), k.clone()));
            };
            tc.maybe_evict(&mut listener);
        }

        check!(tc.total_bytes() <= max_bytes);

        // 92 over budget 30. Sorted name order ["a","b"] always tries "a" first:
        //   evict a/A -> 69 (a now empty); next round "a" is empty so cross to "b":
        //   evict b/B -> 46; evict b/C -> 23 (<= 30, stop).
        // This is exactly the cross-cache path: "a" fully drains, then "b" is hit.
        check!(
            evicted
                == vec![
                    ("a".to_string(), key(b"A")),
                    ("b".to_string(), key(b"B")),
                    ("b".to_string(), key(b"C")),
                ]
        );
        check!(ca.lock().unwrap().len() == 0, "cache a fully emptied");
        // Remaining: only D in cache "b".
        check!(tc.total_bytes() == bytes(23));
    }
}
