//! Thread-wide record cache: a byte budget shared across `NamedCache`s. Ports
//! Kafka `ThreadCache`. One per task; budget = `statestore.cache.max.bytes` /
//! `task_count`.
use crate::store::cache::entry::LruCacheEntry;
use crate::store::cache::named::NamedCache;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) struct ThreadCache {
    caches: HashMap<String, Arc<Mutex<NamedCache>>>,
    max_bytes: usize,
}

impl ThreadCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            caches: HashMap::new(),
            max_bytes,
        }
    }

    /// Caching is active only when a positive byte budget is configured.
    pub fn enabled(&self) -> bool {
        self.max_bytes > 0
    }

    /// Return the named cache for `name`, creating it if absent.
    pub fn register(&mut self, name: &str) -> Arc<Mutex<NamedCache>> {
        Arc::clone(
            self.caches
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(NamedCache::new(name.to_string())))),
        )
    }

    /// Total bytes held across all named caches.
    pub fn total_bytes(&self) -> usize {
        self.caches
            .values()
            .map(|c| c.lock().expect("named cache poisoned").size_bytes())
            .sum()
    }

    /// While over budget, evict the LRU entry from a non-empty cache, routing
    /// each evicted dirty entry through `listener(cache_name, key, entry)`.
    ///
    /// Cross-cache policy: cache names are visited in sorted (lexicographic)
    /// order, and one entry is evicted from the first non-empty cache each round.
    /// This is deterministic and terminates: every round either frees bytes or,
    /// if all caches are empty, breaks. Within a cache, eviction targets that
    /// cache's own LRU (head) entry, matching `NamedCache::evict`.
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
        assert!(!ThreadCache::new(0).enabled());
    }

    #[test]
    fn over_budget_evicts_lru() {
        // Each entry: key.len(1) + value(1) + 21 context = 23 bytes.
        let max_bytes = 50;
        let mut tc = ThreadCache::new(max_bytes);

        let ca = tc.register("a");
        let cb = tc.register("b");

        // Insert oldest -> newest across two caches so total > 50.
        // a: A(0), C(2) ; b: B(1), D(3) — relative LRU order within each cache.
        ca.lock().unwrap().put(key(b"A"), dirty_entry(b"0")); // 23
        cb.lock().unwrap().put(key(b"B"), dirty_entry(b"1")); // 23 -> 46
        ca.lock().unwrap().put(key(b"C"), dirty_entry(b"2")); // 23 -> 69
        cb.lock().unwrap().put(key(b"D"), dirty_entry(b"3")); // 23 -> 92

        assert_eq!(tc.total_bytes(), 92);

        let mut evicted: Vec<(String, Bytes)> = Vec::new();
        {
            let mut listener = |name: &str, k: &Bytes, _: &LruCacheEntry| {
                evicted.push((name.to_string(), k.clone()));
            };
            tc.maybe_evict(&mut listener);
        }

        assert!(
            tc.total_bytes() <= max_bytes,
            "total {} should be <= {}",
            tc.total_bytes(),
            max_bytes
        );

        // 92 over budget 50. Each round restarts from the sorted name list and
        // evicts the first non-empty cache's LRU head, so cache "a" drains first:
        // evict A -> 69 (still over), evict C -> 46 (<= 50, stop). Both are
        // cache "a"'s LRU heads in turn (A was inserted before C).
        assert_eq!(
            evicted,
            vec![("a".to_string(), key(b"A")), ("a".to_string(), key(b"C")),]
        );
        // Remaining: B and D (23 + 23 = 46 <= 50).
        assert_eq!(tc.total_bytes(), 46);
    }
}
