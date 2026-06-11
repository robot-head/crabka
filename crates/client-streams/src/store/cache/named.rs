//! Per-store record cache: a doubly-linked LRU over `Bytes -> LruCacheEntry`
//! plus a dirty-key set kept in insertion order. Ports Kafka `NamedCache`.
//!
//! The LRU is implemented without `unsafe`: each [`Node`] stores its predecessor
//! and successor *keys* in a `HashMap`, with `head` (LRU/eviction end) and `tail`
//! (MRU/most-recently-used end) tracking the list ends. The dirty set is a
//! `BTreeMap<u64, Bytes>` keyed by a monotonically increasing insertion counter,
//! so `flush` visits dirty keys in the order they first became dirty.

use crate::processor::record::RecordContext;
use crate::store::cache::entry::LruCacheEntry;
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};

/// Callback invoked per dirty entry on flush/evict.
pub(crate) type FlushListener<'a> = dyn FnMut(&Bytes, &LruCacheEntry) + 'a;

struct Node {
    entry: LruCacheEntry,
    prev: Option<Bytes>,
    next: Option<Bytes>,
    /// Insertion-order sequence used to key the dirty set; only meaningful while
    /// the entry is dirty.
    dirty_seq: Option<u64>,
}

pub(crate) struct NamedCache {
    name: String,
    map: HashMap<Bytes, Node>,
    head: Option<Bytes>,
    tail: Option<Bytes>,
    /// Dirty keys in insertion order (seq -> key).
    dirty: BTreeMap<u64, Bytes>,
    next_dirty_seq: u64,
    size_bytes: usize,
}

impl NamedCache {
    pub fn new(name: String) -> Self {
        Self {
            name,
            map: HashMap::new(),
            head: None,
            tail: None,
            dirty: BTreeMap::new(),
            next_dirty_seq: 0,
            size_bytes: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Lookup without promotion (read-only borrow).
    pub fn get(&self, key: &Bytes) -> Option<&LruCacheEntry> {
        self.map.get(key).map(|n| &n.entry)
    }

    /// Lookup and promote the key to the MRU (tail) position.
    pub fn get_promote(&mut self, key: &Bytes) -> Option<&LruCacheEntry> {
        if self.map.contains_key(key) {
            self.unlink(key);
            self.link_at_tail(key.clone());
            self.map.get(key).map(|n| &n.entry)
        } else {
            None
        }
    }

    /// Insert or update an entry. New keys are linked at the MRU end; updates
    /// promote the key. Dirty entries are tracked in the dirty set in insertion
    /// order; size accounting is kept current.
    pub fn put(&mut self, key: Bytes, entry: LruCacheEntry) {
        let new_value_size = entry.value_size();
        let dirty = entry.dirty;

        if let Some(node) = self.map.get_mut(&key) {
            // Update: adjust size, replace entry, refresh dirty tracking, promote.
            self.size_bytes -= node.entry.value_size();
            self.size_bytes += new_value_size;
            let old_seq = node.dirty_seq.take();
            node.entry = entry;
            if let Some(seq) = old_seq {
                self.dirty.remove(&seq);
            }
            if dirty {
                let seq = self.alloc_dirty_seq();
                self.dirty.insert(seq, key.clone());
                if let Some(node) = self.map.get_mut(&key) {
                    node.dirty_seq = Some(seq);
                }
            }
            self.unlink(&key);
            self.link_at_tail(key);
        } else {
            // Insert new node at the MRU end.
            self.size_bytes += key.len() + new_value_size;
            let dirty_seq = if dirty {
                let seq = self.alloc_dirty_seq();
                self.dirty.insert(seq, key.clone());
                Some(seq)
            } else {
                None
            };
            self.map.insert(
                key.clone(),
                Node {
                    entry,
                    prev: None,
                    next: None,
                    dirty_seq,
                },
            );
            self.link_at_tail(key);
        }
    }

    /// Store a dirty tombstone (`value = None`) for `key`.
    pub fn delete(&mut self, key: Bytes, context: RecordContext) {
        self.put(key, LruCacheEntry::new(None, true, context));
    }

    /// Visit every dirty entry in insertion order, then clear all dirty flags
    /// and the dirty set.
    pub fn flush(&mut self, listener: &mut FlushListener) {
        let order: Vec<Bytes> = self.dirty.values().cloned().collect();
        for key in order {
            if let Some(node) = self.map.get(&key) {
                listener(&key, &node.entry);
            }
        }
        for key in self.dirty.values() {
            if let Some(node) = self.map.get_mut(key) {
                node.entry.dirty = false;
                node.dirty_seq = None;
            }
        }
        self.dirty.clear();
    }

    /// Evict the LRU (head) entry. If it is dirty, the listener is called for it
    /// first. Returns the number of bytes freed (`key.len() + value_size`).
    pub fn evict(&mut self, listener: &mut FlushListener) -> usize {
        let Some(key) = self.head.clone() else {
            return 0;
        };
        let freed = {
            let node = &self.map[&key];
            if node.entry.dirty {
                listener(&key, &node.entry);
            }
            key.len() + node.entry.value_size()
        };
        self.unlink(&key);
        if let Some(Node {
            dirty_seq: Some(seq),
            ..
        }) = self.map.remove(&key)
        {
            self.dirty.remove(&seq);
        }
        self.size_bytes -= freed;
        freed
    }

    fn alloc_dirty_seq(&mut self) -> u64 {
        let seq = self.next_dirty_seq;
        self.next_dirty_seq += 1;
        seq
    }

    /// Unlink `key` from the doubly-linked list, fixing neighbors and head/tail.
    /// Leaves the node in the map with stale `prev`/`next` (callers relink).
    fn unlink(&mut self, key: &Bytes) {
        let (prev, next) = {
            let node = &self.map[key];
            (node.prev.clone(), node.next.clone())
        };
        match &prev {
            Some(p) => {
                if let Some(pn) = self.map.get_mut(p) {
                    pn.next.clone_from(&next);
                }
            }
            None => self.head.clone_from(&next),
        }
        match &next {
            Some(n) => {
                if let Some(nn) = self.map.get_mut(n) {
                    nn.prev.clone_from(&prev);
                }
            }
            None => self.tail.clone_from(&prev),
        }
        if let Some(node) = self.map.get_mut(key) {
            node.prev = None;
            node.next = None;
        }
    }

    /// Link `key`'s node at the tail (MRU) end.
    fn link_at_tail(&mut self, key: Bytes) {
        let old_tail = self.tail.clone();
        if let Some(node) = self.map.get_mut(&key) {
            node.prev.clone_from(&old_tail);
            node.next = None;
        }
        match &old_tail {
            Some(t) => {
                if let Some(tn) = self.map.get_mut(t) {
                    tn.next = Some(key.clone());
                }
            }
            None => self.head = Some(key.clone()),
        }
        self.tail = Some(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> RecordContext {
        RecordContext {
            topic: "t".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        }
    }

    fn entry(value: &'static [u8]) -> LruCacheEntry {
        LruCacheEntry::new(Some(Bytes::from_static(value)), false, ctx())
    }

    fn dirty_entry(value: &'static [u8]) -> LruCacheEntry {
        LruCacheEntry::new(Some(Bytes::from_static(value)), true, ctx())
    }

    fn key(b: &'static [u8]) -> Bytes {
        Bytes::from_static(b)
    }

    #[test]
    fn put_get() {
        let mut c = NamedCache::new("s".to_string());
        c.put(key(b"a"), entry(b"1"));
        c.put(key(b"bb"), entry(b"22"));
        c.put(key(b"ccc"), entry(b"333"));

        assert_eq!(c.len(), 3);
        assert_eq!(
            c.get(&key(b"a")).unwrap().value,
            Some(Bytes::from_static(b"1"))
        );
        assert_eq!(
            c.get(&key(b"bb")).unwrap().value,
            Some(Bytes::from_static(b"22"))
        );
        assert_eq!(
            c.get(&key(b"ccc")).unwrap().value,
            Some(Bytes::from_static(b"333"))
        );

        // size = sum(key.len + value_size); value_size = v + 8+8+4 + topic("t"=1)
        // a:  1 + (1 + 21) = 23
        // bb: 2 + (2 + 21) = 25
        // ccc:3 + (3 + 21) = 27
        assert_eq!(c.size_bytes(), 23 + 25 + 27);
    }

    #[test]
    fn lru_eviction_order() {
        let mut c = NamedCache::new("s".to_string());
        c.put(key(b"A"), entry(b"1"));
        c.put(key(b"B"), entry(b"2"));
        c.put(key(b"C"), entry(b"3"));

        // Promote B to MRU; LRU order is now A, C, B.
        assert!(c.get_promote(&key(b"B")).is_some());

        let mut noop = |_: &Bytes, _: &LruCacheEntry| {};
        c.evict(&mut noop);
        assert!(c.get(&key(b"A")).is_none(), "A (LRU) evicted first");
        assert!(c.get(&key(b"B")).is_some());
        assert!(c.get(&key(b"C")).is_some());

        c.evict(&mut noop);
        assert!(c.get(&key(b"C")).is_none(), "C evicted next");
        assert!(c.get(&key(b"B")).is_some(), "B promoted, survives");
    }

    #[test]
    fn dirty_flush_in_insertion_order() {
        let mut c = NamedCache::new("s".to_string());
        c.put(key(b"A"), dirty_entry(b"1"));
        c.put(key(b"B"), dirty_entry(b"2"));
        c.put(key(b"C"), dirty_entry(b"3"));

        let mut seen: Vec<Bytes> = Vec::new();
        {
            let mut listener = |k: &Bytes, _: &LruCacheEntry| seen.push(k.clone());
            c.flush(&mut listener);
        }
        assert_eq!(seen, vec![key(b"A"), key(b"B"), key(b"C")]);

        // No entry remains dirty.
        assert!(!c.get(&key(b"A")).unwrap().dirty);
        assert!(!c.get(&key(b"B")).unwrap().dirty);
        assert!(!c.get(&key(b"C")).unwrap().dirty);
    }

    #[test]
    fn evict_flushes_dirty_head() {
        let mut c = NamedCache::new("s".to_string());
        c.put(key(b"A"), dirty_entry(b"1"));
        c.put(key(b"B"), entry(b"2"));

        let mut count = 0usize;
        let mut seen_key: Option<Bytes> = None;
        {
            let mut listener = |k: &Bytes, _: &LruCacheEntry| {
                count += 1;
                seen_key = Some(k.clone());
            };
            // Head is A (LRU, dirty).
            c.evict(&mut listener);
        }
        assert_eq!(count, 1, "listener called once for dirty head");
        assert_eq!(seen_key, Some(key(b"A")));
        assert!(c.get(&key(b"A")).is_none());
    }

    #[test]
    fn tombstone() {
        let mut c = NamedCache::new("s".to_string());
        c.delete(key(b"k"), ctx());
        let e = c.get(&key(b"k")).unwrap();
        assert_eq!(e.value, None);
        assert!(e.dirty);
    }
}
