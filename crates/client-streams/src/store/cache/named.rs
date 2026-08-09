//! Per-store record cache: a doubly-linked LRU over `Bytes -> LruCacheEntry`,
//! plus a dirty-key set in insertion order.
//!
//! This module ports Kafka `NamedCache`. The entry map is a
//! `BTreeMap<Bytes, Node>`, which matches Kafka's backing `TreeMap`. It orders
//! keys in memcmp (lexicographic) byte order. That order gives the caching store
//! wrappers the ordered [`range`](NamedCache::range) and
//! [`all`](NamedCache::all) scans they need to merge cache and inner reads. The
//! scans cost nothing extra and need no parallel key index.
//!
//! The LRU uses no `unsafe`. Each [`Node`] stores its predecessor and successor
//! *keys* in the map. `head` is the LRU eviction end, `tail` is the MRU end, and
//! the two track the list ends. LRU recency lives only in those links, apart from
//! the map's iteration order, so an ordered scan does not disturb recency. The
//! dirty set is a `BTreeMap<u64, Bytes>` keyed by a monotonically increasing
//! insertion counter, so `flush` visits the dirty keys in the order they first
//! became dirty.

use std::collections::BTreeMap;

use bytes::Bytes;
use crabka_units::prelude::*;

use crate::{processor::record::RecordContext, store::cache::entry::LruCacheEntry};

/// Callback that runs for each dirty entry on flush and on evict.
pub(crate) type FlushListener<'a> = dyn FnMut(&Bytes, &LruCacheEntry) + 'a;

struct Node {
    entry: LruCacheEntry,
    prev: Option<Bytes>,
    next: Option<Bytes>,
    /// Insertion-order sequence that keys the dirty set. It is only meaningful
    /// while the entry is dirty.
    dirty_seq: Option<u64>,
}

pub(crate) struct NamedCache {
    name: String,
    map: BTreeMap<Bytes, Node>,
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
            map: BTreeMap::new(),
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

    /// The cache's total held size. A `usize` holds the running total, so the
    /// incremental add and subtract stay exact. This method puts the dimension
    /// back on.
    pub fn size_bytes(&self) -> ByteSize {
        ByteSize::from_bytes(self.size_bytes.try_into().unwrap_or(u64::MAX))
    }

    /// Look up a key and do not promote it. This is a read-only borrow.
    pub fn get(&self, key: &Bytes) -> Option<&LruCacheEntry> {
        self.map.get(key).map(|n| &n.entry)
    }

    /// Entries with a key in `[lo, hi)`, in ascending memcmp key order.
    ///
    /// This method clones the entries, tombstones included, so a caller can hide
    /// the underlying value. A range scan does NOT promote LRU recency, which
    /// matches Kafka's range read.
    pub fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<(Bytes, LruCacheEntry)> {
        self.map
            .range::<[u8], _>((std::ops::Bound::Included(lo), std::ops::Bound::Excluded(hi)))
            .map(|(k, n)| (k.clone(), n.entry.clone()))
            .collect()
    }

    /// Every entry in ascending memcmp key order, unbounded.
    ///
    /// This method clones the entries, tombstones included. Like
    /// [`range`](Self::range), it does NOT promote recency.
    pub fn all(&self) -> Vec<(Bytes, LruCacheEntry)> {
        self.map
            .iter()
            .map(|(k, n)| (k.clone(), n.entry.clone()))
            .collect()
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

    /// Insert or update an entry. This method links a new key at the MRU end, and
    /// an update promotes the key. The dirty set tracks the dirty entries in
    /// insertion order, and the size accounting stays current.
    pub fn put(&mut self, key: Bytes, entry: LruCacheEntry) {
        let new_value_size = entry.value_size();
        let dirty = entry.dirty;

        if let Some(node) = self.map.get_mut(&key) {
            // Update: adjust size, replace entry, refresh dirty tracking, promote.
            self.size_bytes -= node.entry.value_size();
            self.size_bytes += new_value_size;
            let old_seq = node.dirty_seq;
            node.entry = entry;
            // LinkedHashSet parity: a key that is already dirty KEEPS its original
            // dirty position. Only (re)allocate a sequence when transitioning a
            // clean/absent entry to dirty, and only drop the tracking entry when
            // transitioning dirty -> clean.
            match (old_seq, dirty) {
                // Newly dirtied (was clean): allocate a fresh sequence at the end.
                (None, true) => {
                    let seq = self.alloc_dirty_seq();
                    self.dirty.insert(seq, key.clone());
                    if let Some(node) = self.map.get_mut(&key) {
                        node.dirty_seq = Some(seq);
                    }
                }
                // Was dirty, now clean: drop it from the dirty set.
                (Some(seq), false) => {
                    self.dirty.remove(&seq);
                    if let Some(node) = self.map.get_mut(&key) {
                        node.dirty_seq = None;
                    }
                }
                // No dirty-state transition: already dirty stays dirty (preserve
                // its existing position), or already clean stays clean.
                (Some(_), true) | (None, false) => {}
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

    /// Evict the LRU (head) entry. The listener runs for that entry first when it
    /// is dirty. This method returns the number of bytes freed, which is
    /// `key.len() + value_size`.
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

    /// Unlink `key` from the doubly-linked list and fix the neighbors, the head,
    /// and the tail. The node stays in the map with a stale `prev` and `next`,
    /// and the caller relinks it.
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
    use assert2::check;

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
        check!(c.size_bytes() == bytes(23 + 25 + 27));
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
        check!(c.get(&key(b"A")).is_none(), "A (LRU) evicted first");
        check!(c.get(&key(b"B")).is_some());
        check!(c.get(&key(b"C")).is_some());

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
        check!(!c.get(&key(b"A")).unwrap().dirty);
        check!(!c.get(&key(b"B")).unwrap().dirty);
        check!(!c.get(&key(b"C")).unwrap().dirty);
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
    fn redirty_preserves_insertion_order() {
        // Kafka's dirtyKeys is a LinkedHashSet: re-adding an already-present key
        // keeps its original position. put A (dirty), put B (dirty), put A again
        // (still dirty, value changed) -> flush order must be [A, B], not [B, A].
        let mut c = NamedCache::new("s".to_string());
        c.put(key(b"A"), dirty_entry(b"1"));
        c.put(key(b"B"), dirty_entry(b"2"));
        c.put(key(b"A"), dirty_entry(b"9"));

        let mut seen: Vec<Bytes> = Vec::new();
        {
            let mut listener = |k: &Bytes, _: &LruCacheEntry| seen.push(k.clone());
            c.flush(&mut listener);
        }
        assert_eq!(
            seen,
            vec![key(b"A"), key(b"B")],
            "A keeps its original dirty position on re-dirty"
        );
        // The updated value is the one flushed.
        assert_eq!(
            c.get(&key(b"A")).unwrap().value,
            Some(Bytes::from_static(b"9"))
        );
    }

    #[test]
    fn range_returns_entries_in_key_order() {
        let mut c = NamedCache::new("s".to_string());
        // Insert out of key order; range must still come back ascending.
        c.put(key(b"ccc"), entry(b"3"));
        c.put(key(b"a"), entry(b"1"));
        c.put(key(b"bb"), entry(b"2"));
        // A tombstone inside the range: it must be returned (callers hide it).
        c.delete(key(b"bb2"), ctx());

        // Range [a, ccc) is half-open: includes a, bb, bb2; EXCLUDES ccc at hi.
        let r = c.range(b"a", b"ccc");
        let keys: Vec<Bytes> = r.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(
            keys,
            vec![key(b"a"), key(b"bb"), key(b"bb2")],
            "ascending memcmp order, hi-exclusive"
        );
        // The in-range tombstone is present with a None value.
        let bb2 = r.iter().find(|(k, _)| k == &key(b"bb2")).unwrap();
        assert_eq!(bb2.1.value, None);
        assert!(bb2.1.dirty);

        // A range scan must NOT promote recency: LRU head stays the
        // first-inserted key (ccc), so it is evicted first.
        let mut noop = |_: &Bytes, _: &LruCacheEntry| {};
        c.evict(&mut noop);
        assert!(
            c.get(&key(b"ccc")).is_none(),
            "ccc (LRU head) evicted first; range did not touch recency"
        );

        // all() returns the full set in key order, including the out-of-range ccc.
        let mut c2 = NamedCache::new("s".to_string());
        c2.put(key(b"z"), entry(b"9"));
        c2.put(key(b"a"), entry(b"1"));
        let all: Vec<Bytes> = c2.all().into_iter().map(|(k, _)| k).collect();
        assert_eq!(all, vec![key(b"a"), key(b"z")]);
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
