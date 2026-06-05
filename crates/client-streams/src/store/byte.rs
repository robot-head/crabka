//! Byte-level pluggable KV backend. The typed `KeyValueBytesStore<K,V>` sits on
//! top; backends (`InMemoryBytes`, later `TursoBytes`) are swapped underneath.
use std::collections::BTreeMap;

use bytes::Bytes;

/// Object-safe raw-byte KV backend. `range` is half-open `[lo, hi)` in memcmp
/// (lexicographic) key order — used by 4d-ii's window store; KV stores don't call it.
pub(crate) trait ByteKeyValueStore: Send {
    fn get(&self, key: &[u8]) -> Option<Bytes>;
    fn put(&mut self, key: Bytes, value: Bytes);
    fn delete(&mut self, key: &[u8]) -> Option<Bytes>;
    #[allow(dead_code)] // used by 4d-ii window store
    fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<(Bytes, Bytes)>;
}

/// In-memory backend over a `BTreeMap` (ordered → serves `range`).
#[derive(Default)]
pub(crate) struct InMemoryBytes {
    map: BTreeMap<Bytes, Bytes>,
}

impl ByteKeyValueStore for InMemoryBytes {
    fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).cloned()
    }
    fn put(&mut self, key: Bytes, value: Bytes) {
        self.map.insert(key, value);
    }
    fn delete(&mut self, key: &[u8]) -> Option<Bytes> {
        self.map.remove(key)
    }
    fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<(Bytes, Bytes)> {
        self.map
            .iter()
            .filter(|(k, _)| k.as_ref() >= lo && k.as_ref() < hi)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn inmemory_put_get_delete_range_ordered() {
        let mut s = InMemoryBytes::default();
        s.put(Bytes::from_static(&[1, 0]), Bytes::from_static(b"a"));
        s.put(Bytes::from_static(&[1, 2]), Bytes::from_static(b"b"));
        s.put(Bytes::from_static(&[2, 0]), Bytes::from_static(b"c"));
        check!(s.get(&[1, 2]) == Some(Bytes::from_static(b"b")));
        let r = s.range(&[1, 0], &[2, 0]);
        check!(r.len() == 2);
        check!(r[0].1 == Bytes::from_static(b"a")); // ordered
        check!(s.delete(&[1, 0]) == Some(Bytes::from_static(b"a")));
        check!(s.get(&[1, 0]) == None);
    }
}
