//! Byte-level pluggable KV backend. The typed `KeyValueBytesStore<K,V>` sits on
//! top; backends (`InMemoryBytes`, later `TursoBytes`) are swapped underneath.
use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;

/// Object-safe raw-byte KV backend. `range` is half-open `[lo, hi)` in memcmp
/// (lexicographic) key order — used by 4d-ii's window store; KV stores don't call it.
///
/// `Send + Sync`: the boxed backend is held behind a `&self` across `get`'s
/// `.await`, and the whole execution chain runs inside `tokio::spawn`, so the
/// backend future must be `Send` — which requires `&dyn ByteKeyValueStore` to be
/// `Send`, i.e. the trait object to be `Sync`.
#[async_trait]
pub(crate) trait ByteKeyValueStore: Send + Sync {
    async fn get(&self, key: &[u8]) -> Option<Bytes>;
    async fn put(&mut self, key: Bytes, value: Bytes);
    async fn delete(&mut self, key: &[u8]) -> Option<Bytes>;
    #[allow(dead_code)] // used by 4d-ii window store
    async fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<(Bytes, Bytes)>;
}

/// In-memory backend over a `BTreeMap` (ordered → serves `range`).
#[derive(Default)]
pub(crate) struct InMemoryBytes {
    map: BTreeMap<Bytes, Bytes>,
}

#[async_trait]
impl ByteKeyValueStore for InMemoryBytes {
    async fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).cloned()
    }
    async fn put(&mut self, key: Bytes, value: Bytes) {
        self.map.insert(key, value);
    }
    async fn delete(&mut self, key: &[u8]) -> Option<Bytes> {
        self.map.remove(key)
    }
    async fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<(Bytes, Bytes)> {
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

    #[tokio::test]
    async fn inmemory_put_get_delete_range_ordered() {
        let mut s = InMemoryBytes::default();
        s.put(Bytes::from_static(&[1, 0]), Bytes::from_static(b"a"))
            .await;
        s.put(Bytes::from_static(&[1, 2]), Bytes::from_static(b"b"))
            .await;
        s.put(Bytes::from_static(&[2, 0]), Bytes::from_static(b"c"))
            .await;
        check!(s.get(&[1, 2]).await == Some(Bytes::from_static(b"b")));
        let r = s.range(&[1, 0], &[2, 0]).await;
        check!(r.len() == 2);
        check!(r[0].1 == Bytes::from_static(b"a")); // ordered
        check!(s.delete(&[1, 0]).await == Some(Bytes::from_static(b"a")));
        check!(s.get(&[1, 0]).await == None);
    }
}
