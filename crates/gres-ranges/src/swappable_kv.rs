//! A [`Kv`] whose backing store can be replaced under its holders.
//!
//! A consumer of a range-0 follower captures its catalog store handle exactly
//! once, at construction, and keeps it forever. A follower that has to rebuild
//! itself from a checkpoint must therefore be able to swap the store *behind*
//! that handle. A replacement of the handle is not an option, because nothing
//! would ever read the replacement.

use std::sync::Arc;

use arc_swap::ArcSwap;
use crabka_pgkv::{Kv, KvError, KvScan, WriteOp};

/// A `Kv` that forwards every call to the store that is currently installed.
///
/// The indirection costs one pointer load per call. Holders keep one stable
/// `Arc<SwappableKv>`, and [`SwappableKv::swap`] exchanges the store they reach
/// through it.
pub struct SwappableKv {
    // `Arc<Arc<dyn Kv>>`: `arc_swap` can only swap sized payloads, so the
    // trait object lives one indirection down.
    inner: ArcSwap<Arc<dyn Kv>>,
}

impl SwappableKv {
    /// Wrap `store` in a handle whose backing store can later be replaced.
    #[must_use]
    pub fn new(store: Arc<dyn Kv>) -> Self {
        Self {
            inner: ArcSwap::from_pointee(store),
        }
    }

    /// Install `store` for every later call through this handle.
    ///
    /// A call already in flight finishes against the store it loaded. The
    /// previous store stays alive until the last such call returns.
    pub fn swap(&self, store: Arc<dyn Kv>) {
        self.inner.store(Arc::new(store));
    }
}

impl std::fmt::Debug for SwappableKv {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SwappableKv(..)")
    }
}

impl Kv for SwappableKv {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.inner.load().get(key)
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), KvError> {
        self.inner.load().put(key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), KvError> {
        self.inner.load().delete(key)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvScan, KvError> {
        self.inner.load().scan_prefix(prefix)
    }

    fn scan_range(&self, start: &[u8], end: &[u8]) -> Result<KvScan, KvError> {
        self.inner.load().scan_range(start, end)
    }

    fn for_each_key(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        visit: &mut dyn FnMut(&[u8]),
    ) -> Result<usize, KvError> {
        self.inner.load().for_each_key(start, end, limit, visit)
    }

    fn write_batch(&self, ops: &[WriteOp]) -> Result<(), KvError> {
        self.inner.load().write_batch(ops)
    }

    fn maintain(&self) -> Result<(), KvError> {
        self.inner.load().maintain()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgkv::MemKv;

    use super::*;

    fn seeded(key: &[u8], value: &[u8]) -> Arc<dyn Kv> {
        let store = Arc::new(MemKv::default());
        store.put(key.to_vec(), value.to_vec()).expect("seed");
        store
    }

    #[test]
    fn every_operation_reaches_the_installed_store() {
        let backing = Arc::new(MemKv::default());
        let swappable = SwappableKv::new(Arc::clone(&backing) as Arc<dyn Kv>);

        swappable
            .put(b"a".to_vec(), b"1".to_vec())
            .expect("put through handle");
        swappable
            .write_batch(&[WriteOp::Put {
                key: b"b".to_vec(),
                value: b"2".to_vec(),
            }])
            .expect("batch through handle");
        swappable.maintain().expect("maintain through handle");

        assert!(backing.get(b"a").expect("backing get") == Some(b"1".to_vec()));
        assert!(swappable.get(b"b").expect("get") == Some(b"2".to_vec()));
        assert!(
            swappable.scan_prefix(b"").expect("scan prefix")
                == vec![
                    (b"a".to_vec(), b"1".to_vec()),
                    (b"b".to_vec(), b"2".to_vec()),
                ]
        );
        assert!(
            swappable.scan_range(b"a", b"b").expect("scan range")
                == vec![(b"a".to_vec(), b"1".to_vec())]
        );

        swappable.delete(b"a").expect("delete through handle");
        assert!(backing.get(b"a").expect("backing get") == None);
    }

    #[test]
    fn a_handle_captured_before_the_swap_reads_the_new_store() {
        let swappable = Arc::new(SwappableKv::new(seeded(b"catalog", b"old")));
        // Exactly what a consumer does: capture the handle once, keep it.
        let captured: Arc<dyn Kv> = Arc::clone(&swappable) as Arc<dyn Kv>;

        swappable.swap(seeded(b"catalog", b"new"));

        assert!(captured.get(b"catalog").expect("get") == Some(b"new".to_vec()));
    }

    #[test]
    fn the_replaced_store_stops_receiving_writes() {
        let first = Arc::new(MemKv::default());
        let second = Arc::new(MemKv::default());
        let swappable = SwappableKv::new(Arc::clone(&first) as Arc<dyn Kv>);

        swappable.swap(Arc::clone(&second) as Arc<dyn Kv>);
        swappable
            .put(b"k".to_vec(), b"v".to_vec())
            .expect("put after swap");

        assert!(first.get(b"k").expect("get") == None);
        assert!(second.get(b"k").expect("get") == Some(b"v".to_vec()));
    }
}
