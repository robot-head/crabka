//! The key-value storage seam. Gres ships an in-memory `MemKv` and a durable LSM
//! backend behind the same `Kv` trait.

use std::{collections::BTreeMap, sync::RwLock};

use crate::KvError;

const RESTORE_BATCH_SIZE: usize = 4_096;

/// One key-value pair returned by ordered scans.
pub type KvPair = (Vec<u8>, Vec<u8>);

/// Ordered scan result.
pub type KvScan = Vec<KvPair>;

/// One mutation in an atomic batch.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum WriteOp {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// Put only when the durable value still equals `expected`. `None` means the
    /// key must be absent. A failed conditional rejects the complete batch.
    ConditionalPut {
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
}

/// An ordered byte-key/byte-value store. Synchronous for the local engine; the
/// distributed layer will introduce an async, transactional variant behind this
/// boundary. All methods are fallible because a durable backend can hit I/O
/// errors.
pub trait Kv: Send + Sync {
    /// Read one value by key.
    ///
    /// # Errors
    ///
    /// Returns [`KvError`] when the backing store cannot complete the read.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError>;
    /// Insert or replace one key-value pair.
    ///
    /// # Errors
    ///
    /// Returns [`KvError`] when the backing store cannot persist the write.
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), KvError>;
    /// Delete one key-value pair.
    ///
    /// # Errors
    ///
    /// Returns [`KvError`] when the backing store cannot persist the deletion.
    fn delete(&self, key: &[u8]) -> Result<(), KvError>;
    /// All (key, value) pairs whose key starts with `prefix`, in key order.
    ///
    /// # Errors
    ///
    /// Returns [`KvError`] when the backing store cannot complete the scan.
    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvScan, KvError>;
    /// All (key, value) pairs with `start <= key < end`, in key order
    /// (inclusive start, exclusive end).
    ///
    /// # Errors
    ///
    /// Returns [`KvError`] when the backing store cannot complete the scan.
    fn scan_range(&self, start: &[u8], end: &[u8]) -> Result<KvScan, KvError>;
    /// Apply all ops atomically and durably (fsync on a durable backend).
    /// All-or-nothing across a crash.
    ///
    /// # Errors
    ///
    /// Returns [`KvError`] when validation fails or the backing store cannot
    /// persist the complete batch.
    fn write_batch(&self, ops: &[WriteOp]) -> Result<(), KvError>;
    /// Give the store a chance to retire shadowed data (LSM memtable rotation
    /// so flush + compaction can drop deleted entries and tombstones). Called
    /// after garbage-collection sweeps; a no-op for stores without background
    /// structure.
    ///
    /// # Errors
    ///
    /// Returns [`KvError`] when the backing store cannot start maintenance.
    fn maintain(&self) -> Result<(), KvError> {
        Ok(())
    }
}

/// A consistent point-in-time, key-ordered stream of committed key-value pairs.
pub trait KvSnapshot: Send {
    /// Returns the next pair in ascending key order, or `None` at end of stream.
    ///
    /// # Errors
    ///
    /// Returns [`KvError`] when the backing snapshot cannot read the next pair.
    fn next(&mut self) -> Result<Option<KvPair>, KvError>;
}

/// Stores that can produce an online whole-store snapshot.
pub trait SnapshotKv: Kv {
    /// Captures the store's committed state for deterministic key-ordered iteration.
    ///
    /// # Errors
    ///
    /// Returns [`KvError`] when the backing store cannot open the snapshot.
    fn snapshot(&self) -> Result<Box<dyn KvSnapshot>, KvError>;
}

/// Stores that can bulk-restore a strictly ascending snapshot stream.
pub trait RestoreKv: Kv {
    /// Restores all pairs from `pairs` into an empty store and returns the count.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::RestoreTargetNotEmpty`] when the destination already has
    /// data, [`KvError::UnsortedSnapshot`] when keys are not strictly ascending, or
    /// another [`KvError`] from the backing store or snapshot stream.
    fn restore_sorted(&self, pairs: &mut dyn KvSnapshot) -> Result<u64, KvError>;
}

/// In-memory ordered store backed by a `BTreeMap`. Infallible internally; returns
/// `Ok` to satisfy the fallible trait. Used for tests and the ephemeral default.
#[derive(Default)]
pub struct MemKv {
    map: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl MemKv {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn is_empty(&self) -> bool {
        self.map.read().expect("kv lock").is_empty()
    }
}

struct VecKvSnapshot {
    pairs: std::vec::IntoIter<KvPair>,
}

impl VecKvSnapshot {
    fn new(pairs: KvScan) -> Self {
        Self {
            pairs: pairs.into_iter(),
        }
    }
}

impl KvSnapshot for VecKvSnapshot {
    fn next(&mut self) -> Result<Option<KvPair>, KvError> {
        Ok(self.pairs.next())
    }
}

impl Kv for MemKv {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        Ok(self.map.read().expect("kv lock").get(key).cloned())
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), KvError> {
        self.map.write().expect("kv lock").insert(key, value);
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), KvError> {
        self.map.write().expect("kv lock").remove(key);
        Ok(())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvScan, KvError> {
        Ok(self
            .map
            .read()
            .expect("kv lock")
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    fn scan_range(&self, start: &[u8], end: &[u8]) -> Result<KvScan, KvError> {
        Ok(self
            .map
            .read()
            .expect("kv lock")
            .range(start.to_vec()..end.to_vec())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    fn write_batch(&self, ops: &[WriteOp]) -> Result<(), KvError> {
        let mut map = self.map.write().expect("kv lock");
        if ops.iter().any(|op| {
            matches!(op, WriteOp::ConditionalPut { key, expected, .. } if map.get(key) != expected.as_ref())
        }) {
            return Ok(());
        }
        for op in ops {
            match op {
                WriteOp::Put { key, value } | WriteOp::ConditionalPut { key, value, .. } => {
                    map.insert(key.clone(), value.clone());
                }
                WriteOp::Delete { key } => {
                    map.remove(key);
                }
            }
        }
        Ok(())
    }
}

impl SnapshotKv for MemKv {
    fn snapshot(&self) -> Result<Box<dyn KvSnapshot>, KvError> {
        let pairs = self
            .map
            .read()
            .expect("kv lock")
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();

        Ok(Box::new(VecKvSnapshot::new(pairs)))
    }
}

impl RestoreKv for MemKv {
    fn restore_sorted(&self, pairs: &mut dyn KvSnapshot) -> Result<u64, KvError> {
        if !self.is_empty() {
            return Err(KvError::RestoreTargetNotEmpty);
        }

        let mut previous_key: Option<Vec<u8>> = None;
        let mut ops = Vec::new();

        while let Some((key, value)) = pairs.next()? {
            if previous_key
                .as_deref()
                .is_some_and(|previous| previous >= key.as_slice())
            {
                return Err(KvError::UnsortedSnapshot);
            }

            previous_key = Some(key.clone());
            ops.push(WriteOp::Put { key, value });
        }

        let count = u64::try_from(ops.len())
            .map_err(|_| KvError::Io("snapshot pair count overflow".to_owned()))?;
        for chunk in ops.chunks(RESTORE_BATCH_SIZE) {
            self.write_batch(chunk)?;
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StaticSnapshot {
        pairs: std::vec::IntoIter<KvPair>,
    }

    impl StaticSnapshot {
        fn new(pairs: KvScan) -> Self {
            Self {
                pairs: pairs.into_iter(),
            }
        }
    }

    impl KvSnapshot for StaticSnapshot {
        fn next(&mut self) -> Result<Option<KvPair>, KvError> {
            Ok(self.pairs.next())
        }
    }

    struct FailingSnapshot {
        first_pair: Option<KvPair>,
    }

    impl FailingSnapshot {
        fn after_one_pair(pair: KvPair) -> Self {
            Self {
                first_pair: Some(pair),
            }
        }
    }

    impl KvSnapshot for FailingSnapshot {
        fn next(&mut self) -> Result<Option<KvPair>, KvError> {
            let Some(pair) = self.first_pair.take() else {
                return Err(KvError::Io("snapshot stream failed".to_owned()));
            };

            Ok(Some(pair))
        }
    }

    fn collect_snapshot(snapshot: &mut dyn KvSnapshot) -> Result<KvScan, KvError> {
        let mut pairs = Vec::new();

        while let Some(pair) = snapshot.next()? {
            pairs.push(pair);
        }

        Ok(pairs)
    }

    #[test]
    fn put_get_delete() {
        let kv = MemKv::new();
        assert_eq!(kv.get(b"a").expect("get"), None);
        kv.put(b"a".to_vec(), b"1".to_vec()).expect("put");
        assert_eq!(kv.get(b"a").expect("get"), Some(b"1".to_vec()));
        kv.delete(b"a").expect("delete");
        assert_eq!(kv.get(b"a").expect("get"), None);
    }

    #[test]
    fn scan_prefix_returns_sorted_matches_only() {
        let kv = MemKv::new();
        kv.put(b"t/1/b".to_vec(), b"B".to_vec()).expect("put");
        kv.put(b"t/1/a".to_vec(), b"A".to_vec()).expect("put");
        kv.put(b"t/2/a".to_vec(), b"X".to_vec()).expect("put");
        let rows = kv.scan_prefix(b"t/1/").expect("scan");
        assert_eq!(
            rows,
            vec![
                (b"t/1/a".to_vec(), b"A".to_vec()),
                (b"t/1/b".to_vec(), b"B".to_vec()),
            ]
        );
    }

    #[test]
    fn scan_range_returns_inclusive_start_exclusive_end_in_order() {
        let kv = MemKv::new();
        for i in [1u8, 3, 5, 7, 9] {
            kv.put(vec![b'k', i], vec![i]).expect("put");
        }
        let got = kv.scan_range(&[b'k', 3], &[b'k', 7]).expect("scan_range");
        assert_eq!(
            got,
            vec![(vec![b'k', 3], vec![3]), (vec![b'k', 5], vec![5])]
        );
        let all = kv.scan_range(&[b'k', 0], &[b'k', 255]).expect("scan_range");
        assert_eq!(all.len(), 5);
        assert!(
            kv.scan_range(&[b'k', 5], &[b'k', 5])
                .expect("scan")
                .is_empty()
        );
        assert!(
            kv.scan_range(&[b'k', 200], &[b'k', 255])
                .expect("scan")
                .is_empty()
        );
    }

    #[test]
    fn write_batch_applies_all_ops() {
        let kv = MemKv::new();
        kv.put(b"keep".to_vec(), b"0".to_vec()).expect("put");
        kv.write_batch(&[
            WriteOp::Put {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
            WriteOp::Put {
                key: b"b".to_vec(),
                value: b"2".to_vec(),
            },
            WriteOp::Delete {
                key: b"keep".to_vec(),
            },
        ])
        .expect("batch");
        assert_eq!(kv.get(b"a").expect("get"), Some(b"1".to_vec()));
        assert_eq!(kv.get(b"b").expect("get"), Some(b"2".to_vec()));
        assert_eq!(kv.get(b"keep").expect("get"), None);
    }

    #[test]
    fn snapshot_iterates_committed_state_in_key_order() {
        let kv = MemKv::new();
        kv.put(b"b".to_vec(), b"2".to_vec()).expect("put");
        kv.put(b"a".to_vec(), b"1".to_vec()).expect("put");

        let mut snapshot = kv.snapshot().expect("snapshot");
        kv.put(b"c".to_vec(), b"3".to_vec()).expect("put");
        kv.delete(b"a").expect("delete");

        assert_eq!(
            collect_snapshot(snapshot.as_mut()).expect("collect"),
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec())
            ],
        );
    }

    #[test]
    fn restore_sorted_round_trips_snapshot_into_empty_store() {
        let source = MemKv::new();
        source.put(b"b".to_vec(), b"2".to_vec()).expect("put");
        source.put(b"a".to_vec(), b"1".to_vec()).expect("put");

        let target = MemKv::new();
        let mut snapshot = source.snapshot().expect("snapshot");
        let restored = target.restore_sorted(snapshot.as_mut()).expect("restore");

        assert_eq!(restored, 2);
        assert_eq!(
            target.scan_range(b"a", b"c").expect("scan"),
            source.scan_range(b"a", b"c").expect("scan")
        );
    }

    #[test]
    fn restore_sorted_refuses_to_overwrite_existing_mem_store() {
        let target = MemKv::new();
        target
            .put(b"existing".to_vec(), b"value".to_vec())
            .expect("put");
        let mut snapshot = StaticSnapshot::new(vec![(b"a".to_vec(), b"1".to_vec())]);

        assert_eq!(
            target.restore_sorted(&mut snapshot),
            Err(KvError::RestoreTargetNotEmpty),
        );
        assert_eq!(
            target.get(b"existing").expect("get"),
            Some(b"value".to_vec())
        );
    }

    #[test]
    fn restore_sorted_rejects_unsorted_snapshot_without_partial_mem_writes() {
        let target = MemKv::new();
        let mut snapshot = StaticSnapshot::new(vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"a".to_vec(), b"1".to_vec()),
        ]);

        assert_eq!(
            target.restore_sorted(&mut snapshot),
            Err(KvError::UnsortedSnapshot)
        );
        assert!(target.scan_range(b"a", b"z").expect("scan").is_empty());
    }

    #[test]
    fn restore_sorted_rejects_partial_snapshot_without_partial_mem_writes() {
        let target = MemKv::new();
        let mut snapshot = FailingSnapshot::after_one_pair((b"a".to_vec(), b"1".to_vec()));

        assert_eq!(
            target.restore_sorted(&mut snapshot),
            Err(KvError::Io("snapshot stream failed".to_owned())),
        );
        assert!(target.scan_range(b"a", b"z").expect("scan").is_empty());
    }
}
