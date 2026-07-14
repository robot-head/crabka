//! Durable Kv over a fjall LSM partition. Crash recovery is fjall's journal
//! replay on open; durability is fsync on each commit.

use std::{path::Path, sync::Arc};

use fjall::{
    Iter, KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase,
    SingleWriterTxKeyspace, Snapshot,
};

use crate::{Kv, KvError, KvPair, KvSnapshot, RestoreKv, SnapshotKv, WriteOp, store::KvScan};

/// A `Kv` over one fjall keyspace within a (possibly shared) transactional
/// database. Every mutation fsyncs the whole database as its tail, so a returned
/// `Ok` is power-loss durable. Multiple `KeyspaceKv`s over the same transactional
/// database share that fsync (a single `persist` flushes all pending writes).
pub struct KeyspaceKv {
    db: Arc<SingleWriterTxDatabase>,
    ks: SingleWriterTxKeyspace,
    persist_mode: LocalPersistMode,
}

#[derive(Debug, Clone, Copy)]
enum LocalPersistMode {
    SyncAll,
    Buffer,
}

impl KeyspaceKv {
    /// Wrap an already-open keyspace `ks` belonging to `db`.
    #[must_use]
    pub fn new(db: Arc<SingleWriterTxDatabase>, ks: SingleWriterTxKeyspace) -> Self {
        Self {
            db,
            ks,
            persist_mode: LocalPersistMode::SyncAll,
        }
    }

    /// Wrap an already-open keyspace without fsyncing each mutation.
    #[must_use]
    pub fn new_cache(db: Arc<SingleWriterTxDatabase>, ks: SingleWriterTxKeyspace) -> Self {
        Self {
            db,
            ks,
            persist_mode: LocalPersistMode::Buffer,
        }
    }

    fn sync(&self) -> Result<(), KvError> {
        match self.persist_mode {
            LocalPersistMode::SyncAll => self.db.persist(PersistMode::SyncAll).map_err(io),
            LocalPersistMode::Buffer => self.db.persist(PersistMode::Buffer).map_err(io),
        }
    }

    fn is_empty(&self) -> Result<bool, KvError> {
        self.db.read_tx().is_empty(&self.ks).map_err(io)
    }
}

struct FjallSnapshot {
    _snapshot: Snapshot,
    iter: Iter,
}

impl FjallSnapshot {
    fn new(snapshot: Snapshot, ks: &SingleWriterTxKeyspace) -> Self {
        let iter = snapshot.iter(ks);
        Self {
            _snapshot: snapshot,
            iter,
        }
    }
}

impl KvSnapshot for FjallSnapshot {
    fn next(&mut self) -> Result<Option<KvPair>, KvError> {
        let Some(guard) = self.iter.next() else {
            return Ok(None);
        };

        let (key, value) = guard.into_inner().map_err(io)?;
        Ok(Some((key.to_vec(), value.to_vec())))
    }
}

fn io(e: impl std::fmt::Display) -> KvError {
    KvError::Io(e.to_string())
}

impl Kv for KeyspaceKv {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        Ok(self
            .db
            .read_tx()
            .get(&self.ks, key)
            .map_err(io)?
            .map(|value| value.to_vec()))
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), KvError> {
        self.write_batch(&[WriteOp::Put { key, value }])
    }

    fn delete(&self, key: &[u8]) -> Result<(), KvError> {
        self.write_batch(&[WriteOp::Delete { key: key.to_vec() }])
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvScan, KvError> {
        let snapshot = self.db.read_tx();
        let mut out = Vec::new();
        for guard in snapshot.prefix(&self.ks, prefix) {
            let (k, v) = guard.into_inner().map_err(io)?;
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }

    fn scan_range(&self, start: &[u8], end: &[u8]) -> Result<KvScan, KvError> {
        let snapshot = self.db.read_tx();
        let mut out = Vec::new();
        for guard in snapshot.range(&self.ks, start.to_vec()..end.to_vec()) {
            let (k, v) = guard.into_inner().map_err(io)?;
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }

    fn write_batch(&self, ops: &[WriteOp]) -> Result<(), KvError> {
        // Fjall's single-writer transaction serializes every handle sharing this
        // database. Fjall holds the database directory lock while it is open, so
        // another process cannot open a competing writer for this directory.
        let mut transaction = self.db.write_tx();

        // Evaluate every expectation before staging any mutation. MemKv defines
        // conditional puts against the durable state at the start of the batch;
        // using transaction reads while applying would accidentally make a later
        // conditional observe an earlier staged write.
        for op in ops {
            let WriteOp::ConditionalPut { key, expected, .. } = op else {
                continue;
            };
            if transaction.get(&self.ks, key).map_err(io)?.as_deref() != expected.as_deref() {
                // Match MemKv's compare-and-swap contract: reject the complete
                // batch without changing durable state.
                return Ok(());
            }
        }
        for op in ops {
            match op {
                WriteOp::Put { key, value } | WriteOp::ConditionalPut { key, value, .. } => {
                    transaction.insert(&self.ks, key, value);
                }
                WriteOp::Delete { key } => {
                    transaction.remove(&self.ks, key);
                }
            }
        }
        transaction.commit().map_err(io)?;
        self.sync()
    }

    fn maintain(&self) -> Result<(), KvError> {
        // Rotate the active memtable so flush + compaction retire shadowed
        // entries and GC tombstones. Byte-size rotation alone lets small-value
        // churn (MVCC version keys and their delete tombstones on one row
        // prefix) linger in the memtable for minutes, and every prefix scan
        // walks all of it.
        self.ks.inner().rotate_memtable_and_wait().map_err(io)
    }
}

impl SnapshotKv for KeyspaceKv {
    fn snapshot(&self) -> Result<Box<dyn KvSnapshot>, KvError> {
        let snapshot = self.db.read_tx();
        Ok(Box::new(FjallSnapshot::new(snapshot, &self.ks)))
    }
}

impl RestoreKv for KeyspaceKv {
    fn restore_sorted(&self, pairs: &mut dyn KvSnapshot) -> Result<u64, KvError> {
        if !self.is_empty()? {
            return Err(KvError::RestoreTargetNotEmpty);
        }

        // Fjall does not expose ingestion through `SingleWriterTxKeyspace`.
        // Ingestion itself serializes its final registration with concurrent
        // writers; use the wrapped keyspace only for this API gap.
        let mut ingestion = self.ks.inner().start_ingestion().map_err(io)?;
        let mut previous_key: Option<Vec<u8>> = None;
        let mut count = 0_u64;

        while let Some((key, value)) = pairs.next()? {
            if previous_key
                .as_deref()
                .is_some_and(|previous| previous >= key.as_slice())
            {
                return Err(KvError::UnsortedSnapshot);
            }

            previous_key = Some(key.clone());
            ingestion.write(key, value).map_err(io)?;
            count = count
                .checked_add(1)
                .ok_or_else(|| KvError::Io("snapshot pair count overflow".to_owned()))?;
        }

        ingestion.finish().map_err(io)?;
        self.db.persist(PersistMode::SyncAll).map_err(io)?;

        Ok(count)
    }
}

/// Durable single-keyspace `Kv`: opens (or recovers) a one-keyspace `Database`.
///
/// Opening an existing directory recovers via fjall's journal replay — no
/// bespoke recovery code required. Every write is fsynced before returning.
pub struct FjallKv {
    inner: KeyspaceKv,
}

/// Memtable cap for crabka keyspaces. Fjall's 64 MiB default means MVCC
/// churn (distinct version keys plus GC tombstones on the same row prefix)
/// accumulates in the active memtable for a very long time, and every prefix
/// scan walks all of it — measured as a hot-row throughput collapse. A small
/// cap rotates the memtable early so flush + compaction retire shadowed
/// entries and tombstones.
const MAX_MEMTABLE_SIZE_BYTES: u64 = 8 * 1024 * 1024;

fn crabka_keyspace_options() -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default().max_memtable_size(MAX_MEMTABLE_SIZE_BYTES)
}

impl FjallKv {
    /// Opens (or creates) a `FjallKv` at the given path.
    ///
    /// If the directory already contains a database, it is recovered via fjall's
    /// journal replay.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::Io`] when the database or keyspace cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, KvError> {
        let db = Arc::new(SingleWriterTxDatabase::builder(path).open().map_err(io)?);
        let ks = db.keyspace("data", crabka_keyspace_options).map_err(io)?;
        Ok(Self {
            inner: KeyspaceKv::new(db, ks),
        })
    }

    /// Opens (or creates) a `FjallKv` cache at the given path.
    ///
    /// Mutations are visible to this process immediately but are not fsynced per
    /// operation. This mode is for the Gres substrate read model, where the WAL
    /// topic is the durable truth and this local store is disposable.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::Io`] when the database or keyspace cannot be opened.
    pub fn open_cache(path: impl AsRef<Path>) -> Result<Self, KvError> {
        let db = Arc::new(SingleWriterTxDatabase::builder(path).open().map_err(io)?);
        let ks = db.keyspace("data", crabka_keyspace_options).map_err(io)?;
        Ok(Self {
            inner: KeyspaceKv::new_cache(db, ks),
        })
    }
}

impl Kv for FjallKv {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.inner.get(key)
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), KvError> {
        self.inner.put(key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), KvError> {
        self.inner.delete(key)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvScan, KvError> {
        self.inner.scan_prefix(prefix)
    }

    fn scan_range(&self, start: &[u8], end: &[u8]) -> Result<KvScan, KvError> {
        self.inner.scan_range(start, end)
    }

    fn write_batch(&self, ops: &[WriteOp]) -> Result<(), KvError> {
        self.inner.write_batch(ops)
    }

    fn maintain(&self) -> Result<(), KvError> {
        self.inner.maintain()
    }
}

impl SnapshotKv for FjallKv {
    fn snapshot(&self) -> Result<Box<dyn KvSnapshot>, KvError> {
        self.inner.snapshot()
    }
}

impl RestoreKv for FjallKv {
    fn restore_sorted(&self, pairs: &mut dyn KvSnapshot) -> Result<u64, KvError> {
        self.inner.restore_sorted(pairs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KvPair, WriteOp, store::KvScan};

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

    fn temp() -> tempfile::TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    #[test]
    fn put_get_delete_durable() {
        let dir = temp();
        let kv = FjallKv::open(dir.path()).expect("open");
        assert_eq!(kv.get(b"a").expect("get"), None);
        kv.put(b"a".to_vec(), b"1".to_vec()).expect("put");
        assert_eq!(kv.get(b"a").expect("get"), Some(b"1".to_vec()));
        kv.delete(b"a").expect("delete");
        assert_eq!(kv.get(b"a").expect("get"), None);
    }

    #[test]
    fn scan_prefix_ordered_matches_only() {
        let dir = temp();
        let kv = FjallKv::open(dir.path()).expect("open");
        kv.put(b"t/1/b".to_vec(), b"B".to_vec()).expect("put");
        kv.put(b"t/1/a".to_vec(), b"A".to_vec()).expect("put");
        kv.put(b"t/2/a".to_vec(), b"X".to_vec()).expect("put");
        assert_eq!(
            kv.scan_prefix(b"t/1/").expect("scan"),
            vec![
                (b"t/1/a".to_vec(), b"A".to_vec()),
                (b"t/1/b".to_vec(), b"B".to_vec()),
            ]
        );
    }

    #[test]
    fn scan_range_inclusive_start_exclusive_end_on_fjall() {
        let dir = temp();
        let kv = FjallKv::open(dir.path()).expect("open");
        for i in [1u8, 3, 5, 7, 9] {
            kv.put(vec![b'k', i], vec![i]).expect("put");
        }
        assert_eq!(
            kv.scan_range(&[b'k', 3], &[b'k', 7]).expect("scan_range"),
            vec![(vec![b'k', 3], vec![3]), (vec![b'k', 5], vec![5])],
        );
        assert_eq!(
            kv.scan_range(&[b'k', 0], &[b'k', 255]).expect("scan").len(),
            5
        );
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
    fn cache_batch_is_immediately_visible_to_point_and_range_reads() {
        let dir = temp();
        let kv = FjallKv::open_cache(dir.path()).expect("open cache");

        kv.write_batch(&[WriteOp::Put {
            key: b"row/21".to_vec(),
            value: b"committed".to_vec(),
        }])
        .expect("batch");

        assert_eq!(
            kv.get(b"row/21").expect("point read"),
            Some(b"committed".to_vec())
        );
        assert_eq!(
            kv.scan_range(b"row/", b"row0").expect("range read"),
            vec![(b"row/21".to_vec(), b"committed".to_vec())]
        );
    }

    #[test]
    fn write_batch_is_atomic() {
        let dir = temp();
        let kv = FjallKv::open(dir.path()).expect("open");
        kv.put(b"keep".to_vec(), b"0".to_vec()).expect("put");
        kv.write_batch(&[
            WriteOp::Put {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
            WriteOp::Delete {
                key: b"keep".to_vec(),
            },
        ])
        .expect("batch");
        assert_eq!(kv.get(b"a").expect("get"), Some(b"1".to_vec()));
        assert_eq!(kv.get(b"keep").expect("get"), None);
    }

    #[test]
    fn conditional_puts_observe_the_batch_start_snapshot() {
        let dir = temp();
        let kv = FjallKv::open(dir.path()).expect("open");
        kv.write_batch(&[
            WriteOp::Put {
                key: b"created".to_vec(),
                value: b"value".to_vec(),
            },
            WriteOp::ConditionalPut {
                key: b"created".to_vec(),
                expected: None,
                value: b"unexpected".to_vec(),
            },
        ])
        .expect("batch");
        assert_eq!(
            kv.get(b"created").expect("get"),
            Some(b"unexpected".to_vec())
        );
    }

    #[test]
    fn conditional_put_ordering_uses_one_durable_snapshot() {
        let dir = temp();
        let kv = FjallKv::open(dir.path()).expect("open");
        kv.put(b"cas".to_vec(), b"before".to_vec()).expect("put");
        kv.write_batch(&[
            WriteOp::ConditionalPut {
                key: b"cas".to_vec(),
                expected: Some(b"before".to_vec()),
                value: b"first".to_vec(),
            },
            WriteOp::ConditionalPut {
                key: b"cas".to_vec(),
                expected: Some(b"first".to_vec()),
                value: b"second".to_vec(),
            },
        ])
        .expect("batch");
        assert_eq!(kv.get(b"cas").expect("get"), Some(b"before".to_vec()));
    }

    #[test]
    fn conditional_put_serializes_cross_handle_contention() {
        use std::sync::{Arc, Barrier};

        let dir = temp();
        let opened = FjallKv::open(dir.path()).expect("open first handle");
        let first = Arc::new(KeyspaceKv::new(
            Arc::clone(&opened.inner.db),
            opened.inner.ks.clone(),
        ));
        let second = Arc::new(KeyspaceKv::new(
            Arc::clone(&opened.inner.db),
            opened.inner.ks.clone(),
        ));
        let barrier = Arc::new(Barrier::new(2));

        std::thread::scope(|scope| {
            let first_barrier = Arc::clone(&barrier);
            let first_handle = Arc::clone(&first);
            let first_writer = scope.spawn(move || {
                first_barrier.wait();
                first_handle.write_batch(&[WriteOp::ConditionalPut {
                    key: b"descriptor".to_vec(),
                    expected: None,
                    value: b"first-terminal".to_vec(),
                }])
            });
            let second_barrier = Arc::clone(&barrier);
            let second_handle = Arc::clone(&second);
            let second_writer = scope.spawn(move || {
                second_barrier.wait();
                second_handle.write_batch(&[WriteOp::ConditionalPut {
                    key: b"descriptor".to_vec(),
                    expected: None,
                    value: b"second-terminal".to_vec(),
                }])
            });
            first_writer
                .join()
                .expect("first writer")
                .expect("first cas");
            second_writer
                .join()
                .expect("second writer")
                .expect("second cas");
        });

        let winner = opened.get(b"descriptor").expect("read");
        assert!(matches!(
            winner.as_deref(),
            Some(b"first-terminal" | b"second-terminal")
        ));
        opened
            .write_batch(&[WriteOp::ConditionalPut {
                key: b"descriptor".to_vec(),
                expected: Some(b"missing".to_vec()),
                value: b"unexpected".to_vec(),
            }])
            .expect("rejected cas");
        assert_eq!(opened.get(b"descriptor").expect("read"), winner);
        drop(first);
        drop(second);
        drop(opened);

        let reopened = FjallKv::open(dir.path()).expect("reopen");
        assert_eq!(reopened.get(b"descriptor").expect("durable read"), winner);
    }

    #[test]
    fn data_survives_reopen() {
        let dir = temp();
        {
            let kv = FjallKv::open(dir.path()).expect("open");
            kv.put(b"persist".to_vec(), b"yes".to_vec()).expect("put");
        }
        let kv = FjallKv::open(dir.path()).expect("reopen");
        assert_eq!(kv.get(b"persist").expect("get"), Some(b"yes".to_vec()));
    }

    #[test]
    fn cache_mode_round_trips_within_process() {
        let dir = temp();
        let kv = FjallKv::open_cache(dir.path()).expect("open cache");

        kv.put(b"cache".to_vec(), b"visible".to_vec()).expect("put");
        kv.write_batch(&[WriteOp::Put {
            key: b"batch".to_vec(),
            value: b"visible".to_vec(),
        }])
        .expect("batch");

        assert_eq!(kv.get(b"cache").expect("get"), Some(b"visible".to_vec()));
        assert_eq!(kv.get(b"batch").expect("get"), Some(b"visible".to_vec()));
    }

    #[test]
    fn snapshot_iterates_durable_committed_state_in_key_order() {
        let dir = temp();
        let kv = FjallKv::open(dir.path()).expect("open");
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
    fn restore_sorted_round_trips_snapshot_into_empty_durable_store() {
        let source_dir = temp();
        let source = FjallKv::open(source_dir.path()).expect("open source");
        source.put(b"b".to_vec(), b"2".to_vec()).expect("put");
        source.put(b"a".to_vec(), b"1".to_vec()).expect("put");

        let target_dir = temp();
        let target = FjallKv::open(target_dir.path()).expect("open target");
        let mut snapshot = source.snapshot().expect("snapshot");
        let restored = target.restore_sorted(snapshot.as_mut()).expect("restore");

        assert_eq!(restored, 2);
        assert_eq!(
            target.scan_range(b"a", b"c").expect("scan"),
            source.scan_range(b"a", b"c").expect("scan"),
        );
    }

    #[test]
    fn restore_sorted_persists_durable_store() {
        let dir = temp();
        {
            let target = FjallKv::open(dir.path()).expect("open target");
            let mut snapshot = StaticSnapshot::new(vec![(b"a".to_vec(), b"1".to_vec())]);
            assert_eq!(target.restore_sorted(&mut snapshot).expect("restore"), 1);
        }

        let reopened = FjallKv::open(dir.path()).expect("reopen");
        assert_eq!(reopened.get(b"a").expect("get"), Some(b"1".to_vec()));
    }

    #[test]
    fn restore_sorted_refuses_to_overwrite_existing_durable_store() {
        let dir = temp();
        let target = FjallKv::open(dir.path()).expect("open target");
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
    fn restore_sorted_rejects_unsorted_snapshot_without_finishing_ingestion() {
        let dir = temp();
        let target = FjallKv::open(dir.path()).expect("open target");
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
    fn restore_sorted_rejects_partial_snapshot_without_finishing_ingestion() {
        let dir = temp();
        let target = FjallKv::open(dir.path()).expect("open target");
        let mut snapshot = FailingSnapshot::after_one_pair((b"a".to_vec(), b"1".to_vec()));

        assert_eq!(
            target.restore_sorted(&mut snapshot),
            Err(KvError::Io("snapshot stream failed".to_owned())),
        );
        assert!(target.scan_range(b"a", b"z").expect("scan").is_empty());
    }
}
