//! Durable Kv over a fjall LSM partition. Crash recovery is fjall's journal
//! replay on open; durability is one fsync per commit group — concurrent
//! `write_batch` callers coalesce into a single fjall transaction and share
//! its trailing fsync.

use std::{
    collections::VecDeque,
    path::Path,
    sync::{
        Arc, Condvar, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

use fjall::{
    Iter, KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase,
    SingleWriterTxKeyspace, Snapshot,
};

use crate::{Kv, KvError, KvPair, KvSnapshot, RestoreKv, SnapshotKv, WriteOp, store::KvScan};

/// A `Kv` over one fjall keyspace within a (possibly shared) transactional
/// database. Mutations are group-committed: concurrent `write_batch` callers
/// coalesce into one fjall transaction whose tail is a single whole-database
/// fsync, so a returned `Ok` is power-loss durable and N concurrent writers
/// share one fsync instead of paying one each. Multiple `KeyspaceKv`s over the
/// same transactional database also share that fsync (a single `persist`
/// flushes all pending writes).
///
/// Sustained writes rotate the active memtable every [`ROTATE_AFTER_OPS`]
/// committed ops (on top of fjall's byte-based trigger), so fjall's worker
/// pool keeps flushing sstables and compacting instead of letting shadowed
/// entries pile up on hot key prefixes.
pub struct KeyspaceKv {
    db: Arc<SingleWriterTxDatabase>,
    ks: SingleWriterTxKeyspace,
    persist_mode: LocalPersistMode,
    group: GroupCommit,
    /// Ops committed since the last memtable rotation this handle requested;
    /// crossing [`ROTATE_AFTER_OPS`] resets it and rotates.
    ops_since_rotate: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
enum LocalPersistMode {
    SyncAll,
    Buffer,
}

/// Leader/follower group-commit coordinator for one `KeyspaceKv`.
///
/// A `write_batch` caller that finds no leader running becomes the leader: it
/// drains the queue, applies every queued batch plus its own in one fjall
/// transaction, commits, fsyncs once, and completes every drained slot. A
/// caller that finds a leader running enqueues its batch and waits; batches
/// that arrive during the leader's fsync form the next group. The leader never
/// holds `state` while it touches fjall's single-writer transaction, so the
/// coordinator cannot deadlock with fjall's own write serialization.
#[derive(Default)]
struct GroupCommit {
    state: Mutex<GroupState>,
    /// Signalled by a finishing leader; wakes completed followers to return
    /// and still-queued followers to elect the next leader.
    wake: Condvar,
}

#[derive(Default)]
struct GroupState {
    /// Batches awaiting the next group, in enqueue (application) order.
    queue: VecDeque<Pending>,
    /// Whether a leader is currently committing a group.
    leader_active: bool,
}

/// One enqueued batch and the slot its leader completes it through.
struct Pending {
    ops: Vec<WriteOp>,
    slot: Arc<OnceLock<Result<(), KvError>>>,
}

/// Rotate the active memtable after this many committed write ops.
///
/// Fjall's only automatic rotation trigger is byte-based
/// ([`MAX_MEMTABLE_SIZE_BYTES`]), but the cost of scanning a hot key prefix is
/// per *entry*: every rewrite of a key leaves another shadowed version in the
/// active memtable's skiplist until rotation, and each MVCC prefix read skims
/// all of them. Small values accumulate tens of thousands of entries — a
/// measured 0.05ms -> 13ms hot-row read collapse — before the byte cap fires.
/// Counting ops bounds that skim directly; rotation seals the memtable and
/// fjall's worker pool flushes it to an sstable (dropping shadowed versions at
/// flush) and then compacts.
const ROTATE_AFTER_OPS: u64 = 1024;

/// Hands leadership off when a leader finishes — including by unwinding.
///
/// A leader panicking inside fjall must not leave `leader_active` set (every
/// later writer would block forever) or its drained batches uncompleted
/// (their callers would block forever, and re-electing a leader cannot help
/// because the drained batches are no longer queued). Completing on `Drop`
/// turns a leader panic into propagated errors instead of a stalled store.
struct LeaderHandoff<'a> {
    coordinator: &'a GroupCommit,
    group: &'a [Pending],
}

impl Drop for LeaderHandoff<'_> {
    fn drop(&mut self) {
        for pending in self.group {
            // A no-op on the normal path, where the leader already completed
            // every slot with the group's real result.
            let _unreached = pending
                .slot
                .set(Err(KvError::Io("group commit leader panicked".to_owned())));
        }
        // `if let` rather than `expect`: never double-panic during an unwind.
        if let Ok(mut state) = self.coordinator.state.lock() {
            state.leader_active = false;
            self.coordinator.wake.notify_all();
        }
    }
}

impl KeyspaceKv {
    /// Wrap an already-open keyspace `ks` belonging to `db`.
    #[must_use]
    pub fn new(db: Arc<SingleWriterTxDatabase>, ks: SingleWriterTxKeyspace) -> Self {
        Self {
            db,
            ks,
            persist_mode: LocalPersistMode::SyncAll,
            group: GroupCommit::default(),
            ops_since_rotate: AtomicU64::new(0),
        }
    }

    /// Wrap an already-open keyspace without fsyncing each mutation.
    #[must_use]
    pub fn new_cache(db: Arc<SingleWriterTxDatabase>, ks: SingleWriterTxKeyspace) -> Self {
        Self {
            db,
            ks,
            persist_mode: LocalPersistMode::Buffer,
            group: GroupCommit::default(),
            ops_since_rotate: AtomicU64::new(0),
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

    /// Group-commit a batch: lead a group if no leader is running, otherwise
    /// enqueue and wait for a leader to commit it.
    fn write_batch_grouped(&self, ops: &[WriteOp]) -> Result<(), KvError> {
        let mut state = self.group.state.lock().expect("group commit lock");
        if !state.leader_active {
            state.leader_active = true;
            let group: Vec<Pending> = state.queue.drain(..).collect();
            drop(state);
            // Own ops apply last: everything queued was enqueued earlier.
            return self.lead(&group, Some(ops));
        }

        let slot = Arc::new(OnceLock::new());
        state.queue.push_back(Pending {
            ops: ops.to_vec(),
            slot: Arc::clone(&slot),
        });
        state = self
            .group
            .wake
            .wait_while(state, |state| state.leader_active && slot.get().is_none())
            .expect("group commit lock");
        if let Some(result) = slot.get() {
            return result.clone();
        }
        // No leader is running and this batch is still queued (a leader sets
        // every drained slot before clearing `leader_active`, and both reads
        // happen under `state`): lead the group that accumulated behind the
        // previous leader. This batch keeps its enqueue position in `group`.
        state.leader_active = true;
        let group: Vec<Pending> = state.queue.drain(..).collect();
        drop(state);
        self.lead(&group, None)
    }

    /// Commit `group` (plus the leader's own trailing batch, when it is not
    /// already queued) as one transaction with one fsync, then complete every
    /// slot and hand off leadership.
    fn lead(&self, group: &[Pending], trailing_ops: Option<&[WriteOp]>) -> Result<(), KvError> {
        let handoff = LeaderHandoff {
            coordinator: &self.group,
            group,
        };
        let result = self.commit_group(
            group
                .iter()
                .map(|pending| pending.ops.as_slice())
                .chain(trailing_ops),
        );
        // A commit or persist failure fails every batch in the group: none of
        // them became durable. Slots are completed before `handoff` clears
        // `leader_active`; a follower observing no leader while its slot is
        // still empty can therefore conclude its batch is still queued.
        for pending in group {
            pending
                .slot
                .set(result.clone())
                .expect("each pending batch is completed by exactly one leader");
        }
        drop(handoff);
        result
    }

    /// Apply `batches` in order inside one fjall transaction and persist once
    /// via [`Self::sync`]. Equivalent to committing each batch sequentially:
    /// a batch's conditional reads observe earlier batches' staged writes.
    fn commit_group<'ops, I>(&self, batches: I) -> Result<(), KvError>
    where
        I: IntoIterator<Item = &'ops [WriteOp]>,
    {
        // Fjall's single-writer transaction serializes every handle sharing
        // this database. Fjall holds the database directory lock while it is
        // open, so another process cannot open a competing writer for this
        // directory.
        let mut transaction = self.db.write_tx();
        let mut staged_ops = 0_u64;

        for ops in batches {
            // Evaluate every expectation before staging any of this batch's
            // mutations. MemKv defines conditional puts against the state at
            // the start of the batch; a conditional must not observe its own
            // batch's earlier staged writes.
            let mut expectations_hold = true;
            for op in ops {
                let WriteOp::ConditionalPut { key, expected, .. } = op else {
                    continue;
                };
                if transaction.get(&self.ks, key).map_err(io)?.as_deref() != expected.as_deref() {
                    expectations_hold = false;
                    break;
                }
            }
            if !expectations_hold {
                // Match MemKv's compare-and-swap contract: reject the complete
                // batch (Ok, no state change) without failing the group's
                // other batches.
                continue;
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
            staged_ops += ops.len() as u64;
        }

        if staged_ops == 0 {
            // Every batch was rejected (or empty); dropping the transaction
            // discards nothing and there is nothing to make durable.
            return Ok(());
        }
        transaction.commit().map_err(io)?;
        self.sync()?;
        self.rotate_after_ops(staged_ops)
    }

    /// Credit `staged_ops` toward [`ROTATE_AFTER_OPS`] and rotate the active
    /// memtable once the threshold is crossed. Runs strictly after the group's
    /// commit and persist, so it cannot weaken the durability of a returned
    /// `Ok`; rotation itself returns immediately and fjall's worker pool
    /// flushes and compacts in the background.
    fn rotate_after_ops(&self, staged_ops: u64) -> Result<(), KvError> {
        let before = self
            .ops_since_rotate
            .fetch_add(staged_ops, Ordering::Relaxed);
        if before + staged_ops < ROTATE_AFTER_OPS {
            return Ok(());
        }
        self.ops_since_rotate.store(0, Ordering::Relaxed);
        // Racing rotations (another handle, fjall's own byte-cap trigger) are
        // benign: whoever loses finds a fresh or already-sealed memtable and
        // no-ops.
        self.ks.inner().rotate_memtable().map_err(io)?;
        Ok(())
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
        match self.persist_mode {
            // Group commit amortizes the per-commit fsync across concurrent
            // callers; a lone caller leads a group of one with no extra wait.
            LocalPersistMode::SyncAll => self.write_batch_grouped(ops),
            // Buffer mode has no durability wait to amortize; commit inline.
            LocalPersistMode::Buffer => self.commit_group(std::iter::once(ops)),
        }
    }

    fn maintain(&self) -> Result<(), KvError> {
        // Rotate the active memtable so flush + compaction retire shadowed
        // entries and GC tombstones. Byte-size rotation alone lets small-value
        // churn (MVCC version keys and their delete tombstones on one row
        // prefix) linger in the memtable for minutes, and every prefix scan
        // walks all of it.
        self.ops_since_rotate.store(0, Ordering::Relaxed);
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
    fn concurrent_write_batches_are_all_durable() {
        use std::sync::Barrier;

        use assert2::assert;

        const WRITERS: usize = 8;
        const BATCHES_PER_WRITER: usize = 5;

        let dir = temp();
        {
            let kv = FjallKv::open(dir.path()).expect("open");
            let barrier = Barrier::new(WRITERS);
            std::thread::scope(|scope| {
                for writer in 0..WRITERS {
                    let kv = &kv;
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        for batch in 0..BATCHES_PER_WRITER {
                            kv.write_batch(&[WriteOp::Put {
                                key: format!("w{writer}/b{batch}").into_bytes(),
                                value: format!("{writer}-{batch}").into_bytes(),
                            }])
                            .expect("write batch");
                        }
                    });
                }
            });
        }

        // Every batch returned Ok before the handle dropped, so every write
        // must survive reopen (journal replay of fsynced state).
        let reopened = FjallKv::open(dir.path()).expect("reopen");
        for writer in 0..WRITERS {
            for batch in 0..BATCHES_PER_WRITER {
                let key = format!("w{writer}/b{batch}");
                let value = reopened.get(key.as_bytes()).expect("get");
                assert!(
                    value == Some(format!("{writer}-{batch}").into_bytes()),
                    "key {key}"
                );
            }
        }
    }

    #[test]
    fn group_applies_batches_in_enqueue_order_within_one_transaction() {
        use assert2::assert;

        let dir = temp();
        let kv = FjallKv::open(dir.path()).expect("open");
        kv.put(b"k".to_vec(), b"old".to_vec()).expect("seed");

        // The second batch's conditional observes the first batch's staged
        // (not yet committed) write, exactly as sequential application would.
        kv.inner
            .commit_group([
                &[WriteOp::Put {
                    key: b"k".to_vec(),
                    value: b"first".to_vec(),
                }][..],
                &[WriteOp::ConditionalPut {
                    key: b"k".to_vec(),
                    expected: Some(b"first".to_vec()),
                    value: b"second".to_vec(),
                }][..],
            ])
            .expect("group");

        assert!(kv.get(b"k").expect("get") == Some(b"second".to_vec()));
    }

    #[test]
    fn group_conditional_expecting_pre_group_state_is_skipped() {
        use assert2::assert;

        let dir = temp();
        let kv = FjallKv::open(dir.path()).expect("open");
        kv.put(b"k".to_vec(), b"old".to_vec()).expect("seed");

        // Sequentially, the conditional batch runs after the put batch, so an
        // expectation pinned to the pre-group value must fail and skip the
        // whole second batch.
        kv.inner
            .commit_group([
                &[WriteOp::Put {
                    key: b"k".to_vec(),
                    value: b"first".to_vec(),
                }][..],
                &[
                    WriteOp::ConditionalPut {
                        key: b"k".to_vec(),
                        expected: Some(b"old".to_vec()),
                        value: b"lost".to_vec(),
                    },
                    WriteOp::Put {
                        key: b"marker".to_vec(),
                        value: b"ran".to_vec(),
                    },
                ][..],
            ])
            .expect("group");

        assert!(kv.get(b"k").expect("get") == Some(b"first".to_vec()));
        assert!(kv.get(b"marker").expect("get") == None);
    }

    #[test]
    fn failing_conditional_batch_no_ops_without_poisoning_its_group() {
        use assert2::assert;

        let dir = temp();
        let kv = FjallKv::open(dir.path()).expect("open");
        kv.put(b"k".to_vec(), b"old".to_vec()).expect("seed");

        kv.inner
            .commit_group([
                &[WriteOp::Put {
                    key: b"before".to_vec(),
                    value: b"1".to_vec(),
                }][..],
                &[
                    WriteOp::ConditionalPut {
                        key: b"k".to_vec(),
                        expected: Some(b"stale".to_vec()),
                        value: b"clobbered".to_vec(),
                    },
                    WriteOp::Put {
                        key: b"skipped".to_vec(),
                        value: b"x".to_vec(),
                    },
                ][..],
                &[WriteOp::Put {
                    key: b"after".to_vec(),
                    value: b"2".to_vec(),
                }][..],
            ])
            .expect("group");

        assert!(kv.get(b"before").expect("get") == Some(b"1".to_vec()));
        assert!(kv.get(b"after").expect("get") == Some(b"2".to_vec()));
        assert!(kv.get(b"k").expect("get") == Some(b"old".to_vec()));
        assert!(kv.get(b"skipped").expect("get") == None);
    }

    #[test]
    fn concurrent_conditional_put_matches_a_sequential_order() {
        use std::sync::Barrier;

        use assert2::assert;

        let dir = temp();
        let kv = FjallKv::open(dir.path()).expect("open");

        for round in 0..20_u32 {
            let k1 = format!("r{round}/k1").into_bytes();
            let k2 = format!("r{round}/k2").into_bytes();
            kv.put(k1.clone(), b"old".to_vec()).expect("seed");

            let barrier = Barrier::new(2);
            std::thread::scope(|scope| {
                let put_key = k1.clone();
                let put = scope.spawn(|| {
                    barrier.wait();
                    kv.write_batch(&[WriteOp::Put {
                        key: put_key,
                        value: b"a".to_vec(),
                    }])
                });
                let (cas_key, marker_key) = (k1.clone(), k2.clone());
                let cas = scope.spawn(|| {
                    barrier.wait();
                    kv.write_batch(&[
                        WriteOp::ConditionalPut {
                            key: cas_key,
                            expected: Some(b"old".to_vec()),
                            value: b"b".to_vec(),
                        },
                        WriteOp::Put {
                            key: marker_key,
                            value: b"b-ran".to_vec(),
                        },
                    ])
                });
                put.join().expect("put thread").expect("put ok");
                // A failed compare-and-swap still returns Ok, matching MemKv.
                cas.join().expect("cas thread").expect("cas ok");
            });

            // Whichever way the group (or two groups) applied, the outcome
            // must match one sequential order:
            //   put then cas: the expectation fails -> k1 = "a", k2 absent;
            //   cas then put: both apply -> k1 = "a", k2 = "b-ran".
            let k1_value = kv.get(&k1).expect("get k1");
            let k2_value = kv.get(&k2).expect("get k2");
            assert!(k1_value == Some(b"a".to_vec()), "round {round}");
            assert!(
                k2_value.is_none() || k2_value == Some(b"b-ran".to_vec()),
                "round {round}"
            );
        }
    }

    #[test]
    fn sustained_hot_key_churn_flushes_memtable_to_tables() {
        use assert2::assert;

        let dir = temp();
        let kv = FjallKv::open_cache(dir.path()).expect("open cache");

        // Rewriting one key never grows the logical dataset, so a byte-based
        // rotation trigger alone would leave every shadowed version in the
        // active memtable. Crossing the op-count threshold must rotate and
        // background-flush regardless of size.
        let final_round = 2 * ROTATE_AFTER_OPS;
        for round in 0..=final_round {
            kv.put(b"hot".to_vec(), round.to_le_bytes().to_vec())
                .expect("put");
        }

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while kv.inner.ks.inner().table_count() == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "no sstable appeared after sustained hot-key churn"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }

        assert!(kv.get(b"hot").expect("get") == Some(final_round.to_le_bytes().to_vec()));
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
