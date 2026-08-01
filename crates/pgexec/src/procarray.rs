//! The running-transaction registry (PostgreSQL's ProcArray). Shared across all
//! connections behind an `Arc`. Owns the next-xid counter (seeded from the
//! durable `/0/meta/next_xid` at open) and the set of currently-running xids,
//! and builds `crabka_pgmvcc::visibility::Snapshot`s. After a restart it starts empty, so
//! any clog `in-progress` xid is in no snapshot and resolves as aborted.
//!
//! In `Durable` mode the on-disk counter is persisted in blocks *ahead* of
//! hand-out (like `SequenceManager`'s rowid blocks), so most `begin_write`
//! calls are pure memory ops and the mutex-held fsync happens once per
//! ~[`DURABLE_XID_BLOCK`] transactions instead of once per write statement.
//! The invariant is that the persisted value is always >= every xid ever
//! handed out, so a restart never reuses an xid. A crash leaks up to a block
//! of handed-out-but-undecided xids; leaked xids have no clog entry, which is
//! exactly the crashed-transaction shape recovery already handles: an absent
//! clog entry reads as `InProgress`, is in no post-restart snapshot, and once
//! below the garbage horizon settles as decided-by-crash (aborted), so any
//! versions it stamped become reclaimable and the horizon walk — which only
//! visits EXISTING clog entries — is never wedged by the gap.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use crabka_pgkv::Kv;
use crabka_pgmvcc::{
    visibility::Snapshot,
    xid::{FIRST_NORMAL_XID, first_allocatable_xid_at_or_after},
};
use zerocopy::{FromBytes, IntoBytes, byteorder::big_endian::U64};

use crate::{PersistMode, error::ExecError};

/// How many xids beyond the current demand each durable extension reserves.
/// Larger blocks mean fewer mutex-held fsyncs on the write path (one per
/// ~`DURABLE_XID_BLOCK` transactions instead of one per statement) at the cost
/// of a larger leaked xid gap after a crash or restart. Gaps are harmless —
/// xids are visibility ordinals, not user-visible sequences, and a leaked xid
/// is indistinguishable from any other crashed transaction — so this only
/// trades fsync frequency against gap size. Matches `seq.rs`'s rowid block.
pub(crate) const DURABLE_XID_BLOCK: u64 = 1024;

/// Registry state: `next_xid` is the next xid to hand out; `durable_end` is
/// the exclusive end of the durably persisted reservation (the on-disk
/// counter value in `Durable` mode). Invariant: `next_xid <= durable_end` in
/// `Durable` mode, so every handed-out xid is below the persisted counter.
/// In `Replicated` mode `durable_end` tracks `next_xid` and is otherwise
/// unused (persistence rides the commit batch via `next_xid_op`).
struct Inner {
    next_xid: u64,
    durable_end: u64,
    running: BTreeSet<u64>,
}

/// The running-transaction registry.
pub(crate) struct ProcArray {
    inner: Mutex<Inner>,
    kv: Arc<dyn Kv>,
    mode: PersistMode,
    durable_block: u64,
}

impl ProcArray {
    /// Seed the next-xid counter from the durable key. Absent or stale reserved
    /// values are clamped to the first normal xid; reserved xids are MVCC
    /// sentinels and must never be assigned to a real transaction.
    #[allow(dead_code)]
    pub fn open(kv: Arc<dyn Kv>, mode: PersistMode) -> Result<Self, ExecError> {
        Self::open_with_block(kv, mode, DURABLE_XID_BLOCK)
    }

    pub fn open_with_block(
        kv: Arc<dyn Kv>,
        mode: PersistMode,
        durable_block: u64,
    ) -> Result<Self, ExecError> {
        let next_xid = match kv.get(&crabka_pgkv::key::next_xid_key())? {
            Some(b) => {
                let (v, _) = U64::read_from_prefix(b.as_slice())
                    .map_err(|_| crabka_pgkv::KvError::CorruptRow("next_xid is not u64".into()))?;
                v.get()
            }
            None => FIRST_NORMAL_XID,
        };
        let seeded = first_allocatable_xid_at_or_after(next_xid);
        Ok(Self {
            inner: Mutex::new(Inner {
                next_xid: seeded,
                // The persisted value is >= every xid ever handed out (the
                // invariant above), so seeding the reservation end at it means
                // the first begin_write extends and persists a fresh block.
                durable_end: seeded,
                running: BTreeSet::new(),
            }),
            kv,
            mode,
            durable_block,
        })
    }

    /// Allocate the next xid and register it as running. In `Durable` mode the
    /// allocation is served from the in-memory block reservation; only when the
    /// reservation is exhausted is the counter extended by [`DURABLE_XID_BLOCK`]
    /// and persisted (under the lock, BEFORE the xid is handed out, so the
    /// durable counter can never regress below a handed-out xid and a restart
    /// never reuses one, even when concurrent commit batches land out of
    /// order). In `Replicated` mode, the counter is NOT persisted here: the
    /// session folds `next_xid_op()` into the same commit batch as the write
    /// that triggered it (max-merged by the state machine), and
    /// `reseed_from_applied` lifts the counter on leadership change.
    pub fn begin_write(&self) -> Result<u64, ExecError> {
        let mut g = self.inner.lock().expect("procarray");
        let xid = first_allocatable_xid_at_or_after(g.next_xid);
        let new_next = xid + 1;
        match self.mode {
            PersistMode::Durable => {
                if new_next > g.durable_end {
                    // Reservation exhausted: extend and persist the new end
                    // BEFORE handing anything out and BEFORE updating
                    // `next_xid`, so a failed write leaves the state untouched
                    // and a crash after the write only leaks undecided xids
                    // (absent clog entries — decided-by-crash once below the
                    // horizon). This fsync is under the mutex but happens once
                    // per ~DURABLE_XID_BLOCK transactions, not per statement;
                    // allocations that fit the reservation never touch the
                    // store, so concurrent writers' commit batches can group-
                    // commit instead of serializing behind a per-xid fsync.
                    let new_end = new_next.checked_add(self.durable_block).ok_or_else(|| {
                        ExecError::Unsupported("durable XID reservation exhausted u64".into())
                    })?;
                    self.kv.write_batch(&[crabka_pgkv::WriteOp::Put {
                        key: crabka_pgkv::key::next_xid_key(),
                        value: U64::new(new_end).as_bytes().to_vec(),
                    }])?;
                    g.durable_end = new_end;
                }
            }
            PersistMode::Replicated => g.durable_end = new_next,
        }
        g.next_xid = new_next;
        g.running.insert(xid);
        Ok(xid)
    }

    /// Reseed the in-memory counter from the applied store (called when this node
    /// becomes leader, so it never hands out an xid the old leader already used).
    pub fn reseed_from_applied(&self) -> Result<(), ExecError> {
        let durable = match self.kv.get(&crabka_pgkv::key::next_xid_key())? {
            Some(b) => {
                let (v, _) = U64::read_from_prefix(b.as_slice())
                    .map_err(|_| crabka_pgkv::KvError::CorruptRow("next_xid not u64".into()))?;
                v.get()
            }
            None => FIRST_NORMAL_XID,
        };
        let durable = first_allocatable_xid_at_or_after(durable);
        let mut g = self.inner.lock().expect("procarray");
        g.next_xid = first_allocatable_xid_at_or_after(g.next_xid).max(durable);
        // Lift the reservation end only to the value actually read from the
        // applied store: the on-disk counter is exactly that value, and
        // claiming a higher durable coverage than what is persisted would
        // break the handed-out-below-persisted invariant.
        g.durable_end = g.durable_end.max(durable);
        Ok(())
    }

    /// The WriteOp recording the current next_xid (folded into the commit batch in
    /// Replicated mode).
    pub fn next_xid_op(&self) -> crabka_pgkv::WriteOp {
        let next = self.inner.lock().expect("procarray").next_xid;
        crabka_pgkv::WriteOp::Put {
            key: crabka_pgkv::key::next_xid_key(),
            value: U64::new(next).as_bytes().to_vec(),
        }
    }

    /// The in-memory next-xid value (one past the highest allocated). In
    /// `Durable` mode `begin_write` persists a block-ahead reservation before
    /// hand-out, so callers no longer batch the counter with their writes;
    /// retained as a test accessor that proves the counter advanced.
    #[cfg(test)]
    pub(crate) fn next_xid(&self) -> u64 {
        self.inner.lock().expect("procarray").next_xid
    }

    /// A snapshot of the currently-running transactions.
    pub fn snapshot(&self) -> Snapshot {
        let g = self.inner.lock().expect("procarray");
        let xip: Vec<u64> = g.running.iter().copied().collect(); // BTreeSet => sorted ascending
        let xmax = g.next_xid;
        let xmin = xip.first().copied().unwrap_or(xmax);
        Snapshot { xmin, xmax, xip }
    }

    /// Deregister a finished (committed or aborted) transaction. Call only after
    /// its clog entry is durable.
    pub fn finish(&self, xid: u64) {
        self.inner.lock().expect("procarray").running.remove(&xid);
    }

    /// Number of currently-registered running transactions (test helper).
    #[cfg(test)]
    pub(crate) fn running_len(&self) -> usize {
        self.inner.lock().expect("procarray").running.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use assert2::assert;
    use crabka_pgkv::MemKv;

    use super::*;

    /// A `Kv` that counts `write_batch` calls, proving allocations within the
    /// durable block reservation never touch the store.
    struct CountingKv {
        inner: MemKv,
        write_batches: AtomicUsize,
    }

    impl CountingKv {
        fn new() -> Self {
            Self {
                inner: MemKv::new(),
                write_batches: AtomicUsize::new(0),
            }
        }

        fn write_batches(&self) -> usize {
            self.write_batches.load(Ordering::SeqCst)
        }
    }

    impl Kv for CountingKv {
        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, crabka_pgkv::KvError> {
            self.inner.get(key)
        }

        fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), crabka_pgkv::KvError> {
            self.inner.put(key, value)
        }

        fn delete(&self, key: &[u8]) -> Result<(), crabka_pgkv::KvError> {
            self.inner.delete(key)
        }

        fn scan_prefix(&self, prefix: &[u8]) -> Result<crabka_pgkv::KvScan, crabka_pgkv::KvError> {
            self.inner.scan_prefix(prefix)
        }

        fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
        ) -> Result<crabka_pgkv::KvScan, crabka_pgkv::KvError> {
            self.inner.scan_range(start, end)
        }

        fn write_batch(&self, ops: &[crabka_pgkv::WriteOp]) -> Result<(), crabka_pgkv::KvError> {
            self.write_batches.fetch_add(1, Ordering::SeqCst);
            self.inner.write_batch(ops)
        }
    }

    fn persisted_next_xid(kv: &dyn Kv) -> Option<u64> {
        kv.get(&crabka_pgkv::key::next_xid_key())
            .expect("get")
            .map(|b| u64::from_be_bytes(b.try_into().expect("u64")))
    }

    #[test]
    fn fresh_store_starts_at_first_normal_xid() {
        let pa = ProcArray::open(Arc::new(MemKv::new()), PersistMode::Durable).expect("open");
        let s = pa.snapshot();
        assert!(s.xmax == FIRST_NORMAL_XID);
        assert!(s.xip.is_empty());
    }

    #[test]
    fn fresh_store_allocates_first_normal_xid() {
        let pa = ProcArray::open(Arc::new(MemKv::new()), PersistMode::Durable).expect("open");

        assert!(pa.begin_write().expect("begin_write") == FIRST_NORMAL_XID);
        assert!(pa.next_xid() == FIRST_NORMAL_XID + 1);
    }

    #[test]
    fn allocate_registers_running_and_snapshot_excludes_committed() {
        let pa = ProcArray::open(Arc::new(MemKv::new()), PersistMode::Durable).expect("open");
        let x1 = pa.begin_write().expect("begin_write");
        let x2 = pa.begin_write().expect("begin_write");
        assert!((x1, x2) == (FIRST_NORMAL_XID, FIRST_NORMAL_XID + 1));
        let s = pa.snapshot();
        assert!(s.xmax == FIRST_NORMAL_XID + 2);
        assert!(s.xip == vec![FIRST_NORMAL_XID, FIRST_NORMAL_XID + 1]);
        pa.finish(x1);
        let s2 = pa.snapshot();
        assert!(s2.xip == vec![FIRST_NORMAL_XID + 1]);
        assert!(s2.xmax == FIRST_NORMAL_XID + 2);
    }

    #[test]
    fn open_clamps_reserved_durable_counter_to_first_normal_xid() {
        for reserved in [
            crabka_pgmvcc::xid::INVALID_XID,
            crabka_pgmvcc::xid::FROZEN_XID,
        ] {
            let kv = Arc::new(MemKv::new());
            kv.write_batch(&[crabka_pgkv::WriteOp::Put {
                key: crabka_pgkv::key::next_xid_key(),
                value: reserved.to_be_bytes().to_vec(),
            }])
            .expect("seed reserved counter");
            let pa = ProcArray::open(Arc::clone(&kv) as Arc<dyn Kv>, PersistMode::Durable)
                .expect("open");

            assert!(pa.begin_write().expect("begin_write") == FIRST_NORMAL_XID);
            assert!(pa.next_xid() == FIRST_NORMAL_XID + 1);
        }
    }

    #[test]
    fn durable_begin_write_persists_a_block_ahead() {
        let kv = Arc::new(MemKv::new());
        let pa =
            ProcArray::open(Arc::clone(&kv) as Arc<dyn Kv>, PersistMode::Durable).expect("open");
        assert!(pa.begin_write().expect("begin_write") == FIRST_NORMAL_XID);
        // The counter is persisted a whole block ahead of what was handed out,
        // under the lock, before the xid is returned.
        assert!(persisted_next_xid(kv.as_ref()) == Some(FIRST_NORMAL_XID + 1 + DURABLE_XID_BLOCK));
    }

    #[test]
    fn durable_allocations_within_the_block_do_not_touch_the_store() {
        let kv = Arc::new(CountingKv::new());
        let pa =
            ProcArray::open(Arc::clone(&kv) as Arc<dyn Kv>, PersistMode::Durable).expect("open");
        // Everything that fits inside the persisted reservation is served from
        // memory: N allocations cost ~N/DURABLE_XID_BLOCK store writes, not N.
        let allocations = DURABLE_XID_BLOCK + 1; // first alloc extends; the rest fit
        for _ in 0..allocations {
            pa.begin_write().expect("begin_write");
        }
        assert!(
            kv.write_batches() == 1,
            "one block extension, no per-xid I/O"
        );
        // The next allocation exhausts the reservation and extends again.
        pa.begin_write().expect("begin_write");
        assert!(kv.write_batches() == 2);
        assert!(
            persisted_next_xid(&kv.inner)
                == Some(FIRST_NORMAL_XID + allocations + 1 + DURABLE_XID_BLOCK)
        );
    }

    #[test]
    fn open_seeds_next_xid_from_durable_counter() {
        let kv = Arc::new(MemKv::new());
        kv.write_batch(&[crabka_pgkv::WriteOp::Put {
            key: crabka_pgkv::key::next_xid_key(),
            value: 42u64.to_be_bytes().to_vec(),
        }])
        .expect("seed");
        let pa =
            ProcArray::open(Arc::clone(&kv) as Arc<dyn Kv>, PersistMode::Durable).expect("open");
        assert!(pa.begin_write().expect("begin_write") == 42);
        assert!(pa.next_xid() == 43);

        // Prove monotonic persist: a fresh ProcArray on the same kv seeds from
        // the durable counter, which covers the whole persisted reservation —
        // a restart leaks the unused block but never reuses 42.
        let pa2 =
            ProcArray::open(Arc::clone(&kv) as Arc<dyn Kv>, PersistMode::Durable).expect("open2");
        assert!(
            pa2.begin_write().expect("begin_write2") == 43 + DURABLE_XID_BLOCK,
            "restart resumes past the persisted reservation"
        );
    }

    #[test]
    fn partial_block_use_never_collides_after_a_crash() {
        let kv = Arc::new(MemKv::new());
        // A "process" that only ever used a small slice of its persisted block.
        let pa =
            ProcArray::open(Arc::clone(&kv) as Arc<dyn Kv>, PersistMode::Durable).expect("open");
        let mut highest = 0;
        for _ in 0..5 {
            highest = pa.begin_write().expect("begin_write");
        }
        assert!(
            highest == FIRST_NORMAL_XID + 4,
            "used a fraction of the block"
        );
        // Crash: the in-memory state (including the running set) is lost; only
        // the persisted counter survives, and it is >= every handed-out xid.
        drop(pa);
        let persisted = persisted_next_xid(kv.as_ref()).expect("persisted");
        assert!(persisted > highest);
        let pa2 =
            ProcArray::open(Arc::clone(&kv) as Arc<dyn Kv>, PersistMode::Durable).expect("open2");
        assert!(
            pa2.begin_write().expect("begin_write") >= persisted,
            "no collision with pre-crash xids"
        );
    }

    #[test]
    fn concurrent_begin_write_storm_hands_out_unique_xids() {
        let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
        let pa = Arc::new(ProcArray::open(Arc::clone(&kv), PersistMode::Durable).expect("open"));
        const THREADS: u64 = 8;
        const ALLOCS_PER_THREAD: u64 = 400; // > DURABLE_XID_BLOCK total: straddles block edges
        let mut xids: Vec<u64> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..THREADS)
                .map(|_| {
                    let pa = Arc::clone(&pa);
                    scope.spawn(move || {
                        (0..ALLOCS_PER_THREAD)
                            .map(|_| pa.begin_write().expect("begin_write"))
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|h| h.join().expect("thread"))
                .collect()
        });
        assert!(xids.len() == usize::try_from(THREADS * ALLOCS_PER_THREAD).expect("usize"));
        xids.sort_unstable();
        xids.dedup();
        assert!(
            xids.len() == usize::try_from(THREADS * ALLOCS_PER_THREAD).expect("usize"),
            "every handed-out xid is unique"
        );
        // The invariant: the persisted counter is > every handed-out xid.
        let max_xid = *xids.last().expect("nonempty");
        assert!(persisted_next_xid(kv.as_ref()) > Some(max_xid));
    }

    #[test]
    fn replicated_begin_write_does_not_persist_but_reseed_lifts_counter() {
        let kv = Arc::new(MemKv::new());
        let pa =
            ProcArray::open(Arc::clone(&kv) as Arc<dyn Kv>, PersistMode::Replicated).expect("open");
        assert!(pa.begin_write().expect("bw") == FIRST_NORMAL_XID);
        // Nothing persisted (replicated mode folds via the batch, not here).
        assert!(
            kv.get(&crabka_pgkv::key::next_xid_key())
                .expect("get")
                .is_none()
        );
        // The folded op carries the EXACT next xid, never a block reservation:
        // replicated apply must stay byte-identical across replicas.
        assert!(
            pa.next_xid_op()
                == crabka_pgkv::WriteOp::Put {
                    key: crabka_pgkv::key::next_xid_key(),
                    value: (FIRST_NORMAL_XID + 1).to_be_bytes().to_vec(),
                }
        );
        // Simulate the applied store advancing to 50 (via Raft), then becoming leader.
        kv.put(
            crabka_pgkv::key::next_xid_key(),
            50u64.to_be_bytes().to_vec(),
        )
        .expect("put");
        pa.reseed_from_applied().expect("reseed");
        assert!(
            pa.begin_write().expect("bw") == 50,
            "reseed lifts the counter above applied"
        );
    }

    #[test]
    fn replicated_reseed_clamps_reserved_applied_counter() {
        let kv = Arc::new(MemKv::new());
        let pa =
            ProcArray::open(Arc::clone(&kv) as Arc<dyn Kv>, PersistMode::Replicated).expect("open");
        kv.put(
            crabka_pgkv::key::next_xid_key(),
            1u64.to_be_bytes().to_vec(),
        )
        .expect("put frozen counter");

        pa.reseed_from_applied().expect("reseed");

        assert!(pa.begin_write().expect("bw") == FIRST_NORMAL_XID);
        assert!(pa.next_xid() == FIRST_NORMAL_XID + 1);
    }
}
