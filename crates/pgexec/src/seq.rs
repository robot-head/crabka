//! Atomic per-table rowid allocation for concurrent INSERTs. An in-memory
//! counter per table, seeded once from the durable `/0/seq/<table>` key and
//! bumped under a mutex. In `Durable` mode the on-disk counter is persisted in
//! blocks *ahead* of hand-out (like a PostgreSQL sequence with `CACHE`), so
//! most allocations are pure memory ops and the mutex-held fsync happens once
//! per block instead of once per statement. The invariant is that the persisted
//! value is always >= every rowid ever handed out — a restart never reuses a
//! rowid, and a crash only leaks up to a block of unused rowids (gaps are fine;
//! rowids are internal heap ids, not user-visible sequences).

use std::{collections::HashMap, sync::Mutex};

use crabka_pgkv::Kv;
use zerocopy::{IntoBytes, byteorder::big_endian::U64};

use crate::{PersistMode, error::ExecError};

/// How many rowids beyond the current demand each durable extension reserves.
/// Larger blocks mean fewer mutex-held fsyncs on the INSERT path (one per
/// ~`DURABLE_BLOCK` rows instead of one per statement) at the cost of a larger
/// leaked rowid gap after a crash or restart. Gaps are harmless — scans are
/// keyspace range scans, not per-rowid probes — so this only trades fsync
/// frequency against gap size.
const DURABLE_BLOCK: u64 = 1024;

/// Per-table allocator state: `next` is the next rowid to hand out;
/// `durable_end` is the exclusive end of the durably persisted reservation
/// (the on-disk counter value in `Durable` mode). Invariant: `next <=
/// durable_end` in `Durable` mode, so every handed-out rowid is below the
/// persisted counter. In `Replicated` mode `durable_end` tracks `next` and is
/// otherwise unused (persistence rides the commit batch).
struct TableSeq {
    next: u64,
    durable_end: u64,
}

pub(crate) struct SequenceManager {
    inner: Mutex<HashMap<crabka_pgcatalog::TableId, TableSeq>>,
    mode: PersistMode,
}

impl SequenceManager {
    pub fn new(mode: PersistMode) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            mode,
        }
    }

    /// Reserve `count` consecutive rowids for `table` and return the first, plus
    /// the seq `WriteOp`. In `Durable` mode the op is returned as `None`: the
    /// allocation is served from the in-memory block reservation, and only when
    /// the reservation is exhausted is the counter extended by `DURABLE_BLOCK`
    /// and persisted (under the lock, BEFORE the rowids are handed out, so the
    /// durable counter can never regress below a handed-out rowid). In
    /// `Replicated` mode nothing is persisted here: the op is returned as
    /// `Some(op)` for the caller to fold into the same commit batch as the
    /// inserted rows (max-merged by the state machine), and
    /// `reseed_from_applied` re-seeds on leadership change.
    pub fn alloc(
        &self,
        kv: &dyn Kv,
        table: crabka_pgcatalog::TableId,
        count: u64,
    ) -> Result<(u64, Option<crabka_pgkv::WriteOp>), ExecError> {
        let mut g = self.inner.lock().expect("seqmgr");
        let state = match g.entry(table) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(v) => {
                // Seed once from disk. The persisted value is >= every rowid
                // ever handed out (the invariant above), so starting here can
                // never collide with a rowid from before a restart.
                let seeded = crate::exec::read_seq_kv(kv, table)?;
                v.insert(TableSeq {
                    next: seeded,
                    durable_end: seeded,
                })
            }
        };
        let start = state.next;
        let new_next = start + count;
        let folded = match self.mode {
            PersistMode::Durable => {
                if new_next > state.durable_end {
                    // Reservation exhausted: extend and persist the new end
                    // BEFORE handing anything out and BEFORE updating `next`,
                    // so a failed write leaves the state untouched and a crash
                    // after the write only leaks unused rowids. This fsync is
                    // under the mutex but happens once per ~DURABLE_BLOCK rows,
                    // not per statement; allocations that fit the reservation
                    // never touch the store.
                    let new_end = new_next + DURABLE_BLOCK;
                    kv.write_batch(&[crabka_pgkv::WriteOp::Put {
                        key: crabka_pgkv::key::seq_key(table),
                        value: U64::new(new_end).as_bytes().to_vec(),
                    }])?;
                    state.durable_end = new_end;
                }
                None
            }
            PersistMode::Replicated => {
                state.durable_end = new_next;
                Some(crabka_pgkv::WriteOp::Put {
                    key: crabka_pgkv::key::seq_key(table),
                    value: U64::new(new_next).as_bytes().to_vec(),
                })
            }
        };
        state.next = new_next;
        Ok((start, folded))
    }

    /// On leadership change, clear the cache so the next alloc re-seeds from the
    /// applied store (counters seed lazily via `read_seq_kv` on first use).
    pub fn reseed_from_applied(&self) {
        self.inner.lock().expect("seqmgr").clear();
    }

    pub fn nextval(&self, kv: &dyn Kv, name: &str) -> Result<i64, ExecError> {
        let mut sequence = crabka_pgcatalog::get_sequence(kv, name)?;
        let value = next_sequence_value(name, &sequence)?;
        sequence.last_value = value;
        sequence.is_called = true;
        let op = crabka_pgcatalog::put_sequence_op(name, sequence);
        match self.mode {
            PersistMode::Durable => kv.write_batch(&[op])?,
            PersistMode::Replicated => {
                return Err(ExecError::Unsupported(
                    "replicated SQL sequence updates are not wired yet".into(),
                ));
            }
        }
        Ok(value)
    }

    pub fn setval(
        &self,
        kv: &dyn Kv,
        name: &str,
        value: i64,
        is_called: bool,
    ) -> Result<i64, ExecError> {
        let mut sequence = crabka_pgcatalog::get_sequence(kv, name)?;
        if value < sequence.min || value > sequence.max {
            return Err(ExecError::SequenceLimit(format!(
                "setval: value {value} is out of bounds for sequence \"{name}\""
            )));
        }
        sequence.last_value = value;
        sequence.is_called = is_called;
        let op = crabka_pgcatalog::put_sequence_op(name, sequence);
        match self.mode {
            PersistMode::Durable => kv.write_batch(&[op])?,
            PersistMode::Replicated => {
                return Err(ExecError::Unsupported(
                    "replicated SQL sequence updates are not wired yet".into(),
                ));
            }
        }
        Ok(value)
    }
}

fn next_sequence_value(
    name: &str,
    sequence: &crabka_pgcatalog::Sequence,
) -> Result<i64, ExecError> {
    if !sequence.is_called {
        return Ok(sequence.last_value);
    }
    let Some(next) = sequence.last_value.checked_add(sequence.increment) else {
        return sequence_wrapped_value(name, sequence);
    };
    if next >= sequence.min && next <= sequence.max {
        return Ok(next);
    }
    sequence_wrapped_value(name, sequence)
}

fn sequence_wrapped_value(
    name: &str,
    sequence: &crabka_pgcatalog::Sequence,
) -> Result<i64, ExecError> {
    if !sequence.cycle {
        return Err(ExecError::SequenceLimit(format!(
            "nextval: reached limit of sequence \"{name}\""
        )));
    }
    if sequence.increment > 0 {
        Ok(sequence.min)
    } else {
        Ok(sequence.max)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_pgkv::MemKv;

    use super::*;

    fn persisted_seq(kv: &dyn Kv, table: crabka_pgcatalog::TableId) -> Option<u64> {
        kv.get(&crabka_pgkv::key::seq_key(table))
            .expect("get")
            .map(|b| u64::from_be_bytes(b.try_into().expect("u64")))
    }

    #[test]
    fn allocates_distinct_increasing_rowids() {
        let kv: Arc<dyn crabka_pgkv::Kv> = Arc::new(MemKv::new());
        let seq = SequenceManager::new(PersistMode::Durable);
        let (start, _op) = seq.alloc(&*kv, 7, 3).expect("alloc");
        assert!(start == 1); // rows 1,2,3
        let (start, _op) = seq.alloc(&*kv, 7, 2).expect("alloc");
        assert!(start == 4); // rows 4,5
        let (start, _op) = seq.alloc(&*kv, 8, 1).expect("alloc");
        assert!(start == 1); // a different table is independent
    }

    #[test]
    fn durable_alloc_persists_a_block_ahead_and_returns_no_op() {
        let kv: Arc<dyn crabka_pgkv::Kv> = Arc::new(MemKv::new());
        let seq = SequenceManager::new(PersistMode::Durable);
        let (start, op) = seq.alloc(&*kv, 7, 3).expect("alloc");
        assert!(start == 1);
        assert!(op.is_none(), "Durable mode self-persists, folds nothing");
        // The counter is persisted a whole block ahead of what was handed out,
        // under the lock, before the rowids are returned.
        assert!(persisted_seq(&*kv, 7) == Some(4 + DURABLE_BLOCK));
    }

    #[test]
    fn durable_allocs_within_the_block_do_not_touch_the_store() {
        let kv: Arc<dyn crabka_pgkv::Kv> = Arc::new(MemKv::new());
        let seq = SequenceManager::new(PersistMode::Durable);
        seq.alloc(&*kv, 7, 3).expect("alloc"); // extends + persists 4 + BLOCK
        let end = persisted_seq(&*kv, 7);
        // Everything that fits inside the persisted reservation is served from
        // memory: the on-disk counter must not move again.
        let mut handed_out = 4;
        while handed_out + 5 <= 4 + DURABLE_BLOCK {
            let (start, op) = seq.alloc(&*kv, 7, 5).expect("alloc");
            assert!(start == handed_out);
            assert!(op.is_none());
            handed_out += 5;
        }
        assert!(persisted_seq(&*kv, 7) == end, "no I/O within the block");
        // The first alloc past the reservation extends and persists again.
        let (start, _op) = seq.alloc(&*kv, 7, DURABLE_BLOCK).expect("alloc");
        assert!(start == handed_out);
        assert!(persisted_seq(&*kv, 7) == Some(handed_out + 2 * DURABLE_BLOCK));
    }

    #[test]
    fn durable_seq_is_monotonic_and_seeds_a_fresh_manager() {
        let kv: Arc<dyn crabka_pgkv::Kv> = Arc::new(MemKv::new());
        let seq = SequenceManager::new(PersistMode::Durable);
        seq.alloc(&*kv, 7, 5).expect("alloc"); // consumes 1..=5, persists 6 + BLOCK
        let seq2 = SequenceManager::new(PersistMode::Durable); // simulate restart
        let (start, _op) = seq2.alloc(&*kv, 7, 1).expect("alloc");
        assert!(start >= 6, "must not reuse 1..=5");
        assert!(start == 6 + DURABLE_BLOCK, "restart leaks the unused block");
    }

    #[test]
    fn partial_block_use_never_collides_after_a_crash() {
        let kv: Arc<dyn crabka_pgkv::Kv> = Arc::new(MemKv::new());
        // A "process" that only ever used a small slice of its persisted block.
        let seq = SequenceManager::new(PersistMode::Durable);
        let mut max_end = 0u64;
        for count in [3u64, 1, 7, 2] {
            let (start, _op) = seq.alloc(&*kv, 7, count).expect("alloc");
            max_end = max_end.max(start + count);
        }
        assert!(max_end == 14, "used 1..=13, a fraction of the block");
        // Crash: the in-memory state is lost; only the persisted counter
        // survives. Every persisted value must be >= every handed-out rowid.
        drop(seq);
        let seq2 = SequenceManager::new(PersistMode::Durable);
        let (start, _op) = seq2.alloc(&*kv, 7, 1).expect("alloc");
        assert!(start >= max_end, "no collision with pre-crash rowids");
    }

    #[test]
    fn concurrent_allocs_hand_out_disjoint_ranges() {
        let kv: Arc<dyn crabka_pgkv::Kv> = Arc::new(MemKv::new());
        let seq = Arc::new(SequenceManager::new(PersistMode::Durable));
        const THREADS: u64 = 8;
        const ALLOCS_PER_THREAD: u64 = 300;
        let mut ranges: Vec<(u64, u64)> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..THREADS)
                .map(|t| {
                    let seq = Arc::clone(&seq);
                    let kv = Arc::clone(&kv);
                    scope.spawn(move || {
                        let mut out = Vec::new();
                        for i in 0..ALLOCS_PER_THREAD {
                            // Vary the count so ranges straddle block edges.
                            let count = (t + i) % 5 + 1;
                            let (start, op) = seq.alloc(&*kv, 7, count).expect("alloc");
                            assert!(op.is_none());
                            out.push((start, start + count));
                        }
                        out
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|h| h.join().expect("thread"))
                .collect()
        });
        assert!(ranges.len() == usize::try_from(THREADS * ALLOCS_PER_THREAD).expect("usize"));
        ranges.sort_unstable();
        for pair in ranges.windows(2) {
            assert!(pair[0].1 <= pair[1].0, "ranges overlap: {pair:?}");
        }
        // The invariant: the persisted counter is >= every handed-out rowid.
        let max_end = ranges.last().expect("nonempty").1;
        assert!(persisted_seq(&*kv, 7) >= Some(max_end));
    }

    #[test]
    fn seeds_from_existing_durable_seq_key() {
        let kv: Arc<dyn crabka_pgkv::Kv> = Arc::new(MemKv::new());
        kv.write_batch(&[crabka_pgkv::WriteOp::Put {
            key: crabka_pgkv::key::seq_key(7),
            value: 42u64.to_be_bytes().to_vec(),
        }])
        .expect("seed");
        let seq = SequenceManager::new(PersistMode::Durable);
        let (start, _op) = seq.alloc(&*kv, 7, 1).expect("alloc");
        assert!(start == 42);
    }

    #[test]
    fn replicated_alloc_folds_op_and_does_not_persist_and_reseed_clears_cache() {
        let kv: Arc<dyn crabka_pgkv::Kv> = Arc::new(MemKv::new());
        let seq = SequenceManager::new(PersistMode::Replicated);
        // Replicated alloc returns the op to fold and persists nothing itself.
        let (start, op) = seq.alloc(&*kv, 7, 3).expect("alloc");
        assert!(start == 1);
        let op = op.expect("Replicated mode folds the seq op via the batch");
        assert!(
            op == crabka_pgkv::WriteOp::Put {
                key: crabka_pgkv::key::seq_key(7),
                value: 4u64.to_be_bytes().to_vec(),
            },
            "Replicated ops carry the exact next-rowid, never a block"
        );
        assert!(
            kv.get(&crabka_pgkv::key::seq_key(7))
                .expect("get")
                .is_none(),
            "Replicated mode must not self-persist the seq counter"
        );
        // Next alloc continues from the in-memory cache (4), still no persist.
        let (start, _op) = seq.alloc(&*kv, 7, 1).expect("alloc");
        assert!(start == 4);
        // Simulate the applied store advancing (via Raft) to next-rowid=50, then
        // becoming leader: reseed clears the cache so the next alloc re-seeds from
        // the applied store via read_seq_kv.
        kv.write_batch(&[crabka_pgkv::WriteOp::Put {
            key: crabka_pgkv::key::seq_key(7),
            value: 50u64.to_be_bytes().to_vec(),
        }])
        .expect("apply");
        seq.reseed_from_applied();
        let (start, _op) = seq.alloc(&*kv, 7, 1).expect("alloc");
        assert!(start == 50, "reseed re-seeds from the applied store");
    }
}
