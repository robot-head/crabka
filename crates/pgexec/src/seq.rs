//! Atomic per-table rowid allocation for concurrent INSERTs. An in-memory
//! counter per table, seeded once from the durable `/0/seq/<table>` key, bumped
//! under a mutex, with the new value persisted durably *under the mutex* before
//! the rowid is returned — so the durable counter is monotonic and a restart
//! never reuses a rowid (a crash only leaks a gap, like a PostgreSQL sequence).

use std::{collections::HashMap, sync::Mutex};

use crabka_pgkv::Kv;
use zerocopy::{IntoBytes, byteorder::big_endian::U64};

use crate::{PersistMode, error::ExecError};

pub(crate) struct SequenceManager {
    inner: Mutex<HashMap<crabka_pgcatalog::TableId, u64>>,
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
    /// the seq `WriteOp`. In `Durable` mode the new next-rowid is persisted here
    /// (under the lock, before returning, so the durable counter cannot regress)
    /// and the op is returned as `None`. In `Replicated` mode nothing is
    /// persisted here: the op is returned as `Some(op)` for the caller to fold
    /// into the same commit batch as the inserted rows (max-merged by the state
    /// machine), and `reseed_from_applied` re-seeds on leadership change.
    pub fn alloc(
        &self,
        kv: &dyn Kv,
        table: crabka_pgcatalog::TableId,
        count: u64,
    ) -> Result<(u64, Option<crabka_pgkv::WriteOp>), ExecError> {
        let mut g = self.inner.lock().expect("seqmgr");
        let next = match g.get(&table) {
            Some(&n) => n,
            None => crate::exec::read_seq_kv(kv, table)?, // seed once from disk
        };
        let new_next = next + count;
        let op = crabka_pgkv::WriteOp::Put {
            key: crabka_pgkv::key::seq_key(table),
            value: U64::new(new_next).as_bytes().to_vec(),
        };
        let folded = match self.mode {
            // Persist BEFORE releasing the lock and BEFORE handing out the rowid,
            // so the durable counter is monotonic even under concurrent allocators.
            PersistMode::Durable => {
                kv.write_batch(std::slice::from_ref(&op))?;
                None
            }
            PersistMode::Replicated => Some(op),
        };
        g.insert(table, new_next);
        Ok((next, folded))
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

    use crabka_pgkv::MemKv;

    use super::*;

    #[test]
    fn allocates_distinct_increasing_rowids() {
        let kv: Arc<dyn crabka_pgkv::Kv> = Arc::new(MemKv::new());
        let seq = SequenceManager::new(PersistMode::Durable);
        let (start, _op) = seq.alloc(&*kv, 7, 3).expect("alloc");
        assert_eq!(start, 1); // rows 1,2,3
        let (start, _op) = seq.alloc(&*kv, 7, 2).expect("alloc");
        assert_eq!(start, 4); // rows 4,5
        let (start, _op) = seq.alloc(&*kv, 8, 1).expect("alloc");
        assert_eq!(start, 1); // a different table is independent
    }

    #[test]
    fn durable_alloc_self_persists_and_returns_no_op() {
        let kv: Arc<dyn crabka_pgkv::Kv> = Arc::new(MemKv::new());
        let seq = SequenceManager::new(PersistMode::Durable);
        let (start, op) = seq.alloc(&*kv, 7, 3).expect("alloc");
        assert_eq!(start, 1);
        assert!(op.is_none(), "Durable mode self-persists, folds nothing");
        // The counter is durable (persisted under the lock by alloc itself).
        assert_eq!(
            kv.get(&crabka_pgkv::key::seq_key(7)).expect("get"),
            Some(4u64.to_be_bytes().to_vec())
        );
    }

    #[test]
    fn durable_seq_is_monotonic_and_seeds_a_fresh_manager() {
        let kv: Arc<dyn crabka_pgkv::Kv> = Arc::new(MemKv::new());
        let seq = SequenceManager::new(PersistMode::Durable);
        seq.alloc(&*kv, 7, 5).expect("alloc"); // consumes 1..=5, persists next=6
        let seq2 = SequenceManager::new(PersistMode::Durable); // simulate restart
        let (start, _op) = seq2.alloc(&*kv, 7, 1).expect("alloc");
        assert_eq!(start, 6, "must not reuse 1..=5");
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
        assert_eq!(start, 42);
    }

    #[test]
    fn replicated_alloc_folds_op_and_does_not_persist_and_reseed_clears_cache() {
        let kv: Arc<dyn crabka_pgkv::Kv> = Arc::new(MemKv::new());
        let seq = SequenceManager::new(PersistMode::Replicated);
        // Replicated alloc returns the op to fold and persists nothing itself.
        let (start, op) = seq.alloc(&*kv, 7, 3).expect("alloc");
        assert_eq!(start, 1);
        let op = op.expect("Replicated mode folds the seq op via the batch");
        assert_eq!(
            op,
            crabka_pgkv::WriteOp::Put {
                key: crabka_pgkv::key::seq_key(7),
                value: 4u64.to_be_bytes().to_vec(),
            }
        );
        assert!(
            kv.get(&crabka_pgkv::key::seq_key(7))
                .expect("get")
                .is_none(),
            "Replicated mode must not self-persist the seq counter"
        );
        // Next alloc continues from the in-memory cache (4), still no persist.
        let (start, _op) = seq.alloc(&*kv, 7, 1).expect("alloc");
        assert_eq!(start, 4);
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
        assert_eq!(start, 50, "reseed re-seeds from the applied store");
    }
}
