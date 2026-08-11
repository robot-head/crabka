//! Atomic per-table rowid allocation for concurrent INSERTs.
//!
//! There is one in-memory counter per table. It is seeded once from the durable
//! `/0/seq/<table>` key and bumped under a mutex. In `Durable` mode the manager
//! persists the on-disk counter in blocks *ahead* of hand-out, like a
//! PostgreSQL sequence with `CACHE`. So most allocations are pure memory ops,
//! and the mutex-held fsync happens once per block instead of once per
//! statement.
//!
//! The invariant is that the persisted value is always >= every rowid ever
//! handed out. A restart never reuses a rowid, and a crash only leaks up to a
//! block of unused rowids. Gaps are fine, because rowids are internal heap ids,
//! not user-visible sequences.

use std::{collections::HashMap, sync::Mutex};

use crabka_pgkv::Kv;
use zerocopy::{IntoBytes, byteorder::big_endian::U64};

use crate::{PersistMode, error::ExecError};

/// How many rowids beyond the current demand each durable extension reserves.
///
/// Larger blocks mean fewer mutex-held fsyncs on the INSERT path, one per
/// ~`DURABLE_BLOCK` rows instead of one per statement. The cost is a larger
/// leaked rowid gap after a crash or restart. Gaps are harmless, because scans
/// are keyspace range scans, not per-rowid probes. So this only trades fsync
/// frequency against gap size.
pub(crate) const DURABLE_BLOCK: u64 = 1024;

/// Per-table allocator state.
///
/// `next` is the next rowid to hand out. `durable_end` is the exclusive end of
/// the durably persisted reservation, which is the on-disk counter value in
/// `Durable` mode. Invariant: `next <= durable_end` in `Durable` mode, so every
/// handed-out rowid is below the persisted counter. In `Replicated` mode
/// `durable_end` tracks `next` and is otherwise unused, because persistence
/// rides the commit batch.
struct TableSeq {
    next: u64,
    durable_end: u64,
}

/// One SQL sequence's advance, staged for the caller to fold into a commit
/// batch.
///
/// `Replicated` mode hands these out instead of a write through the store, the
/// same way [`SequenceManager::alloc`] hands out a rowid `WriteOp`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedSequence {
    pub name: crabka_pgcatalog::RelationName,
    pub sequence: crabka_pgcatalog::Sequence,
}

/// A session's sequence advances that are not in the applied store yet.
///
/// A sequence advance happens inside *synchronous* expression evaluation,
/// which cannot await a commit. So the advance is staged here, and the session
/// folds it into the next batch it commits. This is the same seam
/// [`crate::clock::EvalCtx::notify`] gives `pg_notify()`. A key by name, rather
/// than an append, collapses the thousand advances of a thousand-row `INSERT`
/// into the one `Put` that records the last of them.
#[derive(Debug, Default)]
pub(crate) struct PendingSequences {
    staged: std::collections::BTreeMap<crabka_pgcatalog::RelationName, crabka_pgcatalog::Sequence>,
}

impl PendingSequences {
    pub fn stage(&mut self, staged: StagedSequence) {
        self.staged.insert(staged.name, staged.sequence);
    }

    /// Remove and return the staged advances as write ops, in name order so a
    /// batch is deterministic.
    pub fn take_ops(&mut self) -> Vec<crabka_pgkv::WriteOp> {
        std::mem::take(&mut self.staged)
            .into_iter()
            .map(|(name, sequence)| crabka_pgcatalog::put_sequence_op(&name, sequence))
            .collect()
    }
}

pub(crate) struct SequenceManager {
    inner: Mutex<HashMap<crabka_pgcatalog::TableId, TableSeq>>,
    /// The `Replicated`-mode SQL sequence cache.
    ///
    /// It holds each sequence's record as of this writer's most recent advance,
    /// which the applied store has not necessarily caught up to yet.
    ///
    /// It exists because a `Replicated` advance does not write through the
    /// store. The advance rides a commit batch. Without the cache, every
    /// `nextval` inside one uncommitted statement would re-read the same stale
    /// record and hand out the same value. The cache is engine-wide, not per
    /// session, because two sessions that insert into the same `SERIAL` table
    /// must not both be served the value the other already took.
    ///
    /// Cache entries are authoritative only for the writer that filled them.
    /// See [`SequenceManager::reseed_sql_sequences`] for why that is safe and
    /// what enforces it.
    sql: Mutex<HashMap<crabka_pgcatalog::RelationName, crabka_pgcatalog::Sequence>>,
    mode: PersistMode,
    durable_block: u64,
}

impl SequenceManager {
    #[allow(dead_code)]
    pub fn new(mode: PersistMode) -> Self {
        Self::with_durable_block(mode, DURABLE_BLOCK)
    }

    pub fn with_durable_block(mode: PersistMode, durable_block: u64) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            sql: Mutex::new(HashMap::new()),
            mode,
            durable_block,
        }
    }

    /// Reserve `count` consecutive rowids for `table` and return the first,
    /// plus the seq `WriteOp`.
    ///
    /// In `Durable` mode the op comes back as `None`. The in-memory block
    /// reservation serves the allocation. Only when the reservation is
    /// exhausted does the manager extend the counter by `DURABLE_BLOCK` and
    /// persist it. It does that under the lock and BEFORE it hands out the
    /// rowids, so the durable counter can never regress below a handed-out
    /// rowid.
    ///
    /// In `Replicated` mode this persists nothing. The op comes back as
    /// `Some(op)` for the caller to fold into the same commit batch as the
    /// inserted rows, and the state machine max-merges it.
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
                    let new_end = new_next.checked_add(self.durable_block).ok_or_else(|| {
                        ExecError::Unsupported("durable row-ID reservation exhausted u64".into())
                    })?;
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

    /// Clear the rowid cache so the next alloc re-seeds from the applied
    /// store.
    ///
    /// Counters seed lazily with `read_seq_kv` on first use.
    ///
    /// This is the *rowid* counter only. Callers use it both on leadership
    /// change and whenever a distributed transaction another node owned
    /// resolves, so it can fire while this node is midway through a statement.
    /// That is safe for rowids, because a rowid is only ever observed through
    /// the batch that carries it. It is not safe for SQL sequences, whose
    /// values go to the client. [`SequenceManager::reseed_sql_sequences`]
    /// clears those instead.
    pub fn reseed_from_applied(&self) {
        self.inner.lock().expect("seqmgr").clear();
    }

    /// Drop the whole SQL sequence cache, so the next `nextval` re-seeds from
    /// the applied store.
    ///
    /// Callers use this when this node becomes the writer.
    ///
    /// This is the failover invariant, and it only holds because of what the
    /// caller of a staged advance guarantees: **no `nextval` value reaches a
    /// client before the op recording it is durable**. Every statement that can
    /// advance a sequence commits before it returns. So at the moment a new
    /// writer re-seeds, the applied store already reflects every value the old
    /// writer handed out, and a re-seed from it can only move forward. Nobody
    /// ever observed the values a dead writer took but never committed, so a
    /// second issue of them is invisible.
    ///
    /// The converse is why this is *not* wired into
    /// [`SequenceManager::reseed_from_applied`]. A clear mid-statement would
    /// drop advances this writer had handed out but not yet committed, and the
    /// re-seed would hand the same values out a second time. Inside a single
    /// multi-row `INSERT`, that is a duplicate key.
    pub fn reseed_sql_sequences(&self) {
        self.sql.lock().expect("sql seqmgr").clear();
    }

    /// Forget the cached record of every sequence `ops` creates or drops.
    ///
    /// `DROP SEQUENCE s; CREATE SEQUENCE s;` reuses the name for a record that
    /// starts over, and a cache entry that outlived the drop would keep
    /// advancing the old one. A read of the names out of the committed batch,
    /// rather than out of the statement, covers every path that reaches the
    /// catalog:
    /// `CREATE`/`DROP SEQUENCE`, the implicit sequence of a `SERIAL` column, and
    /// a `DROP TABLE` that cascades to one.
    pub fn forget_sequences(&self, ops: &[crabka_pgkv::WriteOp]) {
        let names: Vec<_> = ops
            .iter()
            .filter_map(|op| match op {
                crabka_pgkv::WriteOp::Put { key, .. }
                | crabka_pgkv::WriteOp::ConditionalPut { key, .. }
                | crabka_pgkv::WriteOp::Delete { key } => {
                    crabka_pgcatalog::sequence_name_from_key(key)
                }
            })
            .collect();
        if names.is_empty() {
            return;
        }
        let mut cache = self.sql.lock().expect("sql seqmgr");
        for name in names {
            cache.remove(&name);
        }
    }

    /// `nextval` over the sequence a stored column default names.
    ///
    /// The stored text is the catalog's own rendering of a
    /// [`crabka_pgcatalog::RelationName`]: bare in `public`, `schema.name`
    /// elsewhere, never quoted. It is not a `regclass` literal, so this reads
    /// it back the way it was written and not through
    /// [`crate::relname::parse_written_relation`]. `nextval('…')` as SQL calls
    /// it is [`SequenceManager::nextval_written`].
    pub fn nextval(
        &self,
        kv: &dyn Kv,
        scope: &crate::relname::ResolutionScope,
        stored: &str,
    ) -> Result<(i64, Option<StagedSequence>), ExecError> {
        self.advance(kv, stored_sequence_name(kv, scope, stored)?)
    }

    /// `nextval(regclass)` as SQL calls it, over a name the user wrote.
    pub fn nextval_written(
        &self,
        kv: &dyn Kv,
        scope: &crate::relname::ResolutionScope,
        written: &str,
    ) -> Result<(i64, Option<StagedSequence>), ExecError> {
        self.advance(kv, written_sequence_name(kv, scope, written)?)
    }

    /// Advance `name` and return its new value.
    ///
    /// In `Replicated` mode this also returns the advance for the caller to
    /// fold into a commit batch.
    ///
    /// `Durable` mode writes the record through the store itself and stages
    /// nothing, exactly as [`SequenceManager::alloc`] does for rowids.
    /// `Replicated` mode may not: the applied store is only reachable through
    /// the replication log, so the record it reads would not see this writer's
    /// own uncommitted advances. So it reads through the cache, which holds the
    /// record as of the last advance whether or not that advance is applied
    /// yet, and it hands the new record back for the caller to commit.
    fn advance(
        &self,
        kv: &dyn Kv,
        name: crabka_pgcatalog::RelationName,
    ) -> Result<(i64, Option<StagedSequence>), ExecError> {
        match self.mode {
            PersistMode::Durable => {
                let mut sequence = crabka_pgcatalog::get_sequence(kv, &name)?;
                sequence.last_value = next_sequence_value(&name, &sequence)?;
                sequence.is_called = true;
                kv.write_batch(&[crabka_pgcatalog::put_sequence_op(&name, sequence)])?;
                Ok((sequence.last_value, None))
            }
            PersistMode::Replicated => {
                let mut cache = self.sql.lock().expect("sql seqmgr");
                let mut sequence = match cache.get(&name) {
                    Some(cached) => *cached,
                    None => crabka_pgcatalog::get_sequence(kv, &name)?,
                };
                sequence.last_value = next_sequence_value(&name, &sequence)?;
                sequence.is_called = true;
                cache.insert(name.clone(), sequence);
                Ok((sequence.last_value, Some(StagedSequence { name, sequence })))
            }
        }
    }

    /// `setval(regclass, …)` as SQL calls it.
    ///
    /// There is no stored-default counterpart: only `nextval` appears in a
    /// column default.
    pub fn setval_written(
        &self,
        kv: &dyn Kv,
        scope: &crate::relname::ResolutionScope,
        written: &str,
        value: i64,
        is_called: bool,
    ) -> Result<(i64, Option<StagedSequence>), ExecError> {
        let name = written_sequence_name(kv, scope, written)?;
        match self.mode {
            PersistMode::Durable => {
                let mut sequence = crabka_pgcatalog::get_sequence(kv, &name)?;
                setval_record(&name, &mut sequence, value, is_called)?;
                kv.write_batch(&[crabka_pgcatalog::put_sequence_op(&name, sequence)])?;
                Ok((value, None))
            }
            PersistMode::Replicated => {
                let mut cache = self.sql.lock().expect("sql seqmgr");
                let mut sequence = match cache.get(&name) {
                    Some(cached) => *cached,
                    None => crabka_pgcatalog::get_sequence(kv, &name)?,
                };
                setval_record(&name, &mut sequence, value, is_called)?;
                cache.insert(name.clone(), sequence);
                Ok((value, Some(StagedSequence { name, sequence })))
            }
        }
    }
}

/// Apply a `setval` to a sequence record.
///
/// This rejects a value outside the record's bounds the way `PostgreSQL` does,
/// before it writes anything.
fn setval_record(
    name: &crabka_pgcatalog::RelationName,
    sequence: &mut crabka_pgcatalog::Sequence,
    value: i64,
    is_called: bool,
) -> Result<(), ExecError> {
    if value < sequence.min || value > sequence.max {
        return Err(ExecError::SequenceLimit(format!(
            "setval: value {value} is out of bounds for sequence \"{}\"",
            name.name
        )));
    }
    sequence.last_value = value;
    sequence.is_called = is_called;
    Ok(())
}

/// The sequence a `nextval('…')` / `setval('…')` argument names.
///
/// The argument is a `regclass` input, so the one parser that reads those,
/// [`crate::relname::parse_written_relation`], reads it, and nothing splits it
/// on a dot. `nextval('"My Seq"')` names one sequence with a space in it, and
/// `nextval('MY SEQ')` is `42602 invalid name syntax`, both on `postgres:18.4`.
/// An unqualified name then resolves through the session's search path like any
/// other written name, which is why this takes a scope and does not assume
/// `public`.
///
/// `regclassin` reports a missing schema as a missing *relation*.
/// `nextval('nosuch.s')` is `42P01 relation "nosuch.s" does not exist` on
/// `postgres:18.4`, not `3F000`. So the disposition is
/// [`SchemaDisposition::Reference`](crate::relname::SchemaDisposition::Reference).
fn written_sequence_name(
    kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    written: &str,
) -> Result<crabka_pgcatalog::RelationName, ExecError> {
    resolve_sequence(
        kv,
        scope,
        crate::relname::parse_written_relation(scope, written)?.reference,
    )
}

/// The sequence a stored `nextval` column default names.
///
/// Its text is a [`crabka_pgcatalog::RelationName`]'s own rendering, not a
/// `regclass` literal: unquoted throughout, and dotted only outside `public`.
fn stored_sequence_name(
    kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    stored: &str,
) -> Result<crabka_pgcatalog::RelationName, ExecError> {
    let reference = match stored.split_once('.') {
        Some((schema, name)) => crabka_pgparser::ast::RelationRef::qualified(schema, name),
        None => crabka_pgparser::ast::RelationRef::bare(stored),
    };
    resolve_sequence(kv, scope, reference)
}

fn resolve_sequence(
    kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    reference: crabka_pgparser::ast::RelationRef,
) -> Result<crabka_pgcatalog::RelationName, ExecError> {
    crate::relname::resolve_relation(
        kv,
        scope,
        &reference,
        crate::relname::SchemaDisposition::Reference,
    )
}

fn next_sequence_value(
    name: &crabka_pgcatalog::RelationName,
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
    name: &crabka_pgcatalog::RelationName,
    sequence: &crabka_pgcatalog::Sequence,
) -> Result<i64, ExecError> {
    if !sequence.cycle {
        // `PostgreSQL` names the sequence with `RelationGetRelationName`, so the
        // message carries the bare name even for a sequence outside `public`.
        return Err(ExecError::SequenceLimit(format!(
            "nextval: reached limit of sequence \"{}\"",
            name.name
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

    /// A `regclass` argument is written text, not one identifier: a dot in it
    /// separates the schema, a quoted part keeps its case, and an unqualified
    /// name resolves through the session's search path and does not land in
    /// `public`.
    #[test]
    fn a_written_sequence_name_resolves_through_the_search_path() {
        let kv = MemKv::new();
        for schema in ["sch", "other"] {
            let ops =
                crabka_pgcatalog::create_schema_ops(&kv, schema, "postgres").expect("schema ops");
            kv.write_batch(&ops).expect("write");
            let ops = crabka_pgcatalog::create_sequence_ops(
                &kv,
                &crabka_pgcatalog::RelationName::new(schema, "s"),
                crabka_pgcatalog::Sequence::new(1, 1, None, None, Some(1), false),
            )
            .expect("sequence ops");
            kv.write_batch(&ops).expect("write");
        }
        let scope = crate::relname::ResolutionScope {
            search_path: crate::search_path::SearchPath::from_items(&["sch".into()]),
            ..crate::relname::ResolutionScope::default()
        };
        let cases = [
            ("s", crabka_pgcatalog::RelationName::new("sch", "s")),
            ("other.s", crabka_pgcatalog::RelationName::new("other", "s")),
            ("S", crabka_pgcatalog::RelationName::new("sch", "s")),
            (
                " OTHER . \"s\" ",
                crabka_pgcatalog::RelationName::new("other", "s"),
            ),
        ];
        for (written, expected) in cases {
            assert!(written_sequence_name(&kv, &scope, written).expect("resolves") == expected);
        }
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

    fn seed_sequence(kv: &dyn Kv, name: &crabka_pgcatalog::RelationName) {
        let ops = crabka_pgcatalog::create_sequence_ops(
            kv,
            name,
            crabka_pgcatalog::Sequence::new(1, 1, None, None, Some(1), false),
        )
        .expect("sequence ops");
        kv.write_batch(&ops).expect("write");
    }

    fn public(name: &str) -> crabka_pgcatalog::RelationName {
        crabka_pgcatalog::RelationName::new("public", name)
    }

    /// The `Replicated` advance mirrors `alloc`: the value comes from the cache,
    /// the store is untouched, and the op comes back for the caller to fold.
    #[test]
    fn replicated_advance_serves_from_cache_and_stages_the_op() {
        let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
        let name = public("s");
        seed_sequence(&*kv, &name);
        let seq = SequenceManager::new(PersistMode::Replicated);
        let scope = crate::relname::ResolutionScope::default();

        let mut staged = PendingSequences::default();
        for expected in 1..=3 {
            let (value, op) = seq.nextval_written(&*kv, &scope, "s").expect("nextval");
            assert!(value == expected);
            staged.stage(op.expect("Replicated mode stages the advance"));
        }
        // Nothing was written: the applied store still holds the seeded record.
        assert!(
            crabka_pgcatalog::get_sequence(&*kv, &name).expect("get")
                == crabka_pgcatalog::Sequence::new(1, 1, None, None, Some(1), false),
            "Replicated mode must not write the sequence through the store"
        );
        // Three advances collapse to the one op that records the last of them.
        assert!(
            staged.take_ops()
                == vec![crabka_pgcatalog::put_sequence_op(&name, {
                    let mut expected =
                        crabka_pgcatalog::Sequence::new(1, 1, None, None, Some(1), false);
                    expected.last_value = 3;
                    expected.is_called = true;
                    expected
                })]
        );
    }

    /// The failover invariant at the manager level.
    ///
    /// Once the staged advance is applied, a fresh manager, the successor
    /// writer, resumes past every value the predecessor handed out, never at
    /// one of them.
    #[test]
    fn a_reseeded_manager_resumes_past_every_value_already_handed_out() {
        let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
        seed_sequence(&*kv, &public("s"));
        let seq = SequenceManager::new(PersistMode::Replicated);
        let scope = crate::relname::ResolutionScope::default();

        let mut handed_out = Vec::new();
        let mut staged = PendingSequences::default();
        for _ in 0..5 {
            let (value, op) = seq.nextval_written(&*kv, &scope, "s").expect("nextval");
            handed_out.push(value);
            staged.stage(op.expect("staged"));
        }
        // The statement commits before returning, so the advance is applied.
        kv.write_batch(&staged.take_ops()).expect("apply");

        // Both shapes of "this node is now the writer": a brand-new manager
        // (process restart) and an explicit reseed of the live one.
        for manager in [SequenceManager::new(PersistMode::Replicated), {
            seq.reseed_sql_sequences();
            seq
        }] {
            let (value, _op) = manager.nextval_written(&*kv, &scope, "s").expect("nextval");
            assert!(
                !handed_out.contains(&value),
                "successor reissued {value}, already handed out {handed_out:?}"
            );
            assert!(value > *handed_out.iter().max().expect("nonempty"));
        }
    }

    /// A batch that creates or drops a sequence invalidates that name and only
    /// that name.
    ///
    /// The cache is read out of the committed ops, so this catches a `SERIAL`
    /// sequence nobody spelled too.
    #[test]
    fn forget_sequences_drops_exactly_the_names_the_batch_touches() {
        let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
        seed_sequence(&*kv, &public("kept"));
        seed_sequence(&*kv, &public("reused"));
        let seq = SequenceManager::new(PersistMode::Replicated);
        let scope = crate::relname::ResolutionScope::default();
        for name in ["kept", "reused"] {
            for _ in 0..2 {
                seq.nextval_written(&*kv, &scope, name).expect("nextval");
            }
        }

        // `reused` is dropped and recreated; `kept` is untouched. An unrelated
        // key in the same batch must not disturb anything.
        seq.forget_sequences(&[
            crabka_pgkv::WriteOp::Delete {
                key: crabka_pgkv::key::seq_key(7),
            },
            crabka_pgcatalog::drop_sequence_ops(&*kv, &public("reused")).expect("drop")[0].clone(),
        ]);
        kv.write_batch(
            &crabka_pgcatalog::drop_sequence_ops(&*kv, &public("reused")).expect("drop"),
        )
        .expect("apply drop");
        seed_sequence(&*kv, &public("reused"));

        assert!(
            seq.nextval_written(&*kv, &scope, "reused")
                .expect("nextval")
                .0
                == 1,
            "a recreated sequence starts over"
        );
        assert!(
            seq.nextval_written(&*kv, &scope, "kept")
                .expect("nextval")
                .0
                == 3,
            "an untouched sequence keeps its cached position"
        );
    }

    /// `setval` repositions the cache as well as the record, so the next
    /// `nextval` follows the value that was set rather than the cached one.
    #[test]
    fn replicated_setval_repositions_the_cache() {
        let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
        seed_sequence(&*kv, &public("s"));
        let seq = SequenceManager::new(PersistMode::Replicated);
        let scope = crate::relname::ResolutionScope::default();
        seq.nextval_written(&*kv, &scope, "s").expect("nextval");

        let (value, op) = seq
            .setval_written(&*kv, &scope, "s", 50, true)
            .expect("setval");
        assert!(value == 50);
        assert!(op.is_some(), "Replicated mode stages the setval");
        assert!(seq.nextval_written(&*kv, &scope, "s").expect("nextval").0 == 51);

        // `is_called => false` hands the value itself out next.
        seq.setval_written(&*kv, &scope, "s", 100, false)
            .expect("setval");
        assert!(seq.nextval_written(&*kv, &scope, "s").expect("nextval").0 == 100);
        assert!(seq.nextval_written(&*kv, &scope, "s").expect("nextval").0 == 101);
    }

    /// An out-of-bounds `setval` is rejected without disturbing the cache.
    #[test]
    fn replicated_setval_out_of_bounds_leaves_the_cache_alone() {
        let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
        let name = public("s");
        let ops = crabka_pgcatalog::create_sequence_ops(
            &*kv,
            &name,
            crabka_pgcatalog::Sequence::new(1, 1, Some(1), Some(10), Some(1), false),
        )
        .expect("sequence ops");
        kv.write_batch(&ops).expect("write");
        let seq = SequenceManager::new(PersistMode::Replicated);
        let scope = crate::relname::ResolutionScope::default();
        assert!(seq.nextval_written(&*kv, &scope, "s").expect("nextval").0 == 1);
        assert!(seq.setval_written(&*kv, &scope, "s", 99, true).is_err());
        assert!(
            seq.nextval_written(&*kv, &scope, "s").expect("nextval").0 == 2,
            "the rejected setval must not have moved the cache"
        );
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
