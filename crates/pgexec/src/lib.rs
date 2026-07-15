#![allow(
    clippy::assigning_clones,
    clippy::bool_to_int_with_if,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::doc_markdown,
    clippy::items_after_statements,
    clippy::manual_let_else,
    clippy::manual_string_new,
    clippy::many_single_char_names,
    clippy::map_unwrap_or,
    clippy::match_same_arms,
    clippy::match_wildcard_for_single_variants,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value,
    clippy::redundant_closure_for_method_calls,
    clippy::ref_option,
    clippy::return_self_not_must_use,
    clippy::single_match_else,
    clippy::stable_sort_primitive,
    clippy::too_many_lines,
    clippy::unnested_or_patterns,
    clippy::unused_async,
    clippy::used_underscore_binding,
    reason = "ported donor executor keeps PostgreSQL-compatible behavior; style cleanup is deferred"
)]

//! executor: turns parsed SQL into catalog/KV operations and implements the
//! pgwire `Engine` trait. SP5 swaps SP4's commit_ts MVCC for PostgreSQL's
//! xid/clog/snapshot model with uncommitted versions on disk. SP6 removes the
//! global writer lock: writers run concurrently, serialized only at the row
//! level via the `RowLockManager`, with rowid allocation via the
//! `SequenceManager` and DDL serialized behind a small catalog lock.

#![doc(html_root_url = "https://docs.rs/crabka-pgexec/0.3.9")]

mod agg;
pub mod clock;
mod commit;
mod cte;
mod datetime_fn;
mod error;
mod eval;
mod exec;
pub mod foreign;
mod format_fn;
mod func;
mod gtm;
mod join;
mod lockmgr;
pub mod plan_dist;
mod procarray;
mod query;
mod read_gate;
pub mod scanner;
mod scope;
mod seq;
mod session;
mod setops;
mod subquery;
pub mod timestamp_txn;
mod values;

use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex, OnceLock, Weak},
};

pub use commit::{Committer, LocalCommitter};
use crabka_pgkv::{FjallKv, Kv, MemKv};
use crabka_pgwire::engine::Engine;
pub use error::ExecError;
pub use gtm::GlobalXidLease;
pub use read_gate::{Linearizer, LocalLinearizer};
pub use scanner::{
    ColumnPredicate, JoinExecutionStrategy, JoinKind, JoinRangeRequest, JoinRangeResult, JoinRow,
    JoinSnapshot, JoinTableInterval, JoinValidationError, LocalRangeScanner,
    MaterializedRangeCursor, PartialAggregateFunction, PartialAggregateSpec, PredicateOp,
    PredicatePushdown, ProjectionPushdown, RangeCursor, RangeScanner, RowInterval, ScanPage,
    ScanRequest, ScannedRow, TimestampedRangeScanner, TopKColumn, TopKSpec,
};
pub use session::SqlSession;
pub use timestamp_txn::{
    CommitTimestamp, DurableTimestampIntentIdentity, PrimaryTxnDecision, ReadTimestamp,
    TimestampOracle, TimestampOracleError, TimestampTransactionId, TimestampTxnDecision,
    TimestampTxnDescriptor, TimestampTxnIdentity, TimestampTxnOperation, TimestampTxnParticipant,
    TimestampWrite, decode_timestamp_txn_descriptor_value, timestamp_txn_descriptor_op,
};

use crate::{lockmgr::RowLockManager, procarray::ProcArray, seq::SequenceManager};

/// Process-local coordination shared by every engine opened over the same KV
/// handle.
struct EngineCoordination {
    catalog_lock: Arc<tokio::sync::Mutex<()>>,
    table_write_gate: Arc<tokio::sync::RwLock<()>>,
    writer_fence: Arc<WriterFence>,
}

impl EngineCoordination {
    fn new() -> Self {
        Self {
            catalog_lock: Arc::new(tokio::sync::Mutex::new(())),
            table_write_gate: Arc::new(tokio::sync::RwLock::new(())),
            writer_fence: Arc::new(WriterFence::new()),
        }
    }
}

/// Tracks xid writers separately from the physical conversion gate.
///
/// A transaction retains its writer lease through commit or rollback. This lets
/// DDL release its shared gate before waiting for the catalog lock without
/// allowing conversion to rewrite that transaction's xid tuples.
pub(crate) struct WriterFence {
    state: Mutex<WriterFenceState>,
    changed: tokio::sync::Notify,
    #[cfg(test)]
    conversion_waiting: tokio::sync::Notify,
}

struct WriterFenceState {
    active_writers: usize,
    conversion_active: bool,
}

impl WriterFence {
    fn new() -> Self {
        Self {
            state: Mutex::new(WriterFenceState {
                active_writers: 0,
                conversion_active: false,
            }),
            changed: tokio::sync::Notify::new(),
            #[cfg(test)]
            conversion_waiting: tokio::sync::Notify::new(),
        }
    }

    pub(crate) async fn writer(self: &Arc<Self>) -> WriterFenceGuard {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.state.lock().expect("writer fence lock");
                if !state.conversion_active {
                    state.active_writers += 1;
                    return WriterFenceGuard::Writer(Arc::clone(self));
                }
            }
            notified.await;
        }
    }

    async fn conversion(self: &Arc<Self>) -> WriterFenceGuard {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.state.lock().expect("writer fence lock");
                if state.active_writers == 0 && !state.conversion_active {
                    state.conversion_active = true;
                    return WriterFenceGuard::Conversion(Arc::clone(self));
                }
            }
            #[cfg(test)]
            self.conversion_waiting.notify_waiters();
            notified.await;
        }
    }

    #[cfg(test)]
    fn conversion_waiter(&self) -> tokio::sync::futures::Notified<'_> {
        self.conversion_waiting.notified()
    }
}

pub(crate) enum WriterFenceGuard {
    Writer(Arc<WriterFence>),
    Conversion(Arc<WriterFence>),
}

impl Drop for WriterFenceGuard {
    fn drop(&mut self) {
        match self {
            Self::Writer(fence) => {
                let mut state = fence.state.lock().expect("writer fence lock");
                state.active_writers -= 1;
                drop(state);
                fence.changed.notify_waiters();
            }
            Self::Conversion(fence) => {
                let mut state = fence.state.lock().expect("writer fence lock");
                state.conversion_active = false;
                drop(state);
                fence.changed.notify_waiters();
            }
        }
    }
}

fn coordination_for(kv: &Arc<dyn Kv>) -> Arc<EngineCoordination> {
    static COORDINATORS: OnceLock<Mutex<HashMap<usize, Weak<EngineCoordination>>>> =
        OnceLock::new();

    let identity = Arc::as_ptr(kv).cast::<()>() as usize;
    let mut coordinators = COORDINATORS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("conversion coordinator registry lock");
    coordinators.retain(|_, coordinator| coordinator.strong_count() != 0);
    if let Some(coordinator) = coordinators.get(&identity).and_then(Weak::upgrade) {
        return coordinator;
    }
    let coordinator = Arc::new(EngineCoordination::new());
    coordinators.insert(identity, Arc::downgrade(&coordinator));
    coordinator
}

/// Whether the counter managers (`ProcArray`, `SequenceManager`) persist their
/// counters themselves (`Durable` — the local/single-node path) or fold the
/// counter advance into the commit batch for the replicated state machine to
/// max-merge (`Replicated` — the Raft path, reseeded on leadership change).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum PersistMode {
    Durable,
    Replicated,
}

/// The SQL engine over a durable (or in-memory) KV store. Catalog, sequences,
/// the xid counter, and the clog live in the KV store. Writers run concurrently
/// (SP6): row-level conflicts serialize through the `RowLockManager`, rowid
/// allocation goes through the `SequenceManager`, and DDL serializes among DDLs
/// behind `catalog_lock`. The `ProcArray` is shared so every connection's
/// snapshots see the same running-transaction set.
pub struct SqlEngine {
    /// Keeps the registry entry strongly referenced for this engine's lifetime.
    coordination: Arc<EngineCoordination>,
    pub(crate) kv: Arc<dyn Kv>,
    /// The store catalog lookups (table name→id→schema) resolve through. For the
    /// single-range engine this is the same store as `kv`; under multi-range
    /// sharding the catalog lives only on range 0, so a data range's engine
    /// points this at range 0's store while `kv` holds its own rows.
    pub(crate) catalog_kv: Arc<dyn Kv>,
    pub(crate) procarray: Arc<ProcArray>,
    pub(crate) seq: Arc<SequenceManager>,
    pub(crate) lockmgr: Arc<RowLockManager>,
    pub(crate) catalog_lock: Arc<tokio::sync::Mutex<()>>,
    /// Serializes physical rewrites with ordinary writes. Explicit xid writers
    /// additionally retain `writer_fence` through their terminal outcome.
    pub(crate) table_write_gate: Arc<tokio::sync::RwLock<()>>,
    pub(crate) writer_fence: Arc<WriterFence>,
    /// DML on local tables holds this SHARED (per statement, or until
    /// COMMIT/ROLLBACK in an explicit transaction); unique-index DDL (CREATE
    /// UNIQUE INDEX backfill, CREATE TABLE with a unique constraint) holds it
    /// EXCLUSIVELY so its backfill scan cannot race in-flight writers.
    /// Same-key DML conflicts serialize through per-key locks in `lockmgr`.
    pub(crate) unique_index_lock: Arc<tokio::sync::RwLock<()>>,
    pub(crate) committer: Arc<dyn crate::commit::Committer>,
    pub(crate) linearizer: Arc<dyn crate::read_gate::Linearizer>,
    pub(crate) persist_mode: PersistMode,
    /// Range 0's Global Transaction Manager. `Some` on every range engine of a
    /// multi-range cluster (injected by the cluster after construction); `None`
    /// on a single-range engine. Single-range behavior is byte-for-byte unchanged
    /// when `gtm` is `None`.
    pub(crate) gtm: Option<Arc<gtm::Gtm>>,
    /// A range-0 read barrier, injected by the cluster on every DATA-range engine
    /// (range != 0) of a multi-range node. Before a cross-range resolver reads
    /// range 0's global clog, this catches the node's LOCAL range-0 replica up to
    /// range 0's linearizable applied index. `None` on range 0's own engine (it
    /// reads its own current store) and on single-range engines.
    pub(crate) range0_barrier: Option<Arc<dyn crate::read_gate::Linearizer>>,
    /// SP37: the clock backing each session's transaction/statement instant (and,
    /// later, `now()`/`current_timestamp`). `SystemClock` in production; tests
    /// inject a `FixedClock` via `with_clock` for deterministic temporal eval.
    pub(crate) clock: Arc<dyn crate::clock::Clock>,
    /// SP40: the foreign-table scanner (the `kafka_fdw` seam). `None` until the
    /// binary registers one via `set_foreign_scanner`; a `SELECT` from a foreign
    /// table with no scanner registered returns `0A000`.
    pub(crate) foreign_scanner: Option<Arc<dyn foreign::ForeignScanner>>,
    /// G-8: range-aware table scanner. The default local scanner preserves the
    /// single-range scan path; multi-range assemblies inject a scatter-gather
    /// implementation for sharded/global-visibility reads.
    pub(crate) range_scanner: Arc<dyn scanner::RangeScanner>,
    pub(crate) join_stats: Arc<dyn plan_dist::Stats>,
    pub(crate) join_strategy_config: plan_dist::PlannerConfig,
    /// Timestamp oracle backing the sharded timestamp transaction path.
    pub(crate) timestamp_oracle: Arc<dyn timestamp_txn::TimestampOracle>,
    /// Cached durable-timestamp horizon over `kv`/`catalog_kv`. Seeded lazily
    /// with one full scan, then kept exact by the horizon-observing committer,
    /// so per-statement read floors are O(1) instead of a store scan.
    pub(crate) timestamp_horizon: timestamp_txn::TimestampHorizonSource,
    /// Snapshot pins and the cached decided floor backing the garbage horizon.
    /// Sessions pin their snapshots here so neither write-path pruning, `vacuum`,
    /// nor checkpoint compaction can reclaim a version a live snapshot still sees.
    pub(crate) gc_horizon: Arc<crabka_pgmvcc::gc::GcHorizon>,
    /// The committer vacuum sweeps use for their own prune/freeze/clear
    /// batches. On local engines this is `committer` WITHOUT the
    /// demand-observing wrapper, so a sweep's version rewrites do not re-mark
    /// the swept table as dirty.
    pub(crate) sweep_committer: Arc<dyn crate::commit::Committer>,
    /// Per-table committed version-write counters feeding demand-driven
    /// vacuum skipping (see [`VacuumDemand`]).
    pub(crate) vacuum_demand: Arc<VacuumDemand>,
    /// Resumable sweep cursor and cycle bookkeeping; the async mutex also
    /// serializes concurrent `vacuum`/`vacuum_step` callers.
    vacuum_progress: Arc<tokio::sync::Mutex<VacuumProgress>>,
}

/// Counts returned by [`SqlEngine::vacuum`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct VacuumStats {
    /// Dead tuple versions physically deleted.
    pub versions_pruned: u64,
    /// Orphaned local secondary-index entries physically deleted.
    pub index_entries_pruned: u64,
    /// Surviving tuple versions whose creator was rewritten to `FROZEN_XID`.
    pub versions_frozen: u64,
    /// Clog entries below the horizon physically deleted.
    pub clog_entries_pruned: u64,
    /// Aborted/crashed deleter stamps (`xmax`) cleared from surviving versions.
    pub stamps_cleared: u64,
}

impl std::ops::AddAssign for VacuumStats {
    fn add_assign(&mut self, other: Self) {
        self.versions_pruned += other.versions_pruned;
        self.index_entries_pruned += other.index_entries_pruned;
        self.versions_frozen += other.versions_frozen;
        self.clog_entries_pruned += other.clog_entries_pruned;
        self.stamps_cleared += other.stamps_cleared;
    }
}

/// Counts returned by one bounded [`SqlEngine::vacuum_step`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct VacuumStepStats {
    /// Physical reclamation performed by this step.
    pub stats: VacuumStats,
    /// Tuple version keys examined by this step's interval scans.
    pub keys_examined: u64,
    /// Whether this step completed a full sweep cycle over every ordinary
    /// table (including tables provably clean enough to skip).
    pub cycle_completed: bool,
}

/// Rowids spanned by one bounded vacuum interval scan. Version keys sort by
/// rowid within a table, so `[cursor, cursor + stride)` is a cheap range scan
/// that never materializes more than one interval of chain keys.
const VACUUM_INTERVAL_ROWIDS: u64 = 8_192;

/// Tuple version keys one [`SqlEngine::vacuum_step`] examines before pausing;
/// the sweep resumes from its cursor on the next step. Bounds a step's work at
/// O(budget) instead of O(total data) so the periodic sweep cannot starve
/// foreground statements on large stores. Sized so a fully-dirty step (every
/// scanned version needs a per-row freeze commit — the post-bulk-load
/// catch-up worst case) stays well under 10% of a 2s tick; measured on a
/// 10M-row point-SELECT workload, 32k budgets cost ~30% foreground
/// throughput while 8k budgets are within noise. Pacing loops that need to
/// catch up should prefer MORE steps over larger ones
/// ([`SqlEngine::vacuum_step_budgeted`] callers own that trade-off).
pub const VACUUM_STEP_KEY_BUDGET: usize = 8_192;

/// Minimum budget charge per interval scan, so one step over sparse or empty
/// rowid space still terminates after a bounded number of range scans.
const VACUUM_INTERVAL_MIN_COST: usize = 64;

/// Candidate rows processed between cooperative yields inside one interval.
const VACUUM_YIELD_EVERY_ROWS: usize = 128;

/// Per-table version-write accounting driving demand-driven vacuum sweeps.
///
/// The demand-observing committer bumps a table's counter after every
/// committed batch that Puts an ordinary primary MVCC version key (new
/// versions and `xmax` stamps alike). The sweep snapshots the counter when it
/// enters a table and skips the table on later cycles while the counter is
/// unchanged AND the recorded sweep left every surviving version fully
/// settled (frozen `xmin`, invalid `xmax`): such a table holds no reclaimable
/// garbage, nothing to freeze or clear, and no clog dependence, so skipping
/// it can never invalidate a later clog truncation.
#[derive(Default)]
pub(crate) struct VacuumDemand {
    /// Monotone count of committed primary-version Puts per ordinary table.
    version_puts: Mutex<HashMap<u32, u64>>,
    /// Monotone engine-wide sum of the per-table counters. Every committed
    /// primary-version Put is one unit of eventual sweep work (a dead version
    /// to prune, a survivor to freeze, or a stamp to clear), so the delta of
    /// this counter between two sweep steps is the garbage-creation side of a
    /// pacing controller's debt ledger.
    total_version_puts: std::sync::atomic::AtomicU64,
}

impl VacuumDemand {
    /// Table ids of every ordinary primary-version Put in `ops` (duplicates
    /// kept: each Put counts once).
    fn version_put_tables(ops: &[crabka_pgkv::WriteOp]) -> Vec<u32> {
        ops.iter()
            .filter_map(|op| {
                let (crabka_pgkv::WriteOp::Put { key, .. }
                | crabka_pgkv::WriteOp::ConditionalPut { key, .. }) = op
                else {
                    return None;
                };
                match crabka_pgkv::key::classify_key(key) {
                    crabka_pgkv::key::KeyClass::PrimaryVersion { table_id, .. } => Some(table_id),
                    _ => None,
                }
            })
            .collect()
    }

    /// Record committed version Puts. Called only AFTER the batch is durably
    /// applied, so a sweep that observed a count has the counted data visible.
    fn record(&self, touched: &[u32]) {
        if touched.is_empty() {
            return;
        }
        let mut version_puts = self.version_puts.lock().expect("vacuum demand counters");
        for table_id in touched {
            *version_puts.entry(*table_id).or_insert(0) += 1;
        }
        self.total_version_puts
            .fetch_add(touched.len() as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// The current committed version-Put count for one table.
    fn version_puts_to(&self, table_id: u32) -> u64 {
        self.version_puts
            .lock()
            .expect("vacuum demand counters")
            .get(&table_id)
            .copied()
            .unwrap_or(0)
    }
}

/// Committer decorator feeding [`VacuumDemand`]. Counting happens after the
/// inner commit succeeds, so a sweep never observes a count whose data is not
/// yet visible. The engine's own sweep commits bypass this wrapper (through
/// `sweep_committer`) so freeze/clear rewrites do not re-mark their table as
/// dirty and re-trigger the sweep forever.
struct VacuumDemandObservingCommitter {
    inner: Arc<dyn crate::commit::Committer>,
    demand: Arc<VacuumDemand>,
}

#[async_trait::async_trait]
impl crate::commit::Committer for VacuumDemandObservingCommitter {
    async fn commit(&self, ops: Vec<crabka_pgkv::WriteOp>) -> Result<(), ExecError> {
        let touched = VacuumDemand::version_put_tables(&ops);
        self.inner.commit(ops).await?;
        self.demand.record(&touched);
        Ok(())
    }
}

/// Resumable engine-level sweep state: the table/rowid cursor, per-cycle clog
/// bookkeeping, and the per-table clean-sweep records demand skipping uses.
/// Shared by every handle of one engine; steps serialize on the enclosing
/// async mutex.
#[derive(Default)]
struct VacuumProgress {
    /// The sweep resumes at the first ordinary table whose id is at least
    /// this (`u64` so advancing past `u32::MAX` cannot overflow).
    cursor_table: u64,
    /// The sweep resumes at this rowid within the cursor table.
    cursor_rowid: u64,
    /// Lowest garbage horizon used by any interval of the in-progress cycle:
    /// the only clog-truncation floor every swept region provably supports.
    /// (The horizon is monotone across steps, so this is normally the first
    /// step's horizon.)
    cycle_floor: Option<u64>,
    /// Whether any candidate row was skipped this cycle (a transient lock
    /// verdict) — defers clog truncation to the next full cycle.
    cycle_skipped: bool,
    /// Scratch for the table currently under the cursor.
    current: Option<TableSweepScratch>,
    /// Latest completed clean sweep per table id (see [`VacuumDemand`]).
    swept: HashMap<u32, TableSweepRecord>,
}

impl VacuumProgress {
    /// Restart at the beginning of a fresh cycle (the full-pass
    /// [`SqlEngine::vacuum`] entry point), discarding partial-cycle state but
    /// keeping the per-table clean-sweep records — their validity depends
    /// only on the demand counters, not on cycle boundaries.
    fn restart_cycle(&mut self) {
        self.cursor_table = 0;
        self.cursor_rowid = 0;
        self.cycle_floor = None;
        self.cycle_skipped = false;
        self.current = None;
    }
}

/// In-progress accounting for the table currently under the sweep cursor.
struct TableSweepScratch {
    table_id: u32,
    /// Demand counter value when this cycle's sweep entered the table.
    entry_version_puts: u64,
    /// Surviving versions this sweep leaves less than fully settled
    /// (non-frozen `xmin` or a deleter stamp it cannot clear).
    unsettled: u64,
    /// Whether a transient lock verdict skipped a candidate row in this table.
    skipped: bool,
    /// Exclusive rowid bound for this table's sweep: the table's durable
    /// next-rowid at entry. Durable-mode sequences persist a block ahead of
    /// every handed-out rowid (see `SequenceManager`), so no row present at
    /// entry can sit at or beyond it; rows inserted afterwards carry xids at
    /// or above the cycle floor and need no pruning, freezing, or clog entry
    /// this cycle.
    terminal: u64,
}

/// One table's latest completed clean sweep (no lock-skipped rows).
struct TableSweepRecord {
    /// Demand counter snapshot taken when that sweep entered the table.
    version_puts_at_entry: u64,
    /// Surviving versions that sweep left less than fully settled.
    unsettled: u64,
}

/// Outcome of sweeping one bounded rowid interval of one table.
#[derive(Default)]
struct VacuumIntervalOutcome {
    stats: VacuumStats,
    /// Tuple version keys the interval scan returned.
    keys: usize,
    /// Surviving versions left less than fully settled.
    unsettled: u64,
    /// Whether a transient lock verdict skipped a candidate row.
    skipped: bool,
}

/// Whether `xmax` is a deleter stamp vacuum may clear at `horizon`: a decided
/// abort (terminal, immutable at any xid), or a sub-horizon absent/in-progress
/// entry — a crashed transaction that can never commit (every xid below the
/// horizon is decided, and a PRESENT sub-horizon entry is always terminal).
/// Committed stamps are never cleared: a committed sub-horizon deleter makes
/// the version dead instead, and a committed deleter at or above the horizon
/// may still be visible-to-delete for some snapshot.
fn vacuum_stamp_is_clearable(
    xmax: u64,
    horizon: u64,
    clog_status: &impl Fn(u64) -> Result<crabka_pgmvcc::clog::XidStatus, crabka_pgkv::KvError>,
) -> Result<bool, crabka_pgkv::KvError> {
    if xmax == crabka_pgmvcc::xid::INVALID_XID {
        return Ok(false);
    }
    Ok(match clog_status(xmax)? {
        crabka_pgmvcc::clog::XidStatus::Aborted => true,
        crabka_pgmvcc::clog::XidStatus::InProgress => xmax < horizon,
        crabka_pgmvcc::clog::XidStatus::Committed | crabka_pgmvcc::clog::XidStatus::Prepared(_) => {
            false
        }
    })
}

/// Timestamp ownership carried by a local MVCC scan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TimestampScanOwner {
    /// Statement read timestamp used for sharded scans.
    pub read_ts: Option<timestamp_txn::ReadTimestamp>,
    /// Pending timestamp transaction whose intents are visible to this scan.
    pub own_start_ts: Option<timestamp_txn::TimestampTransactionId>,
}

impl Default for SqlEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlEngine {
    /// Ephemeral in-memory engine (tests, default when no --data-dir).
    /// # Panics
    ///
    /// Panics if an internal execution invariant is violated.
    pub fn new() -> Self {
        Self::with_kv(Arc::new(MemKv::new())).expect("in-memory engine never fails to open")
    }

    /// Durable engine backed by a fjall store at `path`.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ExecError> {
        Self::with_kv(Arc::new(FjallKv::open(path)?))
    }

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn with_kv(kv: Arc<dyn Kv>) -> Result<Self, ExecError> {
        let coordination = coordination_for(&kv);
        let procarray = Arc::new(ProcArray::open(Arc::clone(&kv), PersistMode::Durable)?);
        let sweep_committer = timestamp_txn::HorizonObservingCommitter::wrap(
            Arc::new(crate::commit::LocalCommitter {
                kv: Arc::clone(&kv),
            }),
            &kv,
        );
        let vacuum_demand = Arc::new(VacuumDemand::default());
        let committer: Arc<dyn crate::commit::Committer> =
            Arc::new(VacuumDemandObservingCommitter {
                inner: Arc::clone(&sweep_committer),
                demand: Arc::clone(&vacuum_demand),
            });
        let timestamp_horizon =
            timestamp_txn::TimestampHorizonSource::new(Arc::clone(&kv), Arc::clone(&kv), false);
        Ok(Self {
            coordination: Arc::clone(&coordination),
            catalog_kv: Arc::clone(&kv),
            kv: Arc::clone(&kv),
            procarray,
            seq: Arc::new(SequenceManager::new(PersistMode::Durable)),
            lockmgr: Arc::new(RowLockManager::new()),
            catalog_lock: Arc::clone(&coordination.catalog_lock),
            table_write_gate: Arc::clone(&coordination.table_write_gate),
            writer_fence: Arc::clone(&coordination.writer_fence),
            unique_index_lock: Arc::new(tokio::sync::RwLock::new(())),
            committer,
            linearizer: Arc::new(crate::read_gate::LocalLinearizer),
            persist_mode: PersistMode::Durable,
            gtm: None,
            range0_barrier: None,
            clock: Arc::new(crate::clock::SystemClock),
            foreign_scanner: None,
            range_scanner: Arc::new(scanner::LocalRangeScanner),
            join_stats: Arc::new(plan_dist::DurableSequenceStats::new(Arc::clone(&kv))),
            join_strategy_config: plan_dist::PlannerConfig::default(),
            timestamp_oracle: Arc::new(timestamp_txn::LocalTimestampOracle::default()),
            timestamp_horizon,
            gc_horizon: Arc::new(crabka_pgmvcc::gc::GcHorizon::new()),
            sweep_committer,
            vacuum_demand,
            vacuum_progress: Arc::new(tokio::sync::Mutex::new(VacuumProgress::default())),
        })
    }

    /// Oldest xid that a checkpoint may vacuum without changing visibility.
    ///
    /// The active-snapshot xmin is capped by the lowest registered snapshot pin
    /// and by the first non-terminal clog entry at or above the durable recovery
    /// scan watermark, so neither a live reader's snapshot nor prepared/
    /// in-progress state can ever be pruned or frozen past.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn checkpoint_garbage_horizon(&self) -> Result<u64, ExecError> {
        checkpoint_garbage_horizon(
            self.procarray.as_ref(),
            self.kv.as_ref(),
            self.gc_horizon.as_ref(),
        )
    }

    /// A shareable horizon callback for checkpoint runtimes that outlive engine setup.
    pub fn checkpoint_horizon_provider(
        &self,
    ) -> Arc<dyn Fn() -> Result<u64, ExecError> + Send + Sync> {
        let procarray = Arc::clone(&self.procarray);
        let kv = Arc::clone(&self.kv);
        let gc_horizon = Arc::clone(&self.gc_horizon);
        Arc::new(move || {
            checkpoint_garbage_horizon(procarray.as_ref(), kv.as_ref(), gc_horizon.as_ref())
        })
    }

    /// Whether this engine may physically reclaim dead MVCC versions locally
    /// (write-path chain pruning and [`SqlEngine::vacuum`]).
    ///
    /// True only for the plain single-range local engine (`new`/`open`/
    /// `with_kv`): Durable persist mode, no GTM, no range-0 barrier, and a
    /// single store for data + catalog. Replicated engines apply WAL batches
    /// deterministically — a local delete outside the WAL would diverge
    /// replicas and checkpoints — and multi-range engines can carry global
    /// xids whose lifecycle the local horizon cannot judge.
    #[must_use]
    pub fn supports_local_vacuum(&self) -> bool {
        self.persist_mode == PersistMode::Durable
            && self.gtm.is_none()
            && self.range0_barrier.is_none()
            && Arc::ptr_eq(&self.kv, &self.catalog_kv)
    }

    /// Engine-level garbage sweep: physically delete every dead MVCC version
    /// (and its orphaned local secondary-index entries) in every ordinary
    /// table, freeze the surviving sub-horizon tuples, truncate the clog
    /// below the horizon, and advance the durable clog scan floor to it.
    ///
    /// A version is dead iff its creator aborted (or crashed below the
    /// horizon), or it was deleted/superseded by a transaction that committed
    /// below the garbage horizon ([`crabka_pgmvcc::gc::version_is_dead`]);
    /// the horizon is capped by running writers, registered snapshot pins,
    /// and the first non-terminal clog entry, so nothing any live or future
    /// snapshot can see is touched. Freezing rewrites a surviving committed
    /// sub-horizon creator to `FROZEN_XID` (always visible without a clog
    /// lookup), which is what makes deleting the sub-horizon clog entries
    /// safe — without truncation the clog grows one entry per write
    /// transaction forever.
    ///
    /// A no-op (returning zeroed [`VacuumStats`]) on engines where
    /// [`SqlEngine::supports_local_vacuum`] is false.
    ///
    /// Internally this restarts the incremental sweep at the first table and
    /// runs bounded [`SqlEngine::vacuum_step`] chunks until the cycle
    /// completes, so one call still means one full pass. Long-running
    /// processes should call `vacuum_step` on a short period instead and let
    /// the pass spread across steps.
    ///
    /// # Errors
    ///
    /// Returns an error when catalog or store access fails or a delete batch
    /// cannot be committed.
    pub async fn vacuum(&self) -> Result<VacuumStats, ExecError> {
        if !self.supports_local_vacuum() {
            return Ok(VacuumStats::default());
        }
        let mut progress = self.vacuum_progress.lock().await;
        progress.restart_cycle();
        let mut total = VacuumStats::default();
        loop {
            let step = self
                .vacuum_step_locked(&mut progress, VACUUM_STEP_KEY_BUDGET)
                .await?;
            total += step.stats;
            if step.cycle_completed {
                return Ok(total);
            }
        }
    }

    /// Monotone count of committed primary-version Puts across every ordinary
    /// table (new versions and `xmax` stamps alike), observed after each
    /// batch is durably applied. Each Put is one unit of eventual sweep work,
    /// so the delta between two reads measures garbage creation — the input
    /// side of an adaptive vacuum pacing loop.
    #[must_use]
    pub fn committed_version_puts(&self) -> u64 {
        self.vacuum_demand
            .total_version_puts
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Run one bounded increment of the engine-level garbage sweep with the
    /// default [`VACUUM_STEP_KEY_BUDGET`] (see [`SqlEngine::vacuum`] for what
    /// a full cycle does).
    ///
    /// A step examines at most a budgeted number of tuple version keys,
    /// resuming from a persistent-in-memory `(table, rowid)` cursor and
    /// wrapping around the table list, so a full pass over a large store
    /// spreads across many steps instead of storming the store in one call.
    /// Tables whose demand counters prove them fully settled since their last
    /// clean sweep are skipped without scanning. The clog-truncation + scan
    /// floor advance runs only on the step that completes a cycle in which
    /// nothing was lock-skipped.
    ///
    /// A no-op on engines where [`SqlEngine::supports_local_vacuum`] is false.
    ///
    /// # Errors
    ///
    /// Returns an error when catalog or store access fails or a delete batch
    /// cannot be committed.
    pub async fn vacuum_step(&self) -> Result<VacuumStepStats, ExecError> {
        self.vacuum_step_budgeted(VACUUM_STEP_KEY_BUDGET).await
    }

    /// [`SqlEngine::vacuum_step`] with a caller-chosen key budget, for pacing
    /// loops that tune step size to observed step latency. The budget bounds
    /// one step's scan work; callers keep steps short (low-single-digit
    /// milliseconds) and adapt by running MORE steps, not unbounded ones.
    ///
    /// # Errors
    ///
    /// Returns an error when catalog or store access fails or a delete batch
    /// cannot be committed.
    pub async fn vacuum_step_budgeted(
        &self,
        key_budget: usize,
    ) -> Result<VacuumStepStats, ExecError> {
        if !self.supports_local_vacuum() {
            return Ok(VacuumStepStats::default());
        }
        let mut progress = self.vacuum_progress.lock().await;
        self.vacuum_step_locked(&mut progress, key_budget.max(1))
            .await
    }

    /// One budgeted sweep step over the cursor's tables (caller holds the
    /// progress lock).
    async fn vacuum_step_locked(
        &self,
        progress: &mut VacuumProgress,
        key_budget: usize,
    ) -> Result<VacuumStepStats, ExecError> {
        let horizon = self.checkpoint_garbage_horizon()?;
        // Timestamp/sharded tables use ts tuples with their own resolution
        // rules; only ordinary xid-MVCC tables are swept.
        let mut tables: Vec<crabka_pgcatalog::Table> =
            crabka_pgcatalog::list_tables(self.catalog_kv.as_ref())?
                .into_iter()
                .filter(|table| !table.sharded && table.sharding.is_none())
                .collect();
        tables.sort_unstable_by_key(|table| table.id);
        progress
            .swept
            .retain(|id, _| tables.iter().any(|table| table.id == *id));
        progress.cycle_floor = Some(
            progress
                .cycle_floor
                .map_or(horizon, |floor| floor.min(horizon)),
        );

        // A unique lock-owner id for the sweep, allocated lazily on the first
        // interval that actually locks a row. `begin_write` registers it as
        // running (harmless: it is the newest xid, so it lowers no horizon)
        // and it writes no tuples and no clog entry, so after `finish` it is
        // indistinguishable from a crashed no-op transaction.
        let mut vacuum_xid: Option<u64> = None;
        let result = self
            .vacuum_step_chunks(progress, &tables, horizon, &mut vacuum_xid, key_budget)
            .await;
        // Belt-and-braces: per-row sweeps release as they go; make sure nothing
        // stays held (or registered) on an error path.
        if let Some(xid) = vacuum_xid {
            self.lockmgr.release_all(xid);
            self.procarray.finish(xid);
        }
        let mut out = result?;
        if out.cycle_completed {
            let floor = progress.cycle_floor.take();
            let lock_skipped = std::mem::take(&mut progress.cycle_skipped);
            // Truncate the clog below the cycle floor — but only after EVERY
            // ordinary table was fully swept this cycle (or skipped as
            // provably settled): sweeping pruned every dead sub-floor version
            // and froze every surviving one, so no visibility check can
            // consult a deleted entry (an absent sub-floor entry already
            // reads as decided-by-crash). A row skipped by a transient lock
            // verdict defers truncation to the next cycle.
            if !lock_skipped && let Some(floor) = floor {
                out.stats.clog_entries_pruned += self.truncate_clog_below(floor).await?;
                // Advance the durable clog scan floor. Safe by the horizon
                // contract: every xid below `floor` is decided (or a crashed
                // leftover that can never commit), and the horizon never
                // passes a non-terminal (InProgress/Prepared) entry — so the
                // recovery watermark invariant ("never advance past a
                // non-terminal marker") holds. On this single-range local
                // engine no Prepared marker can exist at all.
                self.advance_clog_scan_lo(floor).await?;
            }
        }
        Ok(out)
    }

    /// Walk the sweep cursor forward until the step budget is spent or the
    /// cycle wraps past the last table.
    async fn vacuum_step_chunks(
        &self,
        progress: &mut VacuumProgress,
        tables: &[crabka_pgcatalog::Table],
        horizon: u64,
        vacuum_xid: &mut Option<u64>,
        key_budget: usize,
    ) -> Result<VacuumStepStats, ExecError> {
        let mut out = VacuumStepStats::default();
        let mut budget = key_budget;
        loop {
            let Some(table) = tables
                .iter()
                .find(|table| u64::from(table.id) >= progress.cursor_table)
            else {
                // Past the last table: the cycle is complete; wrap the cursor.
                out.cycle_completed = true;
                progress.cursor_table = 0;
                progress.cursor_rowid = 0;
                progress.current = None;
                break;
            };
            let entering = u64::from(table.id) != progress.cursor_table
                || progress.current.as_ref().map(|scratch| scratch.table_id) != Some(table.id);
            if entering {
                progress.cursor_table = u64::from(table.id);
                progress.cursor_rowid = 0;
                let entry_version_puts = self.vacuum_demand.version_puts_to(table.id);
                if progress.swept.get(&table.id).is_some_and(|record| {
                    record.version_puts_at_entry == entry_version_puts && record.unsettled == 0
                }) {
                    // No version write since the table's last clean sweep, and
                    // that sweep left every survivor fully settled (frozen
                    // xmin, invalid xmax): nothing to prune, freeze, or clear,
                    // and no clog dependence — skip the table without a scan.
                    progress.cursor_table = u64::from(table.id) + 1;
                    progress.current = None;
                    continue;
                }
                progress.current = Some(TableSweepScratch {
                    table_id: table.id,
                    entry_version_puts,
                    unsettled: 0,
                    skipped: false,
                    terminal: crate::exec::read_seq_kv(self.kv.as_ref(), table.id)?,
                });
            }
            let terminal = progress.current.as_ref().expect("sweep scratch").terminal;
            if progress.cursor_rowid >= terminal {
                // Table finished: record the sweep so demand skipping can
                // prove the table clean on later cycles.
                let scratch = progress.current.take().expect("sweep scratch");
                if scratch.skipped {
                    progress.cycle_skipped = true;
                    progress.swept.remove(&table.id);
                } else {
                    progress.swept.insert(
                        table.id,
                        TableSweepRecord {
                            version_puts_at_entry: scratch.entry_version_puts,
                            unsettled: scratch.unsettled,
                        },
                    );
                }
                progress.cursor_table = u64::from(table.id) + 1;
                progress.cursor_rowid = 0;
                continue;
            }
            if budget == 0 {
                break;
            }
            let start = progress.cursor_rowid;
            let end = start.saturating_add(VACUUM_INTERVAL_ROWIDS).min(terminal);
            let interval = self
                .vacuum_interval(table, horizon, vacuum_xid, start..end)
                .await?;
            budget = budget.saturating_sub(interval.keys.max(VACUUM_INTERVAL_MIN_COST));
            out.keys_examined += interval.keys as u64;
            out.stats += interval.stats;
            let scratch = progress.current.as_mut().expect("sweep scratch");
            scratch.unsettled += interval.unsettled;
            scratch.skipped |= interval.skipped;
            progress.cursor_rowid = end;
            // Yield between intervals so foreground statements interleave
            // even when every interval completes without blocking.
            tokio::task::yield_now().await;
        }
        Ok(out)
    }

    /// Sweep one table's version chains for rowids in `interval` at `horizon`
    /// (see `vacuum`). Allocates the sweep's lock-owner xid on first use.
    async fn vacuum_interval(
        &self,
        table: &crabka_pgcatalog::Table,
        horizon: u64,
        vacuum_xid: &mut Option<u64>,
        interval: std::ops::Range<u64>,
    ) -> Result<VacuumIntervalOutcome, ExecError> {
        let mut outcome = VacuumIntervalOutcome::default();
        // Hold the shared physical gate so a concurrent conversion (which
        // rewrites the whole table under the exclusive half) serializes with
        // the sweep, exactly like an ordinary writer.
        let _gate = Arc::clone(&self.table_write_gate).read_owned().await;
        let clog_status = |xid| crabka_pgmvcc::clog::get(self.kv.as_ref(), xid);
        let scan = self.kv.scan_range(
            &crabka_pgkv::key::row_key(table.id, interval.start),
            &crabka_pgkv::key::row_key(table.id, interval.end),
        )?;
        outcome.keys = scan.len();
        // Lock-free candidate pre-scan: deadness, freezability, and stamp
        // clearability at a fixed horizon are stable (terminal clog states
        // are immutable), so this can only under-report relative to the
        // locked re-read below — and versions written after it carry xids at
        // or above the horizon, which need no sweep work this cycle. The
        // per-rowid flag records whether any version needs a stamp clear, so
        // the common freeze-only row skips a third chain scan under its lock.
        let mut candidates: std::collections::BTreeMap<u64, bool> =
            std::collections::BTreeMap::new();
        for (key, value) in scan {
            let (xmin, xmax, _row) = crabka_pgmvcc::version::decode_tuple(&value)?;
            let dead = crabka_pgmvcc::gc::version_is_dead(xmin, xmax, horizon, &clog_status)?;
            let freezable = !dead
                && xmin != crabka_pgmvcc::xid::FROZEN_XID
                && xmin < horizon
                && matches!(
                    clog_status(xmin)?,
                    crabka_pgmvcc::clog::XidStatus::Committed
                );
            let clearable = !dead && vacuum_stamp_is_clearable(xmax, horizon, &clog_status)?;
            if dead || freezable || clearable {
                let prefix = crabka_pgmvcc::version::row_prefix_of(&key)?;
                *candidates
                    .entry(crabka_pgkv::key::rowid_of(table.id, prefix)?)
                    .or_insert(false) |= clearable;
            }
            // Count survivors this pass will NOT leave fully settled: they
            // keep the table on the sweep schedule for the next cycle.
            if !dead
                && !((xmin == crabka_pgmvcc::xid::FROZEN_XID || freezable)
                    && (xmax == crabka_pgmvcc::xid::INVALID_XID || clearable))
            {
                outcome.unsettled += 1;
            }
        }
        if candidates.is_empty() {
            return Ok(outcome);
        }
        let local_indexes: Vec<crabka_pgcatalog::Index> =
            crabka_pgcatalog::list_table_indexes(self.catalog_kv.as_ref(), &table.name)?
                .into_iter()
                .filter(|index| index.placement == crabka_pgcatalog::IndexPlacement::Local)
                .collect();
        let owner_xid = match *vacuum_xid {
            Some(xid) => xid,
            None => {
                let xid = self.procarray.begin_write()?;
                *vacuum_xid = Some(xid);
                xid
            }
        };
        for (processed, (rowid, needs_clear)) in candidates.into_iter().enumerate() {
            if processed != 0 && processed % VACUUM_YIELD_EVERY_ROWS == 0 {
                tokio::task::yield_now().await;
            }
            // Take the writer's exclusive row lock: dead version KEYS never
            // collide with a writer's puts, but the index-entry survivor
            // computation must not race a concurrent writer re-adding the
            // same indexed values, and a freeze/clear rewrite must not race a
            // writer stamping xmax on the same key. Holding at most this
            // one lock, the sweep cannot close a wait-for cycle of its
            // own; a transient deadlock verdict (a just-woken waiter's
            // stale edge) simply skips the row until the next sweep.
            if self
                .lockmgr
                .acquire(
                    table.id,
                    rowid,
                    crate::lockmgr::LockMode::Exclusive,
                    owner_xid,
                )
                .await
                .is_err()
            {
                outcome.skipped = true;
                continue;
            }
            let pruned = crate::exec::prune_rowid_chain_ops(
                self.kv.as_ref(),
                table,
                &local_indexes,
                &crate::exec::ChainPruneRequest {
                    rowid,
                    horizon,
                    keep_xids: &[],
                    new_row: None,
                    freeze_below: Some(horizon),
                },
            )
            .and_then(|mut prune| {
                // A row whose pre-scan saw no clearable stamp skips the clear
                // re-scan; a stamp aborted since then stays unsettled in this
                // interval's accounting, so the next cycle picks it up.
                let cleared = if needs_clear {
                    self.clear_settled_stamp_ops(table.id, rowid, horizon, &mut prune)?
                } else {
                    0
                };
                Ok((prune, cleared))
            });
            let commit = match pruned {
                Ok((prune, _)) if prune.ops.is_empty() => Ok(()),
                Ok((prune, cleared)) => {
                    outcome.stats.versions_pruned += prune.versions;
                    outcome.stats.index_entries_pruned += prune.index_entries;
                    outcome.stats.versions_frozen += prune.frozen;
                    outcome.stats.stamps_cleared += cleared;
                    self.sweep_committer.commit(prune.ops).await
                }
                Err(error) => Err(error),
            };
            // Targeted release of the single row lock this iteration took:
            // `release_all` walks the WHOLE lock table, which is quadratic
            // per interval whenever a concurrent bulk writer holds a large
            // lock set.
            self.lockmgr
                .release_key(&crate::lockmgr::LockKey::Row(table.id, rowid), owner_xid);
            commit?;
        }
        Ok(outcome)
    }

    /// Under the row's exclusive lock, extend a prune batch with rewrites that
    /// clear aborted/crashed deleter stamps from surviving versions (see
    /// [`vacuum_stamp_is_clearable`]). Every snapshot already reads such a
    /// version as not deleted, so the rewrite is invisible; it only removes
    /// the version's dependence on the deleter's clog entry so the row can
    /// become fully settled and its table skippable. Returns the number of
    /// stamps cleared.
    fn clear_settled_stamp_ops(
        &self,
        table_id: u32,
        rowid: u64,
        horizon: u64,
        prune: &mut crate::exec::ChainPrune,
    ) -> Result<u64, ExecError> {
        let clog_status = |xid| crabka_pgmvcc::clog::get(self.kv.as_ref(), xid);
        let mut cleared: u64 = 0;
        for (key, value) in self
            .kv
            .scan_prefix(&crabka_pgkv::key::row_key(table_id, rowid))?
        {
            // Versions the batch already deletes need no stamp rewrite.
            if prune.ops.iter().any(
                |op| matches!(op, crabka_pgkv::WriteOp::Delete { key: deleted } if *deleted == key),
            ) {
                continue;
            }
            let (_, xmax, _) = crabka_pgmvcc::version::decode_tuple(&value)?;
            if !vacuum_stamp_is_clearable(xmax, horizon, &clog_status)? {
                continue;
            }
            // Rebase on the batch's own freeze rewrite of the same key, if
            // any, so both header rewrites land in one Put.
            if let Some(pending) = prune.ops.iter_mut().find_map(|op| match op {
                crabka_pgkv::WriteOp::Put { key: frozen, value } if *frozen == key => Some(value),
                _ => None,
            }) {
                *pending = crabka_pgmvcc::version::clear_tuple_xmax(pending)?;
            } else {
                prune.ops.push(crabka_pgkv::WriteOp::Put {
                    key,
                    value: crabka_pgmvcc::version::clear_tuple_xmax(&value)?,
                });
            }
            cleared += 1;
        }
        Ok(cleared)
    }

    /// Delete every clog entry strictly below `horizon`, in bounded batches.
    /// Callers must have frozen/pruned every version referencing those xids.
    async fn truncate_clog_below(&self, horizon: u64) -> Result<u64, ExecError> {
        let mut deleted: u64 = 0;
        let mut batch: Vec<crabka_pgkv::WriteOp> = Vec::new();
        for (key, _) in self.kv.scan_range(
            &crabka_pgkv::key::clog_key(0),
            &crabka_pgkv::key::clog_key(horizon),
        )? {
            batch.push(crabka_pgkv::WriteOp::Delete { key });
            if batch.len() == 4096 {
                deleted += batch.len() as u64;
                self.committer.commit(std::mem::take(&mut batch)).await?;
            }
        }
        if !batch.is_empty() {
            deleted += batch.len() as u64;
            self.committer.commit(batch).await?;
        }
        Ok(deleted)
    }

    /// Build an engine whose reads come from `sm_kv` (the applied state machine)
    /// and whose writes are proposed through `committer` (a RaftCommitter). Uses
    /// the Replicated persist mode so counters fold into the proposed batch.
    ///
    /// `catalog_kv` is the store catalog (schema) lookups resolve through. For a
    /// single-range node it is the same `Arc` as `sm_kv`; a multi-range data
    /// node passes range 0's applied store here while `sm_kv` holds its own rows.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn replicated(
        catalog_kv: Arc<dyn Kv>,
        sm_kv: Arc<dyn Kv>,
        committer: Arc<dyn crate::commit::Committer>,
        linearizer: Arc<dyn crate::read_gate::Linearizer>,
    ) -> Result<Self, ExecError> {
        let coordination = coordination_for(&sm_kv);
        let procarray = Arc::new(ProcArray::open(
            Arc::clone(&sm_kv),
            PersistMode::Replicated,
        )?);
        let committer = timestamp_txn::HorizonObservingCommitter::wrap(committer, &sm_kv);
        let timestamp_horizon = timestamp_txn::TimestampHorizonSource::new(
            Arc::clone(&sm_kv),
            Arc::clone(&catalog_kv),
            false,
        );
        Ok(Self {
            coordination: Arc::clone(&coordination),
            catalog_kv,
            kv: Arc::clone(&sm_kv),
            procarray,
            seq: Arc::new(SequenceManager::new(PersistMode::Replicated)),
            lockmgr: Arc::new(RowLockManager::new()),
            catalog_lock: Arc::clone(&coordination.catalog_lock),
            table_write_gate: Arc::clone(&coordination.table_write_gate),
            writer_fence: Arc::clone(&coordination.writer_fence),
            unique_index_lock: Arc::new(tokio::sync::RwLock::new(())),
            // Replicated engines never vacuum locally, so no demand-observing
            // wrapper and the sweep committer is the ordinary one.
            sweep_committer: Arc::clone(&committer),
            committer,
            linearizer,
            persist_mode: PersistMode::Replicated,
            gtm: None,
            range0_barrier: None,
            clock: Arc::new(crate::clock::SystemClock),
            foreign_scanner: None,
            range_scanner: Arc::new(scanner::LocalRangeScanner),
            join_stats: Arc::new(plan_dist::DurableSequenceStats::new(Arc::clone(&sm_kv))),
            join_strategy_config: plan_dist::PlannerConfig::default(),
            timestamp_oracle: Arc::new(timestamp_txn::LocalTimestampOracle::default()),
            timestamp_horizon,
            gc_horizon: Arc::new(crabka_pgmvcc::gc::GcHorizon::new()),
            vacuum_demand: Arc::new(VacuumDemand::default()),
            vacuum_progress: Arc::new(tokio::sync::Mutex::new(VacuumProgress::default())),
        })
    }

    /// Reseed counters from the applied store (call when this node becomes leader).
    ///
    /// Also invalidates the cached durable-timestamp horizon, so the next
    /// statement rescans the applied store for timestamp state that was
    /// replicated to this node while another leader was committing.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn reseed_counters(&self) -> Result<(), ExecError> {
        self.procarray.reseed_from_applied()?;
        self.seq.reseed_from_applied();
        self.timestamp_horizon.invalidate();
        Ok(())
    }

    /// Record that timestamp state at or below `timestamp` is durable on this
    /// engine's stores even though the write did not flow through this
    /// engine's committer (for example, an external replication apply path).
    /// Read, transaction, and commit timestamps allocated afterwards stay
    /// strictly above it.
    pub fn observe_durable_timestamp(&self, timestamp: u64) {
        self.timestamp_horizon.observe(timestamp);
    }

    /// A second handle to the SAME engine (all fields are `Arc`/`Copy`): every
    /// clone shares the applied store, committer, linearizer, and counters.
    /// Used by the gateway to give each connection its own router without
    /// re-opening the engine.
    pub fn clone_handle(&self) -> SqlEngine {
        SqlEngine {
            coordination: Arc::clone(&self.coordination),
            kv: Arc::clone(&self.kv),
            catalog_kv: Arc::clone(&self.catalog_kv),
            procarray: Arc::clone(&self.procarray),
            seq: Arc::clone(&self.seq),
            lockmgr: Arc::clone(&self.lockmgr),
            catalog_lock: Arc::clone(&self.catalog_lock),
            table_write_gate: Arc::clone(&self.table_write_gate),
            writer_fence: Arc::clone(&self.writer_fence),
            unique_index_lock: Arc::clone(&self.unique_index_lock),
            committer: Arc::clone(&self.committer),
            linearizer: Arc::clone(&self.linearizer),
            persist_mode: self.persist_mode,
            gtm: self.gtm.as_ref().map(Arc::clone),
            range0_barrier: self.range0_barrier.as_ref().map(Arc::clone),
            clock: Arc::clone(&self.clock),
            foreign_scanner: self.foreign_scanner.as_ref().map(Arc::clone),
            range_scanner: Arc::clone(&self.range_scanner),
            join_stats: Arc::clone(&self.join_stats),
            join_strategy_config: self.join_strategy_config,
            timestamp_oracle: Arc::clone(&self.timestamp_oracle),
            timestamp_horizon: self.timestamp_horizon.clone(),
            gc_horizon: Arc::clone(&self.gc_horizon),
            sweep_committer: Arc::clone(&self.sweep_committer),
            vacuum_demand: Arc::clone(&self.vacuum_demand),
            vacuum_progress: Arc::clone(&self.vacuum_progress),
        }
    }

    /// Return a new handle that uses `timestamp_oracle` for sharded timestamp DML.
    #[must_use]
    pub fn with_timestamp_oracle(
        mut self,
        timestamp_oracle: Arc<dyn timestamp_txn::TimestampOracle>,
    ) -> Self {
        self.timestamp_oracle = timestamp_oracle;
        self
    }

    /// Replace this engine's timestamp oracle for subsequently created sessions.
    pub fn set_timestamp_oracle(
        &mut self,
        timestamp_oracle: Arc<dyn timestamp_txn::TimestampOracle>,
    ) {
        self.timestamp_oracle = timestamp_oracle;
    }

    /// Inject a clock (tests use FixedClock for deterministic now()/current_timestamp).
    pub fn with_clock(mut self, clock: Arc<dyn crate::clock::Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// SP40: register the foreign-table scanner (the `kafka_fdw` seam). The binary
    /// calls this once at startup; every `SqlSession` this engine `connect`s then
    /// shares the same `Arc`. With no scanner registered, a `SELECT` from a foreign
    /// table returns `0A000`.
    pub fn set_foreign_scanner(&mut self, s: Arc<dyn foreign::ForeignScanner>) {
        self.foreign_scanner = Some(s);
    }

    /// Return the foreign scanner shared by subsequently initialized engines.
    #[must_use]
    pub fn foreign_scanner_handle(&self) -> Option<Arc<dyn foreign::ForeignScanner>> {
        self.foreign_scanner.as_ref().map(Arc::clone)
    }

    /// Register the table scanner seam used for ordinary table scans.
    pub fn set_range_scanner(&mut self, scanner: Arc<dyn scanner::RangeScanner>) {
        self.range_scanner = scanner;
    }

    /// Inject statistics used by distributed SQL join planning.
    pub fn set_join_stats(&mut self, stats: Arc<dyn plan_dist::Stats>) {
        self.join_stats = stats;
    }

    /// Return the live statistics source used by subsequently created sessions.
    #[must_use]
    pub fn join_stats(&self) -> Arc<dyn plan_dist::Stats> {
        Arc::clone(&self.join_stats)
    }

    /// Configure distributed SQL join strategy thresholds.
    pub fn set_join_strategy_config(&mut self, config: plan_dist::PlannerConfig) {
        self.join_strategy_config = config;
    }

    /// Return the range scanner shared by subsequently initialized local engines.
    #[must_use]
    pub fn range_scanner_handle(&self) -> Arc<dyn scanner::RangeScanner> {
        Arc::clone(&self.range_scanner)
    }

    /// Return the timestamp oracle shared by subsequently initialized local engines.
    #[must_use]
    pub fn timestamp_oracle_handle(&self) -> Arc<dyn timestamp_txn::TimestampOracle> {
        Arc::clone(&self.timestamp_oracle)
    }

    /// Scan only this engine's local MVCC store for a table interval. Range-aware
    /// scanners use this to make the owning range evaluate visibility against its
    /// own local clog while sharing the caller's global snapshot.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn scan_local_visible(
        &self,
        table: &crabka_pgcatalog::Table,
        global_snapshot: &crabka_pgmvcc::visibility::Snapshot,
        snapshot: &crabka_pgmvcc::visibility::Snapshot,
        own_xid: Option<u64>,
        read_ts: Option<timestamp_txn::ReadTimestamp>,
        interval: scanner::RowInterval,
    ) -> Result<Vec<scanner::ScannedRow>, ExecError> {
        self.scan_local_visible_with_timestamp_owner(
            table,
            global_snapshot,
            snapshot,
            own_xid,
            TimestampScanOwner {
                read_ts,
                own_start_ts: None,
            },
            interval,
        )
    }

    /// Scan local visibility while exposing pending intents owned by `own_start_ts`.
    ///
    /// # Errors
    ///
    /// Returns an execution error when timestamp metadata is missing or the
    /// underlying visibility scan fails.
    pub fn scan_local_visible_with_timestamp_owner(
        &self,
        table: &crabka_pgcatalog::Table,
        global_snapshot: &crabka_pgmvcc::visibility::Snapshot,
        snapshot: &crabka_pgmvcc::visibility::Snapshot,
        own_xid: Option<u64>,
        timestamp_owner: TimestampScanOwner,
        interval: scanner::RowInterval,
    ) -> Result<Vec<scanner::ScannedRow>, ExecError> {
        let TimestampScanOwner {
            read_ts,
            own_start_ts,
        } = timestamp_owner;
        if table.sharded {
            let read_ts = read_ts.ok_or_else(|| {
                ExecError::Unsupported(
                    "sharded scans require a finite statement read timestamp".into(),
                )
            })?;
            return crate::exec::scan_ts_live_interval(
                self.kv.as_ref(),
                self.catalog_kv.as_ref(),
                table,
                read_ts,
                own_start_ts,
                interval,
            );
        }
        crate::exec::scan_live_interval(
            self.kv.as_ref(),
            self.catalog_kv.as_ref(),
            global_snapshot,
            snapshot,
            own_xid,
            table,
            interval,
        )
    }

    /// Snapshot the exclusive terminal rowid for a local table cursor.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn scan_local_terminal(&self, table: &crabka_pgcatalog::Table) -> Result<u64, ExecError> {
        let sequence = crate::exec::read_seq_kv(self.kv.as_ref(), table.id)?;
        let physical_max =
            crate::exec::scan_table_interval(self.kv.as_ref(), table.id, RowInterval::ALL)?
                .into_iter()
                .try_fold(None, |maximum, (key, _)| {
                    let prefix = crabka_pgmvcc::version::row_prefix_of(&key)?;
                    let rowid = if matches!(
                        table.sharding,
                        Some(crabka_pgcatalog::ShardingStrategy::Hash(_))
                    ) {
                        crabka_pgkv::key::bucket_rowid_of(table.id, prefix)?.1
                    } else {
                        crabka_pgkv::key::rowid_of(table.id, prefix)?
                    };
                    Ok::<_, ExecError>(Some(
                        maximum.map_or(rowid, |current: u64| current.max(rowid)),
                    ))
                })?;
        exclusive_cursor_terminal(sequence, physical_max)
    }

    /// Return whether a catalog table uses global visibility semantics.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn table_uses_global_visibility(&self, name: &str) -> Result<bool, ExecError> {
        let table = crabka_pgcatalog::get_table(self.catalog_kv.as_ref(), name)?;
        Ok(crate::exec::table_uses_global_visibility(&table))
    }

    /// Return a catalog table's optional sharding strategy.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn table_sharding(
        &self,
        name: &str,
    ) -> Result<Option<crabka_pgcatalog::ShardingStrategy>, ExecError> {
        let table = crabka_pgcatalog::get_table(self.catalog_kv.as_ref(), name)?;
        Ok(table.sharding)
    }

    /// Atomically flip an ordinary table to timestamp-sharded catalog metadata.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn convert_table_to_sharded_metadata(
        &self,
        name: &str,
        sharding: Option<&crabka_pgcatalog::ShardingStrategy>,
    ) -> Result<(), ExecError> {
        let _xid_writer_fence = self.writer_fence.conversion().await;
        let _writer_fence = Arc::clone(&self.table_write_gate).write_owned().await;
        let _catalog_lock = self.catalog_lock.lock().await;
        let table = crabka_pgcatalog::get_table(self.catalog_kv.as_ref(), name)?;
        let rewrite_ops =
            timestamp_conversion_ops(self.kv.as_ref(), self.catalog_kv.as_ref(), &table)?;
        let ops = crabka_pgcatalog::complete_table_conversion_ops(
            self.catalog_kv.as_ref(),
            name,
            sharding,
            rewrite_ops,
        )?;
        self.committer.commit(ops).await
    }

    /// Return catalog metadata for a table.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn catalog_table(&self, name: &str) -> Result<crabka_pgcatalog::Table, ExecError> {
        crabka_pgcatalog::get_table(self.catalog_kv.as_ref(), name).map_err(Into::into)
    }

    /// Validate the physical bucket identity carried by a timestamp operation.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn validate_timestamp_bucket(
        &self,
        table_id: u32,
        bucket: Option<u32>,
    ) -> Result<(), ExecError> {
        let Some(table) = crabka_pgcatalog::list_tables(self.catalog_kv.as_ref())?
            .into_iter()
            .find(|table| table.id == table_id)
        else {
            return Ok(());
        };
        match (&table.sharding, bucket) {
            (Some(crabka_pgcatalog::ShardingStrategy::Hash(spec)), Some(bucket))
                if bucket < spec.buckets =>
            {
                Ok(())
            }
            (Some(crabka_pgcatalog::ShardingStrategy::Hash(_)), None) => Err(
                ExecError::Unsupported("hash timestamp operation is missing its bucket".into()),
            ),
            (Some(crabka_pgcatalog::ShardingStrategy::Hash(spec)), Some(bucket)) => {
                Err(ExecError::Unsupported(format!(
                    "hash timestamp bucket {bucket} is outside 0..{}",
                    spec.buckets
                )))
            }
            (_, Some(_)) => Err(ExecError::Unsupported(
                "non-hash timestamp operation carries a bucket".into(),
            )),
            (_, None) => Ok(()),
        }
    }

    /// Resolve a timestamp transaction's primary decision from this engine's store.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn primary_timestamp_decision(
        &self,
        start_ts: TimestampTransactionId,
    ) -> Result<PrimaryTxnDecision, ExecError> {
        Ok(
            crate::timestamp_txn::read_timestamp_txn_descriptor(self.kv.as_ref(), start_ts)?
                .map_or(PrimaryTxnDecision::Pending, |descriptor| {
                    descriptor.decision
                }),
        )
    }

    /// Build sharded timestamp write operations for one autocommit DML statement.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn plan_timestamp_write_sql(
        &self,
        sql: &str,
    ) -> Result<crate::exec::TimestampWritePlan, ExecError> {
        let statements = crabka_pgparser::parse(sql)?;
        let [statement] = statements.as_slice() else {
            return Err(ExecError::Unsupported(
                "timestamp scatter requires exactly one DML statement".into(),
            ));
        };
        let now = self.clock.now();
        let ctx = crate::clock::EvalCtx {
            now,
            stmt_now: now,
            time_zone: jiff::tz::TimeZone::UTC,
            current_user: "public".into(),
            session_user: "public".into(),
            clock: Arc::clone(&self.clock),
            sequence: Some(Arc::new(crate::clock::SequenceRuntime {
                kv: Arc::clone(&self.catalog_kv),
                manager: Arc::clone(&self.seq),
                currvals: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            })),
        };
        crate::exec::execute_timestamp_write(
            self.catalog_kv.as_ref(),
            self.kv.as_ref(),
            self.seq.as_ref(),
            statement,
            &ctx,
        )
    }

    /// Build a timestamp write plan using TSO-leased hidden row IDs.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn plan_timestamp_write_sql_with_rowids(
        &self,
        sql: &str,
        hidden_rowids: &[u64],
    ) -> Result<crate::exec::TimestampWritePlan, ExecError> {
        let mut plan = self.plan_timestamp_write_sql(sql)?;
        if plan.writes.len() != hidden_rowids.len() {
            return Err(ExecError::Unsupported(
                "hidden row-id lease does not match timestamp write count".into(),
            ));
        }
        for (write, rowid) in plan.writes.iter_mut().zip(hidden_rowids) {
            write.rowid = *rowid;
        }
        plan.commit_ops.clear();
        Ok(plan)
    }

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn allocate_timestamp_write_lease(
        &self,
        hidden_rowid_count: usize,
    ) -> Result<crate::timestamp_txn::TimestampWriteLease, ExecError> {
        self.timestamp_oracle
            .allocate_write_lease(hidden_rowid_count)
            .await
            .map_err(|error| ExecError::Unsupported(error.to_string()))
    }

    /// Return a timestamp transaction participant for this engine's local range.
    #[must_use]
    pub fn timestamp_txn_participant(&self, range_id: u32) -> TimestampTxnParticipant {
        TimestampTxnParticipant::new(
            Arc::clone(&self.kv),
            Arc::clone(&self.catalog_kv),
            Arc::clone(&self.committer),
            range_id,
        )
        .with_primary_barrier(self.range0_barrier.as_ref().map(Arc::clone))
        .with_sequence_manager(Arc::clone(&self.seq))
    }

    /// Allocate a timestamp transaction id from this engine's configured oracle.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn allocate_timestamp_transaction_id(
        &self,
    ) -> Result<TimestampTransactionId, ExecError> {
        self.timestamp_oracle
            .allocate_transaction_id_after(self.timestamp_horizon.current()?)
            .await
            .map_err(|error| ExecError::Unsupported(error.to_string()))
    }

    /// Allocate the read point shared by every scan in one SQL statement.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn allocate_timestamp_read_timestamp(&self) -> Result<ReadTimestamp, ExecError> {
        self.timestamp_oracle
            .allocate_read_timestamp_after(self.timestamp_horizon.current()?)
            .await
            .map_err(|error| ExecError::Unsupported(error.to_string()))
    }

    /// Allocate a commit timestamp after the supplied transaction id.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn allocate_commit_timestamp_after(
        &self,
        start_ts: TimestampTransactionId,
    ) -> Result<CommitTimestamp, ExecError> {
        self.timestamp_oracle
            .allocate_commit_after_durable(start_ts, self.timestamp_horizon.current()?)
            .await
            .map_err(|error| ExecError::Unsupported(error.to_string()))
    }

    /// Persist a range-0 timestamp transaction descriptor before participant
    /// prewrite begins.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn begin_timestamp_transaction(
        &self,
        descriptor: &TimestampTxnDescriptor,
    ) -> Result<(), ExecError> {
        if descriptor.generation != 0
            || descriptor.decision != PrimaryTxnDecision::Pending
            || !descriptor.prepared.is_empty()
            || !descriptor.operations.is_empty()
        {
            return Err(ExecError::Unsupported(
                "timestamp transaction descriptor must begin without prepared operations".into(),
            ));
        }
        if let Some(existing) =
            timestamp_txn::read_timestamp_txn_descriptor(self.kv.as_ref(), descriptor.start_ts)?
        {
            if existing == *descriptor {
                return Ok(());
            }
            return Err(ExecError::Unsupported(
                "timestamp transaction descriptor already exists with different contents".into(),
            ));
        }
        self.committer
            .commit(vec![timestamp_txn::timestamp_txn_descriptor_cas_op(
                descriptor, None,
            )])
            .await?;
        let stored =
            timestamp_txn::read_timestamp_txn_descriptor(self.kv.as_ref(), descriptor.start_ts)?;
        if stored.as_ref() == Some(descriptor) {
            return Ok(());
        }
        Err(ExecError::Unsupported(
            "timestamp transaction descriptor create was fenced".into(),
        ))
    }

    /// Persist one participant's durable prewrite acknowledgement and its physical
    /// row operations so a committed descriptor can be replayed after a restart.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn acknowledge_timestamp_participant_operations(
        &self,
        start_ts: TimestampTransactionId,
        range_id: u32,
        operations: &[TimestampTxnOperation],
    ) -> Result<TimestampTxnDescriptor, ExecError> {
        loop {
            let Some(current) =
                timestamp_txn::read_timestamp_txn_descriptor(self.kv.as_ref(), start_ts)?
            else {
                return Err(ExecError::Unsupported(
                    "timestamp transaction descriptor is missing".into(),
                ));
            };
            let mut acknowledged = current.clone();
            acknowledged
                .acknowledge_operations(range_id, operations)
                .map_err(|error| ExecError::Unsupported(error.to_string()))?;
            if acknowledged == current {
                return Ok(current);
            }
            self.committer
                .commit(vec![timestamp_txn::timestamp_txn_descriptor_cas_op(
                    &acknowledged,
                    Some(&current),
                )])
                .await?;
            let stored = timestamp_txn::read_timestamp_txn_descriptor(self.kv.as_ref(), start_ts)?
                .ok_or_else(|| {
                    ExecError::Unsupported("timestamp transaction descriptor disappeared".into())
                })?;
            if stored == acknowledged {
                return Ok(stored);
            }
        }
    }

    /// Fence primary mutations against the complete immutable transaction identity.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn validate_timestamp_primary_identity(
        &self,
        identity: TimestampTxnIdentity,
    ) -> Result<TimestampTxnDescriptor, ExecError> {
        let descriptor =
            timestamp_txn::read_timestamp_txn_descriptor(self.kv.as_ref(), identity.start_ts)?
                .ok_or_else(|| {
                    ExecError::Unsupported("timestamp primary identity is fenced".into())
                })?;
        if descriptor.global_xid != identity.global_xid {
            return Err(ExecError::Unsupported(
                "timestamp primary identity is fenced".into(),
            ));
        }
        Ok(descriptor)
    }

    /// Durably expand a pending descriptor's participant set with CAS fencing.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn add_timestamp_transaction_participant(
        &self,
        start_ts: TimestampTransactionId,
        range_id: u32,
    ) -> Result<TimestampTxnDescriptor, ExecError> {
        loop {
            let Some(current) =
                timestamp_txn::read_timestamp_txn_descriptor(self.kv.as_ref(), start_ts)?
            else {
                return Err(ExecError::Unsupported(
                    "timestamp transaction descriptor is missing".into(),
                ));
            };
            let mut expanded = current.clone();
            expanded
                .add_participant(range_id)
                .map_err(|error| ExecError::Unsupported(error.to_string()))?;
            if expanded == current {
                return Ok(current);
            }
            self.committer
                .commit(vec![timestamp_txn::timestamp_txn_descriptor_cas_op(
                    &expanded,
                    Some(&current),
                )])
                .await?;
            let stored = timestamp_txn::read_timestamp_txn_descriptor(self.kv.as_ref(), start_ts)?
                .ok_or_else(|| {
                    ExecError::Unsupported("timestamp transaction descriptor disappeared".into())
                })?;
            if stored == expanded {
                return Ok(stored);
            }
        }
    }

    /// Make range 0's timestamp decision durable. Commit is refused unless all
    /// participant acknowledgements are already durable. The descriptor is the
    /// sole write-once primary record and includes the exact commit timestamp.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn decide_timestamp_transaction(
        &self,
        start_ts: TimestampTransactionId,
        requested: PrimaryTxnDecision,
    ) -> Result<PrimaryTxnDecision, ExecError> {
        loop {
            let Some(current) =
                timestamp_txn::read_timestamp_txn_descriptor(self.kv.as_ref(), start_ts)?
            else {
                return Err(ExecError::Unsupported(
                    "timestamp transaction descriptor is missing".into(),
                ));
            };
            if current.decision != PrimaryTxnDecision::Pending
                || requested == PrimaryTxnDecision::Pending
            {
                return Ok(current.decision);
            }
            if matches!(requested, PrimaryTxnDecision::Committed(_)) && !current.all_prepared() {
                return Err(ExecError::Unsupported("timestamp transaction cannot commit before every participant prewrite is durable".into()));
            }
            let mut decided = current.clone();
            decided
                .decide(requested)
                .map_err(|error| ExecError::Unsupported(error.to_string()))?;
            self.committer
                .commit(vec![timestamp_txn::timestamp_txn_descriptor_cas_op(
                    &decided,
                    Some(&current),
                )])
                .await?;
            let stored = timestamp_txn::read_timestamp_txn_descriptor(self.kv.as_ref(), start_ts)?
                .ok_or_else(|| {
                    ExecError::Unsupported("timestamp transaction descriptor disappeared".into())
                })?;
            if stored.decision != PrimaryTxnDecision::Pending {
                return Ok(stored.decision);
            }
        }
    }

    /// Recover a descriptor that has no terminal range-0 decision by choosing
    /// durable abort.  A delayed coordinator subsequently attempting commit is
    /// fenced by the GTM's write-once global decision.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn recover_timestamp_transaction(
        &self,
        start_ts: TimestampTransactionId,
    ) -> Result<PrimaryTxnDecision, ExecError> {
        self.decide_timestamp_transaction(start_ts, PrimaryTxnDecision::Aborted)
            .await
    }

    /// Enumerate every durable timestamp descriptor in range 0. Recovery uses this
    /// authoritative log rather than an in-memory list of recently coordinated work.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn timestamp_transaction_descriptors(
        &self,
    ) -> Result<Vec<TimestampTxnDescriptor>, ExecError> {
        timestamp_txn::timestamp_txn_descriptors(self.kv.as_ref()).map_err(Into::into)
    }

    /// Read one durable range-control receipt from the range-0 system keyspace.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn range_control_receipt(
        &self,
        tenant: &str,
        receipt: &str,
    ) -> Result<Option<Vec<u8>>, ExecError> {
        self.kv
            .get(&crabka_pgkv::key::range_control_receipt_key(
                tenant, receipt,
            ))
            .map_err(Into::into)
    }

    /// Enumerate this tenant's durable range-control receipts before SQL readiness.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn range_control_receipts(&self, tenant: &str) -> Result<Vec<Vec<u8>>, ExecError> {
        self.kv
            .scan_prefix(&crabka_pgkv::key::range_control_receipt_prefix(tenant))
            .map(|pairs| pairs.into_iter().map(|(_, value)| value).collect())
            .map_err(Into::into)
    }

    /// Compare-and-swap one range-control receipt through this engine's durable committer.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn compare_and_swap_range_control_receipt(
        &self,
        tenant: &str,
        receipt: &str,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<bool, ExecError> {
        let key = crabka_pgkv::key::range_control_receipt_key(tenant, receipt);
        let operation = crabka_pgkv::WriteOp::ConditionalPut {
            key: key.clone(),
            expected: expected.clone(),
            value: value.clone(),
        };
        if let Err(error) = self.committer.commit(vec![operation]).await {
            let actual = self.kv.get(&key)?;
            if actual == Some(value) {
                return Ok(true);
            }
            if actual != expected {
                return Ok(false);
            }
            return Err(error);
        }
        Ok(self.kv.get(&key)? == Some(value))
    }

    /// Read one durable topology-activation receipt from range zero.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn topology_activation_receipt(
        &self,
        tenant: &str,
        operation_id: &str,
    ) -> Result<Option<Vec<u8>>, ExecError> {
        self.kv
            .get(&crabka_pgkv::key::topology_activation_receipt_key(
                tenant,
                operation_id,
            ))
            .map_err(Into::into)
    }

    /// Enumerate topology activations that startup must reconcile before readiness.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn topology_activation_receipts(&self, tenant: &str) -> Result<Vec<Vec<u8>>, ExecError> {
        self.kv
            .scan_prefix(&crabka_pgkv::key::topology_activation_receipt_prefix(
                tenant,
            ))
            .map(|pairs| pairs.into_iter().map(|(_, value)| value).collect())
            .map_err(Into::into)
    }

    /// CAS one topology activation phase through range zero's durable committer.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn compare_and_swap_topology_activation_receipt(
        &self,
        tenant: &str,
        operation_id: &str,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<bool, ExecError> {
        let key = crabka_pgkv::key::topology_activation_receipt_key(tenant, operation_id);
        let operation = crabka_pgkv::WriteOp::ConditionalPut {
            key: key.clone(),
            expected: expected.clone(),
            value: value.clone(),
        };
        if let Err(error) = self.committer.commit(vec![operation]).await {
            let actual = self.kv.get(&key)?;
            if actual == Some(value) {
                return Ok(true);
            }
            if actual != expected {
                return Ok(false);
            }
            return Err(error);
        }
        Ok(self.kv.get(&key)? == Some(value))
    }

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn durable_timestamp_intent_identities(
        &self,
    ) -> Result<Vec<crate::timestamp_txn::DurableTimestampIntentIdentity>, ExecError> {
        crate::timestamp_txn::timestamp_intent_identities(self.kv.as_ref()).map_err(Into::into)
    }

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn timestamp_transaction_operations_are_resolved(
        &self,
        range_id: u32,
        identity: TimestampTxnIdentity,
        decision: PrimaryTxnDecision,
        operations: &[TimestampTxnOperation],
    ) -> Result<bool, ExecError> {
        timestamp_txn::timestamp_operations_are_resolved(
            self.kv.as_ref(),
            range_id,
            identity,
            decision,
            operations,
        )
        .map_err(timestamp_txn::map_ts_error)
    }

    /// Idempotently abort every timestamp intent owned by this range for `start_ts`.
    /// The range-0 descriptor is the logical decision; physical cleanup can be retried.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn abort_timestamp_transaction_intents(
        &self,
        start_ts: TimestampTransactionId,
    ) -> Result<(), ExecError> {
        let ops = timestamp_txn::abort_timestamp_intent_ops(self.kv.as_ref(), start_ts)?;
        if ops.is_empty() {
            return Ok(());
        }
        self.committer.commit(ops).await
    }

    /// Idempotently resolve this range's operations using range 0's terminal
    /// descriptor decision. Global-index intents are discovered from durable local
    /// state, while delete-vs-put semantics come from the descriptor operations.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn resolve_timestamp_transaction_operations(
        &self,
        range_id: u32,
        identity: TimestampTxnIdentity,
        decision: PrimaryTxnDecision,
        operations: &[TimestampTxnOperation],
    ) -> Result<(), ExecError> {
        let decision = match decision {
            PrimaryTxnDecision::Pending => {
                return Err(ExecError::Unsupported(
                    "cannot physically resolve a pending timestamp transaction".into(),
                ));
            }
            PrimaryTxnDecision::Aborted => TimestampTxnDecision::Aborted,
            PrimaryTxnDecision::Committed(commit_ts) => TimestampTxnDecision::Committed(commit_ts),
        };
        let participant = self.timestamp_txn_participant(range_id);
        participant
            .resolve_operations_with_primary(identity, decision, operations)
            .await
    }

    /// Commit statement bookkeeping only after the timestamp primary is durable.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn commit_timestamp_statement_ops(
        &self,
        ops: Vec<crabka_pgkv::WriteOp>,
    ) -> Result<(), ExecError> {
        if ops.is_empty() {
            return Ok(());
        }
        self.committer.commit(ops).await
    }

    /// Advance local scan terminals to include recovered timestamp operations.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn recover_timestamp_scan_terminals(
        &self,
        operations: &[TimestampTxnOperation],
    ) -> Result<(), ExecError> {
        let mut maxima = std::collections::BTreeMap::<u32, u64>::new();
        for operation in operations {
            maxima
                .entry(operation.table_id)
                .and_modify(|max| *max = (*max).max(operation.rowid))
                .or_insert(operation.rowid);
        }
        let mut ops = Vec::new();
        for (table_id, max_rowid) in maxima {
            let key = crabka_pgkv::key::seq_key(table_id);
            let current = self
                .kv
                .get(&key)?
                .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                .map(u64::from_be_bytes)
                .unwrap_or(1);
            let next = max_rowid.checked_add(1).ok_or_else(|| {
                ExecError::Unsupported("row id exhausted during timestamp recovery".into())
            })?;
            if next > current {
                ops.push(crabka_pgkv::WriteOp::Put {
                    key,
                    value: next.to_be_bytes().to_vec(),
                });
            }
        }
        if !ops.is_empty() {
            self.committer.commit(ops).await?;
        }
        Ok(())
    }

    /// Return the catalog KV used by this engine.
    pub fn catalog_kv(&self) -> &dyn Kv {
        self.catalog_kv.as_ref()
    }

    /// Return a shared handle to this engine's local KV store.
    #[must_use]
    pub fn kv_handle(&self) -> Arc<dyn Kv> {
        Arc::clone(&self.kv)
    }

    /// Point this engine's catalog/global-decision view at range 0's store.
    pub fn set_catalog_kv(&mut self, catalog_kv: Arc<dyn Kv>) {
        self.catalog_kv = catalog_kv;
        self.rebuild_timestamp_horizon();
    }

    /// Rebuild the cached horizon source after the store wiring changed. A
    /// range-0 barrier marks the catalog as a replica applied by another
    /// process, whose timestamp descriptors must be rescanned per lookup.
    fn rebuild_timestamp_horizon(&mut self) {
        self.timestamp_horizon = timestamp_txn::TimestampHorizonSource::new(
            Arc::clone(&self.kv),
            Arc::clone(&self.catalog_kv),
            self.range0_barrier.is_some(),
        );
    }

    /// Open a GTM over this engine's `kv` (range 0's store) and make this engine
    /// the GTM coordinator. Called once on range 0's engine by the cluster during
    /// construction, before `share_gtm_to` distributes the same `Arc` to every
    /// other range engine.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn init_gtm_coordinator(&mut self) -> Result<(), ExecError> {
        let g = Arc::new(gtm::Gtm::open(Arc::clone(&self.kv))?);
        self.gtm = Some(g);
        Ok(())
    }

    /// Copy this engine's `Arc<Gtm>` into `other`. Both engines then share the same
    /// GTM — any range can resolve a `Prepared` row and the coordinator can drive
    /// range 0. `self` must have been initialized via `init_gtm_coordinator` first;
    /// `other` can be any range's engine.
    pub fn share_gtm_to(&self, other: &mut SqlEngine) {
        other.gtm = self.gtm.as_ref().map(Arc::clone);
    }

    /// Inject a range-0 read barrier on this (data-range) engine. Called by the
    /// cluster on every range != 0 engine so its cross-range resolver reads a
    /// caught-up range-0 replica. Range 0's own engine needs no barrier.
    pub fn set_range0_barrier(&mut self, b: Arc<dyn crate::read_gate::Linearizer>) {
        self.range0_barrier = Some(b);
        self.rebuild_timestamp_horizon();
    }

    /// Whether this engine carries the shared GTM (so `begin_global_durable` and
    /// global-decision methods are available). `true` on range 0's engine in any
    /// multi-range configuration; `false` on a single-range engine.
    pub fn has_gtm(&self) -> bool {
        self.gtm.is_some()
    }

    /// Allocate a global (cross-range) txn id. Coordinator-only (range 0's engine).
    /// # Panics
    ///
    /// Panics if an internal execution invariant is violated.
    pub fn begin_global(&self) -> u64 {
        self.gtm
            .as_ref()
            .expect("begin_global on a non-GTM engine")
            .begin_global()
    }

    /// Durably allocate a global xid: bump the in-memory counter, then persist
    /// `next_global` through range 0's committer BEFORE returning, so any later
    /// range-0 leader reseeds past `g` and a global xid is never reused across a
    /// range-0 leader change. Only succeeds on range 0's leader (the committer
    /// rejects non-leaders -> ExecError::NotLeader).
    /// # Panics
    ///
    /// Panics if an internal execution invariant is violated.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn begin_global_durable(&self) -> Result<u64, ExecError> {
        let gtm = self
            .gtm
            .as_ref()
            .expect("begin_global_durable on a non-GTM engine");
        let g = gtm.begin_global();
        self.committer
            .commit(vec![gtm.next_global_xid_op()])
            .await?;
        Ok(g)
    }

    /// Durably lease a contiguous block of global xids from range 0. The in-memory
    /// allocator advances past the whole block before `next_global` is persisted,
    /// so a later leader reseed starts after every xid the lease may hand out.
    /// # Panics
    ///
    /// Panics if an internal execution invariant is violated.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn lease_global_xid_block(&self, count: u64) -> Result<GlobalXidLease, ExecError> {
        let gtm = self
            .gtm
            .as_ref()
            .expect("lease_global_xid_block on a non-GTM engine");
        let lease = gtm.lease_global_block(count)?;
        self.committer
            .commit(vec![gtm.next_global_xid_op()])
            .await?;
        Ok(lease)
    }

    /// Lift the GTM's in-memory `next_global` to the durable value (never
    /// regresses). Called on the range-0 leadership rising edge.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn reseed_gtm(&self) -> Result<(), ExecError> {
        if let Some(gtm) = self.gtm.as_ref() {
            gtm.reseed_from_applied()?;
        }
        Ok(())
    }

    /// Durably record the global decision (Committed/Aborted) for `g` in range 0's
    /// group, folding the global next-id advance. The atomic commit instant.
    /// # Panics
    ///
    /// Panics if an internal execution invariant is violated.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn commit_global_decision(
        &self,
        g: u64,
        status: crabka_pgmvcc::clog::XidStatus,
    ) -> Result<crabka_pgmvcc::clog::XidStatus, ExecError> {
        let gtm = self
            .gtm
            .as_ref()
            .expect("commit_global_decision on a non-GTM engine");
        self.committer
            .commit(vec![
                crabka_pgmvcc::clog::put_op(g, status),
                gtm.next_global_xid_op(),
            ])
            .await?;
        // Write-once: apply keeps any prior terminal decision, so the EFFECTIVE
        // decision (what is actually recorded) may differ from `status` if a
        // participant won an abort-race. `commit` guarantees applied-on-leader, and
        // `self.kv` is range 0's applied store, so this read-back is authoritative.
        Ok(crabka_pgmvcc::clog::get(self.kv.as_ref(), g)?)
    }

    /// Scan THIS range's clog from `scan_lo` for in-doubt `Prepared(Li -> g)` markers.
    /// Returns `(in_doubt_gs, new_scan_lo)` where `new_scan_lo` is the smallest scanned
    /// `Li` whose `g` is NOT durably terminal (so it must keep being swept), or one past
    /// the largest scanned `Li` if every scanned marker is terminal (or `scan_lo` if the
    /// range is empty). `new_scan_lo` NEVER passes a non-terminal `g` — the recovery
    /// (zombie-commit) safety invariant. Markers are never deleted.
    ///
    /// The decidedness check reads `self.catalog_kv` directly (NOT through the range-0
    /// read barrier), so on a lagging local range-0 replica an already-decided `g` may
    /// be reported in-doubt. That is harmless: the recovery sweep merely abort-races
    /// `g` to range 0, and the decision is WRITE-ONCE — racing an already-terminal `g`
    /// is a no-op against the real decision. Do not "fix" this by routing through the
    /// barrier; the staleness is intentional and adds no latency to the hot path.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn in_doubt_globals_from(&self, scan_lo: u64) -> Result<(Vec<u64>, u64), ExecError> {
        use std::collections::BTreeSet;
        let mut gs: BTreeSet<u64> = BTreeSet::new();
        let mut first_undecided: Option<u64> = None;
        let mut max_li: Option<u64> = None;
        for (k, v) in self.kv.scan_range(
            &crabka_pgkv::key::clog_key(scan_lo),
            &crabka_pgkv::key::clog_key(crabka_pgmvcc::xid::GLOBAL_XID_BASE),
        )? {
            let Some(li) = crabka_pgkv::key::clog_xid_of(&k) else {
                continue;
            };
            max_li = Some(li);
            if let crabka_pgmvcc::clog::XidStatus::Prepared(g) = crabka_pgmvcc::clog::decode(&v)? {
                let terminal = matches!(
                    crabka_pgmvcc::clog::get(self.catalog_kv.as_ref(), g)?,
                    crabka_pgmvcc::clog::XidStatus::Committed
                        | crabka_pgmvcc::clog::XidStatus::Aborted
                );
                if !terminal {
                    gs.insert(g);
                    first_undecided.get_or_insert(li);
                }
            }
        }
        let new_scan_lo = first_undecided
            // `max_li` is a local `Li < GLOBAL_XID_BASE` on a real data range, so this
            // never saturates; `saturating_add` is belt-and-suspenders.
            .or_else(|| max_li.map(|m| m.saturating_add(1)))
            .unwrap_or(scan_lo)
            .max(scan_lo); // monotone
        Ok((gs.into_iter().collect(), new_scan_lo))
    }

    /// Back-compat: the full-scan in-doubt set (callers that don't track a watermark).
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn in_doubt_globals(&self) -> Result<Vec<u64>, ExecError> {
        Ok(self.in_doubt_globals_from(0).await?.0)
    }

    /// List every local prepared participant marker, including markers whose
    /// range-0 decision is already terminal. Recovery uses this to release
    /// abandoned live owner sessions after a coordinator restart.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn prepared_globals(&self) -> Result<Vec<u64>, ExecError> {
        let mut globals = std::collections::BTreeSet::new();
        for (key, value) in self.kv.scan_range(
            &crabka_pgkv::key::clog_key(0),
            &crabka_pgkv::key::clog_key(crabka_pgmvcc::xid::GLOBAL_XID_BASE),
        )? {
            if crabka_pgkv::key::clog_xid_of(&key).is_some()
                && let crabka_pgmvcc::clog::XidStatus::Prepared(global_xid) =
                    crabka_pgmvcc::clog::decode(&value)?
            {
                globals.insert(global_xid);
            }
        }
        Ok(globals.into_iter().collect())
    }

    /// SP24 abort-atomicity ROOT FIX — re-acquire the in-memory row locks for every
    /// inherited in-doubt participant version on this range, returning the in-doubt
    /// local xids `Li` that now hold those locks. Call on the leadership-rising edge,
    /// AFTER the apply-wait (so every inherited `Prepared(Li -> g)` marker is visible)
    /// and BEFORE the recovery gate opens (`mark_served`) — the settle-before-serve-for-
    /// LOCKS step.
    ///
    /// Why locks, not just the per-session `effective_global_xid` fence: row locks live
    /// ONLY in the in-memory `RowLockManager`, which is WIPED when this range's leader is
    /// killed and a new one rises — yet the in-doubt `Prepared(Li -> g)` row version it
    /// left behind is DURABLE + replicated. So a cross-range row can carry an unresolved
    /// in-doubt marker with NO live lock holder; a concurrent re-staging writer whose
    /// apply-lagged read misses the inherited version then writes a COMPETING version
    /// under a different global decision → two live versions on commit (money created).
    /// A per-statement fence cannot serialize N concurrent writers across apply lag; a
    /// re-acquired exclusive lock does — the next writer BLOCKS until the inherited row
    /// resolves, giving exactly one live version.
    ///
    /// Scans the clog from the recovery watermark for in-doubt `Prepared(Li -> g)`
    /// markers (mirrors `in_doubt_globals_from`'s decidedness rule), then scans this
    /// range's primary-index version keyspace once and, for every version whose `xmin`
    /// is an in-doubt `Li`, re-acquires `(table, rowid)` EXCLUSIVELY under `Li`. Returns
    /// the `(Li, g)` pairs so the rise sweep can release each `Li`'s lock the moment its
    /// `g` is driven terminal (the abort-race), so the lock is NEVER freed while its `g`
    /// is still in-doubt. Idempotent (a re-scan re-acquires the same locks).
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn reacquire_in_doubt_locks(&self) -> Result<Vec<(u64, u64)>, ExecError> {
        use std::collections::BTreeMap;
        // 1) In-doubt `(Li -> g)` markers on this range (those whose `g` is not terminal).
        let scan_lo = self.clog_scan_lo()?;
        let mut in_doubt: BTreeMap<u64, u64> = BTreeMap::new();
        for (k, v) in self.kv.scan_range(
            &crabka_pgkv::key::clog_key(scan_lo),
            &crabka_pgkv::key::clog_key(crabka_pgmvcc::xid::GLOBAL_XID_BASE),
        )? {
            let Some(li) = crabka_pgkv::key::clog_xid_of(&k) else {
                continue;
            };
            if let crabka_pgmvcc::clog::XidStatus::Prepared(g) = crabka_pgmvcc::clog::decode(&v)? {
                let terminal = matches!(
                    crabka_pgmvcc::clog::get(self.catalog_kv.as_ref(), g)?,
                    crabka_pgmvcc::clog::XidStatus::Committed
                        | crabka_pgmvcc::clog::XidStatus::Aborted
                );
                if !terminal {
                    in_doubt.insert(li, g);
                }
            }
        }
        if in_doubt.is_empty() {
            return Ok(Vec::new());
        }
        // 2) Scan this range's primary-index version keyspace once; for every version
        //    whose creating xid (xmin, encoded in the version key's 8-byte suffix) is an
        //    in-doubt `Li`, re-acquire `(table, rowid)` exclusively under `Li`. User
        //    tables start at id 1 (`SYSTEM_TABLE_ID == 0`), so scan from `table_prefix(1)`
        //    onward; `table_rowid_of` filters non-primary/system keys.
        let start = crabka_pgkv::key::table_prefix(crabka_pgkv::key::SYSTEM_TABLE_ID + 1);
        // Upper bound above every primary-index version key. A version key is
        // `put_u32(table) ++ put_u32(INDEX_PRIMARY=1) ++ put_u64(rowid) ++ put_u64(xid)`;
        // its 5th byte is the high byte of `INDEX_PRIMARY`, i.e. `0x00`, so any key
        // whose 5th byte is `0xFF` (e.g. five `0xFF` bytes) sorts strictly after every
        // real version key regardless of table id.
        let end = [0xFFu8; 5];
        for (k, _v) in self.kv.scan_range(&start, &end)? {
            let Some((table, rowid)) = crabka_pgkv::key::table_rowid_of(&k) else {
                continue;
            };
            let Ok(li) = crabka_pgmvcc::version::xid_of_key(&k) else {
                continue;
            };
            if in_doubt.contains_key(&li) {
                self.lockmgr.reacquire_exclusive(table, rowid, li);
            }
        }
        Ok(in_doubt.into_iter().collect())
    }

    /// Release the in-doubt row locks re-acquired under `li` (frees every `(table, rowid)`
    /// lock that local xid holds). Called by the rise sweep the moment `li`'s global `g`
    /// has been driven TERMINAL by the abort-race, so the lock is never freed while its
    /// `g` is in-doubt. A re-staging writer that blocked on the lock wakes to a fully-
    /// RESOLVED row: its `effective_global_xid` fence sees the terminal decision and
    /// proceeds correctly (exactly one live version). A no-op for an `li` that holds no
    /// lock.
    pub fn release_in_doubt_lock(&self, li: u64) {
        self.lockmgr.release_all(li);
    }

    /// Scan THIS range's clog (from the recovery watermark) for an existing durable
    /// `Prepared(Li -> g)` marker for the given in-doubt global xid `g`; return the local
    /// xid `Li` of the first such marker, or `None`.
    ///
    /// Makes participant `Stage` IDEMPOTENT per `(g, range)`. A `Stage(g)` RPC retried across
    /// a participant-leader failover (the original leader durably staged then died; the retry
    /// lands on the new leader, whose in-memory held-session map is empty) must NOT write a
    /// SECOND `Prepared(-> g)` version of the row. The first attempt's marker was
    /// Raft-committed before the old leader died, so the new leader — which won election with
    /// that entry in its log — finds it here and the retry becomes a no-op. Bounded by the
    /// watermark: an in-doubt `g`'s marker is never below `clog_scan_lo` (the watermark never
    /// advances past a non-terminal `g`).
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn staged_local_for(&self, g: u64) -> Result<Option<u64>, ExecError> {
        let scan_lo = self.clog_scan_lo()?;
        for (k, v) in self.kv.scan_range(
            &crabka_pgkv::key::clog_key(scan_lo),
            &crabka_pgkv::key::clog_key(crabka_pgmvcc::xid::GLOBAL_XID_BASE),
        )? {
            let Some(li) = crabka_pgkv::key::clog_xid_of(&k) else {
                continue;
            };
            if let crabka_pgmvcc::clog::XidStatus::Prepared(pg) = crabka_pgmvcc::clog::decode(&v)?
                && pg == g
            {
                return Ok(Some(li));
            }
        }
        Ok(None)
    }

    /// Read this range's durable recovery-scan watermark (`0` if absent/unset).
    /// # Panics
    ///
    /// Panics if an internal execution invariant is violated.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn clog_scan_lo(&self) -> Result<u64, ExecError> {
        match self.kv.get(&crabka_pgkv::key::clog_scan_lo_key())? {
            Some(b) if b.len() == 8 => Ok(u64::from_be_bytes(b[..8].try_into().expect("8 bytes"))),
            _ => Ok(0),
        }
    }

    /// Durably advance this range's recovery-scan watermark (monotone; a no-op if `lo`
    /// is not greater than the current value). Proposed through the range committer.
    ///
    /// The read-then-write is NOT a CAS: monotonicity relies on the single-writer
    /// discipline of the edge-triggered per-range leadership-rise sweep (one advance at a
    /// time). Even a hypothetical interleaving that regressed the value low is
    /// correctness-preserving — a lower watermark only enlarges the next scan, never skips
    /// an in-doubt marker.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn advance_clog_scan_lo(&self, lo: u64) -> Result<(), ExecError> {
        if lo <= self.clog_scan_lo()? {
            return Ok(());
        }
        self.committer
            .commit(vec![crabka_pgkv::store::WriteOp::Put {
                key: crabka_pgkv::key::clog_scan_lo_key(),
                value: lo.to_be_bytes().to_vec(),
            }])
            .await
    }

    /// Deregister a decided global txn from the in-memory running-set.
    /// # Panics
    ///
    /// Panics if an internal execution invariant is violated.
    pub fn finish_global(&self, g: u64) {
        self.gtm
            .as_ref()
            .expect("finish_global on a non-GTM engine")
            .finish_global(g);
    }
}

fn exclusive_cursor_terminal(sequence: u64, physical_max: Option<u64>) -> Result<u64, ExecError> {
    let physical = physical_max
        .map(|rowid| {
            rowid.checked_add(1).ok_or_else(|| {
                ExecError::Unsupported("local cursor rowid terminal overflow".into())
            })
        })
        .transpose()?
        .unwrap_or(0);
    Ok(sequence.max(physical))
}

#[cfg(test)]
mod cursor_terminal_tests {
    use crabka_pgkv::WriteOp;
    use crabka_pgwire::engine::{Engine, Session};

    use super::{SqlEngine, exclusive_cursor_terminal};

    #[test]
    fn combines_structural_and_physical_cursor_horizons() {
        assert_eq!(exclusive_cursor_terminal(9, None).unwrap(), 9);
        assert_eq!(exclusive_cursor_terminal(0, Some(7)).unwrap(), 8);
        assert_eq!(exclusive_cursor_terminal(20, Some(7)).unwrap(), 20);
        assert_eq!(exclusive_cursor_terminal(3, Some(7)).unwrap(), 8);
        assert_eq!(exclusive_cursor_terminal(0, None).unwrap(), 0);
        assert!(exclusive_cursor_terminal(0, Some(u64::MAX)).is_err());
    }

    #[tokio::test]
    async fn hash_cursor_terminal_uses_logical_rowid_not_sparse_bucket_prefix() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE h (id int4) SHARDED BY HASH (id) BUCKETS 16")
            .await
            .expect("create hash table");
        let table = engine.catalog_table("h").expect("hash table");
        engine
            .kv_handle()
            .write_batch(&[WriteOp::Put {
                key: crabka_pgmvcc::version::hash_version_key_ts(table.id, 15, 7, 1),
                value: vec![0],
            }])
            .expect("seed high-bucket physical key");

        assert_eq!(engine.scan_local_terminal(&table).unwrap(), 8);
    }
}

pub(crate) fn checkpoint_garbage_horizon(
    procarray: &ProcArray,
    kv: &dyn Kv,
    gc_horizon: &crabka_pgmvcc::gc::GcHorizon,
) -> Result<u64, ExecError> {
    use crabka_pgmvcc::{clog::XidStatus, xid::FIRST_NORMAL_XID};

    // The horizon cap: no higher than the oldest running writer xid AND the
    // lowest registered snapshot pin. Writers register in the ProcArray;
    // read-only snapshots (REPEATABLE READ transactions, per-statement
    // snapshots) register pins in the GcHorizon — without the pin cap a
    // version whose committed deleter was still running when such a snapshot
    // was taken could be pruned out from under it.
    let active_xmin = procarray.snapshot().xmin;
    let cap = gc_horizon
        .min_pinned()
        .map_or(active_xmin, |pinned| pinned.min(active_xmin));
    // Scan the clog from the durable recovery watermark, tightened by the
    // in-process decided floor (everything below a previously returned horizon
    // is already decided — decidedness is immutable), so the walk is amortized
    // O(1) per xid instead of O(all xids) per call. Absent clog entries below
    // `cap` never appear in the scan: an absent xid below the active xmin is
    // not running, so it is a crash leftover that can never commit
    // (aborted-equivalent) — exactly the existing recovery semantics.
    let scan_lo = match kv.get(&crabka_pgkv::key::clog_scan_lo_key())? {
        Some(bytes) if bytes.len() == 8 => {
            u64::from_be_bytes(bytes[..8].try_into().expect("checked length"))
        }
        _ => FIRST_NORMAL_XID,
    }
    .max(FIRST_NORMAL_XID)
    .max(gc_horizon.decided_floor())
    .min(cap);
    for (key, value) in kv.scan_range(
        &crabka_pgkv::key::clog_key(scan_lo),
        &crabka_pgkv::key::clog_key(cap),
    )? {
        let Some(xid) = crabka_pgkv::key::clog_xid_of(&key) else {
            continue;
        };
        if matches!(
            crabka_pgmvcc::clog::decode(&value)?,
            XidStatus::InProgress | XidStatus::Prepared(_)
        ) {
            let horizon = xid.min(cap);
            gc_horizon.advance_decided_floor(horizon);
            return Ok(horizon);
        }
    }
    gc_horizon.advance_decided_floor(cap);
    Ok(cap)
}

/// A sentinel global snapshot for single-range (non-GTM) engines. Any global xid
/// `g >= xmax` is treated as InProgress by the resolver, but no `Prepared` tuples
/// ever exist on a single-range engine so the Prepared branch is unreachable.
#[allow(non_snake_case)]
pub(crate) fn NO_GLOBAL_SNAPSHOT() -> crabka_pgmvcc::visibility::Snapshot {
    use crabka_pgmvcc::xid::GLOBAL_XID_BASE;
    crabka_pgmvcc::visibility::Snapshot {
        xmin: GLOBAL_XID_BASE,
        xmax: GLOBAL_XID_BASE,
        xip: vec![],
    }
}

/// Replace every xid-MVCC tuple for `table` with timestamp tuples and return the
/// rewrite in the same batch as the catalog transition.  Keeping stale invisible
/// xid tuples would make a sharded scan fail to decode them, so the rewrite first
/// deletes the complete old version set and then installs only the rows visible at
/// the conversion point.
fn timestamp_conversion_ops(
    kv: &dyn Kv,
    catalog_kv: &dyn Kv,
    table: &crabka_pgcatalog::Table,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    if table.sharded {
        return Ok(Vec::new());
    }
    let horizon = timestamp_txn::durable_timestamp_horizon_with_catalog(kv, catalog_kv)?;
    let commit_ts = horizon.checked_add(2).ok_or_else(|| {
        ExecError::Unsupported("timestamp conversion exhausted timestamp space".into())
    })?;
    let start_ts = commit_ts - 1;
    let snapshot = crabka_pgmvcc::visibility::Snapshot {
        xmin: 1,
        xmax: u64::MAX,
        xip: Vec::new(),
    };
    let visible = crate::exec::scan_live(kv, catalog_kv, &snapshot, &snapshot, None, table)?;
    let old_versions = kv.scan_prefix(&crabka_pgkv::key::table_prefix(table.id))?;
    let mut ops = Vec::with_capacity(old_versions.len() + visible.len());
    ops.extend(
        old_versions
            .into_iter()
            .map(|(key, _)| crabka_pgkv::WriteOp::Delete { key }),
    );
    ops.extend(
        visible
            .into_iter()
            .map(|(rowid, _, row)| crabka_pgkv::WriteOp::Put {
                key: crabka_pgmvcc::version::version_key_ts(table.id, rowid, start_ts),
                value: crabka_pgmvcc::version::encode_ts_tuple(
                    start_ts,
                    crabka_pgmvcc::version::TsVersionState::Committed { commit_ts },
                    &row,
                ),
            }),
    );
    if ops.is_empty() {
        // Preserve an explicit physical-rewrite proof for an empty table. The
        // key cannot name a real tuple (tuple keys include an index and rowid),
        // so this is a durable no-op in the atomic conversion batch.
        ops.push(crabka_pgkv::WriteOp::Delete {
            key: crabka_pgkv::key::table_prefix(table.id),
        });
    }
    Ok(ops)
}

/// Field descriptions for `sql` resolving schema from `catalog_kv`, without a
/// data store or execution (the gateway's Describe only needs the catalog).
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn describe_fields(
    catalog_kv: &dyn Kv,
    sql: &str,
) -> Result<Vec<crabka_pgwire::engine::FieldDescription>, ExecError> {
    crate::exec::describe(catalog_kv, catalog_kv, sql)
}

impl Engine for SqlEngine {
    type Session = SqlSession;

    fn connect(&self) -> SqlSession {
        SqlSession::new(session::SqlSessionConfig {
            kv: Arc::clone(&self.kv),
            catalog_kv: Arc::clone(&self.catalog_kv),
            procarray: Arc::clone(&self.procarray),
            seq: Arc::clone(&self.seq),
            lockmgr: Arc::clone(&self.lockmgr),
            catalog_lock: Arc::clone(&self.catalog_lock),
            table_write_gate: Arc::clone(&self.table_write_gate),
            writer_fence: Arc::clone(&self.writer_fence),
            coordination: Arc::clone(&self.coordination),
            unique_index_lock: Arc::clone(&self.unique_index_lock),
            committer: Arc::clone(&self.committer),
            linearizer: Arc::clone(&self.linearizer),
            persist_mode: self.persist_mode,
            gtm: self.gtm.as_ref().map(Arc::clone),
            range0_barrier: self.range0_barrier.as_ref().map(Arc::clone),
            clock: Arc::clone(&self.clock),
            foreign_scanner: self.foreign_scanner.as_ref().map(Arc::clone),
            range_scanner: Arc::clone(&self.range_scanner),
            join_stats: Arc::clone(&self.join_stats),
            join_strategy_config: self.join_strategy_config,
            timestamp_oracle: Arc::clone(&self.timestamp_oracle),
            timestamp_horizon: self.timestamp_horizon.clone(),
            gc_horizon: Arc::clone(&self.gc_horizon),
        })
    }
}

#[cfg(test)]
mod tests {
    use crabka_pgwire::engine::Session;
    use tokio::sync::{Barrier, Notify};

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_doubt_globals_lists_undecided_prepared_markers() {
        use crabka_pgmvcc::{
            clog::{XidStatus, put_op},
            xid::GLOBAL_XID_BASE,
        };
        // Single-store in-memory engine: `self.kv == self.catalog_kv` (both are the
        // same `Arc` per `with_kv`), so `MemKv` here is range 0's global clog too.
        let kv = Arc::new(MemKv::new());
        let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
        let g_undecided = GLOBAL_XID_BASE + 1;
        let g_committed = GLOBAL_XID_BASE + 2;
        // Two local participants prepared into two global xids.
        kv.write_batch(&[put_op(11, XidStatus::Prepared(g_undecided))])
            .expect("p1");
        kv.write_batch(&[put_op(12, XidStatus::Prepared(g_committed))])
            .expect("p2");
        // g_committed is decided; g_undecided is not.
        kv.write_batch(&[put_op(g_committed, XidStatus::Committed)])
            .expect("decide");
        let mut got = engine.in_doubt_globals().await.expect("scan");
        got.sort();
        assert_eq!(
            got,
            vec![g_undecided],
            "only undecided Prepared markers are returned"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_table_sharded_persists_catalog_metadata() {
        let kv = Arc::new(MemKv::new());
        let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
        let mut session = engine.connect();

        session
            .simple_query("CREATE TABLE sharded_t (id int4) SHARDED")
            .await
            .expect("create sharded table");
        session
            .simple_query("CREATE TABLE local_t (id int4)")
            .await
            .expect("create local table");

        let sharded = crabka_pgcatalog::get_table(kv.as_ref(), "sharded_t")
            .expect("sharded table catalog row");
        let local =
            crabka_pgcatalog::get_table(kv.as_ref(), "local_t").expect("local table catalog row");

        assert!(sharded.sharded);
        assert!(!local.sharded);
        assert_eq!(sharded.columns.len(), 1);
        assert_eq!(local.columns.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_table_hash_sharded_persists_catalog_metadata() {
        let kv = Arc::new(MemKv::new());
        let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
        let mut session = engine.connect();

        session
            .simple_query("CREATE TABLE hash_t (id int4, value text) SHARDED BY HASH (id) BUCKETS 16 COLOCATED WITH users")
            .await
            .expect("create hash sharded table");

        let table = crabka_pgcatalog::get_table(kv.as_ref(), "hash_t").expect("table");
        assert!(table.sharded);
        assert_eq!(
            table.sharding,
            Some(crabka_pgcatalog::ShardingStrategy::Hash(
                crabka_pgcatalog::HashSharding {
                    columns: vec!["id".into()],
                    buckets: 16,
                    co_location_group: Some("users".into()),
                }
            ))
        );
    }

    #[tokio::test]
    async fn timestamp_bucket_validation_rejects_missing_wrong_shape_and_out_of_range() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query(
                "CREATE TABLE h (id int4) SHARDED BY HASH (id) BUCKETS 16; \
                 CREATE TABLE s (id int4) SHARDED",
            )
            .await
            .expect("create tables");
        let hash = engine.catalog_table("h").expect("hash table");
        let ordinary = engine.catalog_table("s").expect("ordinary table");

        assert!(engine.validate_timestamp_bucket(hash.id, Some(0)).is_ok());
        assert!(engine.validate_timestamp_bucket(hash.id, Some(15)).is_ok());
        assert!(engine.validate_timestamp_bucket(hash.id, None).is_err());
        assert!(engine.validate_timestamp_bucket(hash.id, Some(16)).is_err());
        assert!(
            engine
                .validate_timestamp_bucket(ordinary.id, Some(0))
                .is_err()
        );
        assert!(engine.validate_timestamp_bucket(ordinary.id, None).is_ok());
    }

    #[tokio::test]
    async fn supplied_hash_hidden_rowids_emit_no_sequence_commit_ops() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE h (id int4) SHARDED BY HASH (id) BUCKETS 4")
            .await
            .expect("create hash table");

        let plan = engine
            .plan_timestamp_write_sql_with_rowids("INSERT INTO h VALUES (10), (20)", &[101, 102])
            .expect("timestamp plan");

        assert_eq!(
            plan.writes
                .iter()
                .map(|write| write.rowid)
                .collect::<Vec<_>>(),
            [101, 102]
        );
        assert!(plan.commit_ops.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn conversion_metadata_seam_flips_plain_table_to_hash_sharded() {
        let kv = Arc::new(MemKv::new());
        let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE convert_t (id int4, value text)")
            .await
            .expect("create table");
        let sharding = crabka_pgcatalog::ShardingStrategy::Hash(crabka_pgcatalog::HashSharding {
            columns: vec!["id".into()],
            buckets: 8,
            co_location_group: None,
        });

        engine
            .convert_table_to_sharded_metadata("convert_t", Some(&sharding))
            .await
            .expect("convert metadata");

        let table = crabka_pgcatalog::get_table(kv.as_ref(), "convert_t").expect("table");
        assert!(table.sharded);
        assert_eq!(table.sharding, Some(sharding));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn converted_table_keeps_query_visibility_after_tuple_rewrite() {
        let kv = Arc::new(MemKv::new());
        let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE convert_visible (id int4, value text)")
            .await
            .expect("create table");
        session
            .simple_query("INSERT INTO convert_visible VALUES (1, 'before'), (2, 'after')")
            .await
            .expect("insert rows");
        let before = session
            .simple_query("SELECT id, value FROM convert_visible ORDER BY id")
            .await
            .expect("read before");

        engine
            .convert_table_to_sharded_metadata("convert_visible", None)
            .await
            .expect("metadata conversion");
        let after = session
            .simple_query("SELECT id, value FROM convert_visible ORDER BY id")
            .await
            .expect("read after");

        assert_eq!(format!("{before:?}"), format!("{after:?}"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn conversion_waits_for_an_in_progress_xid_writer_before_rewriting() {
        let kv = Arc::new(MemKv::new());
        let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
        let mut writer = engine.connect();
        writer
            .simple_query("CREATE TABLE conversion_fence (id int4)")
            .await
            .expect("create table");
        writer.simple_query("BEGIN").await.expect("begin writer");
        writer
            .simple_query("INSERT INTO conversion_fence VALUES (1)")
            .await
            .expect("stage xid tuple");

        let converting_engine = engine.clone_handle();
        let conversion_started = Arc::new(Notify::new());
        let release_conversion = Arc::new(Notify::new());
        let conversion = tokio::spawn({
            let conversion_started = Arc::clone(&conversion_started);
            let release_conversion = Arc::clone(&release_conversion);
            async move {
                conversion_started.notify_one();
                release_conversion.notified().await;
                converting_engine
                    .convert_table_to_sharded_metadata("conversion_fence", None)
                    .await
            }
        });
        conversion_started.notified().await;
        let conversion_waiter = engine.writer_fence.conversion_waiter();
        tokio::pin!(conversion_waiter);
        conversion_waiter.as_mut().enable();
        release_conversion.notify_one();
        conversion_waiter.await;
        assert!(
            !conversion.is_finished(),
            "conversion must wait for the writer lease before rewriting"
        );

        writer.simple_query("COMMIT").await.expect("commit writer");
        conversion
            .await
            .expect("conversion task")
            .expect("convert table");

        let table = crabka_pgcatalog::get_table(kv.as_ref(), "conversion_fence").expect("table");
        for (_, value) in kv
            .scan_prefix(&crabka_pgkv::key::table_prefix(table.id))
            .expect("scan converted tuples")
        {
            assert!(
                crabka_pgmvcc::version::decode_ts_tuple(&value).is_ok(),
                "conversion leaves no xid tuple behind"
            );
        }
        let mut reader = engine.connect();
        let rows = reader
            .simple_query("SELECT id FROM conversion_fence")
            .await
            .expect("read converted row");
        assert!(format!("{rows:?}").contains('1'));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn independently_constructed_engines_share_the_conversion_fence() {
        let kv = Arc::new(MemKv::new());
        let converting_engine =
            SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("converting engine");
        let writing_engine =
            SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("writing engine");
        assert!(Arc::ptr_eq(
            &converting_engine.coordination,
            &writing_engine.coordination
        ));
        let mut writer = writing_engine.connect();
        writer
            .simple_query("CREATE TABLE shared_conversion_fence (id int4)")
            .await
            .expect("create table");
        writer.simple_query("BEGIN").await.expect("begin writer");
        writer
            .simple_query("INSERT INTO shared_conversion_fence VALUES (1)")
            .await
            .expect("stage xid tuple");

        let conversion_barrier = Arc::new(Barrier::new(2));
        let conversion_started = Arc::new(Notify::new());
        let conversion = tokio::spawn({
            let conversion_barrier = Arc::clone(&conversion_barrier);
            let conversion_started = Arc::clone(&conversion_started);
            async move {
                conversion_barrier.wait().await;
                conversion_started.notify_one();
                converting_engine
                    .convert_table_to_sharded_metadata("shared_conversion_fence", None)
                    .await
            }
        });
        let conversion_waiter = writing_engine.writer_fence.conversion_waiter();
        tokio::pin!(conversion_waiter);
        conversion_waiter.as_mut().enable();
        conversion_barrier.wait().await;
        conversion_started.notified().await;
        conversion_waiter.await;
        assert!(
            !conversion.is_finished(),
            "conversion must wait for the independently opened writer lease"
        );

        writer.simple_query("COMMIT").await.expect("commit writer");
        conversion
            .await
            .expect("conversion task")
            .expect("convert table");

        let table =
            crabka_pgcatalog::get_table(kv.as_ref(), "shared_conversion_fence").expect("table");
        assert!(table.sharded);
        for (_, value) in kv
            .scan_prefix(&crabka_pgkv::key::table_prefix(table.id))
            .expect("scan converted tuples")
        {
            assert!(crabka_pgmvcc::version::decode_ts_tuple(&value).is_ok());
        }
        let mut reader = writing_engine.connect();
        let rows = reader
            .simple_query("SELECT id FROM shared_conversion_fence")
            .await
            .expect("query committed row after conversion");
        assert!(format!("{rows:?}").contains('1'));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_session_keeps_conversion_coordination_registered_after_engines_drop() {
        let kv = Arc::new(MemKv::new());
        let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
        let mut writer = engine.connect();
        writer
            .simple_query("CREATE TABLE retained_conversion_fence (id int4)")
            .await
            .expect("create table");
        writer.simple_query("BEGIN").await.expect("begin writer");
        writer
            .simple_query("INSERT INTO retained_conversion_fence VALUES (1)")
            .await
            .expect("stage xid tuple");
        drop(engine);

        let reopened = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("reopen engine");
        let writer_fence = Arc::clone(&reopened.writer_fence);
        let conversion_waiter = writer_fence.conversion_waiter();
        tokio::pin!(conversion_waiter);
        conversion_waiter.as_mut().enable();
        let conversion = tokio::spawn(async move {
            reopened
                .convert_table_to_sharded_metadata("retained_conversion_fence", None)
                .await
        });
        conversion_waiter.await;
        assert!(
            !conversion.is_finished(),
            "a reopened engine must wait for the live session's writer lease"
        );

        writer.simple_query("COMMIT").await.expect("commit writer");
        conversion
            .await
            .expect("conversion task")
            .expect("convert table");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transaction_ddl_releases_its_shared_gate_without_unfencing_conversion() {
        let kv = Arc::new(MemKv::new());
        let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
        let mut writer = engine.connect();
        writer
            .simple_query("CREATE TABLE conversion_upgrade (id int4)")
            .await
            .expect("create table");
        writer.simple_query("BEGIN").await.expect("begin writer");
        writer
            .simple_query("INSERT INTO conversion_upgrade VALUES (1)")
            .await
            .expect("stage xid tuple");

        let conversion_barrier = Arc::new(Barrier::new(2));
        let conversion_started = Arc::new(Notify::new());
        let converting_engine = engine.clone_handle();
        let conversion = tokio::spawn({
            let conversion_barrier = Arc::clone(&conversion_barrier);
            let conversion_started = Arc::clone(&conversion_started);
            async move {
                conversion_barrier.wait().await;
                conversion_started.notify_one();
                converting_engine
                    .convert_table_to_sharded_metadata("conversion_upgrade", None)
                    .await
            }
        });
        let conversion_waiter = engine.writer_fence.conversion_waiter();
        tokio::pin!(conversion_waiter);
        conversion_waiter.as_mut().enable();
        conversion_barrier.wait().await;
        conversion_started.notified().await;
        conversion_waiter.await;

        writer
            .simple_query("CREATE TABLE ddl_upgrade_progress (id int4)")
            .await
            .expect("DDL must not deadlock behind conversion");
        writer.simple_query("COMMIT").await.expect("commit writer");
        conversion
            .await
            .expect("conversion task")
            .expect("convert table");

        let mut reader = engine.connect();
        let rows = reader
            .simple_query("SELECT id FROM conversion_upgrade")
            .await
            .expect("query committed row after conversion");
        assert!(format!("{rows:?}").contains('1'));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unsharded_local_scan_is_unchanged_through_scanner_seam() {
        let mut engine = SqlEngine::new();
        engine.set_range_scanner(Arc::new(LocalRangeScanner));
        let mut session = engine.connect();

        session
            .simple_query("CREATE TABLE local_t (id int4)")
            .await
            .expect("create");
        session
            .simple_query("INSERT INTO local_t VALUES (1)")
            .await
            .expect("insert 1");
        session
            .simple_query("INSERT INTO local_t VALUES (2)")
            .await
            .expect("insert 2");

        let table = crabka_pgcatalog::get_table(engine.catalog_kv(), "local_t").expect("table");
        let snapshot = engine.procarray.snapshot();
        let direct_rows = engine
            .scan_local_visible(
                &table,
                &NO_GLOBAL_SNAPSHOT(),
                &snapshot,
                None,
                None,
                RowInterval::ALL,
            )
            .expect("direct scan");
        let via_scanner = engine
            .range_scanner
            .scan(ScanRequest {
                local: engine.kv.as_ref(),
                global: engine.catalog_kv.as_ref(),
                global_snapshot: &NO_GLOBAL_SNAPSHOT(),
                snapshot: &snapshot,
                own_xid: None,
                read_ts: None,
                own_start_ts: None,
                table: &table,
                interval: RowInterval::ALL,
                predicate: PredicatePushdown::FullScan,
                projection: ProjectionPushdown::All,
                partial_aggregate: None,
                top_k: None,
            })
            .expect("scanner scan");

        assert_eq!(via_scanner, direct_rows);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn staged_local_for_finds_an_existing_prepared_marker() {
        use crabka_pgmvcc::{
            clog::{XidStatus, put_op},
            xid::GLOBAL_XID_BASE,
        };
        // Single-store in-memory engine: `self.kv == self.catalog_kv` (both the same `Arc`
        // per `with_kv`), so `MemKv` here is this range's local clog.
        let kv = Arc::new(MemKv::new());
        let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
        let g = GLOBAL_XID_BASE + 1;
        // A durable Prepared(Li=11 -> g) marker exists on this range.
        kv.write_batch(&[put_op(11, XidStatus::Prepared(g))])
            .expect("stage marker");
        assert_eq!(
            engine.staged_local_for(g).await.expect("scan"),
            Some(11),
            "finds the existing Prepared(-> g) marker's local xid"
        );
        assert_eq!(
            engine
                .staged_local_for(GLOBAL_XID_BASE + 2)
                .await
                .expect("scan"),
            None,
            "no marker for a different global xid"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_doubt_globals_from_bounds_the_scan_and_advances_past_terminal() {
        use crabka_pgmvcc::{
            clog::{XidStatus, put_op},
            xid::GLOBAL_XID_BASE,
        };
        // Two stores: sm_kv = this data range's local clog; catalog_kv = range 0's global-G clog.
        let sm_kv: std::sync::Arc<dyn crabka_pgkv::Kv> =
            std::sync::Arc::new(crabka_pgkv::MemKv::new());
        let catalog_kv: std::sync::Arc<dyn crabka_pgkv::Kv> =
            std::sync::Arc::new(crabka_pgkv::MemKv::new());
        let committer = std::sync::Arc::new(crate::commit::LocalCommitter {
            kv: std::sync::Arc::clone(&sm_kv),
        });
        let linearizer = std::sync::Arc::new(crate::read_gate::LocalLinearizer);
        let engine = SqlEngine::replicated(
            std::sync::Arc::clone(&catalog_kv),
            std::sync::Arc::clone(&sm_kv),
            committer,
            linearizer,
        )
        .expect("engine");

        let (g_term, g_doubt) = (GLOBAL_XID_BASE + 1, GLOBAL_XID_BASE + 2);
        // Local markers at Li = 10 (terminal G), 11 (in-doubt G), 12 (terminal G) — sm_kv ONLY.
        sm_kv
            .write_batch(&[put_op(10, XidStatus::Prepared(g_term))])
            .expect("p10");
        sm_kv
            .write_batch(&[put_op(11, XidStatus::Prepared(g_doubt))])
            .expect("p11");
        sm_kv
            .write_batch(&[put_op(12, XidStatus::Prepared(g_term))])
            .expect("p12");
        // Global decisions — catalog_kv ONLY.
        catalog_kv
            .write_batch(&[put_op(g_term, XidStatus::Committed)])
            .expect("decide g_term");
        // from(0): only g_doubt is in-doubt; watermark stops at the in-doubt Li (11).
        let (gs, lo) = engine.in_doubt_globals_from(0).await.expect("scan");
        assert_eq!(gs, vec![g_doubt]);
        assert_eq!(lo, 11, "watermark = smallest in-doubt Li");
        // Decide g_doubt; from(11) finds nothing in-doubt -> watermark = one past the largest local Li (12).
        catalog_kv
            .write_batch(&[put_op(g_doubt, XidStatus::Aborted)])
            .expect("decide g_doubt");
        let (gs2, lo2) = engine.in_doubt_globals_from(11).await.expect("scan");
        assert!(gs2.is_empty());
        assert_eq!(
            lo2, 13,
            "all terminal -> watermark = one past the largest local Li (12)"
        );
        // Edge: scan_lo above all markers -> empty scan -> watermark unchanged.
        assert_eq!(engine.in_doubt_globals_from(99).await.expect("scan").1, 99);
        // Edge: an in-doubt marker at the HIGHEST Li (terminals below) holds the
        // watermark exactly there (the ascending scan stops at the first undecided).
        sm_kv
            .write_batch(&[put_op(20, XidStatus::Prepared(GLOBAL_XID_BASE + 3))])
            .expect("high in-doubt marker");
        let (gs3, lo3) = engine.in_doubt_globals_from(0).await.expect("scan");
        assert_eq!(gs3, vec![GLOBAL_XID_BASE + 3]);
        assert_eq!(
            lo3, 20,
            "in-doubt at the highest Li holds the watermark there"
        );
    }

    /// SP24 root fix: on a leadership rise, `reacquire_in_doubt_locks` re-takes the
    /// exclusive row lock for an inherited in-doubt `Prepared(Li -> g)` version even
    /// though the in-memory lock table was wiped — and a concurrent writer BLOCKS on
    /// that lock until `release_in_doubt_lock(Li)` frees it once `g` is terminal. This
    /// is the serialize-before-serve guarantee the per-session fence cannot give under
    /// apply lag.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reacquire_in_doubt_locks_blocks_a_concurrent_writer_until_released() {
        use crabka_pgmvcc::{
            clog::{XidStatus, put_op},
            xid::GLOBAL_XID_BASE,
        };
        use crabka_pgwire::engine::{Engine, Session};

        use crate::lockmgr::LockMode;

        // One in-memory store plays both this range's local clog/versions AND range 0's
        // global clog (single-store engine: kv == catalog_kv).
        let kv = Arc::new(MemKv::new());
        let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
        // Seed schema + a row, then stage an UPDATE under a held participant session so the
        // store carries a real `Prepared(Li -> g)` version with the row-keyed xmin == Li.
        {
            let mut s = engine.connect();
            s.simple_query("CREATE TABLE t (id int4)")
                .await
                .expect("create");
            s.simple_query("INSERT INTO t VALUES (1)")
                .await
                .expect("insert");
        }
        let g = GLOBAL_XID_BASE + 1;
        let li = {
            let mut s = engine.connect();
            s.ensure_began().await.expect("begin");
            s.simple_query("UPDATE t SET id = 2 WHERE id = 1")
                .await
                .expect("stage");
            let li = s.local_xid().expect("li");
            s.prepare_global_participant(g)
                .await
                .expect("prepare participant"); // Prepared(li -> g) durable
            // Drop the held session WITHOUT resolving — mirrors the killed leader losing its
            // in-memory session + lock table, while the durable Prepared(li -> g) version stays.
            drop(s);
            li
        };
        // The dropped session freed li's lock (presumed-abort), so the inherited in-doubt
        // row now has NO live lock holder — exactly the wiped-lock-table condition.
        let table = crabka_pgcatalog::get_table(kv.as_ref(), "t").expect("t").id;
        // g is still in-doubt (no global decision written): recovery re-acquires li's lock.
        let pairs = engine.reacquire_in_doubt_locks().await.expect("reacquire");
        assert_eq!(
            pairs,
            vec![(li, g)],
            "the inherited (Li -> g) is re-acquired"
        );

        // A concurrent writer for the SAME row must BLOCK on the re-acquired lock.
        let blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let lockmgr = Arc::clone(&engine.lockmgr);
        let blocked2 = Arc::clone(&blocked);
        let other_xid = li + 1000;
        let waiter = tokio::spawn(async move {
            lockmgr
                .acquire(table, /*rowid*/ 1, LockMode::Exclusive, other_xid)
                .await
                .expect("not a deadlock");
            blocked2.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        // Give the waiter a chance to register; it must NOT have acquired (lock held by li).
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert!(
            !blocked.load(std::sync::atomic::Ordering::SeqCst),
            "a concurrent writer must block on the re-acquired in-doubt lock"
        );

        // Resolve g terminally, then release li's lock — the waiter must now proceed.
        kv.write_batch(&[put_op(g, XidStatus::Aborted)])
            .expect("decide g");
        engine.release_in_doubt_lock(li);
        tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("waiter did not hang")
            .expect("waiter task");
        assert!(
            blocked.load(std::sync::atomic::Ordering::SeqCst),
            "the writer proceeds once the in-doubt lock is released"
        );
    }

    /// `reacquire_in_doubt_locks` skips a marker whose `g` is already TERMINAL (no lock
    /// taken — the row is settled), and returns only the genuinely in-doubt pairs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reacquire_in_doubt_locks_skips_terminal_g() {
        use crabka_pgmvcc::{
            clog::{XidStatus, put_op},
            xid::GLOBAL_XID_BASE,
        };
        use crabka_pgwire::engine::{Engine, Session};
        let kv = Arc::new(MemKv::new());
        let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
        {
            let mut s = engine.connect();
            s.simple_query("CREATE TABLE t (id int4)")
                .await
                .expect("create");
            s.simple_query("INSERT INTO t VALUES (1)")
                .await
                .expect("insert");
        }
        let g = GLOBAL_XID_BASE + 1;
        {
            let mut s = engine.connect();
            s.ensure_began().await.expect("begin");
            s.simple_query("UPDATE t SET id = 2 WHERE id = 1")
                .await
                .expect("stage");
            s.prepare_global_participant(g)
                .await
                .expect("prepare participant");
            drop(s);
        }
        // g is COMMITTED (terminal) → the row is settled → recovery takes no lock.
        kv.write_batch(&[put_op(g, XidStatus::Committed)])
            .expect("decide g");
        assert!(
            engine
                .reacquire_in_doubt_locks()
                .await
                .expect("reacquire")
                .is_empty(),
            "a terminally-decided g is not re-locked"
        );
    }

    #[tokio::test]
    async fn in_doubt_scan_watermark_stays_below_global_xid_base_on_range_0() {
        use crabka_pgmvcc::xid::GLOBAL_XID_BASE;
        let kv: std::sync::Arc<dyn crabka_pgkv::Kv> =
            std::sync::Arc::new(crabka_pgkv::MemKv::new());
        // A terminal LOCAL entry (so the scan has a local row) and a GLOBAL-decision entry keyed
        // high in the global-xid space (range 0 mixes participant markers with the global clog).
        // NO in-doubt local marker → first_undecided == None → the watermark is max_li+1, which the
        // bound must keep below GLOBAL_XID_BASE.
        kv.write_batch(&[
            crabka_pgmvcc::clog::put_op(5, crabka_pgmvcc::clog::XidStatus::Committed),
            crabka_pgmvcc::clog::put_op(
                GLOBAL_XID_BASE + 3,
                crabka_pgmvcc::clog::XidStatus::Committed,
            ),
        ])
        .expect("seed");
        let engine = SqlEngine::with_kv(kv.clone()).expect("engine"); // catalog_kv == kv (range-0 self)
        let (gs, new_lo) = engine.in_doubt_globals_from(0).await.expect("scan");
        assert!(gs.is_empty(), "no in-doubt markers → empty (got {gs:?})");
        assert!(
            new_lo < GLOBAL_XID_BASE,
            "the watermark never jumps into the global-xid space (got {new_lo})"
        );
    }

    #[tokio::test]
    async fn in_doubt_scan_returns_only_local_participant_markers_on_range_0() {
        use crabka_pgmvcc::xid::GLOBAL_XID_BASE;
        let kv: std::sync::Arc<dyn crabka_pgkv::Kv> =
            std::sync::Arc::new(crabka_pgkv::MemKv::new());
        let g_indoubt = GLOBAL_XID_BASE + 7; // its decision is absent → in-doubt
        kv.write_batch(&[
            crabka_pgmvcc::clog::put_op(5, crabka_pgmvcc::clog::XidStatus::Prepared(g_indoubt)),
            crabka_pgmvcc::clog::put_op(
                GLOBAL_XID_BASE + 3,
                crabka_pgmvcc::clog::XidStatus::Committed,
            ),
        ])
        .expect("seed");
        let engine = SqlEngine::with_kv(kv.clone()).expect("engine");
        let (gs, _new_lo) = engine.in_doubt_globals_from(0).await.expect("scan");
        assert_eq!(
            gs,
            vec![g_indoubt],
            "only the in-doubt local participant g is returned, never a global decision"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn clog_scan_lo_persists_and_is_monotone() {
        let sm_kv: std::sync::Arc<dyn crabka_pgkv::Kv> =
            std::sync::Arc::new(crabka_pgkv::MemKv::new());
        let catalog_kv: std::sync::Arc<dyn crabka_pgkv::Kv> =
            std::sync::Arc::new(crabka_pgkv::MemKv::new());
        let committer = std::sync::Arc::new(crate::commit::LocalCommitter {
            kv: std::sync::Arc::clone(&sm_kv),
        });
        let linearizer = std::sync::Arc::new(crate::read_gate::LocalLinearizer);
        let engine = SqlEngine::replicated(
            catalog_kv,
            std::sync::Arc::clone(&sm_kv),
            committer,
            linearizer,
        )
        .expect("engine");
        assert_eq!(engine.clog_scan_lo().expect("lo"), 0); // absent -> 0
        engine.advance_clog_scan_lo(5).await.expect("advance");
        assert_eq!(engine.clog_scan_lo().expect("lo"), 5);
        engine.advance_clog_scan_lo(3).await.expect("no-op"); // lower -> no-op
        assert_eq!(engine.clog_scan_lo().expect("lo"), 5, "monotone");
    }

    #[test]
    fn checkpoint_horizon_advances_over_terminal_clog_but_stops_at_prepared() {
        let engine = SqlEngine::new();
        let committed = engine.procarray.begin_write().expect("committed xid");
        let prepared = engine.procarray.begin_write().expect("prepared xid");
        engine
            .kv
            .write_batch(&[
                crabka_pgmvcc::clog::put_op(committed, crabka_pgmvcc::clog::XidStatus::Committed),
                crabka_pgmvcc::clog::put_op(prepared, crabka_pgmvcc::clog::XidStatus::Prepared(99)),
            ])
            .expect("seed clog");
        engine.procarray.finish(committed);
        engine.procarray.finish(prepared);

        assert_eq!(
            engine.checkpoint_garbage_horizon().expect("horizon"),
            prepared
        );
    }
}
