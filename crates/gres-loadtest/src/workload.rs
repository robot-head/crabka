//! SQL workload driver: connections, operation mix, pacing, measurement.
//!
//! # Schema
//!
//! [`prepare_schema`] mirrors `scripts/gres-range-scaling.sh`: table names
//! carry an explicit numeric id and land on the range whose boundary span
//! contains that id (boundaries sit at multiples of 1 000 000). Range `r`
//! gets `t{r * 1_000_000} (id int4)`, and a hot table
//! `t{(ranges - 1) * 1_000_000 + 1} (id int4, v int4)` lands inside the last
//! range's span, seeded with `hot_rows` rows (`id` 1..=`hot_rows`, `v` 0) via
//! batched multi-row `INSERT`s.
//!
//! # Operation classes
//!
//! Every operation stays inside the SQL surface proven against the sharded
//! engine (`crates/gres-ranges/tests/jepsen_bank.rs` and `multirange.rs`) and
//! the conformance corpus (`crates/gres-conformance/corpus/`):
//!
//! - [`OpClass::SingleShardInsert`] — autocommit
//!   `INSERT INTO t<range> VALUES (<id>)` into a uniformly-chosen range
//!   table; ids are `worker * 1_000_000 + counter`, mirroring the script's
//!   disjoint per-worker id spaces.
//! - [`OpClass::CrossShardTxn`] — `BEGIN`, one `INSERT` into each of two
//!   distinct range tables, `COMMIT` (best-effort `ROLLBACK` on error): the
//!   2PC + global-timestamp path.
//! - [`OpClass::ReadOnly`] —
//!   `SELECT count(*) FROM t<range> WHERE id >= <lo> AND id < <lo + 1024>`:
//!   a bounded slice, never an unbounded scan of an ever-growing table.
//! - [`OpClass::ContendedUpdate`] —
//!   `UPDATE t<hot> SET v = v + 1 WHERE id = <rank>` with Zipf-distributed
//!   ranks. `SQLSTATE` 40001 (`serialization_failure`) is retried up to 5
//!   times with jittered backoff before the transaction counts as failed.
//!
//! # Connection routing
//!
//! The gateway DML path currently needs a **local** range-0 engine
//! (`statement_targets_sharded_table` in `gres-ranges/src/tenant.rs` and the
//! timestamp-coordinator paths resolve `RangeId::COORDINATOR` locally), so a
//! write issued through a node not hosting r0 fails with `SQLSTATE` 0A000
//! "range r0 is not hosted". Node 0 always hosts r0 under the harness's
//! round-robin range assignment, so every worker sends the write classes
//! (inserts, cross-shard transactions, contended updates) over a connection
//! to `endpoints[0]` — node 0's SQL front door — while
//! [`OpClass::ReadOnly`] traffic fans out round-robin across every node's
//! front door. A worker whose read endpoint is node 0, or whose mix never
//! issues one of the two kinds, holds a single connection instead of two.
//! Each connection reconnects with independent backoff, and only node 0's
//! front door must accept a connection at startup; other endpoints'
//! initial unavailability is counted and retried like any mid-run
//! connection error.
//!
//! # Pacing, faults, and measurement
//!
//! [`run`] drives one tokio task per configured connection. Workers pace
//! through a shared token bucket under [`RateSpec::Fixed`] and free-run
//! under [`RateSpec::Saturate`]. Connection loss triggers
//! reconnect-with-backoff forever (faults are expected to kill links
//! mid-run), and a per-operation timeout turns a blackholed link into an
//! `unavailable` count instead of a hung worker. After `warmup_s` of
//! unrecorded load, a `duration_s` window records per-class HDR histograms
//! (1µs..60s, 3 significant figures), commit/failure counters, the error
//! taxonomy, and a per-second timeline; in-flight operations get a short
//! grace period to finish before workers are aborted.

use std::{
    collections::BTreeMap,
    error::Error as _,
    fmt::Write as _,
    io::ErrorKind as IoErrorKind,
    mem,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Context as _;
use futures::future;
use hdrhistogram::Histogram;
use rand::{RngExt, SeedableRng, rngs::SmallRng};
use tokio::{sync::watch, time::Instant};
use tokio_postgres::{Client, NoTls, error::SqlState};

use crate::{
    cluster::SqlEndpoint,
    report::{ErrorSummary, LatencySummary, SecondSample},
    scenario::{MixSpec, RateSpec, TopologySpec, WorkloadSpec},
};

/// Width of one range's id span; table `t{r * RANGE_SPAN}` lands on range
/// `r` (matches `scripts/gres-range-scaling.sh`).
const RANGE_SPAN: i64 = 1_000_000;
/// Width of one worker's insert-id space.
const WORKER_ID_SPAN: i64 = 1_000_000;
/// Worker insert-id counters wrap at this bound so values stay within
/// `int4` even on long runs.
const WORKER_COUNTER_SPAN: u64 = 1_000_000;
/// Worker ids wrap at this bound so `worker * WORKER_ID_SPAN + counter`
/// stays within `int4`.
const WORKER_SLOTS: u32 = 2_000;
/// Rows covered by one read-only slice predicate.
const READ_SLICE_ROWS: i64 = 1_024;
/// Rows per multi-row `INSERT` while seeding the hot table.
const SEED_BATCH_ROWS: u32 = 500;
/// Serialization-failure retries before an operation counts as failed.
const MAX_SERIALIZATION_RETRIES: u32 = 5;
/// Per-operation timeout; a blackholed link becomes `unavailable`.
const OP_TIMEOUT: Duration = Duration::from_secs(30);
/// Timeout for a single connection attempt.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Total budget for the startup probe / schema connection.
const STARTUP_DEADLINE: Duration = Duration::from_secs(30);
/// How long in-flight operations may finish after the window closes.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// First reconnect backoff step (doubles up to [`RECONNECT_BACKOFF_MAX`]).
const RECONNECT_BACKOFF_MIN: Duration = Duration::from_millis(100);
/// Reconnect backoff cap.
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(2);

/// Operation classes the driver issues.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum OpClass {
    /// Autocommit single-range insert.
    SingleShardInsert,
    /// Two-range read-write transaction.
    CrossShardTxn,
    /// Single-range snapshot read.
    ReadOnly,
    /// Zipf-distributed hot-row update.
    ContendedUpdate,
}

impl OpClass {
    /// Kebab-case name used as the report key.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::SingleShardInsert => "single-shard-insert",
            Self::CrossShardTxn => "cross-shard-txn",
            Self::ReadOnly => "read-only",
            Self::ContendedUpdate => "contended-update",
        }
    }
}

/// All classes in report order.
const ALL_CLASSES: [OpClass; 4] = [
    OpClass::SingleShardInsert,
    OpClass::CrossShardTxn,
    OpClass::ReadOnly,
    OpClass::ContendedUpdate,
];

/// Everything the workload measured during the window.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkloadOutcome {
    /// Transactions committed inside the measurement window.
    pub committed: u64,
    /// Transactions that ultimately failed (retries exhausted or fatal).
    pub failed: u64,
    /// Latency distribution per class.
    pub latency_by_class: BTreeMap<OpClass, LatencySummary>,
    /// Per-second committed/error counts.
    pub timeline: Vec<SecondSample>,
    /// Error taxonomy totals.
    pub errors: ErrorSummary,
    /// Actual measured wall-clock seconds.
    pub measured_wall_s: f64,
}

/// Creates the workload schema and seed rows through one node.
///
/// The runner invokes this against the schema-bootstrap node — a temporary
/// node hosting **all** ranges (see `Cluster::launch_schema_bootstrap` in
/// the cluster module) — so the DDL and hot-table seeding all execute
/// range-locally.
///
/// # Errors
///
/// Returns an error if connecting or any DDL/seed statement fails.
pub async fn prepare_schema(
    endpoint: &SqlEndpoint,
    workload: &WorkloadSpec,
    topology: &TopologySpec,
) -> anyhow::Result<()> {
    let client = connect_with_retry(endpoint).await?;
    let mut ddl = String::new();
    for range in 0..topology.ranges {
        let table_id = range_table_id(range);
        let _ = write!(ddl, "CREATE TABLE t{table_id} (id int4);");
    }
    let hot_id = hot_table_id(topology.ranges);
    let _ = write!(ddl, "CREATE TABLE t{hot_id} (id int4, v int4);");
    client
        .simple_query(&ddl)
        .await
        .context("create workload tables")?;
    for statement in seed_statements(hot_id, workload.hot_rows) {
        client
            .simple_query(&statement)
            .await
            .context("seed hot table")?;
    }
    Ok(())
}

/// Runs warmup then the measured window against the given SQL endpoints
/// (workers are spread round-robin across them).
///
/// # Errors
///
/// Returns an error only on harness-level failures (e.g. the write gateway —
/// node 0's SQL front door, which every write path requires — never accepted
/// a connection during startup); workload-level errors are counted in the
/// outcome, because faults are expected to cause them.
pub async fn run(
    endpoints: &[SqlEndpoint],
    workload: &WorkloadSpec,
    topology: &TopologySpec,
) -> anyhow::Result<WorkloadOutcome> {
    anyhow::ensure!(!endpoints.is_empty(), "no SQL endpoints to drive");
    // Only the write gateway must accept at startup; other endpoints'
    // unavailability is retried by workers like any mid-run fault.
    drop(
        connect_with_retry(&endpoints[0])
            .await
            .context("startup probe of node 0's SQL front door (the write gateway)")?,
    );

    let context = Arc::new(build_context(workload, topology));
    let (stop_tx, stop_rx) = watch::channel(false);
    let handles: Vec<_> = (0..workload.connections)
        .map(|worker| {
            let routing = route_worker(worker, endpoints.len(), &workload.mix);
            tokio::spawn(worker_loop(
                Arc::clone(&context),
                worker,
                build_connections(endpoints, routing),
                stop_rx.clone(),
            ))
        })
        .collect();
    drop(stop_rx);

    tokio::time::sleep(Duration::from_secs(workload.warmup_s)).await;
    context.stats.recording.store(true, Ordering::SeqCst);
    let window_start = Instant::now();
    let mut timeline = Vec::new();
    for t_s in 0..workload.duration_s {
        tokio::time::sleep_until(window_start + Duration::from_secs(t_s + 1)).await;
        let accum = context.stats.drain_second();
        timeline.push(SecondSample {
            t_s,
            committed: accum.committed,
            errors: accum.errors,
            mean_latency_ms: mean_latency(&accum),
        });
    }
    let measured_wall_s = window_start.elapsed().as_secs_f64();

    let _ = stop_tx.send(true);
    let abort_handles: Vec<_> = handles
        .iter()
        .map(tokio::task::JoinHandle::abort_handle)
        .collect();
    if tokio::time::timeout(SHUTDOWN_GRACE, future::join_all(handles))
        .await
        .is_err()
    {
        for abort in &abort_handles {
            abort.abort();
        }
    }
    context.stats.recording.store(false, Ordering::SeqCst);
    Ok(build_outcome(&context.stats, measured_wall_s, timeline))
}

/// Table layout the operations target.
#[derive(Debug, Clone, Copy)]
struct TableLayout {
    /// Number of ranges (one `t{r * RANGE_SPAN}` table each).
    ranges: u16,
    /// Numeric id of the hot table (inside the last range's span).
    hot_table_id: i64,
    /// Worker count, bounding the live insert-id space for reads.
    connections: u32,
}

/// Everything the worker tasks share.
struct WorkerContext {
    layout: TableLayout,
    mix: MixSpec,
    total_weight: u64,
    zipf: ZipfSampler,
    bucket: Option<TokenBucket>,
    stats: Stats,
}

fn build_context(workload: &WorkloadSpec, topology: &TopologySpec) -> WorkerContext {
    WorkerContext {
        layout: TableLayout {
            ranges: topology.ranges,
            hot_table_id: hot_table_id(topology.ranges),
            connections: workload.connections,
        },
        mix: workload.mix,
        total_weight: workload.mix.total_weight().max(1),
        zipf: ZipfSampler::new(workload.hot_rows.max(1), workload.zipf_exponent),
        bucket: match workload.rate {
            RateSpec::Saturate => None,
            RateSpec::Fixed { tps } => Some(TokenBucket::new(tps)),
        },
        stats: Stats::new(),
    }
}

/// Which endpoints a worker's connections dial.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkerRouting {
    /// Endpoint index for the write classes, if the mix issues any. Always
    /// node 0's front door: the only gateway hosting range 0 today (see the
    /// module docs on connection routing).
    write_endpoint: Option<usize>,
    /// Endpoint index for read-only traffic, if the mix issues any:
    /// round-robin over every node's front door.
    read_endpoint: Option<usize>,
    /// Both kinds ride one connection (the read endpoint IS the write one).
    shared: bool,
}

/// Whether the mix ever issues a class that must write through node 0.
fn mix_needs_write_connection(mix: &MixSpec) -> bool {
    mix.single_shard_insert > 0 || mix.cross_shard_txn > 0 || mix.contended_update > 0
}

/// Whether the mix ever issues read-only operations.
fn mix_needs_read_connection(mix: &MixSpec) -> bool {
    mix.read_only > 0
}

/// Routes one worker's connections; workers never open a connection their
/// mix cannot use, and never open two connections to the same endpoint.
fn route_worker(worker: u32, endpoint_count: usize, mix: &MixSpec) -> WorkerRouting {
    let needs_write = mix_needs_write_connection(mix);
    let needs_read = mix_needs_read_connection(mix);
    let read_index = usize::try_from(worker).unwrap_or(usize::MAX) % endpoint_count.max(1);
    WorkerRouting {
        write_endpoint: needs_write.then_some(0),
        read_endpoint: needs_read.then_some(read_index),
        shared: needs_write && needs_read && read_index == 0,
    }
}

/// Materialises a routing decision into (up to two) connection slots; when
/// the routes coincide the write slot serves both kinds.
fn build_connections(endpoints: &[SqlEndpoint], routing: WorkerRouting) -> WorkerConnections {
    let slot = |index: usize| ConnectionSlot::new(endpoint_at(endpoints, index));
    WorkerConnections {
        write: routing.write_endpoint.map(&slot),
        read: if routing.shared {
            None
        } else {
            routing.read_endpoint.map(&slot)
        },
    }
}

fn endpoint_at(endpoints: &[SqlEndpoint], index: usize) -> SqlEndpoint {
    endpoints[index % endpoints.len().max(1)].clone()
}

/// The numeric id of range `range`'s table; the table lands on that range
/// because its id falls inside the range's boundary span.
fn range_table_id(range: u16) -> i64 {
    i64::from(range) * RANGE_SPAN
}

/// The numeric id of the hot table, inside the last range's boundary span.
fn hot_table_id(ranges: u16) -> i64 {
    i64::from(ranges.saturating_sub(1)) * RANGE_SPAN + 1
}

/// A unique-ish `int4` insert value: disjoint per-worker id spaces, wrapping
/// so long runs stay within `int4` (duplicates are harmless — the tables
/// carry no unique constraint).
fn insert_id(worker: u32, counter: u64) -> i64 {
    let base = i64::from(worker % WORKER_SLOTS) * WORKER_ID_SPAN;
    let offset = i64::try_from(counter % WORKER_COUNTER_SPAN).unwrap_or(0);
    base + offset
}

/// Batched multi-row seed `INSERT`s for the hot table.
fn seed_statements(table_id: i64, hot_rows: u32) -> Vec<String> {
    let mut statements = Vec::new();
    let mut row = 1_u32;
    while row <= hot_rows {
        let end = row.saturating_add(SEED_BATCH_ROWS - 1).min(hot_rows);
        let values: Vec<String> = (row..=end).map(|id| format!("({id}, 0)")).collect();
        statements.push(format!(
            "INSERT INTO t{table_id} VALUES {}",
            values.join(", ")
        ));
        row = end.saturating_add(1);
    }
    statements
}

/// One planned operation: the exact SQL a worker will send.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OpPlan {
    /// A single autocommit statement.
    Statement(String),
    /// `BEGIN` + statements + `COMMIT`, with `ROLLBACK` on error.
    Transaction(Vec<String>),
}

/// Builds the SQL for one operation of `class`.
fn build_plan(
    class: OpClass,
    layout: &TableLayout,
    worker: u32,
    counter: u64,
    zipf: &ZipfSampler,
    rng: &mut SmallRng,
) -> OpPlan {
    match class {
        OpClass::CrossShardTxn if layout.ranges > 1 => {
            let first = u32::from(rng.random_range(0..layout.ranges));
            let offset = u32::from(rng.random_range(1..layout.ranges));
            let second = (first + offset) % u32::from(layout.ranges);
            let value = insert_id(worker, counter);
            let insert = |range: u32| {
                let table_id = i64::from(range) * RANGE_SPAN;
                format!("INSERT INTO t{table_id} VALUES ({value})")
            };
            OpPlan::Transaction(vec![insert(first), insert(second)])
        }
        OpClass::SingleShardInsert | OpClass::CrossShardTxn => {
            let table_id = range_table_id(rng.random_range(0..layout.ranges));
            let value = insert_id(worker, counter);
            OpPlan::Statement(format!("INSERT INTO t{table_id} VALUES ({value})"))
        }
        OpClass::ReadOnly => {
            let table_id = range_table_id(rng.random_range(0..layout.ranges));
            let span = i64::from(layout.connections.min(WORKER_SLOTS)) * WORKER_ID_SPAN;
            let low = rng.random_range(0..span.max(1));
            let high = low + READ_SLICE_ROWS;
            OpPlan::Statement(format!(
                "SELECT count(*) FROM t{table_id} WHERE id >= {low} AND id < {high}"
            ))
        }
        OpClass::ContendedUpdate => {
            let rank = zipf.sample(rng.random());
            OpPlan::Statement(format!(
                "UPDATE t{} SET v = v + 1 WHERE id = {rank}",
                layout.hot_table_id
            ))
        }
    }
}

/// Picks a class by mix weight from a uniform draw in
/// `[0, mix.total_weight())`.
fn pick_class(mix: &MixSpec, draw: u64) -> OpClass {
    let mut remaining = draw;
    for (class, weight) in [
        (
            OpClass::SingleShardInsert,
            u64::from(mix.single_shard_insert),
        ),
        (OpClass::CrossShardTxn, u64::from(mix.cross_shard_txn)),
        (OpClass::ReadOnly, u64::from(mix.read_only)),
        (OpClass::ContendedUpdate, u64::from(mix.contended_update)),
    ] {
        if remaining < weight {
            return class;
        }
        remaining -= weight;
    }
    OpClass::SingleShardInsert
}

/// Where a failed operation lands in the error taxonomy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureKind {
    /// `SQLSTATE` 40001: retryable serialization failure.
    Serialization,
    /// The cluster was unreachable: refused, timed out, or other IO failure.
    Unavailable,
    /// A live connection was closed or reset mid-operation.
    ConnectionLost,
    /// Any other error (non-retryable `SQLSTATE`s included).
    Other,
}

/// Classifies a driver error: `SQLSTATE` first, then transport inspection.
fn classify(error: &tokio_postgres::Error) -> FailureKind {
    if let Some(code) = error.code() {
        return classify_sqlstate(code);
    }
    if error.is_closed() {
        return FailureKind::ConnectionLost;
    }
    classify_transport(io_kind(error))
}

fn classify_sqlstate(code: &SqlState) -> FailureKind {
    if *code == SqlState::T_R_SERIALIZATION_FAILURE {
        FailureKind::Serialization
    } else {
        FailureKind::Other
    }
}

/// The first `std::io::Error` kind in the error's source chain, if any.
fn io_kind(error: &tokio_postgres::Error) -> Option<IoErrorKind> {
    let mut source = error.source();
    while let Some(cause) = source {
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            return Some(io.kind());
        }
        source = cause.source();
    }
    None
}

fn classify_transport(kind: Option<IoErrorKind>) -> FailureKind {
    match kind {
        Some(
            IoErrorKind::ConnectionReset
            | IoErrorKind::ConnectionAborted
            | IoErrorKind::BrokenPipe
            | IoErrorKind::UnexpectedEof
            | IoErrorKind::NotConnected,
        ) => FailureKind::ConnectionLost,
        Some(_) => FailureKind::Unavailable,
        None => FailureKind::Other,
    }
}

/// Zipf sampler over ranks `1..=n` with probability proportional to
/// `1 / rank^s`, via a precomputed CDF and binary search.
#[derive(Debug, Clone)]
struct ZipfSampler {
    cdf: Vec<f64>,
}

impl ZipfSampler {
    fn new(ranks: u32, exponent: f64) -> Self {
        let mut cdf = Vec::new();
        let mut running = 0.0;
        for rank in 1..=ranks.max(1) {
            running += f64::from(rank).powf(-exponent);
            cdf.push(running);
        }
        for bound in &mut cdf {
            *bound /= running;
        }
        Self { cdf }
    }

    /// Maps a uniform draw in `[0, 1)` to a rank in `1..=n`.
    fn sample(&self, uniform: f64) -> u32 {
        let index = self.cdf.partition_point(|bound| *bound < uniform);
        let clamped = index.min(self.cdf.len() - 1);
        u32::try_from(clamped).map_or(u32::MAX, |value| value + 1)
    }
}

/// Continuously-refilled token bucket shared by all workers; capacity is one
/// second of tokens, starting empty.
#[derive(Debug)]
struct TokenBucket {
    state: Mutex<BucketState>,
    rate_per_s: f64,
    capacity: f64,
}

#[derive(Debug)]
struct BucketState {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(tps: u32) -> Self {
        let rate_per_s = f64::from(tps.max(1));
        Self {
            state: Mutex::new(BucketState {
                tokens: 0.0,
                last_refill: Instant::now(),
            }),
            rate_per_s,
            capacity: rate_per_s,
        }
    }

    async fn acquire(&self) {
        loop {
            let wait = {
                let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                let now = Instant::now();
                let elapsed = now.duration_since(state.last_refill).as_secs_f64();
                state.last_refill = now;
                state.tokens = (state.tokens + elapsed * self.rate_per_s).min(self.capacity);
                if state.tokens >= 1.0 {
                    state.tokens -= 1.0;
                    None
                } else {
                    let seconds = ((1.0 - state.tokens) / self.rate_per_s).max(0.000_5);
                    Some(Duration::from_secs_f64(seconds))
                }
            };
            match wait {
                None => return,
                Some(duration) => tokio::time::sleep(duration).await,
            }
        }
    }
}

/// One second of accumulated progress for the timeline.
#[derive(Debug, Default, Clone, Copy)]
struct SecondAccum {
    committed: u64,
    errors: u64,
    latency_sum_ms: f64,
    completions: u64,
}

/// Shared measurement state; every mutation is gated on `recording`.
struct Stats {
    recording: AtomicBool,
    committed: AtomicU64,
    failed: AtomicU64,
    serialization_retries: AtomicU64,
    unavailable: AtomicU64,
    connection_errors: AtomicU64,
    other: AtomicU64,
    histograms: [Mutex<Histogram<u64>>; 4],
    second: Mutex<SecondAccum>,
}

impl Stats {
    fn new() -> Self {
        Self {
            recording: AtomicBool::new(false),
            committed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            serialization_retries: AtomicU64::new(0),
            unavailable: AtomicU64::new(0),
            connection_errors: AtomicU64::new(0),
            other: AtomicU64::new(0),
            histograms: std::array::from_fn(|_| Mutex::new(new_histogram())),
            second: Mutex::new(SecondAccum::default()),
        }
    }

    fn recording(&self) -> bool {
        self.recording.load(Ordering::Relaxed)
    }

    fn record_commit(&self, class: OpClass, elapsed: Duration) {
        if !self.recording() {
            return;
        }
        self.committed.fetch_add(1, Ordering::Relaxed);
        let us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        record_us(&mut lock(&self.histograms[class_index(class)]), us);
        let mut second = lock(&self.second);
        second.committed += 1;
        second.completions += 1;
        second.latency_sum_ms += us_to_ms(us);
    }

    /// A transaction that ultimately failed.
    fn record_failure(&self, kind: FailureKind) {
        if !self.recording() {
            return;
        }
        self.failed.fetch_add(1, Ordering::Relaxed);
        self.bump_error(kind);
    }

    /// A failed connection attempt (no transaction was in flight).
    fn record_connect_failure(&self, kind: FailureKind) {
        if !self.recording() {
            return;
        }
        self.bump_error(kind);
    }

    fn record_serialization_retry(&self) {
        if !self.recording() {
            return;
        }
        self.serialization_retries.fetch_add(1, Ordering::Relaxed);
    }

    fn bump_error(&self, kind: FailureKind) {
        let counter = match kind {
            FailureKind::Serialization | FailureKind::Other => &self.other,
            FailureKind::Unavailable => &self.unavailable,
            FailureKind::ConnectionLost => &self.connection_errors,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        lock(&self.second).errors += 1;
    }

    fn drain_second(&self) -> SecondAccum {
        mem::take(&mut *lock(&self.second))
    }
}

impl Default for Stats {
    fn default() -> Self {
        Self::new()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn class_index(class: OpClass) -> usize {
    match class {
        OpClass::SingleShardInsert => 0,
        OpClass::CrossShardTxn => 1,
        OpClass::ReadOnly => 2,
        OpClass::ContendedUpdate => 3,
    }
}

/// HDR histogram with the workspace-standard latency bounds
/// (see `crates/bench-driver/src/hist.rs`).
fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000, 3).expect("histogram bounds are valid")
}

/// Records one latency sample in microseconds, clamped into range so a
/// single outlier cannot blow up the recorder.
fn record_us(histogram: &mut Histogram<u64>, us: u64) {
    let value = us.clamp(1, histogram.high());
    let _ = histogram.record(value);
}

fn us_to_ms(us: u64) -> f64 {
    f64::from(u32::try_from(us).unwrap_or(u32::MAX)) / 1000.0
}

fn count_to_f64(count: u64) -> f64 {
    f64::from(u32::try_from(count).unwrap_or(u32::MAX))
}

fn summarize(histogram: &Histogram<u64>) -> LatencySummary {
    LatencySummary {
        count: histogram.len(),
        mean_ms: histogram.mean() / 1000.0,
        p50_ms: us_to_ms(histogram.value_at_quantile(0.50)),
        p95_ms: us_to_ms(histogram.value_at_quantile(0.95)),
        p99_ms: us_to_ms(histogram.value_at_quantile(0.99)),
        p999_ms: us_to_ms(histogram.value_at_quantile(0.999)),
        max_ms: us_to_ms(histogram.max()),
    }
}

fn mean_latency(accum: &SecondAccum) -> Option<f64> {
    if accum.completions == 0 {
        None
    } else {
        Some(accum.latency_sum_ms / count_to_f64(accum.completions))
    }
}

fn build_outcome(
    stats: &Stats,
    measured_wall_s: f64,
    timeline: Vec<SecondSample>,
) -> WorkloadOutcome {
    let mut latency_by_class = BTreeMap::new();
    for class in ALL_CLASSES {
        let histogram = lock(&stats.histograms[class_index(class)]);
        if !histogram.is_empty() {
            latency_by_class.insert(class, summarize(&histogram));
        }
    }
    WorkloadOutcome {
        committed: stats.committed.load(Ordering::Relaxed),
        failed: stats.failed.load(Ordering::Relaxed),
        latency_by_class,
        timeline,
        errors: ErrorSummary {
            serialization_retries: stats.serialization_retries.load(Ordering::Relaxed),
            unavailable: stats.unavailable.load(Ordering::Relaxed),
            connection_errors: stats.connection_errors.load(Ordering::Relaxed),
            other: stats.other.load(Ordering::Relaxed),
        },
        measured_wall_s,
    }
}

/// Connects to one endpoint and detaches the connection driver task.
async fn connect(endpoint: &SqlEndpoint) -> Result<Client, tokio_postgres::Error> {
    let mut config = tokio_postgres::Config::new();
    config
        .host(endpoint.addr.ip().to_string())
        .port(endpoint.addr.port())
        .user(&endpoint.user)
        .password(&endpoint.password)
        .dbname(&endpoint.database);
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

/// Connects to one endpoint, retrying until [`STARTUP_DEADLINE`].
async fn connect_with_retry(endpoint: &SqlEndpoint) -> anyhow::Result<Client> {
    let deadline = Instant::now() + STARTUP_DEADLINE;
    loop {
        let last_error = match tokio::time::timeout(CONNECT_TIMEOUT, connect(endpoint)).await {
            Ok(Ok(client)) => return Ok(client),
            Ok(Err(error)) => error.to_string(),
            Err(_) => "connect timed out".to_owned(),
        };
        anyhow::ensure!(
            Instant::now() < deadline,
            "endpoint {} did not accept a connection within {STARTUP_DEADLINE:?} \
             (last error: {last_error})",
            endpoint.addr
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// One (re)connectable client with its own reconnect backoff.
struct ConnectionSlot {
    endpoint: SqlEndpoint,
    client: Option<Client>,
    backoff: Duration,
}

impl ConnectionSlot {
    fn new(endpoint: SqlEndpoint) -> Self {
        Self {
            endpoint,
            client: None,
            backoff: RECONNECT_BACKOFF_MIN,
        }
    }

    /// Returns the live client, making one (re)connect attempt if the slot
    /// is empty. `None` means the attempt failed — already counted, backoff
    /// already slept (stop-aware) — so the caller just continues its loop;
    /// a worker is never wedged on one endpoint while another could serve.
    async fn ensure_connected(
        &mut self,
        worker: u32,
        stats: &Stats,
        stop: &mut watch::Receiver<bool>,
    ) -> Option<&Client> {
        if self.client.is_none() {
            match tokio::time::timeout(CONNECT_TIMEOUT, connect(&self.endpoint)).await {
                Ok(Ok(client)) => {
                    self.backoff = RECONNECT_BACKOFF_MIN;
                    self.client = Some(client);
                }
                Ok(Err(error)) => {
                    tracing::debug!(
                        worker,
                        endpoint = %self.endpoint.addr,
                        error = %error,
                        "worker connect failed"
                    );
                    stats.record_connect_failure(classify(&error));
                    self.backoff_sleep(stop).await;
                    return None;
                }
                Err(_) => {
                    tracing::debug!(
                        worker,
                        endpoint = %self.endpoint.addr,
                        "worker connect timed out"
                    );
                    stats.record_connect_failure(FailureKind::Unavailable);
                    self.backoff_sleep(stop).await;
                    return None;
                }
            }
        }
        self.client.as_ref()
    }

    async fn backoff_sleep(&mut self, stop: &mut watch::Receiver<bool>) {
        sleep_with_stop(self.backoff, stop).await;
        self.backoff = (self.backoff * 2).min(RECONNECT_BACKOFF_MAX);
    }

    fn drop_client(&mut self) {
        self.client = None;
    }
}

/// A worker's connection slots: writes through node 0's gateway, reads
/// through the worker's round-robin endpoint. `read` is `None` when the mix
/// never reads or when the read route coincides with the write route (the
/// write slot then serves both — see [`WorkerConnections::slot_for`]).
struct WorkerConnections {
    write: Option<ConnectionSlot>,
    read: Option<ConnectionSlot>,
}

impl WorkerConnections {
    /// The slot a class must use; read-only falls back to the write slot
    /// when the routes are shared.
    fn slot_for(&mut self, class: OpClass) -> Option<&mut ConnectionSlot> {
        match class {
            OpClass::ReadOnly => self.read.as_mut().or(self.write.as_mut()),
            OpClass::SingleShardInsert | OpClass::CrossShardTxn | OpClass::ContendedUpdate => {
                self.write.as_mut()
            }
        }
    }
}

/// One worker: pick a class, run it on that class's connection (connecting
/// with per-slot backoff as needed, forever), until told to stop.
async fn worker_loop(
    context: Arc<WorkerContext>,
    worker: u32,
    mut connections: WorkerConnections,
    mut stop: watch::Receiver<bool>,
) {
    let mut rng = SmallRng::seed_from_u64(u64::from(worker).wrapping_add(0x5eed_c0de));
    let mut counter: u64 = 0;
    while !*stop.borrow() {
        if let Some(bucket) = &context.bucket {
            tokio::select! {
                () = bucket.acquire() => {}
                _ = stop.changed() => return,
            }
        }
        let class = pick_class(&context.mix, rng.random_range(0..context.total_weight));
        counter += 1;
        let plan = build_plan(
            class,
            &context.layout,
            worker,
            counter,
            &context.zipf,
            &mut rng,
        );
        let Some(slot) = connections.slot_for(class) else {
            // Unreachable with a validated scenario: the mix only picks
            // classes whose connection the routing opened.
            return;
        };
        let Some(client) = slot
            .ensure_connected(worker, &context.stats, &mut stop)
            .await
        else {
            continue;
        };
        let started = Instant::now();
        match run_op(client, &plan, &context.stats, &mut rng).await {
            OpResult::Committed => context.stats.record_commit(class, started.elapsed()),
            OpResult::Failed { kind, reconnect } => {
                context.stats.record_failure(kind);
                if reconnect {
                    slot.drop_client();
                }
            }
        }
    }
}

/// Outcome of one operation, after serialization retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpResult {
    Committed,
    Failed { kind: FailureKind, reconnect: bool },
}

/// Executes one plan with the per-op timeout and serialization retries.
async fn run_op(client: &Client, plan: &OpPlan, stats: &Stats, rng: &mut SmallRng) -> OpResult {
    let mut retries = 0_u32;
    loop {
        match tokio::time::timeout(OP_TIMEOUT, execute_plan(client, plan)).await {
            Ok(Ok(())) => return OpResult::Committed,
            Ok(Err(error)) => {
                let kind = classify(&error);
                let (code, detail) = match error.as_db_error() {
                    Some(db) => (db.code().code(), db.message()),
                    None => ("-", ""),
                };
                tracing::debug!(?kind, %error, code, detail, "operation failed");
                match kind {
                    FailureKind::Serialization if retries < MAX_SERIALIZATION_RETRIES => {
                        retries += 1;
                        stats.record_serialization_retry();
                        let jitter_ms = u64::from(retries) * 2 + rng.random_range(0..3);
                        tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                    }
                    FailureKind::Serialization => {
                        return OpResult::Failed {
                            kind: FailureKind::Other,
                            reconnect: false,
                        };
                    }
                    kind @ (FailureKind::Unavailable | FailureKind::ConnectionLost) => {
                        return OpResult::Failed {
                            kind,
                            reconnect: true,
                        };
                    }
                    FailureKind::Other => {
                        return OpResult::Failed {
                            kind: FailureKind::Other,
                            // No SQLSTATE means the session state is unknown —
                            // safer to reconnect than to reuse the connection.
                            reconnect: error.code().is_none(),
                        };
                    }
                }
            }
            Err(_elapsed) => {
                return OpResult::Failed {
                    kind: FailureKind::Unavailable,
                    reconnect: true,
                };
            }
        }
    }
}

/// Sends a plan over one connection: autocommit statement, or
/// `BEGIN`/statements/`COMMIT` with best-effort `ROLLBACK` on error
/// (the pattern proven in `crates/gres-ranges/tests/jepsen_bank.rs`).
async fn execute_plan(client: &Client, plan: &OpPlan) -> Result<(), tokio_postgres::Error> {
    match plan {
        OpPlan::Statement(sql) => client.simple_query(sql).await.map(|_| ()),
        OpPlan::Transaction(statements) => {
            client.simple_query("BEGIN").await?;
            for sql in statements {
                if let Err(error) = client.simple_query(sql).await {
                    let _ = client.simple_query("ROLLBACK").await;
                    return Err(error);
                }
            }
            client.simple_query("COMMIT").await.map(|_| ())
        }
    }
}

async fn sleep_with_stop(duration: Duration, stop: &mut watch::Receiver<bool>) {
    tokio::select! {
        () = tokio::time::sleep(duration) => {}
        _ = stop.changed() => {}
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn rank_counts(sampler: &ZipfSampler, ranks: u32, draws: u32, seed: u64) -> Vec<u32> {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut counts = vec![0_u32; usize::try_from(ranks).expect("ranks fit usize")];
        for _ in 0..draws {
            let rank = sampler.sample(rng.random());
            assert!((1..=ranks).contains(&rank), "rank {rank} out of range");
            counts[usize::try_from(rank - 1).expect("rank fits usize")] += 1;
        }
        counts
    }

    #[test]
    fn zipf_skew_prefers_rank_one() {
        let sampler = ZipfSampler::new(100, 1.1);
        let counts = rank_counts(&sampler, 100, 10_000, 7);
        let top = counts[0];
        check!(top > 1_000, "rank 1 should dominate, got {top}");
        for (index, count) in counts.iter().enumerate().skip(1) {
            assert!(
                *count < top,
                "rank {} ({count}) should be less frequent than rank 1 ({top})",
                index + 1
            );
        }
    }

    #[test]
    fn zipf_near_zero_exponent_approaches_uniform() {
        let sampler = ZipfSampler::new(100, 0.01);
        let counts = rank_counts(&sampler, 100, 10_000, 42);
        for (index, count) in counts.iter().enumerate() {
            assert!(
                (50..=200).contains(count),
                "rank {} count {count} outside loose uniform bounds",
                index + 1
            );
        }
    }

    #[test]
    fn zipf_sample_covers_boundary_draws() {
        let sampler = ZipfSampler::new(100, 1.1);
        check!(sampler.sample(0.0) == 1);
        check!(sampler.sample(1.0) == 100);
    }

    #[test]
    fn pick_class_is_exactly_proportional_over_the_draw_space() {
        let cases = [
            (MixSpec {
                single_shard_insert: 6,
                cross_shard_txn: 0,
                read_only: 3,
                contended_update: 1,
            }),
            (MixSpec {
                single_shard_insert: 0,
                cross_shard_txn: 5,
                read_only: 0,
                contended_update: 5,
            }),
            (MixSpec {
                single_shard_insert: 1,
                cross_shard_txn: 1,
                read_only: 1,
                contended_update: 1,
            }),
        ];
        for mix in cases {
            let mut counts = [0_u64; 4];
            for draw in 0..mix.total_weight() {
                counts[class_index(pick_class(&mix, draw))] += 1;
            }
            let expected = [
                u64::from(mix.single_shard_insert),
                u64::from(mix.cross_shard_txn),
                u64::from(mix.read_only),
                u64::from(mix.contended_update),
            ];
            assert!(counts == expected, "mix {mix:?}");
        }
    }

    #[test]
    fn pick_class_never_selects_zero_weight_classes() {
        let mix = MixSpec {
            single_shard_insert: 0,
            cross_shard_txn: 7,
            read_only: 0,
            contended_update: 3,
        };
        let mut rng = SmallRng::seed_from_u64(11);
        for _ in 0..10_000 {
            let class = pick_class(&mix, rng.random_range(0..mix.total_weight()));
            assert!(
                class == OpClass::CrossShardTxn || class == OpClass::ContendedUpdate,
                "zero-weight class {class:?} was chosen"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn token_bucket_grants_about_rate_per_virtual_second() {
        let bucket = Arc::new(TokenBucket::new(100));
        let granted = Arc::new(AtomicU64::new(0));
        let task = tokio::spawn({
            let bucket = Arc::clone(&bucket);
            let granted = Arc::clone(&granted);
            async move {
                loop {
                    bucket.acquire().await;
                    granted.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        let count = granted.load(Ordering::Relaxed);
        task.abort();
        assert!(
            (99..=101).contains(&count),
            "expected ~100 permits in one virtual second, got {count}"
        );
    }

    #[test]
    fn summarize_converts_a_known_distribution_to_milliseconds() {
        let mut histogram = new_histogram();
        for us in [1_000_u64, 2_000, 3_000, 4_000, 5_000] {
            record_us(&mut histogram, us);
        }
        let summary = summarize(&histogram);
        assert!(summary.count == 5);
        check!(
            (summary.mean_ms - 3.0).abs() < 0.05,
            "mean {}",
            summary.mean_ms
        );
        check!(
            (summary.p50_ms - 3.0).abs() < 0.05,
            "p50 {}",
            summary.p50_ms
        );
        check!(
            (summary.p95_ms - 5.0).abs() < 0.05,
            "p95 {}",
            summary.p95_ms
        );
        check!(
            (summary.p99_ms - 5.0).abs() < 0.05,
            "p99 {}",
            summary.p99_ms
        );
        check!(
            (summary.p999_ms - 5.0).abs() < 0.05,
            "p999 {}",
            summary.p999_ms
        );
        check!(
            (summary.max_ms - 5.0).abs() < 0.05,
            "max {}",
            summary.max_ms
        );
    }

    #[test]
    fn classifier_retries_only_serialization_failures() {
        check!(
            classify_sqlstate(&SqlState::T_R_SERIALIZATION_FAILURE) == FailureKind::Serialization
        );
        let non_retryable = [
            SqlState::UNIQUE_VIOLATION,
            SqlState::SYNTAX_ERROR,
            SqlState::T_R_DEADLOCK_DETECTED,
            SqlState::UNDEFINED_TABLE,
        ];
        for code in non_retryable {
            assert!(
                classify_sqlstate(&code) == FailureKind::Other,
                "{} should be non-retryable",
                code.code()
            );
        }
    }

    #[test]
    fn classifier_maps_transport_kinds() {
        let cases = [
            (
                Some(IoErrorKind::ConnectionRefused),
                FailureKind::Unavailable,
            ),
            (Some(IoErrorKind::TimedOut), FailureKind::Unavailable),
            (Some(IoErrorKind::WouldBlock), FailureKind::Unavailable),
            (
                Some(IoErrorKind::ConnectionReset),
                FailureKind::ConnectionLost,
            ),
            (
                Some(IoErrorKind::ConnectionAborted),
                FailureKind::ConnectionLost,
            ),
            (Some(IoErrorKind::BrokenPipe), FailureKind::ConnectionLost),
            (
                Some(IoErrorKind::UnexpectedEof),
                FailureKind::ConnectionLost,
            ),
            (Some(IoErrorKind::NotConnected), FailureKind::ConnectionLost),
            (None, FailureKind::Other),
        ];
        for (kind, expected) in cases {
            assert!(classify_transport(kind) == expected, "kind {kind:?}");
        }
    }

    #[test]
    fn table_ids_place_ranges_and_hot_table() {
        let cases: [(u16, &[i64], i64); 4] = [
            (1, &[0], 1),
            (2, &[0, 1_000_000], 1_000_001),
            (3, &[0, 1_000_000, 2_000_000], 2_000_001),
            (
                8,
                &[
                    0, 1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000, 6_000_000, 7_000_000,
                ],
                7_000_001,
            ),
        ];
        for (ranges, expected_range_ids, expected_hot) in cases {
            let range_ids: Vec<i64> = (0..ranges).map(range_table_id).collect();
            assert!(range_ids == expected_range_ids, "ranges {ranges}");
            let hot = hot_table_id(ranges);
            assert!(hot == expected_hot, "ranges {ranges}");
            let last_span_start = i64::from(ranges - 1) * RANGE_SPAN;
            assert!(
                hot >= last_span_start && hot < last_span_start + RANGE_SPAN,
                "hot id {hot} outside last range span for {ranges} ranges"
            );
            assert!(!range_ids.contains(&hot));
        }
    }

    #[test]
    fn insert_ids_use_disjoint_worker_spaces_within_int4() {
        check!(insert_id(0, 1) == 1);
        check!(insert_id(1, 0) == 1_000_000);
        check!(insert_id(3, 5) == 3_000_005);
        // Counter wraps within the worker's span.
        check!(insert_id(0, 1_000_001) == 1);
        // Worker ids wrap so the value stays within int4.
        check!(insert_id(2_000, 5) == 5);
        check!(insert_id(1_999, 999_999) == 1_999_999_999);
        check!(i64::from(i32::MAX) > insert_id(1_999, 999_999));
    }

    #[test]
    fn seed_statements_batch_hot_rows() {
        check!(seed_statements(1_000_001, 0).is_empty());

        let statements = seed_statements(1_000_001, 1_201);
        assert!(statements.len() == 3);
        let total_rows: usize = statements
            .iter()
            .map(|statement| statement.matches(", 0)").count())
            .sum();
        assert!(total_rows == 1_201);
        for statement in &statements {
            check!(statement.starts_with("INSERT INTO t1000001 VALUES ("));
            check!(statement.matches(", 0)").count() <= 500);
        }
        check!(statements[0].contains("(1, 0)"));
        check!(statements[2].ends_with("(1201, 0)"));
    }

    fn test_layout(ranges: u16) -> TableLayout {
        TableLayout {
            ranges,
            hot_table_id: hot_table_id(ranges),
            connections: 4,
        }
    }

    #[test]
    fn single_shard_plan_targets_the_only_range_table() {
        let mut rng = SmallRng::seed_from_u64(0);
        let zipf = ZipfSampler::new(10, 1.1);
        let plan = build_plan(
            OpClass::SingleShardInsert,
            &test_layout(1),
            7,
            42,
            &zipf,
            &mut rng,
        );
        let expected = format!("INSERT INTO t0 VALUES ({})", insert_id(7, 42));
        assert!(plan == OpPlan::Statement(expected));
    }

    #[test]
    fn cross_shard_plan_writes_two_distinct_range_tables() {
        let mut rng = SmallRng::seed_from_u64(1);
        let zipf = ZipfSampler::new(10, 1.1);
        for counter in 0..50 {
            let plan = build_plan(
                OpClass::CrossShardTxn,
                &test_layout(3),
                2,
                counter,
                &zipf,
                &mut rng,
            );
            assert!(let OpPlan::Transaction(statements) = plan);
            assert!(statements.len() == 2);
            let tables: Vec<&str> = statements
                .iter()
                .map(|statement| {
                    statement
                        .strip_prefix("INSERT INTO ")
                        .and_then(|rest| rest.split_once(' '))
                        .map(|(table, _)| table)
                        .expect("insert statement shape")
                })
                .collect();
            assert!(tables[0] != tables[1], "tables must differ: {tables:?}");
            for table in &tables {
                assert!(
                    ["t0", "t1000000", "t2000000"].contains(table),
                    "unexpected table {table}"
                );
            }
            let value = insert_id(2, counter);
            for statement in &statements {
                check!(statement.ends_with(&format!("VALUES ({value})")));
            }
        }
    }

    #[test]
    fn read_only_plan_scans_a_bounded_slice() {
        let mut rng = SmallRng::seed_from_u64(2);
        let zipf = ZipfSampler::new(10, 1.1);
        let plan = build_plan(OpClass::ReadOnly, &test_layout(1), 0, 0, &zipf, &mut rng);
        assert!(let OpPlan::Statement(sql) = plan);
        let rest = sql
            .strip_prefix("SELECT count(*) FROM t0 WHERE id >= ")
            .expect("read-only statement shape");
        let (low, high) = rest.split_once(" AND id < ").expect("two bounds");
        let low: i64 = low.parse().expect("low bound");
        let high: i64 = high.parse().expect("high bound");
        assert!(high - low == READ_SLICE_ROWS);
        assert!(low >= 0);
    }

    #[test]
    fn contended_update_plan_targets_the_hot_table_by_zipf_rank() {
        let mut rng = SmallRng::seed_from_u64(3);
        let zipf = ZipfSampler::new(100, 1.1);
        for _ in 0..50 {
            let plan = build_plan(
                OpClass::ContendedUpdate,
                &test_layout(4),
                0,
                0,
                &zipf,
                &mut rng,
            );
            assert!(let OpPlan::Statement(sql) = plan);
            let rank: u32 = sql
                .strip_prefix("UPDATE t3000001 SET v = v + 1 WHERE id = ")
                .expect("update statement shape")
                .parse()
                .expect("rank literal");
            assert!((1..=100).contains(&rank));
        }
    }

    fn mix_of(write_insert: u32, cross: u32, read: u32, contended: u32) -> MixSpec {
        MixSpec {
            single_shard_insert: write_insert,
            cross_shard_txn: cross,
            read_only: read,
            contended_update: contended,
        }
    }

    fn full_mix() -> MixSpec {
        mix_of(4, 3, 2, 1)
    }

    #[test]
    fn mix_predicates_identify_needed_connections() {
        let cases = [
            (mix_of(1, 0, 0, 0), true, false),
            (mix_of(0, 1, 0, 0), true, false),
            (mix_of(0, 0, 0, 1), true, false),
            (mix_of(0, 0, 1, 0), false, true),
            (mix_of(4, 3, 2, 1), true, true),
            (mix_of(0, 0, 0, 0), false, false),
        ];
        for (mix, needs_write, needs_read) in cases {
            assert!(
                mix_needs_write_connection(&mix) == needs_write,
                "write predicate for {mix:?}"
            );
            assert!(
                mix_needs_read_connection(&mix) == needs_read,
                "read predicate for {mix:?}"
            );
        }
    }

    #[test]
    fn route_worker_pins_writes_to_node_zero_and_spreads_reads() {
        let cases = [
            (0, 1, 0, true),
            (2, 1, 0, true),
            (0, 3, 0, true),
            (1, 3, 1, false),
            (2, 3, 2, false),
            (3, 3, 0, true),
            (5, 4, 1, false),
        ];
        for (worker, endpoint_count, read_index, shared) in cases {
            let routing = route_worker(worker, endpoint_count, &full_mix());
            let expected = WorkerRouting {
                write_endpoint: Some(0),
                read_endpoint: Some(read_index),
                shared,
            };
            assert!(
                routing == expected,
                "worker {worker} over {endpoint_count} endpoints"
            );
        }
    }

    #[test]
    fn route_worker_omits_connections_the_mix_never_uses() {
        let cases = [
            // Pure-read mixes open no write connection.
            (mix_of(0, 0, 1, 0), 0, None, Some(0), false),
            (mix_of(0, 0, 1, 0), 1, None, Some(1), false),
            // Pure-write mixes open no read connection.
            (mix_of(1, 0, 0, 0), 1, Some(0), None, false),
            (mix_of(0, 1, 0, 0), 2, Some(0), None, false),
            (mix_of(0, 0, 0, 1), 1, Some(0), None, false),
        ];
        for (mix, worker, write_endpoint, read_endpoint, shared) in cases {
            let routing = route_worker(worker, 3, &mix);
            let expected = WorkerRouting {
                write_endpoint,
                read_endpoint,
                shared,
            };
            assert!(routing == expected, "worker {worker} with mix {mix:?}");
        }
    }

    fn test_endpoints(count: u16) -> Vec<SqlEndpoint> {
        (0..count)
            .map(|node| SqlEndpoint {
                addr: format!("127.0.0.1:{}", 5_000 + node).parse().expect("addr"),
                user: "crab".to_owned(),
                password: String::new(),
                database: "crab".to_owned(),
            })
            .collect()
    }

    #[test]
    fn shared_routes_hold_one_connection_and_reads_fall_back_to_it() {
        let endpoints = test_endpoints(3);
        let mut shared = build_connections(&endpoints, route_worker(0, 3, &full_mix()));
        assert!(shared.write.is_some());
        assert!(
            shared.read.is_none(),
            "shared route must not open a second connection"
        );
        let slot = shared.slot_for(OpClass::ReadOnly).expect("read slot");
        check!(slot.endpoint.addr == endpoints[0].addr);

        let mut split = build_connections(&endpoints, route_worker(1, 3, &full_mix()));
        let read = split.slot_for(OpClass::ReadOnly).expect("read slot");
        check!(read.endpoint.addr == endpoints[1].addr);
        for class in [
            OpClass::SingleShardInsert,
            OpClass::CrossShardTxn,
            OpClass::ContendedUpdate,
        ] {
            let write = split.slot_for(class).expect("write slot");
            check!(write.endpoint.addr == endpoints[0].addr, "class {class:?}");
        }

        let mut read_only = build_connections(&endpoints, route_worker(2, 3, &mix_of(0, 0, 1, 0)));
        assert!(read_only.write.is_none());
        let slot = read_only.slot_for(OpClass::ReadOnly).expect("read slot");
        check!(slot.endpoint.addr == endpoints[2].addr);
        assert!(read_only.slot_for(OpClass::SingleShardInsert).is_none());
    }
}
