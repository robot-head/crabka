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
//! batched multi-row `INSERT`s. Each table is preceded by a
//! `DROP TABLE IF EXISTS`, so re-runs against a persistent external system
//! (`run --external`) start from a clean slate; against a freshly-launched
//! crabka cluster the drops are no-ops.
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
//! Every gateway routes DDL and DML for ranges it does not host locally, so
//! no endpoint is special: worker `w` holds one connection to
//! `endpoints[w % endpoint_count]` and issues its whole mix — writes and
//! reads alike — through it, fanning load round-robin across every node's
//! SQL front door. Each worker reconnects with independent backoff, and an
//! endpoint's initial unavailability is counted and retried like any
//! mid-run connection error.
//!
//! # Pacing, faults, and measurement
//!
//! [`run`] drives one tokio task per configured connection. Workers pace
//! through a shared token bucket under [`RateSpec::Fixed`] and free-run
//! under [`RateSpec::Saturate`]. Connection loss triggers
//! reconnect-with-backoff forever (faults are expected to kill links
//! mid-run), and a per-operation timeout turns a blackholed link into an
//! `unavailable` count instead of a hung worker. After the workload's
//! `warmup` of unrecorded load, its `duration` window records per-class HDR
//! histograms (1µs..60s, 3 significant figures), commit/failure counters,
//! the error taxonomy, and a per-second timeline; in-flight operations get a
//! short grace period to finish before workers are aborted.

use std::{
    collections::BTreeMap,
    error::Error as _,
    io::ErrorKind as IoErrorKind,
    mem,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use anyhow::Context as _;
use crabka_units::{fmt::Human as _, prelude::*};
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
const OP_TIMEOUT: Time = secs(30);
/// Timeout for a single connection attempt.
const CONNECT_TIMEOUT: Time = secs(5);
/// Total budget for the schema-preparation connection.
const STARTUP_DEADLINE: Time = secs(30);
/// Pause between schema-preparation connection attempts.
const STARTUP_RETRY_DELAY: Time = millis(250);
/// How long in-flight operations may finish after the window closes.
const SHUTDOWN_GRACE: Time = secs(5);
/// First reconnect backoff step (doubles up to [`RECONNECT_BACKOFF_MAX`]).
const RECONNECT_BACKOFF_MIN: Time = millis(100);
/// Reconnect backoff cap.
const RECONNECT_BACKOFF_MAX: Time = secs(2);
/// Shortest latency the per-class histograms resolve.
const HISTOGRAM_MIN: Time = micros(1);
/// Longest latency the per-class histograms record; anything slower clamps
/// to it.
const HISTOGRAM_MAX: Time = secs(60);
/// Floor on a pacing wait, so a huge configured rate cannot spin the token
/// bucket on sub-microsecond sleeps.
const MIN_PACING_WAIT: Time = micros(500);

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
    /// Actual measured wall-clock extent.
    pub measured_wall: Time,
}

/// Creates the workload schema and seed rows through one node.
///
/// Any node's front door works: gateways route DDL and DML to every range
/// engine, and DDL returns only after the cluster-wide catalog barrier, so
/// the schema is visible everywhere once this returns.
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
    for statement in schema_statements(topology.ranges, workload.hot_rows) {
        client
            .simple_query(&statement)
            .await
            .with_context(|| format!("prepare schema: {statement}"))?;
    }
    Ok(())
}

/// Every statement [`prepare_schema`] issues, in order: per range table a
/// `DROP TABLE IF EXISTS` then its `CREATE TABLE`, the same pair for the
/// hot table, then the hot-table seed `INSERT`s. Statements are issued one
/// at a time — each in its own implicit transaction — because external
/// targets (`run --external`) may not accept DDL inside a multi-statement
/// batch.
fn schema_statements(ranges: u16, hot_rows: u32) -> Vec<String> {
    let mut statements = Vec::new();
    for range in 0..ranges {
        let table_id = range_table_id(range);
        statements.push(format!("DROP TABLE IF EXISTS t{table_id}"));
        statements.push(format!("CREATE TABLE t{table_id} (id int4)"));
    }
    let hot_id = hot_table_id(ranges);
    statements.push(format!("DROP TABLE IF EXISTS t{hot_id}"));
    statements.push(format!("CREATE TABLE t{hot_id} (id int4, v int4)"));
    statements.extend(seed_statements(hot_id, hot_rows));
    statements
}

/// Runs warmup then the measured window against the given SQL endpoints
/// (workers are spread round-robin across them).
///
/// # Errors
///
/// Returns an error only on harness-level failures (no endpoints to drive);
/// workload-level errors are counted in the outcome, because faults are
/// expected to cause them.
pub async fn run(
    endpoints: &[SqlEndpoint],
    workload: &WorkloadSpec,
    topology: &TopologySpec,
) -> anyhow::Result<WorkloadOutcome> {
    anyhow::ensure!(!endpoints.is_empty(), "no SQL endpoints to drive");
    let context = Arc::new(build_context(workload, topology));
    let (stop_tx, stop_rx) = watch::channel(false);
    let handles: Vec<_> = (0..workload.connections)
        .map(|worker| {
            let endpoint = endpoints[route_worker(worker, endpoints.len())].clone();
            tokio::spawn(worker_loop(
                Arc::clone(&context),
                worker,
                ConnectionSlot::new(endpoint),
                stop_rx.clone(),
            ))
        })
        .collect();
    drop(stop_rx);

    tokio::time::sleep(workload.warmup.to_std()).await;
    context.stats.recording.store(true, Ordering::SeqCst);
    let window_start = Instant::now();
    let mut timeline = Vec::new();
    for elapsed_secs in 0..workload.duration.secs_i64().max(0) {
        tokio::time::sleep_until(window_start + Time::from_secs(elapsed_secs + 1).to_std()).await;
        let accum = context.stats.drain_second();
        timeline.push(SecondSample {
            t: Time::from_secs(elapsed_secs),
            committed: accum.committed,
            errors: accum.errors,
            mean_latency: mean_latency(&accum),
        });
    }
    let measured_wall = window_start.elapsed().as_time();

    let _ = stop_tx.send(true);
    let abort_handles: Vec<_> = handles
        .iter()
        .map(tokio::task::JoinHandle::abort_handle)
        .collect();
    if tokio::time::timeout(SHUTDOWN_GRACE.to_std(), future::join_all(handles))
        .await
        .is_err()
    {
        for abort in &abort_handles {
            abort.abort();
        }
    }
    context.stats.recording.store(false, Ordering::SeqCst);
    Ok(build_outcome(&context.stats, measured_wall, timeline))
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
            RateSpec::Fixed { target_rate } => Some(TokenBucket::new(target_rate)),
        },
        stats: Stats::new(),
    }
}

/// The endpoint index a worker's connection dials: round-robin over every
/// node's front door, for writes and reads alike (any gateway routes DML
/// and DDL for ranges it does not host).
fn route_worker(worker: u32, endpoint_count: usize) -> usize {
    usize::try_from(worker).unwrap_or(usize::MAX) % endpoint_count.max(1)
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
///
/// Tokens are dimensionless permits, so the refill is the [`Ratio`] of a
/// measured extent to the bucket's rate and the wait is the deficit divided
/// by that same rate — both checked by the compiler rather than by a
/// hand-written seconds multiply.
#[derive(Debug)]
struct TokenBucket {
    state: Mutex<BucketState>,
    rate: Frequency,
    capacity: f64,
}

#[derive(Debug)]
struct BucketState {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(target_rate: Frequency) -> Self {
        let rate = target_rate.max(per_sec(1));
        Self {
            state: Mutex::new(BucketState {
                tokens: 0.0,
                last_refill: Instant::now(),
            }),
            rate,
            capacity: rate.per_sec_f64(),
        }
    }

    async fn acquire(&self) {
        loop {
            let wait = {
                let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                let now = Instant::now();
                let elapsed = now.duration_since(state.last_refill).as_time();
                state.last_refill = now;
                let earned: Ratio = elapsed * self.rate;
                state.tokens = (state.tokens + earned.as_f64()).min(self.capacity);
                if state.tokens >= 1.0 {
                    state.tokens -= 1.0;
                    None
                } else {
                    let deficit: Ratio = fraction(1.0 - state.tokens);
                    let wait: Time = deficit / self.rate;
                    Some(wait.max(MIN_PACING_WAIT))
                }
            };
            match wait {
                None => return,
                Some(wait) => tokio::time::sleep(wait.to_std()).await,
            }
        }
    }
}

/// One second of accumulated progress for the timeline.
#[derive(Debug, Clone, Copy)]
struct SecondAccum {
    committed: u64,
    errors: u64,
    latency_sum: Time,
    completions: u64,
}

impl Default for SecondAccum {
    fn default() -> Self {
        Self {
            committed: 0,
            errors: 0,
            latency_sum: Time::ZERO,
            completions: 0,
        }
    }
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

    fn record_commit(&self, class: OpClass, elapsed: Time) {
        if !self.recording() {
            return;
        }
        self.committed.fetch_add(1, Ordering::Relaxed);
        record_latency(&mut lock(&self.histograms[class_index(class)]), elapsed);
        let mut second = lock(&self.second);
        second.committed += 1;
        second.completions += 1;
        second.latency_sum += elapsed;
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
/// (see `crates/bench-driver/src/hist.rs`). The recorder counts whole
/// microseconds, which is the unit the bounds convert into.
fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(
        histogram_micros(HISTOGRAM_MIN),
        histogram_micros(HISTOGRAM_MAX),
        3,
    )
    .expect("histogram bounds are valid")
}

/// A histogram bound as the whole microseconds `hdrhistogram` counts in.
fn histogram_micros(bound: Time) -> u64 {
    u64::try_from(bound.micros_i64()).unwrap_or(u64::MAX).max(1)
}

/// Records one latency sample, clamped into the histogram's range so a
/// single outlier cannot blow up the recorder.
fn record_latency(histogram: &mut Histogram<u64>, latency: Time) {
    let value = histogram_micros(latency).clamp(histogram.low(), histogram.high());
    let _ = histogram.record(value);
}

/// A histogram reading, which `hdrhistogram` reports in the microseconds it
/// was fed, as a latency.
fn micros_reading(micros_count: f64) -> Time {
    micros(1) * micros_count
}

fn count_to_f64(count: u64) -> f64 {
    f64::from(u32::try_from(count).unwrap_or(u32::MAX))
}

fn summarize(histogram: &Histogram<u64>) -> LatencySummary {
    let quantile = |q: f64| micros_reading(count_to_f64(histogram.value_at_quantile(q)));
    LatencySummary {
        count: histogram.len(),
        mean: micros_reading(histogram.mean()),
        p50: quantile(0.50),
        p95: quantile(0.95),
        p99: quantile(0.99),
        p999: quantile(0.999),
        max: micros_reading(count_to_f64(histogram.max())),
    }
}

fn mean_latency(accum: &SecondAccum) -> Option<Time> {
    (accum.completions > 0).then(|| accum.latency_sum / count_to_f64(accum.completions))
}

fn build_outcome(
    stats: &Stats,
    measured_wall: Time,
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
        measured_wall,
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
    let deadline = Instant::now() + STARTUP_DEADLINE.to_std();
    loop {
        let last_error =
            match tokio::time::timeout(CONNECT_TIMEOUT.to_std(), connect(endpoint)).await {
                Ok(Ok(client)) => return Ok(client),
                Ok(Err(error)) => error.to_string(),
                Err(_) => "connect timed out".to_owned(),
            };
        anyhow::ensure!(
            Instant::now() < deadline,
            "endpoint {} did not accept a connection within {} (last error: {last_error})",
            endpoint.addr,
            STARTUP_DEADLINE.human()
        );
        tokio::time::sleep(STARTUP_RETRY_DELAY.to_std()).await;
    }
}

/// One (re)connectable client with its own reconnect backoff.
struct ConnectionSlot {
    endpoint: SqlEndpoint,
    client: Option<Client>,
    backoff: Time,
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
            match tokio::time::timeout(CONNECT_TIMEOUT.to_std(), connect(&self.endpoint)).await {
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
        self.backoff = (self.backoff * 2.0).min(RECONNECT_BACKOFF_MAX);
    }

    fn drop_client(&mut self) {
        self.client = None;
    }
}

/// One worker: pick a class, run it on the worker's connection
/// (reconnecting with backoff as needed, forever), until told to stop.
async fn worker_loop(
    context: Arc<WorkerContext>,
    worker: u32,
    mut slot: ConnectionSlot,
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
        let Some(client) = slot
            .ensure_connected(worker, &context.stats, &mut stop)
            .await
        else {
            continue;
        };
        let started = Instant::now();
        match run_op(client, &plan, &context.stats, &mut rng).await {
            OpResult::Committed => context
                .stats
                .record_commit(class, started.elapsed().as_time()),
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
        match tokio::time::timeout(OP_TIMEOUT.to_std(), execute_plan(client, plan)).await {
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
                        let backoff = millis(retries * 2 + rng.random_range(0..3_u32));
                        tokio::time::sleep(backoff.to_std()).await;
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

async fn sleep_with_stop(delay: Time, stop: &mut watch::Receiver<bool>) {
    tokio::select! {
        () = tokio::time::sleep(delay.to_std()) => {}
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
        let bucket = Arc::new(TokenBucket::new(per_sec(100)));
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
        tokio::time::sleep(secs(1).to_std()).await;
        let count = granted.load(Ordering::Relaxed);
        task.abort();
        assert!(
            (99..=101).contains(&count),
            "expected ~100 permits in one virtual second, got {count}"
        );
    }

    #[test]
    fn summarize_reports_the_recorded_distribution_as_latencies() {
        let mut histogram = new_histogram();
        for sample in [millis(1), millis(2), millis(3), millis(4), millis(5)] {
            record_latency(&mut histogram, sample);
        }
        let summary = summarize(&histogram);
        assert!(summary.count == 5);
        // The histogram is 3-significant-figure, so every reading lands
        // within a bucket width of the exact value.
        let tolerance = micros(50);
        let cases = [
            ("mean", summary.mean, millis(3)),
            ("p50", summary.p50, millis(3)),
            ("p95", summary.p95, millis(5)),
            ("p99", summary.p99, millis(5)),
            ("p99.9", summary.p999, millis(5)),
            ("max", summary.max, millis(5)),
        ];
        for (label, got, expected) in cases {
            check!(
                (got - expected).abs() < tolerance,
                "{label} {}",
                got.human()
            );
        }
    }

    /// HDR buckets are three-significant-figure wide, so a reading at the
    /// histogram ceiling lands just above the configured bound.
    const BUCKET_SLACK: Ratio = percent(1);

    #[test]
    fn latency_recording_clamps_outliers_into_the_histogram_range() {
        let mut histogram = new_histogram();
        record_latency(&mut histogram, nanos(1));
        record_latency(&mut histogram, hours(1));
        let summary = summarize(&histogram);
        assert!(summary.count == 2);
        // A sub-resolution sample is pulled up to the floor and an
        // hour-long one pushed down to the ceiling, so neither escapes the
        // recorder's range.
        check!(summary.p50 >= HISTOGRAM_MIN);
        let ceiling = HISTOGRAM_MAX * (1.0 + BUCKET_SLACK.as_f64());
        check!(summary.max <= ceiling, "max {}", summary.max.human());
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
    fn schema_statements_drop_each_table_before_creating_it_then_seed() {
        // (ranges, hot_rows, expected DDL prefix of the statement list).
        let cases: [(u16, u32, &[&str]); 3] = [
            (
                1,
                0,
                &[
                    "DROP TABLE IF EXISTS t0",
                    "CREATE TABLE t0 (id int4)",
                    "DROP TABLE IF EXISTS t1",
                    "CREATE TABLE t1 (id int4, v int4)",
                ],
            ),
            (
                2,
                3,
                &[
                    "DROP TABLE IF EXISTS t0",
                    "CREATE TABLE t0 (id int4)",
                    "DROP TABLE IF EXISTS t1000000",
                    "CREATE TABLE t1000000 (id int4)",
                    "DROP TABLE IF EXISTS t1000001",
                    "CREATE TABLE t1000001 (id int4, v int4)",
                ],
            ),
            (
                3,
                1,
                &[
                    "DROP TABLE IF EXISTS t0",
                    "CREATE TABLE t0 (id int4)",
                    "DROP TABLE IF EXISTS t1000000",
                    "CREATE TABLE t1000000 (id int4)",
                    "DROP TABLE IF EXISTS t2000000",
                    "CREATE TABLE t2000000 (id int4)",
                    "DROP TABLE IF EXISTS t2000001",
                    "CREATE TABLE t2000001 (id int4, v int4)",
                ],
            ),
        ];
        for (ranges, hot_rows, expected_ddl) in cases {
            let statements = schema_statements(ranges, hot_rows);
            let expected_seeds = seed_statements(hot_table_id(ranges), hot_rows);
            let mut expected: Vec<String> = expected_ddl.iter().map(|s| (*s).to_owned()).collect();
            expected.extend(expected_seeds);
            assert!(
                statements == expected,
                "ranges {ranges}, hot_rows {hot_rows}"
            );
        }
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

    #[test]
    fn route_worker_spreads_workers_round_robin_across_endpoints() {
        let cases = [
            (0, 1, 0),
            (2, 1, 0),
            (0, 3, 0),
            (1, 3, 1),
            (2, 3, 2),
            (3, 3, 0),
            (5, 4, 1),
        ];
        for (worker, endpoint_count, expected) in cases {
            assert!(
                route_worker(worker, endpoint_count) == expected,
                "worker {worker} over {endpoint_count} endpoints"
            );
        }
    }
}
