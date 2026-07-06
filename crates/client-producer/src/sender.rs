//! Background sender task. Drains ready batches from every accumulator
//! and ships them as `ProduceRequest`s through `crabka-client-core`.
//!
//! The sender is `tokio::spawn`'d by the builder. It owns the `wake_rx`
//! `Receiver` end of the wake channel (the `Producer` holds the
//! `wake_tx` `Sender`), the `flush_notify`, the `accumulators` map, and
//! the `next_seq` map. On every linger tick or wake signal it walks the
//! accumulators, seals + drains batches, and builds a v2 `RecordBatch` per
//! batch (allocating its `base_sequence`). Each batch becomes its own
//! single-partition `ProduceRequest`, sent via `Client::broker(id)` — falling
//! back to the bootstrap `Client::send` when the leader is unknown — with all
//! of a cycle's requests sent **concurrently** to keep every broker busy.
//!
//! ## Per-partition pipelining (idempotence-critical)
//!
//! Brokers stay busy because independent partitions send **concurrently** — up
//! to [`SenderConfig::max_in_flight`] Produce requests overlap on the wire per
//! drain cycle. But each *single* partition keeps **at most one** request in
//! flight ([`MAX_IN_FLIGHT_PER_PARTITION`]): its next batch is not drained until
//! the previous one is acked. That makes per-partition idempotent
//! `base_sequence` ordering hold **by construction** — the broker never sees two
//! outstanding sequences for one partition, so concurrently-issued requests
//! cannot reach it out of `base_sequence` order and trip
//! `OUT_OF_ORDER_SEQUENCE_NUMBER`.
//!
//! Recovery is correspondingly simple: a batch that fails (transport error,
//! routing miss, or a defensive `OUT_OF_ORDER`) is parked in its partition's
//! single **retry slot** and resent verbatim — same allocated `base_sequence`,
//! same bytes, so a re-landed write is deduped by the broker via
//! `DUPLICATE_SEQUENCE_NUMBER` — ahead of any new batch for that partition, on
//! the next cycle. The retry slots persist across cycles (owned by [`run`]).

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use crabka_protocol::{
    owned::{
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid,
    records::{Attributes, Record, RecordBatch, RecordHeader},
};
use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::{
    accumulator::{Accumulator, InProgressBatch, PendingRecord},
    compression::Compression,
    error::ProducerError,
    partitioner::UniformStickyPartitioner,
    producer::{Acks, STATE_ACTIVE, STATE_FENCED, TopicMetadata},
    record::RecordMetadata,
    transactional::TxnState,
    transport::ProduceTransport,
};

/// Wire error codes referenced when interpreting `PartitionProduceResponse`.
mod codes {
    pub const NONE: i16 = 0;
    /// The Produce reached a broker that does not lead the partition (stale
    /// routing). Refresh metadata, re-resolve the leader, and retry.
    pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    /// The Produce reached a broker that does not lead the partition. With
    /// rf=1 a misroute to a non-hosting broker surfaces as
    /// `UNKNOWN_TOPIC_OR_PARTITION`; a misroute to a follower surfaces here.
    pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
    pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
    pub const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
    /// `INVALID_PRODUCER_EPOCH` per the canonical Apache Kafka table (code 47).
    pub const INVALID_PRODUCER_EPOCH: i16 = 47;
}

/// Synthetic leader id meaning "leader unknown → use the bootstrap connection".
/// Matches the consumer's convention (`poll.rs`): a partition whose leader id is
/// `< 0` or whose advertised address the pool can't dial falls back to the
/// bootstrap `Client::send` rather than `Client::broker(id)`.
const BOOTSTRAP_LEADER: i32 = -1;

/// Transport attempts to a specific leader before re-routing. `1` means: on the
/// first failure, re-resolve immediately rather than burning more
/// `request_timeout`s on a leader that has likely moved (failover). A transient
/// blip (socket dropped, broker still alive) is handled just as cheaply — the
/// re-route re-resolves to the same alive leader and reconnects — so paying
/// multiple full request-timeouts here only slows failover recovery.
const TRANSPORT_RETRIES: i32 = 1;

/// Wall-clock budget for routing a batch to a reachable leader, measured from
/// its first send and preserved across resends. Spans a typical failover leader
/// re-election (the broker session timeout is single-digit seconds) with
/// margin, then fails the still-unroutable batch so the caller's ack/`flush`
/// resolves instead of hanging forever. Enforced per cycle in
/// [`collect_retries`] as a resend batch is about to be re-sent, so a batch that
/// keeps bouncing between leaders gives up by ~30s rather than resending
/// indefinitely.
const ROUTING_RETRY_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

/// Maximum Produce requests in flight **per partition** at once.
///
/// Pinned to `1`: a partition's next batch is not sent until its previous batch
/// is acked. This preserves idempotent per-partition `base_sequence` ordering
/// *by construction* — the broker only ever sees one sequence outstanding for a
/// partition, so there is no window in which concurrently-issued requests can
/// reach the broker out of `base_sequence` order and trip
/// `OUT_OF_ORDER_SEQUENCE_NUMBER`.
///
/// ## Why not `> 1` (same-partition pipelining)?
///
/// The previous design drained up to `max_in_flight` batches per partition and
/// fired them via `futures::future::join_all`. But [`crabka_client_core::Client`]'s
/// `send` writes the request frame **and** awaits its response in a single
/// future; when several same-partition futures are polled concurrently their
/// frame writes race on the connection's writer channel, so the broker can
/// receive `base_sequence` 16 before 0. The broker rejects the gap with
/// `OUT_OF_ORDER_SEQUENCE_NUMBER`, the producer resends — concurrently again —
/// re-triggering the reorder. Under sustained load this livelocks: some batch
/// never converges, its records' ack-oneshots never resolve, and the caller
/// hangs.
///
/// True same-partition pipelining (`> 1`) requires a client-core API that
/// guarantees **ordered frame writes** for a partition's in-flight requests
/// (write 0, 1, 2 to the wire in order, then await their responses
/// concurrently) — e.g. a pipelined `Connection::send_batch` or a write-then-await
/// split. That is deferred; until it exists, one-in-flight-per-partition is the
/// only ordering-safe option. Cross-partition pipelining is unaffected:
/// independent partitions still send concurrently, bounded by
/// [`SenderConfig::max_in_flight`].
const MAX_IN_FLIGHT_PER_PARTITION: usize = 1;

// The one-slot-per-partition pipeline (a single retry slot per partition, no
// ordered drain) is only sound while a partition never has more than one request
// outstanding. If this is ever raised above `1`, that model is insufficient — a
// partition could have several outstanding sequences needing an ordered drain —
// and the recovery path must be redesigned. Enforce the dependency at compile
// time so the assumption can't silently drift.
const _: () = assert!(
    MAX_IN_FLIGHT_PER_PARTITION == 1,
    "the one-slot retry model requires exactly one in-flight request per partition",
);

/// All the bits of state the sender task needs. The builder constructs
/// one of these, hands it to [`run`], and drops it.
#[allow(clippy::type_complexity)] // accumulators map mirrors the Producer field; alias deferred.
pub(crate) struct SenderConfig {
    /// Broker-facing transport (real `Client` in production, a deterministic
    /// in-process broker model in tests). See [`crate::transport`].
    pub transport: Box<dyn ProduceTransport>,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub acks: Acks,
    pub compression: Compression,
    pub linger: Duration,
    pub request_timeout: Duration,
    pub retry_backoff: Duration,
    /// Maximum number of Produce requests fired **concurrently per drain
    /// cycle**, across all partitions — the cross-partition / per-connection
    /// pipelining bound (Kafka's `max.in.flight.requests.per.connection`).
    /// Per-partition in-flight is separately pinned to
    /// [`MAX_IN_FLIGHT_PER_PARTITION`] (`1`) for ordering; this bounds how many
    /// *distinct partitions'* requests overlap on the wire at once.
    pub max_in_flight: usize,
    pub metadata_cache: Arc<Mutex<HashMap<String, TopicMetadata>>>,
    /// Per-`(topic, partition)` leader-id cache, shared with the `Producer`.
    /// Populated from `Metadata` (see `Producer::partitions_for`); the sender
    /// consults it to route each Produce to the partition leader and refreshes
    /// it on `NOT_LEADER_OR_FOLLOWER` / `UNKNOWN_TOPIC_OR_PARTITION`.
    pub partition_leaders: Arc<DashMap<(String, i32), i32>>,
    /// Shared null-key sticky partitioner. Rotated when the sender seals a
    /// topic batch so subsequent keyless records fan out across partitions.
    pub partitioner: Arc<UniformStickyPartitioner>,
    pub accumulators: Arc<DashMap<(String, i32), Arc<Mutex<Accumulator>>>>,
    pub next_seq: Arc<DashMap<(String, i32), i32>>,
    pub state: Arc<AtomicU8>,
    pub wake_rx: tokio::sync::mpsc::Receiver<()>,
    pub flush_notify: Arc<Notify>,
    /// Shared with `Producer`; tracks batches popped from an accumulator that
    /// are still being sent so `flush` can wait for them. See the field doc on
    /// [`crate::producer::Producer`].
    pub in_flight: Arc<AtomicUsize>,
    pub shutdown: CancellationToken,
    /// `transactional_id` from the producer config; `None` for non-transactional producers.
    pub transactional_id: Option<String>,
    /// Shared with `Producer`; the sender snapshots this at send time to decide
    /// whether to stamp batches as transactional.
    pub txn_state: Arc<Mutex<TxnState>>,
    /// Shared with `Producer`; holds the `(producer_id, producer_epoch)` assigned
    /// by the transaction coordinator via `InitProducerId`. The sender reads this
    /// when stamping transactional batches.
    pub txn_pid_epoch: Arc<Mutex<(i64, i16)>>,
}

/// Mutable per-partition pipeline state, owned by [`run`] and threaded into
/// every [`drain_once`] so it persists across drain cycles.
///
/// With [`MAX_IN_FLIGHT_PER_PARTITION`] pinned to `1`, the only state a
/// partition can carry between cycles is a single failed batch awaiting a
/// verbatim resend — there is never more than one request outstanding, so there
/// is nothing to "drain" and no resend *set* to order. Hence exactly one slot
/// per partition.
#[derive(Default)]
struct PipelineState {
    /// Per-`(topic, partition)` retry slot: a batch that failed its last send
    /// and must be resent verbatim (same `base_sequence`, same bytes) ahead of
    /// any new batch for that partition. Already counted in `in_flight` (counted
    /// when first drained from the accumulator), so it is NOT re-counted on
    /// resend. Presence means the partition's single in-flight slot is occupied.
    retry: HashMap<(String, i32), PreparedBatch>,
}

#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(producer_id = cfg.producer_id, max_in_flight = cfg.max_in_flight),
)]
pub(crate) async fn run(mut cfg: SenderConfig) {
    let mut ticker = tokio::time::interval(cfg.linger.max(Duration::from_millis(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut state = PipelineState::default();

    loop {
        tokio::select! {
            () = cfg.shutdown.cancelled() => break,
            _ = ticker.tick() => {
                drain_once(&mut cfg, &mut state).await;
            }
            _ = cfg.wake_rx.recv() => {
                drain_once(&mut cfg, &mut state).await;
            }
        }
    }

    // Drain anything left when we shut down so `close()` doesn't drop records.
    drain_once(&mut cfg, &mut state).await;
}

/// One drained partition's batch, prepared for sending: the encoded v2
/// `RecordBatch` (with its `base_sequence` already allocated), the topic id, and
/// the `PendingRecord`s whose oneshot acks the response resolves.
///
/// The `record_batch` is built **once** (sequence allocated once) so a
/// re-route or resend ships the identical bytes — preserving per-partition
/// idempotent sequencing: the leader sees each partition's `base_sequence`
/// exactly once, in increasing order, regardless of which broker it reaches.
struct PreparedBatch {
    topic: String,
    partition: i32,
    topic_id: Uuid,
    /// The allocated base sequence for this batch. Cached here (rather than
    /// re-read from `record_batch.base_sequence`) so it is unambiguous for a
    /// transactional batch and so debug logging can name the batch.
    base_sequence: i32,
    record_batch: RecordBatch,
    records: Vec<PendingRecord>,
    /// Wall-clock time the batch was first handed to the transport. Set on the
    /// first send and preserved across resends so the routing-retry budget
    /// (`ROUTING_RETRY_BUDGET`) is measured from the first attempt, not the
    /// most recent — a batch that keeps failing to route gives up by ~30s.
    first_sent: Option<Instant>,
    /// When `Some`, the batch is backing off after a **transport/connection**
    /// failure and must not be resent until this instant. This keeps a leader
    /// whose pod is down and refusing connections from hot-looping the drain
    /// every linger tick. Routing redirects (`NOT_LEADER` / `UNKNOWN`) leave
    /// this `None` so they resend immediately at the freshly-resolved leader.
    backoff_until: Option<Instant>,
}

/// One drain cycle. Builds the send list — each partition's pending resend
/// first, then one newly-drained batch for each *idle* partition — sends every
/// batch as its own single-partition `ProduceRequest` **concurrently**, then
/// dispatches each [`BatchVerdict`] (ack / park-for-resend / terminal-fail /
/// fence).
///
/// **Per-partition ordering / idempotence.** A partition contributes at most one
/// batch per cycle: either its pending resend (held in [`PipelineState::retry`])
/// *or* one new batch when idle, never both — the `occupied` set enforces the
/// "never both". So the broker never sees two outstanding sequences for a
/// partition, and a failing partition can never interleave a fresh batch ahead
/// of its pending resend. Each batch's `record_batch` (hence `base_sequence`) is
/// built once and resent verbatim, so the leader sees each sequence exactly
/// once, in order — ordering preserved by construction.
///
/// `in_flight` accounting: `fetch_add` only when a NEW batch is drained from an
/// accumulator (resends were counted when first drained); `fetch_sub` only when
/// a batch reaches a terminal outcome (ack, terminal failure, fence, or routing
/// budget exhausted). `flush_notify` is woken when `in_flight` hits zero and
/// when there is nothing to send.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(batches = tracing::field::Empty),
)]
async fn drain_once(cfg: &mut SenderConfig, state: &mut PipelineState) {
    let now = Instant::now();

    // 1. Resends first: each partition's single failed batch must precede any
    //    new batch for that partition. `collect_retries` drains the retry slots
    //    and returns batches whose routing budget elapsed, which we fail here
    //    (their in-flight slot was counted at first drain, so `finish_in_flight`
    //    once).
    let (mut to_send, expired) = collect_retries(&mut state.retry, now);
    for pb in expired {
        fail_batch(
            pb.records,
            ProducerError::Server(codes::NOT_LEADER_OR_FOLLOWER),
        );
        finish_in_flight(cfg);
    }

    // A partition with a pending resend is "occupied": it must not also send a
    // new batch (two same-partition requests on the wire could reorder and trip
    // `OUT_OF_ORDER_SEQUENCE_NUMBER`). Its next batch waits in the accumulator
    // until the resend acks and the slot frees. This covers both batches
    // resending this cycle (`to_send`) and ones still parked in their retry slot
    // backing off (`state.retry`) — a backed-off slot is still occupied.
    let mut occupied: HashSet<(String, i32)> = to_send
        .iter()
        .map(|pb| (pb.topic.clone(), pb.partition))
        .chain(state.retry.keys().cloned())
        .collect();

    // 2. One new batch per idle partition. Across partitions we fan out
    //    concurrently, but bound the cycle's total fan-out to `max_in_flight`
    //    (the per-connection pipelining bound); partitions not reached this cycle
    //    are picked up on the next linger tick (their retry slots carry forward,
    //    so none is starved). `in_flight` is incremented per new batch while the
    //    accumulator lock is held, so a concurrent `flush` never sees a batch
    //    that is neither in the accumulator nor counted in flight.
    let keys: Vec<(String, i32)> = cfg.accumulators.iter().map(|e| e.key().clone()).collect();
    for key in keys {
        if to_send.len() >= cfg.max_in_flight {
            break;
        }
        // A partition with a resend in flight keeps its one slot; skip it.
        if occupied.contains(&key) {
            continue;
        }
        let acc = match cfg.accumulators.get(&key) {
            Some(a) => Arc::clone(a.value()),
            None => continue,
        };
        // Seal the in-progress batch, then take a single ready batch.
        {
            let mut a = acc.lock().await;
            let had_current = a.current.as_ref().is_some_and(|b| !b.is_empty());
            a.seal_current();
            if had_current && let Some(num_partitions) = topic_partition_count(cfg, &key.0).await {
                cfg.partitioner.rotate(&key.0, num_partitions);
            }
        }
        let batch = {
            let mut a = acc.lock().await;
            let b = a.ready.pop_front();
            if b.is_some() {
                cfg.in_flight.fetch_add(1, Ordering::AcqRel);
            }
            b
        };
        let Some(batch) = batch else { continue };
        let mut pb = prepare_batch(cfg, &key.0, key.1, batch).await;
        pb.first_sent = Some(now);
        occupied.insert(key);
        to_send.push(pb);
    }

    tracing::Span::current().record("batches", to_send.len());
    if to_send.is_empty() {
        cfg.flush_notify.notify_waiters();
        return;
    }

    // 3. Send every batch concurrently, then apply each verdict to its window.
    send_batches(cfg, state, to_send).await;
}

/// Drain the per-partition retry slots into an ordered send list (one-slot
/// model). Each partition holds **at most one** failed batch awaiting a verbatim
/// resend; a batch whose routing budget ([`ROUTING_RETRY_BUDGET`], measured from
/// its first send) has elapsed is split off into `expired` for the caller to
/// fail instead of resending. Every resent batch keeps its allocated
/// `base_sequence` and bytes (the broker dedups a re-landed write via
/// `DUPLICATE_SEQUENCE_NUMBER`); `first_sent` is initialized defensively if unset.
///
/// Pure over the retry map (no `Client`, no I/O) so the budget-expiry logic is
/// unit-testable without a broker.
fn collect_retries(
    retry: &mut HashMap<(String, i32), PreparedBatch>,
    now: Instant,
) -> (Vec<PreparedBatch>, Vec<PreparedBatch>) {
    let mut to_send: Vec<PreparedBatch> = Vec::new();
    let mut expired: Vec<PreparedBatch> = Vec::new();
    // Batches still backing off after a transport failure are re-parked here so
    // a down/refusing leader doesn't hot-loop the drain on every linger tick.
    let mut parked: Vec<((String, i32), PreparedBatch)> = Vec::new();

    for (key, mut pb) in retry.drain() {
        if pb
            .first_sent
            .is_some_and(|t| now.duration_since(t) >= ROUTING_RETRY_BUDGET)
        {
            expired.push(pb);
            continue;
        }
        // Honour a retry backoff: the batch waits in its slot until its
        // `backoff_until` passes. Set after a transport failure and after a
        // routing rejection (NOT_LEADER / UNKNOWN_TOPIC) so a partition that is
        // still settling at cold boot isn't hammered in a tight resend loop.
        if pb.backoff_until.is_some_and(|t| now < t) {
            parked.push((key, pb));
            continue;
        }
        if pb.first_sent.is_none() {
            pb.first_sent = Some(now);
        }
        pb.backoff_until = None;
        to_send.push(pb);
    }

    for (key, pb) in parked {
        retry.insert(key, pb);
    }

    (to_send, expired)
}

/// Per-batch verdict consumed by [`send_batches`] in the one-slot model: the
/// broker durably accepted it, it must be resent verbatim, it failed terminally
/// with a server code, or it fatally fenced the producer.
#[derive(Debug, Clone, Copy, PartialEq)]
enum BatchVerdict {
    /// Durably written (`NONE`) or already present (`DUPLICATE_SEQUENCE_NUMBER`).
    Acked { base_offset: i64 },
    /// Resend verbatim next cycle (transport failure, `OUT_OF_ORDER`, or routing).
    Retry,
    /// Terminal but non-fatal server error — fail the records with `Server(code)`.
    Terminal(i16),
    /// Fatal idempotence failure (`INVALID_PRODUCER_EPOCH`) — fence the producer.
    Fence,
}

/// Classification of a per-partition `error_code`: either a direct
/// [`BatchVerdict`], or [`Classification::Routing`] (`NOT_LEADER`/`UNKNOWN` — a
/// retry, plus the leader-hint adoption / metadata refresh side effects applied
/// by [`interpret_response`]). Kept separate so the pure code→verdict mapping is
/// unit-testable without a `Client`.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Classification {
    Verdict(BatchVerdict),
    Routing,
}

/// Map a per-partition `error_code` (and the broker's `base_offset`) to its
/// [`Classification`]. Pure (no I/O).
fn classify_verdict(error_code: i16, base_offset: i64) -> Classification {
    match error_code {
        // The broker durably wrote the batch (NONE) or already had it
        // (DUPLICATE_SEQUENCE_NUMBER returns the same base_offset) — ack either.
        codes::NONE | codes::DUPLICATE_SEQUENCE_NUMBER => {
            Classification::Verdict(BatchVerdict::Acked { base_offset })
        }
        // A gap from an earlier failed send: resend this batch verbatim (same
        // base_sequence) once the partition's slot is free. With one in-flight
        // per partition this is rare, but handled identically to a transport
        // failure for safety.
        codes::OUT_OF_ORDER_SEQUENCE_NUMBER => Classification::Verdict(BatchVerdict::Retry),
        codes::INVALID_PRODUCER_EPOCH => Classification::Verdict(BatchVerdict::Fence),
        codes::NOT_LEADER_OR_FOLLOWER | codes::UNKNOWN_TOPIC_OR_PARTITION => {
            Classification::Routing
        }
        // Any other code is terminal-but-not-fatal: fail the records with
        // Server(code); never fence.
        code => Classification::Verdict(BatchVerdict::Terminal(code)),
    }
}

/// Resolve a partition's leader id from the cache. Returns [`BOOTSTRAP_LEADER`]
/// when the leader is unknown (`< 0`), uncached, or the pool has no dialable
/// address for it (e.g. a port-0 in-process test broker) — those cases fall
/// back to the bootstrap connection.
fn resolve_leader(cfg: &SenderConfig, topic: &str, partition: i32) -> i32 {
    match cfg
        .partition_leaders
        .get(&(topic.to_string(), partition))
        .map(|e| *e.value())
    {
        Some(id) if id >= 0 && cfg.transport.knows_broker(id) => id,
        _ => BOOTSTRAP_LEADER,
    }
}

async fn topic_partition_count(cfg: &SenderConfig, topic: &str) -> Option<i32> {
    cfg.metadata_cache
        .lock()
        .await
        .get(topic)
        .and_then(|meta| positive_partition_count(meta.num_partitions))
}

fn positive_partition_count(count: i32) -> Option<i32> {
    (count > 0).then_some(count)
}

/// Build the v2 `RecordBatch` for a drained partition batch, allocating its
/// `base_sequence` range from `next_seq`. The result is sent (and any retry
/// resent) verbatim, so the sequence is allocated exactly once per batch.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(
        topic = %topic,
        partition,
        batch_records = batch.records.len(),
        base_sequence = tracing::field::Empty,
    ),
)]
async fn prepare_batch(
    cfg: &SenderConfig,
    topic: &str,
    partition: i32,
    batch: InProgressBatch,
) -> PreparedBatch {
    // Allocate the base_sequence range for this batch (once).
    let base_sequence = {
        let mut entry = cfg
            .next_seq
            .entry((topic.to_string(), partition))
            .or_insert(0);
        let cur = *entry;
        let count = i32::try_from(batch.records.len()).unwrap_or(i32::MAX);
        *entry = cur.wrapping_add(count);
        cur
    };
    tracing::Span::current().record("base_sequence", base_sequence);

    // Resolve the topic_id from the metadata cache (zero is fine — the broker
    // falls back to the `name` field for v ≤ 12).
    let topic_id = cfg
        .metadata_cache
        .lock()
        .await
        .get(topic)
        .map_or(Uuid::ZERO, |m| m.topic_id);

    // Snapshot the transactional pid/epoch once per batch.
    let txn_snapshot = txn_pid_snapshot(cfg).await;

    let record_batch = build_record_batch(cfg, &batch, base_sequence, txn_snapshot);

    PreparedBatch {
        topic: topic.to_string(),
        partition,
        topic_id,
        base_sequence,
        record_batch,
        records: batch.records,
        first_sent: None,
        backoff_until: None,
    }
}

/// One batch's send result: the batch, its [`BatchVerdict`], and whether a
/// metadata refresh is needed before any resend can route correctly.
struct BatchSendResult {
    pb: PreparedBatch,
    verdict: BatchVerdict,
    /// A metadata refresh is required before a resend can route correctly (the
    /// partition came back mis-routed with no usable inline leader hint, or the
    /// hinted leader's address is unknown).
    refresh_needed: bool,
}

/// Send every batch in `to_send` as its own single-partition `ProduceRequest`,
/// **concurrently**, then dispatch each [`BatchVerdict`]: ack the records, fail
/// them terminally, park the batch in its partition's retry slot for a verbatim
/// resend next cycle, or fence the producer.
///
/// Concurrency is cross-partition request pipelining: every batch in `to_send`
/// is for a *distinct* partition ([`drain_once`]'s `occupied` set guarantees it),
/// and the brokers are independent — so overlapping the round-trips keeps every
/// broker busy without ever putting two same-partition requests on the wire, and
/// per-partition ordering is undisturbed. Futures are polled on this one task
/// (no spawn), so they share `&cfg` safely.
#[tracing::instrument(level = "debug", skip_all, fields(batches = to_send.len()))]
async fn send_batches(cfg: &SenderConfig, state: &mut PipelineState, to_send: Vec<PreparedBatch>) {
    let mut results: FuturesUnordered<_> = to_send
        .into_iter()
        .map(|pb| send_one_batch(cfg, pb))
        .collect();

    let mut needs_refresh = false;
    let mut fenced: Option<Vec<PreparedBatch>> = None;
    while let Some(res) = results.next().await {
        let BatchSendResult {
            pb,
            verdict,
            refresh_needed,
        } = res;
        needs_refresh |= refresh_needed;

        if let Some(to_fail) = &mut fenced {
            to_fail.push(pb);
            continue;
        }

        match verdict {
            // Durable: resolve the records with their offsets, free the slot.
            BatchVerdict::Acked { base_offset } => ack_batch(cfg, pb, base_offset),
            // Terminal server error: fail the records, free the slot.
            BatchVerdict::Terminal(code) => terminal_fail_batch(cfg, pb, code),
            // Retriable (transport / routing / defensive OUT_OF_ORDER): park in
            // the partition's single retry slot, resent verbatim next cycle. The
            // batch is still outstanding, so its in-flight slot stays counted —
            // no `finish_in_flight` here.
            BatchVerdict::Retry => {
                tracing::debug!(
                    topic = %pb.topic,
                    partition = pb.partition,
                    base_sequence = pb.base_sequence,
                    "parking batch for verbatim resend",
                );
                state.retry.insert((pb.topic.clone(), pb.partition), pb);
            }
            // Fatal idempotence failure. Fail this batch plus every batch we have
            // not yet processed (their in-flight slots are counted, so they must
            // be released), then fence the producer and stop sending.
            BatchVerdict::Fence => {
                fenced = Some(vec![pb]);
            }
        }
    }

    if let Some(to_fail) = fenced {
        fence(cfg, state, to_fail);
        return;
    }

    if needs_refresh {
        update_leaders_from_metadata(cfg).await;
    }
}

/// Ack a batch's records with their broker-assigned offsets and release its
/// in-flight slot. `base_offset + offset_delta` is the per-record offset.
fn ack_batch(cfg: &SenderConfig, pb: PreparedBatch, base_offset: i64) {
    let partition = pb.partition;
    for r in pb.records {
        let _ = r.ack.send(Ok(RecordMetadata {
            topic_index: 0,
            partition,
            offset: base_offset + i64::from(r.offset_delta),
            timestamp_ms: r.timestamp_ms,
        }));
    }
    finish_in_flight(cfg);
}

/// Terminally fail a batch the broker rejected with an unmodeled error code,
/// resolving its records with `Server(code)` and releasing the in-flight slot.
/// This is the single owner of the slot release for the batch.
fn terminal_fail_batch(cfg: &SenderConfig, pb: PreparedBatch, code: i16) {
    fail_batch(pb.records, ProducerError::Server(code));
    finish_in_flight(cfg);
}

/// Fence the producer: mark `STATE_FENCED`, fail `to_fail` (this cycle's still-
/// live batches), every batch parked in a retry slot, and everything in the
/// accumulators with `FencedProducer`, releasing each in-flight slot. Called on
/// a fatal idempotence failure (`INVALID_PRODUCER_EPOCH`).
fn fence(cfg: &SenderConfig, state: &mut PipelineState, to_fail: Vec<PreparedBatch>) {
    cfg.state
        .compare_exchange(
            STATE_ACTIVE,
            STATE_FENCED,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .ok();

    // Fail every batch from this cycle we were still holding.
    for batch in to_fail {
        fail_batch(batch.records, ProducerError::FencedProducer);
        finish_in_flight(cfg);
    }
    // Fail everything parked in the retry slots; the producer is dead.
    for (_, batch) in state.retry.drain() {
        fail_batch(batch.records, ProducerError::FencedProducer);
        finish_in_flight(cfg);
    }

    // Fail anything still sitting in the accumulators (current + ready) so no
    // caller's oneshot hangs. We use try_lock to avoid blocking — a record
    // being appended concurrently will observe STATE_FENCED on its next send.
    for entry in cfg.accumulators.iter() {
        if let Ok(mut a) = entry.value().try_lock() {
            if let Some(b) = a.current.take() {
                fail_batch(b.records, ProducerError::FencedProducer);
            }
            while let Some(b) = a.ready.pop_front() {
                fail_batch(b.records, ProducerError::FencedProducer);
            }
        }
    }
}

/// The instant a transport-failed batch becomes eligible to resend: `now` plus
/// the configured `retry_backoff`. Pulled out so the offset direction (the
/// deadline must be in the *future*) is unit-testable.
fn backoff_deadline(now: Instant, retry_backoff: Duration) -> Instant {
    now + retry_backoff
}

/// Send a single batch as its own single-partition `ProduceRequest`, resolving
/// its transport/broker result to a [`BatchVerdict`]. The batch is returned
/// alongside the verdict (the caller still owns its records). On a connection
/// error the broker is evicted so a reconnect targets its current address.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(
        topic = %pb.topic,
        partition = pb.partition,
        base_sequence = pb.base_sequence,
        leader = tracing::field::Empty,
    ),
)]
async fn send_one_batch(cfg: &SenderConfig, mut pb: PreparedBatch) -> BatchSendResult {
    // A batch prepared before its topic existed (cold-boot race: the WAL topic's
    // leadership is still settling) carries a ZERO `topic_id`. Produce v≥13 keys
    // topics by id and drops the name on the wire, so a ZERO id makes the broker
    // return an un-correlatable UNKNOWN_TOPIC response that the sender retries
    // forever. Re-resolve the id from the metadata cache (which `partitions_for`
    // backfills once the topic exists) before each (verbatim) resend, so the
    // SAME batch converges in place — same `base_sequence`, so the idempotent
    // sequence stays gap-free and no records are dropped.
    if pb.topic_id == Uuid::ZERO
        && let Some(resolved) = cfg
            .metadata_cache
            .lock()
            .await
            .get(&pb.topic)
            .map(|m| m.topic_id)
    {
        pb.topic_id = resolved;
    }

    let leader = resolve_leader(cfg, &pb.topic, pb.partition);
    tracing::Span::current().record("leader", leader);
    let req = build_single_batch_request(cfg, &pb);

    let route = if leader == BOOTSTRAP_LEADER {
        None
    } else {
        Some(leader)
    };

    let mut attempts: i32 = 0;
    let resp: ProduceResponse = loop {
        attempts += 1;
        let send = cfg.transport.send_produce(route, req.clone()).await;
        match send {
            Ok(r) => break r,
            Err(e) => {
                // The cached connection is likely dead (broker bounced / failed
                // over). Evict it so a reconnect targets the broker's current
                // address; never evict the shared bootstrap connection.
                if leader != BOOTSTRAP_LEADER {
                    cfg.transport.evict_broker(leader);
                }
                if attempts >= TRANSPORT_RETRIES {
                    tracing::warn!(
                        leader,
                        partition = pb.partition,
                        base_sequence = pb.base_sequence,
                        error = %e,
                        "produce to leader failed {attempts}×; will re-route",
                    );
                    // Transport failure → retry (park in the retry slot, resend
                    // verbatim). Back off before the resend so a down/refusing
                    // leader isn't hammered every linger tick, and force a
                    // metadata refresh so the resend re-resolves to whatever
                    // leader the cluster (re-)elected.
                    pb.backoff_until = Some(backoff_deadline(Instant::now(), cfg.retry_backoff));
                    return BatchSendResult {
                        pb,
                        verdict: BatchVerdict::Retry,
                        refresh_needed: true,
                    };
                }
                tracing::warn!(leader, error = %e, "produce attempt {attempts} failed; reconnecting");
                tokio::time::sleep(cfg.retry_backoff).await;
            }
        }
    };

    interpret_response(cfg, pb, &resp)
}

/// Interpret a single-partition `ProduceResponse` into a [`BatchSendResult`].
/// Applies the routing case's leader-hint side effects; the pure code→verdict
/// mapping lives in [`classify_verdict`].
fn interpret_response(
    cfg: &SenderConfig,
    mut pb: PreparedBatch,
    resp: &ProduceResponse,
) -> BatchSendResult {
    let part_resp = resp
        .responses
        .iter()
        .find(|t| t.name == pb.topic || (pb.topic_id != Uuid::ZERO && t.topic_id == pb.topic_id))
        .and_then(|t| {
            t.partition_responses
                .iter()
                .find(|p| p.index == pb.partition)
        });

    let Some(part_resp) = part_resp else {
        // No matching partition in the response: treat as a retriable failure so
        // the batch resends verbatim rather than being dropped. This happens at
        // cold boot when a Produce for a not-yet-existing topic comes back as
        // UNKNOWN_TOPIC with an empty (name="", topic_id=ZERO) identity that
        // can't be correlated to our batch; the resend re-resolves `topic_id`
        // (see `send_one_batch`) once the topic exists.
        tracing::debug!(
            topic = %pb.topic,
            partition = pb.partition,
            base_sequence = pb.base_sequence,
            "produce response carried no matching partition; resending"
        );
        pb.backoff_until = Some(backoff_deadline(Instant::now(), cfg.retry_backoff));
        return BatchSendResult {
            pb,
            verdict: BatchVerdict::Retry,
            refresh_needed: true,
        };
    };

    // Surface the actual broker error code (and any inline leader hint) for a
    // rejected produce, so a retry loop can be diagnosed from the code rather
    // than inferred from the resend pattern. Error-gated to stay off the
    // happy-path hot loop; enable with RUST_LOG=crabka_client_producer=debug.
    if part_resp.error_code != 0 {
        tracing::debug!(
            topic = %pb.topic,
            partition = pb.partition,
            base_sequence = pb.base_sequence,
            error_code = part_resp.error_code,
            base_offset = part_resp.base_offset,
            leader_hint = part_resp.current_leader.leader_id,
            "produce partition rejected"
        );
    }
    match classify_verdict(part_resp.error_code, part_resp.base_offset) {
        Classification::Verdict(verdict) => {
            // Back off before a verbatim resend (e.g. a defensive OUT_OF_ORDER)
            // so a partition that keeps rejecting isn't hammered in a tight loop.
            if matches!(verdict, BatchVerdict::Retry) {
                pb.backoff_until = Some(backoff_deadline(Instant::now(), cfg.retry_backoff));
            }
            BatchSendResult {
                pb,
                verdict,
                refresh_needed: false,
            }
        }
        Classification::Routing => {
            // Adopt any inline leader hint immediately; otherwise (or if the
            // hinted leader's address is unknown) force a metadata refresh. The
            // batch resends verbatim, so sequencing stays monotonic.
            let hint = part_resp.current_leader.leader_id;
            let refresh_needed = if hint >= 0 {
                cfg.partition_leaders
                    .insert((pb.topic.clone(), pb.partition), hint);
                !cfg.transport.knows_broker(hint)
            } else {
                true
            };
            // Back off before re-routing. A NOT_LEADER/UNKNOWN_TOPIC at cold boot
            // (the partition's leader/writer-actor still settling) otherwise spins
            // a tight refresh+resend loop that hammers the broker and can itself
            // starve the reconcile that would make the partition writable —
            // leaving the producer stuck (observed: traces/logs WAL never advances
            // on some cold boots). Backing off lets the partition become ready.
            pb.backoff_until = Some(backoff_deadline(Instant::now(), cfg.retry_backoff));
            BatchSendResult {
                pb,
                verdict: BatchVerdict::Retry,
                refresh_needed,
            }
        }
    }
}

/// Build a single-partition, single-batch `ProduceRequest`. Transactional state
/// is read from the batch's own attributes (set at build time), so the
/// request-level `transactional_id` matches the batch exactly.
fn build_single_batch_request(cfg: &SenderConfig, pb: &PreparedBatch) -> ProduceRequest {
    let is_txn = pb.record_batch.attributes.is_transactional();
    let req_txn_id = if is_txn {
        cfg.transactional_id.clone()
    } else {
        None
    };

    ProduceRequest {
        transactional_id: req_txn_id,
        acks: cfg.acks.wire(),
        timeout_ms: i32::try_from(cfg.request_timeout.as_millis()).unwrap_or(i32::MAX),
        topic_data: vec![TopicProduceData {
            name: pb.topic.clone(),
            topic_id: pb.topic_id,
            partition_data: vec![PartitionProduceData {
                index: pb.partition,
                records: Some(pb.record_batch.clone().into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Refresh cluster metadata and adopt the fresh partition→leader map. The
/// refresh also re-populates the pool's broker-address registry, so a leader
/// re-elected onto a broker the pool hadn't dialed becomes routable.
#[tracing::instrument(level = "debug", skip_all)]
async fn update_leaders_from_metadata(cfg: &SenderConfig) {
    if let Ok(md) = cfg.transport.refresh_metadata().await {
        // Hold the cache lock across the loop so a tracked topic's correction is
        // applied atomically alongside the leader-map update.
        let mut cache = cfg.metadata_cache.lock().await;
        for t in &md.topics {
            let Some(name) = &t.name else { continue };
            if t.error_code != 0 {
                continue;
            }
            for p in &t.partitions {
                cfg.partition_leaders
                    .insert((name.clone(), p.partition_index), p.leader_id);
            }
            // Correct a previously-cached unresolved entry now that the topic
            // exists. `partitions_for` caches `{count: 1, topic_id: ZERO}` when a
            // produce races ahead of the topic's creation at cold boot; left
            // frozen, that ZERO `topic_id` makes a v≥13 Produce (name dropped on
            // the wire) come back as an un-correlatable UNKNOWN_TOPIC response the
            // sender retries forever. Refreshing the id here lets a parked batch
            // backfill it on resend (see `send_one_batch`). Only update topics we
            // already track, so a full-cluster refresh doesn't bloat the cache.
            if let Some(entry) = cache.get_mut(name) {
                entry.num_partitions = i32::try_from(t.partitions.len()).unwrap_or(1).max(1);
                entry.topic_id = t.topic_id;
            }
        }
    }
}

/// Decrement `in_flight` for a completed batch, waking any `flush` waiter when
/// it was the last one outstanding.
fn finish_in_flight(cfg: &SenderConfig) {
    if cfg.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
        cfg.flush_notify.notify_waiters();
    }
}

/// Snapshot the transactional `(producer_id, producer_epoch)` if and only if
/// the producer is currently inside an active transaction.
///
/// Returns `Some((pid, epoch))` when a transactional batch should be emitted,
/// `None` for non-transactional or out-of-transaction sends.
async fn txn_pid_snapshot(cfg: &SenderConfig) -> Option<(i64, i16)> {
    cfg.transactional_id.as_ref()?;
    let state = *cfg.txn_state.lock().await;
    if state == TxnState::InTransaction {
        Some(*cfg.txn_pid_epoch.lock().await)
    } else {
        None
    }
}

/// Build a v2 `RecordBatch` from a sealed `InProgressBatch`. Compression
/// is encoded into the batch attributes; the actual compress step runs
/// inside `RecordBatch::encode`.
///
/// `txn_snapshot` is `Some((pid, epoch))` when the batch is being sent
/// inside an active transaction. In that case the `is_transactional`
/// attribute bit is set and the txn-coordinator-assigned pid/epoch are
/// used instead of the idempotence pid/epoch.
fn build_record_batch(
    cfg: &SenderConfig,
    batch: &InProgressBatch,
    base_sequence: i32,
    txn_snapshot: Option<(i64, i16)>,
) -> RecordBatch {
    let is_transactional = txn_snapshot.is_some();
    let attributes = Attributes::default()
        .with_compression(cfg.compression.compression_type())
        .with_transactional(is_transactional);

    // Use the txn pid/epoch when inside a transaction; fall back to the
    // idempotence pid/epoch for non-transactional batches.
    let (producer_id, producer_epoch) =
        txn_snapshot.unwrap_or((cfg.producer_id, cfg.producer_epoch));

    let base_timestamp = batch.records.first().map_or(0, |r| r.timestamp_ms);
    let max_timestamp = batch
        .records
        .iter()
        .map(|r| r.timestamp_ms)
        .max()
        .unwrap_or(0);
    let last_offset_delta =
        i32::try_from(batch.records.len().saturating_sub(1)).unwrap_or(i32::MAX);

    let mut records: Vec<Record> = Vec::with_capacity(batch.records.len());
    for r in &batch.records {
        let headers: Vec<RecordHeader> = r
            .headers
            .iter()
            .map(|h| RecordHeader {
                key: h.key.clone(),
                value: h.value.clone(),
            })
            .collect();
        records.push(Record {
            attributes: 0,
            timestamp_delta: r.timestamp_ms - base_timestamp,
            offset_delta: r.offset_delta,
            key: r.key.clone(),
            value: r.value.clone(),
            headers,
        });
    }

    RecordBatch {
        base_offset: 0,
        partition_leader_epoch: 0,
        attributes,
        last_offset_delta,
        base_timestamp,
        max_timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        records,
    }
}

/// Resolve every record in `records` with an error.
///
/// `ClientError`, `Protocol`, and `Compression` are not `Clone`, so only
/// the first record receives the real error for those variants. Trivially
/// cloneable variants (`Server`, `FencedProducer`, `Closed`, etc.) are
/// propagated to every record so callers see the true error code.
fn fail_batch(records: Vec<PendingRecord>, err: ProducerError) {
    fn clone_if_possible(e: &ProducerError) -> Option<ProducerError> {
        match e {
            ProducerError::Server(c) => Some(ProducerError::Server(*c)),
            ProducerError::FencedProducer => Some(ProducerError::FencedProducer),
            ProducerError::Closed => Some(ProducerError::Closed),
            ProducerError::FlushTimeout => Some(ProducerError::FlushTimeout),
            ProducerError::BufferFull => Some(ProducerError::BufferFull),
            ProducerError::BatchTooLarge { batch_size } => Some(ProducerError::BatchTooLarge {
                batch_size: *batch_size,
            }),
            ProducerError::RecordTooLarge { record_size } => Some(ProducerError::RecordTooLarge {
                record_size: *record_size,
            }),
            ProducerError::InvalidConfig(s) => Some(ProducerError::InvalidConfig(s)),
            _ => None, // Client, Protocol, Compression — not Clone.
        }
    }

    let clone = clone_if_possible(&err);
    let mut iter = records.into_iter();
    if let Some(first) = iter.next() {
        let _ = first.ack.send(Err(err));
    }
    for r in iter {
        let e = clone
            .as_ref()
            .and_then(clone_if_possible)
            .unwrap_or(ProducerError::Closed);
        let _ = r.ack.send(Err(e));
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use tokio::sync::oneshot;

    use super::*;

    /// Build a `PreparedBatch` for `(topic, partition)` with `base_sequence` and
    /// a single record, returning the batch and the record's ack receiver so a
    /// test can observe how it resolves.
    fn prepared(
        topic: &str,
        partition: i32,
        base_sequence: i32,
        first_sent: Option<Instant>,
    ) -> (
        PreparedBatch,
        oneshot::Receiver<Result<RecordMetadata, ProducerError>>,
    ) {
        let (tx, rx) = oneshot::channel();
        let record = PendingRecord {
            offset_delta: 0,
            timestamp_ms: 0,
            key: None,
            value: None,
            headers: Vec::new(),
            ack: tx,
        };
        let pb = PreparedBatch {
            topic: topic.to_string(),
            partition,
            topic_id: Uuid::ZERO,
            base_sequence,
            record_batch: RecordBatch {
                base_offset: 0,
                partition_leader_epoch: 0,
                attributes: Attributes::default(),
                last_offset_delta: 0,
                base_timestamp: 0,
                max_timestamp: 0,
                producer_id: 1,
                producer_epoch: 0,
                base_sequence,
                records: Vec::new(),
            },
            records: vec![record],
            first_sent,
            backoff_until: None,
        };
        (pb, rx)
    }

    #[test]
    fn classify_verdict_maps_codes() {
        for (code, base_offset, want) in [
            (
                codes::NONE,
                42,
                Classification::Verdict(BatchVerdict::Acked { base_offset: 42 }),
            ),
            // DUPLICATE is acked like a success (broker already wrote it).
            (
                codes::DUPLICATE_SEQUENCE_NUMBER,
                7,
                Classification::Verdict(BatchVerdict::Acked { base_offset: 7 }),
            ),
            (
                codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
                0,
                Classification::Verdict(BatchVerdict::Retry),
            ),
            (
                codes::INVALID_PRODUCER_EPOCH,
                0,
                Classification::Verdict(BatchVerdict::Fence),
            ),
            (codes::NOT_LEADER_OR_FOLLOWER, 0, Classification::Routing),
            (
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                0,
                Classification::Routing,
            ),
            // An arbitrary server error (MESSAGE_TOO_LARGE = 10) is terminal-but-
            // not-fatal: fail the records with Server(10), never fence.
            (10, 0, Classification::Verdict(BatchVerdict::Terminal(10))),
        ] {
            assert!(classify_verdict(code, base_offset) == want);
        }
    }

    #[test]
    fn collect_retries_splits_expired_and_drains_map() {
        // Two partitions, each holding one retry batch (one slot per partition).
        // The batch past its routing budget is split off as expired; the recent
        // one is returned to send. The map is fully drained either way.
        let mut retry: HashMap<(String, i32), PreparedBatch> = HashMap::new();
        let long_ago = Instant::now()
            .checked_sub(ROUTING_RETRY_BUDGET + Duration::from_secs(1))
            .expect("instant in range");
        let (old, _rx_old) = prepared("t", 0, 0, Some(long_ago));
        let (recent, _rx_recent) = prepared("t", 1, 16, Some(Instant::now()));
        retry.insert(("t".to_string(), 0), old);
        retry.insert(("t".to_string(), 1), recent);

        let (to_send, expired) = collect_retries(&mut retry, Instant::now());

        assert!(expired.len() == 1);
        check!(expired[0].base_sequence == 0);
        assert!(to_send.len() == 1);
        check!(to_send[0].base_sequence == 16);
        check!(retry.is_empty());
    }

    #[test]
    fn collect_retries_sets_first_sent_when_unset() {
        let mut retry: HashMap<(String, i32), PreparedBatch> = HashMap::new();
        let (pb, _rx) = prepared("t", 0, 0, None);
        retry.insert(("t".to_string(), 0), pb);

        let now = Instant::now();
        let (to_send, expired) = collect_retries(&mut retry, now);

        check!(expired.is_empty());
        assert!(to_send.len() == 1);
        check!(to_send[0].first_sent == Some(now));
    }

    #[test]
    fn collect_retries_honours_connection_backoff_until() {
        // A batch parked with `backoff_until` set (after a transport failure)
        // must NOT be resent until that instant passes — otherwise a leader
        // whose pod is down and refusing connections hot-loops the drain every
        // linger tick. The three sample points (before / exactly at / after the
        // backoff instant) pin the `now < backoff_until` comparison so no
        // `<` → `<=`/`>`/`>=`/`==`/`!=` mutant survives.
        let backoff = Duration::from_millis(100);
        let now = Instant::now();
        // (to_send.len(), retry.len()) collected `elapsed` after a batch that is
        // backing off until `now + backoff`.
        let collect_after = |elapsed: Duration| -> (usize, usize) {
            let mut retry: HashMap<(String, i32), PreparedBatch> = HashMap::new();
            let (mut pb, _rx) = prepared("t", 0, 0, Some(now));
            pb.backoff_until = Some(now + backoff);
            retry.insert(("t".to_string(), 0), pb);
            let (to_send, expired) = collect_retries(&mut retry, now + elapsed);
            assert!(expired.is_empty());
            (to_send.len(), retry.len())
        };

        for (elapsed, want) in [
            // Before the backoff instant: parked in its slot, nothing sent.
            (Duration::from_millis(40), (0, 1)),
            // Exactly at the backoff instant: eligible — `now < t` is false
            // here, so `<` resends while `<=` would keep it parked.
            (backoff, (1, 0)),
            // After the backoff instant: eligible and drained out to send.
            (Duration::from_millis(160), (1, 0)),
        ] {
            assert!(collect_after(elapsed) == want);
        }
    }

    #[test]
    fn backoff_deadline_is_in_the_future() {
        // The resend deadline must be `now + retry_backoff` — strictly after
        // `now`. A `+` -> `-` (deadline in the past) would disable the backoff
        // and re-admit the connection-refused hot loop.
        let now = Instant::now();
        let d = Duration::from_millis(100);
        assert!(backoff_deadline(now, d) == now + d);
        assert!(backoff_deadline(now, d) > now);
    }

    #[test]
    fn positive_partition_count_filters_boundary_values() {
        for (input, want) in [(-1, None), (0, None), (1, Some(1)), (2, Some(2))] {
            assert!(positive_partition_count(input) == want);
        }
    }
}

/// Deterministic in-process integration harness.
///
/// Drives the real [`run`] sender loop against a [`MockTransport`] that models a
/// broker's per-partition idempotent sequencing — no socket, no real `Client`.
/// Used to reproduce (and then guard against) the same-partition pipelining hang
/// described in the module docs.
#[cfg(test)]
mod harness {
    use std::sync::{Mutex as StdMutex, atomic::AtomicI64};

    use assert2::{assert, check};
    use crabka_client_core::ClientError;
    use crabka_protocol::{
        owned::{
            metadata_response::{
                MetadataResponse, MetadataResponsePartition, MetadataResponseTopic,
            },
            produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
            produce_response::{
                LeaderIdAndEpoch, PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
            },
        },
        records::{Attributes, Record, RecordBatch},
    };
    use tokio::sync::oneshot;

    use super::*;
    use crate::{
        accumulator::Accumulator,
        producer::{STATE_ACTIVE, STATE_FENCED, TopicMetadata},
        transactional::TxnState,
    };

    /// Per-`(topic, partition)` accumulator map (mirrors the production type).
    type AccumulatorMap = Arc<DashMap<(String, i32), Arc<Mutex<Accumulator>>>>;

    /// Adapter so a sender can own a `Box<dyn ProduceTransport>` while the test
    /// keeps a clone of the same `Arc<MockTransport>` to inspect.
    struct ArcTransport(Arc<MockTransport>);

    #[async_trait::async_trait]
    impl ProduceTransport for ArcTransport {
        async fn send_produce(
            &self,
            leader: Option<i32>,
            req: ProduceRequest,
        ) -> Result<ProduceResponse, ClientError> {
            self.0.send_produce(leader, req).await
        }
        fn evict_broker(&self, id: i32) {
            self.0.evict_broker(id);
        }
        fn knows_broker(&self, id: i32) -> bool {
            self.0.knows_broker(id)
        }
        async fn refresh_metadata(&self) -> Result<MetadataResponse, ClientError> {
            self.0.refresh_metadata().await
        }
    }

    /// Per-partition broker sequencing state.
    #[derive(Default)]
    struct PartitionState {
        /// Next `base_sequence` the broker will accept (strictly increasing, no
        /// gaps), à la Kafka idempotent producer.
        expected: i32,
        /// Running log-end offset; each accepted batch is assigned the current
        /// value as its `base_offset`, then advanced by the record count.
        next_offset: i64,
        /// `base_sequence -> base_offset` for sequences already accepted, so a
        /// resend of a written batch can be answered `DUPLICATE_SEQUENCE_NUMBER`
        /// with its original offset (the broker dedups; the sender maps to ack).
        accepted: HashMap<i32, i64>,
    }

    /// A broker model with faithful per-partition idempotent sequencing.
    ///
    /// `reorder_delay`: when non-zero, `send_produce` sleeps for
    /// `reorder_delay * (REORDER_SPREAD - min(base_sequence, REORDER_SPREAD))`
    /// before applying the broker logic, so that several *concurrently issued*
    /// same-partition requests complete **higher-`base_sequence`-first** — a
    /// lower sequence waits longer. This deterministically models the
    /// on-the-wire write race the old `join_all` same-partition pipelining
    /// suffered. With at most one same-partition request in flight (the fix),
    /// only one request is ever outstanding per partition, so the staggered
    /// delay cannot reorder anything and the broker sees a clean increasing
    /// sequence.
    struct MockTransport {
        partitions: StdMutex<HashMap<(String, i32), PartitionState>>,
        /// Arrival order of `(topic, partition, base_sequence)` as the broker
        /// *applied* them (post-delay), for assertions / debugging.
        arrivals: StdMutex<Vec<(String, i32, i32)>>,
        reorder_delay: Duration,
        /// Total Produce requests applied (lets a test bound livelock churn).
        applied: AtomicUsize,
        /// One-shot transport error: the next send to this `base_sequence`
        /// returns `Err(Disconnected)` exactly once, then is cleared.
        fail_once_seq: StdMutex<Option<i32>>,
        /// One-shot transport error for the next send to a specific broker id.
        fail_once_leader: StdMutex<Option<i32>>,
        /// Artificial per-leader delay before producing a response/error.
        leader_delay: StdMutex<HashMap<i32, Duration>>,
        /// One-shot injected broker response, keyed by `base_sequence`. The next
        /// send to that sequence returns a synthesized `ProduceResponse` (custom
        /// name / `topic_id` / error code / offset / leader hint) once, then is
        /// cleared. Drives terminal, routing, and topic-correlation paths.
        inject_once: StdMutex<Option<Inject>>,
        /// `broker_id`s passed to `evict_broker`, in order.
        evicted: StdMutex<Vec<i32>>,
        /// `timeout_ms` of the most recent Produce request the broker received.
        last_timeout_ms: AtomicI64,
        /// Response `refresh_metadata` returns (default empty).
        refresh_response: StdMutex<MetadataResponse>,
        /// Broker ids the transport claims to have a dialable address for
        /// (drives [`resolve_leader`]). Empty by default → every send falls back
        /// to the bootstrap connection, as the original harness assumed.
        known_brokers: StdMutex<HashSet<i32>>,
        /// The `leader` argument of every `send_produce` call, in order, so a
        /// test can assert how a batch was routed.
        sent_leaders: StdMutex<Vec<Option<i32>>>,
        /// Count of `refresh_metadata` calls, so a test can assert the sender
        /// refreshed after a routing/transport failure.
        refreshes: AtomicUsize,
        offsets_seen: AtomicI64,
    }

    /// A one-shot synthesized broker response, keyed by `base_sequence`.
    /// `name`/`topic_id` of `None` echo the request's; `leader_hint >= 0` sets
    /// the partition response's `current_leader`.
    #[derive(Clone)]
    struct Inject {
        seq: i32,
        name: Option<String>,
        topic_id: Option<Uuid>,
        error_code: i16,
        base_offset: i64,
        leader_hint: i32,
    }

    /// Caps the per-request reorder stagger to a bounded number of delay units
    /// so the total sleep stays small (`reorder_delay * REORDER_SPREAD` worst
    /// case) while still completing higher sequences ahead of lower ones within
    /// a single concurrent `join_all` poll.
    const REORDER_SPREAD: i32 = 32;

    impl MockTransport {
        fn new(reorder_delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                partitions: StdMutex::new(HashMap::new()),
                arrivals: StdMutex::new(Vec::new()),
                reorder_delay,
                applied: AtomicUsize::new(0),
                fail_once_seq: StdMutex::new(None),
                fail_once_leader: StdMutex::new(None),
                leader_delay: StdMutex::new(HashMap::new()),
                inject_once: StdMutex::new(None),
                evicted: StdMutex::new(Vec::new()),
                last_timeout_ms: AtomicI64::new(0),
                refresh_response: StdMutex::new(MetadataResponse::default()),
                known_brokers: StdMutex::new(HashSet::new()),
                sent_leaders: StdMutex::new(Vec::new()),
                refreshes: AtomicUsize::new(0),
                offsets_seen: AtomicI64::new(0),
            })
        }

        fn fail_once_on(self: &Arc<Self>, seq: i32) {
            *self.fail_once_seq.lock().unwrap() = Some(seq);
        }

        fn fail_once_on_leader(self: &Arc<Self>, leader: i32) {
            *self.fail_once_leader.lock().unwrap() = Some(leader);
        }

        fn delay_leader(self: &Arc<Self>, leader: i32, delay: Duration) {
            self.leader_delay.lock().unwrap().insert(leader, delay);
        }

        /// Make the next send to `seq` return a `ProduceResponse` carrying
        /// `error_code` (echoing the request's topic, no leader hint), once.
        fn inject_code_once(self: &Arc<Self>, seq: i32, error_code: i16) {
            self.inject(Inject {
                seq,
                name: None,
                topic_id: None,
                error_code,
                base_offset: -1,
                leader_hint: -1,
            });
        }

        /// Arm a fully-specified one-shot injected response.
        fn inject(self: &Arc<Self>, inject: Inject) {
            *self.inject_once.lock().unwrap() = Some(inject);
        }

        /// `broker_id`s the sender asked to evict, in order.
        fn evicted(self: &Arc<Self>) -> Vec<i32> {
            self.evicted.lock().unwrap().clone()
        }

        /// `timeout_ms` carried by the most recent Produce request.
        fn last_timeout_ms(self: &Arc<Self>) -> i64 {
            self.last_timeout_ms.load(Ordering::Relaxed)
        }

        /// Set the `MetadataResponse` returned by `refresh_metadata`.
        fn set_refresh_response(self: &Arc<Self>, md: MetadataResponse) {
            *self.refresh_response.lock().unwrap() = md;
        }

        /// Mark `id` as a broker the transport can dial (so `resolve_leader`
        /// routes to it instead of the bootstrap connection).
        fn add_known_broker(self: &Arc<Self>, id: i32) {
            self.known_brokers.lock().unwrap().insert(id);
        }

        /// The `leader` argument of every `send_produce` call, in order.
        fn sent_leaders(self: &Arc<Self>) -> Vec<Option<i32>> {
            self.sent_leaders.lock().unwrap().clone()
        }

        /// How many times the sender refreshed cluster metadata.
        fn refresh_count(self: &Arc<Self>) -> usize {
            self.refreshes.load(Ordering::Relaxed)
        }

        fn applied_count(self: &Arc<Self>) -> usize {
            self.applied.load(Ordering::Relaxed)
        }

        /// Apply one single-partition, single-batch `ProduceRequest` to the
        /// broker model and synthesize the matching `ProduceResponse`.
        fn apply(&self, req: &ProduceRequest) -> ProduceResponse {
            let topic = &req.topic_data[0];
            let part = &topic.partition_data[0];
            let batch = part
                .records
                .as_ref()
                .and_then(|p| p.as_v2())
                .and_then(|b| b.first())
                .expect("single v2 record batch");
            let base_sequence = batch.base_sequence;
            let count = i32::try_from(batch.records.len().max(1)).unwrap_or(1);
            let key = (topic.name.clone(), part.index);

            self.arrivals
                .lock()
                .unwrap()
                .push((topic.name.clone(), part.index, base_sequence));
            self.applied.fetch_add(1, Ordering::Relaxed);

            let mut parts = self.partitions.lock().unwrap();
            let st = parts.entry(key).or_default();

            let (error_code, base_offset) = if base_sequence == st.expected {
                // In-order: accept, assign offset, advance.
                let base_offset = st.next_offset;
                st.accepted.insert(base_sequence, base_offset);
                st.next_offset += i64::from(count);
                st.expected = st.expected.wrapping_add(count);
                self.offsets_seen.fetch_max(base_offset, Ordering::Relaxed);
                (codes::NONE, base_offset)
            } else if let Some(&prev) = st.accepted.get(&base_sequence) {
                // Already written (a resend of a durable batch): dedup.
                (codes::DUPLICATE_SEQUENCE_NUMBER, prev)
            } else {
                // A gap: a lower sequence hasn't been accepted yet.
                (codes::OUT_OF_ORDER_SEQUENCE_NUMBER, -1)
            };

            ProduceResponse {
                responses: vec![TopicProduceResponse {
                    name: topic.name.clone(),
                    topic_id: topic.topic_id,
                    partition_responses: vec![PartitionProduceResponse {
                        index: part.index,
                        error_code,
                        base_offset,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }
        }
    }

    #[async_trait::async_trait]
    impl ProduceTransport for MockTransport {
        async fn send_produce(
            &self,
            leader: Option<i32>,
            req: ProduceRequest,
        ) -> Result<ProduceResponse, ClientError> {
            self.sent_leaders.lock().unwrap().push(leader);
            self.last_timeout_ms
                .store(i64::from(req.timeout_ms), Ordering::Relaxed);

            if let Some(delay) =
                leader.and_then(|id| self.leader_delay.lock().unwrap().get(&id).copied())
            {
                tokio::time::sleep(delay).await;
            }

            {
                let mut guard = self.fail_once_leader.lock().unwrap();
                if let (Some(target), Some(actual)) = (*guard, leader)
                    && target == actual
                {
                    *guard = None;
                    drop(guard);
                    return Err(ClientError::Disconnected);
                }
            }

            let batch_seq = req.topic_data[0].partition_data[0]
                .records
                .as_ref()
                .and_then(|p| p.as_v2())
                .and_then(|b| b.first())
                .map(|b| b.base_sequence);

            // One-shot injected transport error.
            {
                let mut guard = self.fail_once_seq.lock().unwrap();
                if let (Some(target), Some(seq)) = (*guard, batch_seq)
                    && target == seq
                {
                    *guard = None;
                    drop(guard);
                    return Err(ClientError::Disconnected);
                }
            }

            // One-shot injected broker response (terminal / routing / correlation).
            {
                let inj = {
                    let mut guard = self.inject_once.lock().unwrap();
                    match (guard.as_ref(), batch_seq) {
                        (Some(i), Some(seq)) if i.seq == seq => guard.take(),
                        _ => None,
                    }
                };
                if let Some(inj) = inj {
                    let topic = &req.topic_data[0];
                    let part = &topic.partition_data[0];
                    let current_leader = if inj.leader_hint >= 0 {
                        LeaderIdAndEpoch {
                            leader_id: inj.leader_hint,
                            ..Default::default()
                        }
                    } else {
                        LeaderIdAndEpoch::default()
                    };
                    return Ok(ProduceResponse {
                        responses: vec![TopicProduceResponse {
                            name: inj.name.unwrap_or_else(|| topic.name.clone()),
                            topic_id: inj.topic_id.unwrap_or(topic.topic_id),
                            partition_responses: vec![PartitionProduceResponse {
                                index: part.index,
                                error_code: inj.error_code,
                                base_offset: inj.base_offset,
                                current_leader,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    });
                }
            }

            // Reorder model: higher base_sequence completes first when several
            // same-partition requests are issued concurrently.
            if !self.reorder_delay.is_zero() {
                let units =
                    u32::try_from(REORDER_SPREAD - batch_seq.unwrap_or(0).min(REORDER_SPREAD))
                        .unwrap_or(0);
                tokio::time::sleep(self.reorder_delay * units).await;
            }

            Ok(self.apply(&req))
        }

        fn evict_broker(&self, broker_id: i32) {
            self.evicted.lock().unwrap().push(broker_id);
        }

        fn knows_broker(&self, broker_id: i32) -> bool {
            self.known_brokers.lock().unwrap().contains(&broker_id)
        }

        async fn refresh_metadata(&self) -> Result<MetadataResponse, ClientError> {
            self.refreshes.fetch_add(1, Ordering::Relaxed);
            Ok(self.refresh_response.lock().unwrap().clone())
        }
    }

    /// Shared handles a test needs to drive and observe a sender.
    struct Harness {
        accumulators: AccumulatorMap,
        next_seq: Arc<DashMap<(String, i32), i32>>,
        partition_leaders: Arc<DashMap<(String, i32), i32>>,
        metadata_cache: Arc<Mutex<HashMap<String, TopicMetadata>>>,
        state: Arc<AtomicU8>,
        wake_tx: tokio::sync::mpsc::Sender<()>,
        flush_notify: Arc<Notify>,
        in_flight: Arc<AtomicUsize>,
        shutdown: CancellationToken,
        partitioner: Arc<UniformStickyPartitioner>,
        transport: Arc<MockTransport>,
        handle: tokio::task::JoinHandle<()>,
    }

    /// Spawn a sender backed by `transport`, with `max_in_flight` and a fast
    /// 1ms linger so the loop spins quickly.
    fn spawn_sender(transport: Arc<MockTransport>, max_in_flight: usize) -> Harness {
        spawn_sender_with(transport, max_in_flight, Duration::from_millis(1))
    }

    /// Spawn a sender with an explicit `linger`. A long linger lets a test
    /// observe wake-triggered drains in isolation (no empty linger-tick drains).
    fn spawn_sender_with(
        transport: Arc<MockTransport>,
        max_in_flight: usize,
        linger: Duration,
    ) -> Harness {
        let accumulators: AccumulatorMap = Arc::new(DashMap::new());
        let next_seq: Arc<DashMap<(String, i32), i32>> = Arc::new(DashMap::new());
        let (wake_tx, wake_rx) = tokio::sync::mpsc::channel(64);
        let flush_notify = Arc::new(Notify::new());
        let in_flight = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new();
        let metadata_cache: Arc<Mutex<HashMap<String, TopicMetadata>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let partition_leaders: Arc<DashMap<(String, i32), i32>> = Arc::new(DashMap::new());
        let partitioner = Arc::new(UniformStickyPartitioner::new());
        let state = Arc::new(AtomicU8::new(STATE_ACTIVE));

        // Box the same Arc<MockTransport> for the sender; keep a clone for the
        // test to inspect.
        let cfg = SenderConfig {
            transport: Box::new(ArcTransport(transport.clone())),
            producer_id: 1,
            producer_epoch: 0,
            acks: Acks::All,
            compression: Compression::None,
            linger,
            request_timeout: Duration::from_secs(5),
            retry_backoff: Duration::from_millis(1),
            max_in_flight,
            metadata_cache: Arc::clone(&metadata_cache),
            partition_leaders: Arc::clone(&partition_leaders),
            partitioner: Arc::clone(&partitioner),
            accumulators: Arc::clone(&accumulators),
            next_seq: Arc::clone(&next_seq),
            state: Arc::clone(&state),
            wake_rx,
            flush_notify: Arc::clone(&flush_notify),
            in_flight: Arc::clone(&in_flight),
            shutdown: shutdown.clone(),
            transactional_id: None,
            txn_state: Arc::new(Mutex::new(TxnState::Uninitialized)),
            txn_pid_epoch: Arc::new(Mutex::new((1, 0))),
        };

        let handle = tokio::spawn(run(cfg));
        Harness {
            accumulators,
            next_seq,
            partition_leaders,
            metadata_cache,
            state,
            wake_tx,
            flush_notify,
            in_flight,
            shutdown,
            partitioner,
            transport,
            handle,
        }
    }

    /// Append `n` records to `(topic, partition)`, each in its own batch (so the
    /// sender allocates distinct `base_sequence`s and may pipeline them),
    /// returning the ack receivers. We force one-record-per-batch by sealing
    /// after each append.
    async fn produce_burst(
        h: &Harness,
        topic: &str,
        partition: i32,
        n: usize,
    ) -> Vec<oneshot::Receiver<Result<RecordMetadata, ProducerError>>> {
        let key = (topic.to_string(), partition);
        let acc = h
            .accumulators
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(Accumulator::new(16 * 1024))))
            .value()
            .clone();

        let mut rxs = Vec::with_capacity(n);
        for _ in 0..n {
            let mut a = acc.lock().await;
            let rx = match a.try_append(None, Some(bytes::Bytes::from_static(b"x")), vec![], 0) {
                crate::accumulator::AppendResult::Appended(rx) => rx,
                crate::accumulator::AppendResult::BatchFull => panic!("unexpected BatchFull"),
            };
            // Seal so each record becomes its own ready batch with a distinct
            // base_sequence — maximizing same-partition pipelining pressure.
            a.seal_current();
            rxs.push(rx);
        }
        let _ = h.wake_tx.try_send(());
        rxs
    }

    /// Append `n` records to `(topic, partition)` as a SINGLE batch (no seal
    /// between appends), returning the ack receivers in append order. The sender
    /// seals the batch on its next drain, so the records share one
    /// `base_sequence` with `offset_delta` 0..n-1 — exercising the per-record
    /// offset arithmetic (`base_offset + offset_delta`).
    async fn produce_single_batch(
        h: &Harness,
        topic: &str,
        partition: i32,
        n: usize,
    ) -> Vec<oneshot::Receiver<Result<RecordMetadata, ProducerError>>> {
        let rxs = produce_single_batch_without_wake(h, topic, partition, n).await;
        let _ = h.wake_tx.try_send(());
        rxs
    }

    async fn produce_single_batch_without_wake(
        h: &Harness,
        topic: &str,
        partition: i32,
        n: usize,
    ) -> Vec<oneshot::Receiver<Result<RecordMetadata, ProducerError>>> {
        let key = (topic.to_string(), partition);
        let acc = h
            .accumulators
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(Accumulator::new(16 * 1024))))
            .value()
            .clone();

        let mut rxs = Vec::with_capacity(n);
        {
            let mut a = acc.lock().await;
            for _ in 0..n {
                let rx = match a.try_append(None, Some(bytes::Bytes::from_static(b"x")), vec![], 0)
                {
                    crate::accumulator::AppendResult::Appended(rx) => rx,
                    crate::accumulator::AppendResult::BatchFull => panic!("unexpected BatchFull"),
                };
                rxs.push(rx);
            }
        }
        rxs
    }

    async fn shutdown(h: Harness) {
        h.shutdown.cancel();
        let _ = h.handle.await;
    }

    /// THE REGRESSION TEST for the same-partition pipelining hang.
    ///
    /// Burst many single-record batches at ONE partition through the real sender
    /// loop, against a broker that enforces strict per-partition sequencing AND a
    /// reorder model that completes *concurrently issued* same-partition requests
    /// higher-`base_sequence`-first (modeling the on-the-wire write race the old
    /// `join_all` same-partition pipelining suffered).
    ///
    /// With one-in-flight-per-partition (the fix) a partition only ever has one
    /// request outstanding, so the staggered transport delay cannot reorder
    /// anything: the broker sees `base_sequence` 0,1,2,… exactly once each, every
    /// record acks `Ok` with offsets in order, and there is **zero retry churn**
    /// (exactly `N` broker applies). The `applied == N` assertion is the teeth:
    /// the old multi-in-flight design fed reordered concurrent requests to the
    /// broker, drew `OUT_OF_ORDER_SEQUENCE_NUMBER`, drained, and resent — so it
    /// applied strictly more than `N` (and, under sustained load on a cluster,
    /// churned long enough that the caller's time-boxed window saw a record's
    /// ack-oneshot still unresolved — the reported hang).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_partition_burst_all_acks_resolve_in_order() {
        const N: usize = 40;
        let transport = MockTransport::new(Duration::from_millis(2));
        let h = spawn_sender(transport.clone(), 5);

        let rxs = produce_burst(&h, "t", 0, N).await;

        let mut offsets = Vec::with_capacity(N);
        for (i, rx) in rxs.into_iter().enumerate() {
            let md = tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .unwrap_or_else(|_| panic!("record {i} ack-oneshot never resolved (HANG)"))
                .expect("oneshot sender dropped")
                .expect("record must be acked Ok, not failed");
            assert!(md.partition == 0);
            offsets.push(md.offset);
        }

        // Offsets must be the clean increasing sequence 0..N — proof the broker
        // saw each base_sequence exactly once, in order.
        let expected: Vec<i64> = (0..i64::try_from(N).unwrap()).collect();
        assert!(offsets == expected);
        // Zero churn: with one in-flight per partition there is never an
        // out-of-order arrival, so the broker applies each batch exactly once.
        assert!(
            h.transport.applied_count() == N,
            "expected exactly {N} applies (no resend churn), got {}",
            h.transport.applied_count()
        );

        shutdown(h).await;
    }

    /// Cross-partition pipelining still works concurrently and all acks resolve.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multi_partition_burst_all_acks_resolve() {
        const PARTS: i32 = 6;
        const PER: usize = 10;
        let transport = MockTransport::new(Duration::from_millis(1));
        let h = spawn_sender(transport.clone(), 5);

        let mut all = Vec::new();
        for p in 0..PARTS {
            let rxs = produce_burst(&h, "t", p, PER).await;
            all.push((p, rxs));
        }

        for (p, rxs) in all {
            let mut offsets = Vec::new();
            for (i, rx) in rxs.into_iter().enumerate() {
                let md = tokio::time::timeout(Duration::from_secs(10), rx)
                    .await
                    .unwrap_or_else(|_| panic!("part {p} record {i} never resolved (HANG)"))
                    .expect("oneshot dropped")
                    .expect("must be acked Ok");
                assert!(md.partition == p);
                offsets.push(md.offset);
            }
            let expected: Vec<i64> = (0..i64::try_from(PER).unwrap()).collect();
            assert!(offsets == expected, "partition {p} offsets out of order");
        }

        shutdown(h).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sealing_batch_rotates_null_key_sticky_partition() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender(transport.clone(), 5);
        h.metadata_cache.lock().await.insert(
            "t".to_string(),
            TopicMetadata {
                num_partitions: 3,
                topic_id: Uuid::ZERO,
            },
        );

        assert!(h.partitioner.pick("t", None, 3) == 0);

        let mut rxs = produce_single_batch(&h, "t", 0, 1).await;
        tokio::time::timeout(Duration::from_secs(5), rxs.remove(0))
            .await
            .expect("record ack should resolve")
            .expect("oneshot sender should stay alive")
            .expect("record should ack");

        assert!(
            h.partitioner.pick("t", None, 3) == 1,
            "sender should rotate the shared sticky partition after sealing partition 0"
        );

        shutdown(h).await;
    }

    /// A one-shot transport error mid-stream must NOT drop or reorder: the failed
    /// batch is resent (broker dedups via DUPLICATE if it had landed, or accepts
    /// it fresh), all acks resolve, offsets stay a clean increasing run.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn transport_error_mid_stream_recovers_in_order() {
        const N: usize = 12;
        let transport = MockTransport::new(Duration::ZERO);
        // Fail the batch at base_sequence 3 exactly once.
        transport.fail_once_on(3);
        let h = spawn_sender(transport.clone(), 5);

        let rxs = produce_burst(&h, "t", 0, N).await;

        let mut offsets = Vec::with_capacity(N);
        for (i, rx) in rxs.into_iter().enumerate() {
            let md = tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .unwrap_or_else(|_| panic!("record {i} never resolved after transport error"))
                .expect("oneshot dropped")
                .expect("must be acked Ok after recovery");
            offsets.push(md.offset);
        }
        let expected: Vec<i64> = (0..i64::try_from(N).unwrap()).collect();
        assert!(offsets == expected);

        // in_flight must fully drain back to zero (it lags the last ack-oneshot
        // by the `finish_in_flight` decrement, so poll via `flush_notify`).
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            while h.in_flight.load(Ordering::Acquire) != 0 {
                let _ = tokio::time::timeout(Duration::from_millis(20), h.flush_notify.notified())
                    .await;
            }
        })
        .await;
        assert!(drained.is_ok(), "in_flight never settled to zero");
        // A transport failure forces a metadata refresh so the resend re-resolves
        // the leader; the sender must have refreshed at least once.
        assert!(
            h.transport.refresh_count() >= 1,
            "transport failure must trigger a metadata refresh"
        );
        shutdown(h).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dead_leader_failover_refreshes_and_reroutes_before_timeout_churn() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.add_known_broker(0);
        transport.add_known_broker(1);
        transport.fail_once_on_leader(0);
        transport.set_refresh_response(MetadataResponse {
            brokers: Vec::new(),
            topics: vec![MetadataResponseTopic {
                name: Some("t".to_string()),
                partitions: vec![MetadataResponsePartition {
                    partition_index: 0,
                    leader_id: 1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });

        let h = spawn_sender(transport.clone(), 5);
        h.partition_leaders.insert(("t".to_string(), 0), 0);

        let mut rxs = produce_single_batch(&h, "t", 0, 1).await;
        let md = tokio::time::timeout(Duration::from_secs(5), rxs.remove(0))
            .await
            .expect("record ack should resolve after failover reroute")
            .expect("oneshot sender should stay alive")
            .expect("record should ack after reroute");

        check!(md.partition == 0);
        check!(md.offset == 0);
        check!(
            h.transport.refresh_count() >= 1,
            "first transport failure must force a metadata refresh"
        );
        check!(
            h.transport.refresh_count() <= 2,
            "failover should not spin through repeated refreshes"
        );
        check!(
            h.transport.sent_leaders() == vec![Some(0), Some(1)],
            "sender should try stale leader once, then reroute to fresh leader"
        );
        check!(h.transport.evicted() == vec![0]);

        shutdown(h).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn slow_dead_leader_does_not_block_live_partition_ack() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.add_known_broker(0);
        transport.add_known_broker(1);
        transport.add_known_broker(6);
        transport.fail_once_on_leader(0);
        transport.delay_leader(0, Duration::from_millis(250));
        transport.set_refresh_response(MetadataResponse {
            brokers: Vec::new(),
            topics: vec![MetadataResponseTopic {
                name: Some("t".to_string()),
                partitions: vec![MetadataResponsePartition {
                    partition_index: 0,
                    leader_id: 1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });

        let h = spawn_sender(transport.clone(), 5);
        h.partition_leaders.insert(("t".to_string(), 0), 0);
        h.partition_leaders.insert(("t".to_string(), 1), 6);

        let mut dead_rx = produce_single_batch_without_wake(&h, "t", 0, 1).await;
        let mut live_rx = produce_single_batch_without_wake(&h, "t", 1, 1).await;
        let _ = h.wake_tx.try_send(());

        let live_md = tokio::time::timeout(Duration::from_millis(100), live_rx.remove(0))
            .await
            .expect("live partition ack should not wait for a slow dead leader")
            .expect("oneshot sender should stay alive")
            .expect("live partition should ack Ok");
        assert!(live_md.partition == 1);
        assert!(live_md.offset == 0);

        let dead_md = tokio::time::timeout(Duration::from_secs(5), dead_rx.remove(0))
            .await
            .expect("dead leader partition should resolve after reroute")
            .expect("oneshot sender should stay alive")
            .expect("dead leader partition should ack after reroute");
        assert!(dead_md.partition == 0);
        assert!(dead_md.offset == 0);

        let sent = h.transport.sent_leaders();
        assert!(
            sent.len() == 3 && sent[2] == Some(1),
            "sender should reroute the stale leader after the first two sends, got {sent:?}"
        );
        assert!(
            sent[..2].contains(&Some(0)) && sent[..2].contains(&Some(6)),
            "first cycle should include stale leader and live leader sends, got {sent:?}"
        );

        shutdown(h).await;
    }

    /// `flush_notify` fires and `in_flight` returns to zero once the burst drains.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_flight_drains_to_zero() {
        let transport = MockTransport::new(Duration::from_millis(1));
        let h = spawn_sender(transport.clone(), 5);
        let rxs = produce_burst(&h, "t", 0, 20).await;
        for rx in rxs {
            let _ = tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .expect("no hang")
                .expect("oneshot")
                .expect("acked");
        }
        // `in_flight` is decremented just AFTER a batch's ack-oneshots are sent
        // (see `ack_batch`), so it can briefly lag the last `rx.await`. Wait for
        // it to settle to zero via `flush_notify`, mirroring `Producer::flush`.
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            while h.in_flight.load(Ordering::Acquire) != 0 {
                let _ = tokio::time::timeout(Duration::from_millis(20), h.flush_notify.notified())
                    .await;
            }
        })
        .await;
        assert!(drained.is_ok(), "in_flight never settled to zero");
        // And the broker applied each batch exactly once (no churn).
        assert!(h.transport.applied_count() == 20);
        let _ = &h.next_seq;
        shutdown(h).await;
    }

    /// Mechanism proof: issuing several same-partition requests CONCURRENTLY (as
    /// the old `send_batches` did via `join_all`) against the staggered-reorder
    /// broker makes the broker apply them higher-`base_sequence`-first, so every
    /// request except the lowest draws `OUT_OF_ORDER_SEQUENCE_NUMBER`. This is
    /// the gap-and-resend trigger the fix eliminates by never issuing more than
    /// one same-partition request at a time. (Pure transport-level check; no
    /// sender loop — it isolates the reorder mechanism.)
    ///
    /// Uses paused virtual time so the staggered sleeps order the arrivals
    /// deterministically (no reliance on the OS scheduler).
    #[tokio::test(start_paused = true)]
    async fn concurrent_same_partition_sends_reorder_and_trip_out_of_order() {
        let transport = MockTransport::new(Duration::from_millis(5));

        // Build single-partition Produce requests for base_sequences 0,1,2,3,4.
        let make_req = |base_sequence: i32| ProduceRequest {
            acks: -1,
            topic_data: vec![TopicProduceData {
                name: "t".to_string(),
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(
                        RecordBatch {
                            attributes: Attributes::default(),
                            base_sequence,
                            records: vec![Record {
                                attributes: 0,
                                timestamp_delta: 0,
                                offset_delta: 0,
                                key: None,
                                value: Some(bytes::Bytes::from_static(b"x")),
                                headers: vec![],
                            }],
                            ..Default::default()
                        }
                        .into(),
                    ),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        // Fire all five concurrently — exactly what the buggy join_all did.
        let results =
            futures::future::join_all((0..5).map(|s| transport.send_produce(None, make_req(s))))
                .await;

        // The lowest (0) is accepted; every higher one trips OUT_OF_ORDER because
        // it reached the broker ahead of 0.
        let codes: Vec<i16> = results
            .into_iter()
            .map(|r| r.expect("no transport error").responses[0].partition_responses[0].error_code)
            .collect();
        assert!(codes[0] == codes::NONE);
        for c in &codes[1..] {
            assert!(*c == codes::OUT_OF_ORDER_SEQUENCE_NUMBER);
        }

        // Arrivals were applied highest-first (the reorder), confirming the race.
        let arrivals: Vec<i32> = transport
            .arrivals
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, s)| *s)
            .collect();
        assert!(arrivals == vec![4, 3, 2, 1, 0]);
    }

    /// A partition with a batch pending resend must NOT also send its next batch
    /// in the same cycle — otherwise, under a broker that reorders concurrent
    /// same-partition requests, the new batch could overtake the resend and trip
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER` churn. A one-shot transport error parks one
    /// batch for resend mid-stream; with the reorder model active we still expect
    /// each batch applied exactly once (no churn) and offsets in a clean run.
    /// This guards the "ordering preserved by construction" property of the
    /// one-slot-per-partition pipeline against a same-partition send race.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn retry_does_not_race_new_batch_under_reorder() {
        const N: usize = 16;
        let transport = MockTransport::new(Duration::from_millis(2));
        // Fail the batch at base_sequence 5 once: it parks for resend while
        // later batches are still queued behind it.
        transport.fail_once_on(5);
        let h = spawn_sender(transport.clone(), 5);

        let rxs = produce_burst(&h, "t", 0, N).await;
        let mut offsets = Vec::with_capacity(N);
        for (i, rx) in rxs.into_iter().enumerate() {
            let md = tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .unwrap_or_else(|_| panic!("record {i} never resolved"))
                .expect("oneshot dropped")
                .expect("acked Ok");
            offsets.push(md.offset);
        }
        let expected: Vec<i64> = (0..i64::try_from(N).unwrap()).collect();
        assert!(offsets == expected);
        // The failed send errored at the transport before the broker applied it,
        // so each of the N batches is applied exactly once: a new batch never
        // raced (and reordered ahead of) the pending resend.
        assert!(
            h.transport.applied_count() == N,
            "expected exactly {N} applies (no churn), got {}",
            h.transport.applied_count()
        );

        shutdown(h).await;
    }

    /// Routing decision (`resolve_leader`): a partition whose cached leader is a
    /// known (dialable) broker is sent to that broker; a partition whose leader
    /// is unknown (or whose address the pool can't dial) falls back to the
    /// bootstrap connection. Drives the real sender so the `leader` argument
    /// handed to the transport is observed directly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn routes_to_known_leader_else_bootstrap() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.add_known_broker(5); // 5 is dialable; 7 is not.
        let h = spawn_sender(transport.clone(), 5);

        // Partition 0 → leader 5 (known): must route to Some(5).
        h.partition_leaders.insert(("t".to_string(), 0), 5);
        // Partition 1 → leader 7 (unknown address): must fall back to bootstrap.
        h.partition_leaders.insert(("t".to_string(), 1), 7);

        let rx0 = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        let rx1 = produce_burst(&h, "t", 1, 1).await.pop().expect("one rx");
        for (i, rx) in [rx0, rx1].into_iter().enumerate() {
            let _ = tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .unwrap_or_else(|_| panic!("record {i} never resolved"))
                .expect("oneshot dropped")
                .expect("acked Ok");
        }

        let leaders = h.transport.sent_leaders();
        check!(
            leaders.contains(&Some(5)),
            "known leader 5 must be routed to explicitly, got {leaders:?}"
        );
        check!(
            leaders.contains(&None),
            "unknown leader must fall back to bootstrap (None), got {leaders:?}"
        );
        check!(
            !leaders.contains(&Some(7)),
            "unknown-address leader 7 must never be dialed, got {leaders:?}"
        );

        shutdown(h).await;
    }

    /// A terminal-but-not-fatal server error (an unmodeled code) fails the record
    /// with `Server(code)` and releases its in-flight slot — it must not fence,
    /// hang, or be retried forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminal_server_error_fails_record() {
        const MESSAGE_TOO_LARGE: i16 = 10;
        let transport = MockTransport::new(Duration::ZERO);
        transport.inject_code_once(0, MESSAGE_TOO_LARGE);
        let h = spawn_sender(transport.clone(), 5);

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        let res = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("record never resolved (HANG)")
            .expect("oneshot dropped");
        let err = res.expect_err("terminal error must fail the record, not ack it");
        assert!(
            matches!(err, ProducerError::Server(MESSAGE_TOO_LARGE)),
            "expected Server(10), got {err:?}"
        );

        // The slot is released: in_flight drains back to zero.
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            while h.in_flight.load(Ordering::Acquire) != 0 {
                let _ = tokio::time::timeout(Duration::from_millis(20), h.flush_notify.notified())
                    .await;
            }
        })
        .await;
        assert!(drained.is_ok(), "in_flight never settled to zero");

        shutdown(h).await;
    }

    /// A batch with several records assigns each record `base_offset +
    /// offset_delta`. The other tests use one record per batch, where
    /// `offset_delta` is always 0; this pins the per-record offset arithmetic.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multi_record_batch_offsets_use_base_plus_delta() {
        const N: usize = 4;
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender(transport.clone(), 5);

        let rxs = produce_single_batch(&h, "t", 0, N).await;
        let mut offsets = Vec::with_capacity(N);
        for (i, rx) in rxs.into_iter().enumerate() {
            let md = tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .unwrap_or_else(|_| panic!("record {i} never resolved"))
                .expect("oneshot dropped")
                .expect("acked Ok");
            assert!(md.partition == 0);
            offsets.push(md.offset);
        }
        // One batch at base_offset 0, records at deltas 0..N-1 → offsets 0,1,2,3.
        // Under `base_offset - offset_delta` these would be 0,-1,-2,-3.
        let expected: Vec<i64> = (0..i64::try_from(N).unwrap()).collect();
        assert!(offsets == expected, "got {offsets:?}");

        shutdown(h).await;
    }

    /// A fatal `INVALID_PRODUCER_EPOCH` fences the producer: the record fails with
    /// `FencedProducer` and the shared state flips to `STATE_FENCED`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalid_producer_epoch_fences_producer() {
        const INVALID_PRODUCER_EPOCH: i16 = 47;
        let transport = MockTransport::new(Duration::ZERO);
        transport.inject_code_once(0, INVALID_PRODUCER_EPOCH);
        let h = spawn_sender(transport.clone(), 5);

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        let err = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("record never resolved (HANG)")
            .expect("oneshot dropped")
            .expect_err("a fatal epoch error must fail the record, not ack it");
        assert!(
            matches!(err, ProducerError::FencedProducer),
            "expected FencedProducer, got {err:?}"
        );
        assert!(
            h.state.load(Ordering::Acquire) == STATE_FENCED,
            "the producer must be fenced after a fatal idempotence error"
        );

        shutdown(h).await;
    }

    /// A transport failure to a *known* leader evicts that broker's connection so
    /// a reconnect targets its current address; the batch then resends and acks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transport_error_evicts_known_leader() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.add_known_broker(5);
        transport.fail_once_on(0);
        let h = spawn_sender(transport.clone(), 5);
        h.partition_leaders.insert(("t".to_string(), 0), 5);

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("record never resolved")
            .expect("oneshot dropped")
            .expect("acked Ok after recovery");

        assert!(
            h.transport.evicted().contains(&5),
            "a transport error to known leader 5 must evict it, got {:?}",
            h.transport.evicted()
        );

        shutdown(h).await;
    }

    /// On `NOT_LEADER_OR_FOLLOWER` with an inline `current_leader` hint to a
    /// *known* broker, the sender adopts the hint — routes the resend there and
    /// updates its leader cache — WITHOUT a metadata refresh.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn not_leader_adopts_known_inline_hint_without_refresh() {
        const NOT_LEADER_OR_FOLLOWER: i16 = 6;
        let transport = MockTransport::new(Duration::ZERO);
        transport.add_known_broker(5);
        transport.add_known_broker(8);
        transport.inject(Inject {
            seq: 0,
            name: None,
            topic_id: None,
            error_code: NOT_LEADER_OR_FOLLOWER,
            base_offset: -1,
            leader_hint: 8,
        });
        let h = spawn_sender(transport.clone(), 5);
        h.partition_leaders.insert(("t".to_string(), 0), 5);

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("record never resolved")
            .expect("oneshot dropped")
            .expect("acked Ok after re-route");

        let leaders = h.transport.sent_leaders();
        check!(
            leaders.contains(&Some(5)),
            "first send routes to current leader 5, got {leaders:?}"
        );
        check!(
            leaders.contains(&Some(8)),
            "the resend must adopt the inline hint 8, got {leaders:?}"
        );
        check!(
            h.partition_leaders
                .get(&("t".to_string(), 0))
                .map(|e| *e.value())
                == Some(8),
            "the leader cache must be updated to the hinted leader 8"
        );
        check!(
            h.transport.refresh_count() == 0,
            "a known inline hint must not trigger a metadata refresh"
        );

        shutdown(h).await;
    }

    /// The sender correlates a Produce response to its batch by `topic_id` when
    /// the response's topic NAME differs (Kafka v13+ omits the name). The injected
    /// response carries the matching `topic_id` and a distinctive offset.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn correlates_response_by_topic_id_when_name_differs() {
        let topic_id = Uuid([7u8; 16]);
        let transport = MockTransport::new(Duration::ZERO);
        transport.inject(Inject {
            seq: 0,
            name: Some(String::new()), // name does NOT match "t"
            topic_id: Some(topic_id),  // but topic_id does
            error_code: codes::NONE,
            base_offset: 42,
            leader_hint: -1,
        });
        let h = spawn_sender(transport.clone(), 5);
        // Give "t" a non-zero topic_id so the batch carries it.
        h.metadata_cache.lock().await.insert(
            "t".to_string(),
            TopicMetadata {
                num_partitions: 1,
                topic_id,
            },
        );

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        let md = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("record never resolved")
            .expect("oneshot dropped")
            .expect("acked Ok via topic_id correlation");
        // The injected response's base_offset (42) proves the sender matched by
        // topic_id; failing to correlate would resend and ack at the broker's 0.
        assert!(
            md.offset == 42,
            "expected offset 42 from the topic_id-correlated response, got {}",
            md.offset
        );

        shutdown(h).await;
    }

    /// `&&` (not `||`) gates the `topic_id` fallback: a response whose name does
    /// NOT match and whose `topic_id` is ZERO must NOT be (mis)correlated. The
    /// batch has no `topic_id` (ZERO), so only an exact name match binds a
    /// response — a
    /// wrong-name response forces a resend.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn does_not_correlate_mismatched_name_with_zero_topic_id() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.inject(Inject {
            seq: 0,
            name: Some("other".to_string()), // wrong name
            topic_id: Some(Uuid::ZERO),      // zero topic_id
            error_code: codes::NONE,
            base_offset: 99, // a bogus offset that must NOT be adopted
            leader_hint: -1,
        });
        let h = spawn_sender(transport.clone(), 5);
        // No metadata → the batch's topic_id is ZERO.

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        let md = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("record never resolved")
            .expect("oneshot dropped")
            .expect("acked Ok after resend");
        // Correct code ignores the mismatched response and resends, acking at the
        // broker's real offset 0 — never the bogus 99 the wrong response carried.
        assert!(
            md.offset == 0,
            "a name-mismatched, zero-topic_id response must not be correlated; got {}",
            md.offset
        );

        shutdown(h).await;
    }

    /// `update_leaders_from_metadata` adopts leaders only from HEALTHY topics
    /// (`error_code == 0`). A transport error triggers a refresh whose response
    /// advertises a new leader for a healthy topic; the cache picks it up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_adopts_leader_for_healthy_topic() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.add_known_broker(9);
        transport.fail_once_on(0); // transport error → forces a metadata refresh
        transport.set_refresh_response(MetadataResponse {
            topics: vec![MetadataResponseTopic {
                error_code: 0,
                name: Some("t".to_string()),
                partitions: vec![MetadataResponsePartition {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: 9,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });
        let h = spawn_sender(transport.clone(), 5);

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("record never resolved")
            .expect("oneshot dropped")
            .expect("acked Ok after refresh + resend");

        assert!(
            h.partition_leaders
                .get(&("t".to_string(), 0))
                .map(|e| *e.value())
                == Some(9),
            "a healthy topic's advertised leader (9) must be adopted from the refresh"
        );

        shutdown(h).await;
    }

    /// The Produce request carries the configured `request_timeout` as
    /// `timeout_ms` (5s → 5000ms on the wire).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn request_carries_configured_timeout() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender(transport.clone(), 5);

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("record never resolved")
            .expect("oneshot dropped")
            .expect("acked Ok");

        assert!(
            h.transport.last_timeout_ms() == 5000,
            "produce request must carry the configured 5000ms timeout, got {}",
            h.transport.last_timeout_ms()
        );

        shutdown(h).await;
    }

    /// `finish_in_flight` notifies flush waiters exactly when `in_flight` reaches
    /// zero. With a long linger the only drains are wake-triggered, so this
    /// notify is the only one a registered waiter can receive.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn finish_in_flight_notifies_when_drained() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 5, Duration::from_secs(30));

        // Let the immediate first (empty) linger-tick drain pass before
        // registering the waiter. That tick's `notify_waiters` leaves no
        // trace (it wakes only already-registered waiters and this drain
        // mutates no observable state), so there is no positive condition to
        // poll — this is a deliberate ordering delay that keeps the first
        // empty tick from waking the waiter for the wrong reason.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let flush = Arc::clone(&h.flush_notify);
        // Register the flush waiter synchronously: a `Notified` future only
        // registers once enabled/polled, and `notify_waiters` wakes only
        // already-registered waiters, so `enable()` removes the registration
        // race deterministically (no settle needed). From here only
        // finish_in_flight can notify it.
        let watcher = flush.notified();
        tokio::pin!(watcher);
        watcher.as_mut().enable();

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        let fired = tokio::time::timeout(Duration::from_secs(3), watcher).await;
        assert!(
            fired.is_ok(),
            "finish_in_flight must notify flush waiters when in_flight reaches zero"
        );

        let _ = tokio::time::timeout(Duration::from_secs(2), rx).await;
        shutdown(h).await;
    }
}
