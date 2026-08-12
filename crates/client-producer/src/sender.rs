//! Background sender task. It drains ready batches from every accumulator and
//! ships them as `ProduceRequest`s through `crabka-client-core`.
//!
//! The builder `tokio::spawn`s the sender. The sender owns the `wake_rx`
//! `Receiver` end of the wake channel, while the `Producer` holds the `wake_tx`
//! `Sender`. It also owns the `flush_notify`, the `accumulators` map, and the
//! `next_seq` map. Per-batch deadlines seal only expired current batches, ready
//! wakes drain completed rollover batches, and forced wakes stay active until
//! zero-linger, flush, or shutdown work settles. Drained batches become v2
//! `RecordBatch`es, which allocates their `base_sequence`. Each batch becomes
//! its own single-partition `ProduceRequest`, sent through `Client::broker(id)`,
//! and it falls back to the bootstrap `Client::send` when the leader is
//! unknown. All of a cycle's requests are sent **concurrently**, to keep every
//! broker busy.
//!
//! ## Per-partition pipelining (idempotence-critical)
//!
//! Brokers stay busy because independent partitions send **concurrently**. Up
//! to [`SenderConfig::max_in_flight`] Produce requests overlap on the wire per
//! drain cycle. But each *single* partition keeps **at most one** request in
//! flight, which is [`MAX_IN_FLIGHT_PER_PARTITION`]. Its next batch is not
//! drained until the previous one is acked. Per-partition idempotent
//! `base_sequence` ordering therefore holds **by construction**. The broker
//! never sees two outstanding sequences for one partition, so requests issued
//! concurrently cannot reach it out of `base_sequence` order and trip
//! `OUT_OF_ORDER_SEQUENCE_NUMBER`.
//!
//! Recovery is correspondingly simple. A batch that fails, through a transport
//! error, a routing miss, or a defensive `OUT_OF_ORDER`, is parked in its
//! partition's single **retry slot**. On the next cycle the sender resends it
//! verbatim, with the same allocated `base_sequence` and the same bytes, and
//! ahead of any new batch for that partition. The broker dedups a re-landed
//! write with `DUPLICATE_SEQUENCE_NUMBER`. The retry slots persist across
//! cycles, and [`run`] owns them.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
};

use crabka_protocol::{
    owned::{
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid,
    records::{Attributes, Record, RecordBatch, RecordHeader},
};
use crabka_units::{
    Time,
    convert::{StdDurationExt as _, TimeExt as _},
};
use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::{
    sync::{Mutex, Notify},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::{
    accumulator::{AccumulatorMap, InProgressBatch, PendingRecord},
    compression::Compression,
    error::ProducerError,
    partitioner::UniformStickyPartitioner,
    producer::{Acks, STATE_ACTIVE, STATE_FENCED, TopicMetadata, UNRESOLVED_TOPIC_PARTITION_COUNT},
    record::RecordMetadata,
    transactional::TxnState,
    transport::ProduceTransport,
};

/// Wire error codes referenced when interpreting `PartitionProduceResponse`.
mod codes {
    pub const NONE: i16 = 0;
    /// The Produce reached a broker that does not lead the partition, which
    /// means the routing is stale. Refresh metadata, re-resolve the leader, and
    /// retry.
    pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    /// The Produce reached a broker that does not lead the partition. With
    /// rf=1 a misroute to a non-hosting broker surfaces as
    /// `UNKNOWN_TOPIC_OR_PARTITION`, and a misroute to a follower surfaces
    /// here.
    pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
    pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
    pub const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
    /// `INVALID_PRODUCER_EPOCH` per the canonical Apache Kafka table (code 47).
    pub const INVALID_PRODUCER_EPOCH: i16 = 47;
}

/// Synthetic leader id that means the leader is unknown, so the sender uses the
/// bootstrap connection.
///
/// It matches the consumer's convention in `poll.rs`. A partition whose leader
/// id is `< 0`, or whose advertised address the pool cannot dial, falls back to
/// the bootstrap `Client::send` rather than `Client::broker(id)`.
const BOOTSTRAP_LEADER: i32 = -1;

/// Maximum Produce requests in flight **per partition** at once.
///
/// This is pinned to `1`: a partition's next batch is not sent until its
/// previous batch is acked. That preserves idempotent per-partition
/// `base_sequence` ordering *by construction*. The broker only ever sees one
/// sequence outstanding for a partition, so there is no window in which
/// requests issued concurrently can reach the broker out of `base_sequence`
/// order and trip `OUT_OF_ORDER_SEQUENCE_NUMBER`.
///
/// ## Why not `> 1` (same-partition pipelining)?
///
/// The previous design drained up to `max_in_flight` batches per partition and
/// fired them through `futures::future::join_all`. But the `send` of
/// [`crabka_client_core::Client`] writes the request frame **and** awaits its
/// response in a single future. When several same-partition futures are polled
/// concurrently, their frame writes race on the connection's writer channel, so
/// the broker can receive `base_sequence` 16 before 0. The broker rejects the
/// gap with `OUT_OF_ORDER_SEQUENCE_NUMBER`, and the producer resends
/// concurrently again, which re-triggers the reorder. Under sustained load this
/// livelocks: some batch never converges, its records' ack-oneshots never
/// resolve, and the caller hangs.
///
/// True same-partition pipelining (`> 1`) requires a client-core API that
/// guarantees **ordered frame writes** for a partition's in-flight requests
/// (write 0, 1, 2 to the wire in order, then await their responses
/// concurrently) — e.g. a pipelined `Connection::send_batch` or a write-then-await
/// split. Until acknowledgements carry that finer identity, one in-flight batch
/// per partition is the required ordering policy. Cross-partition pipelining is unaffected:
/// independent partitions still send concurrently, bounded by
/// [`SenderConfig::max_in_flight`].
const MAX_IN_FLIGHT_PER_PARTITION: usize = 1;

// The one-slot-per-partition pipeline (a single retry slot per partition, no
// ordered drain) is only sound while a partition never has more than one request
// outstanding. If this is ever raised above `1`, that model is insufficient — a
// partition could have several outstanding sequences needing an ordered drain —
// and the recovery path must be redesigned. Enforce the dependency at compile
// time so the assumption can't silently drift.
const _: [(); 1] = [(); MAX_IN_FLIGHT_PER_PARTITION];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DrainIntent {
    /// Send batches already completed by rollover without sealing young
    /// in-progress batches.
    Ready,
    /// Seal only in-progress batches whose own linger deadline elapsed.
    Expired,
    /// Seal every in-progress batch for zero linger, explicit flush, or shutdown.
    Force,
}

/// All the bits of state the sender task needs. The builder constructs
/// one of these, hands it to [`run`], and drops it.
pub(crate) struct SenderConfig {
    /// Broker-facing transport. Production uses a real `Client`, and tests use
    /// a deterministic in-process broker model. See [`crate::transport`].
    pub transport: Box<dyn ProduceTransport>,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub acks: Acks,
    pub compression: Compression,
    pub linger: Time,
    pub request_timeout_ms: i32,
    pub retries: i32,
    pub retry_backoff: Time,
    pub routing_retry_budget: Time,
    /// Maximum number of Produce requests fired **concurrently per drain
    /// cycle**, across all partitions. This is the cross-partition, or
    /// per-connection, pipelining bound, which Kafka calls
    /// `max.in.flight.requests.per.connection`.
    ///
    /// Per-partition in-flight is pinned separately to
    /// [`MAX_IN_FLIGHT_PER_PARTITION`], which is `1`, for ordering. This field
    /// bounds how many *distinct partitions'* requests overlap on the wire at
    /// once.
    pub max_in_flight: usize,
    pub metadata_cache: Arc<Mutex<HashMap<String, TopicMetadata>>>,
    /// Per-`(topic, partition)` leader-id cache, shared with the `Producer`.
    /// `Metadata` fills it; see `Producer::partitions_for`. The sender reads it
    /// to route each Produce to the partition leader, and refreshes it on
    /// `NOT_LEADER_OR_FOLLOWER` and on `UNKNOWN_TOPIC_OR_PARTITION`.
    pub partition_leaders: Arc<DashMap<(String, i32), i32>>,
    /// Shared null-key sticky partitioner. The sender rotates it when it seals
    /// a topic batch, so later keyless records fan out across partitions.
    pub partitioner: Arc<UniformStickyPartitioner>,
    pub accumulators: AccumulatorMap,
    pub next_seq: Arc<DashMap<(String, i32), i32>>,
    pub state: Arc<AtomicU8>,
    pub wake_rx: tokio::sync::mpsc::Receiver<DrainIntent>,
    pub flush_notify: Arc<Notify>,
    /// Shared with `Producer`. It tracks batches popped from an accumulator
    /// that are still being sent, so `flush` can wait for them. See the field
    /// doc on [`crate::producer::Producer`].
    pub in_flight: Arc<AtomicUsize>,
    pub shutdown: CancellationToken,
    /// `transactional_id` from the producer config. It is `None` for a
    /// non-transactional producer.
    pub transactional_id: Option<String>,
    /// Shared with `Producer`. The sender snapshots it at send time, to decide
    /// whether to stamp batches as transactional.
    pub txn_state: Arc<Mutex<TxnState>>,
    /// Shared with `Producer`. It holds the `(producer_id, producer_epoch)`
    /// that the transaction coordinator assigned through `InitProducerId`. The
    /// sender reads it when it stamps transactional batches.
    pub txn_pid_epoch: Arc<Mutex<(i64, i16)>>,
    pub txn_recovery_required: Arc<AtomicBool>,
    pub txn_recovery_generation: Arc<AtomicU64>,
}

/// Mutable per-partition pipeline state, owned by [`run`] and threaded into
/// every [`drain_once`] so it persists across drain cycles.
///
/// With [`MAX_IN_FLIGHT_PER_PARTITION`] pinned to `1`, the only state a
/// partition can carry between cycles is a single failed batch that awaits a
/// verbatim resend. There is never more than one request outstanding, so there
/// is nothing to "drain" and no resend *set* to order. Each partition therefore
/// has exactly one slot.
#[derive(Default)]
struct PipelineState {
    /// Per-`(topic, partition)` retry slot. It holds a batch that failed its
    /// last send and must be resent verbatim, with the same `base_sequence` and
    /// the same bytes, ahead of any new batch for that partition.
    ///
    /// `in_flight` already counts the batch, from when it was first drained
    /// from the accumulator, so a resend does NOT count it again. A batch in
    /// this slot means the partition's single in-flight slot is occupied.
    retry: HashMap<(String, i32), PreparedBatch>,
}

#[derive(Debug)]
struct Schedule {
    immediate: bool,
    deadline: Option<Instant>,
    settled: bool,
}

fn include_deadline(schedule: &mut Schedule, deadline: Instant, now: Instant) {
    if deadline <= now {
        schedule.immediate = true;
    } else if schedule.deadline.is_none_or(|current| deadline < current) {
        schedule.deadline = Some(deadline);
    }
}

async fn schedule(cfg: &SenderConfig, state: &PipelineState, force: bool) -> Schedule {
    let now = Instant::now();
    let mut schedule = Schedule {
        immediate: false,
        deadline: None,
        settled: state.retry.is_empty() && cfg.in_flight.load(Ordering::Acquire) == 0,
    };

    for batch in state.retry.values() {
        schedule.settled = false;
        if batch_crosses_recovery_barrier(cfg, batch.transaction_generation) {
            schedule.immediate = true;
            continue;
        }
        let Some(first_sent) = batch.first_sent else {
            if let Some(backoff_until) = batch.backoff_until {
                include_deadline(&mut schedule, backoff_until, now);
            } else {
                schedule.immediate = true;
            }
            continue;
        };
        include_deadline(
            &mut schedule,
            first_sent
                .checked_add(cfg.routing_retry_budget.to_std())
                .unwrap_or(now),
            now,
        );
        if let Some(backoff_until) = batch.backoff_until {
            include_deadline(&mut schedule, backoff_until, now);
        } else {
            schedule.immediate = true;
        }
    }

    let keys = cfg
        .accumulators
        .iter()
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    for key in keys {
        let Some(accumulator) = cfg
            .accumulators
            .get(&key)
            .map(|entry| Arc::clone(entry.value()))
        else {
            continue;
        };
        let accumulator = accumulator.lock().await;
        let has_current = accumulator
            .current
            .as_ref()
            .is_some_and(|batch| !batch.is_empty());
        let has_ready = !accumulator.ready.is_empty();
        if !has_current && !has_ready {
            continue;
        }
        schedule.settled = false;
        let has_recovery_invalid =
            accumulator.current.as_ref().is_some_and(|batch| {
                batch_crosses_recovery_barrier(cfg, batch.transaction_generation)
            }) || accumulator
                .ready
                .iter()
                .any(|batch| batch_crosses_recovery_barrier(cfg, batch.transaction_generation));
        if has_recovery_invalid {
            schedule.immediate = true;
            continue;
        }
        if state.retry.contains_key(&key) {
            continue;
        }
        if has_ready {
            schedule.immediate = true;
        }
        if let Some(batch) = accumulator
            .current
            .as_ref()
            .filter(|batch| !batch.is_empty())
        {
            if force || batch_crosses_recovery_barrier(cfg, batch.transaction_generation) {
                schedule.immediate = true;
            } else {
                include_deadline(
                    &mut schedule,
                    batch
                        .first_append_at
                        .checked_add(cfg.linger.to_std())
                        .unwrap_or(now),
                    now,
                );
            }
        }
    }

    schedule
}

#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(producer_id = cfg.producer_id, max_in_flight = cfg.max_in_flight),
)]
pub(crate) async fn run(mut cfg: SenderConfig) {
    let mut state = PipelineState::default();
    let mut force = false;
    let mut stopping = false;
    loop {
        let next = schedule(&cfg, &state, force).await;
        if next.immediate {
            let intent = if force {
                DrainIntent::Force
            } else {
                DrainIntent::Expired
            };
            drain_once(&mut cfg, &mut state, intent).await;
            continue;
        }
        if force && next.settled {
            force = false;
            if stopping {
                break;
            }
        }
        if stopping && next.settled {
            break;
        }
        if stopping {
            tokio::time::sleep_until(
                next.deadline
                    .expect("unsettled stopped sender has a retry deadline"),
            )
            .await;
            continue;
        }

        let received = if let Some(deadline) = next.deadline {
            tokio::select! {
                () = cfg.shutdown.cancelled() => None,
                received = cfg.wake_rx.recv() => received,
                () = tokio::time::sleep_until(deadline) => continue,
            }
        } else {
            tokio::select! {
                () = cfg.shutdown.cancelled() => None,
                received = cfg.wake_rx.recv() => received,
            }
        };
        match received {
            Some(DrainIntent::Force) => force = true,
            Some(DrainIntent::Ready | DrainIntent::Expired) => {}
            None => {
                force = true;
                stopping = true;
            }
        }
    }
}

/// One drained partition's batch, prepared for sending. It holds the encoded v2
/// `RecordBatch`, with its `base_sequence` already allocated, the topic id, and
/// the `PendingRecord`s whose oneshot acks the response resolves.
///
/// The `record_batch` is built **once**, which allocates the sequence once, so
/// a re-route or a resend ships the identical bytes. That preserves
/// per-partition idempotent sequencing: the leader sees each partition's
/// `base_sequence` exactly once, in increasing order, whichever broker it
/// reaches.
struct PreparedBatch {
    topic: String,
    partition: i32,
    topic_id: Uuid,
    /// The allocated base sequence for this batch. It is cached here, rather
    /// than re-read from `record_batch.base_sequence`, so that it is
    /// unambiguous for a transactional batch, and so that debug logging can
    /// name the batch.
    base_sequence: i32,
    record_batch: RecordBatch,
    records: Vec<PendingRecord>,
    /// Wall-clock time the batch was first handed to the transport. The sender
    /// sets it on the first send and keeps it across resends, so it measures
    /// the routing retry budget from the first attempt, not from the most
    /// recent one. A batch that keeps failing to route gives up by about 30s.
    first_sent: Option<Instant>,
    /// When `Some`, the batch must not be resent until this instant after a
    /// transport failure, missing response, or retriable/routing broker
    /// response. This prevents failed sends from hot-looping the drain
    /// scheduler.
    backoff_until: Option<Instant>,
    /// Resends already admitted after the initial send.
    retries_used: i32,
    transaction_generation: Option<u64>,
}

/// One drain cycle.
///
/// It builds the send list: each partition's pending resend first, then one
/// newly drained batch for each *idle* partition. It sends every batch as its
/// own single-partition `ProduceRequest` **concurrently**. It then dispatches
/// each [`BatchVerdict`]: ack, park for resend, terminal fail, or fence.
///
/// **Per-partition ordering / idempotence.** A partition contributes at most one
/// batch per cycle. It contributes either its pending resend, held in
/// [`PipelineState::retry`], *or* one new batch when idle, and never both. The
/// `occupied` set enforces the "never both". The broker therefore never sees
/// two outstanding sequences for a partition, and a failing partition can never
/// interleave a fresh batch ahead of its pending resend. Each batch's
/// `record_batch`, and therefore its `base_sequence`, is built once and resent
/// verbatim, so the leader sees each sequence exactly once, in order. Ordering
/// is preserved by construction.
///
/// `in_flight` accounting works like this. `fetch_add` runs only when a NEW
/// batch is drained from an accumulator, because resends were counted when they
/// were first drained. `fetch_sub` runs only when a batch reaches a terminal
/// outcome: an ack, a terminal failure, a fence, or an exhausted routing budget.
/// `flush_notify` wakes when `in_flight` hits zero, and when there is nothing to
/// send.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(batches = tracing::field::Empty),
)]
async fn drain_once(cfg: &mut SenderConfig, state: &mut PipelineState, intent: DrainIntent) {
    if cfg.state.load(Ordering::Acquire) != STATE_ACTIVE {
        fence(cfg, state, Vec::new());
        return;
    }
    let now = Instant::now();

    // 1. Fail undrained batches that crossed transaction recovery, then process
    //    resends: each partition's single failed batch must precede any new
    //    batch for that partition. `collect_retries` drains the retry slots
    //    and returns batches whose routing budget elapsed, which we fail here
    //    (their in-flight slot was counted at first drain, so `finish_in_flight`
    //    once).
    fail_recovered_accumulator_batches(cfg).await;
    fail_recovered_retry_slots(cfg, &mut state.retry);
    let (mut to_send, mut expired) = collect_retries(
        &mut state.retry,
        now,
        cfg.routing_retry_budget,
        cfg.max_in_flight,
    );
    if !expired.is_empty() {
        expired.append(&mut to_send);
        fence(cfg, state, expired);
        return;
    }
    fail_recovered_batches(cfg, &mut to_send);

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

    // 2. One eligible new batch per idle partition. Across partitions we fan out
    //    concurrently, but bound the cycle's total fan-out to `max_in_flight`
    //    (the per-connection pipelining bound); partitions not reached this cycle
    //    are picked up on the next drain cycle (their retry slots carry forward,
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
        // Seal only when this drain's cause permits it, then take a single
        // ready batch. A rollover wake must not pull an unrelated young
        // partition into the same send.
        {
            let mut a = acc.lock().await;
            let should_seal = a.current.as_ref().is_some_and(|batch| {
                !batch.is_empty()
                    && (matches!(intent, DrainIntent::Force)
                        || batch_crosses_recovery_barrier(cfg, batch.transaction_generation)
                        || (matches!(intent, DrainIntent::Expired)
                            && now
                                .saturating_duration_since(batch.first_append_at)
                                .as_time()
                                >= cfg.linger))
            });
            if should_seal {
                a.seal_current();
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
        if let Some(num_partitions) = topic_partition_count(cfg, &key.0).await {
            cfg.partitioner.rotate(&key.0, num_partitions);
        }
        if batch_crosses_recovery_barrier(cfg, batch.transaction_generation) {
            fail_batch(batch.records, ProducerError::RecoveryRequired);
            finish_in_flight(cfg);
            continue;
        }
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

/// Drain the per-partition retry slots into an ordered send list, in the
/// one-slot model.
///
/// Each partition holds **at most one** failed batch that awaits a verbatim
/// resend. A batch whose routing budget has elapsed, measured from its first
/// send, goes into `expired`, and the caller fails it instead of resending it.
/// Every resent batch keeps its allocated `base_sequence` and its bytes, and the
/// broker dedups a re-landed write with `DUPLICATE_SEQUENCE_NUMBER`. If
/// `first_sent` is unset, this function initializes it defensively.
///
/// The function is pure over the retry map, with no `Client` and no I/O, so the
/// budget-expiry logic is unit-testable without a broker.
fn collect_retries(
    retry: &mut HashMap<(String, i32), PreparedBatch>,
    now: Instant,
    routing_retry_budget: Time,
    max_to_send: usize,
) -> (Vec<PreparedBatch>, Vec<PreparedBatch>) {
    let mut to_send: Vec<PreparedBatch> = Vec::new();
    let mut expired: Vec<PreparedBatch> = Vec::new();
    // Batches still backing off after a transport failure are re-parked here so
    // a down/refusing leader doesn't hot-loop the drain scheduler.
    let mut parked: Vec<((String, i32), PreparedBatch)> = Vec::new();

    for (key, mut pb) in retry.drain() {
        if pb
            .first_sent
            .is_some_and(|t| now.duration_since(t).as_time() >= routing_retry_budget)
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
        if to_send.len() >= max_to_send {
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

/// Per-batch verdict that [`send_batches`] consumes in the one-slot model. The
/// broker durably accepted the batch, or the batch must be resent verbatim, or
/// it failed terminally with a server code, or it fatally fenced the
/// producer.
#[derive(Debug, Clone, Copy, PartialEq)]
enum BatchVerdict {
    /// Durably written, with `NONE`, or already present, with
    /// `DUPLICATE_SEQUENCE_NUMBER`.
    Acked {
        base_offset: i64,
    },
    /// Resend verbatim on the next cycle, after a transport failure, an
    /// `OUT_OF_ORDER`, or a routing error.
    Retry,
    /// Terminal but non-fatal server error. Fail the records with
    /// `Server(code)`.
    Terminal(i16),
    /// Fatal idempotence failure, `INVALID_PRODUCER_EPOCH`. Fence the
    /// producer.
    Fence,
    RecoveryRequired,
}

/// Classification of a per-partition `error_code`. It is either a direct
/// [`BatchVerdict`], or [`Classification::Routing`] for `NOT_LEADER` and
/// `UNKNOWN`. Routing means a retry, plus the leader-hint adoption and metadata
/// refresh side effects that [`interpret_response`] applies. The classification
/// is kept separate so the pure code-to-verdict mapping is unit-testable
/// without a `Client`.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Classification {
    Verdict(BatchVerdict),
    Routing,
}

/// Map a per-partition `error_code`, and the broker's `base_offset`, to its
/// [`Classification`]. The function is pure and does no I/O.
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

/// Resolve a partition's leader id from the cache.
///
/// It returns [`BOOTSTRAP_LEADER`] when the leader is unknown, that is `< 0`,
/// when the leader is uncached, or when the pool has no dialable address for
/// it, such as a port-0 in-process test broker. Those cases fall back to the
/// bootstrap connection.
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

/// Build the v2 `RecordBatch` for a drained partition batch, and allocate its
/// `base_sequence` range from `next_seq`. The sender sends the result verbatim,
/// and resends it verbatim on a retry, so it allocates the sequence exactly
/// once per batch.
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
        retries_used: 0,
        transaction_generation: batch.transaction_generation,
    }
}

/// One batch's send result: the batch, its [`BatchVerdict`], and whether a
/// metadata refresh is needed before any resend can route correctly.
struct BatchSendResult {
    pb: PreparedBatch,
    verdict: BatchVerdict,
    /// A metadata refresh is required before a resend can route correctly. The
    /// partition came back mis-routed with no usable inline leader hint, or the
    /// hinted leader's address is unknown.
    refresh_needed: bool,
}

/// Send every batch in `to_send` as its own single-partition `ProduceRequest`,
/// **concurrently**, then dispatch each [`BatchVerdict`]: ack the records, fail
/// them terminally, park the batch in its partition's retry slot for a verbatim
/// resend on the next cycle, or fence the producer.
///
/// The concurrency is cross-partition request pipelining. Every batch in
/// `to_send` is for a *distinct* partition, which the `occupied` set of
/// [`drain_once`] guarantees, and the brokers are independent. Overlapping the
/// round-trips therefore keeps every broker busy, never puts two same-partition
/// requests on the wire, and leaves per-partition ordering undisturbed. The
/// futures are polled on this one task, with no spawn, so they share `&cfg`
/// safely.
#[tracing::instrument(level = "debug", skip_all, fields(batches = to_send.len()))]
async fn send_batches(cfg: &SenderConfig, state: &mut PipelineState, to_send: Vec<PreparedBatch>) {
    let mut to_send = to_send;
    fail_recovered_batches(cfg, &mut to_send);
    let mut results: FuturesUnordered<_> = to_send
        .into_iter()
        .map(|pb| send_one_batch(cfg, pb))
        .collect();

    let mut needs_refresh = false;
    let mut fenced: Option<Vec<PreparedBatch>> = None;
    while let Some(res) = results.next().await {
        let BatchSendResult {
            mut pb,
            verdict,
            refresh_needed,
        } = res;
        needs_refresh |= refresh_needed;

        if let Some(to_fail) = &mut fenced {
            match verdict {
                BatchVerdict::Acked { base_offset } => ack_batch(cfg, pb, base_offset),
                BatchVerdict::Terminal(code) => terminal_fail_batch(cfg, pb, code),
                BatchVerdict::RecoveryRequired => {
                    fail_batch(pb.records, ProducerError::RecoveryRequired);
                    finish_in_flight(cfg);
                }
                BatchVerdict::Retry | BatchVerdict::Fence => to_fail.push(pb),
            }
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
            BatchVerdict::Retry if take_retry(&mut pb, cfg.retries) => {
                fenced = Some(vec![pb]);
            }
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
            BatchVerdict::RecoveryRequired => {
                fail_batch(pb.records, ProducerError::RecoveryRequired);
                finish_in_flight(cfg);
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

fn take_retry(batch: &mut PreparedBatch, retries: i32) -> bool {
    if batch.retries_used >= retries {
        true
    } else {
        batch.retries_used += 1;
        false
    }
}

/// Ack a batch's records with their broker-assigned offsets, and release its
/// in-flight slot. The per-record offset is `base_offset + offset_delta`.
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

/// Terminally fail a batch that the broker rejected with an unmodeled error
/// code. It resolves the batch's records with `Server(code)` and releases the
/// in-flight slot. It is the single owner of the slot release for the batch.
fn terminal_fail_batch(cfg: &SenderConfig, pb: PreparedBatch, code: i16) {
    fail_batch(pb.records, ProducerError::Server(code));
    finish_in_flight(cfg);
}

/// Fence the producer.
///
/// This marks `STATE_FENCED` and fails, with `FencedProducer`, `to_fail`, which
/// holds this cycle's still-live batches, every batch parked in a retry slot,
/// and everything in the accumulators. It releases each in-flight slot. The
/// sender calls it on a fatal idempotence failure, `INVALID_PRODUCER_EPOCH`.
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

/// The instant a transport-failed batch becomes eligible to resend, that is
/// `now` plus the configured `retry_backoff`. It is a separate function so that
/// the offset direction is unit-testable: the deadline must be in the
/// *future*.
fn backoff_deadline(now: Instant, retry_backoff: Time) -> Instant {
    now.checked_add(retry_backoff.to_std()).unwrap_or(now)
}

/// Send a single batch as its own single-partition `ProduceRequest`, and
/// resolve its transport or broker result to a [`BatchVerdict`].
///
/// The function returns the batch alongside the verdict, because the caller
/// still owns its records. On a connection error it evicts the broker, so a
/// reconnect targets that broker's current address.
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
    if batch_crosses_recovery_barrier(cfg, pb.transaction_generation) {
        return BatchSendResult {
            pb,
            verdict: BatchVerdict::RecoveryRequired,
            refresh_needed: false,
        };
    }
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

    if cfg.acks == Acks::Zero {
        return match cfg.transport.send_produce_no_response(route, req).await {
            Ok(()) => BatchSendResult {
                pb,
                verdict: BatchVerdict::Acked { base_offset: -1 },
                refresh_needed: false,
            },
            Err(error) => {
                if leader != BOOTSTRAP_LEADER {
                    cfg.transport.evict_broker(leader);
                }
                tracing::warn!(
                    leader,
                    partition = pb.partition,
                    base_sequence = pb.base_sequence,
                    error = %error,
                    "acks=0 produce enqueue failed; will re-route",
                );
                pb.backoff_until = Some(backoff_deadline(Instant::now(), cfg.retry_backoff));
                BatchSendResult {
                    pb,
                    verdict: BatchVerdict::Retry,
                    refresh_needed: true,
                }
            }
        };
    }

    let resp: ProduceResponse = match cfg.transport.send_produce(route, req).await {
        Ok(response) => response,
        Err(error) => {
            // The cached connection is likely dead (broker bounced / failed
            // over). Evict it so a reconnect targets the broker's current
            // address; never evict the shared bootstrap connection.
            if leader != BOOTSTRAP_LEADER {
                cfg.transport.evict_broker(leader);
            }
            tracing::warn!(
                leader,
                partition = pb.partition,
                base_sequence = pb.base_sequence,
                error = %error,
                "produce to leader failed; will re-route",
            );
            // Park the batch for a verbatim resend after backoff, and refresh
            // metadata so the resend targets the current leader.
            pb.backoff_until = Some(backoff_deadline(Instant::now(), cfg.retry_backoff));
            return BatchSendResult {
                pb,
                verdict: BatchVerdict::Retry,
                refresh_needed: true,
            };
        }
    };

    interpret_response(cfg, pb, &resp)
}

/// Interpret a single-partition `ProduceResponse` into a [`BatchSendResult`].
/// It applies the leader-hint side effects of the routing case. The pure
/// code-to-verdict mapping lives in [`classify_verdict`].
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

/// Build a single-partition, single-batch `ProduceRequest`. The transactional
/// state comes from the batch's own attributes, which are set at build time, so
/// the request-level `transactional_id` matches the batch exactly.
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
        timeout_ms: cfg.request_timeout_ms,
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

/// Refresh cluster metadata and adopt the fresh partition-to-leader map. The
/// refresh also refills the pool's broker-address registry, so a leader
/// re-elected onto a broker the pool had not dialed becomes routable.
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
                entry.num_partitions = i32::try_from(t.partitions.len())
                    .unwrap_or(UNRESOLVED_TOPIC_PARTITION_COUNT)
                    .max(UNRESOLVED_TOPIC_PARTITION_COUNT);
                entry.topic_id = t.topic_id;
            }
        }
    }
}

fn batch_crosses_recovery_barrier(cfg: &SenderConfig, generation: Option<u64>) -> bool {
    generation.is_some_and(|batch_generation| {
        cfg.txn_recovery_required.load(Ordering::Acquire)
            || batch_generation != cfg.txn_recovery_generation.load(Ordering::Acquire)
    })
}

fn fail_recovered_batches(cfg: &SenderConfig, batches: &mut Vec<PreparedBatch>) {
    let mut retained = Vec::with_capacity(batches.len());
    for batch in batches.drain(..) {
        if batch_crosses_recovery_barrier(cfg, batch.transaction_generation) {
            fail_batch(batch.records, ProducerError::RecoveryRequired);
            finish_in_flight(cfg);
        } else {
            retained.push(batch);
        }
    }
    batches.extend(retained);
}

fn fail_recovered_retry_slots(
    cfg: &SenderConfig,
    retry: &mut HashMap<(String, i32), PreparedBatch>,
) {
    let recovered_keys = retry
        .iter()
        .filter(|(_, batch)| batch_crosses_recovery_barrier(cfg, batch.transaction_generation))
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for key in recovered_keys {
        let batch = retry
            .remove(&key)
            .expect("recovered retry key remains present");
        fail_batch(batch.records, ProducerError::RecoveryRequired);
        finish_in_flight(cfg);
    }
}

async fn fail_recovered_accumulator_batches(cfg: &SenderConfig) {
    let accumulators = cfg
        .accumulators
        .iter()
        .map(|entry| Arc::clone(entry.value()))
        .collect::<Vec<_>>();
    let mut failed_any = false;
    for accumulator in accumulators {
        let mut accumulator = accumulator.lock().await;
        if accumulator
            .current
            .as_ref()
            .is_some_and(|batch| batch_crosses_recovery_barrier(cfg, batch.transaction_generation))
            && let Some(batch) = accumulator.current.take()
        {
            fail_batch(batch.records, ProducerError::RecoveryRequired);
            failed_any = true;
        }

        let mut retained = VecDeque::with_capacity(accumulator.ready.len());
        while let Some(batch) = accumulator.ready.pop_front() {
            if batch_crosses_recovery_barrier(cfg, batch.transaction_generation) {
                fail_batch(batch.records, ProducerError::RecoveryRequired);
                failed_any = true;
            } else {
                retained.push_back(batch);
            }
        }
        accumulator.ready = retained;
    }
    if failed_any {
        cfg.flush_notify.notify_waiters();
    }
}

/// Decrement `in_flight` for a completed batch, and wake any `flush` waiter
/// when that batch was the last one outstanding.
fn finish_in_flight(cfg: &SenderConfig) {
    if cfg.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
        cfg.flush_notify.notify_waiters();
    }
}

/// Snapshot the transactional `(producer_id, producer_epoch)` if and only if
/// the producer is currently inside an active transaction.
///
/// It returns `Some((pid, epoch))` when the sender should emit a transactional
/// batch, and `None` for a non-transactional send or a send outside a
/// transaction.
async fn txn_pid_snapshot(cfg: &SenderConfig) -> Option<(i64, i16)> {
    cfg.transactional_id.as_ref()?;
    let state = *cfg.txn_state.lock().await;
    if matches!(state, TxnState::InTransaction | TxnState::Preparing) {
        Some(*cfg.txn_pid_epoch.lock().await)
    } else {
        None
    }
}

/// Build a v2 `RecordBatch` from a sealed `InProgressBatch`. The batch
/// attributes encode the compression, and the actual compress step runs inside
/// `RecordBatch::encode`.
///
/// `txn_snapshot` is `Some((pid, epoch))` when the sender sends the batch inside
/// an active transaction. In that case the function sets the `is_transactional`
/// attribute bit, and it uses the pid and epoch the txn coordinator assigned
/// instead of the idempotence pid and epoch.
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
/// `ClientError`, `Protocol`, and `Compression` are not `Clone`, so for those
/// variants only the first record receives the real error. Variants that clone
/// trivially, such as `Server`, `FencedProducer` and `Closed`, reach every
/// record, so callers see the true error code.
fn fail_batch(records: Vec<PendingRecord>, err: ProducerError) {
    fn clone_if_possible(e: &ProducerError) -> Option<ProducerError> {
        match e {
            ProducerError::Server(c) => Some(ProducerError::Server(*c)),
            ProducerError::FencedProducer => Some(ProducerError::FencedProducer),
            ProducerError::RecoveryRequired => Some(ProducerError::RecoveryRequired),
            ProducerError::Closed => Some(ProducerError::Closed),
            ProducerError::FlushTimeout => Some(ProducerError::FlushTimeout),
            ProducerError::BufferFull => Some(ProducerError::BufferFull),
            ProducerError::BatchTooLarge { batch_size } => Some(ProducerError::BatchTooLarge {
                batch_size: *batch_size,
            }),
            ProducerError::RecordTooLarge { record_size } => Some(ProducerError::RecordTooLarge {
                record_size: *record_size,
            }),
            ProducerError::InvalidConfig(s) => Some(ProducerError::InvalidConfig(s.clone())),
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
    use std::time::Duration;

    use assert2::check;
    use crabka_units::{millis, secs};
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
            retries_used: 0,
            transaction_generation: None,
        };
        (pb, rx)
    }

    #[test]
    fn classify_verdict_maps_codes() {
        for (_name, code, base_offset, want) in [
            (
                "success",
                codes::NONE,
                42,
                Classification::Verdict(BatchVerdict::Acked { base_offset: 42 }),
            ),
            // DUPLICATE is acked like a success (broker already wrote it).
            (
                "duplicate sequence",
                codes::DUPLICATE_SEQUENCE_NUMBER,
                7,
                Classification::Verdict(BatchVerdict::Acked { base_offset: 7 }),
            ),
            (
                "out of order",
                codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
                0,
                Classification::Verdict(BatchVerdict::Retry),
            ),
            (
                "invalid epoch",
                codes::INVALID_PRODUCER_EPOCH,
                0,
                Classification::Verdict(BatchVerdict::Fence),
            ),
            (
                "not leader",
                codes::NOT_LEADER_OR_FOLLOWER,
                0,
                Classification::Routing,
            ),
            (
                "unknown topic",
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                0,
                Classification::Routing,
            ),
            // An arbitrary server error (MESSAGE_TOO_LARGE = 10) is terminal-but-
            // not-fatal: fail the records with Server(10), never fence.
            (
                "terminal server error",
                10,
                0,
                Classification::Verdict(BatchVerdict::Terminal(10)),
            ),
        ] {
            assert2::assert!(classify_verdict(code, base_offset) == want);
        }
    }

    #[test]
    fn collect_retries_splits_expired_and_drains_map() {
        // Two partitions, each holding one retry batch (one slot per partition).
        // The batch past its routing budget is split off as expired; the recent
        // one is returned to send. The map is fully drained either way.
        let mut retry: HashMap<(String, i32), PreparedBatch> = HashMap::new();
        let long_ago = Instant::now()
            .checked_sub(Duration::from_secs(31))
            .expect("instant in range");
        let (old, _rx_old) = prepared("t", 0, 0, Some(long_ago));
        let (recent, _rx_recent) = prepared("t", 1, 16, Some(Instant::now()));
        retry.insert(("t".to_string(), 0), old);
        retry.insert(("t".to_string(), 1), recent);

        let (to_send, expired) = collect_retries(&mut retry, Instant::now(), secs(30), usize::MAX);

        check!(
            (
                expired.len(),
                expired[0].base_sequence,
                to_send.len(),
                to_send[0].base_sequence,
                retry.is_empty(),
            ) == (1, 0, 1, 16, true)
        );
    }

    #[test]
    fn collect_retries_caps_sends_but_still_extracts_every_expired_batch() {
        let now = Instant::now();
        let long_ago = now
            .checked_sub(Duration::from_secs(31))
            .expect("instant in range");
        let mut retry = HashMap::new();
        for partition in 0..5 {
            let first_sent = Some(if partition == 4 { long_ago } else { now });
            let (batch, _rx) = prepared("t", partition, partition * 16, first_sent);
            retry.insert(("t".to_owned(), partition), batch);
        }

        let (to_send, expired) = collect_retries(&mut retry, now, secs(30), 2);

        assert_eq!(to_send.len(), 2);
        assert_eq!(expired.len(), 1);
        assert_eq!(retry.len(), 2);
    }

    #[test]
    fn retry_count_exhausts_after_configured_resends() {
        let (mut batch, _rx) = prepared("t", 0, 0, None);

        assert2::assert!(!take_retry(&mut batch, 1));
        assert2::assert!(take_retry(&mut batch, 1));
    }

    #[test]
    fn routing_budget_uses_configured_duration() {
        let mut retry = HashMap::new();
        let now = Instant::now();
        let first_sent = now
            .checked_sub(Duration::from_millis(11))
            .expect("instant in range");
        let (old, _rx) = prepared("t", 0, 0, Some(first_sent));
        retry.insert(("t".to_owned(), 0), old);

        let (to_send, expired) = collect_retries(&mut retry, now, millis(10), usize::MAX);

        assert2::assert!((to_send.len(), expired.len()) == (0, 1));
    }

    #[test]
    fn collect_retries_sets_first_sent_when_unset() {
        let mut retry: HashMap<(String, i32), PreparedBatch> = HashMap::new();
        let (pb, _rx) = prepared("t", 0, 0, None);
        retry.insert(("t".to_string(), 0), pb);

        let now = Instant::now();
        let (to_send, expired) = collect_retries(&mut retry, now, secs(30), usize::MAX);

        check!((expired.is_empty(), to_send.len(), to_send[0].first_sent) == (true, 1, Some(now)));
    }

    #[test]
    fn collect_retries_honours_connection_backoff_until() {
        // A batch parked with `backoff_until` set (after a transport failure)
        // must NOT be resent until that instant passes — otherwise a leader
        // whose pod is down and refusing connections hot-loops the drain
        // scheduler. The three sample points (before / exactly at / after the
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
            let (to_send, expired) =
                collect_retries(&mut retry, now + elapsed, secs(30), usize::MAX);
            assert2::assert!(expired.is_empty());
            (to_send.len(), retry.len())
        };

        for (_name, elapsed, want) in [
            // Before the backoff instant: parked in its slot, nothing sent.
            ("before deadline", Duration::from_millis(40), (0, 1)),
            // Exactly at the backoff instant: eligible — `now < t` is false
            // here, so `<` resends while `<=` would keep it parked.
            ("at deadline", backoff, (1, 0)),
            // After the backoff instant: eligible and drained out to send.
            ("after deadline", Duration::from_millis(160), (1, 0)),
        ] {
            assert2::assert!(collect_after(elapsed) == want);
        }
    }

    #[test]
    fn backoff_deadline_is_in_the_future() {
        // The resend deadline must be `now + retry_backoff` — strictly after
        // `now`. A `+` -> `-` (deadline in the past) would disable the backoff
        // and re-admit the connection-refused hot loop.
        let now = Instant::now();
        let d = millis(100);
        assert2::assert!(backoff_deadline(now, d) == now + d.to_std());
    }

    #[test]
    fn positive_partition_count_filters_boundary_values() {
        for (_name, input, want) in [
            ("negative", -1, None),
            ("zero", 0, None),
            ("one", 1, Some(1)),
            ("positive", 2, Some(2)),
        ] {
            assert2::assert!(positive_partition_count(input) == want);
        }
    }
}

/// Deterministic in-process integration harness.
///
/// It drives the real [`run`] sender loop against a [`MockTransport`] that
/// models a broker's per-partition idempotent sequencing, with no socket and no
/// real `Client`. It reproduces, and then guards against, the same-partition
/// pipelining hang the module docs describe.
#[cfg(test)]
mod harness {
    use std::{
        sync::{
            Mutex as StdMutex,
            atomic::{AtomicBool, AtomicI64, AtomicU64},
        },
        time::Duration,
    };

    use assert2::check;
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
    use crabka_units::{millis, minutes, secs};
    use tokio::sync::oneshot;

    use super::*;
    use crate::{
        accumulator::Accumulator,
        producer::{STATE_ACTIVE, STATE_FENCED, TopicMetadata},
        transactional::TxnState,
    };

    /// Adapter that lets a sender own a `Box<dyn ProduceTransport>` while the
    /// test keeps a clone of the same `Arc<MockTransport>` to inspect.
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
        async fn send_produce_no_response(
            &self,
            leader: Option<i32>,
            req: ProduceRequest,
        ) -> Result<(), ClientError> {
            self.0.send_produce_no_response(leader, req).await
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
        /// Next `base_sequence` the broker accepts. It is strictly increasing
        /// with no gaps, as in the Kafka idempotent producer.
        expected: i32,
        /// Running log-end offset. Each accepted batch takes the current value
        /// as its `base_offset`, and then the offset advances by the record
        /// count.
        next_offset: i64,
        /// `base_sequence -> base_offset` for sequences already accepted. A
        /// resend of a written batch can then be answered with
        /// `DUPLICATE_SEQUENCE_NUMBER` and its original offset. The broker
        /// dedups, and the sender maps that answer to an ack.
        accepted: HashMap<i32, i64>,
    }

    /// A broker model with faithful per-partition idempotent sequencing.
    ///
    /// When `reorder_delay` is non-zero, `send_produce` sleeps for
    /// `reorder_delay * (REORDER_SPREAD - min(base_sequence, REORDER_SPREAD))`
    /// before it applies the broker logic. Several same-partition requests
    /// issued *concurrently* then complete **higher-`base_sequence`-first**,
    /// because a lower sequence waits longer. This models the on-the-wire write
    /// race of the old `join_all` same-partition pipelining deterministically.
    ///
    /// With the fix, at most one same-partition request is in flight, so only
    /// one request is ever outstanding per partition. The staggered delay then
    /// cannot reorder anything, and the broker sees a clean increasing
    /// sequence.
    struct MockTransport {
        partitions: StdMutex<HashMap<(String, i32), PartitionState>>,
        /// Arrival order of `(topic, partition, base_sequence)` as the broker
        /// *applied* them, after the delay. It serves assertions and
        /// debugging.
        arrivals: StdMutex<Vec<(String, i32, i32)>>,
        reorder_delay: Duration,
        /// Total Produce requests applied. A test uses it to bound livelock
        /// churn.
        applied: AtomicUsize,
        /// One-shot transport error. The next send to this `base_sequence`
        /// returns `Err(Disconnected)` exactly once, and then the flag
        /// clears.
        fail_once_seq: StdMutex<Option<i32>>,
        /// One-shot transport error for the next send to a specific broker id.
        fail_once_leader: StdMutex<Option<i32>>,
        /// Artificial per-leader delay before producing a response/error.
        leader_delay: StdMutex<HashMap<i32, Duration>>,
        /// One-shot injected broker response, keyed by `base_sequence`. The
        /// next send to that sequence returns a synthesized `ProduceResponse`
        /// once, with a custom name, `topic_id`, error code, offset and leader
        /// hint, and then the entry clears. It drives the terminal, routing and
        /// topic-correlation paths.
        inject_once: StdMutex<Option<Inject>>,
        /// `broker_id`s passed to `evict_broker`, in order.
        evicted: StdMutex<Vec<i32>>,
        /// `timeout_ms` of the most recent Produce request the broker received.
        last_timeout_ms: AtomicI64,
        /// Response that `refresh_metadata` returns. It is empty by
        /// default.
        refresh_response: StdMutex<MetadataResponse>,
        /// Broker ids the transport claims to have a dialable address for.
        /// This drives [`resolve_leader`]. The set is empty by default, so
        /// every send falls back to the bootstrap connection, as the original
        /// harness assumed.
        known_brokers: StdMutex<HashSet<i32>>,
        /// The `leader` argument of every `send_produce` call, in order, so a
        /// test can assert how a batch was routed.
        sent_leaders: StdMutex<Vec<Option<i32>>>,
        /// Signals each entry into `send_produce`, including injected failures
        /// before the broker model applies a request.
        send_started: Notify,
        active_sends: AtomicUsize,
        peak_active_sends: AtomicUsize,
        fail_next_sends: AtomicUsize,
        /// Count of `refresh_metadata` calls, so a test can assert the sender
        /// refreshed after a routing/transport failure.
        refreshes: AtomicUsize,
        offsets_seen: AtomicI64,
        /// Calls made through the dedicated one-way Produce transport path.
        no_response_sends: AtomicUsize,
    }

    /// A one-shot synthesized broker response, keyed by `base_sequence`. A
    /// `name` or `topic_id` of `None` echoes the request's value. A
    /// `leader_hint >= 0` sets the partition response's `current_leader`.
    #[derive(Clone)]
    struct Inject {
        seq: i32,
        name: Option<String>,
        topic_id: Option<Uuid>,
        error_code: i16,
        base_offset: i64,
        leader_hint: i32,
    }

    /// Caps the per-request reorder stagger to a bounded number of delay
    /// units, so the total sleep stays small, at
    /// `reorder_delay * REORDER_SPREAD` in the worst case. Higher sequences
    /// still complete ahead of lower ones within a single concurrent `join_all`
    /// poll.
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
                send_started: Notify::new(),
                active_sends: AtomicUsize::new(0),
                peak_active_sends: AtomicUsize::new(0),
                fail_next_sends: AtomicUsize::new(0),
                refreshes: AtomicUsize::new(0),
                offsets_seen: AtomicI64::new(0),
                no_response_sends: AtomicUsize::new(0),
            })
        }

        fn fail_once_on(self: &Arc<Self>, seq: i32) {
            *self.fail_once_seq.lock().unwrap() = Some(seq);
        }

        fn fail_next(self: &Arc<Self>, count: usize) {
            self.fail_next_sends.store(count, Ordering::Release);
        }

        fn peak_active_sends(self: &Arc<Self>) -> usize {
            self.peak_active_sends.load(Ordering::Acquire)
        }

        fn fail_once_on_leader(self: &Arc<Self>, leader: i32) {
            *self.fail_once_leader.lock().unwrap() = Some(leader);
        }

        fn delay_leader(self: &Arc<Self>, leader: i32, delay: Duration) {
            self.leader_delay.lock().unwrap().insert(leader, delay);
        }

        /// Make the next send to `seq` return, once, a `ProduceResponse` that
        /// carries `error_code`. It echoes the request's topic and gives no
        /// leader hint.
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

        /// Mark `id` as a broker the transport can dial, so `resolve_leader`
        /// routes to it instead of to the bootstrap connection.
        fn add_known_broker(self: &Arc<Self>, id: i32) {
            self.known_brokers.lock().unwrap().insert(id);
        }

        /// The `leader` argument of every `send_produce` call, in order.
        fn sent_leaders(self: &Arc<Self>) -> Vec<Option<i32>> {
            self.sent_leaders.lock().unwrap().clone()
        }

        /// Total Produce transport calls, including failures before the broker
        /// model applies the request.
        fn send_count(self: &Arc<Self>) -> usize {
            self.sent_leaders.lock().unwrap().len()
        }

        /// How many times the sender refreshed cluster metadata.
        fn refresh_count(self: &Arc<Self>) -> usize {
            self.refreshes.load(Ordering::Relaxed)
        }

        fn applied_count(self: &Arc<Self>) -> usize {
            self.applied.load(Ordering::Relaxed)
        }

        fn no_response_count(self: &Arc<Self>) -> usize {
            self.no_response_sends.load(Ordering::Relaxed)
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
            struct ActiveSend<'a>(&'a AtomicUsize);
            impl Drop for ActiveSend<'_> {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::AcqRel);
                }
            }

            let active = self.active_sends.fetch_add(1, Ordering::AcqRel) + 1;
            self.peak_active_sends.fetch_max(active, Ordering::AcqRel);
            let _active_send = ActiveSend(&self.active_sends);
            self.sent_leaders.lock().unwrap().push(leader);
            self.send_started.notify_one();
            self.last_timeout_ms
                .store(i64::from(req.timeout_ms), Ordering::Relaxed);

            if self
                .fail_next_sends
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(ClientError::Disconnected);
            }

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

        async fn send_produce_no_response(
            &self,
            leader: Option<i32>,
            req: ProduceRequest,
        ) -> Result<(), ClientError> {
            self.no_response_sends.fetch_add(1, Ordering::Relaxed);
            self.send_produce(leader, req).await.map(drop)
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
        wake_tx: tokio::sync::mpsc::Sender<DrainIntent>,
        flush_notify: Arc<Notify>,
        in_flight: Arc<AtomicUsize>,
        shutdown: CancellationToken,
        partitioner: Arc<UniformStickyPartitioner>,
        transport: Arc<MockTransport>,
        recovery_required: Arc<AtomicBool>,
        recovery_generation: Arc<AtomicU64>,
        handle: tokio::task::JoinHandle<()>,
    }

    /// Spawn a sender backed by `transport`, with `max_in_flight` and a 1ms
    /// linger, so batch deadlines expire quickly.
    fn spawn_sender(transport: Arc<MockTransport>, max_in_flight: usize) -> Harness {
        spawn_sender_with(transport, max_in_flight, millis(1))
    }

    /// Spawn a sender with an explicit `linger`. A long linger keeps the batch
    /// deadline in the future, so a test can observe wake-triggered drains in
    /// isolation.
    fn spawn_sender_with(
        transport: Arc<MockTransport>,
        max_in_flight: usize,
        linger: Time,
    ) -> Harness {
        spawn_sender_with_retries(transport, max_in_flight, linger, i32::MAX)
    }

    fn spawn_sender_with_retries(
        transport: Arc<MockTransport>,
        max_in_flight: usize,
        linger: Time,
        retries: i32,
    ) -> Harness {
        spawn_sender_with_policy(transport, max_in_flight, linger, retries, secs(30))
    }

    fn spawn_sender_with_policy(
        transport: Arc<MockTransport>,
        max_in_flight: usize,
        linger: Time,
        retries: i32,
        routing_retry_budget: Time,
    ) -> Harness {
        spawn_sender_with_acks(
            transport,
            max_in_flight,
            linger,
            retries,
            routing_retry_budget,
            Acks::All,
        )
    }

    fn spawn_sender_with_acks(
        transport: Arc<MockTransport>,
        max_in_flight: usize,
        linger: Time,
        retries: i32,
        routing_retry_budget: Time,
        acks: Acks,
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
        let recovery_required = Arc::new(AtomicBool::new(false));
        let recovery_generation = Arc::new(AtomicU64::new(0));

        // Box the same Arc<MockTransport> for the sender; keep a clone for the
        // test to inspect.
        let cfg = SenderConfig {
            transport: Box::new(ArcTransport(transport.clone())),
            producer_id: 1,
            producer_epoch: 0,
            acks,
            compression: Compression::None,
            linger,
            request_timeout_ms: 5_000,
            retries,
            retry_backoff: millis(1),
            routing_retry_budget,
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
            txn_recovery_required: Arc::clone(&recovery_required),
            txn_recovery_generation: Arc::clone(&recovery_generation),
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
            recovery_required,
            recovery_generation,
            handle,
        }
    }

    /// Append `n` records to `(topic, partition)`, each in its own batch, and
    /// return the ack receivers. The sender then allocates distinct
    /// `base_sequence`s and may pipeline them. A seal after each append forces
    /// one record per batch.
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
            let crate::accumulator::AppendResult { receiver: rx, .. } =
                a.try_append(None, Some(bytes::Bytes::from_static(b"x")), vec![], 0, None);
            // Seal so each record becomes its own ready batch with a distinct
            // base_sequence — maximizing same-partition pipelining pressure.
            a.seal_current();
            rxs.push(rx);
        }
        let _ = h.wake_tx.try_send(DrainIntent::Ready);
        rxs
    }

    /// Append `n` records to `(topic, partition)` as a SINGLE batch, with no
    /// seal between the appends, and return the ack receivers in append order.
    /// The sender seals the batch on its next drain, so the records share one
    /// `base_sequence` with `offset_delta` 0..n-1. This exercises the per-record
    /// offset arithmetic, `base_offset + offset_delta`.
    async fn produce_single_batch(
        h: &Harness,
        topic: &str,
        partition: i32,
        n: usize,
    ) -> Vec<oneshot::Receiver<Result<RecordMetadata, ProducerError>>> {
        let rxs = produce_single_batch_without_wake(h, topic, partition, n).await;
        let _ = h.wake_tx.try_send(DrainIntent::Force);
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
                let crate::accumulator::AppendResult { receiver: rx, .. } =
                    a.try_append(None, Some(bytes::Bytes::from_static(b"x")), vec![], 0, None);
                rxs.push(rx);
            }
        }
        rxs
    }

    async fn produce_ready_batches_without_wake(
        h: &Harness,
        topic: &str,
        partition: i32,
        n: usize,
    ) -> Vec<oneshot::Receiver<Result<RecordMetadata, ProducerError>>> {
        let key = (topic.to_owned(), partition);
        let accumulator = h
            .accumulators
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(Accumulator::new(16 * 1024))))
            .value()
            .clone();
        let mut receivers = Vec::with_capacity(n);
        let mut accumulator = accumulator.lock().await;
        for _ in 0..n {
            let crate::accumulator::AppendResult { receiver, .. } = accumulator.try_append(
                None,
                Some(bytes::Bytes::from_static(b"x")),
                vec![],
                0,
                None,
            );
            accumulator.seal_current();
            receivers.push(receiver);
        }
        receivers
    }

    async fn shutdown(h: Harness) {
        h.shutdown.cancel();
        let _ = h.handle.await;
    }

    #[tokio::test(start_paused = true)]
    async fn nonzero_linger_coalesces_until_the_batch_expires() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 5, millis(100));
        let rxs = produce_single_batch_without_wake(&h, "t", 0, 2).await;

        tokio::task::yield_now().await;
        assert_eq!(transport.send_count(), 0, "young batch sent before linger");

        tokio::time::advance(Duration::from_millis(99)).await;
        tokio::task::yield_now().await;
        assert_eq!(transport.send_count(), 0, "young batch sent before linger");

        tokio::time::advance(Duration::from_millis(1)).await;
        let mut offsets = Vec::new();
        for rx in rxs {
            offsets.push(
                rx.await
                    .expect("ack channel remains connected")
                    .expect("coalesced batch is acknowledged")
                    .offset,
            );
        }
        assert_eq!(offsets, vec![0, 1]);
        assert_eq!(transport.send_count(), 1);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn rollover_wake_sends_ready_only_and_leaves_young_currents_open() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 5, millis(100));
        tokio::task::yield_now().await;

        let rollover = Arc::new(Mutex::new(Accumulator::new(20)));
        h.accumulators
            .insert(("t".to_owned(), 0), Arc::clone(&rollover));
        let (ready_rx, current_rx) = {
            let mut accumulator = rollover.lock().await;
            let crate::accumulator::AppendResult {
                receiver: ready, ..
            } = accumulator.try_append(
                None,
                Some(bytes::Bytes::from_static(b"a")),
                vec![],
                0,
                None,
            );
            let crate::accumulator::AppendResult {
                receiver: current, ..
            } = accumulator.try_append(
                None,
                Some(bytes::Bytes::from_static(b"b")),
                vec![],
                0,
                None,
            );
            (ready, current)
        };

        let unrelated = Arc::new(Mutex::new(Accumulator::new(1024)));
        h.accumulators
            .insert(("t".to_owned(), 1), Arc::clone(&unrelated));
        let crate::accumulator::AppendResult {
            receiver: unrelated_rx,
            ..
        } = unrelated.lock().await.try_append(
            None,
            Some(bytes::Bytes::from_static(b"young")),
            vec![],
            0,
            None,
        );

        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");
        ready_rx
            .await
            .expect("ready ack channel remains connected")
            .expect("ready rollover batch is acknowledged");

        assert_eq!(transport.send_count(), 1);
        assert!(rollover.lock().await.current.is_some());
        assert!(unrelated.lock().await.current.is_some());

        drop((current_rx, unrelated_rx));
        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn zero_linger_force_wake_sends_without_advancing_time() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 5, secs(0));
        tokio::task::yield_now().await;
        let mut rxs = produce_single_batch_without_wake(&h, "t", 0, 1).await;

        h.wake_tx
            .send(DrainIntent::Force)
            .await
            .expect("sender is running");
        rxs.remove(0)
            .await
            .expect("ack channel remains connected")
            .expect("zero-linger batch is acknowledged");
        assert_eq!(transport.send_count(), 1);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn explicit_flush_intent_bypasses_nonzero_linger() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 5, minutes(1));
        tokio::task::yield_now().await;
        let mut rxs = produce_single_batch_without_wake(&h, "t", 0, 1).await;

        h.wake_tx
            .send(DrainIntent::Force)
            .await
            .expect("sender is running");
        rxs.remove(0)
            .await
            .expect("ack channel remains connected")
            .expect("explicitly flushed batch is acknowledged");
        assert_eq!(transport.send_count(), 1);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn off_phase_append_sends_at_its_own_linger_deadline() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 5, millis(100));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(50)).await;
        let mut receivers = produce_single_batch_without_wake(&h, "t", 0, 1).await;
        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");

        tokio::time::advance(Duration::from_millis(99)).await;
        tokio::task::yield_now().await;
        assert_eq!(transport.send_count(), 0);

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(transport.send_count(), 1);
        receivers
            .remove(0)
            .await
            .expect("ack channel remains connected")
            .expect("batch is acknowledged at its own deadline");

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn force_drains_more_partitions_than_max_in_flight_without_linger_wait() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 2, minutes(1));
        tokio::task::yield_now().await;
        let start = Instant::now();
        let mut receivers = Vec::new();
        for partition in 0..6 {
            receivers.extend(produce_single_batch_without_wake(&h, "t", partition, 1).await);
        }

        h.wake_tx
            .send(DrainIntent::Force)
            .await
            .expect("sender is running");
        for receiver in receivers {
            receiver
                .await
                .expect("ack channel remains connected")
                .expect("forced batch is acknowledged");
        }
        assert_eq!(Instant::now(), start);
        assert_eq!(transport.send_count(), 6);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn eligible_retries_never_exceed_max_in_flight() {
        let transport = MockTransport::new(Duration::from_millis(1));
        transport.fail_next(6);
        let h = spawn_sender_with(transport.clone(), 2, minutes(1));
        let mut receivers = Vec::new();
        for partition in 0..6 {
            receivers.extend(produce_ready_batches_without_wake(&h, "t", partition, 1).await);
        }

        h.wake_tx
            .send(DrainIntent::Force)
            .await
            .expect("sender is running");
        while transport.send_count() < 6 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(1)).await;

        for receiver in receivers {
            receiver
                .await
                .expect("ack channel remains connected")
                .expect("retry is acknowledged");
        }
        assert_eq!(transport.send_count(), 12);
        assert_eq!(transport.peak_active_sends(), 2);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn force_drains_multiple_ready_batches_from_one_partition_without_linger_wait() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 5, minutes(1));
        tokio::task::yield_now().await;
        let start = Instant::now();
        let receivers = produce_ready_batches_without_wake(&h, "t", 0, 3).await;

        h.wake_tx
            .send(DrainIntent::Force)
            .await
            .expect("sender is running");
        for receiver in receivers {
            receiver
                .await
                .expect("ack channel remains connected")
                .expect("forced batch is acknowledged");
        }
        assert_eq!(Instant::now(), start);
        assert_eq!(transport.send_count(), 3);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn one_ready_wake_drains_coalesced_backlog_past_the_cycle_cap() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 2, minutes(1));
        tokio::task::yield_now().await;
        let start = Instant::now();
        let mut receivers = Vec::new();
        for partition in 0..6 {
            receivers.extend(produce_ready_batches_without_wake(&h, "t", partition, 1).await);
        }

        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");
        for receiver in receivers {
            receiver
                .await
                .expect("ack channel remains connected")
                .expect("ready batch is acknowledged");
        }
        assert_eq!(Instant::now(), start);
        assert_eq!(transport.send_count(), 6);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn retry_release_resumes_same_partition_ready_backlog() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.fail_once_on(0);
        let h = spawn_sender_with(transport.clone(), 5, minutes(1));
        tokio::task::yield_now().await;
        let start = Instant::now();
        let receivers = produce_ready_batches_without_wake(&h, "t", 0, 2).await;
        let first_send = transport.send_started.notified();
        tokio::pin!(first_send);
        first_send.as_mut().enable();

        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");
        first_send.await;
        for receiver in receivers {
            receiver
                .await
                .expect("ack channel remains connected")
                .expect("retry and queued batch are acknowledged");
        }
        assert_eq!(
            Instant::now().duration_since(start),
            Duration::from_millis(1)
        );
        assert_eq!(transport.send_count(), 3);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_drains_multiple_batches_without_waiting_for_linger() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 5, minutes(1));
        tokio::task::yield_now().await;
        let start = Instant::now();
        let receivers = produce_ready_batches_without_wake(&h, "t", 0, 3).await;

        h.shutdown.cancel();
        h.handle.await.expect("sender shuts down cleanly");
        assert_eq!(Instant::now(), start);
        assert_eq!(transport.send_count(), 3);
        drop(receivers);
    }

    #[tokio::test(start_paused = true)]
    async fn channel_close_waits_for_retry_deadline_not_linger() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.fail_once_on(0);
        let h = spawn_sender_with(transport.clone(), 5, minutes(1));
        tokio::task::yield_now().await;
        let mut receivers = produce_ready_batches_without_wake(&h, "t", 0, 1).await;
        let first_send = transport.send_started.notified();
        tokio::pin!(first_send);
        first_send.as_mut().enable();
        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");
        first_send.await;
        let start = Instant::now();

        drop(h.wake_tx);
        h.handle.await.expect("closed sender drains cleanly");
        assert_eq!(
            Instant::now().duration_since(start),
            Duration::from_millis(1)
        );
        assert_eq!(transport.send_count(), 2);
        receivers
            .remove(0)
            .await
            .expect("ack channel remains connected")
            .expect("retry is acknowledged before close");
    }

    /// THE REGRESSION TEST for the same-partition pipelining hang.
    ///
    /// The test bursts many single-record batches at ONE partition through the
    /// real sender loop. The broker enforces strict per-partition sequencing AND
    /// a reorder model that completes same-partition requests issued
    /// *concurrently* higher-`base_sequence`-first. That models the on-the-wire
    /// write race of the old `join_all` same-partition pipelining.
    ///
    /// With the fix, one in flight per partition, a partition only ever has one
    /// request outstanding, so the staggered transport delay cannot reorder
    /// anything. The broker sees `base_sequence` 0,1,2,… exactly once each,
    /// every record acks `Ok` with offsets in order, and there is **zero retry
    /// churn**, that is exactly `N` broker applies.
    ///
    /// The `applied == N` assertion is the teeth. The old multi-in-flight design
    /// fed reordered concurrent requests to the broker, drew
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER`, drained, and resent, so it applied
    /// strictly more than `N`. Under sustained load on a cluster it churned long
    /// enough that the caller's time-boxed window saw a record's ack-oneshot
    /// still unresolved. That was the reported hang.
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
            assert2::assert!(md.partition == 0);
            offsets.push(md.offset);
        }

        // Offsets must be the clean increasing sequence 0..N — proof the broker
        // saw each base_sequence exactly once, in order.
        let expected: Vec<i64> = (0..i64::try_from(N).unwrap()).collect();
        assert2::assert!(offsets == expected);
        // Zero churn: with one in-flight per partition there is never an
        // out-of-order arrival, so the broker applies each batch exactly once.
        assert2::assert!(h.transport.applied_count() == N);

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
                assert2::assert!(md.partition == p);
                offsets.push(md.offset);
            }
            let expected: Vec<i64> = (0..i64::try_from(PER).unwrap()).collect();
            assert2::assert!(offsets == expected);
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

        assert2::assert!(h.partitioner.pick("t", None, 3) == 0);

        let mut rxs = produce_single_batch(&h, "t", 0, 1).await;
        tokio::time::timeout(Duration::from_secs(5), rxs.remove(0))
            .await
            .expect("record ack should resolve")
            .expect("oneshot sender should stay alive")
            .expect("record should ack");

        assert2::assert!(h.partitioner.pick("t", None, 3) == 1);

        shutdown(h).await;
    }

    /// A one-shot transport error mid-stream must NOT drop or reorder. The
    /// sender resends the failed batch. The broker dedups it with DUPLICATE if
    /// it had landed, or accepts it fresh. All acks resolve, and the offsets
    /// stay a clean increasing run.
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
        assert2::assert!(offsets == expected);

        // in_flight must fully drain back to zero (it lags the last ack-oneshot
        // by the `finish_in_flight` decrement, so poll via `flush_notify`).
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            while h.in_flight.load(Ordering::Acquire) != 0 {
                let _ = tokio::time::timeout(Duration::from_millis(20), h.flush_notify.notified())
                    .await;
            }
        })
        .await;
        assert2::assert!(drained.is_ok());
        // A transport failure forces a metadata refresh so the resend re-resolves
        // the leader; the sender must have refreshed at least once.
        assert2::assert!(h.transport.refresh_count() >= 1);
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

        let refresh_count = h.transport.refresh_count();
        check!(
            (
                md.partition,
                md.offset,
                (1..=2).contains(&refresh_count),
                h.transport.sent_leaders(),
                h.transport.evicted(),
            ) == (0, 0, true, vec![Some(0), Some(1)], vec![0]),
            "failover must refresh once without churn, evict the stale leader, and reroute"
        );

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
        let _ = h.wake_tx.try_send(DrainIntent::Force);

        let live_md = tokio::time::timeout(Duration::from_millis(100), live_rx.remove(0))
            .await
            .expect("live partition ack should not wait for a slow dead leader")
            .expect("oneshot sender should stay alive")
            .expect("live partition should ack Ok");
        assert2::assert!((live_md.partition, live_md.offset) == (1, 0));

        let dead_md = tokio::time::timeout(Duration::from_secs(5), dead_rx.remove(0))
            .await
            .expect("dead leader partition should resolve after reroute")
            .expect("oneshot sender should stay alive")
            .expect("dead leader partition should ack after reroute");
        assert2::assert!((dead_md.partition, dead_md.offset) == (0, 0));

        let sent = h.transport.sent_leaders();
        assert2::assert!(sent.len() == 3 && sent[2] == Some(1));
        assert2::assert!(sent[..2].contains(&Some(0)) && sent[..2].contains(&Some(6)));

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
        assert2::assert!(drained.is_ok());
        // And the broker applied each batch exactly once (no churn).
        assert2::assert!(h.transport.applied_count() == 20);
        let _ = &h.next_seq;
        shutdown(h).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn acks_zero_uses_one_way_transport_and_returns_unknown_offset() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with_acks(
            transport.clone(),
            1,
            millis(1),
            i32::MAX,
            secs(30),
            Acks::Zero,
        );

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        let metadata = tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("record resolves")
            .expect("sender remains")
            .expect("enqueue succeeds");

        check!(metadata.offset == -1);
        check!(transport.no_response_count() == 1);
        check!(transport.applied_count() == 1);

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
    /// The test uses paused virtual time, so the staggered sleeps order the
    /// arrivals deterministically and do not rely on the OS scheduler.
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
        assert2::assert!(codes[0] == codes::NONE);
        for c in &codes[1..] {
            assert2::assert!(*c == codes::OUT_OF_ORDER_SEQUENCE_NUMBER);
        }

        // Arrivals were applied highest-first (the reorder), confirming the race.
        let arrivals: Vec<i32> = transport
            .arrivals
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, s)| *s)
            .collect();
        assert2::assert!(arrivals == vec![4, 3, 2, 1, 0]);
    }

    /// A partition with a batch pending resend must NOT also send its next
    /// batch in the same cycle. Under a broker that reorders concurrent
    /// same-partition requests, the new batch could otherwise overtake the
    /// resend and trip `OUT_OF_ORDER_SEQUENCE_NUMBER` churn.
    ///
    /// A one-shot transport error parks one batch for resend mid-stream. With
    /// the reorder model active, the test still expects each batch applied
    /// exactly once, with no churn, and offsets in a clean run. This guards the
    /// "ordering preserved by construction" property of the one-slot-per-
    /// partition pipeline against a same-partition send race.
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
        assert2::assert!(offsets == expected);
        // The failed send errored at the transport before the broker applied it,
        // so each of the N batches is applied exactly once: a new batch never
        // raced (and reordered ahead of) the pending resend.
        assert2::assert!(h.transport.applied_count() == N);

        shutdown(h).await;
    }

    /// Routing decision in `resolve_leader`. A partition whose cached leader is
    /// a known, dialable broker goes to that broker. A partition whose leader is
    /// unknown, or whose address the pool cannot dial, falls back to the
    /// bootstrap connection. The test drives the real sender, so it observes the
    /// `leader` argument handed to the transport directly.
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
            (
                leaders.contains(&Some(5)),
                leaders.contains(&None),
                leaders.contains(&Some(7)),
            ) == (true, true, false),
            "known, bootstrap-fallback, and unknown-address leader routing: {leaders:?}"
        );

        shutdown(h).await;
    }

    /// A terminal but non-fatal server error, that is an unmodeled code, fails
    /// the record with `Server(code)` and releases its in-flight slot. It must
    /// not fence, it must not hang, and it must not be retried forever.
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
        assert2::assert!(matches!(err, ProducerError::Server(MESSAGE_TOO_LARGE)));

        // The slot is released: in_flight drains back to zero.
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            while h.in_flight.load(Ordering::Acquire) != 0 {
                let _ = tokio::time::timeout(Duration::from_millis(20), h.flush_notify.notified())
                    .await;
            }
        })
        .await;
        assert2::assert!(drained.is_ok());

        shutdown(h).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exhausted_retry_fences_before_a_sequence_gap_can_be_sent() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.inject_code_once(0, codes::NOT_LEADER_OR_FOLLOWER);
        let h = spawn_sender_with_retries(transport.clone(), 1, millis(1), 0);

        let first = produce_burst(&h, "t", 0, 1).await.pop().expect("first ack");
        let first_error = tokio::time::timeout(Duration::from_secs(1), first)
            .await
            .expect("first ack resolves")
            .expect("first sender remains")
            .expect_err("exhausted batch must fail");
        assert2::assert!(matches!(first_error, ProducerError::FencedProducer));
        assert2::assert!(h.state.load(Ordering::Acquire) == STATE_FENCED);

        let second = produce_burst(&h, "t", 0, 1)
            .await
            .pop()
            .expect("second ack");
        let second_error = tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .expect("second ack resolves")
            .expect("second sender remains")
            .expect_err("fenced producer rejects later records");
        assert2::assert!(matches!(second_error, ProducerError::FencedProducer));
        assert2::assert!(transport.send_count() == 1);

        shutdown(h).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_exhaustion_preserves_concurrent_successful_ack() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.add_known_broker(0);
        transport.add_known_broker(1);
        transport.fail_once_on_leader(0);
        transport.delay_leader(1, Duration::from_millis(20));
        let h = spawn_sender_with_retries(transport.clone(), 2, secs(1), 0);
        h.partition_leaders.insert(("t".to_owned(), 0), 0);
        h.partition_leaders.insert(("t".to_owned(), 1), 1);

        let mut failed = produce_single_batch_without_wake(&h, "t", 0, 1).await;
        let mut accepted = produce_single_batch_without_wake(&h, "t", 1, 1).await;
        let _ = h.wake_tx.try_send(DrainIntent::Force);

        let failed_error = tokio::time::timeout(Duration::from_secs(1), failed.remove(0))
            .await
            .expect("failed ack resolves")
            .expect("failed sender remains")
            .expect_err("exhausted partition must fence");
        let accepted_metadata = tokio::time::timeout(Duration::from_secs(1), accepted.remove(0))
            .await
            .expect("accepted ack resolves")
            .expect("accepted sender remains")
            .expect("broker-accepted partition must remain acknowledged");

        assert2::assert!(matches!(failed_error, ProducerError::FencedProducer));
        assert2::assert!((accepted_metadata.partition, accepted_metadata.offset) == (1, 0));
        assert2::assert!(h.state.load(Ordering::Acquire) == STATE_FENCED);
        assert2::assert!(transport.send_count() == 2);
        shutdown(h).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exhausted_routing_budget_fences_the_producer() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.inject_code_once(0, codes::NOT_LEADER_OR_FOLLOWER);
        let h = spawn_sender_with_policy(transport, 1, millis(1), i32::MAX, millis(1));

        let ack = produce_burst(&h, "t", 0, 1).await.pop().expect("ack");
        let error = tokio::time::timeout(Duration::from_secs(1), ack)
            .await
            .expect("ack resolves")
            .expect("sender remains")
            .expect_err("expired batch must fail");

        assert2::assert!(matches!(error, ProducerError::FencedProducer));
        assert2::assert!(h.state.load(Ordering::Acquire) == STATE_FENCED);
        shutdown(h).await;
    }

    /// A batch with several records gives each record
    /// `base_offset + offset_delta`. The other tests use one record per batch,
    /// where `offset_delta` is always 0. This test pins the per-record offset
    /// arithmetic.
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
            assert2::assert!(md.partition == 0);
            offsets.push(md.offset);
        }
        // One batch at base_offset 0, records at deltas 0..N-1 → offsets 0,1,2,3.
        // Under `base_offset - offset_delta` these would be 0,-1,-2,-3.
        let expected: Vec<i64> = (0..i64::try_from(N).unwrap()).collect();
        assert2::assert!(offsets == expected);

        shutdown(h).await;
    }

    /// A fatal `INVALID_PRODUCER_EPOCH` fences the producer. The record fails
    /// with `FencedProducer`, and the shared state flips to `STATE_FENCED`.
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
        assert2::assert!(matches!(err, ProducerError::FencedProducer));
        assert2::assert!(h.state.load(Ordering::Acquire) == STATE_FENCED);

        shutdown(h).await;
    }

    /// A transport failure to a *known* leader evicts that broker's connection,
    /// so a reconnect targets its current address. The batch then resends and
    /// acks.
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

        assert2::assert!(h.transport.evicted().contains(&5));

        shutdown(h).await;
    }

    /// On `NOT_LEADER_OR_FOLLOWER` with an inline `current_leader` hint to a
    /// *known* broker, the sender adopts the hint WITHOUT a metadata refresh. It
    /// routes the resend there and updates its leader cache.
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
            (
                leaders.contains(&Some(5)),
                leaders.contains(&Some(8)),
                h.partition_leaders
                    .get(&("t".to_string(), 0))
                    .map(|e| *e.value()),
                h.transport.refresh_count(),
            ) == (true, true, Some(8), 0),
            "inline hint must reroute, update the cache, and avoid metadata refresh: {leaders:?}"
        );

        shutdown(h).await;
    }

    /// The sender correlates a Produce response to its batch by `topic_id` when
    /// the response's topic NAME differs, because Kafka v13+ omits the name. The
    /// injected response carries the matching `topic_id` and a distinctive
    /// offset.
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
        assert2::assert!(md.offset == 42);

        shutdown(h).await;
    }

    /// `&&`, not `||`, gates the `topic_id` fallback. A response whose name
    /// does NOT match, and whose `topic_id` is ZERO, must NOT be correlated. The
    /// batch has no `topic_id`, that is ZERO, so only an exact name match binds
    /// a response, and a wrong-name response forces a resend.
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
        assert2::assert!(md.offset == 0);

        shutdown(h).await;
    }

    /// `update_leaders_from_metadata` adopts leaders only from HEALTHY topics,
    /// that is `error_code == 0`. A transport error triggers a refresh whose
    /// response advertises a new leader for a healthy topic, and the cache picks
    /// it up.
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

        assert2::assert!(
            h.partition_leaders
                .get(&("t".to_string(), 0))
                .map(|e| *e.value())
                == Some(9)
        );

        shutdown(h).await;
    }

    /// The Produce request carries the configured `request_timeout` as
    /// `timeout_ms`, so 5s becomes 5000ms on the wire.
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

        assert2::assert!(h.transport.last_timeout_ms() == 5000);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_fails_accumulator_batches_even_behind_same_partition_retry() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.fail_next(1);
        let h = spawn_sender_with(transport.clone(), 1, minutes(1));
        let retry_receiver = produce_ready_batches_without_wake(&h, "t", 0, 1)
            .await
            .remove(0);
        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");
        while transport.send_count() < 1 {
            tokio::task::yield_now().await;
        }

        let accumulator = h
            .accumulators
            .get(&("t".to_owned(), 0))
            .expect("accumulator exists")
            .value()
            .clone();
        let (ready_receiver, current_receiver) = {
            let mut accumulator = accumulator.lock().await;
            let crate::accumulator::AppendResult {
                receiver: ready_receiver,
                ..
            } = accumulator.try_append(
                None,
                Some(bytes::Bytes::from_static(b"old-ready")),
                vec![],
                0,
                Some(0),
            );
            accumulator.seal_current();
            let crate::accumulator::AppendResult {
                receiver: current_receiver,
                ..
            } = accumulator.try_append(
                None,
                Some(bytes::Bytes::from_static(b"old-current")),
                vec![],
                0,
                Some(0),
            );
            (ready_receiver, current_receiver)
        };

        h.recovery_generation.store(1, Ordering::Release);
        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");
        for receiver in [ready_receiver, current_receiver] {
            let result = receiver
                .await
                .expect("recovery acknowledgement channel remains connected");
            assert!(matches!(result, Err(ProducerError::RecoveryRequired)));
        }
        assert_eq!(
            transport.send_count(),
            1,
            "old transactional accumulator batches must fail before retry release"
        );
        assert_eq!(
            h.in_flight.load(Ordering::Acquire),
            1,
            "undrained batches must not decrement the retry's in-flight slot"
        );

        tokio::time::advance(Duration::from_millis(1)).await;
        retry_receiver
            .await
            .expect("retry acknowledgement channel remains connected")
            .expect("nontransactional retry is acknowledged");
        shutdown(h).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_transactional_batch_is_failed_after_recovery_before_reinitialization() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 1, secs(30));
        let accumulator = Arc::new(Mutex::new(Accumulator::new(1024)));
        h.accumulators
            .insert(("t".to_string(), 0), Arc::clone(&accumulator));
        let crate::accumulator::AppendResult { receiver: rx, .. } =
            accumulator.lock().await.try_append(
                None,
                Some(bytes::Bytes::from_static(b"old")),
                vec![],
                0,
                Some(0),
            );

        h.recovery_required.store(true, Ordering::Release);
        h.recovery_generation.store(1, Ordering::Release);
        // Simulate a completed InitProducerId before the sender gets to drain:
        // the old generation must still be rejected under the new epoch.
        h.recovery_required.store(false, Ordering::Release);
        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");

        let acknowledgement = tokio::time::timeout(Duration::from_secs(3), rx)
            .await
            .expect("recovery must resolve queued acknowledgement")
            .expect("acknowledgement channel remains connected");
        assert!(matches!(
            acknowledgement,
            Err(ProducerError::RecoveryRequired)
        ));
        assert_eq!(transport.applied.load(Ordering::Acquire), 0);

        shutdown(h).await;
    }

    /// A transport-failed transactional batch occupies the retry slot. Once
    /// reinitialization advances the recovery generation, that slot must fail
    /// locally instead of resending a batch from the prior transaction epoch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_slot_transactional_batch_is_failed_after_recovery_without_resend() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.fail_once_on(0);
        let h = spawn_sender_with(transport.clone(), 1, secs(30));
        let accumulator = Arc::new(Mutex::new(Accumulator::new(1024)));
        h.accumulators
            .insert(("t".to_string(), 0), Arc::clone(&accumulator));
        let crate::accumulator::AppendResult { receiver: rx, .. } =
            accumulator.lock().await.try_append(
                None,
                Some(bytes::Bytes::from_static(b"old")),
                vec![],
                0,
                Some(0),
            );

        let initial_send = transport.send_started.notified();
        h.wake_tx
            .send(DrainIntent::Force)
            .await
            .expect("sender is running");
        tokio::time::timeout(Duration::from_secs(3), initial_send)
            .await
            .expect("transactional batch should reach the controlled transport failure");
        assert_eq!(
            transport.send_count(),
            1,
            "initial send must fail exactly once"
        );

        // Mirror successful reinitialization: the epoch generation advances and
        // the recovery barrier is lifted before the sender examines its retry slot.
        h.recovery_required.store(true, Ordering::Release);
        h.recovery_generation.store(1, Ordering::Release);
        h.recovery_required.store(false, Ordering::Release);
        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");

        let acknowledgement = tokio::time::timeout(Duration::from_secs(3), rx)
            .await
            .expect("recovery must resolve retry-slot acknowledgement")
            .expect("acknowledgement channel remains connected");
        assert!(matches!(
            acknowledgement,
            Err(ProducerError::RecoveryRequired)
        ));
        assert_eq!(
            transport.send_count(),
            1,
            "a retry-slot batch from the old generation must not resend"
        );

        shutdown(h).await;
    }

    /// `finish_in_flight` notifies flush waiters exactly when `in_flight`
    /// reaches zero. With a long linger, wakes trigger the only early drains, so
    /// this notify is the only one a registered waiter can receive.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn finish_in_flight_notifies_when_drained() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 5, secs(30));

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
        assert2::assert!(fired.is_ok());

        let _ = tokio::time::timeout(Duration::from_secs(2), rx).await;
        shutdown(h).await;
    }
}
