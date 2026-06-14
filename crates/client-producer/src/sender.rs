//! Background sender task. Drains ready batches from every accumulator
//! and ships them as `ProduceRequest`s through `crabka-client-core`.
//!
//! The sender is `tokio::spawn`'d by the builder. It owns the `wake_rx`
//! `Receiver` end of the wake channel (the `Producer` holds the
//! `wake_tx` `Sender`), the `flush_notify`, the `accumulators` map, and
//! the `next_seq` map. On every linger tick or wake signal it walks the
//! accumulators, seals + drains a batch from each, and builds a v2
//! `RecordBatch` per partition (allocating its `base_sequence`). It then
//! groups the drained batches by partition-**leader** and sends one
//! `ProduceRequest` per leader via `Client::broker(id)` — falling back to the
//! bootstrap `Client::send` when the leader is unknown — re-routing on
//! `NOT_LEADER_OR_FOLLOWER` / `UNKNOWN_TOPIC_OR_PARTITION`, and resolving each
//! record's `oneshot::Sender` from the per-partition response.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;
use crabka_compression::CompressionType;
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::primitives::uuid::Uuid;
use crabka_protocol::records::{Attributes, Record, RecordBatch, RecordHeader};

use crate::accumulator::{Accumulator, InProgressBatch, PendingRecord};
use crate::compression::Compression;
use crate::error::ProducerError;
use crate::producer::{Acks, STATE_ACTIVE, STATE_FENCED, TopicMetadata};
use crate::record::RecordMetadata;
use crate::transactional::TxnState;

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

/// Wall-clock budget for routing a batch to a reachable leader across one
/// `send_to_leaders` cycle. Spans a typical failover leader re-election (the
/// broker session timeout is single-digit seconds) with margin, then fails the
/// still-unroutable batch so the caller's ack/`flush` resolves instead of
/// hanging forever. The retry is iterative (no recursion), so this never grows
/// the worker stack no matter how many rounds elapse.
const ROUTING_RETRY_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

/// All the bits of state the sender task needs. The builder constructs
/// one of these, hands it to [`run`], and drops it.
#[allow(clippy::type_complexity)] // accumulators map mirrors the Producer field; alias deferred.
pub(crate) struct SenderConfig {
    pub client: Client,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub acks: Acks,
    pub compression: Compression,
    pub linger: Duration,
    pub request_timeout: Duration,
    pub retries: i32,
    pub retry_backoff: Duration,
    pub metadata_cache: Arc<Mutex<HashMap<String, TopicMetadata>>>,
    /// Per-`(topic, partition)` leader-id cache, shared with the `Producer`.
    /// Populated from `Metadata` (see `Producer::partitions_for`); the sender
    /// consults it to route each Produce to the partition leader and refreshes
    /// it on `NOT_LEADER_OR_FOLLOWER` / `UNKNOWN_TOPIC_OR_PARTITION`.
    pub partition_leaders: Arc<DashMap<(String, i32), i32>>,
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

pub(crate) async fn run(mut cfg: SenderConfig) {
    let mut ticker = tokio::time::interval(cfg.linger.max(Duration::from_millis(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            () = cfg.shutdown.cancelled() => break,
            _ = ticker.tick() => {
                drain_once(&mut cfg).await;
            }
            _ = cfg.wake_rx.recv() => {
                drain_once(&mut cfg).await;
            }
        }
    }

    // Drain anything left when we shut down so `close()` doesn't drop records.
    drain_once(&mut cfg).await;
}

/// One drained partition's batch, prepared for sending: the encoded v2
/// `RecordBatch` (with its `base_sequence` already allocated), the topic id, and
/// the `PendingRecord`s whose oneshot acks the response resolves.
///
/// The `record_batch` is built **once** (sequence allocated once) so a
/// re-route or retry resends the identical bytes — preserving per-partition
/// idempotent sequencing: the leader sees each partition's `base_sequence`
/// exactly once, in increasing order, regardless of which broker it reaches.
struct PreparedBatch {
    topic: String,
    partition: i32,
    topic_id: Uuid,
    record_batch: RecordBatch,
    records: Vec<PendingRecord>,
}

/// Walk every accumulator, seal its current batch, pop one ready batch, build
/// the per-partition `RecordBatch` (allocating its `base_sequence`), then group
/// the prepared batches by partition-leader and send one `ProduceRequest` per
/// leader. Notifies `flush_notify` when there was nothing to do — that's the
/// signal `Producer::flush` waits on.
///
/// **Per-partition ordering / idempotence.** Each `(topic, partition)`
/// contributes at most ONE batch per drain cycle, and `drain_once` runs to
/// completion (all per-leader sends + their bounded retries) before the next
/// cycle. Sequences are allocated per-partition as each batch is built, so a
/// partition's batches still reach its leader in strictly increasing
/// `base_sequence` order with no gaps or reordering — routing changes only the
/// destination broker, never the order or the allocated sequence.
async fn drain_once(cfg: &mut SenderConfig) {
    let keys: Vec<(String, i32)> = cfg.accumulators.iter().map(|e| e.key().clone()).collect();

    // 1. Drain one ready batch per partition and prepare it (build the v2
    //    RecordBatch, allocating its base_sequence). `in_flight` is incremented
    //    per drained batch while the accumulator lock is held, exactly as
    //    before, so a concurrent `flush` never sees a batch that is neither in
    //    the accumulator nor counted in flight. We decrement once per prepared
    //    batch after its leader's request completes (success or failure).
    let mut prepared: Vec<PreparedBatch> = Vec::new();
    for key in keys {
        let acc = match cfg.accumulators.get(&key) {
            Some(a) => a.value().clone(),
            None => continue,
        };
        let batch = {
            let mut a = acc.lock().await;
            a.seal_current();
            let b = a.ready.pop_front();
            if b.is_some() {
                cfg.in_flight.fetch_add(1, Ordering::AcqRel);
            }
            b
        };
        let Some(batch) = batch else { continue };
        prepared.push(prepare_batch(cfg, &key.0, key.1, batch).await);
    }

    if prepared.is_empty() {
        cfg.flush_notify.notify_waiters();
        return;
    }

    // 2. Group prepared batches by their partition leader, then send one
    //    ProduceRequest per leader. `send_to_leaders` decrements `in_flight`
    //    once per batch as each completes and wakes any `flush` waiter when the
    //    last in-flight batch lands.
    send_to_leaders(cfg, prepared).await;
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
        Some(id) if id >= 0 && cfg.client.knows_broker(id) => id,
        _ => BOOTSTRAP_LEADER,
    }
}

/// Build the v2 `RecordBatch` for a drained partition batch, allocating its
/// `base_sequence` range from `next_seq`. The result is sent (and any retry
/// resent) verbatim, so the sequence is allocated exactly once per batch.
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
        record_batch,
        records: batch.records,
    }
}

/// Outcome of one [`send_leader_group`] call.
struct LeaderSendOutcome {
    /// Batches that came back mis-routed (`NOT_LEADER`/`UNKNOWN`) or whose
    /// leader's connection failed — still in-flight, to be re-resolved.
    reroute: Vec<PreparedBatch>,
    /// A metadata refresh is required before the re-route can route correctly
    /// (no usable inline leader hint, or the hinted leader's address is unknown).
    refresh_needed: bool,
}

/// Drive the prepared batches to their partition leaders, re-routing any that
/// come back mis-routed or whose leader's connection failed (broker bounce /
/// failover) until every batch reaches a terminal outcome (ack/fail) or the
/// routing budget elapses.
///
/// Iterative — it re-groups and re-resolves each round rather than recursing —
/// so a partition whose leader takes several seconds to re-elect can be retried
/// across the whole window without growing the worker stack.
async fn send_to_leaders(cfg: &SenderConfig, mut prepared: Vec<PreparedBatch>) {
    let deadline = std::time::Instant::now() + ROUTING_RETRY_BUDGET;
    let mut round: i32 = 0;
    loop {
        // Group by leader id. Resolution is a synchronous registry lookup (no
        // await), so the cache is read here and the per-leader sends happen
        // below with no lock held across the `.await`. Sequential sends keep a
        // single parked leader from starving the others past the per-request
        // timeout, mirroring the consumer's poll.
        let mut by_leader: HashMap<i32, Vec<PreparedBatch>> = HashMap::new();
        for pb in prepared.drain(..) {
            let leader = resolve_leader(cfg, &pb.topic, pb.partition);
            by_leader.entry(leader).or_default().push(pb);
        }

        let mut to_reroute: Vec<PreparedBatch> = Vec::new();
        let mut refresh_needed = false;
        for (leader, batches) in by_leader {
            let outcome = send_leader_group(cfg, leader, batches).await;
            to_reroute.extend(outcome.reroute);
            refresh_needed |= outcome.refresh_needed;
        }

        if to_reroute.is_empty() {
            return;
        }

        round += 1;
        if round > cfg.retries || std::time::Instant::now() >= deadline {
            // Out of routing budget: fail the still-misrouted batches with the
            // routing error so the caller sees a real Server error, and release
            // each in-flight slot.
            for pb in to_reroute {
                fail_batch(
                    pb.records,
                    ProducerError::Server(codes::NOT_LEADER_OR_FOLLOWER),
                );
                finish_in_flight(cfg);
            }
            return;
        }

        // Learn the partition→leader map the cluster (re-)elected so the next
        // round routes correctly, then back off to avoid a hot loop while a
        // leader is mid-election. A pure hint adoption (refresh not needed)
        // re-resolves immediately on the next round without a round-trip.
        if refresh_needed {
            update_leaders_from_metadata(cfg).await;
        }
        tokio::time::sleep(cfg.retry_backoff).await;
        prepared = to_reroute;
    }
}

/// Refresh cluster metadata and adopt the fresh partition→leader map. The
/// refresh also re-populates the pool's broker-address registry, so a leader
/// re-elected onto a broker the pool hadn't dialed becomes routable.
async fn update_leaders_from_metadata(cfg: &SenderConfig) {
    if let Ok(md) = cfg.client.refresh_metadata().await {
        for t in &md.topics {
            let Some(name) = &t.name else { continue };
            if t.error_code != 0 {
                continue;
            }
            for p in &t.partitions {
                cfg.partition_leaders
                    .insert((name.clone(), p.partition_index), p.leader_id);
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

/// Send one `ProduceRequest` carrying every batch routed to `leader`. Returns
/// the batches that came back mis-routed (`NOT_LEADER_OR_FOLLOWER` /
/// `UNKNOWN_TOPIC_OR_PARTITION`) or whose leader's connection failed, for the
/// caller ([`send_to_leaders`]) to re-resolve and resend (same allocated
/// `base_sequence`, so sequencing stays monotonic). Each batch's `in_flight`
/// slot is released exactly once, when its terminal outcome (ack or failure) is
/// reached; a re-routed batch stays held until then.
#[allow(clippy::too_many_lines)] // single per-leader send+response state machine; splitting hurts readability
async fn send_leader_group(
    cfg: &SenderConfig,
    leader: i32,
    batches: Vec<PreparedBatch>,
) -> LeaderSendOutcome {
    // Frame a multi-topic ProduceRequest from the grouped batches. Partition
    // data for the same topic is merged under one TopicProduceData entry.
    let req = build_produce_request(cfg, &batches);

    let mut attempts: i32 = 0;
    let resp = loop {
        attempts += 1;
        let send = if leader == BOOTSTRAP_LEADER {
            cfg.client.send(req.clone()).await
        } else {
            cfg.client.broker(leader).send(req.clone()).await
        };
        match send {
            Ok(r) => break r,
            Err(e) => {
                // The cached connection is likely dead (broker bounced / failed
                // over). Evict it so a reconnect targets the broker's current
                // address; never evict the shared bootstrap connection.
                if leader != BOOTSTRAP_LEADER {
                    cfg.client.evict_broker(leader);
                }
                if attempts >= TRANSPORT_RETRIES {
                    // Stop hammering this leader — hand the batches back for a
                    // metadata-driven re-route to whatever leader the cluster
                    // (re-)elected. In-flight slots stay held (still pending);
                    // `send_to_leaders` bounds the overall routing budget.
                    tracing::warn!(
                        leader,
                        error = %e,
                        "produce to leader failed {attempts}×; re-routing",
                    );
                    return LeaderSendOutcome {
                        reroute: batches,
                        refresh_needed: true,
                    };
                }
                tracing::warn!(leader, error = %e, "produce attempt {attempts} failed; reconnecting");
                tokio::time::sleep(cfg.retry_backoff).await;
            }
        }
    };

    // Resolve each batch's per-partition response. Batches whose partition
    // returned a routing error are collected for re-resolution; all others
    // reach a terminal outcome here (and release their in-flight slot).
    let mut to_reroute: Vec<PreparedBatch> = Vec::new();
    let mut refresh_needed = false;
    for pb in batches {
        let part_resp = resp
            .responses
            .iter()
            .find(|t| {
                t.name == pb.topic || (pb.topic_id != Uuid::ZERO && t.topic_id == pb.topic_id)
            })
            .and_then(|t| {
                t.partition_responses
                    .iter()
                    .find(|p| p.index == pb.partition)
            });
        let Some(part_resp) = part_resp else {
            fail_batch(pb.records, ProducerError::Closed);
            finish_in_flight(cfg);
            continue;
        };

        match part_resp.error_code {
            codes::NONE | codes::DUPLICATE_SEQUENCE_NUMBER => {
                // DUPLICATE_SEQUENCE_NUMBER means the broker already committed
                // this batch — same base_offset is returned, so we can ack the
                // caller as if the original write succeeded.
                let base_offset = part_resp.base_offset;
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
            codes::OUT_OF_ORDER_SEQUENCE_NUMBER | codes::INVALID_PRODUCER_EPOCH => {
                cfg.state
                    .compare_exchange(
                        STATE_ACTIVE,
                        STATE_FENCED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .ok();
                fail_batch(pb.records, ProducerError::FencedProducer);
                finish_in_flight(cfg);
            }
            codes::NOT_LEADER_OR_FOLLOWER | codes::UNKNOWN_TOPIC_OR_PARTITION => {
                // Stale routing: we produced to a broker that doesn't lead this
                // partition. Adopt any inline leader hint immediately; otherwise
                // mark for a metadata refresh. Re-resolve + retry below. Do NOT
                // release the in-flight slot — the batch is still outstanding
                // (it carries the already-allocated base_sequence, which we
                // resend verbatim so sequencing stays monotonic).
                let hint = part_resp.current_leader.leader_id;
                if hint >= 0 {
                    cfg.partition_leaders
                        .insert((pb.topic.clone(), pb.partition), hint);
                    // Knowing the leader *id* isn't enough to route — we also
                    // need its address. If the pool hasn't learned it, force a
                    // metadata refresh below so the re-resolve can dial the
                    // hinted leader instead of falling back to the bootstrap
                    // connection, which would loop forever on NOT_LEADER.
                    if !cfg.client.knows_broker(hint) {
                        refresh_needed = true;
                    }
                } else {
                    refresh_needed = true;
                }
                to_reroute.push(pb);
            }
            code => {
                fail_batch(pb.records, ProducerError::Server(code));
                finish_in_flight(cfg);
            }
        }
    }

    // Hand any mis-routed batches back to `send_to_leaders`, which re-resolves
    // (refreshing metadata when needed) and resends them — iteratively, within
    // a bounded budget — until they reach their leader or time out.
    LeaderSendOutcome {
        reroute: to_reroute,
        refresh_needed,
    }
}

/// Build a multi-topic `ProduceRequest` from a leader's grouped batches.
/// Partitions of the same topic are merged under a single `TopicProduceData`.
fn build_produce_request(cfg: &SenderConfig, batches: &[PreparedBatch]) -> ProduceRequest {
    // Transactional state is uniform across a drain cycle's batches (the
    // producer is either inside a txn or not). Derive the request-level
    // transactional_id from whether any batch was stamped transactional.
    let is_txn = batches
        .iter()
        .any(|b| b.record_batch.attributes.is_transactional());
    let req_txn_id = if is_txn {
        cfg.transactional_id.clone()
    } else {
        None
    };

    // Merge partition data by topic, preserving the order batches were drained.
    let mut topic_order: Vec<String> = Vec::new();
    let mut by_topic: HashMap<String, (Uuid, Vec<PartitionProduceData>)> = HashMap::new();
    for pb in batches {
        let entry = by_topic.entry(pb.topic.clone()).or_insert_with(|| {
            topic_order.push(pb.topic.clone());
            (pb.topic_id, Vec::new())
        });
        entry.1.push(PartitionProduceData {
            index: pb.partition,
            records: Some(pb.record_batch.clone().into()),
            ..Default::default()
        });
    }
    let topic_data: Vec<TopicProduceData> = topic_order
        .into_iter()
        .map(|name| {
            let (topic_id, partition_data) = by_topic.remove(&name).expect("topic in order list");
            TopicProduceData {
                name,
                topic_id,
                partition_data,
                ..Default::default()
            }
        })
        .collect();

    ProduceRequest {
        transactional_id: req_txn_id,
        acks: cfg.acks.wire(),
        timeout_ms: i32::try_from(cfg.request_timeout.as_millis()).unwrap_or(i32::MAX),
        topic_data,
        ..Default::default()
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
    let codec = match cfg.compression {
        Compression::None => CompressionType::None,
        Compression::Gzip => CompressionType::Gzip,
        Compression::Snappy => CompressionType::Snappy,
        Compression::Lz4 => CompressionType::Lz4,
        Compression::Zstd => CompressionType::Zstd,
    };

    let is_transactional = txn_snapshot.is_some();
    let attributes = Attributes::default()
        .with_compression(codec)
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
