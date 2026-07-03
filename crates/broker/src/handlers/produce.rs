//! `Produce` (`api_key=0`). Routes each partition's records to that
//! partition's writer-actor and awaits the assigned base offset.
//!
//! One `RecordBatch` per (topic, partition) per request. The generated
//! `PartitionProduceData.records` field is `Option<RecordsPayload>`.
//! Versions 0-2 carry a v0/v1 `MessageSet` (legacy) which is up-converted
//! to a v2 `RecordBatch` before append. Versions 3+ carry a native v2
//! `RecordBatch`. Clients that send a single v2 batch per partition (the
//! typical modern case) are fully supported.

use std::{sync::Arc, time::Duration};

use bytes::{Bytes, BytesMut};
use crabka_log::{Offset, VerbatimBatch};
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        produce_request::ProduceRequest,
        produce_response::{
            LeaderIdAndEpoch, PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
        },
    },
    primitives::uuid::Uuid as WireUuid,
    records::{
        Attributes, RecordBatch, RecordsPayload, TimestampType, ValidatedBatch,
        count_records_in_v2_batches, produce_framing, validate_one_v2_batch,
    },
};
use tokio::sync::oneshot;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    config_keys::{COMPRESSION_TYPE, MIN_INSYNC_REPLICAS, parse_compression_type},
    error::BrokerError,
    partition::{Partition, ProduceData, ProduceJob, WriterMessage},
    partition_registry::PartitionRegistry,
};

/// Kafka `acks` sentinel `-1` (producer `acks=all`): the leader must hold
/// the response until the high watermark covers the append, i.e. every
/// in-sync replica has it.
const ACKS_ALL: i16 = -1;

/// Kafka's default `min.insync.replicas` — every partition always has at
/// least its leader in the ISR, so `1` preserves the legacy
/// "any-ISR-counts" behavior.
const DEFAULT_MIN_INSYNC_REPLICAS: i32 = 1;

/// Wire sentinel: "no offset assigned" (`ProduceResponse.INVALID_OFFSET`).
/// Stamped on partition rows that failed before any append happened.
const INVALID_OFFSET: i64 = -1;

/// Wire sentinel: "leader unknown" for the KIP-951 `current_leader` hint —
/// used when the leader's `NodeId` doesn't fit the wire's `i32`.
const NO_LEADER_ID: i32 = -1;

/// Resolve `min.insync.replicas` for a topic from the metadata image.
/// Defaults to `1` (Kafka's default — every cluster has at least the
/// leader in ISR), and silently falls back to the default on malformed
/// values (the `AlterConfigs` validator already rejected invalid values,
/// so any non-parseable string here is a corrupt metadata image — safer
/// to err toward the permissive default than to wedge produce).
fn topic_min_insync_replicas(image: &crabka_metadata::MetadataImage, topic: &str) -> i32 {
    image
        .topic_config(topic)
        .and_then(|m| m.get(MIN_INSYNC_REPLICAS))
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(DEFAULT_MIN_INSYNC_REPLICAS)
}

#[allow(clippy::too_many_lines)]
#[tracing::instrument(
    name = "handle_produce",
    level = "info",
    skip_all,
    fields(api = "Produce", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    body_bytes: Bytes,
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    // KIP-124 request_percentage meters server-side handler time; capture the
    // start so the request throttle can be combined with the byte-rate throttle
    // below (KIP-219).
    let handler_start = std::time::Instant::now();
    let partitions = broker.partitions.clone();
    let controller = broker.controller.clone();
    let producer_state = broker.producer_state.clone();
    let txn_coordinator = broker.txn_coordinator.clone();
    let log_dir_status = broker.log_dir_status.clone();
    // ── request decode (header-only on the verbatim-eligible path) ──
    // For v≥3 (native v2 payloads) we decode only the request FRAMING —
    // `transactional_id`, `acks`, `timeout_ms`, and per-topic / per-partition
    // headers plus each partition's `records` field as a zero-copy `Bytes`
    // slice of the request frame — via `produce_framing`. The record BODIES
    // are NOT decoded or decompressed here: a producer-LZ4-compressed batch
    // (1 KiB → 100 KiB) stays compressed, and the per-record parse + the
    // owned-struct materialization are skipped entirely. The owned
    // `RecordBatch` is decoded lazily, per partition, ONLY when the
    // verbatim-passthrough predicate fails (legacy magic, control batch,
    // log-append-time, broker-side recompression, multi-batch slice, or a
    // wire-null / undecodable field) — see `process_partition` /
    // `build_produce_data`.
    //
    // Legacy v0-2 requests carry a v0/v1 `MessageSet` that is always
    // up-converted (never passthrough-eligible), so they take the full owned
    // decode and feed every partition the owned path directly.
    let req: ProduceFramed = if (0..3).contains(&version) {
        let mut cur: &[u8] = req_bytes;
        let owned: ProduceRequest =
            crabka_protocol::kafka_3_6_2::owned::produce_request::ProduceRequest::decode(
                &mut cur, version,
            )?
            .into();
        ProduceFramed::from_owned(owned)
    } else {
        ProduceFramed::from_framing(produce_framing(body_bytes, version)?)
    };
    let timeout = Duration::from_millis(u64::try_from(req.timeout_ms.max(0)).unwrap_or(0));

    // ── ACL preamble ────────────────────────────────────────
    // For transactional Produce (request carries a non-empty
    // `transactional_id`), authorize `Write` on
    // `TransactionalId(transactional_id)` FIRST. On Deny, emit
    // TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53) per-partition on every
    // row of the response (matches Kafka's per-partition error mapping).
    //
    // Then batch-authorize every topic in the request for `Write` (the
    // operation Produce requires). Topics that come back `Deny` will
    // short-circuit the per-partition append below and emit
    // TOPIC_AUTHORIZATION_FAILED on every partition row of that topic.
    // Topic name resolution for v ≥ 13 (topic_id only on the wire) is
    // re-done inline below — but ACLs are keyed by topic
    // *name*, so we resolve the names here too for the authorize call.
    let image = controller.current_image();
    let txn_id_denied = match req.transactional_id.as_deref() {
        Some(tid) if !tid.is_empty() => {
            let acl_req = AuthorizationRequest {
                principal: ctx.principal,
                host: ctx.peer,
                resource_type: ResourceType::TransactionalId,
                resource_name: tid,
                operation: AclOperation::Write,
            };
            broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny
        }
        _ => false,
    };
    let topic_names_for_acl: Vec<String> = req
        .topic_data
        .iter()
        .map(|t| {
            if !t.name.is_empty() {
                t.name.clone()
            } else if t.topic_id != WireUuid::ZERO {
                image
                    .topic_name_by_id(&uuid::Uuid::from_bytes(t.topic_id.0))
                    .map(str::to_string)
                    .unwrap_or_default()
            } else {
                String::new()
            }
        })
        .collect();
    let acl_results = authorize_topics(
        broker.config.authorizer.as_ref(),
        &*image,
        ctx.principal,
        ctx.peer,
        AclOperation::Write,
        topic_names_for_acl.iter().map(String::as_str),
    );
    // Snapshot which topics are denied (by name) so the per-topic loop
    // can check without holding a borrow on `acl_results` (the loop
    // moves out of `req.topic_data`).
    let denied_topics: std::collections::HashSet<String> = acl_results
        .iter()
        .filter_map(|(name, r)| {
            if *r == AuthorizationResult::Deny {
                Some((*name).to_string())
            } else {
                None
            }
        })
        .collect();

    let mut topic_results: Vec<TopicProduceResponse> = Vec::with_capacity(req.topic_data.len());

    // ── KIP-13: measure total request bytes before consuming the topic_data ──
    // Computed here so the iterator doesn't conflict with `for topic in req.topic_data`
    // below (which moves the vector).
    let produce_bytes_by_qos_tier = produce_bytes_by_qos_tier(&image, &req.topic_data);

    for topic in req.topic_data {
        // v ≤ 12 sends the topic name; v ≥ 13 sends only topic_id and
        // we look it up in the metadata image. KIP-516: an explicit
        // non-zero id that is unknown returns UNKNOWN_TOPIC_ID (100) on
        // every partition row; a mismatched name+id returns
        // INCONSISTENT_TOPIC_ID (103). Only name-only misses fall through
        // to the legacy UNKNOWN_TOPIC_OR_PARTITION path.
        let topic_name = match crate::topic_resolve::resolve(&image, &topic.name, topic.topic_id) {
            Ok(rec) => rec.name.clone(),
            Err(codes::UNKNOWN_TOPIC_OR_PARTITION) => topic.name.clone(),
            Err(code) => {
                topic_results.push(build_topic_error_response(&topic, code));
                continue;
            }
        };

        // Account for the topic in Prometheus before
        // consuming `partition_data`. Sum the per-partition records-field
        // wire length so the bytes-in counter matches the actual bytes
        // received (the producer's compressed bytes on the verbatim path —
        // the true on-the-wire payload, no decompression needed). We count
        // even for authorize-denied / unknown-topic paths since the produce
        // *request* arrived; that mirrors Kafka's BrokerTopicMetrics semantics.
        if !topic_name.is_empty() {
            let mut topic_bytes: u64 = 0;
            // Also tally records-per-batch for
            // `messages_in_total`. V2 payloads expose
            // `records.len()` directly; legacy MessageSet payloads
            // remain opaque here and the upconversion-time
            // accounting already counts those arrivals.
            let mut topic_messages: u64 = 0;
            for p in &topic.partition_data {
                let partition_bytes = p.payload.payload_len() as u64;
                broker
                    .metrics
                    .record_partition_produce(&topic_name, p.index, partition_bytes);
                topic_bytes += partition_bytes;
                topic_messages += p.payload.message_count();
            }
            broker.metrics.record_produce(&topic_name, topic_bytes);
            broker
                .metrics
                .record_produce_messages(&topic_name, topic_messages);
        }

        let mut partition_results: Vec<PartitionProduceResponse> =
            Vec::with_capacity(topic.partition_data.len());

        // If the topic was denied by the ACL preamble, every
        // partition row for it gets TOPIC_AUTHORIZATION_FAILED and the
        // real append is skipped. An empty topic_name (v ≥ 13 with an
        // unknown topic_id) maps to "" in the denied set if and only if
        // its authorize result was Deny; the no-ACL compat shim returns
        // Allow uniformly, so existing tests are unaffected.
        let topic_denied = denied_topics.contains(&topic_name);

        // Resolve the topic's broker-side `compression.type` once. `None`
        // means Kafka's `producer` pass-through (no recompression). A
        // concrete codec forces recompression of any batch whose codec
        // differs — those batches must take the owned path. Mirrors the
        // writer's `config_snapshot().compression_type` gate so the
        // handler's verbatim decision matches the writer's recompression
        // decision exactly.
        let topic_compression = resolve_topic_compression(&image, &topic_name);

        for part_data in topic.partition_data {
            let idx = part_data.index;
            // Time the per-partition handler work for the
            // rebalancer's CpuUsage / CpuCapacity goals via
            // tokio_metrics::TaskMonitor — only on-CPU poll duration is
            // charged (not wall-time spent awaiting the writer queue,
            // HW gate under acks=-1, or txn coordinator).
            let monitor = tokio_metrics::TaskMonitor::new();
            let out = monitor
                .instrument(process_partition(
                    part_data,
                    topic_compression,
                    &topic_name,
                    topic_denied,
                    txn_id_denied,
                    req.acks,
                    timeout,
                    &partitions,
                    &txn_coordinator,
                    &producer_state,
                    &log_dir_status,
                    &image,
                    broker.config.node_id,
                    &broker.metrics,
                ))
                .await?;
            let micros = u64::try_from(monitor.cumulative().total_poll_duration.as_micros())
                .unwrap_or(u64::MAX);
            if !topic_name.is_empty() {
                broker
                    .metrics
                    .record_partition_cpu_micros(&topic_name, idx, micros);
                // Per-partition failure accounting. Bumps
                // once per partition whose response carries a non-zero
                // error code (TOPIC_AUTHORIZATION_FAILED,
                // NOT_ENOUGH_REPLICAS, INVALID_RECORD, etc.) —
                // mirrors JVM's `failedProduceRequestRate.mark()`.
                if out.error_code != 0 {
                    broker.metrics.record_failed_produce(&topic_name);
                }
            }
            partition_results.push(out);
        }

        topic_results.push(TopicProduceResponse {
            name: topic_name,
            topic_id: topic.topic_id,
            partition_responses: partition_results,
            ..Default::default()
        });
    }

    // ── KIP-13 producer_byte_rate + KIP-124 request_percentage ──────
    // Combine the data (byte-rate) and request (handler-time) throttles as
    // their max, surface it in throttle_time_ms, and mute the channel once
    // before responding (KIP-219). The dispatch loop skips request_percentage
    // for Produce so it is charged exactly once, here.
    let data_delay = produce_bytes_by_qos_tier
        .iter()
        .map(|(qos_tier, bytes)| {
            crate::quota::consume_producer_quota(
                &image,
                &broker.quota_buckets,
                &ctx.principal.name,
                ctx.client_id,
                qos_tier,
                *bytes,
            )
        })
        .max()
        .unwrap_or(Duration::ZERO);
    #[allow(clippy::cast_possible_truncation)]
    let elapsed_micros = handler_start
        .elapsed()
        .as_micros()
        .min(u128::from(u64::MAX)) as u64;
    let request_delay = crate::quota::consume_request_quota(
        &image,
        &broker.quota_buckets,
        &ctx.principal.name,
        ctx.client_id,
        elapsed_micros,
    );
    let delay = data_delay.max(request_delay);
    let resp = ProduceResponse {
        responses: topic_results,
        throttle_time_ms: i32::try_from(delay.as_millis()).unwrap_or(i32::MAX),
        ..Default::default()
    };
    if delay > Duration::ZERO {
        tokio::time::sleep(delay).await;
    }
    let buf = if (0..3).contains(&version) {
        let legacy_resp: crabka_protocol::kafka_3_6_2::owned::produce_response::ProduceResponse =
            resp.into();
        let mut b = BytesMut::with_capacity(legacy_resp.encoded_len(version));
        legacy_resp.encode(&mut b, version)?;
        b
    } else {
        let mut b = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut b, version)?;
        b
    };
    Ok(buf.freeze())
}

/// Per-partition produce handling, extracted so the call site can wrap it
/// in `tokio_metrics::TaskMonitor` and charge only on-CPU poll time to
/// `partition_cpu_micros_total`. Wall-time spent awaiting the writer
/// queue, the HW gate under `acks=-1`, or the txn coordinator does not
/// count toward CPU usage. Returns the per-partition response on every
/// path; only `txn_coordinator.put` errors propagate via `?`.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn process_partition(
    part_data: FramedPartition,
    topic_compression: Option<crabka_compression::CompressionType>,
    topic_name: &str,
    topic_denied: bool,
    txn_id_denied: bool,
    acks: i16,
    timeout: Duration,
    partitions: &Arc<PartitionRegistry>,
    txn_coordinator: &Arc<crate::txn::coordinator::TxnCoordinator>,
    producer_state: &Arc<crate::producer_state::ProducerState>,
    log_dir_status: &crate::log_dir_status::LogDirRegistry,
    image: &Arc<crabka_metadata::MetadataImage>,
    this_node_id: crabka_metadata::NodeId,
    metrics: &crate::metrics::BrokerMetrics,
) -> Result<PartitionProduceResponse, BrokerError> {
    let idx = part_data.index;
    let mut out = PartitionProduceResponse {
        index: idx,
        ..Default::default()
    };

    if txn_id_denied {
        out.error_code = codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED;
        return Ok(out);
    }

    if topic_denied {
        out.error_code = codes::TOPIC_AUTHORIZATION_FAILED;
        return Ok(out);
    }

    // Decide verbatim-passthrough vs owned-decode and extract the HEADER
    // fields the gates below need (producer id/epoch/sequence,
    // last_offset_delta, max_timestamp, attributes). On the verbatim path
    // this is a header-only CRC check — the (possibly LZ4-compressed) record
    // body is NEVER decompressed or materialized. The owned fallback decodes
    // the records (decompressing) here, exactly as before. A null /
    // undecodable field returns INVALID_REQUEST / INVALID_RECORD, preserving
    // the prior error-code ordering (before the leadership gate).
    let prepared = match prepare_batch(part_data.payload, topic_compression, topic_name, metrics) {
        Ok(p) => p,
        Err(code) => {
            out.error_code = code;
            return Ok(out);
        }
    };

    // ── leadership gate (Kafka: only the LEADER accepts Produce) ──────
    // Only the partition leader may accept a Produce. A Produce misrouted
    // to a non-leader must be rejected so the client refreshes its
    // metadata and re-targets — it must NOT be appended to a local
    // follower replica (the real leader would never see those records and
    // the follower's append would be discarded on its next truncating
    // Fetch from the leader → silent data loss).
    //
    // The authoritative leader is the metadata IMAGE's `partition.leader`,
    // the same source the Fetch handler uses for its KIP-320 / KIP-951
    // `current_leader` hint. We deliberately do NOT gate on the broker's
    // local `leader_partitions` / `is_coordinator_for` set: that set is
    // recomputed on every metadata change and is transiently empty while
    // raft leadership settles on a freshly-booted broker, so it would
    // spuriously reject a legitimate leader's Produces (see the same
    // hazard documented for the transactional path below). The image
    // reflects committed leadership, so a just-elected leader's own image
    // already names it the leader; the only residual window is a follower
    // whose image hasn't yet caught up to a leadership change, which
    // correctly returns NOT_LEADER (the client retries against the new
    // leader) rather than appending to the wrong replica.
    //
    // Partition-level absence in the image (topic exists but this index
    // doesn't, or the topic is unknown) maps to UNKNOWN_TOPIC_OR_PARTITION
    // (3); presence-but-not-leader maps to NOT_LEADER_OR_FOLLOWER (6) with
    // a `current_leader` hint (encodes at Produce v10+, KIP-951) so the
    // client re-routes without a full Metadata round-trip.
    if topic_name.is_empty() {
        out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
        return Ok(out);
    }
    let (leader, leader_epoch) = match image.partition(topic_name, idx) {
        // Topic exists but this partition index doesn't, or the topic is
        // unknown cluster-wide.
        None => {
            out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
            return Ok(out);
        }
        Some(pr) => (pr.leader, pr.leader_epoch),
    };
    if leader != this_node_id {
        out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
        out.current_leader = LeaderIdAndEpoch {
            leader_id: i32::try_from(leader.0).unwrap_or(NO_LEADER_ID),
            leader_epoch,
            ..Default::default()
        };
        return Ok(out);
    }

    // We are the image-designated leader. The local replica must exist;
    // if the local writer-actor hasn't been spun up yet (supervisor
    // reconcile lagging the image on a just-elected leader), treat it as a
    // transient not-leader — the client retries and the append lands once
    // the writer is ready, rather than failing the produce outright.
    let Some(part) = partitions.get(topic_name, crabka_ids::PartitionIndex(idx)) else {
        out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
        out.current_leader = LeaderIdAndEpoch {
            leader_id: i32::try_from(leader.0).unwrap_or(NO_LEADER_ID),
            leader_epoch,
            ..Default::default()
        };
        return Ok(out);
    };

    // KIP-113 offline-dir handling: if the partition's owning log dir
    // has been flipped offline (either by the startup probe or by a
    // runtime fsync failure on any partition in this dir), refuse the
    // append immediately with KAFKA_STORAGE_ERROR. The partition's
    // writer task would otherwise queue the work, fail at fsync, and
    // bounce the same error back via the ack — same outcome, more
    // latency and a wasted disk attempt.
    if log_dir_status.is_offline(&part.log_dir.load()) {
        out.error_code = codes::KAFKA_STORAGE_ERROR;
        return Ok(out);
    }

    // `min.insync.replicas` pre-flight check (KIP-91 / KAFKA-3197).
    // Only `acks=-1` (`all`) cares: with `acks=0`/`1` the leader never
    // waits for followers, so the threshold is meaningless. When the
    // image's ISR is already smaller than the configured threshold,
    // there's no chance the post-append HW gate will satisfy "all
    // in-sync replicas ack" — fail fast with NOT_ENOUGH_REPLICAS (19)
    // before queueing work on the writer. Default `1` matches Apache
    // Kafka and preserves the legacy "any-ISR-counts" behavior.
    if acks == ACKS_ALL
        && let Some(pr) = image.partition(topic_name, idx)
    {
        let min_isr = topic_min_insync_replicas(image, topic_name);
        if i32::try_from(pr.isr.len()).unwrap_or(i32::MAX) < min_isr {
            out.error_code = codes::NOT_ENOUGH_REPLICAS;
            return Ok(out);
        }
    }

    // The current leader epoch — this becomes the `partition_leader_epoch`
    // carried on the wire (stamped onto the owned batch / verbatim bytes at
    // append) and used by KIP-101 fence validation on the follower's Fetch.
    let leader_epoch = part
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);

    // ── transactional produce verify (KIP-1319 v2) ──────────
    // This check is more authoritative than idempotent dedup,
    // so it runs first. Non-transactional batches (pid < 0 or
    // is_transactional=false) skip directly to the dedup gate.
    // All header fields below come from `prepared` — sourced from the v2
    // batch HEADER on the verbatim path (no record decode), or from the
    // decoded owned `RecordBatch` header on the fallback path.
    let is_transactional = prepared.attributes.is_transactional();
    {
        let pid_txn = prepared.producer_id;
        let epoch_txn = prepared.producer_epoch;
        if is_transactional && pid_txn >= 0 {
            // Wrap the decode-side `i64` into `ProducerId` for the coordinator lookup.
            let Some(tid) = txn_coordinator.tid_for_pid(crabka_log::ProducerId(pid_txn)) else {
                // Unknown producer_id — reject.
                out.error_code = codes::INVALID_PRODUCER_ID_MAPPING;
                return Ok(out);
            };
            // Holding the tid's entry locally is the authoritative signal
            // that we coordinate it. Gating on `is_coordinator_for` instead
            // is racy on a freshly-booted broker: `leader_partitions` is
            // recomputed on every metadata change and is transiently empty
            // while raft leadership settles, so a transactional Produce could
            // arrive while the check returns false and silently skip the
            // inline AddPartitionsToTxn — leaving the txn `Empty` so the
            // following EndTxn fails with INVALID_TXN_STATE.
            if let Some(entry_mutex) = txn_coordinator.get(&tid) {
                let mut entry = entry_mutex.lock().await;
                if entry.producer_epoch != epoch_txn {
                    out.error_code = codes::INVALID_PRODUCER_EPOCH;
                    return Ok(out);
                }
                let tp = crate::txn::state::TopicPartition {
                    topic: topic_name.to_string(),
                    partition: crabka_ids::PartitionIndex(idx),
                };
                // Consider the partition "needs registering" if it
                // isn't in the current partition set OR if the
                // current state is CompleteCommit/CompleteAbort
                // (indicating a new transaction after a completed
                // one — the partition set is stale).
                let needs_register = !entry.partitions.contains(&tp)
                    || matches!(
                        entry.state,
                        crate::txn::state::TxnState::CompleteCommit
                            | crate::txn::state::TxnState::CompleteAbort
                    );
                if needs_register {
                    // v2 auto-AddPartitionsToTxn: register the
                    // partition inline if the state allows it.
                    if !entry
                        .state
                        .can_transition_to(crate::txn::state::TxnState::Ongoing)
                    {
                        out.error_code = codes::INVALID_TXN_STATE;
                        return Ok(out);
                    }
                    // If starting a new txn after a completed one,
                    // clear the stale partition set.
                    if matches!(
                        entry.state,
                        crate::txn::state::TxnState::CompleteCommit
                            | crate::txn::state::TxnState::CompleteAbort
                    ) {
                        entry.partitions.clear();
                    }
                    entry.state = crate::txn::state::TxnState::Ongoing;
                    entry.partitions.insert(tp);
                    entry.last_update_ms = crate::txn::util::now_millis();
                    let snap = entry.clone();
                    // Lock must be dropped before the async put.
                    drop(entry);
                    let txnv = crate::txn::version::resolve_txn_version(image);
                    txn_coordinator.put(snap, txnv).await?;
                }
                // else: partition already registered in an active txn — fall through.
            }
            // else: we don't hold this tid's state — not our coordinator.
            // Trust the producer to have called AddPartitionsToTxn through the
            // correct coordinator. Inter-broker v2 auto-add is not yet
            // supported.
        }
    }

    // ── idempotent-producer dedup gate ───────────────────────
    let pid = prepared.producer_id;
    let epoch = prepared.producer_epoch;
    let base_seq = prepared.base_sequence;
    let last_offset_delta = prepared.last_offset_delta;
    let max_timestamp = prepared.max_timestamp;

    let dedup_outcome = if pid >= 0 {
        Some(
            producer_state
                .check(
                    topic_name,
                    crabka_ids::PartitionIndex(idx),
                    pid,
                    epoch,
                    base_seq,
                    last_offset_delta,
                )
                .await,
        )
    } else {
        None
    };

    match dedup_outcome {
        Some(crate::producer_state::Decision::Duplicate { base_offset }) => {
            // The original Produce's append went through but its
            // HW gate may have timed out — that's why the idempotent
            // producer is retrying with the same `base_sequence`.
            // For `acks=-1`, we MUST still wait for HW to reach
            // the duplicate's last offset before claiming success.
            // Returning NONE unconditionally would silently bypass
            // the full-ISR durability guarantee that acks=all
            // promises: the original append is on the leader, but
            // followers may not have it yet, and a leader crash
            // before they catch up would lose data the producer
            // believed acknowledged.
            if acks == ACKS_ALL {
                let target = base_offset + i64::from(last_offset_delta) + 1;
                let deadline = std::time::Instant::now() + timeout;
                // `base_offset` here is the dedup tracker's raw wire `i64`; wrap
                // the HW target into `Offset` for the log-layer gate.
                match part.await_hw_at_least(Offset(target), deadline).await {
                    Ok(()) => {
                        out.error_code = codes::NONE;
                        out.base_offset = base_offset;
                    }
                    Err(_timeout) => {
                        out.error_code = codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND;
                        out.base_offset = base_offset;
                    }
                }
            } else {
                out.error_code = codes::NONE;
                out.base_offset = base_offset;
            }
            return Ok(out);
        }
        Some(crate::producer_state::Decision::OutOfOrder) => {
            out.error_code = codes::OUT_OF_ORDER_SEQUENCE_NUMBER;
            return Ok(out);
        }
        Some(crate::producer_state::Decision::Fenced) => {
            out.error_code = codes::INVALID_PRODUCER_EPOCH;
            return Ok(out);
        }
        Some(crate::producer_state::Decision::Append) | None => {
            // fall through to writer dispatch
        }
    }

    // ── verbatim passthrough vs owned fallback ───────────────
    // Both paths stamp the same `leader_epoch` computed above: the writer
    // patches it into the verbatim bytes in-place at append; the owned batch
    // carries it as a struct field.
    let data = build_produce_data(prepared, leader_epoch);

    let (ack_tx, ack_rx) = oneshot::channel();
    let job = WriterMessage::Produce(ProduceJob { data, ack: ack_tx });

    if part.writer_tx.send(job).await.is_err() {
        out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
        return Ok(out);
    }

    match tokio::time::timeout(timeout, ack_rx).await {
        Ok(Ok(Ok(base_offset))) => {
            // Single finalization site for a successful append: applies
            // the `acks=-1` high-watermark gate and records the
            // idempotent-producer commit exactly once.
            finalize_ack(
                &mut out,
                &part,
                acks,
                timeout,
                base_offset,
                producer_state,
                &CommitKey {
                    topic: topic_name,
                    partition: idx,
                    pid,
                    epoch,
                    base_seq,
                    last_offset_delta,
                    max_timestamp,
                },
            )
            .await;
        }
        Ok(Ok(Err(e))) => {
            out.error_code = codes::from_broker_error(&e);
        }
        Ok(Err(_)) => {
            // Writer dropped the oneshot without sending — shouldn't
            // happen unless the writer task panicked between recv
            // and ack. Map to NOT_LEADER_OR_FOLLOWER.
            out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
        }
        Err(_) => {
            out.error_code = codes::REQUEST_TIMED_OUT;
        }
    }
    Ok(out)
}

/// Borrowed bundle of the idempotent-producer dedup identity plus the
/// fields needed to record a `producer_state.commit`. Groups the eight
/// positional `commit` arguments into a single value so the commit call
/// exists in exactly one place ([`finalize_ack`]).
struct CommitKey<'a> {
    topic: &'a str,
    partition: i32,
    pid: i64,
    epoch: i16,
    base_seq: i32,
    last_offset_delta: i32,
    max_timestamp: i64,
}

/// Finalize a successful writer append: apply the `acks=-1`
/// high-watermark durability gate, set the response `error_code` /
/// `base_offset`, and record the idempotent-producer commit (when
/// `pid >= 0`) exactly once.
///
/// Behavior matches the previous three inlined sites verbatim:
///   * `acks != -1`: NONE, then commit.
///   * `acks == -1`, HW reaches target: NONE, then commit.
///   * `acks == -1`, HW gate times out: `NOT_ENOUGH_REPLICAS_AFTER_APPEND`,
///     then commit (the append is durable on the leader; the
///     idempotent tracker must advance so a retry is recognized as a
///     duplicate rather than out-of-order).
///
/// Note the commit happens on *both* the success and timeout `acks=-1`
/// sub-paths — identical to the pre-refactor code — so it is performed
/// unconditionally here once the `error_code`/`base_offset` are decided.
async fn finalize_ack(
    out: &mut PartitionProduceResponse,
    part: &Arc<Partition>,
    acks: i16,
    timeout: Duration,
    base_offset: Offset,
    producer_state: &Arc<crate::producer_state::ProducerState>,
    key: &CommitKey<'_>,
) {
    let target = base_offset + i64::from(key.last_offset_delta) + 1;
    if acks == ACKS_ALL {
        let deadline = std::time::Instant::now() + timeout;
        out.error_code = match part.await_hw_at_least(target, deadline).await {
            Ok(()) => codes::NONE,
            Err(_timeout) => codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        };
    } else {
        out.error_code = codes::NONE;
    }
    // Unwrap the assigned `Offset` into the wire `base_offset` response field.
    out.base_offset = base_offset.0;
    // Only record the idempotent-producer commit if the appended batch is still
    // on the leader's log. A failover-rejoin divergence truncation can remove
    // the batch while the acks=all HW gate above is waiting (the gate then times
    // out); recording the truncated batch would make a retry dedup against an
    // offset the log no longer holds, and the retry's HW gate would wait forever
    // for a high watermark that can never reach the vanished offset. Skipping
    // the commit lets the retry re-append fresh instead.
    //
    // This is a best-effort check: a truncation racing in between this read and
    // the commit below could still record a stale entry. That is tolerated
    // because the replicator calls `ProducerState::truncate` after *every* log
    // truncation, so any entry stranded by such a race is dropped by the next
    // truncation/failover. Do not "harden" this by removing the check — the
    // check is what avoids recording the common (already-truncated) case.
    if key.pid >= 0 && part.log_end_offset() < target {
        // Evidence for the on-cluster failover verification: the appended batch
        // was truncated before its dedup commit, so we skip the commit (the
        // retry re-appends). Seeing this fire confirms the Bug-D path executed.
        tracing::warn!(
            topic = key.topic,
            partition = key.partition,
            base_offset = base_offset.0,
            target = target.0,
            leo = part.log_end_offset().0,
            "produce: appended batch truncated before dedup commit; skipping commit so retry re-appends"
        );
    }
    if key.pid >= 0 && part.log_end_offset() >= target {
        producer_state
            .commit(
                key.topic,
                crabka_ids::PartitionIndex(key.partition),
                key.pid,
                key.epoch,
                key.base_seq,
                key.last_offset_delta,
                // Unwrap the assigned `Offset` into the dedup tracker's `i64`.
                base_offset.0,
                key.max_timestamp,
            )
            .await;
    }
}

/// Build a topic-level error response for KIP-516 id-resolution failures
/// (`UNKNOWN_TOPIC_ID`, `INCONSISTENT_TOPIC_ID`). Every partition row in
/// the request receives the same error code; `base_offset` is set to -1 to
/// signal "no offset assigned", matching Kafka's behavior on pre-append errors.
fn build_topic_error_response(
    topic: &FramedTopic,
    code: i16,
) -> crabka_protocol::owned::produce_response::TopicProduceResponse {
    use crabka_protocol::owned::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };
    TopicProduceResponse {
        name: topic.name.clone(),
        topic_id: topic.topic_id,
        partition_responses: topic
            .partition_data
            .iter()
            .map(|p| PartitionProduceResponse {
                index: p.index,
                error_code: code,
                base_offset: INVALID_OFFSET,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// One partition's records, as it arrived on the wire and BEFORE any owned
/// decode/decompression. The verbatim hot path keeps the producer's exact
/// bytes here; the owned legacy path carries an already-decoded payload.
enum PartitionPayload {
    /// v≥3 native records bytes captured zero-copy from the request frame
    /// (a refcount view, not a copy). Not yet validated or decompressed —
    /// the per-partition dispatch validates the header only and decides
    /// verbatim-vs-owned, decompressing solely on the owned fallback.
    Slice(Bytes),
    /// Legacy v0-2 (and any pre-decoded) payload. Always takes the owned
    /// path: a v0/v1 `MessageSet` is up-converted, never passed through.
    Owned(RecordsPayload),
    /// Wire-null records field → `INVALID_REQUEST`.
    Null,
}

impl PartitionPayload {
    /// Records-field wire length in bytes (matches `RecordsPayload::payload_len`
    /// for the owned form; the slice's own length for the verbatim form). Used
    /// for the KIP-13 bytes-in metrics + producer byte-rate quota, exactly as
    /// the prior owned decode reported.
    fn payload_len(&self) -> usize {
        match self {
            Self::Slice(b) => b.len(),
            Self::Owned(p) => p.payload_len(),
            Self::Null => 0,
        }
    }

    /// Number of records across the field's batch(es), for `messages_in_total`.
    /// Verbatim slices read each v2 batch header's `records_count` WITHOUT
    /// decompressing; owned payloads sum `records.len()` over their v2 batches.
    fn message_count(&self) -> u64 {
        match self {
            Self::Slice(b) => count_records_in_v2_batches(b),
            Self::Owned(p) => p.as_v2().map_or(0, |batches| {
                batches.iter().map(|b| b.records.len() as u64).sum()
            }),
            Self::Null => 0,
        }
    }
}

/// Header-only framing of a `ProduceRequest`, mirroring the owned struct's
/// field names so the handler body is unchanged except for the records form.
struct ProduceFramed {
    transactional_id: Option<String>,
    acks: i16,
    timeout_ms: i32,
    topic_data: Vec<FramedTopic>,
}

struct FramedTopic {
    name: String,
    topic_id: WireUuid,
    partition_data: Vec<FramedPartition>,
}

struct FramedPartition {
    index: i32,
    payload: PartitionPayload,
}

impl ProduceFramed {
    /// v≥3: build from the header-only `produce_framing` walk — no record
    /// body is decoded or decompressed here.
    fn from_framing(f: crabka_protocol::records::ProduceFraming) -> Self {
        Self {
            transactional_id: f.transactional_id,
            acks: f.acks,
            timeout_ms: f.timeout_ms,
            topic_data: f
                .topics
                .into_iter()
                .map(|t| FramedTopic {
                    name: t.name,
                    topic_id: WireUuid(t.topic_id.0),
                    partition_data: t
                        .partitions
                        .into_iter()
                        .map(|p| FramedPartition {
                            index: p.partition,
                            payload: match p.records {
                                Some(b) => PartitionPayload::Slice(b),
                                None => PartitionPayload::Null,
                            },
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    /// v0-2: wrap the fully-decoded legacy request; every partition takes the
    /// owned path (legacy `MessageSet` up-conversion is never passthrough).
    fn from_owned(req: ProduceRequest) -> Self {
        Self {
            transactional_id: req.transactional_id,
            acks: req.acks,
            timeout_ms: req.timeout_ms,
            topic_data: req
                .topic_data
                .into_iter()
                .map(|t| FramedTopic {
                    name: t.name,
                    topic_id: t.topic_id,
                    partition_data: t
                        .partition_data
                        .into_iter()
                        .map(|p| FramedPartition {
                            index: p.index,
                            payload: match p.records {
                                Some(rp) => PartitionPayload::Owned(rp),
                                None => PartitionPayload::Null,
                            },
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

fn produce_bytes_by_qos_tier(
    image: &crabka_metadata::MetadataImage,
    topics: &[FramedTopic],
) -> std::collections::BTreeMap<String, u64> {
    let mut out = std::collections::BTreeMap::new();
    for topic in topics {
        let topic_name = match crate::topic_resolve::resolve(image, &topic.name, topic.topic_id) {
            Ok(rec) => rec.name.as_str(),
            Err(_) => topic.name.as_str(),
        };
        let qos_tier = crate::config_keys::resolve_qos_tier(image, topic_name).to_string();
        let topic_bytes: u64 = topic
            .partition_data
            .iter()
            .map(|p| p.payload.payload_len() as u64)
            .sum();
        *out.entry(qos_tier).or_default() += topic_bytes;
    }
    out
}

/// Resolve a topic's broker-side `compression.type` from the metadata
/// image. `None` means Kafka's `producer` pass-through (no recompression);
/// `Some(codec)` forces recompression of batches whose codec differs.
/// Mirrors the resolution the partition writer applies via its
/// `LogConfig::compression_type`.
fn resolve_topic_compression(
    image: &crabka_metadata::MetadataImage,
    topic: &str,
) -> Option<crabka_compression::CompressionType> {
    image
        .topic_config(topic)
        .and_then(|m| m.get(COMPRESSION_TYPE))
        .and_then(|v| parse_compression_type(v).ok())
        .flatten()
}

/// All the per-batch HEADER fields the broker's produce gates need
/// (leadership epoch stamp, transactional verify, idempotent dedup,
/// `acks=-1` HW target), sourced WITHOUT materializing or decompressing
/// the records. On the verbatim path these come from the v2 batch header
/// (via [`validate_one_v2_batch`]); on the owned fallback they come from
/// the decoded [`RecordBatch`] header — identical values either way.
#[derive(Debug)]
struct PreparedBatch {
    attributes: Attributes,
    last_offset_delta: i32,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    /// The append source: producer's verbatim bytes (passthrough) or the
    /// decoded owned batch (fallback). The leader epoch is stamped at append
    /// time by the writer (verbatim) or onto the owned batch below.
    source: PreparedSource,
}

#[derive(Debug)]
enum PreparedSource {
    /// Validated, single, CRC-checked v2 batch — append the producer's exact
    /// bytes. No decode/decompress happened.
    Verbatim(Bytes),
    /// Decoded owned batch — the complete fallback path. Decompression (if the
    /// producer compressed) happened here, in `RecordBatch::decode`.
    Owned(RecordBatch),
}

impl PreparedBatch {
    fn from_header(header: ValidatedHeader, bytes: Bytes) -> Self {
        Self {
            attributes: header.attributes,
            last_offset_delta: header.last_offset_delta,
            max_timestamp: header.max_timestamp,
            producer_id: header.producer_id,
            producer_epoch: header.producer_epoch,
            base_sequence: header.base_sequence,
            source: PreparedSource::Verbatim(bytes),
        }
    }

    fn from_owned(batch: RecordBatch) -> Self {
        Self {
            attributes: batch.attributes,
            last_offset_delta: batch.last_offset_delta,
            max_timestamp: batch.max_timestamp,
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            source: PreparedSource::Owned(batch),
        }
    }
}

/// Decide the append shape for one partition's records and extract the header
/// fields the gates need — WITHOUT decompressing on the verbatim path.
///
/// Verbatim-passthrough predicate (ALL must hold), mirroring the writer's
/// recompression gate exactly:
///   1. a v≥3 native-v2 records slice (not legacy, not a wire-null field);
///   2. the slice is exactly one complete, CRC-valid v2 batch (re-validates
///      the producer's CRC header-only — no record materialization);
///   3. `timestamp_type == CreateTime` (no log-append-time rewrite, which
///      would touch CRC-covered header bytes);
///   4. the batch is **not** a control batch (its LSO bookkeeping needs the
///      inner marker record, which the header-only path can't read);
///   5. no broker-side recompression — the topic's `compression.type` is
///      `producer` pass-through (`None`) OR equals the batch's own codec.
///
/// On any miss the records are decoded into an owned `RecordBatch` (the
/// complete fallback — decompressing here only). Legacy v0-2 payloads are
/// up-converted via [`decode_owned_batch`]. The owned arm is a complete
/// alternative, so reverting this feature is "always take the owned path".
///
/// Returns the response error *code* on a bad field (`INVALID_REQUEST` /
/// `INVALID_RECORD`), matching the prior `decode_single_batch` behavior.
fn prepare_batch(
    payload: PartitionPayload,
    topic_compression: Option<crabka_compression::CompressionType>,
    topic_name: &str,
    metrics: &crate::metrics::BrokerMetrics,
) -> Result<PreparedBatch, i16> {
    let bytes = match payload {
        // Legacy / pre-decoded payload: always owned.
        PartitionPayload::Owned(rp) => {
            return decode_owned_batch(rp, topic_name, metrics).map(PreparedBatch::from_owned);
        }
        PartitionPayload::Null => return Err(codes::INVALID_REQUEST),
        PartitionPayload::Slice(b) => b,
    };

    // Owned fallback for a v≥3 records slice that the verbatim predicate
    // rejects. Routes the raw field bytes through `RecordsPayload::from_bytes`
    // — which dispatches v2 (parse every batch) vs legacy (v0/v1 `MessageSet`,
    // kept opaque) by the magic byte — then through `decode_owned_batch`, the
    // SAME pipeline the request decoder used before this change. This is what
    // up-converts a v1 `MessageSet` carried over a v≥3 produce (older
    // message-format clients) and surfaces INVALID_RECORD on malformed bytes.
    let owned_fallback = |bytes: Bytes| -> Result<PreparedBatch, i16> {
        match RecordsPayload::from_bytes(bytes) {
            Ok(rp) => decode_owned_batch(rp, topic_name, metrics).map(PreparedBatch::from_owned),
            Err(_) => Err(codes::INVALID_RECORD),
        }
    };
    // Extract the header fields into owned values up front so the borrow of
    // `bytes` (via the `ValidatedBatch`) ends before any `owned_fallback(bytes)`
    // move or the final `Verbatim(bytes)` construction.
    let header = match validate_one_v2_batch(&bytes) {
        Ok(v) if v.total_len == bytes.len() => ValidatedHeader::from(&v),
        _ => return owned_fallback(bytes),
    };
    let attributes = header.attributes;

    // (3) CreateTime only. (4) No control batches.
    if attributes.timestamp_type() != TimestampType::CreateTime || attributes.is_control_batch() {
        return owned_fallback(bytes);
    }
    // (5) No recompression: producer pass-through, or target == current codec.
    if let Some(target) = topic_compression
        && target != attributes.compression()
    {
        return owned_fallback(bytes);
    }

    Ok(PreparedBatch::from_header(header, bytes))
}

/// The v2 batch header fields the gates need, copied out of a borrowed
/// [`ValidatedBatch`] so the verbatim `Bytes` can be moved afterward.
#[derive(Debug, Clone, Copy)]
struct ValidatedHeader {
    attributes: Attributes,
    last_offset_delta: i32,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
}

impl From<&ValidatedBatch<'_>> for ValidatedHeader {
    fn from(v: &ValidatedBatch<'_>) -> Self {
        Self {
            attributes: Attributes(v.header.attributes.get()),
            last_offset_delta: v.header.last_offset_delta.get(),
            max_timestamp: v.header.max_timestamp.get(),
            producer_id: v.header.producer_id.get(),
            producer_epoch: v.header.producer_epoch.get(),
            base_sequence: v.header.base_sequence.get(),
        }
    }
}

/// Decode/up-convert a legacy or pre-decoded `RecordsPayload` into a single
/// owned `RecordBatch`. Mirrors the prior `decode_single_batch`: a v0/v1
/// `MessageSet` is up-converted (counted once), an empty v2 sequence is
/// `INVALID_REQUEST`, a failed up-conversion is `INVALID_RECORD`.
fn decode_owned_batch(
    payload: RecordsPayload,
    topic_name: &str,
    metrics: &crate::metrics::BrokerMetrics,
) -> Result<RecordBatch, i16> {
    match payload {
        RecordsPayload::V2(batches) => batches.into_iter().next().ok_or(codes::INVALID_REQUEST),
        RecordsPayload::Raw(bytes) => RecordsPayload::from_bytes(bytes)
            .ok()
            .and_then(|p| match p {
                RecordsPayload::V2(mut v) => v.drain(..).next(),
                RecordsPayload::Raw(_) | RecordsPayload::Legacy(_) => None,
                #[cfg(any(
                    target_os = "linux",
                    target_os = "macos",
                    target_os = "ios",
                    target_os = "tvos",
                    target_os = "watchos",
                    target_os = "freebsd",
                    target_os = "dragonfly",
                ))]
                RecordsPayload::FileRegions(_) => None,
            })
            .ok_or(codes::INVALID_REQUEST),
        #[cfg(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "ios",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "freebsd",
            target_os = "dragonfly",
        ))]
        RecordsPayload::FileRegions(_) => Err(codes::INVALID_REQUEST),
        RecordsPayload::Legacy(bytes) => match crabka_records_legacy::legacy_to_v2(&bytes) {
            Ok(rb) => {
                if !topic_name.is_empty() {
                    metrics.record_produce_message_conversion(topic_name);
                }
                Ok(rb)
            }
            Err(e) => {
                tracing::warn!(error = %e, "legacy_to_v2 failed");
                Err(codes::INVALID_RECORD)
            }
        },
    }
}

/// Build the writer's [`ProduceData`] from a prepared batch, stamping the
/// leader epoch. Verbatim batches carry the producer's exact bytes; owned
/// batches carry the decoded `RecordBatch` (with `partition_leader_epoch`
/// already stamped by the caller).
fn build_produce_data(prepared: PreparedBatch, leader_epoch: i32) -> ProduceData {
    let is_transactional = prepared.attributes.is_transactional();
    match prepared.source {
        PreparedSource::Verbatim(bytes) => ProduceData::Verbatim(VerbatimBatch {
            last_offset_delta: prepared.last_offset_delta,
            max_timestamp: prepared.max_timestamp,
            leader_epoch,
            // Wrap the produce path's decode-side `i64` into the log seam's `ProducerId`.
            producer_id: crabka_log::ProducerId(prepared.producer_id),
            is_transactional,
            bytes,
        }),
        PreparedSource::Owned(mut batch) => {
            // The writer stamps the verbatim path's epoch in-place at append;
            // the owned batch carries it as a struct field instead.
            batch.partition_leader_epoch = leader_epoch;
            ProduceData::Owned(batch)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    use assert2::{assert, check};
    use bytes::{Bytes, BytesMut};
    use crabka_compression::CompressionType;
    use crabka_metadata::{
        MetadataImage, MetadataRecord, PartitionRecord, TopicConfigRecord, TopicRecord,
    };
    use crabka_protocol::records::{Record, RecordBatch, RecordsPayload};
    use uuid::Uuid;

    use super::{
        FramedPartition, FramedTopic, MIN_INSYNC_REPLICAS, PartitionPayload,
        build_topic_error_response, decode_owned_batch, process_partition,
        produce_bytes_by_qos_tier, resolve_topic_compression, topic_min_insync_replicas,
    };

    fn image_with_topic(topic: &str, isr: &[u64]) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: topic.into(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: i16::try_from(isr.len().max(1)).unwrap(),
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.into(),
            partition: 0,
            leader: crabka_audit::NodeId(*isr.first().unwrap_or(&1)),
            replicas: isr.iter().copied().map(crabka_audit::NodeId).collect(),
            isr: isr.iter().copied().map(crabka_audit::NodeId).collect(),
            leader_epoch: 0,
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
        img
    }

    fn set_min_isr(img: &mut MetadataImage, topic: &str, n: i32) {
        let mut o = BTreeMap::new();
        o.insert(MIN_INSYNC_REPLICAS.into(), n.to_string());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: topic.into(),
            overrides: o,
        }));
    }

    fn set_qos_tier(img: &mut MetadataImage, topic: &str, tier: &str) {
        let mut o = BTreeMap::new();
        o.insert(crate::config_keys::QOS_TIER.into(), tier.into());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: topic.into(),
            overrides: o,
        }));
    }

    fn framed_topic(name: &str, payload_lens: &[usize]) -> FramedTopic {
        FramedTopic {
            name: name.into(),
            topic_id: crabka_protocol::primitives::uuid::Uuid::ZERO,
            partition_data: payload_lens
                .iter()
                .enumerate()
                .map(|(idx, len)| FramedPartition {
                    index: i32::try_from(idx).unwrap(),
                    payload: PartitionPayload::Slice(Bytes::from(vec![0; *len])),
                })
                .collect(),
        }
    }

    fn encode_batch(batch: &RecordBatch) -> Bytes {
        let mut buf = BytesMut::new();
        batch.encode(&mut buf).expect("encode record batch");
        buf.freeze()
    }

    #[test]
    fn topic_min_isr_defaults_to_one_when_unset() {
        let img = image_with_topic("t", &[1, 2, 3]);
        assert!(topic_min_insync_replicas(&img, "t") == 1);
    }

    #[test]
    fn topic_min_isr_reads_override_when_set() {
        let mut img = image_with_topic("t", &[1, 2, 3]);
        set_min_isr(&mut img, "t", 3);
        assert!(topic_min_insync_replicas(&img, "t") == 3);
    }

    #[test]
    fn topic_min_isr_default_one_on_unknown_topic() {
        let img = MetadataImage::new(Uuid::nil());
        assert!(
            topic_min_insync_replicas(&img, "ghost") == 1,
            "missing topic_config must default to 1, not crash"
        );
    }

    #[test]
    fn topic_min_isr_default_one_on_malformed_value() {
        let mut img = image_with_topic("t", &[1, 2, 3]);
        let mut o = BTreeMap::new();
        o.insert(MIN_INSYNC_REPLICAS.into(), "not-a-number".into());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: o,
        }));
        assert!(
            topic_min_insync_replicas(&img, "t") == 1,
            "unparseable value must fall back to permissive default 1"
        );
    }

    #[test]
    fn topic_min_isr_handles_topic_config_without_min_isr_key() {
        // Topic has *some* override (e.g. retention.ms) but no
        // min.insync.replicas — still defaults to 1.
        let mut img = image_with_topic("t", &[1, 2, 3]);
        let mut o = BTreeMap::new();
        o.insert("retention.ms".into(), "60000".into());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: o,
        }));
        assert!(topic_min_insync_replicas(&img, "t") == 1);
    }

    #[test]
    fn produce_bytes_by_qos_tier_groups_topic_payload_bytes() {
        let mut img = image_with_topic("gold-topic", &[1]);
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "default-topic".into(),
            topic_id: Uuid::from_u128(2),
            partitions: 1,
            replication_factor: 1,
        }));
        set_qos_tier(&mut img, "gold-topic", "gold");

        let topics = vec![
            framed_topic("gold-topic", &[10, 15]),
            framed_topic("default-topic", &[7]),
            framed_topic("gold-topic", &[5]),
        ];

        let grouped = produce_bytes_by_qos_tier(&img, &topics);

        let expected: BTreeMap<String, u64> = BTreeMap::from([
            ("gold".to_string(), 30),
            (crate::config_keys::DEFAULT_QOS_TIER.to_string(), 7),
        ]);
        assert!(grouped == expected);
    }

    #[test]
    fn build_topic_error_response_preserves_topic_and_partition_fields() {
        use crabka_protocol::owned::produce_response::{
            LeaderIdAndEpoch, PartitionProduceResponse, TopicProduceResponse,
        };
        let topic_id = crabka_protocol::primitives::uuid::Uuid([7; 16]);
        let topic = FramedTopic {
            name: "orders".into(),
            topic_id,
            partition_data: vec![
                FramedPartition {
                    index: 0,
                    payload: PartitionPayload::Null,
                },
                FramedPartition {
                    index: 4,
                    payload: PartitionPayload::Slice(Bytes::from_static(b"not-a-batch")),
                },
            ],
        };

        let resp = build_topic_error_response(&topic, crate::codes::UNKNOWN_TOPIC_ID);

        let error_partition = |index: i32| PartitionProduceResponse {
            index,
            error_code: crate::codes::UNKNOWN_TOPIC_ID,
            base_offset: -1,
            log_append_time_ms: -1,
            log_start_offset: -1,
            record_errors: vec![],
            error_message: None,
            current_leader: LeaderIdAndEpoch {
                leader_id: -1,
                leader_epoch: -1,
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
            },
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        let expected = TopicProduceResponse {
            name: "orders".to_string(),
            topic_id,
            partition_responses: vec![error_partition(0), error_partition(4)],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[test]
    fn resolve_topic_compression_distinguishes_producer_and_forced_codecs() {
        let cases = [
            // "producer" keeps the producer's codec → no forced compression.
            ("producer", None),
            // A concrete codec forces recompression to that codec.
            ("zstd", Some(CompressionType::Zstd)),
        ];
        for (config_value, want) in cases {
            let mut img = image_with_topic("t", &[1]);
            let mut overrides = BTreeMap::new();
            overrides.insert(
                crate::config_keys::COMPRESSION_TYPE.into(),
                config_value.into(),
            );
            img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "t".into(),
                overrides,
            }));
            assert!(
                resolve_topic_compression(&img, "t") == want,
                "compression.type {config_value:?}"
            );
        }
    }

    #[test]
    fn decode_owned_batch_preserves_non_default_header_and_record_fields() {
        let batch = RecordBatch {
            last_offset_delta: 1,
            max_timestamp: 9876,
            producer_id: 22,
            producer_epoch: 3,
            base_sequence: 11,
            records: vec![
                Record {
                    value: Some(Bytes::from_static(b"a")),
                    ..Default::default()
                },
                Record {
                    value: Some(Bytes::from_static(b"b")),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let decoded = decode_owned_batch(
            RecordsPayload::V2(vec![batch]),
            "orders",
            &crate::metrics::BrokerMetrics::new(),
        )
        .expect("decode owned batch");

        check!(decoded.last_offset_delta == 1);
        check!(decoded.max_timestamp == 9876);
        check!(decoded.producer_id == 22);
        check!(decoded.producer_epoch == 3);
        check!(decoded.base_sequence == 11);
        assert!(decoded.records.len() == 2);
        check!(decoded.records[0].value.as_deref() == Some(&b"a"[..]));
        check!(decoded.records[1].value.as_deref() == Some(&b"b"[..]));
    }

    #[test]
    fn decode_owned_batch_rejects_empty_v2_payload() {
        let err = decode_owned_batch(
            RecordsPayload::V2(Vec::new()),
            "orders",
            &crate::metrics::BrokerMetrics::new(),
        )
        .unwrap_err();
        assert!(err == crate::codes::INVALID_REQUEST);
    }

    #[tokio::test]
    async fn process_partition_non_leader_preserves_current_leader_hint() {
        use crabka_protocol::owned::produce_response::{
            LeaderIdAndEpoch, PartitionProduceResponse,
        };
        let mut img = image_with_topic("orders", &[2, 3]);
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "orders".into(),
            partition: 0,
            leader: crabka_audit::NodeId(2),
            replicas: vec![crabka_audit::NodeId(2), crabka_audit::NodeId(3)],
            isr: vec![crabka_audit::NodeId(2), crabka_audit::NodeId(3)],
            leader_epoch: 17,
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 1,
        }));
        let image = Arc::new(img);
        let partitions = Arc::new(crate::partition_registry::PartitionRegistry::new());
        let txn_coordinator = Arc::new(crate::txn::coordinator::TxnCoordinator::new(
            crabka_audit::NodeId(1),
            Arc::clone(&partitions),
            Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
        ));
        let producer_state = Arc::new(crate::producer_state::ProducerState::new());
        let log_dir_status = crate::log_dir_status::LogDirRegistry::default();
        let metrics = crate::metrics::BrokerMetrics::new();
        let payload = encode_batch(&RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"hello")),
                ..Default::default()
            }],
            ..Default::default()
        });

        let resp = process_partition(
            FramedPartition {
                index: 0,
                payload: PartitionPayload::Slice(payload),
            },
            None,
            "orders",
            false,
            false,
            1,
            Duration::from_millis(1),
            &partitions,
            &txn_coordinator,
            &producer_state,
            &log_dir_status,
            &image,
            crabka_audit::NodeId(1),
            &metrics,
        )
        .await
        .expect("process partition");

        let expected = PartitionProduceResponse {
            index: 0,
            error_code: crate::codes::NOT_LEADER_OR_FOLLOWER,
            base_offset: 0,
            log_append_time_ms: -1,
            log_start_offset: -1,
            record_errors: vec![],
            error_message: None,
            current_leader: LeaderIdAndEpoch {
                leader_id: 2,
                leader_epoch: 17,
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
            },
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[test]
    fn consume_producer_quota_tuple_match_overage_throttles() {
        use crabka_metadata::{ClientQuotaRecord, MetadataImage, MetadataRecord, QuotaEntity};
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![
                QuotaEntity {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                },
                QuotaEntity {
                    entity_type: "client-id".into(),
                    entity_name: Some("app-x".into()),
                },
            ],
            config_key: "producer_byte_rate".into(),
            config_value: Some(1024.0),
        }));
        let buckets = crate::quota::QuotaBuckets::new();
        // Tuple match → 4096 bytes overage at 1024 B/s → throttle > 0.
        let delay_match =
            crate::quota::consume_producer_quota(&img, &buckets, "alice", "app-x", "default", 4096);
        assert!(
            delay_match > std::time::Duration::ZERO,
            "tuple quota match should throttle on overage; got {delay_match:?}"
        );
        // No tuple match for client_id="other"; no (user=alice)-only quota exists.
        let buckets2 = crate::quota::QuotaBuckets::new();
        let delay_other = crate::quota::consume_producer_quota(
            &img, &buckets2, "alice", "other", "default", 4096,
        );
        assert!(
            delay_other == std::time::Duration::ZERO,
            "non-matching client_id should not throttle; got {delay_other:?}"
        );
    }

    // ── verbatim passthrough predicate (prepare_batch + build_produce_data) ──
    //
    // These drive the new header-only dispatch end to end: `prepare_batch`
    // extracts the v2 header fields (WITHOUT decompressing) and decides
    // verbatim-vs-owned; `build_produce_data` maps the result to the writer's
    // `ProduceData`, stamping the leader epoch.
    mod verbatim {
        use assert2::{assert, check};
        use bytes::{Bytes, BytesMut};
        use crabka_compression::CompressionType;
        use crabka_protocol::records::{Attributes, Record, RecordBatch, TimestampType};

        use super::super::{
            PartitionPayload, PreparedSource, ProduceData, build_produce_data, prepare_batch,
        };

        fn encode(b: &RecordBatch) -> Bytes {
            let mut buf = BytesMut::new();
            b.encode(&mut buf).unwrap();
            buf.freeze()
        }

        fn plain_batch() -> RecordBatch {
            RecordBatch {
                base_offset: 999,
                partition_leader_epoch: -1,
                last_offset_delta: 0,
                max_timestamp: 42,
                producer_id: -1,
                records: vec![Record {
                    value: Some(Bytes::from_static(b"hello")),
                    ..Default::default()
                }],
                ..Default::default()
            }
        }

        #[test]
        fn message_count_reports_v2_record_total() {
            // Multi-record batch so the count can't be mistaken for a constant.
            let batch = RecordBatch {
                last_offset_delta: 2,
                records: vec![
                    Record {
                        value: Some(Bytes::from_static(b"a")),
                        ..Default::default()
                    },
                    Record {
                        value: Some(Bytes::from_static(b"b")),
                        ..Default::default()
                    },
                    Record {
                        value: Some(Bytes::from_static(b"c")),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            };
            let wire = encode(&batch);
            // A null field and a non-v2 (zeroed) slice both contribute zero.
            let cases = [
                (PartitionPayload::Slice(wire), 3, "v2 slice with 3 records"),
                (PartitionPayload::Null, 0, "null records field"),
                (
                    PartitionPayload::Slice(Bytes::from_static(&[0u8; 64])),
                    0,
                    "non-v2 zeroed slice",
                ),
            ];
            for (payload, want, label) in cases {
                assert!(payload.message_count() == want, "case: {label}");
            }
        }

        /// Run the full dispatch over a v≥3 records slice: `prepare_batch`
        /// then `build_produce_data` with the given leader epoch.
        fn dispatch_slice(
            slice: Bytes,
            topic_compression: Option<CompressionType>,
            leader_epoch: i32,
        ) -> ProduceData {
            let m = crate::metrics::BrokerMetrics::new();
            let prepared =
                prepare_batch(PartitionPayload::Slice(slice), topic_compression, "t", &m).unwrap();
            build_produce_data(prepared, leader_epoch)
        }

        #[test]
        fn passthrough_when_all_conditions_hold() {
            let b = plain_batch();
            let wire = encode(&b);
            let data = dispatch_slice(wire.clone(), None, 7);
            match data {
                ProduceData::Verbatim(v) => {
                    check!(&v.bytes[..] == &wire[..]);
                    check!(v.leader_epoch == 7);
                    check!(v.max_timestamp == 42);
                    check!(v.last_offset_delta == 0);
                }
                ProduceData::Owned(_) => panic!("expected Verbatim"),
            }
        }

        #[test]
        fn passthrough_when_target_codec_equals_current() {
            // Topic forces lz4; batch is already lz4 → no recompression needed.
            let mut b = plain_batch();
            b.attributes = b.attributes.with_compression(CompressionType::Lz4);
            let wire = encode(&b);
            let data = dispatch_slice(wire, Some(CompressionType::Lz4), 1);
            assert!(matches!(data, ProduceData::Verbatim(_)));
        }

        #[test]
        fn fallback_when_null_field() {
            // A wire-null records field is rejected as INVALID_REQUEST.
            let m = crate::metrics::BrokerMetrics::new();
            let err = prepare_batch(PartitionPayload::Null, None, "t", &m).unwrap_err();
            assert!(err == crate::codes::INVALID_REQUEST);
        }

        #[test]
        fn fallback_on_recompression_to_different_codec() {
            // Batch uncompressed, topic forces zstd → must recompress (owned).
            let b = plain_batch();
            let wire = encode(&b);
            let data = dispatch_slice(wire, Some(CompressionType::Zstd), 0);
            assert!(matches!(data, ProduceData::Owned(_)));
        }

        #[test]
        fn fallback_on_log_append_time() {
            let mut b = plain_batch();
            b.attributes = b
                .attributes
                .with_timestamp_type(TimestampType::LogAppendTime);
            let wire = encode(&b);
            let data = dispatch_slice(wire, None, 0);
            assert!(matches!(data, ProduceData::Owned(_)));
        }

        #[test]
        fn fallback_on_control_batch() {
            let mut b = plain_batch();
            b.attributes = Attributes::default().with_control(true);
            let wire = encode(&b);
            let data = dispatch_slice(wire, None, 0);
            assert!(matches!(data, ProduceData::Owned(_)));
        }

        #[test]
        fn fallback_on_corrupt_crc_slice() {
            let b = plain_batch();
            let mut wire = encode(&b).to_vec();
            // Corrupt a body byte → CRC validation fails → owned fallback.
            let hdr_len = crabka_protocol::records::HEADER_LEN;
            wire[hdr_len] ^= 0xFF;
            // A corrupt CRC also fails the owned `RecordBatch::decode`, so the
            // fallback surfaces INVALID_RECORD (the prior decode-error code).
            let m = crate::metrics::BrokerMetrics::new();
            let err = prepare_batch(PartitionPayload::Slice(Bytes::from(wire)), None, "t", &m)
                .unwrap_err();
            assert!(err == crate::codes::INVALID_RECORD);
        }

        #[test]
        fn fallback_on_multiple_batches_in_slice() {
            // Two concatenated batches in one slice → not a single batch → owned.
            let b = plain_batch();
            let mut two = BytesMut::new();
            b.encode(&mut two).unwrap();
            b.encode(&mut two).unwrap();
            let data = dispatch_slice(two.freeze(), None, 0);
            assert!(matches!(data, ProduceData::Owned(_)));
        }

        #[test]
        fn transactional_batch_can_pass_through() {
            let mut b = plain_batch();
            b.producer_id = 100;
            b.producer_epoch = 0;
            b.attributes = b.attributes.with_transactional(true);
            let wire = encode(&b);
            let data = dispatch_slice(wire, None, 0);
            match data {
                ProduceData::Verbatim(v) => {
                    assert!(v.is_transactional);
                    assert!(v.producer_id == crabka_log::ProducerId(100));
                }
                ProduceData::Owned(_) => panic!("transactional data batch should pass through"),
            }
        }

        /// A producer-LZ4-compressed batch whose DECOMPRESSED form is huge
        /// (100 KiB) but whose compressed wire bytes are tiny takes the
        /// verbatim path WITHOUT decompressing: the stored `Verbatim.bytes`
        /// equal the compressed wire bytes (far smaller than the decompressed
        /// payload), and the header fields (`last_offset_delta`,
        /// `max_timestamp`) are read straight from the v2 header. This pins
        /// the "no decompress on the verbatim path" guarantee.
        #[test]
        fn lz4_batch_passes_through_without_decompress() {
            // 100 KiB of highly-compressible payload across many records.
            let big = vec![b'A'; 100 * 1024];
            let mut b = RecordBatch {
                last_offset_delta: 0,
                max_timestamp: 7_777,
                producer_id: -1,
                ..RecordBatch::default()
            };
            b.attributes = b.attributes.with_compression(CompressionType::Lz4);
            b.records.push(Record {
                value: Some(Bytes::from(big.clone())),
                ..Default::default()
            });
            let wire = encode(&b);
            // The compressed wire bytes must be far smaller than the raw payload,
            // so an accidental decompress would be obvious.
            assert!(
                wire.len() < big.len() / 4,
                "lz4 wire ({} B) should be much smaller than raw ({} B)",
                wire.len(),
                big.len()
            );

            let data = dispatch_slice(wire.clone(), None, 3);
            match data {
                ProduceData::Verbatim(v) => {
                    // Stored bytes are the COMPRESSED wire bytes — verbatim, not
                    // re-encoded from decompressed records ("must stay compressed").
                    // Header fields came from the v2 header, no record decode.
                    check!(&v.bytes[..] == &wire[..]);
                    check!(v.bytes.len() == wire.len());
                    check!(v.bytes.len() < big.len());
                    check!(v.max_timestamp == 7_777);
                    check!(v.last_offset_delta == 0);
                    check!(v.leader_epoch == 3);
                }
                ProduceData::Owned(_) => {
                    panic!("lz4 producer batch must pass through verbatim (no decompress)")
                }
            }
        }

        /// Idempotent dedup over the verbatim path is driven by HEADER fields:
        /// `prepare_batch` exposes `producer_id`/`producer_epoch`/`base_sequence`/
        /// `last_offset_delta` read from the v2 header (no record decode), and
        /// they match what an owned decode of the same bytes would yield.
        #[test]
        fn header_fields_drive_dedup_on_verbatim_path() {
            let mut b = plain_batch();
            b.producer_id = 4242;
            b.producer_epoch = 9;
            b.base_sequence = 17;
            b.last_offset_delta = 2;
            b.max_timestamp = 555;
            // Force lz4 so a decode would have to decompress; the verbatim path
            // must NOT, yet still surface identical header fields.
            b.attributes = b.attributes.with_compression(CompressionType::Lz4);
            let wire = encode(&b);

            let m = crate::metrics::BrokerMetrics::new();
            let prepared =
                prepare_batch(PartitionPayload::Slice(wire.clone()), None, "t", &m).unwrap();
            assert!(matches!(prepared.source, PreparedSource::Verbatim(_)));
            check!(prepared.producer_id == 4242);
            check!(prepared.producer_epoch == 9);
            check!(prepared.base_sequence == 17);
            check!(prepared.last_offset_delta == 2);
            check!(prepared.max_timestamp == 555);

            // Cross-check: an owned decode of the same compressed bytes yields
            // the same header identity (proving the header read is correct).
            let mut cur: &[u8] = &wire;
            let owned = RecordBatch::decode(&mut cur).unwrap();
            check!(owned.producer_id == prepared.producer_id);
            check!(owned.producer_epoch == prepared.producer_epoch);
            check!(owned.base_sequence == prepared.base_sequence);
            check!(owned.last_offset_delta == prepared.last_offset_delta);
        }
    }
}
