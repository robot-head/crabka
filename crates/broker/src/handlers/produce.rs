//! `Produce` (`api_key=0`). Routes each partition's records to that
//! partition's writer-actor and awaits the assigned base offset.
//!
//! One `RecordBatch` per (topic, partition) per request. The generated
//! `PartitionProduceData.records` field is `Option<RecordsPayload>`.
//! Versions 0-2 carry a v0/v1 `MessageSet` (legacy) which is up-converted
//! to a v2 `RecordBatch` before append. Versions 3+ carry a native v2
//! `RecordBatch`. Clients that send a single v2 batch per partition (the
//! typical modern case) are fully supported.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::produce_request::{PartitionProduceData, ProduceRequest};
use crabka_protocol::owned::produce_response::{
    LeaderIdAndEpoch, PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::records::{
    RecordsPayload, TimestampType, produce_record_slices, validate_one_v2_batch,
};
use crabka_protocol::{Decode, Encode};
use tokio::sync::oneshot;

use crate::authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics};
use crate::broker::Broker;
use crate::codes;
use crate::config_keys::{COMPRESSION_TYPE, MIN_INSYNC_REPLICAS, parse_compression_type};
use crate::error::BrokerError;
use crate::partition::{Partition, ProduceData, ProduceJob, WriterMessage};
use crate::partition_registry::PartitionRegistry;
use crabka_log::VerbatimBatch;

/// Resolve `min.insync.replicas` for a topic from the metadata image.
/// Defaults to `1` (Kafka's default — every cluster has at least the
/// leader in ISR), and silently falls back to `1` on malformed values
/// (the `AlterConfigs` validator already rejected invalid values, so
/// any non-parseable string here is a corrupt metadata image — safer
/// to err toward the permissive default than to wedge produce).
fn topic_min_insync_replicas(image: &crabka_metadata::MetadataImage, topic: &str) -> i32 {
    image
        .topic_config(topic)
        .and_then(|m| m.get(MIN_INSYNC_REPLICAS))
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(1)
}

#[allow(clippy::too_many_lines)]
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
    let mut cur: &[u8] = req_bytes;
    let req: ProduceRequest = if (0..3).contains(&version) {
        crabka_protocol::kafka_3_6_2::owned::produce_request::ProduceRequest::decode(
            &mut cur, version,
        )?
        .into()
    } else {
        ProduceRequest::decode(&mut cur, version)?
    };
    let timeout = Duration::from_millis(u64::try_from(req.timeout_ms.max(0)).unwrap_or(0));

    // ── verbatim passthrough capture (zero-copy) ────────────────
    // For v≥3 (native v2 payloads) re-slice each partition's records
    // field as a refcounted `Bytes` view of the request frame, so the
    // passthrough-safe batches can be appended without decode/re-encode.
    // The walk mirrors the request wire format exactly and is keyed by
    // (topic_index, partition_index) so it lines up with `req.topic_data`
    // iteration order below. Legacy (v0-2) requests never take the
    // passthrough path, so the capture is skipped for them.
    let record_slices: RecordSliceMap = if version >= 3 {
        build_record_slice_map(body_bytes, version)
    } else {
        RecordSliceMap::default()
    };

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
    let total_produce_bytes: u64 = req
        .topic_data
        .iter()
        .flat_map(|t| t.partition_data.iter())
        .map(|p| p.records.as_ref().map_or(0, |r| r.payload_len() as u64))
        .sum();

    for (topic_index, topic) in req.topic_data.into_iter().enumerate() {
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
        // consuming `partition_data`. Sum the record-batch encoded
        // lengths so the bytes-in counter matches the wire-level
        // payload. We count even for authorize-denied / unknown-topic
        // paths since the produce *request* arrived; that mirrors
        // Kafka's BrokerTopicMetrics semantics.
        if !topic_name.is_empty() {
            let mut topic_bytes: u64 = 0;
            // Also tally records-per-batch for
            // `messages_in_total`. V2 payloads expose
            // `records.len()` directly; legacy MessageSet payloads
            // remain opaque here and the upconversion-time
            // accounting already counts those arrivals.
            let mut topic_messages: u64 = 0;
            for p in &topic.partition_data {
                let partition_bytes = p.records.as_ref().map_or(0, |r| r.payload_len() as u64);
                broker
                    .metrics
                    .record_partition_produce(&topic_name, p.index, partition_bytes);
                topic_bytes += partition_bytes;
                if let Some(batches) = p.records.as_ref().and_then(RecordsPayload::as_v2) {
                    topic_messages += batches.iter().map(|b| b.records.len() as u64).sum::<u64>();
                }
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

        for (partition_index, part_data) in topic.partition_data.into_iter().enumerate() {
            let idx = part_data.index;
            // Pull the verbatim records slice captured for this exact
            // (topic, partition) wire position. `None` for legacy requests
            // or a null records field → forces the owned path.
            let verbatim_slice = record_slices.get(topic_index, partition_index);
            // Time the per-partition handler work for the
            // rebalancer's CpuUsage / CpuCapacity goals via
            // tokio_metrics::TaskMonitor — only on-CPU poll duration is
            // charged (not wall-time spent awaiting the writer queue,
            // HW gate under acks=-1, or txn coordinator).
            let monitor = tokio_metrics::TaskMonitor::new();
            let out = monitor
                .instrument(process_partition(
                    part_data,
                    verbatim_slice,
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
    let data_delay = consume_producer_quota(
        &image,
        &broker.quota_buckets,
        &ctx.principal.name,
        ctx.client_id,
        total_produce_bytes,
    );
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
    part_data: PartitionProduceData,
    verbatim_slice: Option<Bytes>,
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

    // Either there's a single decoded RecordBatch to append, or
    // the field was null / undecodable → INVALID_REQUEST / INVALID_RECORD.
    let mut batch = match decode_single_batch(part_data.records, topic_name, metrics) {
        Ok(rb) => rb,
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
            leader_id: i32::try_from(leader).unwrap_or(-1),
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
    let Some(part) = partitions.get(topic_name, idx) else {
        out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
        out.current_leader = LeaderIdAndEpoch {
            leader_id: i32::try_from(leader).unwrap_or(-1),
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
    if acks == -1
        && let Some(pr) = image.partition(topic_name, idx)
    {
        let min_isr = topic_min_insync_replicas(image, topic_name);
        if i32::try_from(pr.isr.len()).unwrap_or(i32::MAX) < min_isr {
            out.error_code = codes::NOT_ENOUGH_REPLICAS;
            return Ok(out);
        }
    }

    // Stamp the current leader epoch onto the batch — this becomes
    // the `partition_leader_epoch` carried on the wire and used by
    // KIP-101 fence validation on the follower's Fetch.
    batch.partition_leader_epoch = part
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);

    // ── transactional produce verify (KIP-1319 v2) ──────────
    // This check is more authoritative than idempotent dedup,
    // so it runs first. Non-transactional batches (pid < 0 or
    // is_transactional=false) skip directly to the dedup gate.
    let is_transactional = batch.attributes.is_transactional();
    {
        let pid_txn = batch.producer_id;
        let epoch_txn = batch.producer_epoch;
        if is_transactional && pid_txn >= 0 {
            let Some(tid) = txn_coordinator.tid_for_pid(pid_txn) else {
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
                    partition: idx,
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
    let pid = batch.producer_id;
    let epoch = batch.producer_epoch;
    let base_seq = batch.base_sequence;
    let last_offset_delta = batch.last_offset_delta;
    let max_timestamp = batch.max_timestamp;

    let dedup_outcome = if pid >= 0 {
        Some(
            producer_state
                .check(topic_name, idx, pid, epoch, base_seq, last_offset_delta)
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
            if acks == -1 {
                let target = base_offset + i64::from(last_offset_delta) + 1;
                let deadline = std::time::Instant::now() + timeout;
                match part.await_hw_at_least(target, deadline).await {
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
    // The leader epoch was just stamped onto the owned `batch` (above);
    // reuse it for the verbatim meta so both paths assign the same epoch.
    let leader_epoch = batch.partition_leader_epoch;
    let data = build_produce_data(batch, verbatim_slice, topic_compression, leader_epoch);

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
    base_offset: i64,
    producer_state: &Arc<crate::producer_state::ProducerState>,
    key: &CommitKey<'_>,
) {
    if acks == -1 {
        let target = base_offset + i64::from(key.last_offset_delta) + 1;
        let deadline = std::time::Instant::now() + timeout;
        out.error_code = match part.await_hw_at_least(target, deadline).await {
            Ok(()) => codes::NONE,
            Err(_timeout) => codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        };
    } else {
        out.error_code = codes::NONE;
    }
    out.base_offset = base_offset;
    if key.pid >= 0 {
        producer_state
            .commit(
                key.topic,
                key.partition,
                key.pid,
                key.epoch,
                key.base_seq,
                key.last_offset_delta,
                base_offset,
                key.max_timestamp,
            )
            .await;
    }
}

/// Extract the single [`RecordBatch`] to append from a partition's
/// `records` field, up-converting legacy v0/v1 `MessageSet` payloads.
///
/// Returns the error *code* to write into the response on failure:
///   * `INVALID_REQUEST` — null field or an empty v2 batch sequence.
///   * `INVALID_RECORD` — legacy up-conversion failed.
///
/// Produce-request decoding (`RecordsPayload::decode` → `from_bytes`)
/// only ever yields `V2` or `Legacy`; the `Raw` variant is produced
/// solely on the Fetch *response* pass-through path and never arrives
/// here. It is handled defensively for totality.
fn decode_single_batch(
    records: Option<RecordsPayload>,
    topic_name: &str,
    metrics: &crate::metrics::BrokerMetrics,
) -> Result<crabka_protocol::records::RecordBatch, i16> {
    let Some(payload) = records else {
        return Err(codes::INVALID_REQUEST);
    };
    match payload {
        RecordsPayload::V2(batches) => batches.into_iter().next().ok_or(codes::INVALID_REQUEST),
        RecordsPayload::Raw(bytes) => {
            // PERF/dead-code: `Raw` is unreachable on the produce path —
            // the request decoder eagerly parses v2 bytes into `V2`
            // (see crate `records::payload::RecordsPayload::from_bytes`,
            // invoked by the generated `PartitionProduceData::decode`),
            // so the already-parsed batch is consumed in the `V2` arm
            // above with no second decode. This arm exists only for
            // totality and preserves the prior decode-the-sole-batch
            // behavior should a `Raw` ever reach here.
            RecordsPayload::from_bytes(bytes)
                .ok()
                .and_then(|p| match p {
                    RecordsPayload::V2(mut v) => v.drain(..).next(),
                    RecordsPayload::Raw(_) | RecordsPayload::Legacy(_) => None,
                })
                .ok_or(codes::INVALID_REQUEST)
        }
        RecordsPayload::Legacy(bytes) => match crabka_records_legacy::legacy_to_v2(&bytes) {
            Ok(rb) => {
                // Account this Produce-path up-conversion. Kept inside the
                // success arm so failed conversions (counted as
                // INVALID_RECORD errors) don't double-count.
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

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
fn consume_producer_quota(
    image: &crabka_metadata::MetadataImage,
    buckets: &crate::quota::QuotaBuckets,
    principal: &str,
    client_id: &str,
    bytes: u64,
) -> Duration {
    let Some((entity_key, rate)) =
        crate::quota::lookup_quota_with_key(image, principal, client_id, "producer_byte_rate")
    else {
        return Duration::ZERO;
    };
    if rate <= 0.0 {
        return Duration::ZERO;
    }
    let bucket = buckets.get_or_create("producer_byte_rate", &entity_key, rate as u64);
    let granted = bucket.try_consume(bytes);
    if granted >= bytes {
        return Duration::ZERO;
    }
    let overage = bytes - granted;
    let delay_secs = overage as f64 / rate;
    Duration::from_micros((delay_secs * 1_000_000.0) as u64).min(Duration::from_secs(1))
}

/// Build a topic-level error response for KIP-516 id-resolution failures
/// (`UNKNOWN_TOPIC_ID`, `INCONSISTENT_TOPIC_ID`). Every partition row in
/// the request receives the same error code; `base_offset` is set to -1 to
/// signal "no offset assigned", matching Kafka's behavior on pre-append errors.
fn build_topic_error_response(
    topic: &crabka_protocol::owned::produce_request::TopicProduceData,
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
                base_offset: -1,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// Per-`(topic_index, partition_index)` verbatim records slices captured
/// from the request frame, indexed in wire order so the handler's
/// `req.topic_data` iteration can look them up positionally.
#[derive(Default)]
struct RecordSliceMap {
    /// `slices[topic_index][partition_index]` = the records field bytes,
    /// or `None` for a wire-null field.
    slices: Vec<Vec<Option<Bytes>>>,
}

impl RecordSliceMap {
    /// Look up and clone (refcount bump) the verbatim slice for a wire
    /// position. Returns `None` when out of range (e.g. capture skipped /
    /// failed) or the field was null.
    fn get(&self, topic_index: usize, partition_index: usize) -> Option<Bytes> {
        self.slices
            .get(topic_index)
            .and_then(|parts| parts.get(partition_index))
            .and_then(Clone::clone)
    }
}

/// Walk the produce request body and group the captured records slices by
/// topic so they can be indexed `[topic_index][partition_index]`. A walk
/// failure (malformed frame — already rejected by the real decoder, so
/// unreachable in practice) yields an empty map, forcing every partition
/// onto the owned path; correctness is never compromised.
fn build_record_slice_map(body_bytes: Bytes, version: i16) -> RecordSliceMap {
    let Ok(flat) = produce_record_slices(body_bytes, version) else {
        return RecordSliceMap::default();
    };
    let mut slices: Vec<Vec<Option<Bytes>>> = Vec::new();
    for s in flat {
        if s.topic_index >= slices.len() {
            slices.resize_with(s.topic_index + 1, Vec::new);
        }
        let parts = &mut slices[s.topic_index];
        if s.partition_index >= parts.len() {
            parts.resize_with(s.partition_index + 1, || None);
        }
        parts[s.partition_index] = s.records;
    }
    RecordSliceMap { slices }
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

/// Decide the append shape for one batch: verbatim passthrough when ALL of
/// the passthrough-safe conditions hold, else the owned decode/re-encode
/// fallback.
///
/// Passthrough-safe predicate (ALL must hold):
///   1. a verbatim records slice was captured (v≥3 native-v2 request);
///   2. `timestamp_type == CreateTime` (no log-append-time rewrite, which
///      would touch CRC-covered header bytes);
///   3. the batch is **not** a control batch (its LSO bookkeeping needs
///      the inner marker record, which the header-only path can't read);
///   4. no broker-side recompression — the topic's `compression.type` is
///      `producer` pass-through (`None`) OR equals the batch's own codec;
///   5. the slice is exactly one complete, CRC-valid v2 batch (the modern
///      single-batch-per-partition producer case).
///
/// On any miss the owned `RecordBatch` is used, preserving today's
/// behavior exactly. The owned arm is a complete fallback, so reverting
/// this whole feature is "always return `Owned`".
fn build_produce_data(
    batch: crabka_protocol::records::RecordBatch,
    verbatim_slice: Option<Bytes>,
    topic_compression: Option<crabka_compression::CompressionType>,
    leader_epoch: i32,
) -> ProduceData {
    let Some(bytes) = verbatim_slice else {
        return ProduceData::Owned(batch);
    };

    // (2) CreateTime only.
    if batch.attributes.timestamp_type() != TimestampType::CreateTime {
        return ProduceData::Owned(batch);
    }
    // (3) No control batches on the verbatim path.
    if batch.attributes.is_control_batch() {
        return ProduceData::Owned(batch);
    }
    // (4) No recompression: producer pass-through, or target == current.
    let current_codec = batch.attributes.compression();
    if let Some(target) = topic_compression
        && target != current_codec
    {
        return ProduceData::Owned(batch);
    }

    // (5) The slice must be exactly one complete, CRC-valid v2 batch.
    // Re-validates the producer's CRC over the verbatim bytes (header-only,
    // no record materialization) and confirms the slice carries a single
    // batch — multiple concatenated batches fall back to owned.
    match validate_one_v2_batch(&bytes) {
        Ok(v) if v.total_len == bytes.len() => ProduceData::Verbatim(VerbatimBatch {
            last_offset_delta: v.header.last_offset_delta.get(),
            max_timestamp: v.header.max_timestamp.get(),
            leader_epoch,
            producer_id: v.header.producer_id.get(),
            is_transactional: batch.attributes.is_transactional(),
            bytes,
        }),
        _ => ProduceData::Owned(batch),
    }
}

#[cfg(test)]
mod tests {
    use super::{MIN_INSYNC_REPLICAS, topic_min_insync_replicas};
    use assert2::assert;
    use crabka_metadata::{
        MetadataImage, MetadataRecord, PartitionRecord, TopicConfigRecord, TopicRecord,
    };
    use std::collections::BTreeMap;
    use uuid::Uuid;

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
            leader: *isr.first().unwrap_or(&1),
            replicas: isr.to_vec(),
            isr: isr.to_vec(),
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
        let delay_match = super::consume_producer_quota(&img, &buckets, "alice", "app-x", 4096);
        assert!(
            delay_match > std::time::Duration::ZERO,
            "tuple quota match should throttle on overage; got {delay_match:?}"
        );
        // No tuple match for client_id="other"; no (user=alice)-only quota exists.
        let buckets2 = crate::quota::QuotaBuckets::new();
        let delay_other = super::consume_producer_quota(&img, &buckets2, "alice", "other", 4096);
        assert!(
            delay_other == std::time::Duration::ZERO,
            "non-matching client_id should not throttle; got {delay_other:?}"
        );
    }

    // ── verbatim passthrough predicate (build_produce_data) ────────────
    mod verbatim {
        use super::super::{ProduceData, build_produce_data};
        use assert2::assert;
        use bytes::{Bytes, BytesMut};
        use crabka_compression::CompressionType;
        use crabka_protocol::records::{Attributes, Record, RecordBatch, TimestampType};

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
        fn passthrough_when_all_conditions_hold() {
            let b = plain_batch();
            let wire = encode(&b);
            let data = build_produce_data(b, Some(wire.clone()), None, 7);
            match data {
                ProduceData::Verbatim(v) => {
                    assert!(&v.bytes[..] == &wire[..]);
                    assert!(v.leader_epoch == 7);
                    assert!(v.max_timestamp == 42);
                    assert!(v.last_offset_delta == 0);
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
            let data = build_produce_data(b, Some(wire), Some(CompressionType::Lz4), 1);
            assert!(matches!(data, ProduceData::Verbatim(_)));
        }

        #[test]
        fn fallback_when_no_slice() {
            let b = plain_batch();
            let data = build_produce_data(b, None, None, 0);
            assert!(matches!(data, ProduceData::Owned(_)));
        }

        #[test]
        fn fallback_on_recompression_to_different_codec() {
            // Batch uncompressed, topic forces zstd → must recompress (owned).
            let b = plain_batch();
            let wire = encode(&b);
            let data = build_produce_data(b, Some(wire), Some(CompressionType::Zstd), 0);
            assert!(matches!(data, ProduceData::Owned(_)));
        }

        #[test]
        fn fallback_on_log_append_time() {
            let mut b = plain_batch();
            b.attributes = b
                .attributes
                .with_timestamp_type(TimestampType::LogAppendTime);
            let wire = encode(&b);
            let data = build_produce_data(b, Some(wire), None, 0);
            assert!(matches!(data, ProduceData::Owned(_)));
        }

        #[test]
        fn fallback_on_control_batch() {
            let mut b = plain_batch();
            b.attributes = Attributes::default().with_control(true);
            let wire = encode(&b);
            let data = build_produce_data(b, Some(wire), None, 0);
            assert!(matches!(data, ProduceData::Owned(_)));
        }

        #[test]
        fn fallback_on_corrupt_crc_slice() {
            let b = plain_batch();
            let mut wire = encode(&b).to_vec();
            // Corrupt a body byte → CRC validation fails → owned fallback.
            let hdr_len = crabka_protocol::records::HEADER_LEN;
            wire[hdr_len] ^= 0xFF;
            let data = build_produce_data(b, Some(Bytes::from(wire)), None, 0);
            assert!(matches!(data, ProduceData::Owned(_)));
        }

        #[test]
        fn fallback_on_multiple_batches_in_slice() {
            // Two concatenated batches in one slice → not a single batch → owned.
            let b = plain_batch();
            let mut two = BytesMut::new();
            b.encode(&mut two).unwrap();
            b.encode(&mut two).unwrap();
            let data = build_produce_data(b, Some(two.freeze()), None, 0);
            assert!(matches!(data, ProduceData::Owned(_)));
        }

        #[test]
        fn transactional_batch_can_pass_through() {
            let mut b = plain_batch();
            b.producer_id = 100;
            b.producer_epoch = 0;
            b.attributes = b.attributes.with_transactional(true);
            let wire = encode(&b);
            let data = build_produce_data(b, Some(wire), None, 0);
            match data {
                ProduceData::Verbatim(v) => {
                    assert!(v.is_transactional);
                    assert!(v.producer_id == 100);
                }
                ProduceData::Owned(_) => panic!("transactional data batch should pass through"),
            }
        }
    }
}
