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
    PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::records::RecordsPayload;
use crabka_protocol::{Decode, Encode};
use tokio::sync::oneshot;

use crate::authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics};
use crate::broker::Broker;
use crate::codes;
use crate::config_keys::MIN_INSYNC_REPLICAS;
use crate::error::BrokerError;
use crate::partition::{Partition, ProduceJob, WriterMessage};
use crate::partition_registry::PartitionRegistry;

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
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
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
            broker.config.authorizer.authorize(&image, &acl_req) == AuthorizationResult::Deny
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
        &image,
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

    // ── KIP-13 producer_byte_rate enforcement ───────────────────────
    let delay = consume_producer_quota(
        &image,
        &broker.quota_buckets,
        &ctx.principal.name,
        ctx.client_id,
        total_produce_bytes,
    );
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

    let part = if topic_name.is_empty() {
        None
    } else {
        partitions.get(topic_name, idx)
    };
    let Some(part) = part else {
        out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
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
                        entry.offset_commit_groups.clear();
                    }
                    entry.state = crate::txn::state::TxnState::Ongoing;
                    entry.partitions.insert(tp);
                    entry.last_update_ms = crate::txn::util::now_millis();
                    let snap = entry.clone();
                    // Lock must be dropped before the async put.
                    drop(entry);
                    txn_coordinator.put(snap).await?;
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

    let (ack_tx, ack_rx) = oneshot::channel();
    let job = WriterMessage::Produce(ProduceJob { batch, ack: ack_tx });

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
}
