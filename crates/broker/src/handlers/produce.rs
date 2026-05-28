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
use dashmap::DashMap;

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

    // ── slice-13 ACL preamble ────────────────────────────────────────
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
    // re-done inline below — but the slice-13 plan keys ACLs by topic
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
                    .topics()
                    .find(|tt| tt.topic_id.into_bytes() == t.topic_id.0)
                    .map(|tt| tt.name.clone())
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
        // we look it up in the metadata image.
        let topic_name = if !topic.name.is_empty() {
            topic.name.clone()
        } else if topic.topic_id != WireUuid::ZERO {
            let image = controller.current_image();
            image
                .topics()
                .find(|t| t.topic_id.into_bytes() == topic.topic_id.0)
                .map(|t| t.name.clone())
                .unwrap_or_default()
        } else {
            String::new()
        };

        // Slice 39: account for the topic in Prometheus before
        // consuming `partition_data`. Sum the record-batch encoded
        // lengths so the bytes-in counter matches the wire-level
        // payload. We count even for authorize-denied / unknown-topic
        // paths since the produce *request* arrived; that mirrors
        // Kafka's BrokerTopicMetrics semantics.
        if !topic_name.is_empty() {
            let mut topic_bytes: u64 = 0;
            // Slice 12j: also tally records-per-batch for
            // `messages_in_total`. V2 payloads expose
            // `records.len()` directly; legacy MessageSet payloads
            // remain opaque here and the upconversion-time slice
            // (12g) already counts those arrivals.
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

        // slice-13: if the topic was denied by the ACL preamble, every
        // partition row for it gets TOPIC_AUTHORIZATION_FAILED and the
        // real append is skipped. An empty topic_name (v ≥ 13 with an
        // unknown topic_id) maps to "" in the denied set if and only if
        // its authorize result was Deny; the no-ACL compat shim returns
        // Allow uniformly, so existing tests are unaffected.
        let topic_denied = denied_topics.contains(&topic_name);

        for part_data in topic.partition_data {
            let idx = part_data.index;
            // Slice 43f: time the per-partition handler work for the
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
                // Slice 12k: per-partition failure accounting. Bumps
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
    partitions: &Arc<DashMap<(String, i32), Arc<Partition>>>,
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
    // the field was null / undecodable → INVALID_REQUEST.
    let Some(payload) = part_data.records else {
        out.error_code = codes::INVALID_REQUEST;
        return Ok(out);
    };
    let mut batch = match payload {
        RecordsPayload::V2(batches) => {
            let Some(rb) = batches.into_iter().next() else {
                out.error_code = codes::INVALID_REQUEST;
                return Ok(out);
            };
            rb
        }
        RecordsPayload::Raw(bytes) => {
            // A producer that sent verbatim v2 bytes: decode the sole batch.
            let sole = RecordsPayload::from_bytes(bytes)
                .ok()
                .and_then(|p| match p {
                    RecordsPayload::V2(mut v) => v.drain(..).next(),
                    _ => None,
                });
            let Some(rb) = sole else {
                out.error_code = codes::INVALID_REQUEST;
                return Ok(out);
            };
            rb
        }
        RecordsPayload::Legacy(bytes) => match crabka_records_legacy::legacy_to_v2(&bytes) {
            Ok(rb) => {
                // Slice 12g: account this Produce-path up-conversion. Kept
                // inside the success arm so failed conversions (counted as
                // INVALID_RECORD errors) don't double-count.
                if !topic_name.is_empty() {
                    metrics.record_produce_message_conversion(topic_name);
                }
                rb
            }
            Err(e) => {
                tracing::warn!(error = %e, "legacy_to_v2 failed");
                out.error_code = codes::INVALID_RECORD;
                return Ok(out);
            }
        },
    };

    let part = if topic_name.is_empty() {
        None
    } else {
        partitions
            .get(&(topic_name.to_string(), idx))
            .map(|p| p.clone())
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
            // correct coordinator. Inter-broker v2 auto-add is deferred
            // (slice 10+).
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
            if acks == -1 {
                let target = base_offset + i64::from(last_offset_delta) + 1;
                let deadline = std::time::Instant::now() + timeout;
                match part.await_hw_at_least(target, deadline).await {
                    Ok(()) => {
                        out.error_code = codes::NONE;
                        out.base_offset = base_offset;
                        if pid >= 0 {
                            producer_state
                                .commit(
                                    topic_name,
                                    idx,
                                    pid,
                                    epoch,
                                    base_seq,
                                    last_offset_delta,
                                    base_offset,
                                    max_timestamp,
                                )
                                .await;
                        }
                    }
                    Err(_timeout) => {
                        out.error_code = codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND;
                        out.base_offset = base_offset;
                        if pid >= 0 {
                            producer_state
                                .commit(
                                    topic_name,
                                    idx,
                                    pid,
                                    epoch,
                                    base_seq,
                                    last_offset_delta,
                                    base_offset,
                                    max_timestamp,
                                )
                                .await;
                        }
                    }
                }
            } else {
                out.error_code = codes::NONE;
                out.base_offset = base_offset;
                if pid >= 0 {
                    producer_state
                        .commit(
                            topic_name,
                            idx,
                            pid,
                            epoch,
                            base_seq,
                            last_offset_delta,
                            base_offset,
                            max_timestamp,
                        )
                        .await;
                }
            }
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

#[cfg(test)]
mod tests {
    use super::{MIN_INSYNC_REPLICAS, topic_min_insync_replicas};
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
        assert_eq!(topic_min_insync_replicas(&img, "t"), 1);
    }

    #[test]
    fn topic_min_isr_reads_override_when_set() {
        let mut img = image_with_topic("t", &[1, 2, 3]);
        set_min_isr(&mut img, "t", 3);
        assert_eq!(topic_min_insync_replicas(&img, "t"), 3);
    }

    #[test]
    fn topic_min_isr_default_one_on_unknown_topic() {
        let img = MetadataImage::new(Uuid::nil());
        assert_eq!(
            topic_min_insync_replicas(&img, "ghost"),
            1,
            "missing topic_config must default to 1, not crash",
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
        assert_eq!(
            topic_min_insync_replicas(&img, "t"),
            1,
            "unparseable value must fall back to permissive default 1",
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
        assert_eq!(topic_min_insync_replicas(&img, "t"), 1);
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
        assert_eq!(
            delay_other,
            std::time::Duration::ZERO,
            "non-matching client_id should not throttle; got {delay_other:?}"
        );
    }
}
