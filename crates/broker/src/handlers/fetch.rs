//! `Fetch` (`api_key=1`) with long-poll support via per-partition
//! `Notify::notified()` futures.
//!
//! Records are returned as verbatim `RecordsPayload::Raw` bytes — the
//! on-disk `.log` bytes for whole v2 batches, read decode-free via
//! `Log::read_raw` and clamped at the visibility window: the high watermark
//! for `read_uncommitted` consumer fetches, `lso.min(hw)` for
//! `read_committed`, and the log-end offset (LEO) for follower fetches.
//! `read_committed` does NO server-side batch filtering — aborted/control
//! batches stay in the byte stream and the consumer drops them client-side
//! using the `aborted_transactions` list, matching Apache Kafka.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::sync::Notify;

use crabka_metadata::AclOperation;
use crabka_protocol::owned::fetch_request::FetchRequest;
use crabka_protocol::owned::fetch_response::{
    AbortedTransaction, FetchResponse, FetchableTopicResponse, PartitionData,
};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::records::{RecordBatch, RecordsPayload};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationResult, authorize_topics};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::fetch_session::{
    CachedPartitionState, FetchSessionKey, INVALID_SESSION_ID, SessionDecision,
};
use crate::partition::Partition;

type WaitFut = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// Resolved read for a single requested (topic, partition) tuple, kept
/// around so we can re-read after a long-poll wake.
struct PendingRead {
    topic_name: String,
    topic_id: WireUuid,
    partition_index: i32,
    fetch_offset: i64,
    max_bytes: i32,
    /// `true` when `isolation_level == 1` on a consumer fetch (not a
    /// follower fetch). Causes batch-level LSO filtering and populates
    /// `aborted_transactions` in the response.
    read_committed: bool,
    /// `true` when `replica_id >= 0` — i.e., the request is from a follower
    /// replicator rather than a consumer. Follower fetches see all records up
    /// to LEO and report LEO as HW/LSO; consumer fetches are clamped at HW.
    is_follower_fetch: bool,
    /// `None` for unknown topic/partition or out-of-range — final response is
    /// already filled out and won't be re-read on wake.
    partition: Option<Arc<Partition>>,
    /// Per-partition output, mutated in place by `do_read`.
    out: PartitionData,
    /// Accumulator for microseconds spent in this partition's `do_read`
    /// calls (first pass plus any long-poll re-reads). Measured as an
    /// `Instant` elapsed delta around each `do_read`. The heavy byte read
    /// runs in `spawn_blocking`, so this charges the read work without
    /// allocating a `tokio_metrics::TaskMonitor` per partition per fetch.
    /// Drained into the response-emit loop's `record_partition_cpu_micros`
    /// call.
    cpu_micros: u64,
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
    let log_dir_status = broker.log_dir_status.clone();
    let mut cur: &[u8] = req_bytes;
    let req: FetchRequest = if version < 4 {
        crabka_protocol::kafka_3_6_2::owned::fetch_request::FetchRequest::decode(&mut cur, version)?
            .into()
    } else {
        FetchRequest::decode(&mut cur, version)?
    };

    // `replica_id >= 0` means follower fetch (Apache Kafka convention).
    // KIP-903 (Kafka 3.5) moved `replica_id` into a tagged `replica_state`
    // struct on Fetch v15+; on v0-14 the original top-level field is used.
    // The codegen serializes whichever the negotiated version requires, so
    // here we accept whichever field is populated. Without this fallback,
    // every v15+ follower fetch decodes with `replica_id = -1` (the default
    // for the deprecated top-level field), the handler treats it as a
    // consumer fetch, clamps records at HW=0, and replication silently
    // stalls — which is exactly the byte-compare test's failure mode.
    let effective_replica_id = if req.replica_id >= 0 {
        req.replica_id
    } else {
        req.replica_state.replica_id
    };
    let is_follower_fetch = effective_replica_id >= 0;
    // isolation_level=1 (read_committed) only applies to consumer fetches.
    // Follower fetches always see all records regardless of isolation.
    let read_committed = !is_follower_fetch && req.isolation_level == 1;

    // ── KIP-227 session classification ───────────────────────────────
    // Decide whether this request is sessionless, opening a new
    // session, an incremental delta on an existing one, or closing
    // one. For incremental fetches the cache has already merged
    // `req.topics` into the cached subscription set and removed
    // anything in `forgotten_topics_data`, so `effective_topics`
    // below works off the resulting full subscription.
    let decision = broker.fetch_session_cache.classify(&req);
    if let SessionDecision::Error { code } = decision {
        let resp = FetchResponse {
            error_code: code,
            session_id: INVALID_SESSION_ID,
            responses: Vec::new(),
            ..Default::default()
        };
        let buf = encode_fetch_response(resp, version)?;
        return Ok(buf.freeze());
    }

    let effective_topics: Vec<EffectiveTopic> = match &decision {
        SessionDecision::Incremental { partitions, .. } => {
            group_cached_into_effective_topics(partitions)
        }
        _ => req
            .topics
            .iter()
            .map(|t| EffectiveTopic {
                topic: t.topic.clone(),
                topic_id: t.topic_id,
                partitions: t
                    .partitions
                    .iter()
                    .map(|fp| EffectivePartition {
                        partition: fp.partition,
                        current_leader_epoch: fp.current_leader_epoch,
                        fetch_offset: fp.fetch_offset,
                        partition_max_bytes: fp.partition_max_bytes,
                    })
                    .collect(),
            })
            .collect(),
    };

    // ── ACL preamble ────────────────────────────────────────
    // Batch-authorize every topic in the request for `Read` (the
    // operation Fetch requires). Topics that come back `Deny` will
    // short-circuit the per-partition log read below and emit
    // TOPIC_AUTHORIZATION_FAILED on every partition row of that topic
    // with empty records.
    //
    // Fetch v ≥ 13 sends only topic_id on the wire; ACLs are keyed
    // by topic *name*, so we resolve the names here too for
    // the authorize call (and re-resolve inline below for log lookup).
    let image = controller.current_image();
    let topic_names_for_acl: Vec<String> = effective_topics
        .iter()
        .map(|t| {
            if !t.topic.is_empty() {
                t.topic.clone()
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
        AclOperation::Read,
        topic_names_for_acl.iter().map(String::as_str),
    );
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

    // Resolve every requested partition up front. We collect pending
    // reads (rather than just doing them inline) so we can re-read once
    // after a long-poll wake without re-decoding the request.
    let mut pending: Vec<PendingRead> = Vec::new();
    for topic in &effective_topics {
        // v ≤ 12 sends the topic name; v ≥ 13 sends only topic_id and
        // we look it up. The client populates whichever the negotiated
        // version requires.
        let topic_name = if topic.topic.is_empty() {
            image
                .topics()
                .find(|t| t.topic_id.into_bytes() == topic.topic_id.0)
                .map_or_else(String::new, |t| t.name.clone())
        } else {
            topic.topic.clone()
        };
        let topic_id = if topic.topic_id == WireUuid::ZERO {
            image
                .topic(&topic_name)
                .map_or(WireUuid::ZERO, |t| WireUuid(t.topic_id.into_bytes()))
        } else {
            topic.topic_id
        };

        // If the topic was denied by the ACL preamble,
        // every partition row gets TOPIC_AUTHORIZATION_FAILED and
        // the real log read is skipped. `records` stays `None`
        // (no batch returned). An empty topic_name (v ≥ 13 with
        // an unknown topic_id) maps to "" in the denied set iff
        // its authorize result was Deny; the no-ACL compat shim
        // returns Allow uniformly, so existing tests are unaffected.
        let topic_denied = denied_topics.contains(&topic_name);

        for fp in &topic.partitions {
            let idx = fp.partition;
            let fetch_offset = fp.fetch_offset;
            let max_bytes = fp.partition_max_bytes;
            let req_current_leader_epoch = fp.current_leader_epoch;

            let mut out = PartitionData {
                partition_index: idx,
                ..Default::default()
            };

            if topic_denied {
                out.error_code = codes::TOPIC_AUTHORIZATION_FAILED;
                // Records stays `None` — the codegen encodes this as
                // an empty/null record buffer.
                pending.push(PendingRead {
                    topic_name: topic_name.clone(),
                    topic_id,
                    partition_index: idx,
                    fetch_offset,
                    max_bytes,
                    read_committed,
                    is_follower_fetch,
                    partition: None,
                    out,
                    cpu_micros: 0,
                });
                continue;
            }

            let part_opt = partitions.get(&topic_name, idx);

            // KIP-101 epoch fence. The follower (or consumer using KIP-320)
            // includes its `current_leader_epoch`; we reject stale or future
            // epochs without serving data.
            if let Some(part) = part_opt.as_ref() {
                let our_epoch = part
                    .current_leader_epoch
                    .load(std::sync::atomic::Ordering::Acquire);
                if req_current_leader_epoch >= 0 && req_current_leader_epoch != our_epoch {
                    out.error_code = if req_current_leader_epoch < our_epoch {
                        codes::FENCED_LEADER_EPOCH
                    } else {
                        codes::UNKNOWN_LEADER_EPOCH
                    };
                    pending.push(PendingRead {
                        topic_name: topic_name.clone(),
                        topic_id,
                        partition_index: idx,
                        fetch_offset,
                        max_bytes,
                        read_committed,
                        is_follower_fetch,
                        partition: None,
                        out,
                        cpu_micros: 0,
                    });
                    continue;
                }
            }

            // KIP-113 offline-dir handling: refuse to read partitions on
            // a log dir flagged offline. The Log handle is still open and
            // a read would (probably) succeed against the page cache,
            // but serving stale bytes from a dir we've told the rest of
            // the cluster is unhealthy hides the failure.
            if let Some(part) = part_opt.as_ref()
                && log_dir_status.is_offline(&part.log_dir.load())
            {
                out.error_code = codes::KAFKA_STORAGE_ERROR;
                pending.push(PendingRead {
                    topic_name: topic_name.clone(),
                    topic_id,
                    partition_index: idx,
                    fetch_offset,
                    max_bytes,
                    read_committed,
                    is_follower_fetch,
                    partition: None,
                    out,
                    cpu_micros: 0,
                });
                continue;
            }

            // Follower-fetch HW maintenance. ISR maintenance prevents stalls
            // by shrinking lagging followers out of the ISR within 2s on CI.
            if is_follower_fetch && let Some(part) = part_opt.as_ref() {
                let leader_leo = part.log_end_offset();
                let advanced = {
                    let mut st = part.replica_state.lock().await;
                    let prev = st.hw;
                    let new = st.update_follower_leo(
                        u64::try_from(effective_replica_id).unwrap_or(0),
                        fetch_offset,
                        leader_leo,
                    );
                    new > prev
                };
                if advanced {
                    part.hw_advance_notify.notify_waiters();
                }
            }

            if part_opt.is_none() || topic_name.is_empty() {
                out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                pending.push(PendingRead {
                    topic_name: topic_name.clone(),
                    topic_id,
                    partition_index: idx,
                    fetch_offset,
                    max_bytes,
                    read_committed,
                    is_follower_fetch,
                    partition: None,
                    out,
                    cpu_micros: 0,
                });
                continue;
            }

            // KIP-392: for a consumer fetch advertising client.rack, ask the
            // configured replica selector which replica it should prefer to
            // read from, and report it in `preferred_read_replica`. The field
            // only encodes at Fetch v11+ (where `rack_id` first appears), so
            // older clients are unaffected. `-1` (the default) means
            // "use the leader".
            if !is_follower_fetch
                && !req.rack_id.is_empty()
                && let Some(pr) = image.partition(&topic_name, idx)
            {
                let isr: std::collections::HashSet<crabka_metadata::NodeId> =
                    pr.isr.iter().copied().collect();
                let views: Vec<crate::replica_selector::ReplicaView> = pr
                    .replicas
                    .iter()
                    .map(|&nid| crate::replica_selector::ReplicaView {
                        node_id: i32::try_from(nid).unwrap_or(-1),
                        rack: image.broker(nid).and_then(|b| b.rack.clone()),
                        in_isr: isr.contains(&nid),
                    })
                    .collect();
                let leader_id = i32::try_from(pr.leader).unwrap_or(-1);
                out.preferred_read_replica = broker.config.replica_selector.select(
                    Some(req.rack_id.as_str()),
                    leader_id,
                    &views,
                );
            }

            pending.push(PendingRead {
                topic_name: topic_name.clone(),
                topic_id,
                partition_index: idx,
                fetch_offset,
                max_bytes,
                read_committed,
                is_follower_fetch,
                partition: part_opt,
                out,
                cpu_micros: 0,
            });
        }
    }

    // First read pass. We time each `do_read` with a wall-clock `Instant`
    // delta and charge it into p.cpu_micros. The heavy byte read now runs in
    // `spawn_blocking` (see `do_read`), so the awaited future itself does
    // little besides the cheap metadata lock + result application; a plain
    // elapsed-time delta is an adequate stand-in for the previous
    // per-partition `TaskMonitor` poll-duration sample without allocating a
    // monitor per partition per fetch.
    let mut total_bytes = 0_usize;
    for p in &mut pending {
        let Some(part) = p.partition.clone() else {
            continue;
        };
        let read_start = std::time::Instant::now();
        total_bytes += do_read(
            &part,
            p.fetch_offset,
            p.max_bytes,
            p.read_committed,
            p.is_follower_fetch,
            &mut p.out,
        )
        .await?;
        let micros = u64::try_from(read_start.elapsed().as_micros()).unwrap_or(u64::MAX);
        p.cpu_micros = p.cpu_micros.saturating_add(micros);

        // KIP-405: if the local read came back
        // OFFSET_OUT_OF_RANGE because the requested offset is below
        // `local_log_start_offset()` on a tiered topic, attempt to
        // serve the batch from the remote tier.
        if p.out.error_code == codes::OFFSET_OUT_OF_RANGE
            && let Some(serviced_bytes) = try_remote_read(broker, p, &part).await
        {
            total_bytes += serviced_bytes;
        }
    }

    // Long-poll: if we didn't satisfy min_bytes, wait on each readable
    // partition's append_notify with a single timeout, then re-read.
    let want_more = total_bytes < usize::try_from(req.min_bytes.max(0)).unwrap_or(0);
    if want_more && req.max_wait_ms > 0 {
        long_poll_then_reread(broker, &mut pending, req.max_wait_ms).await?;
    }

    // Drain per-partition cpu_micros accumulators before
    // `group_into_topic_responses` consumes `pending`. `cpu_micros_by_idx`
    // is aligned positionally with `responses` — `cpu_micros_by_idx[ti][pi]`
    // is the accumulator for `responses[ti].partitions[pi]` — so the
    // response-emit loop indexes it directly rather than re-cloning topic
    // names into a string-keyed map.
    let (mut responses, cpu_micros_by_idx) = group_into_topic_responses(pending);

    // Down-convert v2 batches to legacy MessageSet bytes for Fetch v0-3.
    // Control batches are dropped (records set to None); zstd-compressed
    // batches are re-compressed as snappy (v0/v1 has no zstd codec).
    if version < 4 {
        for topic_resp in &mut responses {
            for part in &mut topic_resp.partitions {
                if let Some(payload) = part.records.take() {
                    match crate::handlers::fetch_downconvert::down_convert_payload_for_fetch(
                        &payload, version,
                    ) {
                        Ok(Some(converted)) => {
                            // Only store the payload if it has content.
                            if converted.payload_len() > 0 {
                                part.records = Some(converted);
                            }
                            // Account this Fetch-path down-conversion.
                            // Counted even when the converted batch was empty
                            // (drops + control skips) — the work happened.
                            if !topic_resp.topic.is_empty() {
                                broker
                                    .metrics
                                    .record_fetch_message_conversion(&topic_resp.topic);
                            }
                        }
                        Ok(None) => {
                            // All batches dropped — records stays None.
                        }
                        Err(error_code) => {
                            part.error_code = error_code;
                        }
                    }
                }
            }
        }
    }

    // KIP-73 leader-side throttle: only applies to follower (inter-broker)
    // fetch requests. Consumer fetches have replica_id < 0.
    if is_follower_fetch {
        use crate::throttle::TopicThrottle;
        // `leader.replication.throttled.replicas` stores (partition, follower_id) pairs.
        // The leader throttles a follower fetch when (partition, effective_replica_id) is
        // in that set. We cast to u64 because NodeId is u64 and replica_id is i32; a
        // valid follower id is always positive so the cast is safe.
        let follower_id = u64::try_from(effective_replica_id).unwrap_or(0);
        let mut throttled_byte_count: u64 = 0;
        // (topic_idx, partition_idx) pairs for throttled chunks.
        let mut throttled_idxs: Vec<(usize, usize)> = Vec::new();
        for (ti, topic_resp) in responses.iter().enumerate() {
            let throttle = TopicThrottle::for_topic(&image, &topic_resp.topic);
            for (pi, part) in topic_resp.partitions.iter().enumerate() {
                if throttle.leader.contains(part.partition_index, follower_id) {
                    let chunk_bytes =
                        part.records.as_ref().map_or(0, RecordsPayload::payload_len) as u64;
                    throttled_byte_count += chunk_bytes;
                    throttled_idxs.push((ti, pi));
                }
            }
        }
        if throttled_byte_count > 0 {
            let granted = broker
                .throttle_state
                .leader_out
                .try_consume(throttled_byte_count);
            if granted < throttled_byte_count {
                truncate_throttled_responses(&mut responses, &throttled_idxs, granted);
            }
        }
    }

    // Consumer fetches (replica_id < 0) use client quotas; inter-broker
    // fetches (replica_id >= 0) use the KIP-73 throttle.
    let mut throttle_time_ms_val: i32 = 0;
    if !is_follower_fetch {
        // KIP-13 consumer_byte_rate. Mutually exclusive with the
        // inter-broker leader throttle (which fires only when replica_id >= 0).
        let total_bytes = sum_response_bytes(&responses);
        let delay = consume_consumer_quota(
            &image,
            &broker.quota_buckets,
            &ctx.principal.name,
            ctx.client_id,
            total_bytes,
        );
        if delay > Duration::ZERO {
            throttle_time_ms_val = i32::try_from(delay.as_millis()).unwrap_or(i32::MAX);
            tokio::time::sleep(delay).await;
        }
    }

    // Per-topic Prometheus accounting. Sum the encoded
    // record-batch bytes the response is about to ship, per topic.
    // Topics that returned an error (empty `records`) still get a
    // request count (the fetch arrived), matching Kafka's
    // BrokerTopicMetrics:TotalFetchRequestsPerSec semantics.
    for (ti, topic_resp) in responses.iter().enumerate() {
        if topic_resp.topic.is_empty() {
            continue;
        }
        let mut bytes: u64 = 0;
        for (pi, p) in topic_resp.partitions.iter().enumerate() {
            let partition_bytes = p.records.as_ref().map_or(0, RecordsPayload::payload_len) as u64;
            broker.metrics.record_partition_fetch(
                &topic_resp.topic,
                p.partition_index,
                partition_bytes,
            );
            // Per-partition failure accounting. Each
            // partition-response error row (OFFSET_OUT_OF_RANGE,
            // KAFKA_STORAGE_ERROR, FENCED_LEADER_EPOCH, etc.) bumps
            // the per-topic counter — mirrors JVM's
            // `failedFetchRequestRate.mark()`.
            if p.error_code != 0 {
                broker.metrics.record_failed_fetch(&topic_resp.topic);
            }
            // When this Fetch arrived from a follower
            // (`replica_id >= 0`), the bytes leaving the leader are
            // replication outbound, not consumer outbound. We emit a
            // separate counter rather than splitting `partition_bytes_out`
            // — the existing counter keeps its established semantics
            // (rebalancer + general broker outbound) and operators get a
            // dedicated `replication_bytes_out` counter for inter-broker
            // traffic.
            if is_follower_fetch {
                broker.metrics.record_replication_out(
                    &topic_resp.topic,
                    p.partition_index,
                    partition_bytes,
                );
            }
            // Drain the per-partition CPU accumulator. Tracks
            // actual poll duration across both the first read pass and any
            // long-poll re-reads, attributing only on-CPU time.
            // `cpu_micros_by_idx` is positionally aligned with `responses`
            // (built by `group_into_topic_responses`); the down-convert and
            // throttle passes above mutate `records` but never add or remove
            // partition rows, so indexing by `(ti, pi)` stays valid here.
            if let Some(micros) = cpu_micros_by_idx
                .get(ti)
                .and_then(|parts| parts.get(pi))
                .copied()
            {
                broker.metrics.record_partition_cpu_micros(
                    &topic_resp.topic,
                    p.partition_index,
                    micros,
                );
            }
            bytes += partition_bytes;
        }
        broker.metrics.record_fetch(&topic_resp.topic, bytes);
    }

    // ── KIP-227 response shaping + cache finalize ────────────────────
    // Decide the response `session_id` and (for Incremental) filter
    // out partitions whose state hasn't changed since the previous
    // response. Then update the cache so the next request's diff
    // comparison sees what we just sent.
    let response_session_id = match &decision {
        SessionDecision::Sessionless => INVALID_SESSION_ID,
        SessionDecision::Close { session_id } => {
            broker.fetch_session_cache.close(*session_id);
            INVALID_SESSION_ID
        }
        SessionDecision::NewSession => {
            // Snapshot what we just sent (for last_* comparison) and the
            // request's desired state (fetch_offset/max_bytes/leader_epoch)
            // so subsequent incremental reads know where to look.
            let snapshot = snapshot_response_state(&effective_topics, &responses);
            broker.fetch_session_cache.try_allocate(
                is_follower_fetch,
                ctx.principal.name.clone(),
                snapshot,
            )
        }
        SessionDecision::Incremental {
            session_id,
            partitions,
            ..
        } => {
            let cached_by_key: std::collections::HashMap<FetchSessionKey, CachedPartitionState> =
                partitions.iter().cloned().collect();
            let sent = filter_incremental_response(&mut responses, &cached_by_key);
            broker
                .fetch_session_cache
                .finalize_incremental(*session_id, &sent);
            *session_id
        }
        SessionDecision::Error { .. } => unreachable!("returned above"),
    };

    // Refresh KIP-227 gauges. Cheap: `len()` and
    // `total_partitions_cached()` read lock-free `AtomicUsize` counters
    // (no mutex, no HashMap scan), so this never contends the cache lock
    // on the hot fetch path and avoids the need for a background sampling
    // task.
    broker
        .metrics
        .incremental_fetch_sessions
        .set(i64::try_from(broker.fetch_session_cache.len()).unwrap_or(i64::MAX));
    broker.metrics.incremental_fetch_partitions_cached.set(
        i64::try_from(broker.fetch_session_cache.total_partitions_cached()).unwrap_or(i64::MAX),
    );
    let cur_evictions = broker.fetch_session_cache.evictions_total();
    let prev_evictions = broker
        .metrics
        .incremental_fetch_session_evictions_total
        .get();
    if cur_evictions > prev_evictions {
        broker
            .metrics
            .incremental_fetch_session_evictions_total
            .inc_by(cur_evictions - prev_evictions);
    }

    let resp = FetchResponse {
        throttle_time_ms: throttle_time_ms_val,
        error_code: 0,
        session_id: response_session_id,
        responses,
        ..Default::default()
    };
    Ok(encode_fetch_response(resp, version)?.freeze())
}

/// Projection of `FetchRequest::topics` / cached session partitions —
/// the minimum the read loop needs. Built once at the top of the
/// handler from either source.
struct EffectiveTopic {
    topic: String,
    topic_id: WireUuid,
    partitions: Vec<EffectivePartition>,
}

struct EffectivePartition {
    partition: i32,
    current_leader_epoch: i32,
    fetch_offset: i64,
    partition_max_bytes: i32,
}

/// Re-group the flat `(key, state)` list returned by
/// `FetchSessionCache::classify` into per-topic chunks. Topic order is
/// the order in which keys first appear — `HashMap` iteration order is
/// not stable across runs but is stable within a single classify call.
fn group_cached_into_effective_topics(
    cached: &[(FetchSessionKey, CachedPartitionState)],
) -> Vec<EffectiveTopic> {
    use std::collections::HashMap;
    let mut order: Vec<String> = Vec::new();
    let mut by_topic: HashMap<String, EffectiveTopic> = HashMap::new();
    for (k, s) in cached {
        let entry = by_topic
            .entry(k.topic_name.clone())
            .or_insert_with(|| EffectiveTopic {
                topic: k.topic_name.clone(),
                topic_id: k.topic_id,
                partitions: Vec::new(),
            });
        entry.partitions.push(EffectivePartition {
            partition: k.partition,
            current_leader_epoch: s.current_leader_epoch,
            fetch_offset: s.fetch_offset,
            partition_max_bytes: s.max_bytes,
        });
        if !order.iter().any(|t| t == &k.topic_name) {
            order.push(k.topic_name.clone());
        }
    }
    order
        .into_iter()
        .map(|n| by_topic.remove(&n).expect("populated above"))
        .collect()
}

/// Walk `responses` and snapshot every `(topic, partition)` row into a
/// `CachedPartitionState` describing what was just emitted (the `last_*`
/// fields) merged with the client's desired state for that partition
/// from `effective` (`fetch_offset`, `max_bytes`, `leader_epoch`). Used to
/// seed a brand-new session.
fn snapshot_response_state(
    effective: &[EffectiveTopic],
    responses: &[FetchableTopicResponse],
) -> Vec<(FetchSessionKey, CachedPartitionState)> {
    use std::collections::HashMap;
    // Pre-index the desired state. Topic identity differs by wire
    // version: v ≤ 12 carries topic name and zero topic_id, v ≥ 13
    // carries topic_id and empty name. The server-side response always
    // has the resolved name *and* the id, but `effective` (built from
    // `req.topics`) may have only one or the other. Index by both so
    // lookup succeeds in either direction.
    let mut by_name: HashMap<(String, i32), &EffectivePartition> = HashMap::new();
    let mut by_id: HashMap<(WireUuid, i32), &EffectivePartition> = HashMap::new();
    for et in effective {
        for ep in &et.partitions {
            if !et.topic.is_empty() {
                by_name.insert((et.topic.clone(), ep.partition), ep);
            }
            if et.topic_id != WireUuid::ZERO {
                by_id.insert((et.topic_id, ep.partition), ep);
            }
        }
    }
    let mut out = Vec::new();
    for tr in responses {
        for p in &tr.partitions {
            let key = FetchSessionKey {
                topic_name: tr.topic.clone(),
                topic_id: tr.topic_id,
                partition: p.partition_index,
            };
            let mut state = CachedPartitionState {
                last_high_watermark: p.high_watermark,
                last_last_stable_offset: p.last_stable_offset,
                last_log_start_offset: p.log_start_offset,
                last_preferred_read_replica: p.preferred_read_replica,
                last_aborted_txns_hash: hash_aborted_transactions(p.aborted_transactions.as_ref()),
                last_error_code: p.error_code,
                ..Default::default()
            };
            let ep = by_id
                .get(&(tr.topic_id, p.partition_index))
                .or_else(|| by_name.get(&(tr.topic.clone(), p.partition_index)));
            if let Some(ep) = ep {
                state.fetch_offset = ep.fetch_offset;
                state.max_bytes = ep.partition_max_bytes;
                state.current_leader_epoch = ep.current_leader_epoch;
            }
            out.push((key, state));
        }
    }
    out
}

/// KIP-227 incremental-response filter. Drops partitions whose
/// outgoing state matches the cached `last_*` snapshot (the broker
/// already told the client these values; re-sending wastes bytes).
/// Returns the `(key, sent_state)` list for the partitions that
/// survived — used by the caller to update the cache's `last_*` fields
/// to reflect what was just emitted.
fn filter_incremental_response(
    responses: &mut Vec<FetchableTopicResponse>,
    cached: &std::collections::HashMap<FetchSessionKey, CachedPartitionState>,
) -> Vec<(FetchSessionKey, CachedPartitionState)> {
    let mut sent: Vec<(FetchSessionKey, CachedPartitionState)> = Vec::new();
    for tr in responses.iter_mut() {
        tr.partitions.retain(|p| {
            let key = FetchSessionKey {
                topic_name: tr.topic.clone(),
                topic_id: tr.topic_id,
                partition: p.partition_index,
            };
            let aborted_hash = hash_aborted_transactions(p.aborted_transactions.as_ref());
            let records_present = p.records.as_ref().is_some_and(|b| b.payload_len() > 0);
            let changed = match cached.get(&key) {
                Some(prev) => {
                    records_present
                        || p.error_code != prev.last_error_code
                        || p.high_watermark != prev.last_high_watermark
                        || p.last_stable_offset != prev.last_last_stable_offset
                        || p.log_start_offset != prev.last_log_start_offset
                        || p.preferred_read_replica != prev.last_preferred_read_replica
                        || aborted_hash != prev.last_aborted_txns_hash
                }
                // Partition not in the cached set — newly added by this
                // request. Always send it once so the client sees its
                // initial state.
                None => true,
            };
            if changed {
                sent.push((
                    key,
                    CachedPartitionState {
                        last_high_watermark: p.high_watermark,
                        last_last_stable_offset: p.last_stable_offset,
                        last_log_start_offset: p.log_start_offset,
                        last_preferred_read_replica: p.preferred_read_replica,
                        last_aborted_txns_hash: aborted_hash,
                        last_error_code: p.error_code,
                        ..Default::default()
                    },
                ));
            }
            changed
        });
    }
    // Drop topics that ended up with no partitions.
    responses.retain(|tr| !tr.partitions.is_empty());
    sent
}

/// Stable hash of the aborted-transaction list for the "did anything
/// change?" comparison. Iteration order within a single response is
/// deterministic (the list is produced by `do_read` in offset order)
/// so a plain `DefaultHasher` over the sequence is enough.
fn hash_aborted_transactions(list: Option<&Vec<AbortedTransaction>>) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    match list {
        None => 0_u8.hash(&mut h),
        Some(v) => {
            1_u8.hash(&mut h);
            (v.len() as u64).hash(&mut h);
            for tx in v {
                tx.producer_id.hash(&mut h);
                tx.first_offset.hash(&mut h);
            }
        }
    }
    h.finish()
}

/// Hold the partition's log mutex briefly to read offsets + (optionally) the
/// verbatim on-disk batch bytes via `Log::read_raw`. Populates `out` in place
/// (with `RecordsPayload::Raw`) and returns the byte-size estimate of the
/// records placed in `out` (0 if none).
///
/// When `read_committed` is `true` (consumer fetch with `isolation_level=1`):
/// - raw bytes are clamped at `min(lso, hw)` (`base_offset < min(lso, hw)`)
/// - NO server-side batch filtering: aborted/control batches stay in the
///   byte stream; the consumer drops them client-side using the list below
/// - `out.last_stable_offset` is set to `min(lso, hw)`
/// - `out.aborted_transactions` is populated from the partition's `.txnindex`
///
/// When `is_follower_fetch` is `true`:
/// - raw bytes up to LEO are returned (no HW clamping)
/// - `out.high_watermark` and `out.last_stable_offset` are set to `log_end`
///
/// When `read_committed` is `false` and `is_follower_fetch` is `false`
/// (consumer fetch in `read_uncommitted`):
/// - raw bytes are clamped at HW (`base_offset < hw`)
/// - `out.high_watermark` and `out.last_stable_offset` are set to `hw`
/// - `out.aborted_transactions` is `None`
#[allow(clippy::too_many_lines)]
async fn do_read(
    part: &Partition,
    fetch_offset: i64,
    max_bytes: i32,
    read_committed: bool,
    is_follower_fetch: bool,
    out: &mut PartitionData,
) -> Result<usize, BrokerError> {
    // Decision derived from a brief metadata-only hold of the log mutex.
    // The actual byte read (`read_raw` + the optional `aborted_in_range`
    // scan, both synchronous syscalls) is deferred to `spawn_blocking` so it
    // never runs on the reactor thread under the lock.
    enum ReadPlan {
        /// `fetch_offset` is below `log_start` — `OFFSET_OUT_OF_RANGE` early
        /// return; `out` has already been fully populated.
        OffsetOutOfRange,
        /// `fetch_offset >= upper_bound` — nothing to read.
        Empty,
        /// Read bytes in `[fetch_offset, limit_offset)`.
        Read {
            limit_offset: i64,
            effective_lso: i64,
            read_committed_aborts: bool,
        },
    }

    let hw = part.high_watermark().await;

    let (log_start, log_end, lso, plan) = {
        let log = part.log.lock().expect("log mutex poisoned");
        let log_start = log.log_start_offset();
        let log_end = log.log_end_offset();
        let lso = log.lso();
        let upper_bound = if is_follower_fetch { log_end } else { hw };
        let effective_lso = if read_committed && !is_follower_fetch {
            lso.min(hw)
        } else {
            lso
        };

        let plan = if fetch_offset < log_start {
            out.error_code = codes::OFFSET_OUT_OF_RANGE;
            out.log_start_offset = log_start;
            out.high_watermark = if is_follower_fetch { log_end } else { hw };
            out.last_stable_offset = if read_committed && !is_follower_fetch {
                effective_lso
            } else if is_follower_fetch {
                log_end
            } else {
                hw
            };
            ReadPlan::OffsetOutOfRange
        } else {
            let limit_offset = if is_follower_fetch {
                log_end
            } else if read_committed {
                effective_lso
            } else {
                hw
            };

            if fetch_offset >= upper_bound {
                ReadPlan::Empty
            } else {
                ReadPlan::Read {
                    limit_offset,
                    effective_lso,
                    read_committed_aborts: read_committed && !is_follower_fetch,
                }
            }
        };
        (log_start, log_end, lso, plan)
    };
    // Log mutex released here.

    let (raw, aborted_txns): (Option<crabka_log::RawRead>, Vec<AbortedTransaction>) = match plan {
        ReadPlan::OffsetOutOfRange => return Ok(0),
        ReadPlan::Empty => (None, Vec::new()),
        ReadPlan::Read {
            limit_offset,
            effective_lso,
            read_committed_aborts,
        } => {
            let read_max = usize::try_from(max_bytes.max(0)).unwrap_or(0);
            // Run the blocking seek+read (and, for read_committed, the
            // aborted-txn index scan) off the reactor thread. The lock is
            // re-acquired inside the closure for the brief duration of the
            // syscalls.
            let log = part.log.clone();
            let join = tokio::task::spawn_blocking(move || {
                let log = log.lock().expect("log mutex poisoned");
                let raw = log.read_raw(fetch_offset, limit_offset, read_max)?;

                // read_committed does NO server-side batch filtering: verbatim
                // bytes (including aborted/control batches) are returned and the
                // consumer drops them client-side via `aborted_transactions`,
                // matching Apache Kafka's behavior. Skip the Vec allocation
                // entirely when there are no aborted txns in range.
                let aborted = if read_committed_aborts {
                    let mut it = log
                        .aborted_in_range(fetch_offset, effective_lso)
                        .into_iter();
                    if let Some(first) = it.next() {
                        let mut v = vec![AbortedTransaction {
                            producer_id: first.producer_id,
                            first_offset: first.start_offset,
                            ..Default::default()
                        }];
                        v.extend(it.map(|e| AbortedTransaction {
                            producer_id: e.producer_id,
                            first_offset: e.start_offset,
                            ..Default::default()
                        }));
                        v
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };

                Ok::<_, BrokerError>((raw, aborted))
            });
            let (raw, aborted) = match join.await {
                Ok(res) => res?,
                Err(join_err) => {
                    // A panic inside the blocking read poisoned/aborted the
                    // closure. Surface it as an I/O failure rather than
                    // propagating the panic across the await point.
                    return Err(BrokerError::Io(std::io::Error::other(format!(
                        "fetch read task panicked: {join_err}"
                    ))));
                }
            };

            let raw = if raw.total > 0 { Some(raw) } else { None };
            (raw, aborted)
        }
    };

    out.error_code = codes::NONE;
    out.high_watermark = if is_follower_fetch { log_end } else { hw };
    out.log_start_offset = log_start;
    out.last_stable_offset = if read_committed && !is_follower_fetch {
        lso.min(hw)
    } else if is_follower_fetch {
        log_end
    } else {
        hw
    };

    if read_committed && !is_follower_fetch {
        // Populate aborted_transactions: None means "no list" (same as not
        // providing it); Some(empty) means "committed window with no aborts".
        // Apache Kafka sends Some(empty) when in read_committed mode.
        out.aborted_transactions = Some(aborted_txns);
    }

    let bytes_est = raw.as_ref().map_or(0, |r| r.total);
    out.records = raw.map(|r| RecordsPayload::Raw(r.bytes));
    Ok(bytes_est)
}

/// KIP-405: try to serve `p`'s requested offset from the remote
/// tier when the local log returned `OFFSET_OUT_OF_RANGE` and the topic has
/// `remote.storage.enable=true`. On success, replaces the partition's error +
/// records and returns the encoded batch size; on miss / error / non-tiered,
/// leaves `p.out` untouched and returns `None`.
async fn try_remote_read(broker: &Broker, p: &mut PendingRead, part: &Partition) -> Option<usize> {
    let reader = broker.remote_reader.clone()?;
    let remote_storage_enable = {
        let log = part.log.lock().expect("log mutex poisoned");
        log.config_snapshot().remote_storage_enable
    };
    if !remote_storage_enable {
        return None;
    }
    if p.topic_id == WireUuid::ZERO {
        // Without a topic_id we can't build `TopicIdPartition` keyed the
        // same way the RLMM stores entries (Kafka's equality is by id +
        // partition).
        return None;
    }
    let topic_id = uuid::Uuid::from_bytes(p.topic_id.0);
    let tp = crabka_remote_storage::TopicIdPartition::new(
        topic_id,
        p.topic_name.clone(),
        p.partition_index,
    );
    let leader_epoch = part
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    let max_bytes = usize::try_from(p.max_bytes.max(0)).unwrap_or(0);

    match reader
        .fetch_batch(&tp, leader_epoch, p.fetch_offset, max_bytes)
        .await
    {
        Ok(Some(batch)) => {
            let bytes_est = <RecordBatch as Encode>::encoded_len(&batch, 0);
            p.out.error_code = codes::NONE;
            // `log_start_offset` / HW / LSO stay at whatever `do_read`
            // wrote out (the local view); the remote tier doesn't change
            // those pointers.

            // KIP-405 read-committed: surface the aborted-transaction list
            // from the segment's `.txnindex` so the consumer drops aborted
            // records client-side, mirroring the local `aborted_in_range`
            // call in `do_read` — bounded here to the single batch this read
            // returns (inclusive last offset), since the local path bounds by
            // the returned window over the LSO. `Some(empty)` is the correct
            // read-committed signal (read-uncommitted leaves it `None`).
            if p.read_committed && !p.is_follower_fetch {
                let batch_last_offset = batch.base_offset + i64::from(batch.last_offset_delta);
                let aborts = match reader
                    .aborted_transactions(&tp, leader_epoch, p.fetch_offset, batch_last_offset)
                    .await
                {
                    Ok(aborts) => aborts,
                    Err(e) => {
                        // Degrade to "no aborts" but make it observable: an
                        // empty list in read-committed means the consumer may
                        // surface aborted records as committed.
                        tracing::warn!(
                            topic = %p.topic_name,
                            partition = p.partition_index,
                            offset = p.fetch_offset,
                            error = %e,
                            "remote-reader: aborted_transactions failed; returning empty abort list"
                        );
                        Vec::new()
                    }
                };
                p.out.aborted_transactions = Some(
                    aborts
                        .into_iter()
                        .map(|e| AbortedTransaction {
                            producer_id: e.producer_id,
                            first_offset: e.start_offset,
                            ..Default::default()
                        })
                        .collect(),
                );
            }

            p.out.records = Some(batch.into());
            Some(bytes_est)
        }
        Ok(None) => None,
        Err(crabka_remote_storage::RemoteStorageError::NotReady { partition }) => {
            // The metadata partition that would answer this read is assigned
            // to this broker but its consumer has not caught up yet. Leave
            // OFFSET_OUT_OF_RANGE (retryable) — NOT a definitive miss — so the
            // client retries. Expected churn during catch-up, so log at debug.
            tracing::debug!(
                topic = %p.topic_name,
                partition = p.partition_index,
                offset = p.fetch_offset,
                metadata_partition = partition,
                "remote-reader: metadata partition not yet caught up; \
                 leaving OFFSET_OUT_OF_RANGE for client retry"
            );
            None
        }
        Err(e) => {
            tracing::warn!(
                topic = %p.topic_name,
                partition = p.partition_index,
                offset = p.fetch_offset,
                error = %e,
                "remote-reader: fetch_batch failed; leaving OFFSET_OUT_OF_RANGE"
            );
            None
        }
    }
}

/// Wait for any readable partition's `append_notify` to fire (with timeout),
/// then re-read every partition once. Resets each partition's accumulated
/// records before re-reading so the new read replaces the old one.
async fn long_poll_then_reread(
    broker: &Broker,
    pending: &mut [PendingRead],
    max_wait_ms: i32,
) -> Result<(), BrokerError> {
    let mut notifies: Vec<Arc<Notify>> = Vec::new();
    for p in pending.iter() {
        if let Some(part) = p.partition.as_ref() {
            notifies.push(part.append_notify.clone());
            // KIP-392: a consumer reading from a follower becomes unblocked
            // when the follower's HW advances (via set_follower_hw), not only
            // on raw append. Follower (inter-broker) fetches don't need this.
            if !p.is_follower_fetch {
                notifies.push(part.hw_advance_notify.clone());
            }
        }
    }
    if notifies.is_empty() {
        return Ok(());
    }
    // `Notify::notified()` returns a non-Send `Notified<'_>` that borrows
    // from its `Arc<Notify>`. Move the Arc into an `async move` block so
    // the future owns its Arc and is `'static + Send` (see `WaitFut` type
    // alias above).
    let waits: Vec<WaitFut> = notifies
        .into_iter()
        .map(|n| Box::pin(async move { n.notified().await }) as WaitFut)
        .collect();
    let max_wait = Duration::from_millis(u64::from(u32::try_from(max_wait_ms).unwrap_or(0)));
    let _ = tokio::time::timeout(max_wait, futures_util::future::select_all(waits)).await;

    for p in pending.iter_mut() {
        let Some(part) = p.partition.clone() else {
            continue;
        };
        p.out = PartitionData {
            partition_index: p.partition_index,
            ..Default::default()
        };
        // Time the re-read so its duration accumulates into the same
        // per-partition CPU counter as the first pass (wall-clock delta;
        // see the first-pass comment for why this replaces TaskMonitor).
        let read_start = std::time::Instant::now();
        do_read(
            &part,
            p.fetch_offset,
            p.max_bytes,
            p.read_committed,
            p.is_follower_fetch,
            &mut p.out,
        )
        .await?;
        let micros = u64::try_from(read_start.elapsed().as_micros()).unwrap_or(u64::MAX);
        p.cpu_micros = p.cpu_micros.saturating_add(micros);

        // Re-attempt the remote-tier read on the re-read pass
        // so a long-poll that fires on a non-tiered partition doesn't
        // clobber the remote batch we'd already served on this one.
        if p.out.error_code == codes::OFFSET_OUT_OF_RANGE {
            let _ = try_remote_read(broker, p, &part).await;
        }
    }
    Ok(())
}

/// KIP-73 leader-side throttle: walk `throttled_idxs` in order and drop
/// whole-partition chunks until the remaining throttled bytes fit within
/// `budget`. Partitions are dropped completely (records set to `None`) — no
/// mid-batch truncation, since Kafka clients expect complete record batches.
fn truncate_throttled_responses(
    responses: &mut [FetchableTopicResponse],
    throttled_idxs: &[(usize, usize)],
    budget: u64,
) {
    let mut remaining = budget;
    for &(ti, pi) in throttled_idxs {
        let part = &mut responses[ti].partitions[pi];
        let chunk_size = part.records.as_ref().map_or(0, RecordsPayload::payload_len) as u64;
        if chunk_size <= remaining {
            remaining -= chunk_size;
        } else {
            // Budget exhausted — drop this chunk and all subsequent throttled ones.
            part.records = None;
            remaining = 0;
        }
    }
}

/// Sum the encoded byte sizes of all record batches across all topic partitions
/// in the assembled Fetch response. Used by the KIP-13 `consumer_byte_rate` hook.
fn sum_response_bytes(responses: &[FetchableTopicResponse]) -> u64 {
    responses
        .iter()
        .flat_map(|t| t.partitions.iter())
        .map(|p| p.records.as_ref().map_or(0, RecordsPayload::payload_len) as u64)
        .sum()
}

/// KIP-13 `consumer_byte_rate` enforcement. Looks up the matching quota for
/// `(principal, client_id)`, consumes `bytes` from the bucket, and returns
/// the throttle delay capped at 1 second. Returns `Duration::ZERO` when no
/// quota is configured or the bucket has sufficient capacity.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
fn consume_consumer_quota(
    image: &crabka_metadata::MetadataImage,
    buckets: &crate::quota::QuotaBuckets,
    principal: &str,
    client_id: &str,
    bytes: u64,
) -> Duration {
    let Some((entity_key, rate)) =
        crate::quota::lookup_quota_with_key(image, principal, client_id, "consumer_byte_rate")
    else {
        return Duration::ZERO;
    };
    if rate <= 0.0 {
        return Duration::ZERO;
    }
    let bucket = buckets.get_or_create("consumer_byte_rate", &entity_key, rate as u64);
    let granted = bucket.try_consume(bytes);
    if granted >= bytes {
        return Duration::ZERO;
    }
    let overage = bytes - granted;
    let delay_secs = overage as f64 / rate;
    Duration::from_micros((delay_secs * 1_000_000.0) as u64).min(Duration::from_secs(1))
}

/// Group resolved `PendingRead`s back into per-topic response entries,
/// preserving the order topics first appeared in the request. Returns the
/// per-topic `cpu_micros` accumulators alongside, positionally aligned with
/// the returned `Vec` (`cpu_micros[ti][pi]` matches `responses[ti].partitions[pi]`)
/// so the caller can attribute CPU without re-keying by topic name.
type GroupedResponses = (Vec<FetchableTopicResponse>, Vec<Vec<u64>>);

fn group_into_topic_responses(pending: Vec<PendingRead>) -> GroupedResponses {
    let mut topic_order: Vec<String> = Vec::new();
    // Value: (topic_id, partitions, cpu_micros) — the trailing Vec mirrors
    // `partitions` positionally.
    let mut by_topic: std::collections::HashMap<String, (WireUuid, Vec<PartitionData>, Vec<u64>)> =
        std::collections::HashMap::new();
    for p in pending {
        let entry = by_topic
            .entry(p.topic_name.clone())
            .or_insert_with(|| (p.topic_id, Vec::new(), Vec::new()));
        entry.1.push(p.out);
        entry.2.push(p.cpu_micros);
        if !topic_order.iter().any(|t| t == &p.topic_name) {
            topic_order.push(p.topic_name);
        }
    }
    let mut responses = Vec::with_capacity(topic_order.len());
    let mut cpu_micros = Vec::with_capacity(topic_order.len());
    for name in topic_order {
        let (topic_id, parts, micros) = by_topic.remove(&name).expect("topic order populated");
        responses.push(FetchableTopicResponse {
            topic: name,
            topic_id,
            partitions: parts,
            ..Default::default()
        });
        cpu_micros.push(micros);
    }
    (responses, cpu_micros)
}

/// Encode a `FetchResponse` into a `BytesMut`, choosing the legacy
/// `kafka_3_6_2` codec for Fetch v0-3 and the current canonical codec
/// for v4+. The version boundary mirrors the request-decode boundary.
fn encode_fetch_response(
    resp: FetchResponse,
    version: i16,
) -> Result<BytesMut, crate::error::BrokerError> {
    if version < 4 {
        let legacy: crabka_protocol::kafka_3_6_2::owned::fetch_response::FetchResponse =
            resp.into();
        let mut buf = BytesMut::with_capacity(legacy.encoded_len(version));
        legacy.encode(&mut buf, version)?;
        Ok(buf)
    } else {
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    #[test]
    fn consume_consumer_quota_tuple_match_overage_throttles() {
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
            config_key: "consumer_byte_rate".into(),
            config_value: Some(1024.0),
        }));
        let buckets = crate::quota::QuotaBuckets::new();
        let delay_match = super::consume_consumer_quota(&img, &buckets, "alice", "app-x", 4096);
        assert!(
            delay_match > std::time::Duration::ZERO,
            "tuple quota match should throttle on overage; got {delay_match:?}"
        );
        let buckets2 = crate::quota::QuotaBuckets::new();
        let delay_other = super::consume_consumer_quota(&img, &buckets2, "alice", "other", 4096);
        assert!(
            delay_other == std::time::Duration::ZERO,
            "non-matching client_id should not throttle; got {delay_other:?}"
        );
    }
}
