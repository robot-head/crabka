//! `Fetch` (`api_key=1`) with long-poll support via per-partition
//! `Notify::notified()` futures.
//!
//! MVP scope: returns at most the *first* `RecordBatch` covering the
//! requested offset for each partition. The generated
//! `PartitionData.records` field is `Option<RecordBatch>` (the codegen
//! models it as a single batch wrapped in nullable bytes), so emitting a
//! concatenated stream of batches would require bypassing the codegen.
//! Clients pulling small batches one at a time and re-fetching from
//! `last.base_offset + last.last_offset_delta + 1` see correct data.

use std::net::SocketAddr;
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
use crabka_protocol::records::RecordBatch;
use crabka_protocol::{Decode, Encode};
use crabka_security::Principal;

use crate::authorizer::{AuthorizationResult, authorize_topics};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
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
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    principal: &Principal,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let partitions = broker.partitions.clone();
    let controller = broker.controller.clone();
    let mut cur: &[u8] = req_bytes;
    let req = FetchRequest::decode(&mut cur, version)?;

    // ── slice-13 ACL preamble ────────────────────────────────────────
    // Batch-authorize every topic in the request for `Read` (the
    // operation Fetch requires). Topics that come back `Deny` will
    // short-circuit the per-partition log read below and emit
    // TOPIC_AUTHORIZATION_FAILED on every partition row of that topic
    // with empty records.
    //
    // Fetch v ≥ 13 sends only topic_id on the wire; the slice-13 plan
    // keys ACLs by topic *name*, so we resolve the names here too for
    // the authorize call (and re-resolve inline below for log lookup).
    let image = controller.current_image();
    let topic_names_for_acl: Vec<String> = req
        .topics
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
        &image,
        &broker.config.super_users,
        principal,
        peer,
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

    // Resolve every requested partition up front. We collect pending
    // reads (rather than just doing them inline) so we can re-read once
    // after a long-poll wake without re-decoding the request.
    let mut pending: Vec<PendingRead> = Vec::new();
    for topic in &req.topics {
        // v ≤ 12 sends the topic name; v ≥ 13 sends only topic_id and
        // we look it up. The client populates whichever the negotiated
        // version requires.
        let topic_name = if topic.topic.is_empty() {
            let image = controller.current_image();
            image
                .topics()
                .find(|t| t.topic_id.into_bytes() == topic.topic_id.0)
                .map_or_else(String::new, |t| t.name.clone())
        } else {
            topic.topic.clone()
        };
        let topic_id = if topic.topic_id == WireUuid::ZERO {
            let image = controller.current_image();
            image
                .topic(&topic_name)
                .map_or(WireUuid::ZERO, |t| WireUuid(t.topic_id.into_bytes()))
        } else {
            topic.topic_id
        };

        // slice-13: if the topic was denied by the ACL preamble,
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
                });
                continue;
            }

            let part_opt = partitions
                .get(&(topic_name.clone(), idx))
                .map(|p| p.clone());

            // KIP-101 epoch fence. The follower (or consumer using KIP-320)
            // includes its `current_leader_epoch`; we reject stale or future
            // epochs without serving data.
            if let Some(part) = part_opt.as_ref() {
                let our_epoch = part
                    .current_leader_epoch
                    .load(std::sync::atomic::Ordering::Acquire);
                if fp.current_leader_epoch >= 0 && fp.current_leader_epoch != our_epoch {
                    out.error_code = if fp.current_leader_epoch < our_epoch {
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
                    });
                    continue;
                }
            }

            // Restore follower-fetch HW maintenance (slice-10a removed this
            // because of stalls; slice-10b's ISR maintenance prevents stalls
            // by shrinking lagging followers out of the ISR within 2s on CI).
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
                });
                continue;
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
            });
        }
    }

    // First read pass.
    let mut total_bytes = 0_usize;
    for p in &mut pending {
        if let Some(part) = &p.partition {
            total_bytes += do_read(
                part,
                p.fetch_offset,
                p.max_bytes,
                p.read_committed,
                p.is_follower_fetch,
                &mut p.out,
            )
            .await?;
        }
    }

    // Long-poll: if we didn't satisfy min_bytes, wait on each readable
    // partition's append_notify with a single timeout, then re-read.
    let want_more = total_bytes < usize::try_from(req.min_bytes.max(0)).unwrap_or(0);
    if want_more && req.max_wait_ms > 0 {
        long_poll_then_reread(&mut pending, req.max_wait_ms).await?;
    }

    let mut responses = group_into_topic_responses(pending);

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
                        part.records.as_ref().map_or(0, RecordBatch::encoded_len) as u64;
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

    let resp = FetchResponse {
        throttle_time_ms: 0,
        error_code: 0,
        session_id: 0,
        responses,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// Hold the partition's log mutex briefly to read offsets + (optionally) a
/// batch. Populates `out` in place and returns the encoded-size estimate of
/// the records placed in `out` (0 if none).
///
/// When `read_committed` is `true` (consumer fetch with `isolation_level=1`):
/// - batches with `base_offset >= min(lso, hw)` are dropped
/// - control batches are hidden from consumers (Apache Kafka behavior)
/// - `out.last_stable_offset` is set to `min(lso, hw)`
/// - `out.aborted_transactions` is populated from the partition's `.txnindex`
///
/// When `is_follower_fetch` is `true`:
/// - all batches up to LEO are returned (no HW clamping)
/// - `out.high_watermark` and `out.last_stable_offset` are set to `log_end`
///
/// When `read_committed` is `false` and `is_follower_fetch` is `false`
/// (consumer fetch in `read_uncommitted`):
/// - batches are clamped at HW (`base_offset < hw`)
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
    let hw = part.high_watermark().await;
    let (log_start, log_end, lso, batch_opt, aborted_txns): (
        i64,
        i64,
        i64,
        Option<RecordBatch>,
        Vec<AbortedTransaction>,
    ) = {
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

        if fetch_offset < log_start {
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
            return Ok(0);
        }
        if fetch_offset >= upper_bound {
            (log_start, log_end, lso, None, Vec::new())
        } else {
            let read_max = usize::try_from(max_bytes.max(0)).unwrap_or(0);
            let read = log.read(fetch_offset, read_max)?;

            if read_committed && !is_follower_fetch {
                // Aborted-txn list for the window [fetch_offset, effective_lso).
                let aborted_raw = log.aborted_in_range(fetch_offset, effective_lso);
                let aborted_pids: std::collections::HashSet<(i64, i64, i64)> = aborted_raw
                    .iter()
                    .map(|e| (e.producer_id, e.start_offset, e.last_offset))
                    .collect();
                let aborted = aborted_raw
                    .into_iter()
                    .map(|e| AbortedTransaction {
                        producer_id: e.producer_id,
                        first_offset: e.start_offset,
                        ..Default::default()
                    })
                    .collect();

                let visible_batch = read
                    .batches
                    .into_iter()
                    .filter(|b| b.base_offset < effective_lso)
                    .filter(|b| !b.attributes.is_control_batch())
                    .find(|b| {
                        if !b.attributes.is_transactional() {
                            return true;
                        }
                        let pid = b.producer_id;
                        let batch_last = b.base_offset + i64::from(b.last_offset_delta);
                        !aborted_pids.iter().any(|&(apid, astart, alast)| {
                            apid == pid && b.base_offset >= astart && batch_last <= alast
                        })
                    });

                (log_start, log_end, lso, visible_batch, aborted)
            } else if !is_follower_fetch {
                // Consumer fetch in read_uncommitted: clamp at HW.
                let batch_opt = read.batches.into_iter().find(|b| b.base_offset < hw);
                (log_start, log_end, lso, batch_opt, Vec::new())
            } else {
                // Follower fetch: no clamping, no filtering.
                let batch_opt = read.batches.into_iter().next();
                (log_start, log_end, lso, batch_opt, Vec::new())
            }
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

    let bytes_est = batch_opt
        .as_ref()
        .map_or(0, |b| <RecordBatch as Encode>::encoded_len(b, 0));
    out.records = batch_opt;
    Ok(bytes_est)
}

/// Wait for any readable partition's `append_notify` to fire (with timeout),
/// then re-read every partition once. Resets each partition's accumulated
/// records before re-reading so the new read replaces the old one.
async fn long_poll_then_reread(
    pending: &mut [PendingRead],
    max_wait_ms: i32,
) -> Result<(), BrokerError> {
    let notifies: Vec<Arc<Notify>> = pending
        .iter()
        .filter_map(|p| p.partition.as_ref().map(|part| part.append_notify.clone()))
        .collect();
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
        if let Some(part) = &p.partition {
            p.out = PartitionData {
                partition_index: p.partition_index,
                ..Default::default()
            };
            do_read(
                part,
                p.fetch_offset,
                p.max_bytes,
                p.read_committed,
                p.is_follower_fetch,
                &mut p.out,
            )
            .await?;
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
        let chunk_size = part.records.as_ref().map_or(0, RecordBatch::encoded_len) as u64;
        if chunk_size <= remaining {
            remaining -= chunk_size;
        } else {
            // Budget exhausted — drop this chunk and all subsequent throttled ones.
            part.records = None;
            remaining = 0;
        }
    }
}

/// Group resolved `PendingRead`s back into per-topic response entries,
/// preserving the order topics first appeared in the request.
fn group_into_topic_responses(pending: Vec<PendingRead>) -> Vec<FetchableTopicResponse> {
    let mut topic_order: Vec<String> = Vec::new();
    let mut by_topic: std::collections::HashMap<String, (WireUuid, Vec<PartitionData>)> =
        std::collections::HashMap::new();
    for p in pending {
        let entry = by_topic
            .entry(p.topic_name.clone())
            .or_insert_with(|| (p.topic_id, Vec::new()));
        entry.1.push(p.out);
        if !topic_order.iter().any(|t| t == &p.topic_name) {
            topic_order.push(p.topic_name);
        }
    }
    topic_order
        .into_iter()
        .map(|name| {
            let (topic_id, parts) = by_topic.remove(&name).expect("topic order populated");
            FetchableTopicResponse {
                topic: name,
                topic_id,
                partitions: parts,
                ..Default::default()
            }
        })
        .collect()
}
