//! `ShareFetch` (`api_key` 78) — KIP-932.
//!
//! Drives the per-`(group, topic, partition)` [`AcquisitionState`] machine
//! owned by [`crate::share_partition::manager::SharePartitionLeaderManager`]:
//! validate the share session, check membership, then for every requested
//! partition this broker leads — apply any piggybacked acknowledgements, expire
//! stale locks, materialize newly produced records up to the high watermark,
//! acquire a batch of `Available` records under a lock, and read the acquired
//! offset range's verbatim bytes from the log. If nothing was acquired and the
//! client asked to wait, long-poll on the partitions' append/HW-advance
//! notifies and retry the acquire pass once.
//!
//! Intercepted inline in `network::dispatch` (not the `&Broker`-only handler
//! table) so the handler receives the per-connection principal + peer
//! `SocketAddr` for the per-topic `Read` ACL gate.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use crabka_log::Offset;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode,
    owned::{
        share_fetch_request::{FetchPartition, ShareFetchRequest},
        share_fetch_response::{
            AcquiredRecords, LeaderIdAndEpoch, PartitionData, ShareFetchResponse,
            ShareFetchableTopicResponse,
        },
    },
    records::RecordsPayload,
};
use crabka_units::{ByteSize, convert::ByteSizeExt as _};
use tokio::sync::Notify;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    coordinator::unified::share::actor::ShareGroupActorMessage,
    error::BrokerError,
    share_partition::state::AckType,
};

type WaitFut = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// One piggybacked acknowledgement batch:
/// `(first_offset, last_offset, per-offset acknowledge_types)`.
type AckBatch = (i64, i64, Vec<i8>);

/// One resolved `(topic, partition)` request row, carried through the acquire
/// pass(es) so the response can be assembled once at the end.
struct PendingPartition {
    topic_id: uuid::Uuid,
    topic_name: Option<String>,
    partition_index: i32,
    partition_max_bytes: i32,
    /// `Some` only when this broker leads the partition and the topic was not
    /// ACL-denied — i.e. when an acquire pass should run. `None` rows already
    /// have their `out` fully populated (error rows).
    leadable: bool,
    /// Acknowledgement batches piggybacked on this fetch (applied before the
    /// acquire pass).
    ack_batches: Vec<AckBatch>,
    out: PartitionData,
}

#[tracing::instrument(
    name = "handle_share_fetch",
    level = "info",
    skip_all,
    fields(api = "ShareFetch", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = ShareFetchRequest::decode(&mut cur, version)?;

    let cfg = broker.config.share_group.clone();
    let lock_timeout_ms = acquisition_timeout_ms(&cfg);

    let (group, member) = match validate_request(broker, &req, &cfg) {
        Ok(identity) => identity,
        Err(code) => return encode_error_response(version, code, lock_timeout_ms),
    };

    // Best-effort membership check: if the group has a live share actor, the
    // member must be present in its describe view. When no actor exists yet
    // (e.g. the group was never joined) we are lenient and skip the check —
    // the Task-7 tests always join via `ShareGroupHeartbeat` first, so a
    // present actor with an absent member is the only hard failure.
    if !member_is_valid(broker, &group, &member).await {
        return encode_error_response(version, codes::UNKNOWN_MEMBER_ID, lock_timeout_ms);
    }

    let mgr = broker.share_partition_leaders.clone();
    let image = broker.controller.current_image();

    // Resolve every requested partition into a PendingPartition: ACL gate,
    // leadership check, and the piggybacked ack batches.
    let mut pending: Vec<PendingPartition> = Vec::new();
    for topic in &req.topics {
        let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
        let topic_name = mgr.topic_name_for(topic_id);

        // Per-topic `Read` ACL — mirrors `fetch::handle`'s authorize call.
        let denied = match topic_name.as_deref() {
            Some(name) => {
                broker.config.authorizer.authorize(
                    &*image,
                    &AuthorizationRequest {
                        principal: ctx.principal,
                        host: ctx.peer,
                        resource_type: ResourceType::Topic,
                        resource_name: name,
                        operation: AclOperation::Read,
                    },
                ) == AuthorizationResult::Deny
            }
            // Unknown topic_id: no name to key the ACL by; treated as denied so
            // we never serve data for an unresolvable topic.
            None => true,
        };

        for fp in &topic.partitions {
            let mut out = PartitionData {
                partition_index: fp.partition_index,
                ..Default::default()
            };
            let ack_batches = collect_ack_batches(fp);

            if denied {
                out.error_code = if topic_name.is_some() {
                    codes::TOPIC_AUTHORIZATION_FAILED
                } else {
                    codes::UNKNOWN_TOPIC_OR_PARTITION
                };
                pending.push(PendingPartition {
                    topic_id,
                    topic_name: topic_name.clone(),
                    partition_index: fp.partition_index,
                    partition_max_bytes: fp.partition_max_bytes,
                    leadable: false,
                    ack_batches,
                    out,
                });
                continue;
            }

            if !mgr.topic_leader_is_self(topic_id, fp.partition_index) {
                let (leader_id, leader_epoch) = mgr.current_leader_of(topic_id, fp.partition_index);
                out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
                out.current_leader = LeaderIdAndEpoch {
                    leader_id,
                    leader_epoch,
                    ..Default::default()
                };
                pending.push(PendingPartition {
                    topic_id,
                    topic_name: topic_name.clone(),
                    partition_index: fp.partition_index,
                    partition_max_bytes: fp.partition_max_bytes,
                    leadable: false,
                    ack_batches,
                    out,
                });
                continue;
            }

            pending.push(PendingPartition {
                topic_id,
                topic_name: topic_name.clone(),
                partition_index: fp.partition_index,
                partition_max_bytes: fp.partition_max_bytes,
                leadable: true,
                ack_batches,
                out,
            });
        }
    }

    let acquire = AcquireContext {
        broker,
        manager: &mgr,
        group: &group,
        member: &member,
        max_records: req.max_records,
        max_bytes: req.max_bytes,
        is_renew_ack: req.is_renew_ack,
        config: &cfg,
    };

    acquire_records(&acquire, &mut pending, req.max_wait_ms).await?;

    // Group pending rows back into per-topic responses, preserving first-seen
    // topic order.
    let responses = group_responses(pending);

    encode_success_response(version, lock_timeout_ms, responses)
}

fn encode_success_response(
    version: i16,
    lock_timeout_ms: i32,
    responses: Vec<ShareFetchableTopicResponse>,
) -> Result<Bytes, BrokerError> {
    let response = ShareFetchResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        error_message: None,
        acquisition_lock_timeout_ms: lock_timeout_ms,
        responses,
        ..Default::default()
    };
    crate::handlers::encode_response(&response, version)
}

async fn acquire_records(
    context: &AcquireContext<'_>,
    pending: &mut [PendingPartition],
    max_wait_ms: i32,
) -> Result<(), BrokerError> {
    let acquired = acquire_pass(context, pending, true).await?;
    if acquired == 0 && max_wait_ms > 0 {
        long_poll(context.broker, pending, max_wait_ms).await;
        acquire_pass(context, pending, false).await?;
    }
    Ok(())
}

fn validate_request(
    broker: &Broker,
    request: &ShareFetchRequest,
    config: &crate::coordinator::unified::share::config::ShareGroupConfig,
) -> Result<(String, String), i16> {
    if !config.enable {
        return Err(codes::UNSUPPORTED_VERSION);
    }
    let group = request.group_id.clone().unwrap_or_default();
    let member = request.member_id.clone().unwrap_or_default();
    broker.share_partition_leaders.validate_session(
        &group,
        &member,
        request.share_session_epoch,
    )?;
    Ok((group, member))
}

fn acquisition_timeout_ms(
    config: &crate::coordinator::unified::share::config::ShareGroupConfig,
) -> i32 {
    i32::try_from(config.record_lock_duration.as_millis()).unwrap_or(i32::MAX)
}

async fn member_is_valid(broker: &Broker, group: &str, member: &str) -> bool {
    let Some(handle) = broker.group_coordinator.find_share(group) else {
        return true;
    };
    let (tx, rx) = tokio::sync::oneshot::channel();
    if handle
        .tx
        .send(ShareGroupActorMessage::Describe { reply: tx })
        .await
        .is_err()
    {
        return true;
    }
    match rx.await {
        Ok(view) => view
            .members
            .iter()
            .any(|candidate| candidate.member_id == member),
        Err(_) => true,
    }
}

/// Collect the piggybacked acknowledgement batches off a request partition into
/// `(first, last, acknowledge_types)` triples.
fn collect_ack_batches(fp: &FetchPartition) -> Vec<AckBatch> {
    fp.acknowledgement_batches
        .iter()
        .map(|b| (b.first_offset, b.last_offset, b.acknowledge_types.clone()))
        .collect()
}

/// Run one acquire pass over the leadable pending partitions. When
/// `apply_acks` is true, the piggybacked acknowledgement batches are applied
/// first (setting `acknowledge_error_code`). When `is_renew_ack` is set, those
/// batches RENEW the acquisition lock instead of acknowledging (KIP-932). Under
/// a `ReadCommitted` isolation level the materialize/read window is clamped to
/// the partition's last stable offset so uncommitted records are never
/// acquired. Returns the total number of offsets acquired across all partitions
/// in this pass.
#[derive(Clone, Copy)]
struct AcquireContext<'a> {
    broker: &'a Broker,
    manager: &'a Arc<crate::share_partition::manager::SharePartitionLeaderManager>,
    group: &'a str,
    member: &'a str,
    max_records: i32,
    max_bytes: i32,
    is_renew_ack: bool,
    config: &'a crate::coordinator::unified::share::config::ShareGroupConfig,
}

async fn acquire_pass(
    context: &AcquireContext<'_>,
    pending: &mut [PendingPartition],
    apply_acks: bool,
) -> Result<i64, BrokerError> {
    let &AcquireContext {
        broker,
        manager: mgr,
        group,
        member,
        max_records,
        max_bytes,
        is_renew_ack,
        config: cfg,
    } = context;
    let now = Instant::now();
    let read_committed = matches!(
        cfg.isolation_level,
        crate::coordinator::unified::share::config::ShareIsolationLevel::ReadCommitted
    );
    let mut total = 0_i64;

    for p in pending.iter_mut() {
        if !p.leadable {
            continue;
        }
        // Reset any prior pass's data for a clean re-acquire.
        p.out.records = None;
        p.out.acquired_records.clear();

        let cell = mgr.get_or_load(group, p.topic_id, p.partition_index).await;
        let mut st = cell.lock().await;

        // Apply piggybacked acknowledgements (first pass only). When the
        // request is a renew-ack, each batch RENEWs the lock on its range
        // rather than acknowledging it.
        if apply_acks && !p.ack_batches.is_empty() {
            let mut ack_err = codes::NONE;
            for (first, last, types) in &p.ack_batches {
                let res = if is_renew_ack {
                    st.renew(
                        member,
                        Offset(*first),
                        Offset(*last),
                        now,
                        cfg.record_lock_duration,
                    )
                } else {
                    apply_one_ack(&mut st, member, *first, *last, types, now)
                };
                if let Err(code) = res {
                    ack_err = code;
                }
            }
            p.out.acknowledge_error_code = ack_err;
        }

        // Expire stale locks, materialize freshly produced records, acquire.
        st.expire_locks(now);
        let part = p.topic_name.as_deref().and_then(|name| {
            broker
                .partitions
                .get(name, crabka_ids::PartitionIndex(p.partition_index))
        });
        let Some(part) = part else {
            // Lost the partition between the leadership check and here.
            p.out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
            p.leadable = false;
            mgr.persist_if_dirty(group, p.topic_id, p.partition_index, &mut st)
                .await;
            continue;
        };
        let hwm = part.high_watermark().await;
        // Under read_committed, never surface records past the last stable
        // offset: clamp the materialize/read window to `min(lso, hwm)` so no
        // record from an OPEN transaction can be acquired.
        let upper = if read_committed {
            part.lso().min(hwm)
        } else {
            hwm
        };
        // Archive aborted (committed-range) records. The LSO
        // clamp above already guarantees no OPEN-transaction records are
        // surfaced; aborted-but-stable records still get acquired because
        // `AbortedTxn` carries only `producer_id + start_offset`, not the
        // aborted region's end offset, so precise per-offset archival needs
        // the control-batch markers.
        st.materialize(upper, cfg.max_inflight_records);
        let acquired = st.acquire(
            member,
            max_records,
            max_bytes,
            now,
            cfg.record_lock_duration,
            cfg.max_delivery_attempts,
        );

        if !acquired.is_empty() {
            total += populate_acquired_response(p, &part, &acquired, upper, max_bytes).await?;
        }

        p.out.error_code = codes::NONE;
        mgr.persist_if_dirty(group, p.topic_id, p.partition_index, &mut st)
            .await;
    }
    Ok(total)
}

async fn populate_acquired_response(
    pending: &mut PendingPartition,
    partition: &Arc<crate::partition::Partition>,
    acquired: &[crate::share_partition::state::AcquiredRange],
    upper: Offset,
    request_max_bytes: i32,
) -> Result<i64, BrokerError> {
    // The per-partition cap is absent at supported protocol versions and
    // decodes to zero, so fall back to the request-wide byte budget.
    let read_budget = if pending.partition_max_bytes > 0 {
        pending.partition_max_bytes
    } else {
        request_max_bytes
    };
    let mut blob = BytesMut::new();
    for range in acquired {
        let limit = (range.last + 1).min(upper);
        if let Some(bytes) = read_acquired_bytes(partition, range.first, limit, read_budget).await?
        {
            blob.extend_from_slice(&bytes);
        }
    }
    if !blob.is_empty() {
        pending.out.records = Some(RecordsPayload::Raw(blob.freeze()));
    }
    pending.out.acquired_records = acquired
        .iter()
        .map(|range| AcquiredRecords {
            first_offset: range.first.0,
            last_offset: range.last.0,
            delivery_count: range.delivery_count,
            ..Default::default()
        })
        .collect();
    Ok(acquired
        .iter()
        .map(|range| range.last.0 - range.first.0 + 1)
        .sum())
}

/// Apply a single acknowledgement batch to the state machine. Each
/// `acknowledge_type` entry maps to one offset starting at `first`; runs of the
/// same type are coalesced into one `acknowledge` call. An empty
/// `acknowledge_types` falls back to applying `Accept` across `[first, last]`
/// (KIP-932's per-batch shorthand). Returns the first error code encountered.
pub(crate) fn apply_one_ack(
    st: &mut crate::share_partition::state::AcquisitionState,
    member: &str,
    first: i64,
    last: i64,
    types: &[i8],
    now: Instant,
) -> Result<(), i16> {
    if types.is_empty() {
        let ack = AckType::Accept;
        return st.acknowledge(member, Offset(first), Offset(last), ack, now);
    }
    // Walk the per-offset type list, coalescing equal-typed runs.
    let mut result = Ok(());
    let mut run_start = first;
    let mut idx = 0_usize;
    while idx < types.len() {
        let t = types[idx];
        let mut run_end = run_start;
        let mut j = idx + 1;
        while j < types.len() && types[j] == t {
            run_end += 1;
            j += 1;
        }
        if let Some(ack) = AckType::from_i8(t) {
            if let Err(code) = st.acknowledge(member, Offset(run_start), Offset(run_end), ack, now)
            {
                result = Err(code);
            }
        } else {
            result = Err(codes::INVALID_RECORD_STATE);
        }
        run_start = run_end + 1;
        idx = j;
    }
    result
}

/// Read the verbatim on-disk batch bytes for `[fetch_offset, limit_offset)`
/// via `Log::read_raw`, off the reactor thread. Returns `None` when nothing was
/// read.
async fn read_acquired_bytes(
    part: &crate::partition::Partition,
    fetch_offset: Offset,
    limit_offset: Offset,
    max_bytes: i32,
) -> Result<Option<Bytes>, BrokerError> {
    if limit_offset <= fetch_offset {
        return Ok(None);
    }
    let read_max = ByteSize::from_bytes_i64(i64::from(max_bytes.max(0)));
    let log = part.log.clone();
    let join = tokio::task::spawn_blocking(move || {
        let log = log.lock().expect("log mutex poisoned");
        log.read_raw(fetch_offset, limit_offset, read_max)
    });
    let raw = match join.await {
        Ok(res) => res?,
        Err(join_err) => {
            return Err(BrokerError::Io(std::io::Error::other(format!(
                "share-fetch read task panicked: {join_err}"
            ))));
        }
    };
    if raw.total > 0 {
        Ok(Some(raw.bytes))
    } else {
        Ok(None)
    }
}

/// Park on the leadable partitions' append + HW-advance notifies with a single
/// timeout. Mirrors `fetch::long_poll_then_reread`'s wait construction.
async fn long_poll(broker: &Broker, pending: &[PendingPartition], max_wait_ms: i32) {
    let mut notifies: Vec<Arc<Notify>> = Vec::new();
    for p in pending {
        if !p.leadable {
            continue;
        }
        if let Some(part) = p.topic_name.as_deref().and_then(|name| {
            broker
                .partitions
                .get(name, crabka_ids::PartitionIndex(p.partition_index))
        }) {
            notifies.push(part.append_notify.clone());
            notifies.push(part.hw_advance_notify.clone());
        }
    }
    if notifies.is_empty() {
        return;
    }
    let waits: Vec<WaitFut> = notifies
        .into_iter()
        .map(|n| Box::pin(async move { n.notified().await }) as WaitFut)
        .collect();
    let max_wait = Duration::from_millis(u64::from(u32::try_from(max_wait_ms).unwrap_or(0)));
    let _ = tokio::time::timeout(max_wait, futures_util::future::select_all(waits)).await;
}

/// Group the resolved pending partitions back into per-topic response entries,
/// preserving the order topics first appeared in the request.
fn group_responses(pending: Vec<PendingPartition>) -> Vec<ShareFetchableTopicResponse> {
    let mut order: Vec<uuid::Uuid> = Vec::new();
    let mut by_topic: std::collections::HashMap<uuid::Uuid, Vec<PartitionData>> =
        std::collections::HashMap::new();
    for p in pending {
        if !by_topic.contains_key(&p.topic_id) {
            order.push(p.topic_id);
        }
        by_topic.entry(p.topic_id).or_default().push(p.out);
    }
    order
        .into_iter()
        .map(|tid| ShareFetchableTopicResponse {
            topic_id: crabka_protocol::primitives::uuid::Uuid(*tid.as_bytes()),
            partitions: by_topic.remove(&tid).unwrap_or_default(),
            ..Default::default()
        })
        .collect()
}

/// Encode a top-level-error `ShareFetchResponse` (feature-gate, session, or
/// membership failure) with no per-partition rows.
fn encode_error_response(
    version: i16,
    error_code: i16,
    lock_timeout_ms: i32,
) -> Result<Bytes, BrokerError> {
    let resp = ShareFetchResponse {
        throttle_time_ms: 0,
        error_code,
        error_message: None,
        acquisition_lock_timeout_ms: lock_timeout_ms,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::{share_fetch_request::AcknowledgementBatch, share_fetch_response},
        primitives::uuid::Uuid as ProtoUuid,
    };

    use super::*;

    fn decode_response(bytes: &Bytes) -> ShareFetchResponse {
        crate::test_support::decode_response(bytes, share_fetch_response::MAX_VERSION)
    }

    #[test]
    fn encode_error_response_preserves_top_level_fields() {
        let resp = encode_error_response(
            share_fetch_response::MAX_VERSION,
            codes::UNSUPPORTED_VERSION,
            12_345,
        )
        .expect("encode");
        let resp = decode_response(&resp);

        let expected = ShareFetchResponse {
            throttle_time_ms: 0,
            error_code: codes::UNSUPPORTED_VERSION,
            error_message: None,
            acquisition_lock_timeout_ms: 12_345,
            responses: Vec::new(),
            node_endpoints: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[test]
    fn collect_ack_batches_preserves_offsets_and_ack_types() {
        let partition = FetchPartition {
            partition_index: 6,
            acknowledgement_batches: vec![
                AcknowledgementBatch {
                    first_offset: 10,
                    last_offset: 12,
                    acknowledge_types: vec![0, 1, 1],
                    ..Default::default()
                },
                AcknowledgementBatch {
                    first_offset: 30,
                    last_offset: 30,
                    acknowledge_types: Vec::new(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let batches = collect_ack_batches(&partition);

        assert!(batches == vec![(10, 12, vec![0, 1, 1]), (30, 30, Vec::new())]);
    }

    #[test]
    fn group_responses_preserves_topic_order_and_partition_fields() {
        let first_topic = uuid::Uuid::from_u128(0xA1);
        let second_topic = uuid::Uuid::from_u128(0xB2);
        let pending = vec![
            PendingPartition {
                topic_id: first_topic,
                topic_name: Some("first".into()),
                partition_index: 0,
                partition_max_bytes: 0,
                leadable: false,
                ack_batches: Vec::new(),
                out: PartitionData {
                    partition_index: 0,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    acknowledge_error_code: codes::NONE,
                    current_leader: LeaderIdAndEpoch {
                        leader_id: -1,
                        leader_epoch: -1,
                        ..Default::default()
                    },
                    acquired_records: vec![AcquiredRecords {
                        first_offset: 4,
                        last_offset: 7,
                        delivery_count: 2,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            },
            PendingPartition {
                topic_id: second_topic,
                topic_name: Some("second".into()),
                partition_index: 3,
                partition_max_bytes: 0,
                leadable: false,
                ack_batches: Vec::new(),
                out: PartitionData {
                    partition_index: 3,
                    error_code: codes::NOT_LEADER_OR_FOLLOWER,
                    current_leader: LeaderIdAndEpoch {
                        leader_id: 2,
                        leader_epoch: 9,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            PendingPartition {
                topic_id: first_topic,
                topic_name: Some("first".into()),
                partition_index: 1,
                partition_max_bytes: 0,
                leadable: false,
                ack_batches: Vec::new(),
                out: PartitionData {
                    partition_index: 1,
                    error_code: codes::NONE,
                    ..Default::default()
                },
            },
        ];

        let responses = group_responses(pending);

        let expected = vec![
            ShareFetchableTopicResponse {
                topic_id: ProtoUuid(*first_topic.as_bytes()),
                partitions: vec![
                    PartitionData {
                        partition_index: 0,
                        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                        error_message: None,
                        acknowledge_error_code: codes::NONE,
                        acknowledge_error_message: None,
                        current_leader: LeaderIdAndEpoch {
                            leader_id: -1,
                            leader_epoch: -1,
                            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                        },
                        records: None,
                        acquired_records: vec![AcquiredRecords {
                            first_offset: 4,
                            last_offset: 7,
                            delivery_count: 2,
                            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                        }],
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    },
                    PartitionData {
                        partition_index: 1,
                        error_code: codes::NONE,
                        error_message: None,
                        acknowledge_error_code: codes::NONE,
                        acknowledge_error_message: None,
                        current_leader: LeaderIdAndEpoch {
                            leader_id: 0,
                            leader_epoch: 0,
                            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                        },
                        records: None,
                        acquired_records: Vec::new(),
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    },
                ],
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            },
            ShareFetchableTopicResponse {
                topic_id: ProtoUuid(*second_topic.as_bytes()),
                partitions: vec![PartitionData {
                    partition_index: 3,
                    error_code: codes::NOT_LEADER_OR_FOLLOWER,
                    error_message: None,
                    acknowledge_error_code: codes::NONE,
                    acknowledge_error_message: None,
                    current_leader: LeaderIdAndEpoch {
                        leader_id: 2,
                        leader_epoch: 9,
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    },
                    records: None,
                    acquired_records: Vec::new(),
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                }],
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            },
        ];
        assert!(responses == expected);
    }
}
