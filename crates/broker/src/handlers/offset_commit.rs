//! `OffsetCommit` (`api_key=8`). Encodes `OffsetCommitKey` +
//! `OffsetCommitValue` records, appends them to `__consumer_offsets-0`
//! via the partition writer, then updates the group's committed offsets
//! through its actor.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::offset_commit_request::OffsetCommitRequest;
use crabka_protocol::owned::offset_commit_response::{
    OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::records::{Record, RecordBatch};
use crabka_protocol::{Decode, Encode};
use tokio::sync::oneshot;

use crate::authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::bootstrap::{OFFSETS_PARTITION, OFFSETS_TOPIC};
use crate::coordinator::persistence::OffsetCommitValue;
use crate::coordinator::unified::actor::{
    GroupActorHandle, GroupActorMessage, GroupKindTag, validate_group_commit,
};
use crate::coordinator::unified::classic_state::OffsetEntry;
use crate::error::BrokerError;
use crate::partition::{ProduceData, ProduceJob, WriterMessage};
use crate::partition_registry::PartitionRegistry;

#[allow(clippy::too_many_lines)] // ACL preamble (group + per-topic) + commit pipeline; splitting hurts readability
#[tracing::instrument(
    name = "handle_offset_commit",
    level = "info",
    skip_all,
    fields(api = "OffsetCommit", version, req_bytes = req_bytes.len()),
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
    let mut req = OffsetCommitRequest::decode(&mut cur, version)?;

    // ── KIP-516 (v10+): topic_id → name normalization ───────────
    // At v10 the client sends `name` empty + `topic_id` set. The internal
    // commit pipeline (and the `__consumer_offsets` record key) is
    // name-keyed, so resolve id→name in place. Topics whose id is unknown
    // are split off: they get UNKNOWN_TOPIC_ID on every partition and are
    // not committed. `finalize` appends those rows to every response, and
    // the response echoes each topic's `topic_id`.
    let mut unknown_id_topics: Vec<OffsetCommitResponseTopic> = Vec::new();
    {
        let image = broker.controller.current_image();
        let mut resolved = Vec::with_capacity(req.topics.len());
        for mut topic in req.topics.drain(..) {
            if topic.name.is_empty() && topic.topic_id != WireUuid::ZERO {
                match image.topic_name_by_id(&uuid::Uuid::from_bytes(topic.topic_id.0)) {
                    Some(name) => {
                        topic.name = name.to_string();
                        resolved.push(topic);
                    }
                    None => {
                        unknown_id_topics.push(OffsetCommitResponseTopic {
                            name: String::new(),
                            topic_id: topic.topic_id,
                            partitions: topic
                                .partitions
                                .iter()
                                .map(|p| OffsetCommitResponsePartition {
                                    partition_index: p.partition_index,
                                    error_code: codes::UNKNOWN_TOPIC_ID,
                                    ..Default::default()
                                })
                                .collect(),
                            ..Default::default()
                        });
                    }
                }
            } else {
                resolved.push(topic);
            }
        }
        req.topics = resolved;
    }

    // ── ACL preamble ────────────────────────────────────────────
    // Step 1: `Read` on `Group(group_id)`. On Deny → whole-response
    // `error_code = GROUP_AUTHORIZATION_FAILED (30)` (with per-topic/
    // per-partition rows reflecting the error too).
    {
        let image = broker.controller.current_image();
        let acl_req = AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Group,
            resource_name: req.group_id.as_str(),
            operation: AclOperation::Read,
        };
        if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
            let resp = build_response_all(&req, codes::GROUP_AUTHORIZATION_FAILED);
            return finalize(version, resp, unknown_id_topics.clone());
        }
    }

    let now_ms = now_ms();
    // Find the group's actor (a classic actor is created for an unknown id —
    // e.g. a "simple" consumer committing offsets without joining a group).
    // Offsets are protocol-agnostic, so an existing actor of either kind serves
    // the commit the same way.
    let handle = broker
        .group_coordinator
        .find(&req.group_id)
        .unwrap_or_else(|| {
            broker
                .group_coordinator
                .get_or_create_group(&req.group_id, GroupKindTag::Classic)
        });

    // Validate membership/epoch through the actor (kind-specific).
    if let Some(code) = validate(&handle, &req).await {
        let resp = build_response_all(&req, code);
        return finalize(version, resp, unknown_id_topics.clone());
    }

    // ── ACL preamble ────────────────────────────────────────────
    // Step 2: `Read` on each `Topic(topic_name)`. On Deny → per-partition
    // `error_code = TOPIC_AUTHORIZATION_FAILED (29)` on the affected rows.
    let topic_decisions = {
        let image = broker.controller.current_image();
        let topic_names: Vec<&str> = req.topics.iter().map(|t| t.name.as_str()).collect();
        authorize_topics(
            broker.config.authorizer.as_ref(),
            &*image,
            ctx.principal,
            ctx.peer,
            AclOperation::Read,
            topic_names,
        )
    };

    // Check if all topics are allowed — if any are denied we need per-topic handling.
    let any_denied = topic_decisions
        .values()
        .any(|r| *r == AuthorizationResult::Deny);

    if any_denied {
        // Build a mixed response: denied topics get TOPIC_AUTHORIZATION_FAILED,
        // allowed topics proceed normally but we need to do the real work for them.
        let topics_out = req
            .topics
            .iter()
            .map(|t| {
                let denied = topic_decisions
                    .get(t.name.as_str())
                    .copied()
                    .unwrap_or(AuthorizationResult::Deny)
                    == AuthorizationResult::Deny;
                let code = if denied {
                    codes::TOPIC_AUTHORIZATION_FAILED
                } else {
                    codes::NONE
                };
                OffsetCommitResponseTopic {
                    name: t.name.clone(),
                    topic_id: t.topic_id,
                    partitions: t
                        .partitions
                        .iter()
                        .map(|p| OffsetCommitResponsePartition {
                            partition_index: p.partition_index,
                            error_code: code,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }
            })
            .collect();

        // Only proceed with allowed topics (append + update).
        let allowed_req = OffsetCommitRequest {
            topics: req
                .topics
                .iter()
                .filter(|t| {
                    topic_decisions
                        .get(t.name.as_str())
                        .copied()
                        .unwrap_or(AuthorizationResult::Deny)
                        == AuthorizationResult::Allow
                })
                .cloned()
                .collect(),
            ..req.clone()
        };
        if !allowed_req.topics.is_empty() {
            if let Err(code) = append_batch(&allowed_req, &broker.partitions, now_ms).await {
                // If append fails, overwrite allowed topics with the error code.
                let topics_out_err: Vec<OffsetCommitResponseTopic> = req
                    .topics
                    .iter()
                    .map(|t| {
                        let denied = topic_decisions
                            .get(t.name.as_str())
                            .copied()
                            .unwrap_or(AuthorizationResult::Deny)
                            == AuthorizationResult::Deny;
                        let final_code = if denied {
                            codes::TOPIC_AUTHORIZATION_FAILED
                        } else {
                            code
                        };
                        OffsetCommitResponseTopic {
                            name: t.name.clone(),
                            topic_id: t.topic_id,
                            partitions: t
                                .partitions
                                .iter()
                                .map(|p| OffsetCommitResponsePartition {
                                    partition_index: p.partition_index,
                                    error_code: final_code,
                                    ..Default::default()
                                })
                                .collect(),
                            ..Default::default()
                        }
                    })
                    .collect();
                let resp = OffsetCommitResponse {
                    topics: topics_out_err,
                    throttle_time_ms: 0,
                    ..Default::default()
                };
                return finalize(version, resp, unknown_id_topics.clone());
            }
            update_committed(&allowed_req, &handle, now_ms).await;
        }

        let resp = OffsetCommitResponse {
            topics: topics_out,
            throttle_time_ms: 0,
            ..Default::default()
        };
        return finalize(version, resp, unknown_id_topics.clone());
    }

    // 2. Append a RecordBatch into `__consumer_offsets-0`.
    if let Err(code) = append_batch(&req, &broker.partitions, now_ms).await {
        let resp = build_response_all(&req, code);
        return finalize(version, resp, unknown_id_topics.clone());
    }

    // 3. Update in-memory state.
    update_committed(&req, &handle, now_ms).await;

    // 4. Uniform per-(topic, partition) success.
    let resp = build_response_all(&req, codes::NONE);
    finalize(version, resp, unknown_id_topics)
}

/// Append the KIP-516 unknown-`topic_id` rows (if any) to the response and
/// encode it. Every return path in `handle` flows through here so unknown-id
/// topics surface `UNKNOWN_TOPIC_ID` even when the rest of the commit errors.
fn finalize(
    version: i16,
    mut resp: OffsetCommitResponse,
    unknown: Vec<OffsetCommitResponseTopic>,
) -> Result<Bytes, BrokerError> {
    resp.topics.extend(unknown);
    encode(version, &resp)
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(0)
}

/// Validate the commit against the group's membership/epoch through its actor.
/// Returns `Some(error_code)` if the request should be rejected. Thin wrapper
/// over the shared [`validate_group_commit`] (also used by `TxnOffsetCommit`),
/// which dispatches on the actor's LIVE `group.kind` — a KIP-848 migration may
/// have flipped the protocol in place after spawn, so validation must run
/// against the current protocol, not the spawn-time `handle.kind`.
async fn validate(handle: &Arc<GroupActorHandle>, req: &OffsetCommitRequest) -> Option<i16> {
    validate_group_commit(
        handle,
        &req.member_id,
        req.generation_id_or_member_epoch,
        req.group_instance_id.as_deref(),
    )
    .await
}

/// Append a single `RecordBatch` covering every (topic, partition) in `req`
/// to the `__consumer_offsets-0` writer. Returns `Err(error_code)` on
/// failure to either find the partition or hear back from the writer.
async fn append_batch(
    req: &OffsetCommitRequest,
    partitions: &Arc<PartitionRegistry>,
    now_ms: i64,
) -> Result<(), i16> {
    let mut batch = RecordBatch {
        max_timestamp: now_ms,
        ..RecordBatch::default()
    };
    let mut delta: i32 = 0;
    for topic in &req.topics {
        for part in &topic.partitions {
            let value = OffsetCommitValue {
                offset: part.committed_offset,
                leader_epoch: part.committed_leader_epoch,
                metadata: part.committed_metadata.clone().unwrap_or_default(),
                commit_timestamp_ms: now_ms,
            };
            batch.records.push(Record {
                offset_delta: delta,
                timestamp_delta: 0,
                key: Some(OffsetCommitValue::encode_key(
                    &req.group_id,
                    &topic.name,
                    part.partition_index,
                )),
                value: Some(value.encode_value()),
                ..Default::default()
            });
            delta += 1;
        }
    }
    batch.last_offset_delta = (delta - 1).max(0);

    let Some(part_handle) = partitions.get(OFFSETS_TOPIC, OFFSETS_PARTITION) else {
        return Err(codes::UNKNOWN_SERVER_ERROR);
    };
    let (ack_tx, ack_rx) = oneshot::channel();
    if part_handle
        .writer_tx
        .send(WriterMessage::Produce(ProduceJob {
            data: ProduceData::Owned(batch),
            ack: ack_tx,
        }))
        .await
        .is_err()
    {
        return Err(codes::UNKNOWN_SERVER_ERROR);
    }
    match ack_rx.await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "OffsetCommit writer returned error");
            Err(codes::from_broker_error(&e))
        }
        Err(e) => {
            tracing::error!(error = %e, "OffsetCommit writer ack dropped");
            Err(codes::UNKNOWN_SERVER_ERROR)
        }
    }
}

async fn update_committed(req: &OffsetCommitRequest, handle: &Arc<GroupActorHandle>, now_ms: i64) {
    let mut entries: Vec<((String, i32), OffsetEntry)> = Vec::new();
    for topic in &req.topics {
        for part in &topic.partitions {
            entries.push((
                (topic.name.clone(), part.partition_index),
                OffsetEntry {
                    offset: part.committed_offset,
                    leader_epoch: part.committed_leader_epoch,
                    metadata: part.committed_metadata.clone().unwrap_or_default(),
                    commit_timestamp_ms: now_ms,
                },
            ));
        }
    }
    let (tx, rx) = oneshot::channel();
    if handle
        .tx
        .send(GroupActorMessage::UpdateCommitted { entries, reply: tx })
        .await
        .is_ok()
    {
        let _ = rx.await;
    }
}

fn build_response_all(req: &OffsetCommitRequest, code: i16) -> OffsetCommitResponse {
    let topics = req
        .topics
        .iter()
        .map(|t| OffsetCommitResponseTopic {
            name: t.name.clone(),
            topic_id: t.topic_id,
            partitions: t
                .partitions
                .iter()
                .map(|p| OffsetCommitResponsePartition {
                    partition_index: p.partition_index,
                    error_code: code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    OffsetCommitResponse {
        topics,
        throttle_time_ms: 0,
        ..Default::default()
    }
}

fn encode(version: i16, resp: &OffsetCommitResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
