//! `OffsetCommit` (`api_key=8`). Encodes `OffsetCommitKey` +
//! `OffsetCommitValue` records, appends them to `__consumer_offsets-0`
//! via the partition writer, then updates `Group.committed_offsets`.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::offset_commit_request::OffsetCommitRequest;
use crabka_protocol::owned::offset_commit_response::{
    OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use crabka_protocol::records::{Record, RecordBatch};
use crabka_protocol::{Decode, Encode};
use dashmap::DashMap;
use tokio::sync::oneshot;

use crate::authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::GroupHandle;
use crate::coordinator::bootstrap::{OFFSETS_PARTITION, OFFSETS_TOPIC};
use crate::coordinator::group::OffsetEntry;
use crate::coordinator::persistence::OffsetCommitValue;
use crate::error::BrokerError;
use crate::partition::{Partition, ProduceJob, WriterMessage};

#[allow(clippy::too_many_lines)] // ACL preamble (group + per-topic) + commit pipeline; splitting hurts readability
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = OffsetCommitRequest::decode(&mut cur, version)?;

    // ── slice-13 ACL preamble ────────────────────────────────────────────
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
        if broker.config.authorizer.authorize(&image, &acl_req) == AuthorizationResult::Deny {
            let resp = build_response_all(&req, codes::GROUP_AUTHORIZATION_FAILED);
            return encode(version, &resp);
        }
    }

    let now_ms = now_ms();
    let group_handle = broker.group_manager.get_or_create(&req.group_id);

    // KIP-848: determine whether this group is managed by the next-gen coordinator.
    let is_next_gen = broker.group_manager.next_gen().is_some_and(|ng| {
        matches!(
            ng.group_type(&req.group_id),
            Some(crate::coordinator::next_gen::GroupType::NextGen)
        )
    });

    if is_next_gen {
        // Next-gen groups validate member_epoch via the per-group actor.
        let ng = broker
            .group_manager
            .next_gen()
            .expect("next_gen present: checked above");
        let Some(ng_handle) = ng.find(&req.group_id) else {
            let resp = build_response_all(&req, codes::GROUP_ID_NOT_FOUND);
            return encode(version, &resp);
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = ng_handle
            .tx
            .send(
                crate::coordinator::next_gen::group_actor::GroupActorMessage::OffsetValidate {
                    member_id: req.member_id.clone(),
                    member_epoch: req.generation_id_or_member_epoch,
                    reply: tx,
                },
            )
            .await;
        match rx.await {
            Ok(Ok(())) => {
                // Validation passed; proceed with topic ACL and persistence below.
                let _ = &group_handle; // group_handle unused for next-gen path
            }
            Ok(Err(code)) => {
                let resp = build_response_all(&req, code);
                return encode(version, &resp);
            }
            Err(_) => {
                let resp = build_response_all(&req, codes::UNKNOWN_SERVER_ERROR);
                return encode(version, &resp);
            }
        }
    } else {
        // 1. Validate (group, generation, member). Empty `member_id` means
        //    a "simple" consumer that doesn't participate in a group — skip
        //    the membership/generation check entirely.
        if let Some(code) = validate(&req, &group_handle).await {
            let resp = build_response_all(&req, code);
            return encode(version, &resp);
        }
    }

    // ── slice-13 ACL preamble ────────────────────────────────────────────
    // Step 2: `Read` on each `Topic(topic_name)`. On Deny → per-partition
    // `error_code = TOPIC_AUTHORIZATION_FAILED (29)` on the affected rows.
    let topic_decisions = {
        let image = broker.controller.current_image();
        let topic_names: Vec<&str> = req.topics.iter().map(|t| t.name.as_str()).collect();
        authorize_topics(
            broker.config.authorizer.as_ref(),
            &image,
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
        // For simplicity, split: if there are allowed topics, we run the full
        // pipeline for the filtered request; here we return per-topic error codes.
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
                return encode(version, &resp);
            }
            update_committed(&allowed_req, &group_handle, now_ms).await;
        }

        let resp = OffsetCommitResponse {
            topics: topics_out,
            throttle_time_ms: 0,
            ..Default::default()
        };
        return encode(version, &resp);
    }

    // 2. Append a RecordBatch into `__consumer_offsets-0`.
    if let Err(code) = append_batch(&req, &broker.partitions, now_ms).await {
        let resp = build_response_all(&req, code);
        return encode(version, &resp);
    }

    // 3. Update in-memory state.
    update_committed(&req, &group_handle, now_ms).await;

    // 4. Uniform per-(topic, partition) success.
    let resp = build_response_all(&req, codes::NONE);
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

/// Returns `Some(error_code)` if the request should be rejected.
async fn validate(req: &OffsetCommitRequest, handle: &Arc<GroupHandle>) -> Option<i16> {
    // KIP-345: a request that supplies a `group.instance.id` with an empty
    // `member_id` is a static-only commit — resolve via the static index.
    if req.member_id.is_empty() && req.group_instance_id.is_none() {
        // Simple consumer (no group membership) — no validation needed.
        return None;
    }
    let g = handle.state.lock().await;
    // KIP-345 fence: if instance id is set, it must resolve and (if
    // member_id is also set) match.
    if let Some(iid) = req.group_instance_id.as_deref() {
        match g.current_member_id_for_instance(iid) {
            None => return Some(codes::UNKNOWN_MEMBER_ID),
            Some(pinned) => {
                if !req.member_id.is_empty() && pinned != req.member_id {
                    return Some(codes::FENCED_INSTANCE_ID);
                }
            }
        }
    } else if !g.members.contains_key(&req.member_id) {
        return Some(codes::UNKNOWN_MEMBER_ID);
    }
    if g.generation_id != req.generation_id_or_member_epoch {
        return Some(codes::ILLEGAL_GENERATION);
    }
    None
}

/// Append a single `RecordBatch` covering every (topic, partition) in `req`
/// to the `__consumer_offsets-0` writer. Returns `Err(error_code)` on
/// failure to either find the partition or hear back from the writer.
async fn append_batch(
    req: &OffsetCommitRequest,
    partitions: &Arc<DashMap<(String, i32), Arc<Partition>>>,
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

    let Some(part_handle) = partitions
        .get(&(OFFSETS_TOPIC.to_string(), OFFSETS_PARTITION))
        .map(|e| e.value().clone())
    else {
        return Err(codes::UNKNOWN_SERVER_ERROR);
    };
    let (ack_tx, ack_rx) = oneshot::channel();
    if part_handle
        .writer_tx
        .send(WriterMessage::Produce(ProduceJob { batch, ack: ack_tx }))
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

async fn update_committed(req: &OffsetCommitRequest, handle: &Arc<GroupHandle>, now_ms: i64) {
    let mut g = handle.state.lock().await;
    for topic in &req.topics {
        for part in &topic.partitions {
            g.committed_offsets.insert(
                (topic.name.clone(), part.partition_index),
                OffsetEntry {
                    offset: part.committed_offset,
                    leader_epoch: part.committed_leader_epoch,
                    metadata: part.committed_metadata.clone().unwrap_or_default(),
                    commit_timestamp_ms: now_ms,
                },
            );
        }
    }
}

fn build_response_all(req: &OffsetCommitRequest, code: i16) -> OffsetCommitResponse {
    let topics = req
        .topics
        .iter()
        .map(|t| OffsetCommitResponseTopic {
            name: t.name.clone(),
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
