//! `TxnOffsetCommit` (`api_key=28`). The consumer side of the
//! consume-process-produce pattern. A transactional producer that also
//! reads commits its consumed offsets atomically with its transaction by
//! appending them to `__consumer_offsets` with `is_transactional=true` +
//! the producer's (pid, epoch). The offsets are held under the partition's
//! LSO until a `WriteTxnMarkers` commit or abort marker arrives.
//!
//! Versions 0–2: non-flexible (no `generation_id`/`member_id` fields).
//! Versions 3–5: flexible (tagged fields; adds `generation_id`, `member_id`,
//!               `group_instance_id`).
//!
//! ## ACL preamble
//!
//! Three gates run in order:
//! * `Write` on `TransactionalId(transactional_id)`. Deny → whole-response
//!   `TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)`.
//! * `Read` on `Group(group_id)`. Deny → whole-response
//!   `GROUP_AUTHORIZATION_FAILED (30)`.
//! * Per-topic `Read` on `Topic(name)`. Deny → per-partition
//!   `TOPIC_AUTHORIZATION_FAILED (29)` on the rows of that topic.

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::txn_offset_commit_request::TxnOffsetCommitRequest;
use crabka_protocol::owned::txn_offset_commit_response::{
    TxnOffsetCommitResponse, TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
};
use crabka_protocol::records::{Attributes, Record, RecordBatch};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::bootstrap::{OFFSETS_PARTITION, OFFSETS_TOPIC};
use crate::coordinator::persistence::OffsetCommitValue;
use crate::coordinator::unified::actor::{GroupActorMessage, GroupKindTag};
use crate::error::BrokerError;
use crate::txn::util::now_millis;

pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let partitions = broker.partitions.clone();
    let mut cur: &[u8] = req_bytes;
    let req = TxnOffsetCommitRequest::decode(&mut cur, version)?;

    // ── ACL preamble: Write on TransactionalId ────────────────
    {
        let image = broker.controller.current_image();
        let authorizer = broker.config.authorizer.as_ref();
        let tid_req = AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::TransactionalId,
            resource_name: req.transactional_id.as_str(),
            operation: AclOperation::Write,
        };
        if authorizer.authorize(&image, &tid_req) == AuthorizationResult::Deny {
            return encode_err_all(version, &req, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
        }
        // Group Read gate.
        let group_req = AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Group,
            resource_name: req.group_id.as_str(),
            operation: AclOperation::Read,
        };
        if authorizer.authorize(&image, &group_req) == AuthorizationResult::Deny {
            return encode_err_all(version, &req, codes::GROUP_AUTHORIZATION_FAILED);
        }
    }

    // ── ACL preamble: per-topic Read ──────────────────────────
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
    let denied_topics: std::collections::HashSet<String> = topic_decisions
        .into_iter()
        .filter_map(|(name, r)| {
            if r == AuthorizationResult::Deny {
                Some(name.to_string())
            } else {
                None
            }
        })
        .collect();

    // 1. Verify the group coordinator is this broker.  In the current
    //    single-broker MVP every group is local. We check that the group
    //    exists (or create it) — if the partition for __consumer_offsets
    //    is not present we'll detect that below and return NOT_COORDINATOR.
    //    For a multi-broker future this would route to the leader for
    //    hash(group_id) % __consumer_offsets.partition_count.
    let handle = broker
        .group_coordinator
        .find(&req.group_id)
        .unwrap_or_else(|| {
            broker
                .group_coordinator
                .get_or_create_group(&req.group_id, GroupKindTag::Classic)
        });

    // 2. KIP-1319 stale-member-epoch check (api_version >= 3 adds
    //    generation_id/member_id). The coordinator does not yet expose a
    //    `member_epoch()` accessor, so we emit ILLEGAL_GENERATION only when the
    //    request carries a non-default generation_id that differs from the
    //    classic group's current generation_id.
    //    TODO(KIP-1319 v4+): implement per-member epoch tracking and
    //    surface STALE_MEMBER_EPOCH (113) when supplied epoch < current.
    // `ClassicInspect` dispatches on the actor's LIVE `group.kind`: it replies
    // ONLY for a classic-kind group, so the `&& let Ok(view) = rx.await` guard
    // skips the generation check for a consumer-kind group (including an
    // UPGRADED one) without consulting the stale spawn-time `handle.kind`. A
    // KIP-848 migration can flip the group's protocol in place after spawn.
    if version >= 3 && req.generation_id >= 0 {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if handle
            .tx
            .send(GroupActorMessage::ClassicInspect { reply: tx })
            .await
            .is_ok()
            && let Ok(view) = rx.await
        {
            // KIP-345 fence: instance id (if present) must resolve to the
            // request's member id when both are set.
            if let Some(iid) = req.group_instance_id.as_deref() {
                match view
                    .members
                    .iter()
                    .find(|m| m.group_instance_id.as_deref() == Some(iid))
                {
                    None => return encode_err_all(version, &req, codes::UNKNOWN_MEMBER_ID),
                    Some(m) => {
                        if !req.member_id.is_empty() && m.member_id != req.member_id {
                            return encode_err_all(version, &req, codes::FENCED_INSTANCE_ID);
                        }
                    }
                }
            }
            if view.generation_id >= 0 && req.generation_id != view.generation_id {
                return encode_err_all(version, &req, codes::ILLEGAL_GENERATION);
            }
        }
    }

    // 3. Append a transactional RecordBatch to __consumer_offsets.
    //    We reuse the OffsetCommitKey/Value layout but stamp the batch with
    //    is_transactional=true + (producer_id, producer_epoch) so the log's
    //    LSO machinery holds the offsets until EndTxn commits/aborts.
    //    Topics denied by the per-topic Read ACL are skipped from the
    //    batch and surfaced as TOPIC_AUTHORIZATION_FAILED in the response.
    let now_ms = now_millis();
    if let Err(code) = append_txn_batch(&req, &partitions, now_ms, &denied_topics).await {
        return encode_resp(version, &build_response(&req, code, &denied_topics));
    }

    // 4. Success — per-(topic, partition) error_code = NONE for allowed,
    //    TOPIC_AUTHORIZATION_FAILED for denied.
    encode_resp(version, &build_response(&req, codes::NONE, &denied_topics))
}

// ── batch construction ────────────────────────────────────────────────────────

async fn append_txn_batch(
    req: &TxnOffsetCommitRequest,
    partitions: &std::sync::Arc<crate::partition_registry::PartitionRegistry>,
    now_ms: i64,
    denied_topics: &std::collections::HashSet<String>,
) -> Result<(), i16> {
    let mut batch = RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        max_timestamp: now_ms,
        producer_id: req.producer_id,
        producer_epoch: req.producer_epoch,
        ..RecordBatch::default()
    };
    let mut delta: i32 = 0;
    for topic in &req.topics {
        if denied_topics.contains(&topic.name) {
            continue;
        }
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

    // If every topic was denied, there's nothing to append; succeed silently.
    if batch.records.is_empty() {
        return Ok(());
    }

    batch.last_offset_delta = (delta - 1).max(0);

    let Some(part_handle) = partitions.get(OFFSETS_TOPIC, OFFSETS_PARTITION) else {
        // __consumer_offsets not hosted here — report NOT_COORDINATOR.
        return Err(codes::NOT_COORDINATOR);
    };
    // `produce_batch` drives the single-writer task and returns the
    // assigned base_offset; we don't need it here.
    part_handle
        .produce_batch(batch)
        .await
        .map(|_| ())
        .map_err(|e| {
            tracing::error!(
                group = %req.group_id,
                tid   = %req.transactional_id,
                error = %e,
                "TxnOffsetCommit: produce_batch failed"
            );
            codes::UNKNOWN_SERVER_ERROR
        })
}

// ── response helpers ──────────────────────────────────────────────────────────

fn build_response(
    req: &TxnOffsetCommitRequest,
    code: i16,
    denied_topics: &std::collections::HashSet<String>,
) -> TxnOffsetCommitResponse {
    let topics = req
        .topics
        .iter()
        .map(|t| {
            let row_code = if denied_topics.contains(&t.name) {
                codes::TOPIC_AUTHORIZATION_FAILED
            } else {
                code
            };
            TxnOffsetCommitResponseTopic {
                name: t.name.clone(),
                partitions: t
                    .partitions
                    .iter()
                    .map(|p| TxnOffsetCommitResponsePartition {
                        partition_index: p.partition_index,
                        error_code: row_code,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }
        })
        .collect();
    TxnOffsetCommitResponse {
        throttle_time_ms: 0,
        topics,
        ..Default::default()
    }
}

fn encode_resp(version: i16, resp: &TxnOffsetCommitResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

fn encode_err_all(
    version: i16,
    req: &TxnOffsetCommitRequest,
    code: i16,
) -> Result<Bytes, BrokerError> {
    let empty: std::collections::HashSet<String> = std::collections::HashSet::new();
    encode_resp(version, &build_response(req, code, &empty))
}
