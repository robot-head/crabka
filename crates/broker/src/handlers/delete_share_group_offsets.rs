//! `DeleteShareGroupOffsets` (`api_key` 92) — KIP-932. Deletes the durable
//! share-state for every initialized partition of the requested topics of an
//! *empty* share group. A non-empty group is rejected top-level with
//! `NON_EMPTY_GROUP`.
//!
//! The request carries only `topic_name` per topic (no partition list), so the
//! handler enumerates the group's initialized partitions for each topic from
//! the cached `ShareGroupStatePartitionMetadata`.
//!
//! Intercepted inline in `network::dispatch` for the per-group `Delete` ACL
//! gate (principal + peer `SocketAddr`).

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::delete_share_group_offsets_request::DeleteShareGroupOffsetsRequest;
use crabka_protocol::owned::delete_share_group_offsets_response::{
    DeleteShareGroupOffsetsResponse, DeleteShareGroupOffsetsResponseTopic,
};
use crabka_protocol::primitives::uuid::Uuid;
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::handlers::alter_share_group_offsets::group_is_empty;

#[tracing::instrument(
    name = "handle_delete_share_group_offsets",
    level = "info",
    skip_all,
    fields(api = "DeleteShareGroupOffsets", version, req_bytes = req_bytes.len()),
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
    let req = DeleteShareGroupOffsetsRequest::decode(&mut cur, version)?;

    // Feature gate: a broker with share groups disabled does not implement the RPC.
    if !broker.config.share_group.enable {
        return encode_top_level(version, codes::UNSUPPORTED_VERSION);
    }

    let image = broker.controller.current_image();
    let ng_opt = Some(broker.group_coordinator.clone());
    let gid = req.group_id;

    // ── ACL preamble ────────────────────────────────────
    // Per-group `Delete` check. On Deny → top-level `error_code = 30`.
    let acl_req = AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type: ResourceType::Group,
        resource_name: gid.as_str(),
        operation: AclOperation::Delete,
    };
    if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
        return encode_top_level(version, codes::GROUP_AUTHORIZATION_FAILED);
    }

    let Some(persister) = ng_opt.as_ref().and_then(|ng| ng.share_persister().cloned()) else {
        return encode_top_level(version, codes::COORDINATOR_NOT_AVAILABLE);
    };

    // Empty-group check: only an empty group may have its offsets deleted. An
    // absent actor is treated as empty.
    if !group_is_empty(ng_opt.as_ref(), &gid).await {
        return encode_top_level(version, codes::NON_EMPTY_GROUP);
    }

    let metadata = ng_opt
        .as_ref()
        .and_then(|ng| ng.share_state_partition_metadata(&gid));

    let mut responses: Vec<DeleteShareGroupOffsetsResponseTopic> =
        Vec::with_capacity(req.topics.len());

    for rt in req.topics {
        let topic_name = rt.topic_name;

        let Some(topic_id) = image.topic(&topic_name).map(|t| t.topic_id) else {
            responses.push(DeleteShareGroupOffsetsResponseTopic {
                topic_name,
                topic_id: Uuid::default(),
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                ..Default::default()
            });
            continue;
        };

        // Enumerate the group's initialized partitions for this topic.
        let part_indices: Vec<i32> = metadata
            .as_ref()
            .and_then(|m| {
                m.initialized
                    .iter()
                    .find(|(tid, _)| *tid == topic_id)
                    .map(|(_, parts)| parts.clone())
            })
            .unwrap_or_default();

        let mut error_code = codes::NONE;
        for p in part_indices {
            match persister.delete(&gid, topic_id, p).await {
                Ok(()) => {
                    broker.share_partition_leaders.invalidate(&gid, topic_id, p);
                }
                Err(_) => error_code = codes::COORDINATOR_NOT_AVAILABLE,
            }
        }

        // KIP-932 lifecycle: drop the topic from the group's v14
        // `ShareGroupStatePartitionMetadata` so it stays absent across restart.
        // Best-effort: a send/await failure must not fail the delete.
        if error_code == codes::NONE
            && let Some(ng) = ng_opt.as_ref()
            && let Some(handle) = ng.find_share(&gid)
        {
            let (tx, rx) = tokio::sync::oneshot::channel();
            handle
                .tx
                .send(
                    crate::coordinator::unified::share::actor::ShareGroupActorMessage::DropTopicMetadata {
                        topic_id,
                        reply: tx,
                    },
                )
                .await
                .ok();
            let _ = rx.await;
        }

        responses.push(DeleteShareGroupOffsetsResponseTopic {
            topic_name,
            topic_id: Uuid(*topic_id.as_bytes()),
            error_code,
            ..Default::default()
        });
    }

    let resp = DeleteShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        responses,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

fn encode_top_level(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    let resp = DeleteShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        error_code,
        responses: Vec::new(),
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
