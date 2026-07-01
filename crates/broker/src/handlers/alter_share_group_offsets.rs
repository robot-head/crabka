//! `AlterShareGroupOffsets` (`api_key` 91) — KIP-932. Resets the
//! share-partition start offset (SPSO) for the requested partitions of an
//! *empty* share group, bumping the state epoch and re-initializing the
//! persister state. A non-empty group is rejected top-level with
//! `NON_EMPTY_GROUP`.
//!
//! Intercepted inline in `network::dispatch` for the per-group `Alter` ACL
//! gate (principal + peer `SocketAddr`).

use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot;

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::alter_share_group_offsets_request::AlterShareGroupOffsetsRequest;
use crabka_protocol::owned::alter_share_group_offsets_response::{
    AlterShareGroupOffsetsResponse, AlterShareGroupOffsetsResponsePartition,
    AlterShareGroupOffsetsResponseTopic,
};
use crabka_protocol::primitives::uuid::Uuid;
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::share::actor::ShareGroupActorMessage;
use crate::error::BrokerError;

#[tracing::instrument(
    name = "handle_alter_share_group_offsets",
    level = "info",
    skip_all,
    fields(api = "AlterShareGroupOffsets", version, req_bytes = req_bytes.len()),
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
    let req = AlterShareGroupOffsetsRequest::decode(&mut cur, version)?;

    // Feature gate: a broker with share groups disabled does not implement the RPC.
    if !broker.config.share_group.enable {
        return encode_top_level(version, codes::UNSUPPORTED_VERSION);
    }

    let image = broker.controller.current_image();
    let ng_opt = Some(broker.group_coordinator.clone());
    let gid = req.group_id;

    // ── ACL preamble ────────────────────────────────────
    // Per-group `Alter` check. On Deny → top-level `error_code = 30`.
    let acl_req = AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type: ResourceType::Group,
        resource_name: gid.as_str(),
        operation: AclOperation::Alter,
    };
    if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
        return encode_top_level(version, codes::GROUP_AUTHORIZATION_FAILED);
    }

    let Some(persister) = ng_opt.as_ref().and_then(|ng| ng.share_persister().cloned()) else {
        return encode_top_level(version, codes::COORDINATOR_NOT_AVAILABLE);
    };

    // Empty-group check: only an empty group may have its offsets reset. An
    // absent actor is treated as empty.
    if !group_is_empty(ng_opt.as_ref(), &gid).await {
        return encode_top_level(version, codes::NON_EMPTY_GROUP);
    }

    let mut responses: Vec<AlterShareGroupOffsetsResponseTopic> =
        Vec::with_capacity(req.topics.len());

    for rt in req.topics {
        let topic_name = rt.topic_name;
        let topic_id = image.topic(&topic_name).map(|t| t.topic_id);

        let mut partitions: Vec<AlterShareGroupOffsetsResponsePartition> =
            Vec::with_capacity(rt.partitions.len());

        for rp in rt.partitions {
            let Some(topic_id) = topic_id else {
                partitions.push(AlterShareGroupOffsetsResponsePartition {
                    partition_index: rp.partition_index,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    ..Default::default()
                });
                continue;
            };

            // Bump the state epoch off the current durable value, then
            // re-initialize at the requested start offset. On success drop the
            // local acquisition-state cell so the next ShareFetch re-reads the
            // new SPSO.
            let error_code = match reset_partition(
                &persister,
                &gid,
                topic_id,
                rp.partition_index,
                rp.start_offset,
            )
            .await
            {
                Ok(()) => {
                    broker
                        .share_partition_leaders
                        .invalidate(&gid, topic_id, rp.partition_index);
                    codes::NONE
                }
                Err(()) => codes::COORDINATOR_NOT_AVAILABLE,
            };

            partitions.push(AlterShareGroupOffsetsResponsePartition {
                partition_index: rp.partition_index,
                error_code,
                ..Default::default()
            });
        }

        responses.push(AlterShareGroupOffsetsResponseTopic {
            topic_name,
            topic_id: topic_id.map_or_else(Uuid::default, |id| Uuid(*id.as_bytes())),
            partitions,
            ..Default::default()
        });
    }

    let resp = AlterShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        responses,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// Read the current durable state epoch for `(group, topic_id, partition)`,
/// then re-initialize the share state at `start_offset` with epoch+1. `Err(())`
/// on any persister failure (mapped to `COORDINATOR_NOT_AVAILABLE`).
async fn reset_partition(
    persister: &crate::share_coordinator::persister_client::SharePersister,
    gid: &str,
    topic_id: uuid::Uuid,
    partition: i32,
    start_offset: i64,
) -> Result<(), ()> {
    let cur_epoch = persister
        .read_state(gid, topic_id, partition)
        .await
        .map_err(|_| ())?
        .map_or(0, |s| s.state_epoch);
    persister
        .initialize(gid, topic_id, partition, cur_epoch + 1, start_offset)
        .await
        .map_err(|_| ())
}

fn encode_top_level(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    let resp = AlterShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        error_code,
        responses: Vec::new(),
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// Returns `true` when the share group has no live members (or no actor at
/// all). Drives the empty-group gate for offset reset/delete.
pub(crate) async fn group_is_empty(
    ng: Option<&std::sync::Arc<crate::coordinator::unified::GroupCoordinator>>,
    gid: &str,
) -> bool {
    let Some(handle) = ng.and_then(|ng| ng.find_share(gid)) else {
        return true;
    };
    let (tx, rx) = oneshot::channel();
    if handle
        .tx
        .send(ShareGroupActorMessage::Describe { reply: tx })
        .await
        .is_err()
    {
        // Actor gone → no live members.
        return true;
    }
    match rx.await {
        Ok(view) => view.members.is_empty(),
        Err(_) => true,
    }
}
