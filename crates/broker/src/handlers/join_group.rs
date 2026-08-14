//! `JoinGroup` (`api_key=11`). This handler routes the request into the
//! group's unified actor as a `ClassicJoin` message, and waits for the reply.
//!
//! The actor parks the reply until the rebalance boundary, which is its
//! rebalance-deadline timer, an all-members-joined early completion, or a
//! membership change. The connection therefore blocks here for exactly as long
//! as the earlier `Notify`-based wait.

use bytes::Bytes;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode,
    owned::{
        join_group_request::JoinGroupRequest,
        join_group_response::{JoinGroupResponse, JoinGroupResponseMember},
    },
};
use tokio::sync::oneshot;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    coordinator::unified::actor::{GroupActorMessage, GroupKindTag},
    error::BrokerError,
    time_util::now_ms,
};

#[tracing::instrument(
    name = "handle_join_group",
    level = "info",
    skip_all,
    fields(api = "JoinGroup", version, req_bytes = req_bytes.len()),
    err,
)]
// cargo-mutants: coordinator-backed response projection; integration-tested.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = JoinGroupRequest::decode(&mut cur, version)?;

    // ── ACL preamble ────────────────────────────────────────────
    // `Read` on `Group(group_id)`. On Deny → whole-response
    // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
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
            return encode(
                version,
                &JoinGroupResponse {
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    ..Default::default()
                },
            );
        }
    }

    if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &req.group_id) {
        return encode(
            version,
            &JoinGroupResponse {
                error_code,
                ..Default::default()
            },
        );
    }

    // Route to the one actor for this id, spawning a classic-kind actor if the
    // id is brand-new. Both RPC families reach the same actor; if a next-gen
    // consumer actor already owns the id, the actor's `ClassicJoin` arm replies
    // `INCONSISTENT_GROUP_PROTOCOL` — that is where the per-group kind lock now
    // lives.
    //
    // Mark the group as Classic so that a later StreamsGroupHeartbeat for the
    // same id can detect it as a classic group and either convert or reject it
    // (KIP-1071 cold upgrade). First-mark-wins: a prior `mark_next_gen` (or any
    // other type lock) from a consumer-protocol group is not overridden.
    // KIP-1071 cold downgrade: a classic JoinGroup for a drained streams group
    // converts it in place to a classic group; a streams group with live members
    // is rejected (online streams migration is unsupported). Non-streams group
    // ids pass through unchanged.
    match broker
        .group_coordinator
        .try_convert_streams_to_classic(&req.group_id, now_ms())
        .await
    {
        Ok(
            crate::coordinator::unified::streams::migration::DowngradeOutcome::RejectLiveMembers,
        ) => {
            return encode(
                version,
                &JoinGroupResponse {
                    error_code: codes::GROUP_ID_NOT_FOUND,
                    ..Default::default()
                },
            );
        }
        Ok(_) => {} // NotStreams | Converted → serve the classic JoinGroup below
        Err(e) => return Err(e),
    }

    broker.group_coordinator.mark_classic(&req.group_id);
    let handle = broker
        .group_coordinator
        .get_or_create_group(&req.group_id, GroupKindTag::Classic);

    let (tx, rx) = oneshot::channel();
    if handle
        .tx
        .send(GroupActorMessage::ClassicJoin {
            req,
            version,
            client_id: ctx.client_id.to_owned(),
            client_host: ctx.client_host(),
            reply: tx,
        })
        .await
        .is_err()
    {
        return encode(
            version,
            &JoinGroupResponse {
                error_code: codes::REBALANCE_IN_PROGRESS,
                ..Default::default()
            },
        );
    }
    let Ok(result) = rx.await else {
        return encode(
            version,
            &JoinGroupResponse {
                error_code: codes::REBALANCE_IN_PROGRESS,
                ..Default::default()
            },
        );
    };

    let resp = JoinGroupResponse {
        error_code: result.error_code,
        generation_id: result.generation_id,
        protocol_type: result.protocol_type,
        protocol_name: result.protocol_name,
        leader: result.leader,
        member_id: result.member_id,
        members: result
            .members
            .into_iter()
            .map(|m| JoinGroupResponseMember {
                member_id: m.member_id,
                group_instance_id: m.group_instance_id,
                metadata: m.metadata,
                ..Default::default()
            })
            .collect(),
        throttle_time_ms: 0,
        ..Default::default()
    };
    encode(version, &resp)
}

fn encode(version: i16, resp: &JoinGroupResponse) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(resp, version)
}
