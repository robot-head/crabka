//! `JoinGroup` (`api_key=11`). Routes the request into the group's unified
//! actor as a `ClassicJoin` message and awaits the reply. The actor parks the
//! reply until the rebalance boundary (its rebalance-deadline timer, an
//! all-members-joined early-complete, or a membership change), so the
//! connection blocks here exactly as long as the old `Notify`-based wait.

use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot;

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::join_group_request::JoinGroupRequest;
use crabka_protocol::owned::join_group_response::{JoinGroupResponse, JoinGroupResponseMember};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::actor::GroupActorMessage;
use crate::error::BrokerError;

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
        if broker.config.authorizer.authorize(&image, &acl_req) == AuthorizationResult::Deny {
            return encode(
                version,
                &JoinGroupResponse {
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    ..Default::default()
                },
            );
        }
    }

    // The group's actor is the per-group type lock: a classic actor is created
    // here; if a next-gen consumer actor already owns the id, reject (the group
    // is not a classic group), mirroring the old `group_types` separation.
    let Some(handle) = broker
        .group_coordinator
        .get_or_create_classic(&req.group_id)
    else {
        return encode(
            version,
            &JoinGroupResponse {
                error_code: codes::INCONSISTENT_GROUP_PROTOCOL,
                member_id: req.member_id,
                ..Default::default()
            },
        );
    };

    let (tx, rx) = oneshot::channel();
    if handle
        .tx
        .send(GroupActorMessage::ClassicJoin {
            req,
            client_host: String::new(),
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
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
