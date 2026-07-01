//! `SyncGroup` (`api_key=14`). Routes into the group's unified actor as a
//! `ClassicSync` message. The leader's call installs assignments and the actor
//! drains the parked followers; a follower with no assignment yet is parked
//! until then (capped here by `FOLLOWER_WAIT`).
//!
//! KIP-559 (v5+): the response carries `protocol_type` + `protocol_name`
//! so an L7 proxy can route the call without remembering the prior
//! `JoinGroup` exchange.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot;

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::sync_group_request::SyncGroupRequest;
use crabka_protocol::owned::sync_group_response::SyncGroupResponse;
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::actor::GroupActorMessage;
use crate::error::BrokerError;

const FOLLOWER_WAIT: Duration = Duration::from_secs(30);

#[tracing::instrument(
    name = "handle_sync_group",
    level = "info",
    skip_all,
    fields(api = "SyncGroup", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let coordinator = broker.group_coordinator.clone();
    {
        let mut cur: &[u8] = req_bytes;
        let req = SyncGroupRequest::decode(&mut cur, version)?;

        // ── ACL preamble ────────────────────────────────────────────
        // `Read` on `Group(group_id)`. On Deny → whole-response
        // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
        {
            let image = broker.controller.current_image();
            if group_read_denied(
                broker.config.authorizer.as_ref(),
                &image,
                ctx.principal,
                ctx.peer,
                &req.group_id,
            ) {
                return encode_err(version, codes::GROUP_AUTHORIZATION_FAILED, None, None);
            }
        }

        let Some(handle) = coordinator.find(&req.group_id) else {
            return encode_err(version, codes::UNKNOWN_MEMBER_ID, None, None);
        };

        let (tx, rx) = oneshot::channel();
        if handle
            .tx
            .send(GroupActorMessage::ClassicSync { req, reply: tx })
            .await
            .is_err()
        {
            return encode_err(version, codes::REBALANCE_IN_PROGRESS, None, None);
        }
        // The leader and the already-Stable follower reply immediately; a
        // not-yet-synced follower is parked and resolved when the leader's
        // SyncGroup installs assignments, bounded by FOLLOWER_WAIT.
        let Ok(Ok(result)) = tokio::time::timeout(FOLLOWER_WAIT, rx).await else {
            return encode_err(version, codes::REBALANCE_IN_PROGRESS, None, None);
        };

        let resp = SyncGroupResponse {
            error_code: result.error_code,
            assignment: result.assignment,
            throttle_time_ms: 0,
            protocol_type: result.protocol_type,
            protocol_name: result.protocol_name,
            ..Default::default()
        };
        encode(version, &resp)
    }
}

/// `Read` on `Group(group_id)` gate. Returns `true` when denied.
fn group_read_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &crabka_metadata::MetadataImage,
    principal: &crabka_security::Principal,
    host: &std::net::SocketAddr,
    group_id: &str,
) -> bool {
    authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal,
            host,
            resource_type: ResourceType::Group,
            resource_name: group_id,
            operation: AclOperation::Read,
        },
    ) == AuthorizationResult::Deny
}

fn encode_err(
    version: i16,
    code: i16,
    protocol_type: Option<String>,
    protocol_name: Option<String>,
) -> Result<Bytes, BrokerError> {
    let resp = SyncGroupResponse {
        error_code: code,
        assignment: Bytes::new(),
        throttle_time_ms: 0,
        protocol_type,
        protocol_name,
        ..Default::default()
    };
    encode(version, &resp)
}

fn encode(version: i16, resp: &SyncGroupResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        use crabka_protocol::owned::sync_group_response::{self, SyncGroupResponse};

        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let principal = crabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));

        assert!(group_read_denied(
            &authorizer,
            &image,
            &principal,
            &peer,
            "g"
        ));

        let bytes = encode_err(
            sync_group_response::MAX_VERSION,
            codes::GROUP_AUTHORIZATION_FAILED,
            None,
            None,
        )
        .expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = SyncGroupResponse::decode(&mut cur, sync_group_response::MAX_VERSION).unwrap();
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
    }
}
