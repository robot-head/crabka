//! `LeaveGroup` (`api_key=13`). Removes one or more members inside the group's
//! actor and (if the group is still `Stable` with survivors) reopens a
//! rebalance.

use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot;

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::leave_group_request::LeaveGroupRequest;
use crabka_protocol::owned::leave_group_response::LeaveGroupResponse;
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::actor::GroupActorMessage;
use crate::error::BrokerError;

#[tracing::instrument(
    name = "handle_leave_group",
    level = "info",
    skip_all,
    fields(api = "LeaveGroup", version, req_bytes = req_bytes.len()),
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
        let req = LeaveGroupRequest::decode(&mut cur, version)?;

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
                return encode(
                    version,
                    &LeaveGroupResponse {
                        error_code: codes::GROUP_AUTHORIZATION_FAILED,
                        throttle_time_ms: 0,
                        members: Vec::new(),
                        ..Default::default()
                    },
                );
            }
        }

        let members = match coordinator.find(&req.group_id) {
            // No such group; respond OK but no member responses.
            None => Vec::new(),
            Some(handle) => {
                let (tx, rx) = oneshot::channel();
                if handle
                    .tx
                    .send(GroupActorMessage::ClassicLeave {
                        req,
                        version,
                        reply: tx,
                    })
                    .await
                    .is_err()
                {
                    Vec::new()
                } else {
                    rx.await.unwrap_or_default()
                }
            }
        };

        let resp = LeaveGroupResponse {
            error_code: codes::NONE,
            throttle_time_ms: 0,
            members,
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

fn encode(version: i16, resp: &LeaveGroupResponse) -> Result<Bytes, BrokerError> {
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
        use crabka_protocol::owned::leave_group_response::{self, LeaveGroupResponse};

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

        let resp = LeaveGroupResponse {
            error_code: codes::GROUP_AUTHORIZATION_FAILED,
            throttle_time_ms: 0,
            members: Vec::new(),
            ..Default::default()
        };
        let bytes = encode(leave_group_response::MAX_VERSION, &resp).expect("encode");
        let mut cur: &[u8] = &bytes;
        let decoded =
            LeaveGroupResponse::decode(&mut cur, leave_group_response::MAX_VERSION).unwrap();
        assert!(decoded.error_code == codes::GROUP_AUTHORIZATION_FAILED);
    }
}
