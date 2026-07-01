//! `Heartbeat` (`api_key=12`). Validates `(generation, member)` and
//! refreshes the member's `last_heartbeat` clock inside the group's actor.

use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot;

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::heartbeat_request::HeartbeatRequest;
use crabka_protocol::owned::heartbeat_response::HeartbeatResponse;
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::actor::GroupActorMessage;
use crate::error::BrokerError;

#[tracing::instrument(
    name = "handle_heartbeat",
    level = "info",
    skip_all,
    fields(api = "Heartbeat", version, req_bytes = req_bytes.len()),
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
        let req = HeartbeatRequest::decode(&mut cur, version)?;

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
                return encode_denied(version);
            }
        }

        let error_code = match coordinator.find(&req.group_id) {
            None => codes::UNKNOWN_MEMBER_ID,
            Some(handle) => {
                let (tx, rx) = oneshot::channel();
                if handle
                    .tx
                    .send(GroupActorMessage::ClassicHeartbeat { req, reply: tx })
                    .await
                    .is_err()
                {
                    codes::UNKNOWN_MEMBER_ID
                } else {
                    rx.await.unwrap_or(codes::UNKNOWN_MEMBER_ID)
                }
            }
        };

        let resp = HeartbeatResponse {
            error_code,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
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

/// Whole-response `GROUP_AUTHORIZATION_FAILED (30)` response built on Deny.
fn encode_denied(version: i16) -> Result<Bytes, BrokerError> {
    let resp = HeartbeatResponse {
        error_code: codes::GROUP_AUTHORIZATION_FAILED,
        throttle_time_ms: 0,
        ..Default::default()
    };
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
        use crabka_protocol::owned::heartbeat_response::{self, HeartbeatResponse};

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

        let bytes = encode_denied(heartbeat_response::MAX_VERSION).expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = HeartbeatResponse::decode(&mut cur, heartbeat_response::MAX_VERSION).unwrap();
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
    }
}
