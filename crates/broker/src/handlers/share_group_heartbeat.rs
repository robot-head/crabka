//! `ShareGroupHeartbeat` (`api_key` 76) — KIP-932 share-group membership.
//! Routes the request to the per-group share actor in `GroupCoordinator`.

use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot;

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::share_group_heartbeat_request::ShareGroupHeartbeatRequest;
use crabka_protocol::owned::share_group_heartbeat_response::ShareGroupHeartbeatResponse;
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::share::actor::ShareGroupActorMessage;
use crate::error::BrokerError;

#[tracing::instrument(
    name = "handle_share_group_heartbeat",
    level = "info",
    skip_all,
    fields(api = "ShareGroupHeartbeat", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let share_enabled = broker.config.share_group.enable;
    let ng = broker.group_coordinator.clone();
    {
        let mut cur: &[u8] = req_bytes;
        let req = ShareGroupHeartbeatRequest::decode(&mut cur, version)?;

        // ── ACL preamble ────────────────────────────────────────────
        // KIP-932 share groups still gate membership on `Read` on
        // `Group(group_id)`. On Deny → whole-response
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
                return encode(version, &error(codes::GROUP_AUTHORIZATION_FAILED));
            }
        }

        if !share_enabled {
            return encode(version, &error(codes::UNSUPPORTED_VERSION));
        }

        ng.mark_share(&req.group_id);
        let handle = ng.get_or_create_share(&req.group_id);
        let (tx, rx) = oneshot::channel();
        if handle
            .tx
            .send(ShareGroupActorMessage::Heartbeat {
                request: req,
                client_host: String::new(),
                reply: tx,
            })
            .await
            .is_err()
        {
            return encode(version, &error(codes::COORDINATOR_LOAD_IN_PROGRESS));
        }
        let resp = rx
            .await
            .unwrap_or_else(|_| error(codes::UNKNOWN_SERVER_ERROR));
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

/// Response returned when share groups are disabled on this broker.
fn disabled_response() -> ShareGroupHeartbeatResponse {
    error(codes::UNSUPPORTED_VERSION)
}

fn error(code: i16) -> ShareGroupHeartbeatResponse {
    ShareGroupHeartbeatResponse {
        error_code: code,
        ..Default::default()
    }
}

fn encode(version: i16, resp: &ShareGroupHeartbeatResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn disabled_feature_yields_unsupported_version() {
        let resp = disabled_response();
        assert!(resp.error_code == codes::UNSUPPORTED_VERSION);
    }

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        use crabka_protocol::owned::share_group_heartbeat_response::{
            self, ShareGroupHeartbeatResponse,
        };

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

        let bytes = encode(
            share_group_heartbeat_response::MAX_VERSION,
            &error(codes::GROUP_AUTHORIZATION_FAILED),
        )
        .expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = ShareGroupHeartbeatResponse::decode(
            &mut cur,
            share_group_heartbeat_response::MAX_VERSION,
        )
        .unwrap();
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
    }
}
