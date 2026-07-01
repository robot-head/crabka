//! `ConsumerGroupHeartbeat` (`api_key` 68) — KIP-848 next-gen consumer
//! group protocol. Routes the request to the per-group actor in
//! `GroupCoordinator`.

use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot;

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use crabka_protocol::owned::consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse;
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::actor::{GroupActorMessage, GroupKindTag};
use crate::error::BrokerError;

#[tracing::instrument(
    name = "handle_consumer_group_heartbeat",
    level = "info",
    skip_all,
    fields(api = "ConsumerGroupHeartbeat", version, req_bytes = req_bytes.len()),
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
    let image = broker.controller.current_image();
    {
        let mut cur: &[u8] = req_bytes;
        let req = ConsumerGroupHeartbeatRequest::decode(&mut cur, version)?;

        // ── ACL preamble ────────────────────────────────────────────
        // `Read` on `Group(group_id)`. On Deny → whole-response
        // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
        if group_read_denied(
            broker.config.authorizer.as_ref(),
            &image,
            ctx.principal,
            ctx.peer,
            &req.group_id,
        ) {
            return encode(version, &error(codes::GROUP_AUTHORIZATION_FAILED));
        }

        // KIP-848 / KIP-584: the next-gen protocol is gated on a finalized
        // group.version >= 1. Below that — including UNFINALIZED, which means
        // disabled — reject so the client falls back to the classic protocol.
        if !crate::features::feature_enabled(
            &image,
            crabka_metadata::group_version::GROUP_VERSION_FEATURE,
            1,
        ) {
            return encode(version, &error(codes::UNSUPPORTED_VERSION));
        }

        if !coordinator.config.next_gen_enabled() {
            return encode(version, &error(codes::GROUP_ID_NOT_FOUND));
        }

        // Route to the one actor for this id, spawning a consumer-kind actor if
        // the id is brand-new. Both RPC families reach the same actor; a classic
        // group rejects a next-gen heartbeat from inside the actor's `Heartbeat`
        // arm (replying `GROUP_ID_NOT_FOUND`), which is where the per-group kind
        // lock now lives.
        let handle = coordinator.get_or_create_group(&req.group_id, GroupKindTag::Consumer);
        let (tx, rx) = oneshot::channel();
        if handle
            .tx
            .send(GroupActorMessage::Heartbeat {
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

fn error(code: i16) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        error_code: code,
        ..Default::default()
    }
}

fn encode(version: i16, resp: &ConsumerGroupHeartbeatResponse) -> Result<Bytes, BrokerError> {
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
        use crabka_protocol::owned::consumer_group_heartbeat_response::{
            self, ConsumerGroupHeartbeatResponse,
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
            consumer_group_heartbeat_response::MAX_VERSION,
            &error(codes::GROUP_AUTHORIZATION_FAILED),
        )
        .expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = ConsumerGroupHeartbeatResponse::decode(
            &mut cur,
            consumer_group_heartbeat_response::MAX_VERSION,
        )
        .unwrap();
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
    }
}
