//! `LeaveGroup` (`api_key=13`). It removes one or more members inside the
//! group's actor. It then opens a rebalance again, if the group is still
//! `Stable` and members remain.

use bytes::Bytes;
use crabka_protocol::{
    Decode,
    owned::{leave_group_request::LeaveGroupRequest, leave_group_response::LeaveGroupResponse},
};
use tokio::sync::oneshot;

use crate::{
    broker::Broker, codes, coordinator::unified::actor::GroupActorMessage, error::BrokerError,
    handlers::group_read_denied,
};

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
                ctx,
                &req.group_id,
            ) {
                return crate::handlers::encode_response(
                    &LeaveGroupResponse {
                        error_code: codes::GROUP_AUTHORIZATION_FAILED,
                        throttle_time_ms: 0,
                        members: Vec::new(),
                        ..Default::default()
                    },
                    version,
                );
            }
        }

        if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &req.group_id) {
            return crate::handlers::encode_response(
                &LeaveGroupResponse {
                    error_code,
                    throttle_time_ms: 0,
                    members: Vec::new(),
                    ..Default::default()
                },
                version,
            );
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
        crate::handlers::encode_response(&resp, version)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

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

        let ctx = crate::test_support::request_context(&principal, &peer, "leave-client");

        assert!(group_read_denied(&authorizer, &image, &ctx, "g"));

        let resp = LeaveGroupResponse {
            error_code: codes::GROUP_AUTHORIZATION_FAILED,
            throttle_time_ms: 0,
            members: Vec::new(),
            ..Default::default()
        };
        let bytes = crate::handlers::encode_response(&resp, leave_group_response::MAX_VERSION)
            .expect("encode");
        let mut cur: &[u8] = &bytes;
        let decoded =
            LeaveGroupResponse::decode(&mut cur, leave_group_response::MAX_VERSION).unwrap();
        assert!(
            (
                decoded.error_code,
                decoded.throttle_time_ms,
                decoded.members,
                cur.is_empty(),
            ) == (codes::GROUP_AUTHORIZATION_FAILED, 0, vec![], true),
            "response decoder consumed all bytes"
        );
    }
}
