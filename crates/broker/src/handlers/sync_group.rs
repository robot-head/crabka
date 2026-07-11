//! `SyncGroup` (`api_key=14`). Routes into the group's unified actor as a
//! `ClassicSync` message. The leader's call installs assignments and the actor
//! drains the parked followers; a follower with no assignment yet is parked
//! until then (capped here by `FOLLOWER_WAIT`).
//!
//! KIP-559 (v5+): the response carries `protocol_type` + `protocol_name`
//! so an L7 proxy can route the call without remembering the prior
//! `JoinGroup` exchange.

use std::time::Duration;

use bytes::Bytes;
use crabka_protocol::{
    Decode,
    owned::{sync_group_request::SyncGroupRequest, sync_group_response::SyncGroupResponse},
};
use tokio::sync::oneshot;

use crate::{
    broker::Broker, codes, coordinator::unified::actor::GroupActorMessage, error::BrokerError,
    handlers::group_read_denied,
};

/// Upper bound on how long a follower's `SyncGroup` is parked waiting for the
/// leader's call to install assignments before giving up with
/// `REBALANCE_IN_PROGRESS`. Matches Kafka's default group rebalance timeout
/// order of magnitude so a healthy leader always beats the deadline.
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
    version: crate::handlers::ApiVersion,
    _correlation_id: crate::handlers::CorrelationId,
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
                ctx,
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
            protocol_type: result.protocol_type,
            protocol_name: result.protocol_name,
            ..Default::default()
        };
        crate::handlers::encode_response(&resp, version)
    }
}

fn encode_err(
    version: crate::handlers::ApiVersion,
    code: crate::handlers::ErrorCode,
    protocol_type: Option<String>,
    protocol_name: Option<String>,
) -> Result<Bytes, BrokerError> {
    let resp = SyncGroupResponse {
        error_code: code,
        protocol_type,
        protocol_name,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use crabka_protocol::owned::{
        join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
        join_group_response::{self, JoinGroupResponse},
        sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
        sync_group_response::{self, SyncGroupResponse},
    };
    use crabka_security::Principal;

    use crate::{
        authorizer::Authorizer,
        broker::{Broker, BrokerHandle},
        test_support::{DenyAll, encode_request},
    };

    const GROUP: &str = "sync-group-unit";
    const PROTOCOL_TYPE: &str = "consumer";
    const PROTOCOL_NAME: &str = "range";

    fn decode_join(bytes: &Bytes) -> JoinGroupResponse {
        crate::test_support::decode_response(bytes, join_group_response::MAX_VERSION)
    }

    fn decode_sync(bytes: &Bytes) -> SyncGroupResponse {
        crate::test_support::decode_response(bytes, sync_group_response::MAX_VERSION)
    }

    fn principal() -> Principal {
        crate::test_support::principal("alice")
    }

    fn context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::test_support::request_context(principal, peer, "sync-group-client")
    }

    async fn start_broker(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.audit_enabled = false;
            cfg.authorizer = authorizer;
        })
        .await
    }

    async fn bootstrap_member(
        broker: &Broker,
        ctx: &crate::handlers::RequestContext<'_>,
    ) -> (String, i32) {
        let version = join_group_response::MAX_VERSION;
        let join = |member_id: String| JoinGroupRequest {
            group_id: GROUP.into(),
            protocol_type: PROTOCOL_TYPE.into(),
            member_id,
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 1_500,
            protocols: vec![JoinGroupRequestProtocol {
                name: PROTOCOL_NAME.into(),
                metadata: Bytes::new(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let r1 = crate::handlers::join_group::handle(
            broker,
            version,
            1,
            &encode_request(&join(String::new()), version),
            ctx,
        )
        .await
        .expect("JoinGroup bootstrap");
        let r1 = decode_join(&r1);
        assert!(r1.error_code == codes::MEMBER_ID_REQUIRED, "{r1:?}");
        assert!(!r1.member_id.is_empty());

        let r2 = crate::handlers::join_group::handle(
            broker,
            version,
            2,
            &encode_request(&join(r1.member_id.clone()), version),
            ctx,
        )
        .await
        .expect("JoinGroup rejoin");
        let r2 = decode_join(&r2);
        assert!(
            (
                r2.error_code,
                r2.protocol_type.as_deref(),
                r2.protocol_name.as_deref()
            ) == (codes::NONE, Some(PROTOCOL_TYPE), Some(PROTOCOL_NAME)),
            "{r2:?}"
        );
        (r2.member_id, r2.generation_id)
    }

    use super::*;

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let principal = crabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = crate::test_support::request_context(&principal, &peer, "sync-client");

        assert!(group_read_denied(&authorizer, &image, &ctx, "g"));

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

    #[test]
    fn encode_err_preserves_empty_assignment_and_kip559_protocol_fields() {
        let bytes = encode_err(
            sync_group_response::MAX_VERSION,
            codes::UNKNOWN_MEMBER_ID,
            Some(PROTOCOL_TYPE.into()),
            Some(PROTOCOL_NAME.into()),
        )
        .expect("encode error");
        let resp = decode_sync(&bytes);

        let expected = SyncGroupResponse {
            throttle_time_ms: 0,
            error_code: codes::UNKNOWN_MEMBER_ID,
            protocol_type: Some(PROTOCOL_TYPE.into()),
            protocol_name: Some(PROTOCOL_NAME.into()),
            assignment: Bytes::new(),
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
    }

    #[tokio::test]
    async fn handle_denies_group_read_and_preserves_error_response_shape() {
        let version = sync_group_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let req = SyncGroupRequest {
            group_id: GROUP.into(),
            member_id: "member-a".into(),
            generation_id: 1,
            protocol_type: Some(PROTOCOL_TYPE.into()),
            protocol_name: Some(PROTOCOL_NAME.into()),
            ..Default::default()
        };

        let resp = handle(&broker, version, 3, &encode_request(&req, version), &ctx)
            .await
            .expect("SyncGroup");
        let resp = decode_sync(&resp);

        let expected = SyncGroupResponse {
            throttle_time_ms: 0,
            error_code: codes::GROUP_AUTHORIZATION_FAILED,
            protocol_type: None,
            protocol_name: None,
            assignment: Bytes::new(),
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_success_preserves_assignment_and_kip559_protocol_fields() {
        let version = sync_group_response::MAX_VERSION;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let (member_id, generation_id) = bootstrap_member(&broker, &ctx).await;
        let assignment = Bytes::from_static(b"assignment-payload");
        let req = SyncGroupRequest {
            group_id: GROUP.into(),
            generation_id,
            member_id: member_id.clone(),
            protocol_type: Some(PROTOCOL_TYPE.into()),
            protocol_name: Some(PROTOCOL_NAME.into()),
            assignments: vec![SyncGroupRequestAssignment {
                member_id,
                assignment: assignment.clone(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let resp = handle(&broker, version, 3, &encode_request(&req, version), &ctx)
            .await
            .expect("SyncGroup");
        let resp = decode_sync(&resp);

        let expected = SyncGroupResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            protocol_type: Some(PROTOCOL_TYPE.into()),
            protocol_name: Some(PROTOCOL_NAME.into()),
            assignment,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }
}
