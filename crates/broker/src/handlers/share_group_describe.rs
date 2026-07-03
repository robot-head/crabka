//! `ShareGroupDescribe` (`api_key` 77) — KIP-932. Returns one
//! `DescribedGroup` per requested `group_id`, rendered from the share
//! actor's `Describe` view.
//!
//! Intercepted inline in `network::dispatch` (not `build_table`) so the
//! handler receives the per-connection principal + peer `SocketAddr` for the
//! per-group `Describe` ACL gate.

use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot;

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::share_group_describe_request::ShareGroupDescribeRequest;
use crabka_protocol::owned::share_group_describe_response::{
    DescribedGroup, ShareGroupDescribeResponse,
};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::share::actor::ShareGroupActorMessage;
use crate::error::BrokerError;

#[tracing::instrument(
    name = "handle_share_group_describe",
    level = "info",
    skip_all,
    fields(api = "ShareGroupDescribe", version, req_bytes = req_bytes.len()),
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
    let req = ShareGroupDescribeRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();
    let share_enabled = broker.config.share_group.enable;
    let ng_opt = Some(broker.group_coordinator.clone());

    let mut groups: Vec<DescribedGroup> = Vec::with_capacity(req.group_ids.len());
    for gid in &req.group_ids {
        // ── ACL preamble ────────────────────────────────────
        // Per-group `Describe` check. On Deny → per-group
        // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
        let acl_req = AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Group,
            resource_name: gid.as_str(),
            operation: AclOperation::Describe,
        };
        if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
            groups.push(DescribedGroup {
                group_id: gid.clone(),
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
                ..Default::default()
            });
            continue;
        }

        if !share_enabled {
            groups.push(DescribedGroup {
                group_id: gid.clone(),
                error_code: codes::GROUP_ID_NOT_FOUND,
                ..Default::default()
            });
            continue;
        }

        let Some(handle) = ng_opt.as_ref().and_then(|ng| ng.find_share(gid)) else {
            groups.push(DescribedGroup {
                group_id: gid.clone(),
                error_code: codes::GROUP_ID_NOT_FOUND,
                ..Default::default()
            });
            continue;
        };

        let (tx, rx) = oneshot::channel();
        if handle
            .tx
            .send(ShareGroupActorMessage::Describe { reply: tx })
            .await
            .is_err()
        {
            groups.push(DescribedGroup {
                group_id: gid.clone(),
                error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                ..Default::default()
            });
            continue;
        }
        match rx.await {
            Ok(view) => groups.push(view.into_described_group()),
            Err(_) => groups.push(DescribedGroup {
                group_id: gid.clone(),
                error_code: codes::UNKNOWN_SERVER_ERROR,
                ..Default::default()
            }),
        }
    }

    let resp = ShareGroupDescribeResponse {
        groups,
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
    use crabka_protocol::UnknownTaggedFields;
    use crabka_protocol::owned::share_group_describe_response;
    use crabka_security::Principal;
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::authorizer::Authorizer;
    use crate::test_support::DenyAll;

    fn request(group_ids: &[&str]) -> ShareGroupDescribeRequest {
        ShareGroupDescribeRequest {
            group_ids: group_ids.iter().map(|g| (*g).to_string()).collect(),
            include_authorized_operations: false,
            ..Default::default()
        }
    }

    fn encode_request(req: &ShareGroupDescribeRequest) -> Bytes {
        crate::test_support::encode_request(req, share_group_describe_response::MAX_VERSION)
    }

    fn decode_response(bytes: &Bytes) -> ShareGroupDescribeResponse {
        crate::test_support::decode_response(bytes, share_group_describe_response::MAX_VERSION)
    }

    fn test_context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::test_support::request_context(principal, peer, "admin-client")
    }

    async fn start_broker(
        authorizer: Arc<dyn Authorizer>,
        share_enabled: bool,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = authorizer;
            cfg.share_group.enable = share_enabled;
        })
        .await
    }

    fn principal() -> Principal {
        crate::test_support::principal("alice")
    }

    #[tokio::test]
    async fn handle_denied_groups_preserve_group_ids_and_error_codes() {
        let version = share_group_describe_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req_bytes = encode_request(&request(&["g1", "g2"]));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        let expected = ShareGroupDescribeResponse {
            throttle_time_ms: 0,
            groups: vec![
                DescribedGroup {
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    error_message: None,
                    group_id: "g1".into(),
                    group_state: String::new(),
                    group_epoch: 0,
                    assignment_epoch: 0,
                    assignor_name: String::new(),
                    members: Vec::new(),
                    authorized_operations: i32::MIN,
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
                DescribedGroup {
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    error_message: None,
                    group_id: "g2".into(),
                    group_state: String::new(),
                    group_epoch: 0,
                    assignment_epoch: 0,
                    assignor_name: String::new(),
                    members: Vec::new(),
                    authorized_operations: i32::MIN,
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
            ],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_disabled_feature_wins_even_when_share_actor_exists() {
        let version = share_group_describe_response::MAX_VERSION;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), false).await;
        let broker = broker_handle.broker_arc_for_test();
        broker.group_coordinator.mark_share("g1");
        let _actor = broker.group_coordinator.get_or_create_share("g1");
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req_bytes = encode_request(&request(&["g1"]));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        let expected = ShareGroupDescribeResponse {
            throttle_time_ms: 0,
            groups: vec![DescribedGroup {
                error_code: codes::GROUP_ID_NOT_FOUND,
                error_message: None,
                group_id: "g1".into(),
                group_state: String::new(),
                group_epoch: 0,
                assignment_epoch: 0,
                assignor_name: String::new(),
                members: Vec::new(),
                authorized_operations: i32::MIN,
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
