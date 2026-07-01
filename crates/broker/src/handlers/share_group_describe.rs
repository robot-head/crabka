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
