//! `ListGroups` (`api_key=16`). Returns every known group: classic groups
//! from `GroupManager::list_groups` plus KIP-932 share groups from the
//! next-gen coordinator's share registry. The optional `states_filter` (v4+)
//! and `types_filter` (v5+, e.g. `["share"]` from `kafka-share-groups.sh
//! --list`) are both honored.

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::list_groups_request::ListGroupsRequest;
use crabka_protocol::owned::list_groups_response::{ListGroupsResponse, ListedGroup};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::group::GroupState;
use crate::error::BrokerError;

pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = ListGroupsRequest::decode(&mut cur, version)?;
    let snapshots = broker.group_manager.list_groups().await;

    let image = broker.controller.current_image();

    // Both filters are plain `Vec<String>` (not Option); they are empty when
    // no filter was requested (older wire versions always decode them empty).
    let states_active = !req.states_filter.is_empty();
    let types_active = !req.types_filter.is_empty();

    // Per-group `Describe` ACL. On Deny the group is silently omitted from the
    // response (no per-group error_code). With the default `AllowAllAuthorizer`
    // every group passes; with `SimpleAclAuthorizer` the super-user bypass plus
    // matching Describe ACLs let groups through; with `OpaAuthorizer` the policy
    // decides per group.
    let authorized = |group_id: &str| {
        let acl_req = AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Group,
            resource_name: group_id,
            operation: AclOperation::Describe,
        };
        broker.config.authorizer.authorize(&image, &acl_req) != AuthorizationResult::Deny
    };

    let mut groups: Vec<ListedGroup> = Vec::with_capacity(snapshots.len());

    // ── Classic groups (group_type "classic") ───────────────────────────
    for s in snapshots {
        if !authorized(s.group_id.as_str()) {
            continue;
        }
        let state_str = state_to_str(s.state);
        if states_active && !req.states_filter.iter().any(|v| v == state_str) {
            continue;
        }
        if types_active
            && !req
                .types_filter
                .iter()
                .any(|t| t.eq_ignore_ascii_case("classic"))
        {
            continue;
        }
        groups.push(ListedGroup {
            group_id: s.group_id,
            protocol_type: s.protocol_type.unwrap_or_else(|| "consumer".into()),
            group_state: state_str.into(),
            group_type: "classic".into(),
            ..Default::default()
        });
    }

    // ── KIP-932 share groups (group_type "share") ───────────────────────
    // Share groups live in the next-gen coordinator's share registry, not the
    // classic `GroupManager` map, so they need a separate pass. `list_groups`
    // stays sync (no actor Describe hop): the share group's runtime state isn't
    // cheaply available without a round-trip, so report "Stable" — `--list`
    // filters on `types_filter`, not on the state here.
    let share_state = "Stable";
    let share_state_ok = !states_active || req.states_filter.iter().any(|v| v == share_state);
    let share_type_ok = !types_active
        || req
            .types_filter
            .iter()
            .any(|t| t.eq_ignore_ascii_case("share"));
    let share_ng = (broker.config.share_group.enable && share_state_ok && share_type_ok)
        .then(|| broker.group_manager.next_gen())
        .flatten();
    if let Some(ng) = share_ng {
        for gid in ng.share_group_ids() {
            if !authorized(&gid) {
                continue;
            }
            groups.push(ListedGroup {
                group_id: gid,
                protocol_type: String::new(),
                group_state: share_state.into(),
                group_type: "share".into(),
                ..Default::default()
            });
        }
    }

    let resp = ListGroupsResponse {
        error_code: codes::NONE,
        groups,
        throttle_time_ms: 0,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

fn state_to_str(s: GroupState) -> &'static str {
    match s {
        GroupState::Empty => "Empty",
        GroupState::PreparingRebalance => "PreparingRebalance",
        GroupState::CompletingRebalance => "CompletingRebalance",
        GroupState::Stable => "Stable",
        GroupState::Dead => "Dead",
    }
}
