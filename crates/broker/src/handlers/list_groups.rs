//! `ListGroups` (`api_key=16`). Returns every known group across all four
//! registries: classic groups from `GroupCoordinator::list_groups` (type
//! `"classic"`), next-gen KIP-848 consumer groups from the unified
//! coordinator's consumer registry (type `"consumer"`), KIP-932 share
//! groups from its share registry (type `"share"`), and KIP-1071 streams
//! groups from its streams registry (type `"streams"`). The optional
//! `states_filter` (v4+) and `types_filter` (v5+, e.g. `["share"]` from
//! `kafka-share-groups.sh --list`, `["streams"]` from `kafka-streams-groups.sh
//! --list`, or `["consumer"]` from `kafka-consumer-groups.sh --list`) are both
//! honored. A `group_id` is emitted at most once: the registries are disjoint
//! by `GroupType`, but we defensively dedup.

use std::collections::HashSet;

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::list_groups_request::ListGroupsRequest;
use crabka_protocol::owned::list_groups_response::{ListGroupsResponse, ListedGroup};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::GroupType;
use crate::coordinator::unified::classic_state::GroupState;
use crate::error::BrokerError;

#[tracing::instrument(
    name = "handle_list_groups",
    level = "info",
    skip_all,
    fields(api = "ListGroups", version, req_bytes = req_bytes.len()),
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
    let req = ListGroupsRequest::decode(&mut cur, version)?;
    let snapshots = broker.group_coordinator.list_groups().await;

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
        broker.config.authorizer.authorize(&*image, &acl_req) != AuthorizationResult::Deny
    };

    let mut groups: Vec<ListedGroup> = Vec::with_capacity(snapshots.len());
    // Ids already emitted, so the same group_id never appears twice across the
    // three (disjoint-by-design) registries.
    let mut emitted: HashSet<String> = HashSet::new();

    // ── Classic groups (group_type "classic") ───────────────────────────
    for s in snapshots {
        // KIP-1071: a Streams-locked group keeps its drained classic-kind
        // offset-home actor in this classic snapshot. Report it via the streams
        // pass (group_type="streams"), never here as "classic".
        // NOTE: do NOT add to `emitted` here — the streams pass must still be
        // able to emit it. The consumer pass is guarded separately below.
        if broker.group_coordinator.group_type(&s.group_id) == Some(GroupType::Streams) {
            continue;
        }
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
        emitted.insert(s.group_id.clone());
        groups.push(ListedGroup {
            group_id: s.group_id,
            protocol_type: s.protocol_type.unwrap_or_else(|| "consumer".into()),
            group_state: state_str.into(),
            group_type: "classic".into(),
            ..Default::default()
        });
    }

    // ── KIP-848 next-gen consumer groups (group_type "consumer") ────────
    // These live in the unified coordinator's consumer registry, separate
    // from the classic group registry.
    let ng = &broker.group_coordinator;
    // KIP-1071: consumer_group_ids returns ALL ids from self.groups (classic
    // included). Filter out Streams-locked ids so a converted group does not
    // appear here as "consumer" — it will be emitted by the streams pass.
    let consumer_ids: Vec<String> = ng
        .consumer_group_ids()
        .into_iter()
        .filter(|id| ng.group_type(id) != Some(GroupType::Streams))
        .collect();
    append_next_gen(
        &mut groups,
        &mut emitted,
        "consumer",
        consumer_ids,
        &req,
        states_active,
        types_active,
        &authorized,
    );
    // ── KIP-932 share groups (group_type "share") ───────────────────────
    // Gated on `share_group.enable`; share groups live in the same
    // coordinator's separate share registry.
    if broker.config.share_group.enable {
        append_next_gen(
            &mut groups,
            &mut emitted,
            "share",
            ng.share_group_ids(),
            &req,
            states_active,
            types_active,
            &authorized,
        );
    }
    // ── KIP-1071 streams groups (group_type "streams") ──────────────────
    // Surfaced so the JVM `kafka-streams-groups.sh --list` / `--describe`
    // (AdminClient `listGroups(typesFilter=[Streams])`) can find them; the
    // describe hop is gated behind a non-empty list on the JVM side.
    if broker.config.streams_group.enable {
        append_next_gen(
            &mut groups,
            &mut emitted,
            "streams",
            ng.streams_group_ids(),
            &req,
            states_active,
            types_active,
            &authorized,
        );
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

/// Append one `ListedGroup` per next-gen group id (`group_type` is either
/// `"consumer"` or `"share"`), honoring `states_filter` / `types_filter` and the
/// per-group Describe ACL, and deduping against already-emitted ids. Stays sync:
/// the group's runtime state isn't cheaply available without an actor
/// round-trip, so we report the constant "Stable" — `--list` filters on
/// `types_filter`, not on the state here.
#[allow(clippy::too_many_arguments)]
fn append_next_gen(
    groups: &mut Vec<ListedGroup>,
    emitted: &mut HashSet<String>,
    group_type: &str,
    ids: Vec<String>,
    req: &ListGroupsRequest,
    states_active: bool,
    types_active: bool,
    authorized: &impl Fn(&str) -> bool,
) {
    const STATE: &str = "Stable";
    if states_active && !req.states_filter.iter().any(|v| v == STATE) {
        return;
    }
    if types_active
        && !req
            .types_filter
            .iter()
            .any(|t| t.eq_ignore_ascii_case(group_type))
    {
        return;
    }
    // Share and streams groups carry an empty protocol_type (Kafka emits no
    // consumer protocol for them); next-gen consumer groups use "consumer".
    let protocol_type = if group_type == "share" || group_type == "streams" {
        String::new()
    } else {
        "consumer".into()
    };
    for gid in ids {
        if emitted.contains(&gid) || !authorized(&gid) {
            continue;
        }
        emitted.insert(gid.clone());
        groups.push(ListedGroup {
            group_id: gid,
            protocol_type: protocol_type.clone(),
            group_state: STATE.into(),
            group_type: group_type.into(),
            ..Default::default()
        });
    }
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
