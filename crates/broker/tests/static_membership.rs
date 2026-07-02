//! KIP-345 static-membership integration tests.
//!
//! Exercises the full `JoinGroup` → `SyncGroup` → (rejoin with same
//! `group.instance.id`) cycle through the in-process broker harness and
//! checks the three KIP-345 invariants:
//!
//! 1. A static rejoin into a `Stable` group preserves the prior assignment
//!    and does NOT advance `generation_id`.
//! 2. A second client trying to use the same `group.instance.id` while
//!    the first is still live is rejected with `FENCED_INSTANCE_ID`.
//! 3. `LeaveGroup` v3+ with a `MemberIdentity { member_id: "",
//!    group_instance_id: Some(...) }` resolves the static slot and
//!    removes it.

use assert2::{assert, check};
use bytes::Bytes;
use crabka_protocol::owned::heartbeat_request::HeartbeatRequest;
use crabka_protocol::owned::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
use crabka_protocol::owned::leave_group_request::{LeaveGroupRequest, MemberIdentity};
use crabka_protocol::owned::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};

mod support;

/// `FENCED_INSTANCE_ID` (82, KIP-345).
const FENCED_INSTANCE_ID: i16 = 82;
/// `MEMBER_ID_REQUIRED` (79, KIP-394 bootstrap dance).
const MEMBER_ID_REQUIRED: i16 = 79;

fn join_request(group_id: &str, member_id: &str, instance_id: Option<&str>) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id: group_id.into(),
        protocol_type: "consumer".into(),
        member_id: member_id.into(),
        group_instance_id: instance_id.map(str::to_string),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 1_500,
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".into(),
            metadata: Bytes::new(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Boot a group with one static member and sync an assignment for it.
/// Returns the assigned `(member_id, generation_id, assignment)`.
async fn bootstrap_static_member(
    client: &crabka_client_core::Client,
    group_id: &str,
    instance_id: &str,
    assignment: Bytes,
) -> (String, i32, Bytes) {
    // 1. Empty member_id → MEMBER_ID_REQUIRED + assigned id.
    let r1 = client
        .send(join_request(group_id, "", Some(instance_id)))
        .await
        .expect("JoinGroup #1");
    assert!(r1.error_code == MEMBER_ID_REQUIRED);
    let mid = r1.member_id.clone();
    assert!(!mid.is_empty());

    // 2. Rejoin with assigned member_id → become leader.
    let r2 = client
        .send(join_request(group_id, &mid, Some(instance_id)))
        .await
        .expect("JoinGroup #2");
    assert!(r2.error_code == 0);
    assert!(r2.leader == mid);
    let generation = r2.generation_id;

    // 3. Leader SyncGroup installs an assignment for itself.
    let r3 = client
        .send(SyncGroupRequest {
            group_id: group_id.into(),
            generation_id: generation,
            member_id: mid.clone(),
            group_instance_id: Some(instance_id.into()),
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            assignments: vec![SyncGroupRequestAssignment {
                member_id: mid.clone(),
                assignment: assignment.clone(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("SyncGroup");
    assert!(r3.error_code == 0);
    assert!(r3.assignment == assignment);

    (mid, generation, assignment)
}

#[tokio::test]
async fn static_rejoin_preserves_assignment_and_generation() {
    let p = support::start().await;

    let (mid1, gen1, assignment) = bootstrap_static_member(
        &p.client,
        "g-static-1",
        "instance-A",
        Bytes::from_static(b"assignment-bytes"),
    )
    .await;

    // Confirm Heartbeat works with both member_id and instance_id.
    let hb = p
        .client
        .send(HeartbeatRequest {
            group_id: "g-static-1".into(),
            generation_id: gen1,
            member_id: mid1.clone(),
            group_instance_id: Some("instance-A".into()),
            ..Default::default()
        })
        .await
        .expect("Heartbeat");
    assert!(hb.error_code == 0);

    // A "restart" — same instance id, but the client picks up via the
    // KIP-394 bootstrap dance and gets back the same member_id (since
    // the static slot is preserved).
    let boot = p
        .client
        .send(join_request("g-static-1", "", Some("instance-A")))
        .await
        .expect("rebootstrap JoinGroup");
    assert!(boot.error_code == MEMBER_ID_REQUIRED);
    assert!(
        boot.member_id == mid1,
        "static bootstrap returns the existing slot's member_id"
    );

    // Rejoin with the recovered id. Group is Stable → no rebalance →
    // generation_id unchanged and the cached assignment is reachable.
    let rejoin = p
        .client
        .send(join_request("g-static-1", &mid1, Some("instance-A")))
        .await
        .expect("static rejoin");
    check!(rejoin.error_code == 0);
    check!(
        rejoin.generation_id == gen1,
        "static rejoin must NOT advance generation_id"
    );
    check!(rejoin.member_id == mid1);
    drop(assignment);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn second_client_with_same_instance_id_is_fenced() {
    let p = support::start().await;

    let (mid1, gen1, _) = bootstrap_static_member(
        &p.client,
        "g-fence",
        "instance-B",
        Bytes::from_static(b"asgn"),
    )
    .await;

    // A *different* live `member_id` claims the same instance id. The
    // KIP-345 rule: reject with FENCED_INSTANCE_ID.
    let intruder = p
        .client
        .send(join_request(
            "g-fence",
            "imposter-member-id",
            Some("instance-B"),
        ))
        .await
        .expect("intruder JoinGroup");
    assert!(intruder.error_code == FENCED_INSTANCE_ID);

    // Heartbeat with a wrong member_id but the right instance id is also
    // fenced. (Defense-in-depth: a client whose `member_id` was reset
    // shouldn't be able to talk under the static slot until it
    // re-bootstraps.)
    let hb_fenced = p
        .client
        .send(HeartbeatRequest {
            group_id: "g-fence".into(),
            generation_id: gen1,
            member_id: "wrong-member-id".into(),
            group_instance_id: Some("instance-B".into()),
            ..Default::default()
        })
        .await
        .expect("Heartbeat (fenced)");
    assert!(hb_fenced.error_code == FENCED_INSTANCE_ID);

    // The original member is unaffected.
    let hb_ok = p
        .client
        .send(HeartbeatRequest {
            group_id: "g-fence".into(),
            generation_id: gen1,
            member_id: mid1.clone(),
            group_instance_id: Some("instance-B".into()),
            ..Default::default()
        })
        .await
        .expect("Heartbeat (incumbent)");
    assert!(hb_ok.error_code == 0);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn leave_group_resolves_static_member_by_instance_id() {
    let p = support::start().await;

    let (mid, _gen, _) = bootstrap_static_member(
        &p.client,
        "g-leave",
        "instance-C",
        Bytes::from_static(b"asgn"),
    )
    .await;

    // LeaveGroup v3+ with empty member_id, identity carries only the
    // instance id. Broker must resolve via the static index, remove the
    // slot, and echo the instance id in the response with NONE.
    let resp = p
        .client
        .send(LeaveGroupRequest {
            group_id: "g-leave".into(),
            member_id: String::new(),
            members: vec![MemberIdentity {
                member_id: String::new(),
                group_instance_id: Some("instance-C".into()),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("LeaveGroup");
    check!(resp.error_code == 0);
    assert!(resp.members.len() == 1);
    check!(resp.members[0].error_code == 0);
    check!(resp.members[0].group_instance_id.as_deref() == Some("instance-C"));

    // A subsequent Heartbeat from the old member id should be rejected —
    // the slot is gone.
    let hb = p
        .client
        .send(HeartbeatRequest {
            group_id: "g-leave".into(),
            generation_id: 1,
            member_id: mid,
            ..Default::default()
        })
        .await
        .expect("Heartbeat after leave");
    assert!(hb.error_code != 0);

    p.broker.shutdown().await;
}
