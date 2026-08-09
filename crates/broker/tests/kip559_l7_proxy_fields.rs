// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.

//! KIP-559: the `JoinGroup` v7+ and `SyncGroup` v5+ responses carry
//! `protocol_type` and `protocol_name`, so an L7 proxy can identify the
//! group-coordination exchange without remembering the earlier one.
//!
//! `Client::send` negotiates to the broker `MAX_VERSION`, which is v9 for
//! `JoinGroup` and v5 for `SyncGroup`, so these tests always exercise the
//! KIP-559 fields.

use assert2::{assert, check};
mod support;

use crabka_protocol::owned::{
    join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
    sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
};

const GROUP: &str = "kip559-grp";
const PROTOCOL_TYPE: &str = "consumer";
const PROTOCOL_NAME: &str = "range";

async fn bootstrap_member(p: &support::InProcess) -> (String, i32) {
    // Step 1: empty member_id → broker returns MEMBER_ID_REQUIRED with a
    // generated id. KIP-559 doesn't require fields here (the group state
    // hasn't recorded a protocol yet).
    let r1 = p
        .client
        .send(JoinGroupRequest {
            group_id: GROUP.into(),
            protocol_type: PROTOCOL_TYPE.into(),
            member_id: String::new(),
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 1_500,
            protocols: vec![JoinGroupRequestProtocol {
                name: PROTOCOL_NAME.into(),
                metadata: bytes::Bytes::new(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("JoinGroup (bootstrap)");
    assert!(r1.error_code == 79, "expected MEMBER_ID_REQUIRED");
    let mid = r1.member_id.clone();
    assert!(!mid.is_empty());

    // Step 2: re-join with the assigned member_id, falls out as leader.
    let r2 = p
        .client
        .send(JoinGroupRequest {
            group_id: GROUP.into(),
            protocol_type: PROTOCOL_TYPE.into(),
            member_id: mid.clone(),
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 1_500,
            protocols: vec![JoinGroupRequestProtocol {
                name: PROTOCOL_NAME.into(),
                metadata: bytes::Bytes::new(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("JoinGroup");
    check!(r2.error_code == 0, "JoinGroup must succeed: {r2:?}");

    // KIP-559: protocol_type and protocol_name must be set on the
    // success response.
    check!(
        r2.protocol_type.as_deref() == Some(PROTOCOL_TYPE),
        "JoinGroup must echo protocol_type: {r2:?}"
    );
    check!(
        r2.protocol_name.as_deref() == Some(PROTOCOL_NAME),
        "JoinGroup must echo protocol_name: {r2:?}"
    );

    (mid, r2.generation_id)
}

#[tokio::test]
async fn join_group_response_carries_protocol_type_and_name_on_success() {
    let p = support::start().await;
    let (_mid, _generation) = bootstrap_member(&p).await;
    p.broker.shutdown().await;
}

#[tokio::test]
async fn sync_group_response_carries_protocol_type_and_name_on_success() {
    let p = support::start().await;
    let (mid, generation) = bootstrap_member(&p).await;

    let r3 = p
        .client
        .send(SyncGroupRequest {
            group_id: GROUP.into(),
            generation_id: generation,
            member_id: mid.clone(),
            protocol_type: Some(PROTOCOL_TYPE.into()),
            protocol_name: Some(PROTOCOL_NAME.into()),
            assignments: vec![SyncGroupRequestAssignment {
                member_id: mid.clone(),
                assignment: bytes::Bytes::from_static(b"asgn"),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("SyncGroup");
    check!(r3.error_code == 0, "SyncGroup must succeed: {r3:?}");

    // KIP-559: both fields must be echoed on the v5+ response.
    check!(
        r3.protocol_type.as_deref() == Some(PROTOCOL_TYPE),
        "SyncGroup must echo protocol_type: {r3:?}"
    );
    check!(
        r3.protocol_name.as_deref() == Some(PROTOCOL_NAME),
        "SyncGroup must echo protocol_name: {r3:?}"
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn join_group_response_carries_protocol_type_on_inconsistent_protocol_error() {
    let p = support::start().await;
    // Bootstrap the group as `consumer/range`.
    let (_mid, _gen) = bootstrap_member(&p).await;

    // Second member tries to join with a different protocol_type — must
    // be rejected with INCONSISTENT_GROUP_PROTOCOL (23). KIP-559: the
    // recorded group protocol_type must still ride along on the error
    // response so the L7 proxy sees what dialog this belongs to.
    let r = p
        .client
        .send(JoinGroupRequest {
            group_id: GROUP.into(),
            protocol_type: "stream".into(),
            member_id: "some-member-2".into(),
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 1_500,
            protocols: vec![JoinGroupRequestProtocol {
                name: "doesnt-matter".into(),
                metadata: bytes::Bytes::new(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("JoinGroup");
    assert!(
        r.error_code == 23,
        "expected INCONSISTENT_GROUP_PROTOCOL (23), got {r:?}"
    );
    assert!(
        r.protocol_type.as_deref() == Some(PROTOCOL_TYPE),
        "INCONSISTENT_GROUP_PROTOCOL response must echo the recorded protocol_type: {r:?}"
    );
    p.broker.shutdown().await;
}

#[tokio::test]
async fn sync_group_response_carries_protocol_type_on_unknown_member_error() {
    let p = support::start().await;
    let (_mid, generation) = bootstrap_member(&p).await;

    // SyncGroup with a member_id the group has never seen → broker
    // returns UNKNOWN_MEMBER_ID (25). KIP-559: protocol_type must still
    // ride along because the group exists and has a recorded protocol.
    let r = p
        .client
        .send(SyncGroupRequest {
            group_id: GROUP.into(),
            generation_id: generation,
            member_id: "ghost-member".into(),
            protocol_type: Some(PROTOCOL_TYPE.into()),
            protocol_name: Some(PROTOCOL_NAME.into()),
            assignments: vec![],
            ..Default::default()
        })
        .await
        .expect("SyncGroup");
    check!(
        r.error_code == 25,
        "expected UNKNOWN_MEMBER_ID (25), got {r:?}"
    );
    check!(
        r.protocol_type.as_deref() == Some(PROTOCOL_TYPE),
        "UNKNOWN_MEMBER_ID response must echo the recorded protocol_type: {r:?}"
    );
    check!(
        r.protocol_name.as_deref() == Some(PROTOCOL_NAME),
        "UNKNOWN_MEMBER_ID response must echo the recorded protocol_name: {r:?}"
    );

    p.broker.shutdown().await;
}
