// rustc 1.95 clippy::pedantic ICEs on some broker integration test files
// (an upstream bug in clippy's body-analysis pass). Disable pedantic
// locally; the rest of the workspace still enforces the full pedantic gate.
#![allow(clippy::pedantic)]
#![cfg(not(target_os = "windows"))]

//! Byte-exactness pin for `DescribeGroups` (`api_key=15`) on a CLASSIC
//! consumer group: the response must carry, per member, the JoinGroup
//! protocol-metadata bytes (`member_metadata`) and, at the group level,
//! the SELECTED protocol name (`protocol_data`). Both were previously
//! dropped in the snapshot projection, returning empties and breaking
//! wire byte-exactness with Apache Kafka (`kafka-consumer-groups
//! --describe --members`, `AdminClient.describeClassicGroups`).
//!
//! Drives a real classic JoinGroup → SyncGroup → DescribeGroups against
//! the in-process broker over `crabka_client_core::Client`, mirroring
//! `group_protocol_negotiation.rs` (the MEMBER_ID_REQUIRED two-step +
//! INITIAL_REBALANCE_DELAY wait) and `unit.rs` (the SyncGroup shape).

use assert2::assert;
use std::time::Duration;

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::owned::describe_groups_request::DescribeGroupsRequest;
use crabka_protocol::owned::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
use crabka_protocol::owned::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};

// Kafka error code consumed by the JoinGroup two-step.
const ERR_NONE: i16 = 0;
const ERR_MEMBER_ID_REQUIRED: i16 = 79;

/// A fixed, recognizable JoinGroup protocol-metadata blob. The byte
/// shape is arbitrary (not a real `ConsumerProtocolSubscription`) — the
/// point is exact round-trip through stored state into `member_metadata`.
const KNOWN_METADATA: &[u8] = b"\x00\x01rangemeta\xde\xad";
/// The leader's SyncGroup assignment blob, echoed back as
/// `member_assignment`.
const ASSIGN: &[u8] = b"assign-bytes";

async fn start_broker() -> (crabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    let handle = Broker::start(config).await.expect("broker must start");
    let bootstrap = handle.listen_addr().to_string();
    (handle, bootstrap, tempdir)
}

fn join_request(group_id: &str, member_id: &str) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id: group_id.to_string(),
        session_timeout_ms: 10_000,
        rebalance_timeout_ms: 30_000,
        member_id: member_id.to_string(),
        group_instance_id: None,
        protocol_type: "consumer".to_string(),
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".to_string(),
            metadata: Bytes::from_static(KNOWN_METADATA),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// A single-member classic consumer group: drive JoinGroup (handling the
/// MEMBER_ID_REQUIRED two-step), then a leader SyncGroup, then assert
/// DescribeGroups surfaces the protocol name + per-member metadata.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_groups_reports_member_metadata_and_protocol_name() {
    let (handle, bootstrap, _tempdir) = start_broker().await;
    let group_id = "cg-describe-metadata";
    let client = Client::builder()
        .bootstrap(bootstrap)
        .client_id("describe-metadata-member")
        .build()
        .await
        .expect("client build");

    // ── JoinGroup round 1: empty member_id → MEMBER_ID_REQUIRED (79). ──
    let r1 = client
        .send(join_request(group_id, ""))
        .await
        .expect("first JoinGroup must round-trip");
    assert!(
        r1.error_code == ERR_MEMBER_ID_REQUIRED,
        "first JoinGroup (empty member_id) must return MEMBER_ID_REQUIRED (79), got {r1:?}"
    );
    let member_id = r1.member_id;
    assert!(
        !member_id.is_empty(),
        "broker must return a generated member_id"
    );

    // ── JoinGroup round 2: with the supplied id. Blocks for up to the
    // ~3 s initial-rebalance-delay before the broker completes the
    // rebalance and returns NONE. This lone member is the leader. ──
    let r2 = tokio::time::timeout(
        Duration::from_secs(10),
        client.send(join_request(group_id, &member_id)),
    )
    .await
    .expect("second JoinGroup timed out")
    .expect("second JoinGroup must round-trip");
    assert!(
        r2.error_code == ERR_NONE,
        "second JoinGroup must succeed, got {r2:?}"
    );
    assert!(
        r2.protocol_name.as_deref() == Some("range"),
        "single member must land on 'range', got {r2:?}"
    );
    assert!(
        r2.leader == member_id,
        "lone member must be the leader, got {r2:?}"
    );
    let generation_id = r2.generation_id;

    // ── SyncGroup: leader supplies its own assignment. ──
    let r3 = client
        .send(SyncGroupRequest {
            group_id: group_id.to_string(),
            generation_id,
            member_id: member_id.clone(),
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            assignments: vec![SyncGroupRequestAssignment {
                member_id: member_id.clone(),
                assignment: Bytes::from_static(ASSIGN),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("SyncGroup must round-trip");
    assert!(
        r3.error_code == ERR_NONE,
        "SyncGroup must succeed, got {r3:?}"
    );
    assert!(
        r3.assignment.as_ref() == ASSIGN,
        "SyncGroup must echo the assignment"
    );

    // ── DescribeGroups: the populated fields are the contract. ──
    let resp = client
        .send(DescribeGroupsRequest {
            groups: vec![group_id.to_string()],
            ..Default::default()
        })
        .await
        .expect("DescribeGroups must round-trip");
    handle.shutdown().await;

    assert!(
        resp.groups.len() == 1,
        "exactly one described group, got {resp:?}"
    );
    let g = &resp.groups[0];
    assert!(g.error_code == ERR_NONE, "DescribeGroups error: {g:?}");
    assert!(
        g.protocol_type == "consumer",
        "protocol_type must be 'consumer', got {:?}",
        g.protocol_type
    );
    assert!(
        g.protocol_data == "range",
        "protocol_data must be the selected protocol name 'range', got {:?}",
        g.protocol_data
    );
    assert!(g.members.len() == 1, "exactly one member, got {g:?}");
    let m = &g.members[0];
    assert!(
        m.member_metadata.as_ref() == KNOWN_METADATA,
        "member_metadata must be the JoinGroup protocol-metadata bytes, got {:?}",
        m.member_metadata
    );
    assert!(
        m.member_assignment.as_ref() == ASSIGN,
        "member_assignment must be the SyncGroup assignment bytes, got {:?}",
        m.member_assignment
    );
}
