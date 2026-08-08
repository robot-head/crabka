// rustc 1.95 clippy::pedantic ICEs on some broker integration test files
// (an upstream bug in clippy's body-analysis pass). Disable pedantic
// locally; the rest of the workspace still enforces the full pedantic gate.

//! Byte-exactness pin for `DescribeGroups` (`api_key=15`) on a CLASSIC
//! consumer group.
//!
//! For each member, the response must carry the `JoinGroup` protocol-metadata
//! bytes as `member_metadata`. At the group level, it must carry the SELECTED
//! protocol name as `protocol_data`. Both matter for wire byte-exactness with
//! Apache Kafka, which `kafka-consumer-groups --describe --members` and
//! `AdminClient.describeClassicGroups` rely on.
//!
//! The test drives a real classic `JoinGroup`, `SyncGroup`, and
//! `DescribeGroups` sequence against the in-process broker over
//! `crabka_client_core::Client`. It mirrors `group_protocol_negotiation.rs`,
//! for the two-step `MEMBER_ID_REQUIRED` flow and the
//! `INITIAL_REBALANCE_DELAY` wait, and `unit.rs`, for the `SyncGroup` shape.

use std::time::Duration;

use assert2::{assert, check};
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::owned::{
    describe_groups_request::DescribeGroupsRequest,
    describe_groups_response::DescribeGroupsResponse,
    join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
    sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
};

fn assert_described_group(resp: &DescribeGroupsResponse) {
    assert!(
        resp.groups.len() == 1,
        "exactly one described group, got {resp:?}"
    );
    let group = &resp.groups[0];
    check!(
        group.error_code == ERR_NONE,
        "described group must be error-free: {group:?}"
    );
    check!(
        group.protocol_type == "consumer",
        "unexpected protocol type: {group:?}"
    );
    check!(
        group.protocol_data == "range",
        "unexpected protocol name: {group:?}"
    );
    check!(group.members.len() == 1, "expected one member: {group:?}");
    let member = &group.members[0];
    assert!(
        member.member_metadata.as_ref() == KNOWN_METADATA,
        "unexpected member metadata"
    );
    assert!(
        member.member_assignment.as_ref() == ASSIGN,
        "unexpected member assignment"
    );
}

// Kafka error code consumed by the JoinGroup two-step.
const ERR_NONE: i16 = 0;
const ERR_MEMBER_ID_REQUIRED: i16 = 79;

/// Upper bound on a rejoin `JoinGroup` round trip. It covers the broker's
/// initial-rebalance delay of about 3 s, with headroom.
const JOIN_GROUP_TIMEOUT: Duration = Duration::from_secs(10);

/// A fixed, recognizable `JoinGroup` protocol-metadata blob. The byte shape is
/// arbitrary and is not a real `ConsumerProtocolSubscription`. The test uses it
/// to check the exact round trip through the stored state into
/// `member_metadata`.
const KNOWN_METADATA: &[u8] = b"\x00\x01rangemeta\xde\xad";
/// The leader's `SyncGroup` assignment blob, echoed back as
/// `member_assignment`.
const ASSIGN: &[u8] = b"assign-bytes";

/// The EXACT `ConsumerProtocolSubscription` bytes that a real `RangeAssignor`
/// console-consumer sent to `mirror.gcr.io/confluentinc/cp-kafka:7.4.0`.
///
/// The `describe_groups_jvm` Docker harness captured them into
/// `tests/fixtures/describe_groups/real_kafka_classic.json`, in the member
/// field `member_metadata_hex`. The wire shape is version `i16=3`, then one
/// subscribed topic `"t"`, `userData=null`, and an empty `ownedPartitions`.
///
/// This pins Crabka's `DescribeGroups` echo to a *realistic* subscription
/// instead of an arbitrary blob. cp and the JVM are the authority.
const REAL_KAFKA_SUBSCRIPTION: &[u8] = &[
    0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x74, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00,
    0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
];
/// The EXACT `ConsumerProtocolAssignment` bytes that cp-kafka 7.4.0 returned
/// for that member, in the fixture field `member_assignment_hex`: version
/// `i16=3`, topic `"t"`, partitions `[0, 1]`, and `userData=null`.
const REAL_KAFKA_ASSIGNMENT: &[u8] = &[
    0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x74, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff,
];

async fn start_broker() -> (crabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    let handle = Broker::start(config).await.expect("broker must start");
    let bootstrap = handle.listen_addr().to_string();
    (handle, bootstrap, tempdir)
}

fn join_request(group_id: &str, member_id: &str, metadata: &'static [u8]) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id: group_id.to_string(),
        session_timeout_ms: 10_000,
        rebalance_timeout_ms: 30_000,
        member_id: member_id.to_string(),
        group_instance_id: None,
        protocol_type: "consumer".to_string(),
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".to_string(),
            metadata: Bytes::from_static(metadata),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// A single-member classic consumer group. The test drives `JoinGroup`,
/// including the two-step `MEMBER_ID_REQUIRED` flow, then a leader
/// `SyncGroup`, then asserts that `DescribeGroups` reports the protocol name
/// and the per-member metadata.
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
        .send(join_request(group_id, "", KNOWN_METADATA))
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
        JOIN_GROUP_TIMEOUT,
        client.send(join_request(group_id, &member_id, KNOWN_METADATA)),
    )
    .await
    .expect("second JoinGroup timed out")
    .expect("second JoinGroup must round-trip");
    check!(
        r2.error_code == ERR_NONE,
        "second JoinGroup must succeed, got {r2:?}"
    );
    check!(
        r2.protocol_name.as_deref() == Some("range"),
        "second JoinGroup must select protocol 'range', got {r2:?}"
    );
    check!(
        r2.leader.as_str() == member_id.as_str(),
        "second JoinGroup must elect the lone member as leader, got {r2:?}"
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

    assert_described_group(&resp);
}

/// cp and JVM cross-validation. The test drives the SAME classic flow, but
/// with the EXACT `ConsumerProtocolSubscription` and
/// `ConsumerProtocolAssignment` bytes that a real `RangeAssignor`
/// console-consumer exchanged with
/// `mirror.gcr.io/confluentinc/cp-kafka:7.4.0`. `describe_groups_jvm.rs`
/// captured them into `real_kafka_classic.json`.
///
/// Crabka's `DescribeGroups` must reproduce real Kafka's semantics:
/// `protocol_type == "consumer"`, `protocol_data == "range"`, and a byte-exact
/// `member_metadata` echo of the realistic subscription, not only of the
/// arbitrary blob that the test above pins.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_groups_matches_real_kafka_range_subscription() {
    let (handle, bootstrap, _tempdir) = start_broker().await;
    let group_id = "cg-describe-real-kafka";
    let client = Client::builder()
        .bootstrap(bootstrap)
        .client_id("describe-real-kafka-member")
        .build()
        .await
        .expect("client build");

    // JoinGroup two-step, supplying the REAL captured subscription bytes.
    let r1 = client
        .send(join_request(group_id, "", REAL_KAFKA_SUBSCRIPTION))
        .await
        .expect("first JoinGroup must round-trip");
    assert!(
        r1.error_code == ERR_MEMBER_ID_REQUIRED,
        "first JoinGroup must return MEMBER_ID_REQUIRED (79), got {r1:?}"
    );
    let member_id = r1.member_id;

    let r2 = tokio::time::timeout(
        JOIN_GROUP_TIMEOUT,
        client.send(join_request(group_id, &member_id, REAL_KAFKA_SUBSCRIPTION)),
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
    let generation_id = r2.generation_id;

    // SyncGroup: leader supplies the REAL captured assignment bytes.
    let r3 = client
        .send(SyncGroupRequest {
            group_id: group_id.to_string(),
            generation_id,
            member_id: member_id.clone(),
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            assignments: vec![SyncGroupRequestAssignment {
                member_id: member_id.clone(),
                assignment: Bytes::from_static(REAL_KAFKA_ASSIGNMENT),
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

    let resp = client
        .send(DescribeGroupsRequest {
            groups: vec![group_id.to_string()],
            ..Default::default()
        })
        .await
        .expect("DescribeGroups must round-trip");
    handle.shutdown().await;

    let g = &resp.groups[0];
    // Real-Kafka authority (from real_kafka_classic.json).
    check!(
        g.error_code == ERR_NONE,
        "DescribeGroups must match real Kafka's authority (error-free), got {g:?}"
    );
    check!(
        g.protocol_type.as_str() == "consumer",
        "DescribeGroups must match real Kafka's authority (protocol_type 'consumer'), got {g:?}"
    );
    check!(
        g.protocol_data.as_str() == "range",
        "DescribeGroups must match real Kafka's authority (selected assignor 'range'), got {g:?}"
    );
    let m = &g.members[0];
    assert!(
        m.member_metadata.as_ref() == REAL_KAFKA_SUBSCRIPTION,
        "member_metadata must be the byte-exact real-Kafka ConsumerProtocolSubscription, got {:02x?}",
        m.member_metadata.as_ref()
    );
    assert!(
        m.member_assignment.as_ref() == REAL_KAFKA_ASSIGNMENT,
        "member_assignment must be the byte-exact real-Kafka ConsumerProtocolAssignment, got {:02x?}",
        m.member_assignment.as_ref()
    );
}
