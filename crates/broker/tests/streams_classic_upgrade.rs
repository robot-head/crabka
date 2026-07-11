//! KIP-1071 classic→streams cold upgrade integration tests.
//!
//! Verifies that a `StreamsGroupHeartbeat` for a **drained** classic group
//! converts it in place (committed offsets survive) and that a classic group
//! with **live members** is rejected with `GROUP_ID_NOT_FOUND` (69).

use std::{sync::Arc, time::Duration};

use assert2::assert;
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        common::streams_group_heartbeat_request::task_ids::TaskIds as ReqTaskIds,
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
        leave_group_request::{LeaveGroupRequest, MemberIdentity},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        offset_commit_request::{
            OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_fetch_request::{
            OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopics,
        },
        streams_group_heartbeat_request::{StreamsGroupHeartbeatRequest, Subtopology, Topology},
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
        sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
        update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
    },
    primitives::uuid::Uuid as WireUuid,
};

// ── error codes ──────────────────────────────────────────────────────────────
const ERR_NONE: i16 = 0;
const ERR_MEMBER_ID_REQUIRED: i16 = 79;
const ERR_GROUP_ID_NOT_FOUND: i16 = 69;

// ── boot / connect helpers ────────────────────────────────────────────────────

async fn boot() -> (crabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn connect(bootstrap: &str) -> Arc<Client> {
    Arc::new(
        Client::builder()
            .bootstrap(bootstrap)
            .client_id("c1")
            .build()
            .await
            .unwrap(),
    )
}

async fn assert_committed_offset(client: &Client, topic_id: WireUuid, expected: i64) {
    let response = client
        .send(OffsetFetchRequest {
            groups: vec![OffsetFetchRequestGroup {
                group_id: "g".into(),
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: "in".into(),
                    topic_id,
                    partition_indexes: vec![0],
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("OffsetFetch");
    let group = response
        .groups
        .iter()
        .find(|g| g.group_id == "g")
        .expect("group g");
    let topic = group
        .topics
        .iter()
        .find(|t| t.topic_id == topic_id)
        .expect("topic in");
    let partition = topic.partitions.first().expect("partition 0");
    assert!(
        partition.error_code == ERR_NONE,
        "OffsetFetch failed: {partition:?}"
    );
    assert!(
        partition.committed_offset == expected,
        "committed offset was not preserved"
    );
}

async fn create_topic(client: &Client, topic: &str, partitions: i32) {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "topic create failed: {resp:?}"
    );
}

/// Finalize `streams.version` to level 1 so the heartbeat/describe handlers
/// stop returning `UNSUPPORTED_VERSION`. `upgrade_type: 1` is UPGRADE.
async fn finalize_streams_version(client: &Client) {
    let resp = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "streams.version".into(),
                max_version_level: 1,
                upgrade_type: 1,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    assert!(
        resp.error_code == 0,
        "streams.version finalize failed: {resp:?}"
    );
}

async fn topic_id_for(client: &Client, name: &str) -> WireUuid {
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

// ── classic group helpers ─────────────────────────────────────────────────────

fn join_request(group_id: &str, member_id: &str) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id: group_id.to_string(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 30_000,
        member_id: member_id.to_string(),
        group_instance_id: None,
        protocol_type: "consumer".to_string(),
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".to_string(),
            metadata: Bytes::from_static(b""),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Drive the `JoinGroup` two-step (`MEMBER_ID_REQUIRED` + re-join) then `SyncGroup`.
/// Returns `(member_id, generation_id)`. The caller is the sole member so it
/// is also the leader and supplies a trivial self-assignment in `SyncGroup`.
async fn classic_join_sync(client: &Client, group_id: &str) -> (String, i32) {
    // Round 1: empty member_id → broker mints one and returns MEMBER_ID_REQUIRED.
    let r1 = tokio::time::timeout(
        Duration::from_secs(5),
        client.send(join_request(group_id, "")),
    )
    .await
    .expect("JoinGroup1 timeout")
    .expect("JoinGroup1");
    assert!(
        r1.error_code == ERR_MEMBER_ID_REQUIRED,
        "expected MEMBER_ID_REQUIRED, got {r1:?}"
    );
    let member_id = r1.member_id.clone();
    assert!(!member_id.is_empty());

    // Round 2: rejoin with assigned member_id — broker blocks for the
    // initial-rebalance-delay then returns as sole leader.
    let r2 = tokio::time::timeout(
        Duration::from_secs(10),
        client.send(join_request(group_id, &member_id)),
    )
    .await
    .expect("JoinGroup2 timeout")
    .expect("JoinGroup2");
    assert!(
        r2.error_code == ERR_NONE,
        "second JoinGroup must succeed, got {r2:?}"
    );
    let generation_id = r2.generation_id;

    // SyncGroup: sole leader supplies its own assignment.
    let r3 = client
        .send(SyncGroupRequest {
            group_id: group_id.to_string(),
            generation_id,
            member_id: member_id.clone(),
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            assignments: vec![SyncGroupRequestAssignment {
                member_id: member_id.clone(),
                assignment: Bytes::from_static(b""),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("SyncGroup");
    assert!(
        r3.error_code == ERR_NONE,
        "SyncGroup must succeed, got {r3:?}"
    );

    (member_id, generation_id)
}

// ── streams helpers ───────────────────────────────────────────────────────────

fn topology(source_topic: &str) -> Topology {
    Topology {
        epoch: 0,
        subtopologies: vec![Subtopology {
            subtopology_id: "0".into(),
            source_topics: vec![source_topic.into()],
            state_changelog_topics: vec![],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn first_join(group: &str, topo: Topology) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: group.into(),
        member_id: String::new(),
        member_epoch: 0,
        process_id: Some("p1".into()),
        rebalance_timeout_ms: 30_000,
        topology: Some(topo),
        ..Default::default()
    }
}

fn follow_up(
    group: &str,
    member_id: &str,
    epoch: i32,
    active: Option<Vec<ReqTaskIds>>,
) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: group.into(),
        member_id: member_id.into(),
        member_epoch: epoch,
        active_tasks: active,
        ..Default::default()
    }
}

/// Drive a single streams member to convergence (at least `want_active`
/// active-task partitions). Returns `(member_id, last_response)`.
async fn streams_join_and_converge(
    client: &Client,
    group: &str,
    topo: Topology,
    want_active: usize,
    tries: usize,
) -> (String, StreamsGroupHeartbeatResponse) {
    let mut resp = client
        .send(first_join(group, topo))
        .await
        .expect("first streams heartbeat");
    let mut member_id = resp.member_id.clone();

    for _ in 0..tries {
        if resp.error_code == 14 {
            resp = client
                .send(first_join(group, topology("")))
                .await
                .expect("retry streams heartbeat");
            member_id = resp.member_id.clone();
            continue;
        }
        if resp.error_code != ERR_NONE {
            break;
        }
        let total: usize = resp
            .active_tasks
            .as_ref()
            .map_or(0, |v| v.iter().map(|t| t.partitions.len()).sum());
        if total >= want_active {
            break;
        }
        // intentional: retry/backoff between bounded streams-heartbeat RPC polls;
        // task-assignment convergence is streams-coordinator-local state, not in
        // the metadata image and exposed by no metric — no awaiter can observe it.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let active = resp.active_tasks.clone().map(|v| {
            v.into_iter()
                .map(|t| ReqTaskIds {
                    subtopology_id: t.subtopology_id,
                    partitions: t.partitions,
                    ..Default::default()
                })
                .collect()
        });
        resp = client
            .send(follow_up(group, &member_id, resp.member_epoch, active))
            .await
            .expect("follow-up streams heartbeat");
        member_id = resp.member_id.clone();
    }
    (member_id, resp)
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// A drained classic group (zero live members, committed offsets retained) is
/// converted to a streams group when a `StreamsGroupHeartbeat` arrives.
/// Committed offsets survive the flip and are readable via `OffsetFetch`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drained_classic_group_converts_and_preserves_offsets() {
    let (broker, bootstrap, _dir) = boot().await;

    // Separate connections: JoinGroup parks the classic-protocol client for
    // the full rebalance-delay; the streams heartbeat must not be queued
    // behind it on the same socket.
    let classic_client = connect(&bootstrap).await;
    let streams_client = connect(&bootstrap).await;

    finalize_streams_version(&classic_client).await;
    create_topic(&classic_client, "in", 1).await;
    let topic_id = topic_id_for(&classic_client, "in").await;

    // ── Phase 1: form a classic group, commit offset 42, then leave. ──
    let (member_id, generation_id) = classic_join_sync(&classic_client, "g").await;

    // Commit offset 42 for ("in", 0).
    let cr = classic_client
        .send(OffsetCommitRequest {
            group_id: "g".into(),
            generation_id_or_member_epoch: generation_id,
            member_id: member_id.clone(),
            topics: vec![OffsetCommitRequestTopic {
                name: "in".into(),
                topic_id,
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: 42,
                    committed_leader_epoch: 0,
                    committed_metadata: Some(String::new()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("OffsetCommit");
    assert!(
        cr.topics[0].partitions[0].error_code == ERR_NONE,
        "OffsetCommit failed: {cr:?}"
    );

    // Leave — group is now drained (no live members).
    // Use the `members` field (v3+ shape) since the client negotiates the
    // max supported version (v5), which uses `members` not `member_id`.
    let lr = classic_client
        .send(LeaveGroupRequest {
            group_id: "g".into(),
            member_id: member_id.clone(),
            members: vec![MemberIdentity {
                member_id: member_id.clone(),
                group_instance_id: None,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("LeaveGroup");
    assert!(lr.error_code == ERR_NONE, "LeaveGroup failed: {lr:?}");

    // Precondition: the group must be Classic-typed.
    broker
        .wait_until_group_type("g", crabka_broker::coordinator::unified::GroupType::Classic)
        .await;
    assert!(
        broker.group_type_for_test("g")
            == Some(crabka_broker::coordinator::unified::GroupType::Classic),
        "precondition: group_type must be Classic before upgrade, got {:?}",
        broker.group_type_for_test("g")
    );

    // ── Phase 2: StreamsGroupHeartbeat for the same group_id → converge. ──
    let (_, resp) = streams_join_and_converge(
        &streams_client,
        "g",
        topology("in"),
        1, // 1 partition
        15,
    )
    .await;
    assert!(
        resp.error_code == ERR_NONE,
        "streams heartbeat after conversion must succeed, got {resp:?}"
    );

    // Group must now be Streams-typed.
    broker
        .wait_until_group_type("g", crabka_broker::coordinator::unified::GroupType::Streams)
        .await;
    assert!(
        broker.group_type_for_test("g")
            == Some(crabka_broker::coordinator::unified::GroupType::Streams),
        "group_type must be Streams after upgrade, got {:?}",
        broker.group_type_for_test("g")
    );

    // ── Phase 3: committed offsets survive the flip. ──
    assert_committed_offset(&streams_client, topic_id, 42).await;
}

/// A classic group with a **live** member rejects the `StreamsGroupHeartbeat`
/// with `GROUP_ID_NOT_FOUND` (69) and remains Classic-typed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_group_with_live_member_rejects_streams_heartbeat() {
    let (broker, bootstrap, _dir) = boot().await;

    let classic_client = connect(&bootstrap).await;
    let streams_client = connect(&bootstrap).await;

    finalize_streams_version(&classic_client).await;
    create_topic(&classic_client, "in2", 1).await;

    // ── Phase 1: join as classic consumer and STAY (no leave). ──
    // First-round JoinGroup (gets member_id back).
    let r1 = tokio::time::timeout(
        Duration::from_secs(5),
        classic_client.send(join_request("g2", "")),
    )
    .await
    .expect("JoinGroup1 timeout")
    .expect("JoinGroup1");
    assert!(
        r1.error_code == ERR_MEMBER_ID_REQUIRED,
        "expected MEMBER_ID_REQUIRED, got {r1:?}"
    );
    let member_id = r1.member_id.clone();

    // Second-round JoinGroup — parks in the rebalance-delay wait. We spawn it
    // so the test continues immediately without waiting for the park to return.
    // The member stays joined (no leave) so the group has a live member.
    let join_bootstrap = bootstrap.clone();
    let mid = member_id.clone();
    let _join_task = tokio::spawn(async move {
        let c = Client::builder()
            .bootstrap(&join_bootstrap)
            .client_id("classic-joiner")
            .build()
            .await
            .unwrap();
        let _ =
            tokio::time::timeout(Duration::from_secs(30), c.send(join_request("g2", &mid))).await;
    });

    // Wait for the member to land in the classic actor's member registry.
    broker.wait_until_classic_group_member_count("g2", 1).await;

    // Precondition: group must be Classic-typed.
    assert!(
        broker.group_type_for_test("g2")
            == Some(crabka_broker::coordinator::unified::GroupType::Classic),
        "precondition: group_type must be Classic, got {:?}",
        broker.group_type_for_test("g2")
    );

    // ── Phase 2: streams heartbeat for the same id must be rejected. ──
    let resp = streams_client
        .send(first_join("g2", topology("in2")))
        .await
        .expect("StreamsGroupHeartbeat");
    assert!(
        resp.error_code == ERR_GROUP_ID_NOT_FOUND,
        "streams heartbeat for classic group with live member must return \
         GROUP_ID_NOT_FOUND (69), got error_code={}",
        resp.error_code
    );

    // Group must STILL be Classic-typed (no flip).
    assert!(
        broker.group_type_for_test("g2")
            == Some(crabka_broker::coordinator::unified::GroupType::Classic),
        "group_type must remain Classic after rejected upgrade, got {:?}",
        broker.group_type_for_test("g2")
    );
}
