#![allow(clippy::pedantic)]

//! KIP-1071 streams→classic cold downgrade + admin type-awareness integration
//! tests (slice 2). A drained streams group converts to classic on a classic
//! JoinGroup (offsets preserved); a streams group with a live member rejects it;
//! and the admin handlers (List/Describe/Delete) respect the type lock.

use assert2::{assert, check};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

use crabka_broker::{BootstrapMode, Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::owned::common::streams_group_heartbeat_request::task_ids::TaskIds as ReqTaskIds;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::delete_groups_request::DeleteGroupsRequest;
use crabka_protocol::owned::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
use crabka_protocol::owned::leave_group_request::{LeaveGroupRequest, MemberIdentity};
use crabka_protocol::owned::list_groups_request::ListGroupsRequest;
use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
use crabka_protocol::owned::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use crabka_protocol::owned::offset_fetch_request::{
    OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopics,
};
use crabka_protocol::owned::streams_group_heartbeat_request::{
    StreamsGroupHeartbeatRequest, Subtopology, Topology,
};
use crabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse;
use crabka_protocol::owned::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
use crabka_protocol::owned::update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

// ── error codes ──────────────────────────────────────────────────────────────
const ERR_NONE: i16 = 0;
const ERR_MEMBER_ID_REQUIRED: i16 = 79;
const ERR_GROUP_ID_NOT_FOUND: i16 = 69;
const ERR_NON_EMPTY_GROUP: i16 = 68;

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

/// Drive the JoinGroup two-step (MEMBER_ID_REQUIRED + re-join) then SyncGroup.
/// Returns `(member_id, generation_id)`. The caller is the sole member so it
/// is also the leader and supplies a trivial self-assignment in SyncGroup.
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
            .map(|v| v.iter().map(|t| t.partitions.len()).sum())
            .unwrap_or(0);
        if total >= want_active {
            break;
        }
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

/// Send a streams LeaveGroup (member_epoch -1) so the group drains.
async fn streams_leave(client: &Client, group: &str, member_id: &str) {
    let _ = client
        .send(StreamsGroupHeartbeatRequest {
            group_id: group.into(),
            member_id: member_id.into(),
            member_epoch: -1,
            ..Default::default()
        })
        .await
        .expect("streams leave heartbeat");
}

/// Commit an offset via the "simple consumer" path (empty member_id) which
/// bypasses classic-member validation. Safe to use for a streams group because
/// the offset-home actor allows commits from unjoined clients (generation -1).
async fn commit_offset_simple(
    client: &Client,
    group_id: &str,
    topic: &str,
    topic_id: WireUuid,
    partition: i32,
    offset: i64,
) {
    let cr = client
        .send(OffsetCommitRequest {
            group_id: group_id.into(),
            generation_id_or_member_epoch: -1,
            member_id: String::new(),
            topics: vec![OffsetCommitRequestTopic {
                name: topic.into(),
                topic_id,
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: partition,
                    committed_offset: offset,
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
        "OffsetCommit (simple consumer) failed: {cr:?}"
    );
}

fn rejoin_config(log_dir: std::path::PathBuf) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir);
    cfg.bootstrap_mode = BootstrapMode::Rejoin;
    cfg
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// A drained streams group with a committed offset converts to classic on a
/// classic JoinGroup; the committed offset survives the flip.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drained_streams_group_downgrades_and_preserves_offsets() {
    let (broker, bootstrap, _dir) = boot().await;
    let streams_client = connect(&bootstrap).await;
    let classic_client = connect(&bootstrap).await;

    finalize_streams_version(&streams_client).await;
    create_topic(&streams_client, "in", 1).await;
    let topic_id = topic_id_for(&streams_client, "in").await;

    // ── Phase 1: form a streams group, commit offset 42, then leave. ──
    let (member_id, resp) =
        streams_join_and_converge(&streams_client, "g", topology("in"), 1, 15).await;
    let group_type = broker.group_type_for_test("g");
    let empty_waiter_timed_out = tokio::time::timeout(
        std::time::Duration::from_millis(75),
        broker.wait_until_streams_group_empty("g"),
    )
    .await
    .is_err();
    check!(
        resp.error_code == ERR_NONE,
        "streams member must converge without error (precondition for the downgrade): {resp:?}"
    );
    check!(
        group_type == Some(crabka_broker::coordinator::unified::GroupType::Streams),
        "streams member must converge on a Streams-typed group (precondition for the \
         downgrade): {resp:?}"
    );
    check!(
        empty_waiter_timed_out,
        "the streams-group-empty waiter must not complete while a member is live: {resp:?}"
    );

    // Commit offset 42 via the simple-consumer path (empty member_id, epoch
    // -1) — the streams offset-home actor allows commits from unjoined clients.
    // A commit using the live streams member_id would be rejected by the
    // classic actor's validate_commit (member not in classic state.members).
    commit_offset_simple(&streams_client, "g", "in", topic_id, 0, 42).await;

    // Leave so the streams group is drained.
    streams_leave(&streams_client, "g", &member_id).await;
    // Wait for the leave to propagate through the streams actor before the
    // classic JoinGroup triggers the streams→classic conversion.
    broker.wait_until_streams_group_empty("g").await;

    // ── Phase 2: classic JoinGroup for the same id → downgrade to classic. ──
    let (_cm, _gen) = classic_join_sync(&classic_client, "g").await;
    assert!(
        broker.group_type_for_test("g")
            == Some(crabka_broker::coordinator::unified::GroupType::Classic),
        "group_type must be Classic after downgrade, got {:?}",
        broker.group_type_for_test("g")
    );

    // ── Phase 3: committed offset survives the flip. ──
    let fr = classic_client
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
    let part = &fr.groups[0].topics[0].partitions[0];
    assert!(part.error_code == ERR_NONE, "OffsetFetch error: {part:?}");
    assert!(
        part.committed_offset == 42,
        "committed offset must survive classic↔streams downgrade, got {}",
        part.committed_offset
    );
}

/// A streams group with a LIVE member rejects a classic JoinGroup with
/// GROUP_ID_NOT_FOUND (69) and stays Streams-typed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streams_group_with_live_member_rejects_classic_join() {
    let (broker, bootstrap, _dir) = boot().await;
    let streams_client = connect(&bootstrap).await;
    let classic_client = connect(&bootstrap).await;

    finalize_streams_version(&streams_client).await;
    create_topic(&streams_client, "in2", 1).await;

    // Live streams member (converge, do NOT leave).
    let (_mid, resp) =
        streams_join_and_converge(&streams_client, "g2", topology("in2"), 1, 15).await;
    assert!(resp.error_code == ERR_NONE);
    assert!(
        broker.group_type_for_test("g2")
            == Some(crabka_broker::coordinator::unified::GroupType::Streams)
    );

    // Round-1 classic JoinGroup (empty member_id) must be rejected BEFORE the
    // MEMBER_ID_REQUIRED dance: the downgrade pre-step runs first.
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        classic_client.send(join_request("g2", "")),
    )
    .await
    .expect("JoinGroup timeout")
    .expect("JoinGroup");
    assert!(
        r.error_code == ERR_GROUP_ID_NOT_FOUND,
        "classic join for streams group with live member must return \
         GROUP_ID_NOT_FOUND (69), got {}",
        r.error_code
    );
    assert!(
        broker.group_type_for_test("g2")
            == Some(crabka_broker::coordinator::unified::GroupType::Streams),
        "group_type must remain Streams after rejected downgrade"
    );
}

/// After a classic→streams conversion (slice 1), the converted group is
/// reported as `streams` by ListGroups and is NOT deletable via the classic
/// path while the streams group has a live member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn converted_group_admin_views_respect_type_lock() {
    let (broker, bootstrap, _dir) = boot().await;
    let classic_client = connect(&bootstrap).await;
    let streams_client = connect(&bootstrap).await;

    finalize_streams_version(&classic_client).await;
    create_topic(&classic_client, "in3", 1).await;

    // Drain a classic group, then upgrade it to streams via a heartbeat.
    let (cm, _gen) = classic_join_sync(&classic_client, "g3").await;
    // Leave so the classic group is drained.
    let _ = classic_client
        .send(LeaveGroupRequest {
            group_id: "g3".into(),
            member_id: cm.clone(),
            members: vec![MemberIdentity {
                member_id: cm.clone(),
                group_instance_id: None,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("LeaveGroup");

    // Wait for the classic leave to propagate before upgrading to streams.
    broker.wait_until_group_empty("g3").await;

    let (_sm, hb) = streams_join_and_converge(&streams_client, "g3", topology("in3"), 1, 15).await;
    assert!(hb.error_code == ERR_NONE);
    assert!(
        broker.group_type_for_test("g3")
            == Some(crabka_broker::coordinator::unified::GroupType::Streams)
    );

    // ListGroups: the converted group appears exactly once, as `streams`.
    let lg = classic_client
        .send(ListGroupsRequest::default())
        .await
        .expect("ListGroups");
    let rows: Vec<_> = lg.groups.iter().filter(|g| g.group_id == "g3").collect();
    assert!(rows.len() == 1, "g3 listed once, got {}", rows.len());
    assert!(
        rows[0].group_type.eq_ignore_ascii_case("streams"),
        "g3 must be typed streams, got {:?}",
        rows[0].group_type
    );

    // DeleteGroups via the classic path must NOT remove the live streams group's
    // offset home: with a live streams member it is NON_EMPTY_GROUP.
    let dg = classic_client
        .send(DeleteGroupsRequest {
            groups_names: vec!["g3".into()],
            ..Default::default()
        })
        .await
        .expect("DeleteGroups");
    assert!(
        dg.results[0].error_code == ERR_NON_EMPTY_GROUP,
        "delete of a live streams group must be NON_EMPTY_GROUP, got {}",
        dg.results[0].error_code
    );
    assert!(
        broker.group_type_for_test("g3")
            == Some(crabka_broker::coordinator::unified::GroupType::Streams),
        "the streams group must survive the rejected delete"
    );
}

/// A streams→classic downgrade survives a broker restart: after replay the
/// group is Classic with its committed offset intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn downgrade_survives_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();
    let topic_id;
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir.clone()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let sc = connect(&bootstrap).await;
        let cc = connect(&bootstrap).await;
        finalize_streams_version(&sc).await;
        create_topic(&sc, "in4", 1).await;
        topic_id = topic_id_for(&sc, "in4").await;

        let (mid, resp) = streams_join_and_converge(&sc, "g4", topology("in4"), 1, 15).await;
        assert!(resp.error_code == ERR_NONE, "streams converge: {resp:?}");

        // Commit offset 42 via simple consumer path (see watch-item).
        commit_offset_simple(&sc, "g4", "in4", topic_id, 0, 42).await;

        // Leave to drain.
        streams_leave(&sc, "g4", &mid).await;
        // Wait for the streams leave to propagate before the downgrade JoinGroup.
        broker.wait_until_streams_group_empty("g4").await;

        // Downgrade: classic JoinGroup on drained streams group.
        let _ = classic_join_sync(&cc, "g4").await;
        assert!(
            broker.group_type_for_test("g4")
                == Some(crabka_broker::coordinator::unified::GroupType::Classic)
        );
        // Pre-shutdown flush: allow the raft log write for the classic
        // group-type record to be flushed to disk before we shut down.
        tokio::time::sleep(Duration::from_millis(300)).await;
        broker.shutdown().await;
    }
    {
        let broker = Broker::start(rejoin_config(log_dir)).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let cc = connect(&bootstrap).await;
        // Replay must reconstruct g4 as Classic (k15 tombstoned), offset intact.
        assert!(
            broker.group_type_for_test("g4")
                != Some(crabka_broker::coordinator::unified::GroupType::Streams),
            "group must not replay as Streams after downgrade"
        );
        let fr = cc
            .send(OffsetFetchRequest {
                groups: vec![OffsetFetchRequestGroup {
                    group_id: "g4".into(),
                    topics: Some(vec![OffsetFetchRequestTopics {
                        name: "in4".into(),
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
        assert!(
            fr.groups[0].topics[0].partitions[0].committed_offset == 42,
            "committed offset must survive downgrade + restart"
        );
    }
}
