//! End-to-end integration tests for KIP-1071 streams-group membership (the
//! Streams Rebalance Protocol), driven against an in-process Crabka broker
//! through `crabka-client-core`.
//!
//! The typed client works because `ApiVersions` advertises `api_keys` 88/89.
//! `StreamsGroupHeartbeatRequest` and `StreamsGroupDescribeRequest` implement
//! `ProtocolRequest`, so `client.send(req)` returns the typed response and
//! exercises the real wire path. Both streams RPCs are MIN=MAX=0, so the client
//! negotiates v0.
//!
//! Unlike share groups, the streams heartbeat handler gates on BOTH the
//! finalized `streams.version >= 1` feature (KIP-1071 early access) AND the
//! `streams_group.enable` config kill-switch, which is true by default in
//! `BrokerConfig::for_tests`. Every test therefore finalizes `streams.version`
//! to level 1 with `UpdateFeatures` before it issues streams RPCs.

use std::{sync::Arc, time::Duration};

use assert2::{assert, check};
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::owned::{
    common::streams_group_heartbeat_request::{
        task_ids::TaskIds as ReqTaskIds, topic_info::TopicInfo,
    },
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    list_groups_request::ListGroupsRequest,
    streams_group_describe_request::StreamsGroupDescribeRequest,
    streams_group_heartbeat_request::{StreamsGroupHeartbeatRequest, Subtopology, Topology},
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};

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

/// Finalize `streams.version` to level 1 so the heartbeat and describe handlers
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

/// A single-subtopology topology that subscribes to one source topic, with the
/// supplied changelog topics. An empty list means stateless.
fn topology(source_topic: &str, changelogs: Vec<TopicInfo>) -> Topology {
    Topology {
        epoch: 0,
        subtopologies: vec![Subtopology {
            subtopology_id: "0".into(),
            source_topics: vec![source_topic.into()],
            state_changelog_topics: changelogs,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// First-join heartbeat. It sends an empty member id, so the server mints one,
/// epoch 0, a process id, a rebalance timeout, and the supplied topology.
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

/// Follow-up heartbeat. It sends a known member id and its current epoch, and
/// it echoes back the owned active tasks, as a steady-state member does.
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

/// Sum of all active-task partitions in a heartbeat response.
fn active_partition_count(resp: &StreamsGroupHeartbeatResponse) -> usize {
    resp.active_tasks
        .as_ref()
        .map_or(0, |v| v.iter().map(|t| t.partitions.len()).sum())
}

/// Active-task partitions for a given subtopology id, sorted.
fn active_partitions_for(resp: &StreamsGroupHeartbeatResponse, sub: &str) -> Vec<i32> {
    let mut parts: Vec<i32> = resp
        .active_tasks
        .as_ref()
        .map(|v| {
            v.iter()
                .filter(|t| t.subtopology_id == sub)
                .flat_map(|t| t.partitions.clone())
                .collect()
        })
        .unwrap_or_default();
    parts.sort_unstable();
    parts
}

/// The response status codes. In the KIP-1071 status enum, 3 is
/// `MISSING_INTERNAL_TOPICS`.
fn status_codes(resp: &StreamsGroupHeartbeatResponse) -> Vec<i8> {
    resp.status
        .as_ref()
        .map(|v| v.iter().map(|s| s.status_code).collect())
        .unwrap_or_default()
}

async fn describe(
    client: &Client,
    group: &str,
) -> crabka_protocol::owned::streams_group_describe_response::StreamsGroupDescribeResponse {
    client
        .send(StreamsGroupDescribeRequest {
            group_ids: vec![group.into()],
            include_authorized_operations: false,
            ..Default::default()
        })
        .await
        .expect("StreamsGroupDescribe")
}

/// Drive a single member to its first join, then re-heartbeat until convergence
/// returns. The returned tuple is `(member_id, last_response)`.
async fn join_and_converge(
    client: &Client,
    group: &str,
    topo: Topology,
    want_active: usize,
    tries: usize,
) -> (String, StreamsGroupHeartbeatResponse) {
    // First join. Tolerate a transient coordinator-load on the very first call.
    let mut resp = client
        .send(first_join(group, topo))
        .await
        .expect("first heartbeat");
    let mut member_id = resp.member_id.clone();

    for _ in 0..tries {
        // COORDINATOR_LOAD_IN_PROGRESS (14): retry the first join.
        if resp.error_code == 14 {
            resp = client
                .send(first_join(group, topology("", vec![])))
                .await
                .expect("retry first heartbeat");
            member_id = resp.member_id.clone();
            continue;
        }
        assert!(resp.error_code == 0, "heartbeat error: {resp:?}");
        if active_partition_count(&resp) >= want_active {
            break;
        }
        // intentional: backoff between heartbeats while polling the RPC response
        // for active-task-assignment convergence. The assignment is coordinator-
        // local state that is not reflected in the metadata image and exposes no
        // metric/awaiter, so a bounded re-heartbeat loop is the only observer.
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
            .expect("follow-up heartbeat");
        member_id = resp.member_id.clone();
    }
    (member_id, resp)
}

/// A lone stateless member joins a 2-partition topic and is assigned both tasks
/// for the single subtopology.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stateless_single_member_converges() {
    let (_b, bootstrap, _dir) = boot().await;
    let client = connect(&bootstrap).await;
    finalize_streams_version(&client).await;
    create_topic(&client, "streams-input", 2).await;

    let (member_id, resp) = join_and_converge(
        &client,
        "streams-app-1",
        topology("streams-input", vec![]),
        2,
        10,
    )
    .await;

    check!(resp.error_code == 0, "heartbeat error: {resp:?}");
    check!(!member_id.is_empty(), "broker must mint a member id");
    check!(
        resp.member_epoch >= 1,
        "first join advances the member epoch, got {}",
        resp.member_epoch
    );
    // The single member owns both partitions of subtopology "0".
    check!(
        active_partition_count(&resp) == 2,
        "lone member must own both input partitions, got {:?}",
        resp.active_tasks
    );
    check!(
        active_partitions_for(&resp, "0") == vec![0, 1],
        "subtopology 0 must be assigned partitions [0, 1], got {:?}",
        resp.active_tasks
    );
}

/// A stateful subtopology, that is, one with a state-changelog topic, drives
/// the broker to auto-create the changelog internal topic. Once that topic
/// exists, the member converges with no `MISSING_INTERNAL_TOPICS` status, which
/// is status code 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stateful_member_triggers_internal_topic_creation() {
    let (broker, bootstrap, _dir) = boot().await;
    let client = connect(&bootstrap).await;
    finalize_streams_version(&client).await;
    create_topic(&client, "sk-input", 1).await;

    let changelog = TopicInfo {
        name: "app-store-changelog".into(),
        partitions: 0, // broker derives from the subtopology's task count
        replication_factor: 1,
        topic_configs: vec![],
        ..Default::default()
    };
    let topo = topology("sk-input", vec![changelog]);

    // First join. The very first reconcile may emit MISSING_INTERNAL_TOPICS (3)
    // because the changelog is created asynchronously and a re-read of the image
    // may not yet observe it; retry until the active task lands with no
    // missing-internal-topics status.
    let mut resp = client
        .send(first_join("streams-app-2", topo.clone()))
        .await
        .expect("first heartbeat");
    let mut member_id = resp.member_id.clone();
    let mut converged = false;
    for _ in 0..15 {
        if resp.error_code == 14 {
            // COORDINATOR_LOAD_IN_PROGRESS: retry the first join with the topology.
            resp = client
                .send(first_join("streams-app-2", topo.clone()))
                .await
                .expect("retry first heartbeat");
            member_id = resp.member_id.clone();
            continue;
        }
        assert!(resp.error_code == 0, "heartbeat error: {resp:?}");
        let missing_internal = status_codes(&resp).contains(&3);
        if active_partitions_for(&resp, "0") == vec![0] && !missing_internal {
            converged = true;
            break;
        }
        // intentional: backoff between heartbeats while polling the RPC response
        // for active-task assignment plus clearing of the MISSING_INTERNAL_TOPICS
        // status. This convergence is coordinator-local; it has no metadata-image
        // signal or metric, so a bounded re-heartbeat loop is the only observer.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let active = resp.active_tasks.clone().map(|v| {
            v.into_iter()
                .map(|t| ReqTaskIds {
                    subtopology_id: t.subtopology_id,
                    partitions: t.partitions,
                    ..Default::default()
                })
                .collect()
        });
        // Repeat the topology so the reconcile keeps the changelog requirement.
        let mut hb = follow_up("streams-app-2", &member_id, resp.member_epoch, active);
        hb.topology = Some(topo.clone());
        resp = client.send(hb).await.expect("follow-up heartbeat");
        member_id = resp.member_id.clone();
    }

    assert!(
        converged,
        "member never converged to active task [0] with no MISSING_INTERNAL_TOPICS; \
         last response: {resp:?}"
    );
    assert!(
        !status_codes(&resp).contains(&3),
        "no MISSING_INTERNAL_TOPICS (3) status once converged, got {:?}",
        resp.status
    );

    // The changelog internal topic must now exist in the controller image with
    // one partition (matching the single-partition source / subtopology task
    // count).
    let image = broker.controller_image_for_test();
    let changelog_rec = image.topic("app-store-changelog");
    let changelog_rec = changelog_rec.unwrap_or_else(|| {
        panic!(
            "changelog topic 'app-store-changelog' must be auto-created; topics present: {:?}",
            image.topics().map(|t| &t.name).collect::<Vec<_>>()
        )
    });
    assert!(
        changelog_rec.partitions == 1,
        "changelog topic must have 1 partition, got {}",
        changelog_rec.partitions
    );
}

/// After a member joins, `StreamsGroupDescribe` reports exactly one group row
/// for the group id with a clean error code, the member present, and a sane
/// group-state string.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_returns_the_group() {
    let (_b, bootstrap, _dir) = boot().await;
    let client = connect(&bootstrap).await;
    finalize_streams_version(&client).await;
    create_topic(&client, "desc-input", 2).await;

    let (member_id, resp) = join_and_converge(
        &client,
        "streams-app-3",
        topology("desc-input", vec![]),
        2,
        10,
    )
    .await;
    assert!(resp.error_code == 0, "join error: {resp:?}");
    assert!(!member_id.is_empty());

    let desc = describe(&client, "streams-app-3").await;
    assert!(
        desc.groups.len() == 1,
        "expected exactly one described group, got {}",
        desc.groups.len()
    );
    let g = &desc.groups[0];
    check!(g.error_code == 0, "describe error: {:?}", g.error_code);
    check!(
        g.group_id == "streams-app-3",
        "described group id mismatch: {:?}",
        g.group_id
    );
    check!(
        !g.members.is_empty(),
        "described group must list the joined member"
    );
    check!(
        g.members.iter().any(|m| m.member_id == member_id),
        "described group must contain member {member_id}, got {:?}",
        g.members.iter().map(|m| &m.member_id).collect::<Vec<_>>()
    );
    check!(
        !g.group_state.is_empty(),
        "group_state must be a non-empty phase string, got {:?}",
        g.group_state
    );
}

/// A member that has joined can leave with `member_epoch == -1`. The leave
/// succeeds, and a later Describe shows the group without that member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leave_removes_member() {
    let (_b, bootstrap, _dir) = boot().await;
    let client = connect(&bootstrap).await;
    finalize_streams_version(&client).await;
    create_topic(&client, "leave-input", 2).await;

    let (member_id, resp) = join_and_converge(
        &client,
        "streams-app-4",
        topology("leave-input", vec![]),
        2,
        10,
    )
    .await;
    assert!(resp.error_code == 0, "join error: {resp:?}");
    assert!(!member_id.is_empty());

    // Leave: member_epoch == -1.
    let leave = client
        .send(follow_up("streams-app-4", &member_id, -1, None))
        .await
        .expect("leave heartbeat");
    assert!(leave.error_code == 0, "leave failed: {leave:?}");

    // The group is retained (Empty) but the member is gone.
    let desc = describe(&client, "streams-app-4").await;
    assert!(
        desc.groups.len() == 1,
        "group row still present after leave, got {}",
        desc.groups.len()
    );
    let g = &desc.groups[0];
    assert!(
        g.error_code == 0,
        "retained group describe error: {:?}",
        g.error_code
    );
    assert!(
        !g.members.iter().any(|m| m.member_id == member_id),
        "left member {member_id} must be gone, got {:?}",
        g.members.iter().map(|m| &m.member_id).collect::<Vec<_>>()
    );
}

/// `ListGroups` surfaces a live streams group with `group_type = "streams"`
/// and honors `types_filter = ["streams"]`. That is the exact path the JVM
/// `kafka-streams-groups.sh` `AdminClient` uses with
/// `listGroups(typesFilter=[Streams])` before it issues
/// `StreamsGroupDescribe`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_groups_surfaces_streams_group() {
    let (_b, bootstrap, _dir) = boot().await;
    let client = connect(&bootstrap).await;
    finalize_streams_version(&client).await;
    create_topic(&client, "list-input", 2).await;

    let (_member_id, resp) = join_and_converge(
        &client,
        "streams-app-5",
        topology("list-input", vec![]),
        2,
        10,
    )
    .await;
    assert!(resp.error_code == 0, "join error: {resp:?}");

    // Filtered list, as the JVM streams-groups admin tool issues it.
    let listed = client
        .send(ListGroupsRequest {
            types_filter: vec!["streams".into()],
            ..Default::default()
        })
        .await
        .expect("ListGroups");
    assert!(listed.error_code == 0, "ListGroups error: {listed:?}");
    let g = listed
        .groups
        .iter()
        .find(|g| g.group_id == "streams-app-5")
        .unwrap_or_else(|| panic!("streams group not listed: {:?}", listed.groups));
    assert!(
        g.group_type == "streams",
        "group_type must be 'streams', got {:?}",
        g.group_type
    );

    // A non-streams type filter must exclude it.
    let consumer_only = client
        .send(ListGroupsRequest {
            types_filter: vec!["consumer".into()],
            ..Default::default()
        })
        .await
        .expect("ListGroups consumer");
    assert!(
        !consumer_only
            .groups
            .iter()
            .any(|g| g.group_id == "streams-app-5"),
        "streams group must not appear under a consumer type filter"
    );
}
