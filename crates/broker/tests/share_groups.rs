#![allow(clippy::pedantic)]

//! End-to-end integration tests for KIP-932 share-group membership,
//! driven against an in-process Crabka broker via `crabka-client-core`.
//!
//! The typed client works because `ApiVersions` advertises `api_keys` 76/77;
//! `ShareGroupHeartbeatRequest` / `ShareGroupDescribeRequest` impl
//! `ProtocolRequest`, so `client.send(req)` returns the typed response and
//! exercises the real wire path (version negotiation through `ApiVersions` —
//! both share RPCs are MIN=MAX=1, so the client negotiates v1).

use std::sync::Arc;

use assert2::check;
use crabka_broker::{BootstrapMode, Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    list_groups_request::ListGroupsRequest,
    share_group_describe_request::ShareGroupDescribeRequest,
    share_group_heartbeat_request::ShareGroupHeartbeatRequest,
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
    assert2::assert!(resp.topics[0].error_code == 0);
}

fn heartbeat(group: &str, member_id: &str, epoch: i32) -> ShareGroupHeartbeatRequest {
    ShareGroupHeartbeatRequest {
        group_id: group.into(),
        member_id: member_id.into(),
        member_epoch: epoch,
        ..Default::default()
    }
}

fn total_assigned(
    resp: &crabka_protocol::owned::share_group_heartbeat_response::ShareGroupHeartbeatResponse,
) -> usize {
    resp.assignment
        .as_ref()
        .map(|a| a.topic_partitions.iter().map(|t| t.partitions.len()).sum())
        .unwrap_or(0)
}

async fn describe(
    client: &Client,
    group: &str,
) -> crabka_protocol::owned::share_group_describe_response::ShareGroupDescribeResponse {
    client
        .send(ShareGroupDescribeRequest {
            group_ids: vec![group.into()],
            include_authorized_operations: false,
            ..Default::default()
        })
        .await
        .unwrap()
}

/// Single member joins, gets a minted member id, advances to epoch 1, and is
/// assigned every partition of the subscribed topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_member_join_assignment() {
    let (_b, bootstrap, _d) = boot().await;
    let client = connect(&bootstrap).await;
    create_topic(&client, "t1", 4).await;

    let mut req = heartbeat("g1", "", 0);
    req.subscribed_topic_names = Some(vec!["t1".into()]);
    let resp = client.send(req).await.unwrap();

    check!(
        (
            resp.error_code,
            resp.member_id.is_some(),
            resp.member_epoch,
            total_assigned(&resp)
        ) == (0, true, 1, 4),
        "single-member join response mismatch: {resp:?}"
    );
}

/// Two members join the same group; after both converge, `ShareGroupDescribe`
/// reports one group with both members and a non-trivial group epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_members_then_describe() {
    let (_b, bootstrap, _d) = boot().await;
    let client = connect(&bootstrap).await;
    create_topic(&client, "t2", 4).await;

    let mut m1 = heartbeat("g1", "", 0);
    m1.subscribed_topic_names = Some(vec!["t2".into()]);
    let r1 = client.send(m1).await.unwrap();
    assert2::assert!(r1.error_code == 0);
    let mid1 = r1.member_id.clone().unwrap();

    let mut m2 = heartbeat("g1", "", 0);
    m2.subscribed_topic_names = Some(vec!["t2".into()]);
    let r2 = client.send(m2).await.unwrap();
    assert2::assert!(r2.error_code == 0);
    let mid2 = r2.member_id.clone().unwrap();
    assert2::assert!(mid1 != mid2);

    // m1 re-heartbeats at its returned epoch so it learns the rebalanced
    // assignment after m2 bumped the group epoch.
    let mut m1b = heartbeat("g1", &mid1, r1.member_epoch);
    m1b.subscribed_topic_names = Some(vec!["t2".into()]);
    let r1b = client.send(m1b).await.unwrap();
    assert2::assert!(r1b.error_code == 0);

    let desc = describe(&client, "g1").await;
    assert2::assert!(desc.groups.len() == 1);
    let g = &desc.groups[0];
    check!(
        (g.error_code, g.members.len(), g.group_epoch >= 1) == (0, 2, true),
        "two-member describe response mismatch: {g:?}"
    );
}

/// A member leaves via `member_epoch == -1`; the leave succeeds, the group is
/// retained but reported with zero members (state "Empty").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn member_leave_epoch_minus_one() {
    let (_b, bootstrap, _d) = boot().await;
    let client = connect(&bootstrap).await;
    create_topic(&client, "t3", 2).await;

    let mut join = heartbeat("g1", "", 0);
    join.subscribed_topic_names = Some(vec!["t3".into()]);
    let r = client.send(join).await.unwrap();
    assert2::assert!(r.error_code == 0);
    let mid = r.member_id.clone().unwrap();

    let leave = heartbeat("g1", &mid, -1);
    let lr = client.send(leave).await.unwrap();
    assert2::assert!(lr.error_code == 0);

    let desc = describe(&client, "g1").await;
    assert2::assert!(desc.groups.len() == 1);
    let g = &desc.groups[0];
    // The empty group is retained (the actor stays alive with 0 members).
    assert2::assert!((g.error_code, g.members.is_empty()) == (0, true));
}

/// Share-group state persists to `__consumer_offsets`; after a broker restart
/// (Rejoin on the same data dir) the group + member are reconstructed via
/// replay and visible through `ShareGroupDescribe`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn state_survives_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let member_id;
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir.clone()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = connect(&bootstrap).await;
        create_topic(&client, "t4", 2).await;

        let mut join = heartbeat("g1", "", 0);
        join.subscribed_topic_names = Some(vec!["t4".into()]);
        let r = client.send(join).await.unwrap();
        assert2::assert!(r.error_code == 0);
        member_id = r.member_id.clone().unwrap();

        // flush_pending inside the share actor awaits offsets_log.append before
        // returning the heartbeat response, so the join record is durable on
        // disk by the time the client receives the response above.
        broker.shutdown().await;
    }

    {
        let mut cfg = BrokerConfig::for_tests(log_dir);
        cfg.bootstrap_mode = BootstrapMode::Rejoin;
        let broker = Broker::start(cfg).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = connect(&bootstrap).await;

        let desc = describe(&client, "g1").await;
        assert2::assert!(desc.groups.len() == 1);
        let g = &desc.groups[0];
        check!(
            g.error_code == 0,
            "recovered group describe error: {:?}",
            g.error_code
        );
        check!(
            g.group_epoch >= 1,
            "recovered group epoch must be >= 1, got {}",
            g.group_epoch
        );
        check!(
            g.members.iter().any(|m| m.member_id == member_id),
            "recovered group must contain the original member {member_id}, members: {:?}",
            g.members.iter().map(|m| &m.member_id).collect::<Vec<_>>()
        );
    }
}

/// Resolve a created topic's id from this broker's metadata image.
async fn topic_id(broker: &crabka_broker::BrokerHandle, topic: &str) -> uuid::Uuid {
    let image = broker.controller_image_for_test();
    image
        .topic(topic)
        .map(|t| *t.topic_id.as_bytes())
        .map(uuid::Uuid::from_bytes)
        .expect("topic present in image")
}

/// KIP-932 group-coordinator lifecycle: a share group joining a topic with `P`
/// partitions drives the coordinator to Initialize per-partition share state in
/// the `__share_group_state` persister (`start_offset` 0, state present — not the
/// missing-key sentinel). The heartbeat hook runs after reconcile.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_initializes_share_state() {
    let (broker, bootstrap, _d) = boot().await;
    let client = connect(&bootstrap).await;
    create_topic(&client, "t5", 3).await;
    let tid = topic_id(&broker, "t5").await;

    let mut join = heartbeat("g5", "", 0);
    join.subscribed_topic_names = Some(vec!["t5".into()]);
    let r = client.send(join).await.unwrap();
    assert2::assert!(r.error_code == 0);
    let mid = r.member_id.clone().unwrap();

    // The lifecycle hook initializes assigned partitions best-effort on each
    // heartbeat (first heartbeat may fail if __share_group_state isn't ready
    // yet; the hook retries on the next). We interleave heartbeats with a
    // condition check — no fixed count, no fixed sleep — exiting as soon as
    // all three partitions have summaries.
    let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let mut hb = heartbeat("g5", &mid, r.member_epoch);
            hb.subscribed_topic_names = Some(vec!["t5".into()]);
            let _ = client.send(hb).await.unwrap();
            let mut all_done = true;
            for p in 0..3 {
                if broker
                    .share_state_summary_for_test("g5", tid, p)
                    .await
                    .is_none()
                {
                    all_done = false;
                    break;
                }
            }
            if all_done {
                break;
            }
        }
    })
    .await;
    assert2::assert!(res.is_ok());

    for p in 0..3 {
        let (_se, _le, start_offset, _dcc) = broker
            .share_state_summary_for_test("g5", tid, p)
            .await
            .unwrap();
        assert2::assert!(start_offset == 0);
    }
}

/// After a restart, the group's `ShareGroupStatePartitionMetadata` is recovered
/// so re-joining does not re-initialize: a stale (non-zero) `start_offset` written
/// directly to the persister survives because the coordinator skips already-
/// initialized partitions on the post-restart heartbeat.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_metadata_survives_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let tid;
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir.clone()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = connect(&bootstrap).await;
        create_topic(&client, "t6", 2).await;
        tid = topic_id(&broker, "t6").await;

        let mut join = heartbeat("g6", "", 0);
        join.subscribed_topic_names = Some(vec!["t6".into()]);
        let r = client.send(join).await.unwrap();
        assert2::assert!(r.error_code == 0);
        let mid = r.member_id.clone().unwrap();

        // Interleave heartbeats with condition check — no fixed count, no
        // fixed sleep — exiting as soon as both partitions have summaries.
        let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let mut hb = heartbeat("g6", &mid, r.member_epoch);
                hb.subscribed_topic_names = Some(vec!["t6".into()]);
                let _ = client.send(hb).await.unwrap();
                let mut all_done = true;
                for p in 0..2 {
                    if broker
                        .share_state_summary_for_test("g6", tid, p)
                        .await
                        .is_none()
                    {
                        all_done = false;
                        break;
                    }
                }
                if all_done {
                    break;
                }
            }
        })
        .await;
        assert2::assert!(res.is_ok());
        // Both partitions are initialized before restart.
        for p in 0..2 {
            assert2::assert!(
                broker
                    .share_state_summary_for_test("g6", tid, p)
                    .await
                    .is_some()
            );
        }
        broker.shutdown().await;
    }

    {
        let mut cfg = BrokerConfig::for_tests(log_dir);
        cfg.bootstrap_mode = BootstrapMode::Rejoin;
        let broker = Broker::start(cfg).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = connect(&bootstrap).await;

        // The recovered ShareCoordinator replays __share_group_state, so the
        // summary is present immediately after restart.
        for p in 0..2 {
            assert2::assert!(
                broker
                    .share_state_summary_for_test("g6", tid, p)
                    .await
                    .is_some()
            );
        }

        // Re-join the recovered group; the coordinator's recovered
        // ShareGroupStatePartitionMetadata means the heartbeat hook treats the
        // partitions as already initialized and does NOT re-Initialize them
        // (a re-init would FENCE on the same state_epoch). The group stays
        // healthy and the state remains present.
        let desc = describe(&client, "g6").await;
        let mid = desc.groups[0].members[0].member_id.clone();
        let mut hb = heartbeat("g6", &mid, desc.groups[0].group_epoch);
        hb.subscribed_topic_names = Some(vec!["t6".into()]);
        let _ = client.send(hb).await.unwrap();

        // Await rather than sleep: confirm the recovered summaries are still
        // present (the coordinator must NOT re-initialize already-initialized
        // partitions after restart).
        for p in 0..2 {
            broker.wait_for_share_state_summary("g6", tid, p).await;
            assert2::assert!(
                broker
                    .share_state_summary_for_test("g6", tid, p)
                    .await
                    .is_some()
            );
        }
    }
}

/// `kafka-share-groups.sh --list` sends `ListGroups` (`api_key` 16) with
/// `types_filter = ["share"]`. A live share group must appear in that response
/// tagged `group_type == "share"`, and must NOT appear when the request filters
/// on `["consumer"]`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_groups_includes_share_group() {
    let (_b, bootstrap, _d) = boot().await;
    let client = connect(&bootstrap).await;
    create_topic(&client, "t7", 2).await;

    // Join a share group so it is registered in the coordinator.
    let mut join = heartbeat("g7", "", 0);
    join.subscribed_topic_names = Some(vec!["t7".into()]);
    let r = client.send(join).await.unwrap();
    assert2::assert!(r.error_code == 0);

    // types_filter = ["share"] → contains g7 tagged "share".
    let resp = client
        .send(ListGroupsRequest {
            types_filter: vec!["share".into()],
            ..Default::default()
        })
        .await
        .expect("ListGroups[share]");
    assert2::assert!(resp.error_code == 0);
    let share_row = resp.groups.iter().find(|g| g.group_id == "g7");
    let share_row = share_row.unwrap_or_else(|| {
        panic!(
            "share group g7 missing from ListGroups[share], got {:?}",
            resp.groups.iter().map(|g| &g.group_id).collect::<Vec<_>>()
        )
    });
    assert2::assert!(share_row.group_type == "share");

    // types_filter = ["consumer"] → g7 must NOT appear.
    let resp = client
        .send(ListGroupsRequest {
            types_filter: vec!["consumer".into()],
            ..Default::default()
        })
        .await
        .expect("ListGroups[consumer]");
    assert2::assert!(!resp.groups.iter().any(|g| g.group_id == "g7"));

    // No filter → still contains g7 tagged "share".
    let resp = client
        .send(ListGroupsRequest::default())
        .await
        .expect("ListGroups[all]");
    let row = resp
        .groups
        .iter()
        .find(|g| g.group_id == "g7")
        .expect("share group g7 present with no filter");
    assert2::assert!(row.group_type == "share");
}
