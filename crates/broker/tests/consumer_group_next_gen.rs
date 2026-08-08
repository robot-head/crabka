//! Raw-RPC integration tests for KIP-848 next-gen consumer groups,
//! driven against an in-process Crabka broker through `crabka-client-core`.

use std::sync::Arc;

use assert2::{assert, check};
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::owned::{
    consumer_group_describe_request::ConsumerGroupDescribeRequest,
    consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    list_groups_request::ListGroupsRequest,
};

async fn boot() -> (crabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
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

fn heartbeat(group: &str, member_id: &str, epoch: i32) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest {
        group_id: group.into(),
        member_id: member_id.into(),
        member_epoch: epoch,
        rebalance_timeout_ms: 60_000,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_member_full_lifecycle() {
    let (_b, bootstrap, _d) = boot().await;
    let client = Arc::new(
        Client::builder()
            .bootstrap(bootstrap.as_str())
            .client_id("c1")
            .build()
            .await
            .unwrap(),
    );
    create_topic(&client, "t1", 4).await;

    let mut req = heartbeat("g1", "", 0);
    req.subscribed_topic_names = Some(vec!["t1".into()]);
    let resp = client.send(req).await.unwrap();
    assert!(resp.error_code == 0);
    let member_id = resp.member_id.clone().unwrap();
    assert!(resp.member_epoch == 1);
    let assigned = resp.assignment.as_ref().unwrap();
    let total_partitions: usize = assigned
        .topic_partitions
        .iter()
        .map(|t| t.partitions.len())
        .sum();
    assert!(total_partitions == 4);

    let mut hb2 = heartbeat("g1", &member_id, 1);
    hb2.subscribed_topic_names = Some(vec!["t1".into()]);
    let resp2 = client.send(hb2).await.unwrap();
    assert!(resp2.error_code == 0);
    assert!(resp2.member_epoch == 1);

    let leave = heartbeat("g1", &member_id, -1);
    let resp3 = client.send(leave).await.unwrap();
    assert!(resp3.error_code == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_members_split_partitions() {
    let (_b, bootstrap, _d) = boot().await;
    let client = Arc::new(
        Client::builder()
            .bootstrap(bootstrap.as_str())
            .client_id("c")
            .build()
            .await
            .unwrap(),
    );
    create_topic(&client, "t2", 4).await;

    let mut a = heartbeat("g2", "", 0);
    a.subscribed_topic_names = Some(vec!["t2".into()]);
    let ra = client.send(a).await.unwrap();
    assert!(ra.error_code == 0, "A join failed: {:?}", ra.error_code);
    let mid_a = ra.member_id.unwrap();

    let mut b = heartbeat("g2", "", 0);
    b.subscribed_topic_names = Some(vec!["t2".into()]);
    let rb = client.send(b).await.unwrap();
    assert!(rb.error_code == 0, "B join failed: {:?}", rb.error_code);
    let mid_b = rb.member_id.unwrap();
    let b_epoch = rb.member_epoch;

    // A re-heartbeats at its own epoch (1) to learn the rebalanced assignment
    // and revoke the partitions B's target needs. B's join bumped the group
    // epoch to 2 and updated A's target, but A's stored member_epoch is still
    // 1 — we must heartbeat at that epoch.
    let mut a3 = heartbeat("g2", &mid_a, ra.member_epoch);
    a3.subscribed_topic_names = Some(vec!["t2".into()]);
    let ra3 = client.send(a3).await.unwrap();
    assert!(ra3.error_code == 0, "A re-hb failed: {:?}", ra3.error_code);

    // B re-heartbeats to acquire the partitions A just released. Per KIP-848 the
    // coordinator withholds a partition from its new owner until the previous
    // owner has revoked it, so B's *join* response (rb) intentionally carries
    // fewer partitions than B's target; B converges on this next heartbeat, now
    // that A's re-heartbeat above revoked them.
    let mut b3 = heartbeat("g2", &mid_b, b_epoch);
    b3.subscribed_topic_names = Some(vec!["t2".into()]);
    let rb3 = client.send(b3).await.unwrap();
    assert!(rb3.error_code == 0, "B re-hb failed: {:?}", rb3.error_code);

    let parts_a: usize = ra3
        .assignment
        .unwrap()
        .topic_partitions
        .iter()
        .map(|t| t.partitions.len())
        .sum();
    let parts_b: usize = rb3
        .assignment
        .unwrap()
        .topic_partitions
        .iter()
        .map(|t| t.partitions.len())
        .sum();
    assert!(parts_a + parts_b == 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_group_locked_against_next_gen() {
    use crabka_protocol::owned::join_group_request::JoinGroupRequest;
    let (_b, bootstrap, _d) = boot().await;
    let client = Arc::new(
        Client::builder()
            .bootstrap(bootstrap.as_str())
            .client_id("c")
            .build()
            .await
            .unwrap(),
    );
    create_topic(&client, "t3", 2).await;

    let join = JoinGroupRequest {
        group_id: "g3".into(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 60_000,
        member_id: String::new(),
        protocol_type: "consumer".into(),
        ..Default::default()
    };
    let _ = client.send(join).await.unwrap();

    let mut req = heartbeat("g3", "", 0);
    req.subscribed_topic_names = Some(vec!["t3".into()]);
    let resp = client.send(req).await.unwrap();
    assert!(resp.error_code == crabka_broker::codes::GROUP_ID_NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_switch_returns_group_id_not_found() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.next_gen_consumer_group.rebalance_protocols =
        vec![crabka_broker::coordinator::unified::config::RebalanceProtocol::Classic];
    let broker = Broker::start(config).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    let client = Arc::new(
        Client::builder()
            .bootstrap(bootstrap.as_str())
            .client_id("c")
            .build()
            .await
            .unwrap(),
    );

    let mut req = heartbeat("g4", "", 0);
    req.subscribed_topic_names = Some(vec!["t".into()]);
    let resp = client.send(req).await.unwrap();
    assert!(resp.error_code == crabka_broker::codes::GROUP_ID_NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_after_join() {
    let (_b, bootstrap, _d) = boot().await;
    let client = Arc::new(
        Client::builder()
            .bootstrap(bootstrap.as_str())
            .client_id("c")
            .build()
            .await
            .unwrap(),
    );
    create_topic(&client, "t5", 2).await;

    let mut req = heartbeat("g5", "", 0);
    req.subscribed_topic_names = Some(vec!["t5".into()]);
    let _ = client.send(req).await.unwrap();

    let desc = client
        .send(ConsumerGroupDescribeRequest {
            group_ids: vec!["g5".into()],
            include_authorized_operations: false,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(desc.groups.len() == 1);
    check!(desc.groups[0].error_code == 0);
    check!(desc.groups[0].group_state == "STABLE");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_epoch_rejected() {
    let (_b, bootstrap, _d) = boot().await;
    let client = Arc::new(
        Client::builder()
            .bootstrap(bootstrap.as_str())
            .client_id("c")
            .build()
            .await
            .unwrap(),
    );
    create_topic(&client, "t6", 2).await;

    // A joins; group_epoch goes 0→1, A's member_epoch = 1.
    let mut req = heartbeat("g6", "", 0);
    req.subscribed_topic_names = Some(vec!["t6".into()]);
    let r = client.send(req).await.unwrap();
    assert!(r.error_code == 0);
    let mid = r.member_id.unwrap();

    // B joins; group_epoch goes 1→2, B's member_epoch = 2, A's is still 1.
    let mut req2 = heartbeat("g6", "", 0);
    req2.subscribed_topic_names = Some(vec!["t6".into()]);
    let rb = client.send(req2).await.unwrap();
    assert!(rb.error_code == 0);

    // A catches up: heartbeat at epoch 1 succeeds and advances A's epoch to 2.
    let mut catch_up = heartbeat("g6", &mid, 1);
    catch_up.subscribed_topic_names = Some(vec!["t6".into()]);
    let rc = client.send(catch_up).await.unwrap();
    assert!(rc.error_code == 0);
    assert!(
        rc.member_epoch == 2,
        "A should be at epoch 2 after catch-up"
    );

    // Now A re-heartbeats at the OLD epoch 1; A's stored epoch is 2 → STALE.
    let stale = heartbeat("g6", &mid, 1);
    let resp = client.send(stale).await.unwrap();
    assert!(resp.error_code == crabka_broker::codes::STALE_MEMBER_EPOCH);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_join_with_client_member_id_echoes_and_assigns() {
    let (_b, bootstrap, _d) = boot().await;
    let client = Arc::new(
        Client::builder()
            .bootstrap(bootstrap.as_str())
            .client_id("c")
            .build()
            .await
            .unwrap(),
    );
    create_topic(&client, "tc", 2).await;

    // Client supplies its own member id (GA KIP-848 semantics).
    let mut req = heartbeat("gc", "client-generated-id", 0);
    req.subscribed_topic_names = Some(vec!["tc".into()]);
    let resp = client.send(req).await.unwrap();

    assert!(resp.error_code == 0, "client-id first-join failed");
    assert!(
        resp.member_id.as_deref() == Some("client-generated-id"),
        "broker must echo the client-supplied member id"
    );
    let parts: usize = resp
        .assignment
        .expect("assignment present")
        .topic_partitions
        .iter()
        .map(|t| t.partitions.len())
        .sum();
    assert!(
        parts == 2,
        "single member should be assigned both partitions"
    );
}

/// `kafka-consumer-groups.sh --list` sends `ListGroups` (`api_key` 16) with
/// `types_filter = ["consumer"]`. A live next-gen consumer group must appear in
/// that response with `group_type == "consumer"`. It must NOT appear when the
/// request filters on `["share"]`, and it must appear exactly once with no
/// filter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_groups_includes_next_gen_consumer_group() {
    let (_b, bootstrap, _d) = boot().await;
    let client = Arc::new(
        Client::builder()
            .bootstrap(bootstrap.as_str())
            .client_id("c")
            .build()
            .await
            .unwrap(),
    );
    create_topic(&client, "tlist", 2).await;

    // Join a next-gen consumer group so it is registered in the coordinator.
    let mut req = heartbeat("glist", "", 0);
    req.subscribed_topic_names = Some(vec!["tlist".into()]);
    let resp = client.send(req).await.unwrap();
    assert!(resp.error_code == 0, "join failed: {:?}", resp.error_code);

    // types_filter = ["consumer"] → contains glist tagged "consumer".
    let resp = client
        .send(ListGroupsRequest {
            types_filter: vec!["consumer".into()],
            ..Default::default()
        })
        .await
        .expect("ListGroups[consumer]");
    assert!(resp.error_code == 0, "list error: {:?}", resp.error_code);
    let row = resp
        .groups
        .iter()
        .find(|g| g.group_id == "glist")
        .unwrap_or_else(|| {
            panic!(
                "consumer group glist missing from ListGroups[consumer], got {:?}",
                resp.groups.iter().map(|g| &g.group_id).collect::<Vec<_>>()
            )
        });
    assert!(
        row.group_type == "consumer",
        "expected group_type=consumer, got {:?}",
        row.group_type
    );

    // types_filter = ["share"] → glist must NOT appear.
    let resp = client
        .send(ListGroupsRequest {
            types_filter: vec!["share".into()],
            ..Default::default()
        })
        .await
        .expect("ListGroups[share]");
    assert!(
        !resp.groups.iter().any(|g| g.group_id == "glist"),
        "consumer group glist must be excluded under types_filter=[share], got {:?}",
        resp.groups.iter().map(|g| &g.group_id).collect::<Vec<_>>()
    );

    // No filter → glist appears exactly once, tagged "consumer".
    let resp = client
        .send(ListGroupsRequest::default())
        .await
        .expect("ListGroups[all]");
    let matches: Vec<_> = resp
        .groups
        .iter()
        .filter(|g| g.group_id == "glist")
        .collect();
    assert!(
        matches.len() == 1,
        "glist must be listed exactly once, got {} rows",
        matches.len()
    );
    assert!(
        matches[0].group_type == "consumer",
        "unfiltered list must tag glist as consumer, got {:?}",
        matches[0].group_type
    );
}
