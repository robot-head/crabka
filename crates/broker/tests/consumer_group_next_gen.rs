//! Raw-RPC integration tests for KIP-848 next-gen consumer groups,
//! driven against an in-process Crabka broker via `crabka-client-core`.

#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]

use std::sync::Arc;

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::owned::consumer_group_describe_request::ConsumerGroupDescribeRequest;
use crabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};

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
    assert_eq!(resp.topics[0].error_code, 0, "topic create failed: {resp:?}");
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
    assert_eq!(resp.error_code, 0);
    let member_id = resp.member_id.clone().unwrap();
    assert_eq!(resp.member_epoch, 1);
    let assigned = resp.assignment.as_ref().unwrap();
    let total_partitions: usize = assigned
        .topic_partitions
        .iter()
        .map(|t| t.partitions.len())
        .sum();
    assert_eq!(total_partitions, 4);

    let mut hb2 = heartbeat("g1", &member_id, 1);
    hb2.subscribed_topic_names = Some(vec!["t1".into()]);
    let resp2 = client.send(hb2).await.unwrap();
    assert_eq!(resp2.error_code, 0);
    assert_eq!(resp2.member_epoch, 1);

    let leave = heartbeat("g1", &member_id, -1);
    let resp3 = client.send(leave).await.unwrap();
    assert_eq!(resp3.error_code, 0);
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
    assert_eq!(ra.error_code, 0, "A join failed: {:?}", ra.error_code);
    let mid_a = ra.member_id.unwrap();

    let mut b = heartbeat("g2", "", 0);
    b.subscribed_topic_names = Some(vec!["t2".into()]);
    let rb = client.send(b).await.unwrap();
    assert_eq!(rb.error_code, 0, "B join failed: {:?}", rb.error_code);

    // A re-heartbeats at its own epoch (1) to learn the rebalanced assignment.
    // B's join bumped the group epoch to 2 and updated A's target, but A's
    // stored member_epoch is still 1 — we must heartbeat at that epoch.
    let mut a3 = heartbeat("g2", &mid_a, ra.member_epoch);
    a3.subscribed_topic_names = Some(vec!["t2".into()]);
    let ra3 = client.send(a3).await.unwrap();
    assert_eq!(ra3.error_code, 0, "A re-hb failed: {:?}", ra3.error_code);

    let parts_a: usize = ra3
        .assignment
        .unwrap()
        .topic_partitions
        .iter()
        .map(|t| t.partitions.len())
        .sum();
    let parts_b: usize = rb
        .assignment
        .unwrap()
        .topic_partitions
        .iter()
        .map(|t| t.partitions.len())
        .sum();
    assert_eq!(parts_a + parts_b, 4);
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
    assert_eq!(resp.error_code, crabka_broker::codes::GROUP_ID_NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_switch_returns_group_id_not_found() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.next_gen_consumer_group.rebalance_protocols = vec![
        crabka_broker::coordinator::next_gen::config::RebalanceProtocol::Classic,
    ];
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
    assert_eq!(resp.error_code, crabka_broker::codes::GROUP_ID_NOT_FOUND);
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
    assert_eq!(desc.groups.len(), 1);
    assert_eq!(desc.groups[0].error_code, 0);
    assert_eq!(desc.groups[0].group_state, "STABLE");
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
    assert_eq!(r.error_code, 0);
    let mid = r.member_id.unwrap();

    // B joins; group_epoch goes 1→2, B's member_epoch = 2, A's is still 1.
    let mut req2 = heartbeat("g6", "", 0);
    req2.subscribed_topic_names = Some(vec!["t6".into()]);
    let rb = client.send(req2).await.unwrap();
    assert_eq!(rb.error_code, 0);

    // A catches up: heartbeat at epoch 1 succeeds and advances A's epoch to 2.
    let mut catch_up = heartbeat("g6", &mid, 1);
    catch_up.subscribed_topic_names = Some(vec!["t6".into()]);
    let rc = client.send(catch_up).await.unwrap();
    assert_eq!(rc.error_code, 0);
    assert_eq!(rc.member_epoch, 2, "A should be at epoch 2 after catch-up");

    // Now A re-heartbeats at the OLD epoch 1; A's stored epoch is 2 → STALE.
    let stale = heartbeat("g6", &mid, 1);
    let resp = client.send(stale).await.unwrap();
    assert_eq!(resp.error_code, crabka_broker::codes::STALE_MEMBER_EPOCH);
}
