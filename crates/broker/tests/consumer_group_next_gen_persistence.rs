//! Broker restart preserves next-gen group state via `__consumer_offsets` replay.

#![allow(clippy::pedantic)]

use std::sync::Arc;

use assert2::assert;
use crabka_broker::{BootstrapMode, Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::owned::{
    consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
};

async fn create_topic(client: &Client, name: &str, partitions: i32) {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: name.to_string(),
            num_partitions: partitions,
            replication_factor: 1,
            assignments: vec![],
            configs: vec![],
            ..Default::default()
        }],
        timeout_ms: 5_000,
        validate_only: false,
        ..Default::default()
    };
    let resp = client.send(req).await.unwrap();
    let code = resp.topics.first().map(|t| t.error_code).unwrap_or(0);
    assert!(
        code == 0,
        "create_topic {name} failed with error_code {code}"
    );
}

fn rejoin_config(log_dir: std::path::PathBuf) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir);
    cfg.bootstrap_mode = BootstrapMode::Rejoin;
    cfg
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_preserves_group_epoch_and_members() {
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let member_id;
    let initial_epoch;
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir.clone()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = Arc::new(
            Client::builder()
                .bootstrap(bootstrap.as_str())
                .client_id("c")
                .build()
                .await
                .unwrap(),
        );
        create_topic(&client, "tp", 2).await;
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "gp".into(),
            member_id: String::new(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec!["tp".into()]),
            rebalance_timeout_ms: 60_000,
            ..Default::default()
        };
        let resp = client.send(req).await.unwrap();
        assert!(resp.error_code == 0);
        member_id = resp.member_id.unwrap();
        initial_epoch = resp.member_epoch;
        // The heartbeat RPC awaits flush_pending→offsets_log.append synchronously,
        // so durability is guaranteed before the RPC returns. Wait for the actor's
        // in-memory state to reflect the member (epoch ≥ 1) as a clean shutdown gate.
        broker.wait_until_group_member_count("gp", 1).await;
        broker.shutdown().await;
    }

    {
        let broker = Broker::start(rejoin_config(log_dir)).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = Arc::new(
            Client::builder()
                .bootstrap(bootstrap.as_str())
                .client_id("c")
                .build()
                .await
                .unwrap(),
        );
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "gp".into(),
            member_id: member_id.clone(),
            member_epoch: initial_epoch,
            subscribed_topic_names: Some(vec!["tp".into()]),
            rebalance_timeout_ms: 60_000,
            ..Default::default()
        };
        let resp = client.send(req).await.unwrap();
        assert!(resp.error_code == 0, "post-restart heartbeat must succeed");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn next_gen_state_cleared_after_leave_then_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let member_id;
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir.clone()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = Arc::new(
            Client::builder()
                .bootstrap(bootstrap.as_str())
                .client_id("c")
                .build()
                .await
                .unwrap(),
        );
        create_topic(&client, "tp2", 1).await;
        let join = ConsumerGroupHeartbeatRequest {
            group_id: "gpx".into(),
            member_id: String::new(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec!["tp2".into()]),
            rebalance_timeout_ms: 60_000,
            ..Default::default()
        };
        let resp = client.send(join).await.unwrap();
        assert!(resp.error_code == 0);
        member_id = resp.member_id.unwrap();
        broker.wait_until_group_member_count("gpx", 1).await;
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(75),
                broker.wait_until_group_empty("gpx"),
            )
            .await
            .is_err(),
            "group-empty waiter must not complete while a member is live"
        );
        let leave = ConsumerGroupHeartbeatRequest {
            group_id: "gpx".into(),
            member_id: member_id.clone(),
            member_epoch: -1,
            ..Default::default()
        };
        let _ = client.send(leave).await.unwrap();
        // The leave RPC awaits flush_pending→offsets_log.append synchronously,
        // so tombstones are durable before the RPC returns. Wait for actor's
        // in-memory view to confirm zero members before shutdown.
        broker.wait_until_group_empty("gpx").await;
        broker.shutdown().await;
    }

    {
        let broker = Broker::start(rejoin_config(log_dir)).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = Arc::new(
            Client::builder()
                .bootstrap(bootstrap.as_str())
                .client_id("c")
                .build()
                .await
                .unwrap(),
        );
        // After leave + restart, the member should be unknown.
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "gpx".into(),
            member_id: member_id.clone(),
            member_epoch: 5,
            subscribed_topic_names: Some(vec!["tp2".into()]),
            ..Default::default()
        };
        let resp = client.send(req).await.unwrap();
        assert!(resp.error_code == crabka_broker::codes::UNKNOWN_MEMBER_ID);
    }
}
