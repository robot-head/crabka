//! Broker restart preserves next-gen group state via __consumer_offsets replay.

#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]

use std::sync::Arc;
use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};

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
    assert_eq!(code, 0, "create_topic {name} failed with error_code {code}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "next-gen persistence write not yet wired; tracked as 64a follow-up"]
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
        assert_eq!(resp.error_code, 0);
        member_id = resp.member_id.unwrap();
        initial_epoch = resp.member_epoch;
        tokio::time::sleep(Duration::from_millis(300)).await;
        broker.shutdown().await;
    }

    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir))
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
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "gp".into(),
            member_id: member_id.clone(),
            member_epoch: initial_epoch,
            subscribed_topic_names: Some(vec!["tp".into()]),
            rebalance_timeout_ms: 60_000,
            ..Default::default()
        };
        let resp = client.send(req).await.unwrap();
        assert_eq!(resp.error_code, 0, "post-restart heartbeat must succeed");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "next-gen persistence write not yet wired; tracked as 64a follow-up"]
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
        assert_eq!(resp.error_code, 0);
        member_id = resp.member_id.unwrap();
        let leave = ConsumerGroupHeartbeatRequest {
            group_id: "gpx".into(),
            member_id: member_id.clone(),
            member_epoch: -1,
            ..Default::default()
        };
        let _ = client.send(leave).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        broker.shutdown().await;
    }

    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir))
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
        // After leave + restart, the member should be unknown.
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "gpx".into(),
            member_id: member_id.clone(),
            member_epoch: 5,
            subscribed_topic_names: Some(vec!["tp2".into()]),
            ..Default::default()
        };
        let resp = client.send(req).await.unwrap();
        assert_eq!(resp.error_code, crabka_broker::codes::UNKNOWN_MEMBER_ID);
    }
}
