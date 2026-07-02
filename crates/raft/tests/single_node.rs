//! In-process single-voter Controller. Validates the openraft + `log_store`
//! + `state_machine` + listener wiring without needing a 3-node cluster.

use std::time::Duration;

use assert2::assert;
use crabka_metadata::{MetadataRecord, TopicRecord};
use crabka_raft::{Controller, ControllerConfig};
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_voter_create_topic_round_trip() {
    let dir = TempDir::new().unwrap();
    let mut cfg = ControllerConfig::for_tests(1, dir.path().to_path_buf());
    // Single-voter elections should be instant; lower the election timeout
    // so we comfortably make the 5-second deadline below.
    cfg.election_timeout = Duration::from_millis(200);
    // Pin the controller listen addr to a real loopback port so the network
    // factory has something to dial when initialize wants to seed members.
    cfg.controller_listen_addr = "127.0.0.1:0".parse().unwrap();

    let controller = Controller::start(cfg).await.expect("controller start");

    // Wait until openraft elects this single voter as leader.
    let mut rx = controller.watch_leader();
    tokio::time::timeout(Duration::from_secs(30), rx.wait_for(Option::is_some))
        .await
        .expect("no leader elected within 30s")
        .expect("leader watch channel closed");

    let topic = MetadataRecord::V1Topic(TopicRecord {
        name: "t".into(),
        topic_id: Uuid::new_v4(),
        partitions: 1,
        replication_factor: 1,
    });
    controller.submit_change(vec![topic]).await.expect("submit");

    assert!(controller.current_image().topic("t").is_some());

    controller.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_voter_duplicate_topic_rejected() {
    let dir = TempDir::new().unwrap();
    let mut cfg = ControllerConfig::for_tests(1, dir.path().to_path_buf());
    cfg.election_timeout = Duration::from_millis(200);
    cfg.controller_listen_addr = "127.0.0.1:0".parse().unwrap();
    let controller = Controller::start(cfg).await.unwrap();

    let mut rx = controller.watch_leader();
    tokio::time::timeout(Duration::from_secs(30), rx.wait_for(Option::is_some))
        .await
        .expect("no leader elected within 30s")
        .expect("leader watch channel closed");

    let topic = MetadataRecord::V1Topic(TopicRecord {
        name: "t".into(),
        topic_id: Uuid::new_v4(),
        partitions: 1,
        replication_factor: 1,
    });
    controller.submit_change(vec![topic.clone()]).await.unwrap();
    let err = controller.submit_change(vec![topic]).await.unwrap_err();
    assert!(matches!(
        err,
        crabka_raft::RaftError::Metadata(crabka_metadata::MetadataError::TopicExists(_))
    ));

    controller.shutdown().await;
}
