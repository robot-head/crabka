//! End-to-end snapshot generation + restart recovery for a single-voter
//! controller. Proves a committed metadata change survives a manual
//! snapshot and a full process-style restart that rebuilds the image
//! from the on-disk checkpoint (not from in-memory state).

use std::net::SocketAddr;
use std::time::Duration;

use crabka_metadata::{MetadataRecord, TopicRecord, VoterEndpoint};
use crabka_raft::{BootstrapMode, Controller, ControllerConfig, Node};
use tempfile::TempDir;
use uuid::Uuid;

/// Bind an ephemeral loopback port, then immediately release it so the
/// controller can claim it. Standard test pattern: the brief window
/// between release and rebind is acceptable for in-process tests.
fn reserve_port() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap()
}

/// A KIP-853 `Node` advertising a single CONTROLLER endpoint at `addr` so
/// the leader's `add_learner` can dial the joiner back.
fn node_at(addr: SocketAddr) -> Node {
    Node {
        directory_id: Uuid::new_v4(),
        endpoints: vec![VoterEndpoint {
            name: "CONTROLLER".into(),
            host: addr.ip().to_string(),
            port: addr.port(),
        }],
        kraft_version: crabka_metadata::KRaftVersionRange::default(),
    }
}

async fn wait_for_leader(controller: &crabka_raft::ControllerHandle) {
    let deadline = std::time::Instant::now() + Duration::from_mins(2);
    loop {
        if controller.watch_leader().borrow().is_some() {
            return;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "no leader elected within 2 min"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_then_restart_recovers_image() {
    let dir = TempDir::new().unwrap();
    let cid = Uuid::new_v4();

    // First boot: bootstrap a fresh single voter, commit a topic, then
    // snapshot it.
    {
        let mut cfg = ControllerConfig::for_tests(1, dir.path().to_path_buf());
        cfg.election_timeout = Duration::from_millis(200);
        cfg.cluster_id = Some(cid);
        cfg.bootstrap_mode = BootstrapMode::Bootstrap;
        let controller = Controller::start(cfg).await.expect("first boot start");
        wait_for_leader(&controller).await;

        controller
            .submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id: Uuid::new_v4(),
                partitions: 1,
                replication_factor: 1,
            })])
            .await
            .expect("submit topic");
        assert!(controller.current_image().topic("t").is_some());

        controller
            .trigger_snapshot()
            .await
            .expect("trigger snapshot");

        // The build runs asynchronously inside the engine; give it time to
        // serialize the checkpoint to disk before we tear the node down.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if has_checkpoint(&dir.path().join("@metadata-0")) {
                break;
            }
            assert!(
                std::time::Instant::now() <= deadline,
                "checkpoint never written to disk"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        controller.shutdown().await;
    }

    // Restart from the SAME dir in Rejoin mode. The state machine must
    // rebuild the image from the on-disk checkpoint.
    {
        let mut cfg = ControllerConfig::for_tests(1, dir.path().to_path_buf());
        cfg.election_timeout = Duration::from_millis(200);
        cfg.cluster_id = Some(cid);
        cfg.bootstrap_mode = BootstrapMode::Rejoin;
        let controller = Controller::start(cfg).await.expect("rejoin start");
        wait_for_leader(&controller).await;

        assert!(
            controller.current_image().topic("t").is_some(),
            "topic 't' must survive snapshot + restart"
        );

        controller.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_learner_catches_up_via_snapshot() {
    let cid = Uuid::new_v4();
    let addr1 = reserve_port();
    let addr2 = reserve_port();

    // Node 1: bootstrap a single-voter cluster.
    let dir1 = TempDir::new().unwrap();
    let mut cfg1 = ControllerConfig::for_tests(1, dir1.path().to_path_buf());
    cfg1.election_timeout = Duration::from_millis(200);
    cfg1.cluster_id = Some(cid);
    cfg1.bootstrap_mode = BootstrapMode::Bootstrap;
    cfg1.controller_listen_addr = addr1;
    let leader = Controller::start(cfg1).await.expect("node 1 start");
    wait_for_leader(&leader).await;

    // Commit a topic, then snapshot. With `max_in_snapshot_log_to_keep =
    // 0`, the completed snapshot drives a purge that compacts the log
    // behind the checkpoint — so a node that has never seen those entries
    // can only learn them through InstallSnapshot.
    leader
        .submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        })])
        .await
        .expect("submit topic");
    assert!(leader.current_image().topic("t").is_some());

    leader.trigger_snapshot().await.expect("trigger snapshot");
    let snap_dir = dir1.path().join("@metadata-0");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if has_checkpoint(&snap_dir) {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "checkpoint never written"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Node 2: start empty in Join mode (waits for an external add_learner;
    // never initializes its own log).
    let dir2 = TempDir::new().unwrap();
    let mut cfg2 = ControllerConfig::for_tests(2, dir2.path().to_path_buf());
    cfg2.election_timeout = Duration::from_millis(200);
    cfg2.cluster_id = Some(cid);
    cfg2.bootstrap_mode = BootstrapMode::Join;
    cfg2.controller_listen_addr = addr2;
    let learner = Controller::start(cfg2).await.expect("node 2 start");

    // Register node 2 as a learner AFTER the snapshot+purge, so its only
    // path to the topic record is InstallSnapshot. Keep it a learner (no
    // change_membership) to isolate the snapshot-install path.
    leader
        .add_learner(2, node_at(addr2))
        .await
        .expect("add learner triggers snapshot install");

    // The learner's image must converge on the snapshot-installed topic.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if learner.current_image().topic("t").is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "learner never caught up via snapshot"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The learner never triggers its own snapshot, so a checkpoint in its
    // snapshot dir can only have come from install_snapshot — proof the
    // catch-up took the InstallSnapshot path, not append-entries.
    let learner_snap_dir = dir2.path().join("@metadata-0");
    assert!(
        has_checkpoint(&learner_snap_dir),
        "learner must have persisted an installed checkpoint"
    );

    learner.shutdown().await;
    leader.shutdown().await;
}

fn has_checkpoint(meta_dir: &std::path::Path) -> bool {
    std::fs::read_dir(meta_dir)
        .into_iter()
        .flatten()
        .flatten()
        .any(|e| e.file_name().to_string_lossy().ends_with(".checkpoint"))
}

#[tokio::test]
async fn byte_threshold_triggers_snapshot() {
    let dir = TempDir::new().unwrap();
    let mut cfg = ControllerConfig::for_tests(1, dir.path().to_path_buf());
    cfg.max_bytes_between_snapshots = 1;
    cfg.max_snapshot_interval = Duration::from_hours(1);
    let ctrl = Controller::start(cfg).await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    ctrl.submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
        name: "t".into(),
        topic_id: Uuid::from_u128(9),
        partitions: 1,
        replication_factor: 1,
    })])
    .await
    .unwrap();
    let meta_dir = dir.path().join("@metadata-0");
    let mut found = false;
    for _ in 0..40 {
        if has_checkpoint(&meta_dir) {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(found, "expected an automatic snapshot");
    ctrl.shutdown().await;
}

#[tokio::test]
async fn interval_triggers_snapshot() {
    let dir = TempDir::new().unwrap();
    let mut cfg = ControllerConfig::for_tests(1, dir.path().to_path_buf());
    cfg.max_bytes_between_snapshots = u64::MAX;
    cfg.max_snapshot_interval = Duration::from_millis(300);
    let ctrl = Controller::start(cfg).await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    ctrl.submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
        name: "t".into(),
        topic_id: Uuid::from_u128(7),
        partitions: 1,
        replication_factor: 1,
    })])
    .await
    .unwrap();
    let meta_dir = dir.path().join("@metadata-0");
    let mut found = false;
    for _ in 0..40 {
        if has_checkpoint(&meta_dir) {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(found, "expected an interval-driven snapshot");
    ctrl.shutdown().await;
}
