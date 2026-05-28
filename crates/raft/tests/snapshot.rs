//! End-to-end snapshot generation + restart recovery for a single-voter
//! controller. Proves a committed metadata change survives a manual
//! snapshot and a full process-style restart that rebuilds the image
//! from the on-disk checkpoint (not from in-memory state).

use std::time::Duration;

use crabka_metadata::{MetadataRecord, TopicRecord};
use crabka_raft::{BootstrapMode, Controller, ControllerConfig};
use tempfile::TempDir;
use uuid::Uuid;

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

        controller.trigger_snapshot().await.expect("trigger snapshot");

        // The build runs asynchronously inside the engine; give it time to
        // serialize the checkpoint to disk before we tear the node down.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let has_checkpoint =
                std::fs::read_dir(dir.path().join("@metadata-0")).is_ok_and(|rd| {
                    rd.filter_map(Result::ok).any(|e| {
                        e.file_name()
                            .to_str()
                            .is_some_and(|n| n.ends_with(".checkpoint"))
                    })
                });
            if has_checkpoint {
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
