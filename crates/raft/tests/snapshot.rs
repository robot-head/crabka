//! End-to-end snapshot generation + restart recovery for a single-voter
//! controller. Proves a committed metadata change survives a manual snapshot
//! and a full process-style restart that rebuilds the image from the on-disk
//! checkpoint (not from in-memory state).
//!
//! Slice 3c reimplements `trigger_snapshot` (image → KIP-630 checkpoint) and
//! restart recovery. The auto-snapshot background pump (byte/interval triggers)
//! and cross-node `InstallSnapshot` learner catch-up that the openraft
//! controller carried are deferred — auto-snapshot heuristics and Slice-4
//! `FetchSnapshot` catch-up respectively — so the tests that exercised them are
//! gone with openraft.

use assert2::assert;
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

    // First boot: bootstrap a fresh single voter, commit a topic, then snapshot.
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

        // The checkpoint is written synchronously inside the engine before
        // `trigger_snapshot` returns; confirm it landed on disk.
        assert!(
            has_checkpoint(&dir.path().join("@metadata-0")),
            "checkpoint must be written to disk"
        );

        controller.shutdown().await;
    }

    // Restart from the SAME dir in Rejoin mode. The engine must rebuild the
    // image from the on-disk checkpoint + log.
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

fn has_checkpoint(meta_dir: &std::path::Path) -> bool {
    std::fs::read_dir(meta_dir)
        .into_iter()
        .flatten()
        .flatten()
        .any(|e| e.file_name().to_string_lossy().ends_with(".checkpoint"))
}
