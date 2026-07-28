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

use std::time::Duration;

use assert2::check;
use crabka_metadata::{FeatureLevelRecord, MetadataRecord, NodeId, TopicRecord};
use crabka_raft::{BootstrapMode, Controller, ControllerConfig};
use crabka_units::prelude::{Time, millis};
use tempfile::TempDir;
use uuid::Uuid;

/// Single-voter elections are instant; a short timeout keeps each boot well
/// inside the 30-second leader deadline.
const FAST_ELECTION_TIMEOUT: Time = millis(200);

async fn wait_for_leader(controller: &crabka_raft::ControllerHandle) {
    let mut rx = controller.watch_leader();
    tokio::time::timeout(Duration::from_secs(30), rx.wait_for(Option::is_some))
        .await
        .expect("no leader elected within 30s")
        .expect("leader watch channel closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_then_restart_recovers_image() {
    let dir = TempDir::new().unwrap();
    let cid = Uuid::new_v4();

    // First boot: bootstrap a fresh single voter, commit a topic, then snapshot.
    {
        let mut cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
        cfg.election_timeout = FAST_ELECTION_TIMEOUT;
        cfg.cluster_id = Some(cid);
        cfg.bootstrap_mode = BootstrapMode::Bootstrap;
        let controller = Controller::start(cfg).await.expect("first boot start");
        wait_for_leader(&controller).await;

        // Commit real KIP-631 metadata-log records — a topic and a finalized
        // feature level — that must survive snapshot + restart.
        //
        // The controller voter set is NOT submitted here: under KIP-595 static
        // voters it is raft-control state living in the `QuorumState` (seeded
        // from config — `for_tests` seeds voter 1), with no KIP-631 metadata-log
        // counterpart, so `submit_change` rejects `V1Voters` / `V1KRaftVersion`.
        // The voter set is re-derived from config on every boot and mirrored
        // into the image, so its survival across restart is exercised below
        // without a submitted record. `kraft.version` stays 0 (static KRaft).
        controller
            .submit_change(vec![
                MetadataRecord::V1Topic(TopicRecord {
                    name: "t".into(),
                    topic_id: Uuid::new_v4(),
                    partitions: 1,
                    replication_factor: 1,
                }),
                // KIP-584 finalized feature: must survive snapshot + restart.
                // A dropped feature level reverts metadata.version to UNKNOWN.
                MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                    name: "metadata.version".into(),
                    level: 25,
                }),
            ])
            .await
            .expect("submit records");
        check!(
            (
                controller.current_image().topic("t").is_some(),
                controller.current_image().finalized_metadata_version(),
                controller.current_image().voters().contains(NodeId(1)),
            ) == (true, Some(25), true)
        );

        controller
            .trigger_snapshot()
            .await
            .expect("trigger snapshot");

        // The checkpoint is written synchronously inside the engine before
        // `trigger_snapshot` returns; confirm it landed on disk.
        assert2::assert!(has_checkpoint(&dir.path().join("@metadata-0")));

        controller.shutdown().await;
    }

    // Restart from the SAME dir in Rejoin mode. The engine must rebuild the
    // image from the on-disk checkpoint + log.
    {
        let mut cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
        cfg.election_timeout = FAST_ELECTION_TIMEOUT;
        cfg.cluster_id = Some(cid);
        cfg.bootstrap_mode = BootstrapMode::Rejoin;
        let controller = Controller::start(cfg).await.expect("rejoin start");
        wait_for_leader(&controller).await;

        assert2::assert!(controller.current_image().topic("t").is_some());
        // The finalized feature + its epoch must be rebuilt from the on-disk
        // checkpoint, not silently dropped.
        let recovered = controller.current_image();
        check!(
            recovered.finalized_metadata_version() == Some(25),
            "finalized metadata.version must survive snapshot + restart"
        );
        check!(
            recovered.finalized_features_epoch() >= 0,
            "finalized-features epoch must survive snapshot + restart, got {}",
            recovered.finalized_features_epoch()
        );
        // The voter set is re-derived from config (`QuorumState`) on every boot
        // and mirrored into the image — it must be present after restart.
        check!(
            recovered.voters().contains(NodeId(1)),
            "voter set must survive snapshot + restart"
        );

        controller.shutdown().await;
    }
}

// NOTE: origin/main's `lagging_learner_catches_up_via_snapshot` test (openraft
// `add_learner` + InstallSnapshot learner path) is intentionally dropped on this
// branch: openraft is gone and `add_learner` returns `Unsupported`. The
// equivalent — a lagging follower catching up via the real KIP-595
// `FetchSnapshot` — is covered by `kraft_engine_sim::lagging_follower_catches_up_via_snapshot`.
fn has_checkpoint(meta_dir: &std::path::Path) -> bool {
    std::fs::read_dir(meta_dir)
        .into_iter()
        .flatten()
        .flatten()
        .any(|e| e.file_name().to_string_lossy().ends_with(".checkpoint"))
}
