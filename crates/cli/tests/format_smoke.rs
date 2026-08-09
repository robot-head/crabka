//! Smoke tests for the `crabka format` binary.
//!
//! Each test runs the binary as a subprocess. It then asserts on the exit
//! code and on the on-disk output. These tests show that the clap surface and
//! the bootstrap-write path work end-to-end without a broker.

use std::process::Command;

use crabka_metadata::MetadataRecord;

fn run_format(dir: &tempfile::TempDir, args: &[&str]) -> std::process::Output {
    let bin = env!("CARGO_BIN_EXE_crabka");
    let mut command = Command::new(bin);
    command
        .args(["format", "--log-dir", dir.path().to_str().unwrap()])
        .args(args)
        .output()
        .expect("run crabka format")
}

fn bootstrap_records(dir: &tempfile::TempDir) -> Vec<MetadataRecord> {
    crabka_broker::bootstrap::load_bootstrap_records(dir.path()).expect("bootstrap records")
}

fn offset_zero_checkpoint(dir: &tempfile::TempDir) -> std::path::PathBuf {
    dir.path()
        .join("__cluster_metadata")
        .join("@metadata-0")
        .join("00000000000000000000-0000000000.checkpoint")
}

#[test]
fn format_with_add_scram_writes_credential_record() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_format(
        &dir,
        &[
            "--add-scram",
            "SCRAM-SHA-512=[name=admin,password=admin-secret,iterations=4096]",
        ],
    );
    assert2::assert!(out.status.success());
    // Bootstrap manifest + binary records file should both exist and
    // be non-empty.
    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert2::assert!(!entries.is_empty());
    let manifest = std::fs::read_to_string(dir.path().join("bootstrap.json"))
        .expect("bootstrap.json must exist");
    // Static format seeds every registered feature whose default at the latest
    // release is > 0 (metadata.version=25 KIP-778 + group.version=1
    // KIP-848 + transaction.version=2 KIP-890; share.version + streams.version
    // default to 0 and are omitted per KIP-1022) + SCRAM = 4 records.
    assert2::assert!(manifest.contains("\"record_count\": 4"));
    assert2::assert!(manifest.contains("cluster_id"));
    let bin_meta = std::fs::metadata(dir.path().join("bootstrap.records.bin"))
        .expect("bootstrap.records.bin must exist");
    assert2::assert!(bin_meta.len() > 0);
    let records = bootstrap_records(&dir);
    assert2::assert!(records.iter().all(|record| !matches!(
        record,
        MetadataRecord::V1KRaftVersion(_) | MetadataRecord::V1Voters(_)
    )));
    assert2::assert!(!offset_zero_checkpoint(&dir).exists());
}

#[test]
fn format_low_iterations_fails() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_format(
        &dir,
        &[
            "--add-scram",
            "SCRAM-SHA-512=[name=admin,password=p,iterations=1]",
        ],
    );
    assert2::assert!(!out.status.success());
    assert2::assert!(out.status.code() == Some(2));
}

#[test]
fn no_initial_controllers_writes_offset_zero_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_format(&dir, &["--no-initial-controllers"]);
    assert2::assert!(out.status.success());

    let records = bootstrap_records(&dir);
    assert2::assert!(!records.is_empty());
    assert2::assert!(records.iter().all(|record| !matches!(
        record,
        MetadataRecord::V1KRaftVersion(_) | MetadataRecord::V1Voters(_)
    )));
    assert2::assert!(std::fs::metadata(offset_zero_checkpoint(&dir)).is_ok_and(|m| m.len() > 0));
}

#[test]
fn standalone_writes_offset_zero_checkpoint_for_local_voter() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_format(
        &dir,
        &[
            "--standalone",
            "--node-id",
            "7",
            "--controller-listener",
            "controller.example:9093",
        ],
    );
    assert2::assert!(out.status.success());

    let directory_id =
        crabka_broker::bootstrap::read_directory_id(dir.path()).expect("formatted directory id");
    let records = bootstrap_records(&dir);
    assert2::assert!(!directory_id.is_nil());
    assert2::assert!(records.iter().all(|record| !matches!(
        record,
        MetadataRecord::V1KRaftVersion(_) | MetadataRecord::V1Voters(_)
    )));
    assert2::assert!(std::fs::metadata(offset_zero_checkpoint(&dir)).is_ok_and(|m| m.len() > 0));
}

#[test]
fn initial_controllers_persists_the_local_listed_directory_id() {
    let dir = tempfile::tempdir().unwrap();
    let local_directory_id = "00000000-0000-0000-0000-000000000003";
    let out = run_format(
        &dir,
        &[
            "--node-id",
            "3",
            "--initial-controllers",
            &format!(
                "2@two.example:9093:00000000-0000-0000-0000-000000000002,3@three.example:9093:{local_directory_id}"
            ),
        ],
    );
    assert2::assert!(out.status.success());

    let directory_id =
        crabka_broker::bootstrap::read_directory_id(dir.path()).expect("formatted directory id");
    assert2::assert!(directory_id.to_string() == local_directory_id);
    let records = bootstrap_records(&dir);
    assert2::assert!(records.iter().all(|record| !matches!(
        record,
        MetadataRecord::V1KRaftVersion(_) | MetadataRecord::V1Voters(_)
    )));
    assert2::assert!(std::fs::metadata(offset_zero_checkpoint(&dir)).is_ok_and(|m| m.len() > 0));
}

#[test]
fn initial_controllers_rejects_ambiguous_or_missing_local_identity() {
    for args in [
        vec![
            "--node-id",
            "3",
            "--initial-controllers",
            "2@two.example:9093:00000000-0000-0000-0000-000000000002",
        ],
        vec![
            "--node-id",
            "2",
            "--initial-controllers",
            "2@two.example:9093:00000000-0000-0000-0000-000000000002,2@other.example:9093:00000000-0000-0000-0000-000000000003",
        ],
        vec![
            "--node-id",
            "2",
            "--initial-controllers",
            "2@two.example:9093:00000000-0000-0000-0000-000000000002,3@three.example:9093:00000000-0000-0000-0000-000000000002",
        ],
    ] {
        let dir = tempfile::tempdir().unwrap();
        let out = run_format(&dir, &args);
        assert2::assert!(!out.status.success());
        assert2::assert!(out.status.code() == Some(4));
    }
}

#[test]
fn dynamic_modes_are_mutually_exclusive() {
    for args in [
        vec!["--standalone", "--no-initial-controllers"],
        vec![
            "--initial-controllers",
            "1@one.example:9093:00000000-0000-0000-0000-000000000001",
            "--no-initial-controllers",
        ],
        vec![
            "--standalone",
            "--initial-controllers",
            "1@one.example:9093:00000000-0000-0000-0000-000000000001",
        ],
    ] {
        let dir = tempfile::tempdir().unwrap();
        let out = run_format(&dir, &args);
        assert2::assert!(!out.status.success());
        assert2::assert!(out.status.code() == Some(2));
    }
}

#[test]
fn kraft_version_must_match_the_selected_format_mode() {
    for args in [
        vec!["--no-initial-controllers", "--feature", "kraft.version=0"],
        vec!["--feature", "kraft.version=1"],
        vec!["--feature", "kraft.version=2"],
    ] {
        let dir = tempfile::tempdir().unwrap();
        let out = run_format(&dir, &args);
        assert2::assert!(!out.status.success());
        assert2::assert!(out.status.code() == Some(5));
    }

    let static_dir = tempfile::tempdir().unwrap();
    let out = run_format(&static_dir, &["--feature", "kraft.version=0"]);
    assert2::assert!(out.status.success());
    assert2::assert!(bootstrap_records(&static_dir).iter().all(|record| {
        !matches!(
            record,
            MetadataRecord::V1KRaftVersion(_) | MetadataRecord::V1Voters(_)
        )
    }));
}
