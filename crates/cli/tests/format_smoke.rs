//! Smoke tests for the `crabka format` binary.
//!
//! Each test runs the binary as a subprocess. It then asserts on the exit
//! code and on the on-disk output. These tests show that the clap surface and
//! the bootstrap-write path work end-to-end without a broker.

use std::process::Command;

#[test]
fn format_with_add_scram_writes_credential_record() {
    let bin = env!("CARGO_BIN_EXE_crabka");
    let dir = tempfile::tempdir().unwrap();
    let out = Command::new(bin)
        .args([
            "format",
            "--log-dir",
            dir.path().to_str().unwrap(),
            "--add-scram",
            "SCRAM-SHA-512=[name=admin,password=admin-secret,iterations=4096]",
        ])
        .output()
        .expect("run crabka format");
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
    // Format seeds KRaftVersion + every registered feature whose default at the
    // latest release is > 0 (metadata.version=25 KIP-778 + group.version=1
    // KIP-848 + transaction.version=2 KIP-890; share.version + streams.version
    // default to 0 and are omitted per KIP-1022) + SCRAM = 5 records.
    assert2::assert!(manifest.contains("\"record_count\": 5"));
    assert2::assert!(manifest.contains("cluster_id"));
    let bin_meta = std::fs::metadata(dir.path().join("bootstrap.records.bin"))
        .expect("bootstrap.records.bin must exist");
    assert2::assert!(bin_meta.len() > 0);
}

#[test]
fn format_low_iterations_fails() {
    let bin = env!("CARGO_BIN_EXE_crabka");
    let dir = tempfile::tempdir().unwrap();
    let out = Command::new(bin)
        .args([
            "format",
            "--log-dir",
            dir.path().to_str().unwrap(),
            "--add-scram",
            "SCRAM-SHA-512=[name=admin,password=p,iterations=1]",
        ])
        .output()
        .expect("run crabka format");
    assert2::assert!(!out.status.success());
    assert2::assert!(out.status.code() == Some(2));
}
