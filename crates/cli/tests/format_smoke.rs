//! Smoke tests: run the `crabka format` binary as a subprocess and
//! assert on its exit code + on-disk output. These tests prove the clap
//! surface + the bootstrap-write path survive end-to-end without booting
//! a broker.

use std::process::Command;

use assert2::assert;

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
    assert!(
        out.status.success(),
        "format failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    // Bootstrap manifest + binary records file should both exist and
    // be non-empty.
    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert!(!entries.is_empty(), "format must write something");
    let manifest = std::fs::read_to_string(dir.path().join("bootstrap.json"))
        .expect("bootstrap.json must exist");
    // Format seeds KRaftVersion + every registered feature whose default at the
    // latest release is > 0 (metadata.version=25 KIP-778 + group.version=1
    // KIP-848 + transaction.version=2 KIP-890; share.version + streams.version
    // default to 0 and are omitted per KIP-1022) + SCRAM = 5 records.
    assert!(
        manifest.contains("\"record_count\": 5"),
        "manifest must list KRaftVersion + metadata.version + group.version + transaction.version + one SCRAM record, got: {manifest}",
    );
    assert!(
        manifest.contains("cluster_id"),
        "manifest must carry a cluster id, got: {manifest}",
    );
    let bin_meta = std::fs::metadata(dir.path().join("bootstrap.records.bin"))
        .expect("bootstrap.records.bin must exist");
    assert!(
        bin_meta.len() > 0,
        "bootstrap.records.bin must be non-empty",
    );
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
    assert!(
        !out.status.success(),
        "must fail with iterations < 4096; stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.code() == Some(2),
        "expected exit code 2 (low iterations), got {:?}; stderr={}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
}
