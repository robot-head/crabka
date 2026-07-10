//! Docker-gated: a Crabka-built `bootstrap.checkpoint` is parsed cleanly by the
//! JVM `kafka-dump-log --cluster-metadata-decoder`.
//!
//! ```text
//! cargo test -p crabka-broker --test kraft_checkpoint_jvm -- --ignored --nocapture
//! ```

use std::{io::Write, process::Command};

use crabka_protocol::records::metadata::checkpoint::build_bootstrap_checkpoint;

const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.0.0";

#[test]
#[ignore = "requires Docker"]
fn jvm_dump_log_parses_crabka_bootstrap_checkpoint() {
    let bytes = build_bootstrap_checkpoint(&[
        ("metadata.version", 25),
        ("group.version", 1),
        ("transaction.version", 2),
    ]);
    let dir = tempfile::tempdir().expect("tempdir");
    // kafka-dump-log infers the base offset from the file name; a real
    // bootstrap.checkpoint is named `bootstrap.checkpoint`.
    let path = dir.path().join("bootstrap.checkpoint");
    std::fs::File::create(&path)
        .unwrap()
        .write_all(&bytes)
        .unwrap();

    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &format!("{}:/work", dir.path().display()),
            KAFKA_IMAGE,
            "/opt/kafka/bin/kafka-dump-log.sh",
            "--cluster-metadata-decoder",
            "--files",
            "/work/bootstrap.checkpoint",
        ])
        .output()
        .expect("docker run kafka-dump-log");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    eprintln!("{text}");
    assert2::assert!(out.status.success());
    // (needle, expected presence in the dump-log output)
    let cases = [
        ("SnapshotHeader", true),
        ("FEATURE_LEVEL_RECORD", true),
        ("metadata.version", true),
        ("SnapshotFooter", true),
        // `isvalid: false` would mean a batch failed CRC validation.
        ("isvalid: false", false),
    ];
    for (needle, expected) in cases {
        assert2::assert!(text.contains(needle) == expected);
    }
}
