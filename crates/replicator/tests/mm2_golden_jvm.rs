//! Byte-exactness proof: Crabka's MirrorMaker-2 record codecs produce and
//! consume the *same* bytes as the real JVM MM2 classes.
//!
//! The golden vectors in `tests/fixtures/mm2_serde_golden.json` were captured
//! from `mirror.gcr.io/apache/kafka:4.0.0`'s
//! `org.apache.kafka.connect.mirror.{Heartbeat,Checkpoint,OffsetSync}`
//! by `scripts/capture-mm2-golden/Capture.java` (see that directory's README
//! for how to reproduce). The fixture is the committed source of truth; the
//! capture program documents how to regenerate it.
//!
//! For every record we assert BOTH directions:
//!   1. `record.key_bytes()   == golden("..._key")`   — encode matches JVM.
//!   2. `record.value_bytes() == golden("..._value")` — encode matches JVM.
//!   3. `Record::from_bytes(golden_key, golden_value) == record` — decode JVM.
//!
//! The FIXED constants below MUST match `scripts/capture-mm2-golden/Capture.java`:
//!   source     = "us-east"
//!   target     = "eu-west"
//!   timestamp  = 100
//!   group      = "analytics"
//!   topic      = "orders"
//!   partition  = 7
//!   upstream   = 1000
//!   downstream = 742
//!   metadata   = "" (empty string)

use std::collections::HashMap;
use std::fmt::Write as _;

use crabka_replicator::mm2::{Checkpoint, Heartbeat, OffsetSync};

/// Load and hex-decode one named golden vector from the committed fixture.
fn golden(name: &str) -> Vec<u8> {
    let raw = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/mm2_serde_golden.json"
    ))
    .expect("read mm2_serde_golden.json");
    let map: HashMap<String, String> =
        serde_json::from_str(&raw).expect("parse mm2_serde_golden.json");
    let hex = map
        .get(name)
        .unwrap_or_else(|| panic!("no golden case {name}"));
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Assert one encoded buffer matches its JVM golden vector byte-for-byte.
fn assert_bytes(case: &str, got: &[u8]) {
    let want = golden(case);
    assert_eq!(
        got,
        want.as_slice(),
        "case `{case}`: Crabka encode != JVM golden\n  got : {}\n  want: {}",
        hex(got),
        hex(&want),
    );
}

fn heartbeat() -> Heartbeat {
    Heartbeat {
        source: "us-east".into(),
        target: "eu-west".into(),
        timestamp_ms: 100,
    }
}

fn checkpoint() -> Checkpoint {
    Checkpoint {
        group: "analytics".into(),
        topic: "orders".into(),
        partition: 7,
        upstream: 1000,
        downstream: 742,
        metadata: String::new(),
    }
}

fn offset_sync() -> OffsetSync {
    OffsetSync {
        topic: "orders".into(),
        partition: 7,
        upstream: 1000,
        downstream: 742,
    }
}

#[test]
fn heartbeat_matches_jvm() {
    let hb = heartbeat();
    assert_bytes("heartbeat_key", &hb.key_bytes());
    assert_bytes("heartbeat_value", &hb.value_bytes());
    let decoded = Heartbeat::from_bytes(&golden("heartbeat_key"), &golden("heartbeat_value"))
        .expect("decode JVM heartbeat bytes");
    assert_eq!(decoded, hb, "Crabka decode(JVM heartbeat) != record");
}

#[test]
fn checkpoint_matches_jvm() {
    let c = checkpoint();
    assert_bytes("checkpoint_key", &c.key_bytes());
    assert_bytes("checkpoint_value", &c.value_bytes());
    let decoded = Checkpoint::from_bytes(&golden("checkpoint_key"), &golden("checkpoint_value"))
        .expect("decode JVM checkpoint bytes");
    assert_eq!(decoded, c, "Crabka decode(JVM checkpoint) != record");
}

#[test]
fn offset_sync_matches_jvm() {
    let os = offset_sync();
    assert_bytes("offset_sync_key", &os.key_bytes());
    assert_bytes("offset_sync_value", &os.value_bytes());
    let decoded = OffsetSync::from_bytes(&golden("offset_sync_key"), &golden("offset_sync_value"))
        .expect("decode JVM offset_sync bytes");
    assert_eq!(decoded, os, "Crabka decode(JVM offset_sync) != record");
}
