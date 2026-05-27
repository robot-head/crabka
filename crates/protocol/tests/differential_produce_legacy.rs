//! JVM differential tests for the `kafka_3_6_2` namespace `ProduceRequest` (v0–v2).
//!
//! These tests verify that Crabka's legacy-namespace encoder produces byte-for-byte
//! identical output to the JVM oracle (Kafka 4.3.0 `ProduceRequestData`) for the same
//! input at each pre-flexible version.
//!
//! All tests carry `#[ignore = "requires JVM oracle"]`.  They are skipped in a local
//! `cargo test` run and exercised in CI where the oracle is built and present.

mod support;
use support::oracle;

use bytes::BytesMut;
use crabka_protocol::kafka_3_6_2::owned::produce_request::ProduceRequest;
use crabka_protocol::{Decode, Encode};
use serde_json::json;

fn rust_encode<T: Encode>(t: &T, version: i16) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(t.encoded_len(version));
    t.encode(&mut buf, version).unwrap();
    buf.to_vec()
}

fn rust_decode<T: for<'a> Decode<'a>>(bytes: &[u8], version: i16) -> T {
    let mut cur: &[u8] = bytes;
    let v = T::decode(&mut cur, version).unwrap();
    assert!(
        cur.is_empty(),
        "Rust decoder left trailing bytes at version {version}"
    );
    v
}

/// Oracle JSON for a default `ProduceRequest` at v0–v2.
///
/// v0/v1/v2 do not have `transactionalId`; only `acks`, `timeoutMs`, and `topicData` are
/// present.  An empty `topicData` array means no records to encode, avoiding the legacy
/// message-format complexity entirely.
fn request_oracle_value() -> serde_json::Value {
    json!({
        "acks": 0,
        "timeoutMs": 0,
        "topicData": []
    })
}

#[test]
#[ignore = "requires JVM oracle"]
fn produce_request_byte_equal_every_version() {
    let mut o = oracle::shared();
    for version in 0..=2i16 {
        let req = ProduceRequest::default();
        let rust = rust_encode(&req, version);
        let oracle_json = request_oracle_value();
        // api_key=0 (Produce), is_request=true
        let java = o.encode(0, version, true, &oracle_json);
        assert_eq!(
            rust,
            java,
            "ProduceRequest v{version} byte mismatch\n  rust: {}\n  java: {}",
            hex::encode(&rust),
            hex::encode(&java),
        );
        // Verify decode roundtrip
        let decoded: ProduceRequest = rust_decode(&rust, version);
        let re_encoded = rust_encode(&decoded, version);
        assert_eq!(
            rust, re_encoded,
            "ProduceRequest v{version} roundtrip mismatch after decode"
        );
    }
}
