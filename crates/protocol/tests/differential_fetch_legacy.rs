//! JVM differential tests for the `kafka_3_6_2` namespace `FetchRequest` (v0–v3).
//!
//! These tests verify that Crabka's legacy-namespace encoder produces byte-for-byte
//! identical output to the JVM oracle (Kafka 4.3.0 `FetchRequestData`) for the same
//! input at each version in the range v0–v3.
//!
//! All tests carry `#[ignore = "requires JVM oracle"]`.  They are skipped in a local
//! `cargo test` run and exercised in CI where the oracle is built and present.

mod support;
use support::oracle;

use bytes::BytesMut;
use crabka_protocol::kafka_3_6_2::owned::fetch_request::FetchRequest;
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

/// Oracle JSON for a default `FetchRequest` at v0–v2.
///
/// v0/v1/v2 include only `replicaId`, `maxWaitMs`, `minBytes`, and `topics`.
/// `replicaId` defaults to -1 (consumer) in the Rust struct and must be
/// supplied explicitly in the oracle JSON because the JVM schema has no default.
fn request_oracle_value_v0_v2() -> serde_json::Value {
    json!({
        "replicaId": -1,
        "maxWaitMs": 0,
        "minBytes": 0,
        "topics": []
    })
}

/// Oracle JSON for a default `FetchRequest` at v3.
///
/// v3 adds `maxBytes` (schema default 0x7fffffff).  The Rust `Default`
/// implementation sets `max_bytes` to `2_147_483_647` (i.e. `i32::MAX`), which
/// matches the schema default, so both sides agree without special-casing.
fn request_oracle_value_v3() -> serde_json::Value {
    json!({
        "replicaId": -1,
        "maxWaitMs": 0,
        "minBytes": 0,
        "maxBytes": 2_147_483_647i64,
        "topics": []
    })
}

#[test]
#[ignore = "requires JVM oracle"]
fn fetch_request_v0_byte_equal() {
    let mut o = oracle::shared();
    let req = FetchRequest::default();
    let rust = rust_encode(&req, 0);
    let oracle_json = request_oracle_value_v0_v2();
    // api_key=1 (Fetch), is_request=true
    let java = o.encode(1, 0, true, &oracle_json);
    assert_eq!(
        rust,
        java,
        "FetchRequest v0 byte mismatch\n  rust: {}\n  java: {}",
        hex::encode(&rust),
        hex::encode(&java),
    );
    // Verify decode roundtrip
    let decoded: FetchRequest = rust_decode(&rust, 0);
    let re_encoded = rust_encode(&decoded, 0);
    assert_eq!(
        rust, re_encoded,
        "FetchRequest v0 roundtrip mismatch after decode"
    );
}

#[test]
#[ignore = "requires JVM oracle"]
fn fetch_request_v1_byte_equal() {
    let mut o = oracle::shared();
    let req = FetchRequest::default();
    let rust = rust_encode(&req, 1);
    let oracle_json = request_oracle_value_v0_v2();
    let java = o.encode(1, 1, true, &oracle_json);
    assert_eq!(
        rust,
        java,
        "FetchRequest v1 byte mismatch\n  rust: {}\n  java: {}",
        hex::encode(&rust),
        hex::encode(&java),
    );
    let decoded: FetchRequest = rust_decode(&rust, 1);
    let re_encoded = rust_encode(&decoded, 1);
    assert_eq!(
        rust, re_encoded,
        "FetchRequest v1 roundtrip mismatch after decode"
    );
}

#[test]
#[ignore = "requires JVM oracle"]
fn fetch_request_v2_byte_equal() {
    let mut o = oracle::shared();
    let req = FetchRequest::default();
    let rust = rust_encode(&req, 2);
    let oracle_json = request_oracle_value_v0_v2();
    let java = o.encode(1, 2, true, &oracle_json);
    assert_eq!(
        rust,
        java,
        "FetchRequest v2 byte mismatch\n  rust: {}\n  java: {}",
        hex::encode(&rust),
        hex::encode(&java),
    );
    let decoded: FetchRequest = rust_decode(&rust, 2);
    let re_encoded = rust_encode(&decoded, 2);
    assert_eq!(
        rust, re_encoded,
        "FetchRequest v2 roundtrip mismatch after decode"
    );
}

#[test]
#[ignore = "requires JVM oracle"]
fn fetch_request_v3_byte_equal() {
    let mut o = oracle::shared();
    let req = FetchRequest::default();
    let rust = rust_encode(&req, 3);
    let oracle_json = request_oracle_value_v3();
    let java = o.encode(1, 3, true, &oracle_json);
    assert_eq!(
        rust,
        java,
        "FetchRequest v3 byte mismatch\n  rust: {}\n  java: {}",
        hex::encode(&rust),
        hex::encode(&java),
    );
    let decoded: FetchRequest = rust_decode(&rust, 3);
    let re_encoded = rust_encode(&decoded, 3);
    assert_eq!(
        rust, re_encoded,
        "FetchRequest v3 roundtrip mismatch after decode"
    );
}
