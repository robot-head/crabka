mod support;
use support::oracle;

use bytes::BytesMut;
use crabka_protocol::owned::offset_commit_request::OffsetCommitRequest;
use crabka_protocol::owned::offset_commit_response::OffsetCommitResponse;
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

/// Assemble the oracle JSON value for a default `OffsetCommitRequest` at the given version.
///
/// Schema defaults:
/// - `groupId`: required string, no explicit default → ""
/// - `generationIdOrMemberEpoch`: int32 v1+, default "-1"  → Rust default -1
/// - `memberId`: string v1+, no explicit default → ""
/// - `groupInstanceId`: nullable string v7+, default "null" → null
/// - `retentionTimeMs`: int64 v2-4, default "-1" → -1 (only present v2-4)
/// - `topics`: array v0+ → []
///
/// The JVM `JsonConverter` requires all fields present in the schema for that version to
/// be explicitly included; omitting a required field causes an error.
fn request_oracle_value(version: i16) -> serde_json::Value {
    match version {
        2..=4 => json!({
            "groupId": "",
            "generationIdOrMemberEpoch": -1,
            "memberId": "",
            "retentionTimeMs": -1,
            "topics": []
        }),
        5..=6 => json!({
            "groupId": "",
            "generationIdOrMemberEpoch": -1,
            "memberId": "",
            "topics": []
        }),
        7..=10 => json!({
            "groupId": "",
            "generationIdOrMemberEpoch": -1,
            "memberId": "",
            "groupInstanceId": null,
            "topics": []
        }),
        _ => json!({}),
    }
}

/// Assemble the oracle JSON value for a default `OffsetCommitResponse` at the given version.
///
/// Schema defaults:
/// - `throttleTimeMs`: int32 v3+, no explicit default → 0
/// - `topics`: array v0+ → []
fn response_oracle_value(version: i16) -> serde_json::Value {
    match version {
        2 => json!({
            "topics": []
        }),
        3..=10 => json!({
            "throttleTimeMs": 0,
            "topics": []
        }),
        _ => json!({}),
    }
}

#[test]
#[ignore = "requires JVM oracle"]
fn offset_commit_request_default_byte_equal_every_version() {
    let mut o = oracle::shared();
    for version in 2..=10i16 {
        let req = OffsetCommitRequest::default();
        let rust = rust_encode(&req, version);
        let oracle_json = request_oracle_value(version);
        // api_key=8, is_request=true
        let java = o.encode(8, version, true, &oracle_json);
        assert_eq!(
            rust,
            java,
            "OffsetCommitRequest v{version} byte mismatch\n  rust: {}\n  java: {}",
            hex::encode(&rust),
            hex::encode(&java),
        );
        // Also verify decode roundtrip
        let decoded: OffsetCommitRequest = rust_decode(&rust, version);
        let re_encoded = rust_encode(&decoded, version);
        assert_eq!(
            rust, re_encoded,
            "OffsetCommitRequest v{version} roundtrip mismatch after decode"
        );
    }
}

#[test]
#[ignore = "requires JVM oracle"]
fn offset_commit_response_default_byte_equal_every_version() {
    let mut o = oracle::shared();
    for version in 2..=10i16 {
        let resp = OffsetCommitResponse::default();
        let rust = rust_encode(&resp, version);
        let oracle_json = response_oracle_value(version);
        // api_key=8, is_request=false
        let java = o.encode(8, version, false, &oracle_json);
        assert_eq!(
            rust,
            java,
            "OffsetCommitResponse v{version} byte mismatch\n  rust: {}\n  java: {}",
            hex::encode(&rust),
            hex::encode(&java),
        );
        // Also verify decode roundtrip
        let decoded: OffsetCommitResponse = rust_decode(&rust, version);
        let re_encoded = rust_encode(&decoded, version);
        assert_eq!(
            rust, re_encoded,
            "OffsetCommitResponse v{version} roundtrip mismatch after decode"
        );
    }
}
