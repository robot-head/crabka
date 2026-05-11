mod support;
use support::oracle;

use bytes::BytesMut;
use crabka_protocol::owned::describe_groups_request::DescribeGroupsRequest;
use crabka_protocol::owned::describe_groups_response::DescribeGroupsResponse;
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

/// Assemble the oracle JSON value for a default `DescribeGroupsRequest` at the given version.
///
/// Schema defaults:
/// - `groups`: array v0+, no explicit default → []
/// - `includeAuthorizedOperations`: bool v3+, no explicit default → false
fn request_oracle_value(version: i16) -> serde_json::Value {
    match version {
        0..=2 => json!({
            "groups": []
        }),
        3..=6 => json!({
            "groups": [],
            "includeAuthorizedOperations": false
        }),
        _ => json!({}),
    }
}

/// Assemble the oracle JSON value for a default `DescribeGroupsResponse` at the given version.
///
/// Schema defaults:
/// - `throttleTimeMs`: int32 v1+, ignorable, no explicit default → 0
/// - `groups`: array v0+, no explicit default → []
fn response_oracle_value(version: i16) -> serde_json::Value {
    match version {
        0 => json!({
            "groups": []
        }),
        1..=6 => json!({
            "throttleTimeMs": 0,
            "groups": []
        }),
        _ => json!({}),
    }
}

#[test]
#[ignore = "requires JVM oracle"]
fn describe_groups_request_default_byte_equal_every_version() {
    let mut o = oracle::shared();
    for version in 0..=6i16 {
        let req = DescribeGroupsRequest::default();
        let rust = rust_encode(&req, version);
        let oracle_json = request_oracle_value(version);
        // api_key=15, is_request=true
        let java = o.encode(15, version, true, &oracle_json);
        assert_eq!(
            rust,
            java,
            "DescribeGroupsRequest v{version} byte mismatch\n  rust: {}\n  java: {}",
            hex::encode(&rust),
            hex::encode(&java),
        );
        // Also verify decode roundtrip
        let decoded: DescribeGroupsRequest = rust_decode(&rust, version);
        let re_encoded = rust_encode(&decoded, version);
        assert_eq!(
            rust, re_encoded,
            "DescribeGroupsRequest v{version} roundtrip mismatch after decode"
        );
    }
}

#[test]
#[ignore = "requires JVM oracle"]
fn describe_groups_response_default_byte_equal_every_version() {
    let mut o = oracle::shared();
    for version in 0..=6i16 {
        let resp = DescribeGroupsResponse::default();
        let rust = rust_encode(&resp, version);
        let oracle_json = response_oracle_value(version);
        // api_key=15, is_request=false
        let java = o.encode(15, version, false, &oracle_json);
        assert_eq!(
            rust,
            java,
            "DescribeGroupsResponse v{version} byte mismatch\n  rust: {}\n  java: {}",
            hex::encode(&rust),
            hex::encode(&java),
        );
        // Also verify decode roundtrip
        let decoded: DescribeGroupsResponse = rust_decode(&rust, version);
        let re_encoded = rust_encode(&decoded, version);
        assert_eq!(
            rust, re_encoded,
            "DescribeGroupsResponse v{version} roundtrip mismatch after decode"
        );
    }
}
