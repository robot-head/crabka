mod support;
use bytes::BytesMut;
use crabka_protocol::{
    Decode, Encode,
    owned::{metadata_request::MetadataRequest, metadata_response::MetadataResponse},
};
use serde_json::json;
use support::oracle;

fn rust_encode<T: Encode>(t: &T, version: i16) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(t.encoded_len(version));
    t.encode(&mut buf, version).unwrap();
    buf.to_vec()
}

fn rust_decode<T: for<'a> Decode<'a>>(bytes: &[u8], version: i16) -> T {
    let mut cur: &[u8] = bytes;
    let v = T::decode(&mut cur, version).unwrap();
    assert2::assert!(cur.is_empty());
    v
}

/// Assemble the oracle JSON value for a default `MetadataRequest` at the given version.
/// The key challenges:
/// - v0: topics must be an empty array (not null); default means "all topics".
/// - v1-3: topics=null (all topics), no other variable fields.
/// - v4-7: allowAutoTopicCreation present (schema default: true).
/// - v8-10: includeClusterAuthorizedOperations + includeTopicAuthorizedOperations.
/// - v9-10: same fields but flexible encoding.
/// - v11-13: includeClusterAuthorizedOperations removed; includeTopicAuthorizedOperations stays.
fn request_oracle_value(version: i16) -> serde_json::Value {
    match version {
        0 => json!({"topics": []}),
        1..=3 => json!({"topics": null}),
        4..=7 => json!({"topics": null, "allowAutoTopicCreation": true}),
        8..=10 => json!({
            "topics": null,
            "allowAutoTopicCreation": true,
            "includeClusterAuthorizedOperations": false,
            "includeTopicAuthorizedOperations": false
        }),
        11..=i16::MAX => json!({
            "topics": null,
            "allowAutoTopicCreation": true,
            "includeTopicAuthorizedOperations": false
        }),
        _ => json!({}),
    }
}

/// Assemble the oracle JSON value for a default `MetadataResponse` at the given version.
fn response_oracle_value(version: i16) -> serde_json::Value {
    match version {
        0 => json!({"brokers": [], "topics": []}),
        1 => json!({"brokers": [], "topics": [], "controllerId": -1}),
        2 => json!({"brokers": [], "topics": [], "clusterId": null, "controllerId": -1}),
        3..=7 | 11..=12 => json!({
            "brokers": [],
            "topics": [],
            "controllerId": -1,
            "throttleTimeMs": 0,
            "clusterId": null
        }),
        8..=10 => json!({
            "brokers": [],
            "topics": [],
            "controllerId": -1,
            "throttleTimeMs": 0,
            "clusterId": null,
            "clusterAuthorizedOperations": -2_147_483_648i64
        }),
        13..=i16::MAX => json!({
            "brokers": [],
            "topics": [],
            "controllerId": -1,
            "throttleTimeMs": 0,
            "clusterId": null,
            "errorCode": 0
        }),
        _ => json!({}),
    }
}

#[test]
#[ignore = "requires JVM oracle"]
fn metadata_request_default_byte_equal_every_version() {
    let mut o = oracle::shared();
    for version in 0..=13i16 {
        let req = MetadataRequest::default();
        let rust = rust_encode(&req, version);
        let oracle_json = request_oracle_value(version);
        let java = o.encode(3, version, true, &oracle_json);
        assert2::assert!(rust == java);
        // Also verify decode roundtrip
        let decoded: MetadataRequest = rust_decode(&rust, version);
        let re_encoded = rust_encode(&decoded, version);
        assert2::assert!(rust == re_encoded);
    }
}

#[test]
#[ignore = "requires JVM oracle"]
fn metadata_response_default_byte_equal_every_version() {
    let mut o = oracle::shared();
    for version in 0..=13i16 {
        let resp = MetadataResponse::default();
        let rust = rust_encode(&resp, version);
        let oracle_json = response_oracle_value(version);
        let java = o.encode(3, version, false, &oracle_json);
        assert2::assert!(rust == java);
        // Also verify decode roundtrip
        let decoded: MetadataResponse = rust_decode(&rust, version);
        let re_encoded = rust_encode(&decoded, version);
        assert2::assert!(rust == re_encoded);
    }
}
