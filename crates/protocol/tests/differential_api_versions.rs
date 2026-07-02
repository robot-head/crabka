use assert2::assert;
mod support;
use support::oracle;

use bytes::BytesMut;
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};
use crabka_protocol::{Decode, Encode, UnknownTaggedFields};
use serde_json::json;

fn rust_encode<T: Encode>(t: &T, version: i16) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(t.encoded_len(version));
    t.encode(&mut buf, version).unwrap();
    buf.to_vec()
}

fn rust_decode<T: for<'a> Decode<'a>>(bytes: &[u8], version: i16) -> T {
    let mut cur: &[u8] = bytes;
    let v = T::decode(&mut cur, version).unwrap();
    assert!(cur.is_empty(), "Rust decoder left trailing bytes");
    v
}

#[test]
#[ignore = "requires JVM oracle"]
fn apiversions_request_v0_byte_equal() {
    let mut o = oracle::shared();
    let req = ApiVersionsRequest::default();
    let rust = rust_encode(&req, 0);
    let java = o.encode(18, 0, true, &json!({}));
    assert!(
        rust == java,
        "v0 byte mismatch\n  rust: {rust:?}\n  java: {java:?}"
    );
}

#[test]
#[ignore = "requires JVM oracle"]
fn apiversions_request_v3_byte_equal() {
    let mut o = oracle::shared();
    let req = ApiVersionsRequest {
        client_software_name: "crabka".into(),
        client_software_version: "0.0.0".into(),
        unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
    };
    let rust = rust_encode(&req, 3);
    let java = o.encode(
        18,
        3,
        true,
        &json!({
            "clientSoftwareName": "crabka",
            "clientSoftwareVersion": "0.0.0",
        }),
    );
    assert!(
        rust == java,
        "v3 byte mismatch\n  rust: {rust:?}\n  java: {java:?}"
    );
}

#[test]
#[ignore = "requires JVM oracle"]
fn apiversions_response_v3_byte_equal() {
    let mut o = oracle::shared();
    let resp = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![
            ApiVersion {
                api_key: 0,
                min_version: 0,
                max_version: 10,
                ..Default::default()
            },
            ApiVersion {
                api_key: 1,
                min_version: 0,
                max_version: 17,
                ..Default::default()
            },
        ],
        throttle_time_ms: 5,
        supported_features: vec![],
        finalized_features_epoch: -1,
        finalized_features: vec![],
        zk_migration_ready: false,
        unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
    };
    let rust = rust_encode(&resp, 3);
    let java = o.encode(
        18,
        3,
        false,
        &json!({
            "errorCode": 0,
            "apiKeys": [
                {"apiKey": 0, "minVersion": 0, "maxVersion": 10},
                {"apiKey": 1, "minVersion": 0, "maxVersion": 17},
            ],
            "throttleTimeMs": 5,
        }),
    );
    assert!(
        rust == java,
        "v3 response byte mismatch\n  rust hex: {}\n  java hex: {}",
        hex::encode(&rust),
        hex::encode(&java)
    );
}

#[test]
#[ignore = "requires JVM oracle"]
fn apiversions_response_decode_matches_java() {
    let mut o = oracle::shared();
    let java = o.encode(
        18,
        3,
        false,
        &json!({
            "errorCode": 0,
            "apiKeys": [{"apiKey": 18, "minVersion": 0, "maxVersion": 3}],
            "throttleTimeMs": 0,
        }),
    );
    let decoded: ApiVersionsResponse = rust_decode(&java, 3);
    let expected = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![ApiVersion {
            api_key: 18,
            min_version: 0,
            max_version: 3,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        throttle_time_ms: 0,
        supported_features: vec![],
        finalized_features_epoch: -1,
        finalized_features: vec![],
        zk_migration_ready: false,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(decoded == expected);
}
