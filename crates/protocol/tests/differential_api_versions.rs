mod support;
use support::oracle;

use bytes::BytesMut;
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};
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
    assert_eq!(
        rust, java,
        "v0 byte mismatch\n  rust: {rust:?}\n  java: {java:?}",
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
    assert_eq!(
        rust, java,
        "v3 byte mismatch\n  rust: {rust:?}\n  java: {java:?}",
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
    assert_eq!(
        rust,
        java,
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
    assert_eq!(decoded.api_keys.len(), 1);
    assert_eq!(decoded.api_keys[0].api_key, 18);
    assert_eq!(decoded.throttle_time_ms, 0);
}
