mod support;
use bytes::BytesMut;
use crabka_protocol::{
    Decode, Encode,
    owned::{produce_request::ProduceRequest, produce_response::ProduceResponse},
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

/// Assemble the oracle JSON value for a default `ProduceRequest` at the given version.
///
/// The schema has no explicit defaults for `acks` or `timeoutMs`, so they are 0.
/// The JVM's `JsonConverter` requires these fields to be present.
fn request_oracle_value(_version: i16) -> serde_json::Value {
    // All supported versions (3-13) share the same default structure.
    // v3-12 use topic name; v13 uses topic UUID. For empty topicData the distinction
    // doesn't matter — the array is empty regardless.
    json!({
        "transactionalId": null,
        "acks": 0,
        "timeoutMs": 0,
        "topicData": []
    })
}

/// Assemble the oracle JSON value for a default `ProduceResponse` at the given version.
///
/// - `responses` is always an empty array.
/// - `throttleTimeMs` is present from v1+ (schema default 0). All our supported versions
///   are v3+, so it is always present.
/// - `logAppendTimeMs`, `logStartOffset`, `recordErrors`, `errorMessage` live in nested
///   partition structs and are absent when there are no partitions.
fn response_oracle_value(_version: i16) -> serde_json::Value {
    json!({
        "responses": [],
        "throttleTimeMs": 0
    })
}

#[test]
#[ignore = "requires JVM oracle"]
fn produce_request_default_byte_equal_every_version() {
    let mut o = oracle::shared();
    for version in 3..=13i16 {
        let req = ProduceRequest::default();
        let rust = rust_encode(&req, version);
        let oracle_json = request_oracle_value(version);
        // api_key=0, is_request=true
        let java = o.encode(0, version, true, &oracle_json);
        assert2::assert!(rust == java);
        // Also verify decode roundtrip
        let decoded: ProduceRequest = rust_decode(&rust, version);
        let re_encoded = rust_encode(&decoded, version);
        assert2::assert!(rust == re_encoded);
    }
}

#[test]
#[ignore = "requires JVM oracle"]
fn produce_response_default_byte_equal_every_version() {
    let mut o = oracle::shared();
    for version in 3..=13i16 {
        let resp = ProduceResponse::default();
        let rust = rust_encode(&resp, version);
        let oracle_json = response_oracle_value(version);
        // api_key=0, is_request=false
        let java = o.encode(0, version, false, &oracle_json);
        assert2::assert!(rust == java);
        // Also verify decode roundtrip
        let decoded: ProduceResponse = rust_decode(&rust, version);
        let re_encoded = rust_encode(&decoded, version);
        assert2::assert!(rust == re_encoded);
    }
}
