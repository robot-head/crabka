// Bespoke tests for owned ApiVersions wrappers that go beyond the standard
// min/max-version roundtrip. Relocated here from the hand-written wrappers as
// part of making those wrappers uniformly generated.

use assert2::assert;
use bytes::BytesMut;
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};
use crabka_protocol::{Decode, Encode, UnknownTaggedFields};

#[test]
fn owned_api_versions_request_v0_is_empty() {
    let req = ApiVersionsRequest::default();
    let mut buf = BytesMut::new();
    req.encode(&mut buf, 0).unwrap();
    assert!(buf.is_empty());
    let mut cur = &buf[..];
    assert!(ApiVersionsRequest::decode(&mut cur, 0).unwrap() == req);
}

#[test]
fn owned_api_versions_request_v3_roundtrip() {
    let req = ApiVersionsRequest {
        client_software_name: "crabka".to_string(),
        client_software_version: "0.0.0".to_string(),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf, 3).unwrap();
    assert!(req.encoded_len(3) == buf.len());
    let mut cur = &buf[..];
    assert!(ApiVersionsRequest::decode(&mut cur, 3).unwrap() == req);
}

fn sample_response(version: i16) -> ApiVersionsResponse {
    ApiVersionsResponse {
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
        throttle_time_ms: if version >= 1 { 5 } else { 0 },
        ..Default::default()
    }
}

#[test]
fn owned_api_versions_response_v0_roundtrip() {
    let r = sample_response(0);
    let mut buf = BytesMut::new();
    r.encode(&mut buf, 0).unwrap();
    assert!(r.encoded_len(0) == buf.len());
    let mut cur = &buf[..];
    assert!(ApiVersionsResponse::decode(&mut cur, 0).unwrap() == r);
    assert!(cur.is_empty());
}

#[test]
fn owned_api_versions_response_v1_includes_throttle_time() {
    let r = sample_response(1);
    let mut buf = BytesMut::new();
    r.encode(&mut buf, 1).unwrap();
    assert!(r.encoded_len(1) == buf.len());
    let mut cur = &buf[..];
    assert!(ApiVersionsResponse::decode(&mut cur, 1).unwrap() == r);
    assert!(cur.is_empty());
}

#[test]
fn owned_api_versions_response_v3_flexible_roundtrip() {
    let r = sample_response(3);
    let mut buf = BytesMut::new();
    r.encode(&mut buf, 3).unwrap();
    assert!(r.encoded_len(3) == buf.len());
    let mut cur = &buf[..];
    assert!(ApiVersionsResponse::decode(&mut cur, 3).unwrap() == r);
    assert!(cur.is_empty());
}
