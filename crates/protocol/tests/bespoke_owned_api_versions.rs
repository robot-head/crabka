// Bespoke tests for owned ApiVersions wrappers that go beyond the standard
// min/max-version roundtrip. Relocated here from the hand-written wrappers as
// part of making those wrappers uniformly generated.

use bytes::BytesMut;
use crabka_protocol::{
    Decode, Encode, UnknownTaggedFields,
    owned::{
        api_versions_request::ApiVersionsRequest,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
    },
};

#[test]
fn owned_api_versions_request_roundtrip_cases() {
    for (_case, version, req) in [
        ("v0 empty", 0, ApiVersionsRequest::default()),
        (
            "v3 populated",
            3,
            ApiVersionsRequest {
                client_software_name: "crabka".to_string(),
                client_software_version: "0.0.0".to_string(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
                ..Default::default()
            },
        ),
        (
            "v5 routing identity",
            5,
            ApiVersionsRequest {
                client_software_name: "crabka".to_string(),
                client_software_version: "0.0.0".to_string(),
                cluster_id: Some("cluster".to_string()),
                node_id: 7,
                ..Default::default()
            },
        ),
    ] {
        let mut buf = BytesMut::new();
        req.encode(&mut buf, version).unwrap();
        assert2::assert!(req.encoded_len(version) == buf.len());
        let mut cur = &buf[..];
        assert2::assert!(
            (
                ApiVersionsRequest::decode(&mut cur, version).unwrap(),
                cur.is_empty()
            ) == (req, true)
        );
    }
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
fn owned_api_versions_response_roundtrip_cases() {
    for (_case, version) in [("v0", 0), ("v1 throttle", 1), ("v3 flexible", 3)] {
        let response = sample_response(version);
        let mut buf = BytesMut::new();
        response.encode(&mut buf, version).unwrap();
        assert2::assert!(response.encoded_len(version) == buf.len());
        let mut cur = &buf[..];
        assert2::assert!(
            (
                ApiVersionsResponse::decode(&mut cur, version).unwrap(),
                cur.is_empty()
            ) == (response, true)
        );
    }
}
