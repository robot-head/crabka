// Bespoke tests for the owned DescribeGroups wrappers. The hand-written tests
// used non-standard function names ("roundtrip_default_*"). Both are preserved
// here for continuity. Relocated from hand-written wrappers.

use bytes::BytesMut;
use crabka_protocol::{
    Decode, Encode,
    owned::{
        describe_groups_request::{DescribeGroupsRequest, MAX_VERSION, MIN_VERSION},
        describe_groups_response::{
            DescribeGroupsResponse, MAX_VERSION as RESP_MAX, MIN_VERSION as RESP_MIN,
        },
    },
};

#[test]
fn owned_describe_groups_request_roundtrip_cases() {
    for (_case, version) in [("minimum", MIN_VERSION), ("maximum", MAX_VERSION)] {
        let request = DescribeGroupsRequest::default();
        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();
        assert2::assert!(request.encoded_len(version) == buf.len());
        let mut cur = &buf[..];
        assert2::assert!(
            (
                DescribeGroupsRequest::decode(&mut cur, version).unwrap(),
                cur.is_empty()
            ) == (request, true)
        );
    }
}

#[test]
fn owned_describe_groups_response_roundtrip_cases() {
    for (_case, version) in [("minimum", RESP_MIN), ("maximum", RESP_MAX)] {
        let response = DescribeGroupsResponse::default();
        let mut buf = BytesMut::new();
        response.encode(&mut buf, version).unwrap();
        assert2::assert!(response.encoded_len(version) == buf.len());
        let mut cur = &buf[..];
        assert2::assert!(
            (
                DescribeGroupsResponse::decode(&mut cur, version).unwrap(),
                cur.is_empty()
            ) == (response, true)
        );
    }
}
