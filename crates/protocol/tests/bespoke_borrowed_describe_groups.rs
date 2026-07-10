// Bespoke tests for borrowed DescribeGroups wrappers. The hand-written tests
// checked .groups.len() which the generated min/max test doesn't assert on.
// Relocated from hand-written wrappers.

use assert2::assert;
use bytes::BytesMut;
use crabka_protocol::{
    DecodeBorrow, Encode,
    borrowed::{
        describe_groups_request::{DescribeGroupsRequest, MAX_VERSION, MIN_VERSION},
        describe_groups_response::{
            DescribeGroupsResponse, MAX_VERSION as RESP_MAX, MIN_VERSION as RESP_MIN,
        },
    },
};

#[test]
fn borrowed_describe_groups_request_cases() {
    for (case, version) in [("minimum", MIN_VERSION), ("maximum", MAX_VERSION)] {
        let request = DescribeGroupsRequest::default();
        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();
        assert!(request.encoded_len(version) == buf.len(), "case {case}");
        let frozen = buf.freeze();
        let mut cur = &frozen[..];
        let decoded = DescribeGroupsRequest::decode_borrow(&mut cur, version).unwrap();
        assert!((decoded, cur.is_empty()) == (request, true), "case {case}");
    }
}

#[test]
fn borrowed_describe_groups_response_cases() {
    for (case, version) in [("minimum", RESP_MIN), ("maximum", RESP_MAX)] {
        let response = DescribeGroupsResponse::default();
        let mut buf = BytesMut::new();
        response.encode(&mut buf, version).unwrap();
        assert!(response.encoded_len(version) == buf.len(), "case {case}");
        let frozen = buf.freeze();
        let mut cur = &frozen[..];
        let decoded = DescribeGroupsResponse::decode_borrow(&mut cur, version).unwrap();
        assert!((decoded, cur.is_empty()) == (response, true), "case {case}");
    }
}
