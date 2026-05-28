// Bespoke tests for borrowed DescribeGroups wrappers. The hand-written tests
// checked .groups.len() which the generated min/max test doesn't assert on.
// Relocated from hand-written wrappers.

use bytes::BytesMut;
use crabka_protocol::borrowed::describe_groups_request::{
    DescribeGroupsRequest, MAX_VERSION, MIN_VERSION,
};
use crabka_protocol::borrowed::describe_groups_response::{
    DescribeGroupsResponse, MAX_VERSION as RESP_MAX, MIN_VERSION as RESP_MIN,
};
use crabka_protocol::{DecodeBorrow, Encode};

#[test]
fn borrowed_describe_groups_request_min_version_groups_empty() {
    let req = DescribeGroupsRequest::default();
    let mut buf = BytesMut::new();
    req.encode(&mut buf, MIN_VERSION).unwrap();
    assert_eq!(req.encoded_len(MIN_VERSION), buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = DescribeGroupsRequest::decode_borrow(&mut cur, MIN_VERSION).unwrap();
    assert!(cur.is_empty());
    assert_eq!(decoded.groups.len(), req.groups.len());
}

#[test]
fn borrowed_describe_groups_request_max_version_groups_empty() {
    let req = DescribeGroupsRequest::default();
    let mut buf = BytesMut::new();
    req.encode(&mut buf, MAX_VERSION).unwrap();
    assert_eq!(req.encoded_len(MAX_VERSION), buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = DescribeGroupsRequest::decode_borrow(&mut cur, MAX_VERSION).unwrap();
    assert!(cur.is_empty());
    assert_eq!(decoded.groups.len(), req.groups.len());
}

#[test]
fn borrowed_describe_groups_response_min_version_groups_empty() {
    let resp = DescribeGroupsResponse::default();
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, RESP_MIN).unwrap();
    assert_eq!(resp.encoded_len(RESP_MIN), buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = DescribeGroupsResponse::decode_borrow(&mut cur, RESP_MIN).unwrap();
    assert!(cur.is_empty());
    assert_eq!(decoded.groups.len(), resp.groups.len());
}

#[test]
fn borrowed_describe_groups_response_max_version_groups_empty() {
    let resp = DescribeGroupsResponse::default();
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, RESP_MAX).unwrap();
    assert_eq!(resp.encoded_len(RESP_MAX), buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = DescribeGroupsResponse::decode_borrow(&mut cur, RESP_MAX).unwrap();
    assert!(cur.is_empty());
    assert_eq!(decoded.groups.len(), resp.groups.len());
}
