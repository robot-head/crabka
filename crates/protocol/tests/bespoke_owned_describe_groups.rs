// Bespoke tests for the owned DescribeGroups wrappers. The hand-written tests
// used non-standard function names ("roundtrip_default_*"). Both are preserved
// here for continuity. Relocated from hand-written wrappers.

use bytes::BytesMut;
use crabka_protocol::owned::describe_groups_request::{
    DescribeGroupsRequest, MAX_VERSION, MIN_VERSION,
};
use crabka_protocol::owned::describe_groups_response::{
    DescribeGroupsResponse, MAX_VERSION as RESP_MAX, MIN_VERSION as RESP_MIN,
};
use crabka_protocol::{Decode, Encode};

#[test]
fn owned_describe_groups_request_roundtrip_default_min_version() {
    let req = DescribeGroupsRequest::default();
    let mut buf = BytesMut::new();
    req.encode(&mut buf, MIN_VERSION).unwrap();
    assert_eq!(req.encoded_len(MIN_VERSION), buf.len());
    let mut cur = &buf[..];
    assert_eq!(
        DescribeGroupsRequest::decode(&mut cur, MIN_VERSION).unwrap(),
        req
    );
    assert!(cur.is_empty());
}

#[test]
fn owned_describe_groups_request_roundtrip_default_max_version() {
    let req = DescribeGroupsRequest::default();
    let mut buf = BytesMut::new();
    req.encode(&mut buf, MAX_VERSION).unwrap();
    assert_eq!(req.encoded_len(MAX_VERSION), buf.len());
    let mut cur = &buf[..];
    assert_eq!(
        DescribeGroupsRequest::decode(&mut cur, MAX_VERSION).unwrap(),
        req
    );
    assert!(cur.is_empty());
}

#[test]
fn owned_describe_groups_response_roundtrip_default_min_version() {
    let resp = DescribeGroupsResponse::default();
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, RESP_MIN).unwrap();
    assert_eq!(resp.encoded_len(RESP_MIN), buf.len());
    let mut cur = &buf[..];
    assert_eq!(
        DescribeGroupsResponse::decode(&mut cur, RESP_MIN).unwrap(),
        resp
    );
    assert!(cur.is_empty());
}

#[test]
fn owned_describe_groups_response_roundtrip_default_max_version() {
    let resp = DescribeGroupsResponse::default();
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, RESP_MAX).unwrap();
    assert_eq!(resp.encoded_len(RESP_MAX), buf.len());
    let mut cur = &buf[..];
    assert_eq!(
        DescribeGroupsResponse::decode(&mut cur, RESP_MAX).unwrap(),
        resp
    );
    assert!(cur.is_empty());
}
