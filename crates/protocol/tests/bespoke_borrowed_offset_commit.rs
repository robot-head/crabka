// Bespoke tests for borrowed OffsetCommit wrappers. Relocated from
// hand-written wrappers.

use bytes::BytesMut;
use crabka_protocol::borrowed::offset_commit_request::{
    MAX_VERSION, MIN_VERSION, OffsetCommitRequest,
};
use crabka_protocol::borrowed::offset_commit_response::{
    MAX_VERSION as RESP_MAX, MIN_VERSION as RESP_MIN, OffsetCommitResponse,
};
use crabka_protocol::{DecodeBorrow, Encode, UnknownTaggedFields};

#[test]
fn borrowed_offset_commit_request_min_version_empty_topics() {
    let req = OffsetCommitRequest::default();
    let mut buf = BytesMut::new();
    req.encode(&mut buf, MIN_VERSION).unwrap();
    assert_eq!(req.encoded_len(MIN_VERSION), buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = OffsetCommitRequest::decode_borrow(&mut cur, MIN_VERSION).unwrap();
    assert!(cur.is_empty(), "decoder left trailing bytes");
    assert_eq!(decoded.topics.len(), 0);
}

#[test]
fn borrowed_offset_commit_request_max_version_specific_values() {
    let req = OffsetCommitRequest {
        group_id: "test-group",
        generation_id_or_member_epoch: -1i32,
        member_id: "",
        group_instance_id: None,
        retention_time_ms: -1i64,
        topics: Vec::new(),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf, MAX_VERSION).unwrap();
    assert_eq!(req.encoded_len(MAX_VERSION), buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = OffsetCommitRequest::decode_borrow(&mut cur, MAX_VERSION).unwrap();
    assert!(cur.is_empty());
    assert_eq!(decoded.group_id, req.group_id);
    assert_eq!(decoded.topics, req.topics);
}

#[test]
fn borrowed_offset_commit_response_min_version_empty_topics() {
    let resp = OffsetCommitResponse::default();
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, RESP_MIN).unwrap();
    assert_eq!(resp.encoded_len(RESP_MIN), buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = OffsetCommitResponse::decode_borrow(&mut cur, RESP_MIN).unwrap();
    assert!(cur.is_empty(), "decoder left trailing bytes");
    assert_eq!(decoded.topics.len(), 0);
}

#[test]
fn borrowed_offset_commit_response_max_version_roundtrips() {
    let resp = OffsetCommitResponse {
        throttle_time_ms: 0i32,
        topics: Vec::new(),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, RESP_MAX).unwrap();
    assert_eq!(resp.encoded_len(RESP_MAX), buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = OffsetCommitResponse::decode_borrow(&mut cur, RESP_MAX).unwrap();
    assert!(cur.is_empty());
    assert_eq!(decoded.topics, resp.topics);
}
