// Bespoke tests for the owned OffsetCommit wrappers. Relocated from
// hand-written wrappers.

use bytes::BytesMut;
use crabka_protocol::{
    Decode, Encode, UnknownTaggedFields,
    owned::{
        offset_commit_request::{MAX_VERSION, MIN_VERSION, OffsetCommitRequest},
        offset_commit_response::{
            MAX_VERSION as RESP_MAX, MIN_VERSION as RESP_MIN, OffsetCommitResponse,
        },
    },
};

#[test]
fn owned_offset_commit_request_min_version_group_id_preserved() {
    let req = OffsetCommitRequest::default();
    let mut buf = BytesMut::new();
    req.encode(&mut buf, MIN_VERSION).unwrap();
    assert2::assert!(req.encoded_len(MIN_VERSION) == buf.len());
    let mut cur = &buf[..];
    let decoded = OffsetCommitRequest::decode(&mut cur, MIN_VERSION).unwrap();
    assert2::assert!(cur.is_empty());
    assert2::assert!(decoded == req);
}

#[test]
fn owned_offset_commit_request_max_version_roundtrips() {
    let req = OffsetCommitRequest {
        group_id: "test-group".to_string(),
        generation_id_or_member_epoch: -1i32,
        member_id: String::new(),
        group_instance_id: None,
        retention_time_ms: -1i64,
        topics: Vec::new(),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf, MAX_VERSION).unwrap();
    assert2::assert!(req.encoded_len(MAX_VERSION) == buf.len());
    let mut cur = &buf[..];
    assert2::assert!(OffsetCommitRequest::decode(&mut cur, MAX_VERSION).unwrap() == req);
    assert2::assert!(cur.is_empty());
}

#[test]
fn owned_offset_commit_response_min_version_empty_topics() {
    let resp = OffsetCommitResponse::default();
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, RESP_MIN).unwrap();
    assert2::assert!(resp.encoded_len(RESP_MIN) == buf.len());
    let mut cur = &buf[..];
    let decoded = OffsetCommitResponse::decode(&mut cur, RESP_MIN).unwrap();
    assert2::assert!(cur.is_empty());
    assert2::assert!(decoded.topics.len() == 0);
}

#[test]
fn owned_offset_commit_response_max_version_roundtrips() {
    let resp = OffsetCommitResponse {
        throttle_time_ms: 0i32,
        topics: Vec::new(),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, RESP_MAX).unwrap();
    assert2::assert!(resp.encoded_len(RESP_MAX) == buf.len());
    let mut cur = &buf[..];
    assert2::assert!(OffsetCommitResponse::decode(&mut cur, RESP_MAX).unwrap() == resp);
    assert2::assert!(cur.is_empty());
}
