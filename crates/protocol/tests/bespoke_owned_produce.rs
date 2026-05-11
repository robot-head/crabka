// Bespoke tests for the owned Produce wrappers (checks beyond default min/max).
// Relocated from hand-written wrappers (sub-plan 1d, Task 1).

use bytes::BytesMut;
use crabka_protocol::owned::produce_request::{MAX_VERSION, MIN_VERSION, ProduceRequest};
use crabka_protocol::owned::produce_response::{
    MAX_VERSION as RESP_MAX, MIN_VERSION as RESP_MIN, ProduceResponse,
};
use crabka_protocol::{Decode, Encode, UnknownTaggedFields};

#[test]
fn owned_produce_request_min_version_empty_topics() {
    let req = ProduceRequest::default();
    let mut buf = BytesMut::new();
    req.encode(&mut buf, MIN_VERSION).unwrap();
    assert_eq!(req.encoded_len(MIN_VERSION), buf.len());
    let mut cur = &buf[..];
    let decoded = ProduceRequest::decode(&mut cur, MIN_VERSION).unwrap();
    assert!(cur.is_empty(), "decoder left trailing bytes");
    assert_eq!(decoded.acks, req.acks);
    assert_eq!(decoded.timeout_ms, req.timeout_ms);
    assert_eq!(decoded.topic_data.len(), 0);
}

#[test]
fn owned_produce_request_max_version_roundtrips() {
    let req = ProduceRequest {
        transactional_id: None,
        acks: 1i16,
        timeout_ms: 30_000i32,
        topic_data: Vec::new(),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf, MAX_VERSION).unwrap();
    assert_eq!(req.encoded_len(MAX_VERSION), buf.len());
    let mut cur = &buf[..];
    assert_eq!(ProduceRequest::decode(&mut cur, MAX_VERSION).unwrap(), req);
    assert!(cur.is_empty());
}

#[test]
fn owned_produce_response_min_version_empty_responses() {
    let resp = ProduceResponse::default();
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, RESP_MIN).unwrap();
    assert_eq!(resp.encoded_len(RESP_MIN), buf.len());
    let mut cur = &buf[..];
    let decoded = ProduceResponse::decode(&mut cur, RESP_MIN).unwrap();
    assert!(cur.is_empty(), "decoder left trailing bytes");
    assert_eq!(decoded.responses.len(), 0);
}

#[test]
fn owned_produce_response_max_version_roundtrips() {
    let resp = ProduceResponse {
        responses: Vec::new(),
        throttle_time_ms: 0i32,
        node_endpoints: Vec::new(),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, RESP_MAX).unwrap();
    assert_eq!(resp.encoded_len(RESP_MAX), buf.len());
    let mut cur = &buf[..];
    assert_eq!(ProduceResponse::decode(&mut cur, RESP_MAX).unwrap(), resp);
    assert!(cur.is_empty());
}
