// Bespoke tests for borrowed Produce wrappers. Relocated from hand-written
// wrappers.

use bytes::BytesMut;
use crabka_protocol::{
    DecodeBorrow, Encode, UnknownTaggedFields,
    borrowed::{
        produce_request::{MAX_VERSION, MIN_VERSION, ProduceRequest},
        produce_response::{MAX_VERSION as RESP_MAX, MIN_VERSION as RESP_MIN, ProduceResponse},
    },
};

#[test]
fn borrowed_produce_request_min_version_empty_topics() {
    let req = ProduceRequest::default();
    let mut buf = BytesMut::new();
    req.encode(&mut buf, MIN_VERSION).unwrap();
    assert2::assert!(req.encoded_len(MIN_VERSION) == buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = ProduceRequest::decode_borrow(&mut cur, MIN_VERSION).unwrap();
    assert2::assert!(cur.is_empty());
    assert2::assert!(decoded.topic_data.len() == 0);
}

#[test]
fn borrowed_produce_request_max_version_specific_values() {
    let req = ProduceRequest {
        transactional_id: None,
        acks: 1i16,
        timeout_ms: 30_000i32,
        topic_data: Vec::new(),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf, MAX_VERSION).unwrap();
    assert2::assert!(req.encoded_len(MAX_VERSION) == buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = ProduceRequest::decode_borrow(&mut cur, MAX_VERSION).unwrap();
    assert2::assert!(cur.is_empty());
    assert2::assert!(decoded.topic_data == req.topic_data);
}

#[test]
fn borrowed_produce_response_min_version_empty_responses() {
    let resp = ProduceResponse::default();
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, RESP_MIN).unwrap();
    assert2::assert!(resp.encoded_len(RESP_MIN) == buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = ProduceResponse::decode_borrow(&mut cur, RESP_MIN).unwrap();
    assert2::assert!(cur.is_empty());
    assert2::assert!(decoded.responses.len() == 0);
}

#[test]
fn borrowed_produce_response_max_version_roundtrips() {
    let resp = ProduceResponse {
        responses: Vec::new(),
        throttle_time_ms: 0i32,
        node_endpoints: Vec::new(),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, RESP_MAX).unwrap();
    assert2::assert!(resp.encoded_len(RESP_MAX) == buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = ProduceResponse::decode_borrow(&mut cur, RESP_MAX).unwrap();
    assert2::assert!(cur.is_empty());
    assert2::assert!(decoded.responses == resp.responses);
}
