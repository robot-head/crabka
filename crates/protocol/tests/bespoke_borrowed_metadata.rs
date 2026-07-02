// Bespoke tests for borrowed Metadata wrappers. Relocated from hand-written
// wrappers.

use assert2::assert;
use bytes::BytesMut;
use crabka_protocol::borrowed::metadata_request::{MAX_VERSION, MIN_VERSION, MetadataRequest};
use crabka_protocol::borrowed::metadata_response::{
    MAX_VERSION as RESP_MAX, MIN_VERSION as RESP_MIN, MetadataResponse,
};
use crabka_protocol::{DecodeBorrow, Encode, UnknownTaggedFields};

#[test]
fn borrowed_metadata_request_min_version_none_topics_encodes_as_empty() {
    let req = MetadataRequest::default();
    let mut buf = BytesMut::new();
    req.encode(&mut buf, MIN_VERSION).unwrap();
    assert!(req.encoded_len(MIN_VERSION) == buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = MetadataRequest::decode_borrow(&mut cur, MIN_VERSION).unwrap();
    assert!(cur.is_empty(), "decoder left trailing bytes");
    // v0: None topics encodes as empty array; decoded will have Some([])
    assert!(decoded.topics.as_ref().map(Vec::len) == Some(0));
}

#[test]
fn borrowed_metadata_request_max_version_null_topics() {
    let req = MetadataRequest {
        topics: None,
        allow_auto_topic_creation: true,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf, MAX_VERSION).unwrap();
    assert!(req.encoded_len(MAX_VERSION) == buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = MetadataRequest::decode_borrow(&mut cur, MAX_VERSION).unwrap();
    assert!(cur.is_empty());
    assert!(decoded.topics == req.topics);
}

#[test]
fn borrowed_metadata_response_min_version_empty_brokers() {
    let resp = MetadataResponse::default();
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, RESP_MIN).unwrap();
    assert!(resp.encoded_len(RESP_MIN) == buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = MetadataResponse::decode_borrow(&mut cur, RESP_MIN).unwrap();
    assert!(cur.is_empty(), "decoder left trailing bytes");
    assert!(decoded.brokers.len() == resp.brokers.len());
}

#[test]
fn borrowed_metadata_response_max_version_empty_collections() {
    let resp = MetadataResponse {
        throttle_time_ms: 0,
        brokers: vec![],
        cluster_id: None,
        controller_id: -1,
        topics: vec![],
        cluster_authorized_operations: i32::MIN,
        error_code: 0,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, RESP_MAX).unwrap();
    assert!(resp.encoded_len(RESP_MAX) == buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = MetadataResponse::decode_borrow(&mut cur, RESP_MAX).unwrap();
    assert!(cur.is_empty());
    assert!(decoded == resp);
}
