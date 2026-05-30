// Bespoke tests for the owned Metadata wrappers that verify specific semantic
// behaviour (e.g. None-topics encoding quirk at v0). Relocated from
// hand-written wrappers.

use assert2::assert;
use bytes::BytesMut;
use crabka_protocol::owned::metadata_request::{MAX_VERSION, MIN_VERSION, MetadataRequest};
use crabka_protocol::owned::metadata_response::{
    MAX_VERSION as RESP_MAX, MIN_VERSION as RESP_MIN, MetadataResponse, MetadataResponseBroker,
};
use crabka_protocol::{Decode, Encode, UnknownTaggedFields};

#[test]
fn owned_metadata_request_v0_topics_none_encodes_as_empty_array() {
    let req = MetadataRequest::default();
    let mut buf = BytesMut::new();
    req.encode(&mut buf, MIN_VERSION).unwrap();
    assert!(req.encoded_len(MIN_VERSION) == buf.len());
    let mut cur = &buf[..];
    let decoded = MetadataRequest::decode(&mut cur, MIN_VERSION).unwrap();
    assert!(cur.is_empty(), "decoder left trailing bytes");
    // v0: topics=None encodes as empty array; decoded will come back as Some([])
    assert!(decoded.topics == Some(vec![]));
    assert!(decoded.allow_auto_topic_creation == req.allow_auto_topic_creation);
}

#[test]
fn owned_metadata_request_max_version_roundtrips_null_topics() {
    let req = MetadataRequest {
        topics: None, // null = "all topics" in v1+
        allow_auto_topic_creation: true,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf, MAX_VERSION).unwrap();
    assert!(req.encoded_len(MAX_VERSION) == buf.len());
    let mut cur = &buf[..];
    assert!(MetadataRequest::decode(&mut cur, MAX_VERSION).unwrap() == req);
    assert!(cur.is_empty());
}

#[test]
fn owned_metadata_response_max_version_roundtrips_with_broker() {
    let resp = MetadataResponse {
        throttle_time_ms: 0,
        brokers: vec![MetadataResponseBroker {
            node_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            rack: None,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        cluster_id: Some("test-cluster".to_string()),
        controller_id: 1,
        topics: vec![],
        cluster_authorized_operations: -2_147_483_648,
        error_code: 0,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, RESP_MAX).unwrap();
    assert!(resp.encoded_len(RESP_MAX) == buf.len());
    let mut cur = &buf[..];
    assert!(MetadataResponse::decode(&mut cur, RESP_MAX).unwrap() == resp);
    assert!(cur.is_empty());
}

#[test]
fn owned_metadata_response_min_version_roundtrips() {
    let resp = MetadataResponse::default();
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, RESP_MIN).unwrap();
    assert!(resp.encoded_len(RESP_MIN) == buf.len());
    let mut cur = &buf[..];
    assert!(MetadataResponse::decode(&mut cur, RESP_MIN).unwrap() == resp);
    assert!(cur.is_empty());
}
