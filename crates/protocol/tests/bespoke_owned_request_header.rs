// Bespoke tests for the owned RequestHeader wrapper that go beyond the standard
// min/max-version roundtrip. Relocated here from the hand-written wrapper as
// part of making wrappers uniformly generated.

use assert2::assert;
use bytes::BytesMut;
use crabka_protocol::{
    Decode, Encode, UnknownTaggedFields,
    owned::request_header::{MAX_VERSION, MIN_VERSION, RequestHeader},
};

#[test]
fn owned_request_header_null_client_id_roundtrips() {
    let hdr = RequestHeader {
        request_api_key: 3i16,
        request_api_version: 5i16,
        correlation_id: 7i32,
        client_id: None,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    hdr.encode(&mut buf, MAX_VERSION).unwrap();
    assert!(hdr.encoded_len(MAX_VERSION) == buf.len());
    let mut cur = &buf[..];
    let decoded = RequestHeader::decode(&mut cur, MAX_VERSION).unwrap();
    assert!(cur.is_empty(), "decoder left trailing bytes");
    assert!(decoded.client_id == None);
}

#[test]
fn owned_request_header_min_version_roundtrips() {
    let hdr = RequestHeader {
        request_api_key: 1i16,
        request_api_version: 0i16,
        correlation_id: 42i32,
        client_id: None,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    hdr.encode(&mut buf, MIN_VERSION).unwrap();
    assert!(hdr.encoded_len(MIN_VERSION) == buf.len());
    let mut cur = &buf[..];
    let decoded = RequestHeader::decode(&mut cur, MIN_VERSION).unwrap();
    assert!(cur.is_empty(), "decoder left trailing bytes");
    assert!(decoded == hdr);
}

#[test]
fn owned_request_header_max_version_roundtrips() {
    let hdr = RequestHeader {
        request_api_key: 18i16,
        request_api_version: 3i16,
        correlation_id: 99i32,
        client_id: Some("test-client".to_string()),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    hdr.encode(&mut buf, MAX_VERSION).unwrap();
    assert!(hdr.encoded_len(MAX_VERSION) == buf.len());
    let mut cur = &buf[..];
    let decoded = RequestHeader::decode(&mut cur, MAX_VERSION).unwrap();
    assert!(cur.is_empty(), "decoder left trailing bytes");
    assert!(decoded == hdr);
}
