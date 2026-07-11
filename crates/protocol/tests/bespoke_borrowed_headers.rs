// Bespoke tests for borrowed RequestHeader and ResponseHeader: the to_owned
// conversion tests and specific-value roundtrips. Relocated from hand-written
// wrappers.

use bytes::BytesMut;
use crabka_protocol::{
    DecodeBorrow, Encode, UnknownTaggedFields,
    borrowed::{
        request_header::{MAX_VERSION, MIN_VERSION, RequestHeader},
        response_header::{MAX_VERSION as RESP_MAX, MIN_VERSION as RESP_MIN, ResponseHeader},
    },
};

#[test]
fn borrowed_request_header_min_version_specific_values() {
    let hdr = RequestHeader {
        request_api_key: 1i16,
        request_api_version: 0i16,
        correlation_id: 42i32,
        client_id: None,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    hdr.encode(&mut buf, MIN_VERSION).unwrap();
    assert2::assert!(hdr.encoded_len(MIN_VERSION) == buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = RequestHeader::decode_borrow(&mut cur, MIN_VERSION).unwrap();
    assert2::assert!(cur.is_empty());
    assert2::assert!(decoded == hdr);
}

#[test]
fn borrowed_request_header_max_version_with_client_id() {
    let hdr = RequestHeader {
        request_api_key: 18i16,
        request_api_version: 3i16,
        correlation_id: 99i32,
        client_id: Some("test-client"),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    hdr.encode(&mut buf, MAX_VERSION).unwrap();
    assert2::assert!(hdr.encoded_len(MAX_VERSION) == buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = RequestHeader::decode_borrow(&mut cur, MAX_VERSION).unwrap();
    assert2::assert!(cur.is_empty());
    assert2::assert!(decoded == hdr);
}

#[test]
fn borrowed_request_header_to_owned_matches_owned_codec() {
    use crabka_protocol::Encode as OwnedEncode;
    let hdr = RequestHeader {
        request_api_key: 18i16,
        request_api_version: 3i16,
        correlation_id: 5i32,
        client_id: Some("crabka"),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut a = BytesMut::new();
    hdr.encode(&mut a, MAX_VERSION).unwrap();
    let owned = hdr.to_owned();
    let mut b = BytesMut::new();
    owned.encode(&mut b, MAX_VERSION).unwrap();
    assert2::assert!(a.as_ref() == b.as_ref());
}

#[test]
fn borrowed_response_header_min_version_specific_values() {
    let hdr = ResponseHeader {
        correlation_id: 42i32,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    hdr.encode(&mut buf, RESP_MIN).unwrap();
    assert2::assert!(hdr.encoded_len(RESP_MIN) == buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = ResponseHeader::decode_borrow(&mut cur, RESP_MIN).unwrap();
    assert2::assert!(cur.is_empty());
    assert2::assert!(decoded.correlation_id == hdr.correlation_id);
}

#[test]
fn borrowed_response_header_max_version_specific_values() {
    let hdr = ResponseHeader {
        correlation_id: 99i32,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::new();
    hdr.encode(&mut buf, RESP_MAX).unwrap();
    assert2::assert!(hdr.encoded_len(RESP_MAX) == buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = ResponseHeader::decode_borrow(&mut cur, RESP_MAX).unwrap();
    assert2::assert!(cur.is_empty());
    assert2::assert!(decoded.correlation_id == hdr.correlation_id);
}

#[test]
fn borrowed_response_header_to_owned_matches_owned_codec() {
    use crabka_protocol::Encode as OwnedEncode;
    let hdr = ResponseHeader {
        correlation_id: 5i32,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut a = BytesMut::new();
    hdr.encode(&mut a, RESP_MAX).unwrap();
    let owned = hdr.to_owned();
    let mut b = BytesMut::new();
    owned.encode(&mut b, RESP_MAX).unwrap();
    assert2::assert!(a.as_ref() == b.as_ref());
}
