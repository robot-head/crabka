// Bespoke tests for borrowed ApiVersionsRequest: the to_owned conversion test
// and the specific-values roundtrip. Relocated from the hand-written wrapper
// wrapper.

use bytes::BytesMut;
use crabka_protocol::{
    DecodeBorrow, Encode, UnknownTaggedFields, borrowed::api_versions_request::ApiVersionsRequest,
};

#[test]
fn borrowed_api_versions_request_v3_roundtrip() {
    let req = ApiVersionsRequest {
        client_software_name: "crabka",
        client_software_version: "0.0.0",
        unknown_tagged_fields: UnknownTaggedFields::default(),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf, 3).unwrap();
    let frozen = buf.freeze();
    let mut cur: &[u8] = &frozen;
    let decoded = ApiVersionsRequest::decode_borrow(&mut cur, 3).unwrap();
    assert2::assert!(decoded == req);
}

#[test]
fn borrowed_api_versions_request_to_owned_matches_owned_codec() {
    use crabka_protocol::Encode as OwnedEncode;
    let req = ApiVersionsRequest {
        client_software_name: "crabka",
        client_software_version: "0.0.0",
        unknown_tagged_fields: UnknownTaggedFields::default(),
        ..Default::default()
    };
    let mut a = BytesMut::new();
    req.encode(&mut a, 3).unwrap();
    let owned = req.to_owned();
    let mut b = BytesMut::new();
    owned.encode(&mut b, 3).unwrap();
    assert2::assert!(a.as_ref() == b.as_ref());
}
