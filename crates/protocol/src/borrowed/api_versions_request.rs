include!(concat!(env!("CARGO_MANIFEST_DIR"), "/generated/ApiVersionsRequest.borrowed.rs"));

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn borrowed_v3_roundtrip() {
        let req = ApiVersionsRequest {
            client_software_name: "crabka",
            client_software_version: "0.0.0",
            unknown_tagged_fields: Default::default(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 3).unwrap();
        let frozen = buf.freeze();
        let mut cur: &[u8] = &frozen;
        let decoded = ApiVersionsRequest::decode_borrow(&mut cur, 3).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn to_owned_matches_owned_codec() {
        let req = ApiVersionsRequest {
            client_software_name: "crabka",
            client_software_version: "0.0.0",
            unknown_tagged_fields: Default::default(),
        };
        let mut a = BytesMut::new();
        req.encode(&mut a, 3).unwrap();
        let owned = req.to_owned();
        let mut b = BytesMut::new();
        owned.encode(&mut b, 3).unwrap();
        assert_eq!(a.as_ref(), b.as_ref());
    }
}
