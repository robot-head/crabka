include!(concat!(env!("CARGO_MANIFEST_DIR"), "/generated/ApiVersionsRequest.owned.rs"));

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn v0_is_empty() {
        let req = ApiVersionsRequest::default();
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 0).unwrap();
        assert!(buf.is_empty());
        let mut cur = &buf[..];
        assert_eq!(ApiVersionsRequest::decode(&mut cur, 0).unwrap(), req);
    }

    #[test]
    fn v3_roundtrip() {
        let req = ApiVersionsRequest {
            client_software_name: "crabka".to_string(),
            client_software_version: "0.0.0".to_string(),
            unknown_tagged_fields: Default::default(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 3).unwrap();
        assert_eq!(req.encoded_len(3), buf.len());
        let mut cur = &buf[..];
        assert_eq!(ApiVersionsRequest::decode(&mut cur, 3).unwrap(), req);
    }
}
