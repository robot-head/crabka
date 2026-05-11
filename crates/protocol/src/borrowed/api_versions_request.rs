// Clippy lints that fire on generated code patterns are suppressed here so
// that regenerating the file does not require manual allow annotations.
#![allow(
    clippy::elidable_lifetime_names,
    clippy::must_use_candidate,
    clippy::unnecessary_wraps,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::default_trait_access,
    clippy::derivable_impls,
    clippy::collapsible_if,
    clippy::new_without_default
)]

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/generated/ApiVersionsRequest.borrowed.rs"
));

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn borrowed_v3_roundtrip() {
        let req = ApiVersionsRequest {
            client_software_name: "crabka",
            client_software_version: "0.0.0",
            unknown_tagged_fields: UnknownTaggedFields::default(),
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
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        let mut a = BytesMut::new();
        req.encode(&mut a, 3).unwrap();
        let owned = req.to_owned();
        let mut b = BytesMut::new();
        owned.encode(&mut b, 3).unwrap();
        assert_eq!(a.as_ref(), b.as_ref());
    }
}
