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
    clippy::new_without_default,
    clippy::unreadable_literal,
    clippy::redundant_closure_for_method_calls,
    clippy::nonminimal_bool,
    clippy::bool_comparison,
    clippy::map_unwrap_or,
    clippy::option_as_ref_deref,
    clippy::manual_range_contains
)]

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/generated/ApiVersionsRequest.owned.rs"
));

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
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 3).unwrap();
        assert_eq!(req.encoded_len(3), buf.len());
        let mut cur = &buf[..];
        assert_eq!(ApiVersionsRequest::decode(&mut cur, 3).unwrap(), req);
    }
}
