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
    "/generated/DescribeGroupsResponse.owned.rs"
));

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn roundtrip_default_min_version() {
        let resp = DescribeGroupsResponse::default();
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, MIN_VERSION).unwrap();
        assert_eq!(resp.encoded_len(MIN_VERSION), buf.len());
        let mut cur = &buf[..];
        assert_eq!(
            DescribeGroupsResponse::decode(&mut cur, MIN_VERSION).unwrap(),
            resp
        );
        assert!(cur.is_empty());
    }

    #[test]
    fn roundtrip_default_max_version() {
        let resp = DescribeGroupsResponse::default();
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, MAX_VERSION).unwrap();
        assert_eq!(resp.encoded_len(MAX_VERSION), buf.len());
        let mut cur = &buf[..];
        assert_eq!(
            DescribeGroupsResponse::decode(&mut cur, MAX_VERSION).unwrap(),
            resp
        );
        assert!(cur.is_empty());
    }
}
