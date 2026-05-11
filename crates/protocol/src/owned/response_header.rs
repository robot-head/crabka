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
    "/generated/ResponseHeader.owned.rs"
));

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn min_version_roundtrips() {
        let hdr = ResponseHeader {
            correlation_id: 42i32,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        let mut buf = BytesMut::new();
        hdr.encode(&mut buf, MIN_VERSION).unwrap();
        assert_eq!(hdr.encoded_len(MIN_VERSION), buf.len());
        let mut cur = &buf[..];
        let decoded = ResponseHeader::decode(&mut cur, MIN_VERSION).unwrap();
        assert!(cur.is_empty(), "decoder left trailing bytes");
        assert_eq!(decoded, hdr);
    }

    #[test]
    fn max_version_roundtrips() {
        // Max version is 1, which is flexible.
        let hdr = ResponseHeader {
            correlation_id: 99i32,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        let mut buf = BytesMut::new();
        hdr.encode(&mut buf, MAX_VERSION).unwrap();
        assert_eq!(hdr.encoded_len(MAX_VERSION), buf.len());
        let mut cur = &buf[..];
        let decoded = ResponseHeader::decode(&mut cur, MAX_VERSION).unwrap();
        assert!(cur.is_empty(), "decoder left trailing bytes");
        assert_eq!(decoded, hdr);
    }
}
