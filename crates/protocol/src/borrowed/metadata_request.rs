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
    "/generated/MetadataRequest.borrowed.rs"
));

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn min_version_roundtrips() {
        let req = MetadataRequest::default();
        let mut buf = BytesMut::new();
        req.encode(&mut buf, MIN_VERSION).unwrap();
        assert_eq!(req.encoded_len(MIN_VERSION), buf.len());
        let frozen = buf.freeze();
        let mut cur = &frozen[..];
        let decoded = MetadataRequest::decode_borrow(&mut cur, MIN_VERSION).unwrap();
        assert!(cur.is_empty(), "decoder left trailing bytes");
        // v0: None topics encodes as empty array; decoded will have Some([])
        assert_eq!(decoded.topics.as_ref().map(Vec::len), Some(0));
    }

    #[test]
    fn max_version_roundtrips() {
        let req = MetadataRequest {
            topics: None,
            allow_auto_topic_creation: true,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, MAX_VERSION).unwrap();
        assert_eq!(req.encoded_len(MAX_VERSION), buf.len());
        let frozen = buf.freeze();
        let mut cur = &frozen[..];
        let decoded = MetadataRequest::decode_borrow(&mut cur, MAX_VERSION).unwrap();
        assert!(cur.is_empty());
        assert_eq!(decoded.topics, req.topics);
    }
}
