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
    "/generated/MetadataRequest.owned.rs"
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
        let mut cur = &buf[..];
        let decoded = MetadataRequest::decode(&mut cur, MIN_VERSION).unwrap();
        assert!(cur.is_empty(), "decoder left trailing bytes");
        // v0: topics=None encodes as empty array; decoded will come back as Some([])
        assert_eq!(decoded.topics, Some(vec![]));
        assert_eq!(
            decoded.allow_auto_topic_creation,
            req.allow_auto_topic_creation
        );
    }

    #[test]
    fn max_version_roundtrips() {
        let req = MetadataRequest {
            topics: None, // null = "all topics" in v1+
            allow_auto_topic_creation: true,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, MAX_VERSION).unwrap();
        assert_eq!(req.encoded_len(MAX_VERSION), buf.len());
        let mut cur = &buf[..];
        assert_eq!(MetadataRequest::decode(&mut cur, MAX_VERSION).unwrap(), req);
        assert!(cur.is_empty());
    }
}
