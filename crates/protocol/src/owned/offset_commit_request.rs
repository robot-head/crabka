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
    "/generated/OffsetCommitRequest.owned.rs"
));

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn min_version_roundtrips() {
        let req = OffsetCommitRequest::default();
        let mut buf = BytesMut::new();
        req.encode(&mut buf, MIN_VERSION).unwrap();
        assert_eq!(req.encoded_len(MIN_VERSION), buf.len());
        let mut cur = &buf[..];
        let decoded = OffsetCommitRequest::decode(&mut cur, MIN_VERSION).unwrap();
        assert!(cur.is_empty(), "decoder left trailing bytes");
        assert_eq!(decoded.group_id, req.group_id);
        assert_eq!(decoded.topics.len(), 0);
    }

    #[test]
    fn max_version_roundtrips() {
        let req = OffsetCommitRequest {
            group_id: "test-group".to_string(),
            generation_id_or_member_epoch: -1i32,
            member_id: String::new(),
            group_instance_id: None,
            retention_time_ms: -1i64,
            topics: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, MAX_VERSION).unwrap();
        assert_eq!(req.encoded_len(MAX_VERSION), buf.len());
        let mut cur = &buf[..];
        assert_eq!(
            OffsetCommitRequest::decode(&mut cur, MAX_VERSION).unwrap(),
            req
        );
        assert!(cur.is_empty());
    }
}
