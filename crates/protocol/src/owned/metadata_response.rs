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
    "/generated/MetadataResponse.owned.rs"
));

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn min_version_roundtrips() {
        let resp = MetadataResponse::default();
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, MIN_VERSION).unwrap();
        assert_eq!(resp.encoded_len(MIN_VERSION), buf.len());
        let mut cur = &buf[..];
        assert_eq!(
            MetadataResponse::decode(&mut cur, MIN_VERSION).unwrap(),
            resp
        );
        assert!(cur.is_empty());
    }

    #[test]
    fn max_version_roundtrips() {
        // cluster_authorized_operations is version 8-10 only; at MAX_VERSION (13) it is
        // absent from the wire. After decode, the value is MetadataResponse::default()'s value
        // which is -2147483648 (the schema default for that field).
        let resp = MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![MetadataResponseBroker {
                node_id: 1,
                host: "localhost".to_string(),
                port: 9092,
                rack: None,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            cluster_id: Some("test-cluster".to_string()),
            controller_id: 1,
            topics: vec![],
            cluster_authorized_operations: -2_147_483_648, // schema default; field absent at v13
            error_code: 0,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, MAX_VERSION).unwrap();
        assert_eq!(resp.encoded_len(MAX_VERSION), buf.len());
        let mut cur = &buf[..];
        assert_eq!(
            MetadataResponse::decode(&mut cur, MAX_VERSION).unwrap(),
            resp
        );
        assert!(cur.is_empty());
    }
}
