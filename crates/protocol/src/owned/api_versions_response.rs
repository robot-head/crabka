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
    "/generated/ApiVersionsResponse.owned.rs"
));

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    fn sample(version: i16) -> ApiVersionsResponse {
        ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![
                ApiVersion {
                    api_key: 0,
                    min_version: 0,
                    max_version: 10,
                    ..Default::default()
                },
                ApiVersion {
                    api_key: 1,
                    min_version: 0,
                    max_version: 17,
                    ..Default::default()
                },
            ],
            throttle_time_ms: if version >= 1 { 5 } else { 0 },
            ..Default::default()
        }
    }

    #[test]
    fn v0_roundtrip() {
        let r = sample(0);
        let mut buf = BytesMut::new();
        r.encode(&mut buf, 0).unwrap();
        assert_eq!(r.encoded_len(0), buf.len());
        let mut cur = &buf[..];
        assert_eq!(ApiVersionsResponse::decode(&mut cur, 0).unwrap(), r);
        assert!(cur.is_empty());
    }

    #[test]
    fn v1_includes_throttle_time() {
        let r = sample(1);
        let mut buf = BytesMut::new();
        r.encode(&mut buf, 1).unwrap();
        assert_eq!(r.encoded_len(1), buf.len());
        let mut cur = &buf[..];
        assert_eq!(ApiVersionsResponse::decode(&mut cur, 1).unwrap(), r);
        assert!(cur.is_empty());
    }

    #[test]
    fn v3_flexible_roundtrip() {
        let r = sample(3);
        let mut buf = BytesMut::new();
        r.encode(&mut buf, 3).unwrap();
        assert_eq!(r.encoded_len(3), buf.len());
        let mut cur = &buf[..];
        assert_eq!(ApiVersionsResponse::decode(&mut cur, 3).unwrap(), r);
        assert!(cur.is_empty());
    }
}
