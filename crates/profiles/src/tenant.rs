//! Tenant id validation (Mimir/Pyroscope-style `X-Scope-OrgID` charset).
//!
//! The accepted charset mirrors Grafana Mimir's tenant validation: a bounded
//! length, a restricted ASCII charset, and explicit rejection of path-unsafe
//! segments (`.`, `..`, `/`, `\`) so a tenant id can never escape a storage
//! prefix or smuggle control characters.

use crate::error::ProfilesError;

/// Maximum tenant id length in bytes.
const MAX_TENANT_LEN: usize = 150;

/// The default tenant used when no `X-Scope-OrgID` header is supplied.
pub const ANONYMOUS_TENANT: &str = "anonymous";

/// Returns `true` if `byte` is an allowed tenant character.
///
/// Allowed: `A-Z a-z 0-9` and the punctuation `! _ * ' ( ) - .`.
fn is_allowed_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(byte, b'!' | b'_' | b'*' | b'\'' | b'(' | b')' | b'-' | b'.')
}

/// Validate a raw tenant id against the Mimir/Pyroscope charset.
///
/// Rejects (with a generic, non-leaky message):
/// - leading or trailing ASCII whitespace (no silent trim),
/// - the empty string,
/// - ids longer than 150 bytes,
/// - the exact segments `"."` and `".."`,
/// - any `/`, `\`, ASCII control byte (`< 0x20` or `0x7f`), or any byte
///   outside the allowed charset.
///
/// On success returns the owned, validated id.
///
/// # Errors
///
/// Returns [`ProfilesError::Invalid`] when `raw` violates any of the rules
/// above.
pub fn validate_tenant(raw: &str) -> Result<String, ProfilesError> {
    let invalid = || ProfilesError::Invalid("invalid tenant id".to_string());

    if raw.is_empty() {
        return Err(invalid());
    }
    // No silent trim: reject leading/trailing ASCII whitespace outright.
    if raw.starts_with(|c: char| c.is_ascii_whitespace())
        || raw.ends_with(|c: char| c.is_ascii_whitespace())
    {
        return Err(invalid());
    }
    if raw.len() > MAX_TENANT_LEN {
        return Err(invalid());
    }
    if raw == "." || raw == ".." {
        return Err(invalid());
    }
    if !raw.bytes().all(is_allowed_byte) {
        return Err(invalid());
    }

    Ok(raw.to_string())
}

/// Resolve a tenant id from an optional header value.
///
/// Returns the [`ANONYMOUS_TENANT`] default when `value` is `None` or an empty
/// string (this preserves the existing anonymous-default behaviour; a non-UTF-8
/// header is signalled by the caller passing `None`). For a present, non-empty
/// value the id is validated via [`validate_tenant`].
///
/// # Errors
///
/// Returns [`ProfilesError::Invalid`] when a present, non-empty value fails
/// [`validate_tenant`].
pub fn tenant_from_header(value: Option<&str>) -> Result<String, ProfilesError> {
    match value {
        None | Some("") => Ok(ANONYMOUS_TENANT.to_string()),
        Some(raw) => validate_tenant(raw),
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn valid_tenant_is_returned() {
        let ok = validate_tenant("tenant-a");
        assert!(ok == Ok("tenant-a".to_string()));
    }

    #[test]
    fn normal_tenant_ok() {
        check!(validate_tenant("tenant-a").is_ok());
        check!(validate_tenant("Team_42!").is_ok());
        check!(validate_tenant("a.b.c").is_ok());
    }

    #[test]
    fn header_none_is_anonymous() {
        assert!(tenant_from_header(None) == Ok("anonymous".to_string()));
    }

    #[test]
    fn header_empty_is_anonymous() {
        assert!(tenant_from_header(Some("")) == Ok("anonymous".to_string()));
    }

    #[test]
    fn header_present_validates() {
        assert!(tenant_from_header(Some("tenant-a")) == Ok("tenant-a".to_string()));
        check!(tenant_from_header(Some("a/b")).is_err());
    }

    #[test]
    fn over_max_length_is_rejected() {
        let long = "a".repeat(MAX_TENANT_LEN + 1);
        check!(validate_tenant(&long).is_err());
        // Exactly at the limit is allowed.
        let at_limit = "a".repeat(MAX_TENANT_LEN);
        check!(validate_tenant(&at_limit).is_ok());
    }

    #[test]
    fn empty_is_rejected() {
        check!(validate_tenant("").is_err());
    }

    #[test]
    fn path_unsafe_segments_are_rejected() {
        check!(validate_tenant("../x").is_err());
        check!(validate_tenant("a/b").is_err());
        check!(validate_tenant("a\\b").is_err());
        check!(validate_tenant("..").is_err());
        check!(validate_tenant(".").is_err());
    }

    #[test]
    fn whitespace_and_control_are_rejected() {
        check!(validate_tenant("a b").is_err());
        check!(validate_tenant("a\tb").is_err());
        check!(validate_tenant(" lead").is_err());
        check!(validate_tenant("trail ").is_err());
        check!(validate_tenant("ctl\u{7f}").is_err());
    }
}
