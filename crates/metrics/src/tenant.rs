//! Tenant-ID validation matching Grafana-Mimir `ValidTenantID`.

/// Maximum tenant-ID length in bytes, matching Mimir's `errTenantIDTooLong`.
const MAX_TENANT_ID_LEN: usize = 150;

/// Validates a tenant ID against Mimir's `ValidTenantID` rules. It rejects an
/// empty ID, a length over 150 bytes, the reserved `.` and `..` path segments,
/// and any character outside the allowed set of alphanumerics plus
/// `! - _ . * ' ( )`.
///
/// It returns a reason a person can read on rejection.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn validate_tenant(id: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err("tenant ID is empty".to_string());
    }
    if id.len() > MAX_TENANT_ID_LEN {
        return Err(format!(
            "tenant ID is too long: max {MAX_TENANT_ID_LEN} bytes, got {}",
            id.len()
        ));
    }
    if id == "." || id == ".." {
        return Err(format!("tenant ID `{id}` is not allowed"));
    }
    for byte in id.bytes() {
        if !is_allowed_tenant_byte(byte) {
            return Err(format!(
                "tenant ID contains unsupported character `{}`",
                char::from(byte)
            ));
        }
    }
    Ok(())
}

/// The allowed tenant-ID bytes are ASCII alphanumerics plus `! - _ . * ' ( )`.
fn is_allowed_tenant_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(byte, b'!' | b'-' | b'_' | b'.' | b'*' | b'\'' | b'(' | b')')
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::validate_tenant;

    #[test]
    fn valid_and_invalid_tenant_ids() {
        let valid = [
            "tenant-a",
            "team_42",
            "user.name",
            "a",
            "ALL-CAPS",
            "ascii!-_.*'()",
        ];
        for id in valid {
            assert!(validate_tenant(id).is_ok(), "expected `{id}` to be valid");
        }

        let invalid = [
            "",
            ".",
            "..",
            "with space",
            "slash/tenant",
            "comma,tenant",
            "unicode-é",
            "tab\ttenant",
        ];
        for id in invalid {
            assert!(
                validate_tenant(id).is_err(),
                "expected `{id}` to be invalid"
            );
        }

        // Length boundary: exactly 150 bytes is allowed, 151 is rejected.
        assert!(validate_tenant(&"x".repeat(150)).is_ok());
        assert!(validate_tenant(&"x".repeat(151)).is_err());
    }
}
