use std::{collections::HashMap, hash::BuildHasher};

use subtle::ConstantTimeEq;

use crate::{AuthError, AuthMethod, Principal};

/// Verifies a SASL/PLAIN auth attempt against a static credential map.
///
/// On a known user, the password comparison is constant-time. On an unknown
/// user, this function returns `UnknownUser`. The wire response upstream
/// collapses both outcomes to `SASL_AUTHENTICATION_FAILED`.
///
/// `skip_all` keeps `creds` and the raw `password` bytes out of the span
/// fields. Only the non-sensitive `user` and mechanism name are recorded. `err`
/// surfaces `AuthError` (Debug) without leaking the password.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(mechanism = "PLAIN", user = %user),
    err
)]
/// # Errors
/// Returns an error when credentials or key material are invalid, cryptographic verification fails, or the TLS, SASL, or Kerberos exchange is rejected.
pub fn verify_plain<S: BuildHasher>(
    creds: &HashMap<String, String, S>,
    user: &str,
    password: &[u8],
) -> Result<Principal, AuthError> {
    let Some(expected) = creds.get(user) else {
        return Err(AuthError::UnknownUser);
    };
    if expected.as_bytes().ct_eq(password).unwrap_u8() == 1 {
        Ok(Principal {
            name: user.to_string(),
            auth_method: AuthMethod::SaslPlain,
            groups: vec![],
        })
    } else {
        Err(AuthError::BadPassword)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn creds() -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("alice".into(), "wonderland".into());
        m
    }

    #[test]
    fn correct_creds_pass() {
        let p = verify_plain(&creds(), "alice", b"wonderland").unwrap();
        assert2::assert!(p.name.as_str() == "alice");
        assert2::assert!(p.auth_method == AuthMethod::SaslPlain);
    }

    #[test]
    fn wrong_password_fails() {
        assert2::assert!(
            verify_plain(&creds(), "alice", b"hunter2") == Err(AuthError::BadPassword)
        );
    }

    #[test]
    fn unknown_user_fails() {
        assert2::assert!(verify_plain(&creds(), "bob", b"anything") == Err(AuthError::UnknownUser));
    }
}
