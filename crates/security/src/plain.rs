use std::collections::HashMap;
use std::hash::BuildHasher;

use subtle::ConstantTimeEq;

use crate::{AuthError, AuthMethod, Principal};

/// Verifies a SASL/PLAIN auth attempt against a static credential map.
///
/// On a known user, password comparison is constant-time. On an unknown
/// user, returns `UnknownUser` (but the wire response collapses both to
/// `SASL_AUTHENTICATION_FAILED` upstream).
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
        assert_eq!(p.name, "alice");
        assert_eq!(p.auth_method, AuthMethod::SaslPlain);
    }

    #[test]
    fn wrong_password_fails() {
        assert_eq!(
            verify_plain(&creds(), "alice", b"hunter2"),
            Err(AuthError::BadPassword),
        );
    }

    #[test]
    fn unknown_user_fails() {
        assert_eq!(
            verify_plain(&creds(), "bob", b"anything"),
            Err(AuthError::UnknownUser),
        );
    }
}
