use crate::{AuthError, Principal, SaslMechanism};
use std::collections::HashMap;
use std::hash::BuildHasher;

pub fn verify_plain<S: BuildHasher>(
    _creds: &HashMap<String, String, S>,
    _user: &str,
    _password: &[u8],
) -> Result<Principal, AuthError> {
    let _ = SaslMechanism::Plain;
    Err(AuthError::UnknownUser)
}
