//! `ScramClientExchange` — implemented in task 3.

use crate::AuthError;

#[derive(Debug)]
pub struct ScramClientExchange {
    _username: String,
    _password: Vec<u8>,
}

impl ScramClientExchange {
    #[must_use]
    pub fn new(username: String, password: Vec<u8>) -> Self {
        Self {
            _username: username,
            _password: password,
        }
    }

    pub fn step(&mut self, _server_bytes: &[u8]) -> Result<Vec<u8>, AuthError> {
        Err(AuthError::MalformedMessage)
    }
}
