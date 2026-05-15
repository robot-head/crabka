//! SCRAM-SHA-512 — implemented in tasks 2-3.

mod client;
mod server;

pub use client::ScramClientExchange;
pub use server::{ScramServerExchange, StepResult};

use crate::SaslMechanism;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredential {
    pub mechanism: SaslMechanism,
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
    pub iterations: u32,
}

#[must_use]
pub fn hash_scram_password(
    _password: &[u8],
    mechanism: SaslMechanism,
    iterations: u32,
) -> ScramCredential {
    ScramCredential {
        mechanism,
        salt: vec![],
        stored_key: vec![],
        server_key: vec![],
        iterations,
    }
}
