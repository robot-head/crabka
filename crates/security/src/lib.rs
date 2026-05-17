//! Pure-logic auth primitives used by the broker and CLI.
//!
//! No I/O, no async, no networking. The broker plumbs streams in; this
//! crate produces verifiers, hashes, and TLS configs.

mod listener;
mod mechanism;
mod plain;
mod principal;
pub mod scram;
mod tls;

pub use listener::ListenerProtocol;
pub use mechanism::SaslMechanism;
pub use plain::verify_plain;
pub use principal::{AuthError, Principal};
pub use scram::{
    ScramClientExchange, ScramCredential, ScramServerExchange, StepResult, derive_keys_from_salted,
    hash_scram_password, scram_hash_len,
};
pub use tls::TlsConfig;
