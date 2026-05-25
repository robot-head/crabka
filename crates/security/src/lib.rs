//! Pure-logic auth primitives used by the broker and CLI.
//!
//! No I/O, no async, no networking. The broker plumbs streams in; this
//! crate produces verifiers, hashes, and TLS configs.

pub mod ca;
pub mod delegation_token;
mod jwks;
mod jwks_trust;
mod listener;
mod mechanism;
mod mtls;
mod oauthbearer;
mod plain;
mod principal;
mod reload;
pub mod scram;
mod tls;

pub use delegation_token::{SecretBytes, compute_token_hmac};
pub use jwks::{Jwks, JwksHandle};
pub use jwks_trust::{JwksTrustError, build_client_config_from_pem};
pub use listener::ListenerProtocol;
pub use mechanism::SaslMechanism;
pub use mtls::extract_principal_from_cert;
pub use oauthbearer::{
    AuthOutcome, ClientInitialResponse, IntrospectionClient, IntrospectionError,
    IntrospectionValidator, OAuthBearerValidator, SignedJwsValidator, UnsecuredJwsValidator,
    invalid_token_json, parse_client_initial_response,
};
pub use plain::verify_plain;
pub use principal::{AuthError, AuthMethod, KafkaPrincipal, Principal};
pub use reload::DynamicServerConfig;
pub use scram::{
    ScramClientExchange, ScramCredential, ScramServerExchange, StepResult, derive_keys_from_salted,
    hash_scram_password, pbkdf2_salted, scram_hash_len,
};
pub use tls::{ClientAuthMode, TlsConfig, TlsError};
