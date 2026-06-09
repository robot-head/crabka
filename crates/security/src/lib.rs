//! Security primitives used by Crabka brokers, clients, and tooling.
//!
//! The crate owns the protocol-independent pieces of TLS, SASL, SCRAM,
//! OAuth/OIDC, mTLS principal extraction, delegation-token HMACs, Kerberos
//! exchange state, and principal modelling. Network I/O remains in the caller:
//! OAuth introspection is represented by the async [`IntrospectionClient`] trait,
//! and the broker/client crates provide concrete transports and wire the
//! resulting validators into listener or connection handshakes.
//!
//! ## SASL/PLAIN verification
//!
//! ```rust
//! use crabka_security::{AuthMethod, verify_plain};
//! use std::collections::HashMap;
//!
//! let mut users = HashMap::new();
//! users.insert("alice".to_string(), "wonderland".to_string());
//!
//! let principal = verify_plain(&users, "alice", b"wonderland").unwrap();
//! assert_eq!(principal.name, "alice");
//! assert_eq!(principal.auth_method, AuthMethod::SaslPlain);
//! ```
//!
//! ## Storing SCRAM credentials
//!
//! ```rust
//! use crabka_security::{SaslMechanism, hash_scram_password};
//!
//! let credential = hash_scram_password(
//!     b"correct horse battery staple",
//!     SaslMechanism::ScramSha512,
//!     4096,
//! );
//! assert_eq!(credential.iterations, 4096);
//! ```

pub mod ca;
pub mod delegation_token;
pub mod gssapi;
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
