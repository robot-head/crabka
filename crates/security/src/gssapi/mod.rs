//! SASL/GSSAPI (Kerberos) support. See
//! SASL/GSSAPI Kerberos support.

pub mod client;
pub mod keytab;
pub mod name;
pub mod provider;
pub mod security_layer;
pub mod server;

use std::path::PathBuf;

/// Broker-side GSSAPI configuration (parallels `OAuthBearer` config).
#[derive(Debug, Clone)]
pub struct GssapiConfig {
    /// Path to the broker's service keytab.
    pub keytab_path: PathBuf,
    /// Kafka `sasl.kerberos.service.name` (the SPN's first component). Default "kafka".
    pub service_name: String,
    /// Parsed `auth_to_local` rules, applied in order; first match wins.
    pub principal_to_local_rules: Vec<name::Rule>,
    /// Default realm (used when a principal omits it / for the initiate path).
    pub realm: Option<String>,
    /// KDC host:port for the initiate path; falls back to krb5.conf discovery when None.
    pub kdc: Option<String>,
}

/// Errors from GSS context operations.
#[derive(Debug, thiserror::Error)]
pub enum GssError {
    #[error("GSS context establishment failed: {0}")]
    Context(String),
    #[error("GSS wrap/unwrap failed: {0}")]
    Wrap(String),
    #[error("keytab error: {0}")]
    Keytab(String),
    #[error("no source principal available")]
    NoSrcPrincipal,
}

/// One step of server-side context establishment.
#[derive(Debug)]
pub enum AcceptStep {
    /// Send this token back to the client; expect another client token.
    Continue(Vec<u8>),
    /// Context established. Optional final token to send (e.g. AP-REP).
    Established(Option<Vec<u8>>),
}

/// One step of client-side context establishment.
#[derive(Debug)]
pub enum InitStep {
    /// Send this token to the server; expect another server token.
    Continue(Vec<u8>),
    /// Context established. Optional final token to send.
    Established(Option<Vec<u8>>),
}

/// Server side: drive GSS context establishment from client tokens, then
/// wrap/unwrap the RFC 4752 security-layer negotiation messages.
///
/// `Send + Sync` so a live `GssapiServerExchange` can live inside the
/// per-connection `ConnectionAuth` state, which the broker's request-handling
/// future holds across `.await` points (the `wrap`/`unwrap`/`src_principal`
/// methods take `&self` and serialise interior mutability behind a mutex).
pub trait GssAcceptor: Send + Sync {
    /// # Errors
    /// Returns an error when credentials or key material are invalid, cryptographic verification fails, or the TLS, SASL, or Kerberos exchange is rejected.
    fn accept(&mut self, client_token: &[u8]) -> Result<AcceptStep, GssError>;
    /// # Errors
    /// Returns an error when credentials or key material are invalid, cryptographic verification fails, or the TLS, SASL, or Kerberos exchange is rejected.
    fn wrap(&self, plaintext: &[u8], confidential: bool) -> Result<Vec<u8>, GssError>;
    /// # Errors
    /// Returns an error when credentials or key material are invalid, cryptographic verification fails, or the TLS, SASL, or Kerberos exchange is rejected.
    fn unwrap(&self, token: &[u8]) -> Result<Vec<u8>, GssError>;
    /// Authenticated source principal, e.g. "alice@REALM" or "alice/host@REALM".
    /// # Errors
    /// Returns an error when credentials or key material are invalid, cryptographic verification fails, or the TLS, SASL, or Kerberos exchange is rejected.
    fn src_principal(&self) -> Result<String, GssError>;
}

/// Client side: produce tokens to send, consume server tokens.
pub trait GssInitiator: Send {
    /// # Errors
    /// Returns an error when credentials or key material are invalid, cryptographic verification fails, or the TLS, SASL, or Kerberos exchange is rejected.
    fn step(&mut self, server_token: Option<&[u8]>) -> Result<InitStep, GssError>;
    /// # Errors
    /// Returns an error when credentials or key material are invalid, cryptographic verification fails, or the TLS, SASL, or Kerberos exchange is rejected.
    fn wrap(&self, plaintext: &[u8], confidential: bool) -> Result<Vec<u8>, GssError>;
    /// # Errors
    /// Returns an error when credentials or key material are invalid, cryptographic verification fails, or the TLS, SASL, or Kerberos exchange is rejected.
    fn unwrap(&self, token: &[u8]) -> Result<Vec<u8>, GssError>;
}
