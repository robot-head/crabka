use crate::SaslMechanism;
use thiserror::Error;

/// How a [`Principal`] was authenticated. A strict superset of
/// [`SaslMechanism`] that also covers mTLS client-cert authentication
/// (slice 29) and the implicit ANONYMOUS path on PLAINTEXT / SSL-no-mTLS
/// listeners.
///
/// Kept distinct from `SaslMechanism` because the latter has a
/// `from_wire`/`wire_name` contract and is stored verbatim in
/// `V1ScramCredential` metadata records — neither applies to mTLS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthMethod {
    /// Anonymous (no SASL, no mTLS). Used for PLAINTEXT listeners and
    /// for SSL listeners where the client did not present a cert.
    Anonymous,
    /// SASL/PLAIN.
    SaslPlain,
    /// SASL/SCRAM-SHA-256 (slice 32).
    SaslScramSha256,
    /// SASL/SCRAM-SHA-512 (slice 12).
    SaslScramSha512,
    /// SASL/OAUTHBEARER (slice 49).
    SaslOAuthBearer,
    /// mTLS client-cert verified against the listener's
    /// `client_ca_path` (slice 29).
    MTls,
}

impl AuthMethod {
    /// Map a SASL `SaslMechanism` onto its `AuthMethod` equivalent.
    #[must_use]
    pub fn from_sasl(m: SaslMechanism) -> Self {
        match m {
            SaslMechanism::Plain => Self::SaslPlain,
            SaslMechanism::ScramSha256 => Self::SaslScramSha256,
            SaslMechanism::ScramSha512 => Self::SaslScramSha512,
            SaslMechanism::OAuthBearer => Self::SaslOAuthBearer,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub name: String,
    pub auth_method: AuthMethod,
    /// Slice 49h: OAuth-derived group memberships from the listener's
    /// `groupsClaim`. Empty vec for non-OAuth principals (PLAIN/SCRAM/
    /// mTLS/anonymous) and for OAuth principals whose listener has no
    /// `groupsClaim` configured. No broker-side authorizer reads this
    /// yet (slice 53/54 will); populated as scaffolding + for
    /// observability.
    pub groups: Vec<String>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AuthError {
    #[error("unknown user")]
    UnknownUser,
    #[error("bad password")]
    BadPassword,
    #[error("bad proof")]
    BadProof,
    #[error("malformed message")]
    MalformedMessage,
    #[error("unsupported mechanism")]
    UnsupportedMechanism,
    /// OAUTHBEARER token failed validation (expired, bad claims, signed token
    /// rejected by the unsecured validator, missing principal, …). Maps to the
    /// RFC 7628 `invalid_token` server error status (slice 49).
    #[error("invalid token")]
    InvalidToken,
    /// OAUTHBEARER introspection HTTP round-trip failed at the transport layer
    /// (slice 49d). Distinct from `InvalidToken` so the SASL handler can
    /// surface "`IdP` unreachable" separately from "client supplied a bad
    /// token". Maps to the RFC 7628 `invalid_token` server error status.
    #[error("oauthbearer introspection transport: {0}")]
    IntrospectionTransport(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_sasl_mapping() {
        assert_eq!(
            AuthMethod::from_sasl(SaslMechanism::Plain),
            AuthMethod::SaslPlain
        );
        assert_eq!(
            AuthMethod::from_sasl(SaslMechanism::ScramSha256),
            AuthMethod::SaslScramSha256
        );
        assert_eq!(
            AuthMethod::from_sasl(SaslMechanism::ScramSha512),
            AuthMethod::SaslScramSha512
        );
        assert_eq!(
            AuthMethod::from_sasl(SaslMechanism::OAuthBearer),
            AuthMethod::SaslOAuthBearer
        );
    }
}
