use crate::SaslMechanism;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// How a [`Principal`] was authenticated. A strict superset of
/// [`SaslMechanism`] that also covers mTLS client-cert authentication
/// and the implicit ANONYMOUS path on PLAINTEXT / SSL-no-mTLS
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
    /// SASL/SCRAM-SHA-256.
    SaslScramSha256,
    /// SASL/SCRAM-SHA-512.
    SaslScramSha512,
    /// SASL/OAUTHBEARER.
    SaslOAuthBearer,
    /// SASL/GSSAPI (Kerberos, RFC 4752).
    SaslGssapi,
    /// mTLS client-cert verified against the listener's
    /// `client_ca_path`.
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
            SaslMechanism::Gssapi => Self::SaslGssapi,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub name: String,
    pub auth_method: AuthMethod,
    /// OAuth-derived group memberships from the listener's
    /// `groupsClaim`. Empty vec for non-OAuth principals (PLAIN/SCRAM/
    /// mTLS/anonymous) and for OAuth principals whose listener has no
    /// `groupsClaim` configured. No broker-side authorizer reads this
    /// yet; populated as scaffolding + for observability.
    pub groups: Vec<String>,
}

impl Principal {
    /// Project a runtime session [`Principal`] onto the Kafka wire-level
    /// [`KafkaPrincipal`] (`principalType:name`) used by ACLs and
    /// delegation-token records. All authenticated callers ride under
    /// `principal_type = "User"`, matching Kafka's
    /// `DefaultKafkaPrincipalBuilder`.
    #[must_use]
    pub fn to_kafka(&self) -> KafkaPrincipal {
        KafkaPrincipal {
            principal_type: "User".to_string(),
            name: self.name.clone(),
        }
    }
}

/// KIP-48: Kafka wire-level principal — the `(principalType,
/// name)` pair carried in delegation-token records, ACL entries, and
/// `KafkaPrincipal`-shaped fields across the Kafka protocol. Distinct
/// from [`Principal`] which models the *runtime session* identity
/// (auth method + OAuth groups). Format-stable: `Display`/`FromStr`
/// round-trip the canonical `Type:Name` form.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KafkaPrincipal {
    pub principal_type: String,
    pub name: String,
}

impl std::fmt::Display for KafkaPrincipal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.principal_type, self.name)
    }
}

impl std::str::FromStr for KafkaPrincipal {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        let (pt, n) = s
            .split_once(':')
            .ok_or_else(|| format!("invalid principal {s:?}"))?;
        Ok(Self {
            principal_type: pt.into(),
            name: n.into(),
        })
    }
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
    /// RFC 7628 `invalid_token` server error status.
    #[error("invalid token")]
    InvalidToken,
    /// OAUTHBEARER introspection HTTP round-trip failed at the transport layer.
    /// Distinct from `InvalidToken` so the SASL handler can
    /// surface "`IdP` unreachable" separately from "client supplied a bad
    /// token". Maps to the RFC 7628 `invalid_token` server error status.
    #[error("oauthbearer introspection transport: {0}")]
    IntrospectionTransport(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn from_sasl_mapping() {
        for (mechanism, want) in [
            (SaslMechanism::Plain, AuthMethod::SaslPlain),
            (SaslMechanism::ScramSha256, AuthMethod::SaslScramSha256),
            (SaslMechanism::ScramSha512, AuthMethod::SaslScramSha512),
            (SaslMechanism::OAuthBearer, AuthMethod::SaslOAuthBearer),
            (SaslMechanism::Gssapi, AuthMethod::SaslGssapi),
        ] {
            assert!(
                AuthMethod::from_sasl(mechanism) == want,
                "case {mechanism:?}"
            );
        }
    }

    #[test]
    fn principal_display_is_type_colon_name() {
        let p = KafkaPrincipal {
            principal_type: "User".into(),
            name: "alice".into(),
        };
        assert!(p.to_string() == "User:alice");
        // FromStr is the inverse.
        assert!("User:alice".parse::<KafkaPrincipal>().unwrap() == p);
    }
}
