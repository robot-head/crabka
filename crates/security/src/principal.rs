use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::SaslMechanism;

/// How a [`Principal`] was authenticated.
///
/// This is a strict superset of [`SaslMechanism`]. It also covers mTLS
/// client-cert authentication and the implicit ANONYMOUS path on PLAINTEXT and
/// SSL-no-mTLS listeners.
///
/// This enum stays distinct from `SaslMechanism`, because `SaslMechanism` has a
/// `from_wire`/`wire_name` contract and the broker stores it verbatim in
/// `V1ScramCredential` metadata records. Neither applies to mTLS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthMethod {
    /// Anonymous, with no SASL and no mTLS. This covers PLAINTEXT listeners and
    /// SSL listeners where the client did not present a cert.
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
    /// OAuth-derived group memberships from the listener's `groupsClaim`.
    ///
    /// The vec is empty for non-OAuth principals, which are PLAIN, SCRAM, mTLS,
    /// and anonymous. It is also empty for OAuth principals whose listener has
    /// no `groupsClaim` configured. No broker-side authorizer reads this field
    /// yet. The broker fills it as scaffolding and for observability.
    pub groups: Vec<String>,
}

impl Principal {
    /// Project a runtime session [`Principal`] onto the Kafka wire-level
    /// [`KafkaPrincipal`].
    ///
    /// The wire-level form is `principalType:name`, and ACLs and
    /// delegation-token records use it. All authenticated callers ride under
    /// `principal_type = "User"`, which matches Kafka's
    /// `DefaultKafkaPrincipalBuilder`.
    #[must_use]
    pub fn to_kafka(&self) -> KafkaPrincipal {
        KafkaPrincipal {
            principal_type: "User".to_string(),
            name: self.name.clone(),
        }
    }
}

/// KIP-48: Kafka wire-level principal.
///
/// This is the `(principalType, name)` pair that delegation-token records, ACL
/// entries, and `KafkaPrincipal`-shaped fields carry across the Kafka protocol.
/// It is distinct from [`Principal`], which models the *runtime session*
/// identity, that is the auth method and the OAuth groups. The format is
/// stable: `Display` and `FromStr` round-trip the canonical `Type:Name` form.
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
    /// OAUTHBEARER token failed validation. The token was expired, carried bad
    /// claims, was a signed token that the unsecured validator rejected, had no
    /// principal, and so on. This maps to the RFC 7628 `invalid_token` server
    /// error status.
    #[error("invalid token")]
    InvalidToken,
    /// OAUTHBEARER introspection HTTP round-trip failed at the transport layer.
    ///
    /// This is distinct from `InvalidToken`, so the SASL handler can surface
    /// "`IdP` unreachable" separately from "client supplied a bad token". It
    /// maps to the RFC 7628 `invalid_token` server error status.
    #[error("oauthbearer introspection transport: {0}")]
    IntrospectionTransport(String),
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn from_sasl_mapping() {
        for (mechanism, want) in [
            (SaslMechanism::Plain, AuthMethod::SaslPlain),
            (SaslMechanism::ScramSha256, AuthMethod::SaslScramSha256),
            (SaslMechanism::ScramSha512, AuthMethod::SaslScramSha512),
            (SaslMechanism::OAuthBearer, AuthMethod::SaslOAuthBearer),
            (SaslMechanism::Gssapi, AuthMethod::SaslGssapi),
        ] {
            assert2::assert!(AuthMethod::from_sasl(mechanism) == want);
        }
    }

    #[test]
    fn principal_display_is_type_colon_name() {
        let p = KafkaPrincipal {
            principal_type: "User".into(),
            name: "alice".into(),
        };
        assert2::assert!(p.to_string() == "User:alice");
        // FromStr is the inverse.
        assert2::assert!("User:alice".parse::<KafkaPrincipal>().unwrap() == p);
    }
}
