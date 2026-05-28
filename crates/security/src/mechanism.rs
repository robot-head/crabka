use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::IntoStaticStr,
    strum::EnumString,
)]
pub enum SaslMechanism {
    #[strum(serialize = "PLAIN")]
    Plain,
    #[strum(serialize = "SCRAM-SHA-256")]
    ScramSha256,
    #[strum(serialize = "SCRAM-SHA-512")]
    ScramSha512,
    /// SASL/OAUTHBEARER (KIP-255 / RFC 7628). The bearer token is validated
    /// by the broker's configured token validator.
    #[strum(serialize = "OAUTHBEARER")]
    OAuthBearer,
}

impl SaslMechanism {
    #[must_use]
    pub fn wire_name(self) -> &'static str {
        self.into()
    }

    #[must_use]
    pub fn from_wire(s: &str) -> Option<Self> {
        s.parse().ok()
    }

    /// `true` for SCRAM mechanisms (SHA-256 and SHA-512). Used by
    /// handshake / authenticate code that treats both the same way at
    /// the dispatch level.
    #[must_use]
    pub fn is_scram(self) -> bool {
        matches!(self, Self::ScramSha256 | Self::ScramSha512)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_name_round_trip() {
        for m in [
            SaslMechanism::Plain,
            SaslMechanism::ScramSha256,
            SaslMechanism::ScramSha512,
            SaslMechanism::OAuthBearer,
        ] {
            assert_eq!(SaslMechanism::from_wire(m.wire_name()), Some(m));
        }
    }

    #[test]
    fn from_wire_unknown_returns_none() {
        assert_eq!(SaslMechanism::from_wire("SCRAM-SHA-128"), None);
        assert_eq!(SaslMechanism::from_wire("OAUTH"), None);
        assert_eq!(SaslMechanism::from_wire(""), None);
    }

    /// Wire-exactness guard: only the canonical Kafka mechanism strings
    /// parse. The Rust variant names and other casings must NOT, so the
    /// `strum(serialize = ...)` attributes can never silently loosen the
    /// SASL handshake's accepted mechanism set.
    #[test]
    fn from_wire_rejects_variant_names_and_casing() {
        assert_eq!(SaslMechanism::from_wire("Plain"), None);
        assert_eq!(SaslMechanism::from_wire("plain"), None);
        assert_eq!(SaslMechanism::from_wire("ScramSha256"), None);
        assert_eq!(SaslMechanism::from_wire("scram-sha-256"), None);
        assert_eq!(SaslMechanism::from_wire("OAuthBearer"), None);
    }

    #[test]
    fn oauthbearer_wire_round_trip() {
        assert_eq!(
            SaslMechanism::from_wire("OAUTHBEARER"),
            Some(SaslMechanism::OAuthBearer)
        );
        assert_eq!(SaslMechanism::OAuthBearer.wire_name(), "OAUTHBEARER");
        assert!(!SaslMechanism::OAuthBearer.is_scram());
    }

    #[test]
    fn is_scram_predicate() {
        assert!(!SaslMechanism::Plain.is_scram());
        assert!(SaslMechanism::ScramSha256.is_scram());
        assert!(SaslMechanism::ScramSha512.is_scram());
    }
}
