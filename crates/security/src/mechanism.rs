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
    /// SASL/GSSAPI (Kerberos, RFC 4752). Context establishment and the
    /// RFC 4752 security-layer negotiation are driven by the broker's
    /// configured service keytab via the GSS provider.
    #[strum(serialize = "GSSAPI")]
    Gssapi,
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
    use assert2::{assert, check};

    #[test]
    fn wire_name_round_trip() {
        for m in [
            SaslMechanism::Plain,
            SaslMechanism::ScramSha256,
            SaslMechanism::ScramSha512,
            SaslMechanism::OAuthBearer,
        ] {
            assert!(SaslMechanism::from_wire(m.wire_name()) == Some(m));
        }
    }

    #[test]
    fn from_wire_unknown_returns_none() {
        for wire in ["SCRAM-SHA-128", "OAUTH", ""] {
            assert!(SaslMechanism::from_wire(wire) == None, "case {wire:?}");
        }
    }

    /// Wire-exactness guard: only the canonical Kafka mechanism strings
    /// parse. The Rust variant names and other casings must NOT, so the
    /// `strum(serialize = ...)` attributes can never silently loosen the
    /// SASL handshake's accepted mechanism set.
    #[test]
    fn from_wire_rejects_variant_names_and_casing() {
        for wire in [
            "Plain",
            "plain",
            "ScramSha256",
            "scram-sha-256",
            "OAuthBearer",
        ] {
            assert!(SaslMechanism::from_wire(wire) == None, "case {wire:?}");
        }
    }

    #[test]
    fn oauthbearer_wire_round_trip() {
        check!(SaslMechanism::from_wire("OAUTHBEARER") == Some(SaslMechanism::OAuthBearer));
        check!(SaslMechanism::OAuthBearer.wire_name() == "OAUTHBEARER");
        check!(!SaslMechanism::OAuthBearer.is_scram());
    }

    #[test]
    fn gssapi_mechanism_roundtrips_wire_name() {
        use std::str::FromStr;
        assert!(SaslMechanism::from_str("GSSAPI").unwrap() == SaslMechanism::Gssapi);
        assert!(SaslMechanism::Gssapi.wire_name() == "GSSAPI");
    }

    #[test]
    fn is_scram_predicate() {
        for (mechanism, want) in [
            (SaslMechanism::Plain, false),
            (SaslMechanism::ScramSha256, true),
            (SaslMechanism::ScramSha512, true),
        ] {
            assert!(mechanism.is_scram() == want, "case {mechanism:?}");
        }
    }
}
