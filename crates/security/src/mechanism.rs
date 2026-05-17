use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl SaslMechanism {
    #[must_use]
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    #[must_use]
    pub fn from_wire(s: &str) -> Option<Self> {
        match s {
            "PLAIN" => Some(Self::Plain),
            "SCRAM-SHA-256" => Some(Self::ScramSha256),
            "SCRAM-SHA-512" => Some(Self::ScramSha512),
            _ => None,
        }
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
        ] {
            assert_eq!(SaslMechanism::from_wire(m.wire_name()), Some(m));
        }
    }

    #[test]
    fn from_wire_unknown_returns_none() {
        assert_eq!(SaslMechanism::from_wire("SCRAM-SHA-128"), None);
        assert_eq!(SaslMechanism::from_wire("OAUTHBEARER"), None);
        assert_eq!(SaslMechanism::from_wire(""), None);
    }

    #[test]
    fn is_scram_predicate() {
        assert!(!SaslMechanism::Plain.is_scram());
        assert!(SaslMechanism::ScramSha256.is_scram());
        assert!(SaslMechanism::ScramSha512.is_scram());
    }
}
