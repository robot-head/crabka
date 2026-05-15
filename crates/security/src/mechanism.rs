use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SaslMechanism {
    Plain,
    ScramSha512,
}

impl SaslMechanism {
    #[must_use]
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    #[must_use]
    pub fn from_wire(s: &str) -> Option<Self> {
        match s {
            "PLAIN" => Some(Self::Plain),
            "SCRAM-SHA-512" => Some(Self::ScramSha512),
            _ => None,
        }
    }
}
