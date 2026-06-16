use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ListenerProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl ListenerProtocol {
    #[must_use]
    pub fn requires_tls(self) -> bool {
        matches!(self, Self::Ssl | Self::SaslSsl)
    }

    #[must_use]
    pub fn requires_sasl(self) -> bool {
        matches!(self, Self::SaslPlaintext | Self::SaslSsl)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn tls_and_sasl_flags_per_protocol() {
        assert!(!ListenerProtocol::Plaintext.requires_tls());
        assert!(!ListenerProtocol::Plaintext.requires_sasl());
        assert!(ListenerProtocol::Ssl.requires_tls());
        assert!(!ListenerProtocol::Ssl.requires_sasl());
        assert!(!ListenerProtocol::SaslPlaintext.requires_tls());
        assert!(ListenerProtocol::SaslPlaintext.requires_sasl());
        assert!(ListenerProtocol::SaslSsl.requires_tls());
        assert!(ListenerProtocol::SaslSsl.requires_sasl());
    }
}
