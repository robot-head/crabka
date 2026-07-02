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
    use assert2::assert;

    use super::*;

    #[test]
    fn tls_and_sasl_flags_per_protocol() {
        for (protocol, tls, sasl) in [
            (ListenerProtocol::Plaintext, false, false),
            (ListenerProtocol::Ssl, true, false),
            (ListenerProtocol::SaslPlaintext, false, true),
            (ListenerProtocol::SaslSsl, true, true),
        ] {
            assert!(protocol.requires_tls() == tls, "requires_tls {protocol:?}");
            assert!(
                protocol.requires_sasl() == sasl,
                "requires_sasl {protocol:?}"
            );
        }
    }
}
