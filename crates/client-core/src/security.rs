//! Client-side TLS/SASL security surface for [`crate::Client`].
//!
//! Mirrors the broker's inter-broker credential + TLS shapes so the
//! public clients and the inter-broker dialer negotiate the same way.

use std::path::PathBuf;
use std::sync::Arc;

use crabka_security::ListenerProtocol;
use rustls_pki_types::pem::PemObject;
use tokio_rustls::TlsConnector;

pub use crate::sasl::SaslCredentials;

/// Client-side TLS trust + SNI. Mirrors the trust-roots half of the
/// broker's `crabka_security::TlsConfig::build_client_config`.
#[derive(Debug, Clone)]
pub struct TlsConnectorConfig {
    /// PEM file of CA certs the client trusts to verify the broker's
    /// server cert. `None` → empty root store (handshake fails unless
    /// the server cert chains to a webpki default, which we do not
    /// install — mirrors the broker's strict `build_client_config`).
    pub trust_roots_pem: Option<PathBuf>,
    /// SNI / server-name used for the TLS handshake and as the
    /// canonical hostname for any GSSAPI SPN.
    pub server_name: String,
}

impl TlsConnectorConfig {
    /// Build a `rustls::ClientConfig` (no client cert; trust-roots only).
    ///
    /// # Errors
    /// Returns a string error if a trust-roots PEM is configured but
    /// fails to load or add to the root store.
    pub fn build(&self) -> Result<Arc<rustls::ClientConfig>, String> {
        let mut roots = rustls::RootCertStore::empty();
        if let Some(path) = &self.trust_roots_pem {
            for cert in rustls::pki_types::CertificateDer::pem_file_iter(path)
                .map_err(|e| format!("trust roots load {}: {e}", path.display()))?
            {
                let cert = cert.map_err(|e| format!("trust roots parse: {e}"))?;
                roots
                    .add(cert)
                    .map_err(|e| format!("trust roots add: {e}"))?;
            }
        }
        let cfg = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Arc::new(cfg))
    }

    /// Build a ready `TlsConnector`.
    ///
    /// # Errors
    /// Propagates [`Self::build`] failures.
    pub fn connector(&self) -> Result<TlsConnector, String> {
        Ok(TlsConnector::from(self.build()?))
    }
}

/// Full client security policy: which listener protocol to speak, plus
/// the TLS and SASL material it implies. `None` fields are required to
/// match `protocol` (a `SaslSsl` policy needs both `tls` and `sasl`).
#[derive(Debug, Clone)]
pub struct ClientSecurity {
    pub protocol: ListenerProtocol,
    pub tls: Option<TlsConnectorConfig>,
    pub sasl: Option<SaslCredentials>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_security::ListenerProtocol;

    #[test]
    fn plaintext_security_has_no_tls_or_sasl() {
        let s = ClientSecurity {
            protocol: ListenerProtocol::Plaintext,
            tls: None,
            sasl: None,
        };
        assert!(!s.protocol.requires_tls());
        assert!(!s.protocol.requires_sasl());
    }

    #[test]
    fn sasl_plaintext_carries_creds() {
        let s = ClientSecurity {
            protocol: ListenerProtocol::SaslPlaintext,
            tls: None,
            sasl: Some(SaslCredentials::Plain {
                username: "u".into(),
                password: "p".into(),
            }),
        };
        assert!(s.protocol.requires_sasl());
        assert!(matches!(s.sasl, Some(SaslCredentials::Plain { .. })));
    }

    #[test]
    fn tls_connector_config_builds_client_config() {
        // Empty trust roots → webpki defaults disabled; we only assert it builds.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cfg = TlsConnectorConfig {
            trust_roots_pem: None,
            server_name: "broker".into(),
        };
        cfg.build().expect("client config builds with empty roots");
    }
}
