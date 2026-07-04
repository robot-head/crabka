//! Client-side TLS/SASL security surface for [`crate::Client`].
//!
//! Mirrors the broker's inter-broker credential + TLS shapes so the
//! public clients and the inter-broker dialer negotiate the same way.

use std::{path::PathBuf, sync::Arc};

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
    /// Optional mTLS client identity: `(cert_chain_pem, private_key_pem)`.
    ///
    /// When `Some`, the cert chain and key are loaded from the given PEM
    /// files and presented to the server during the TLS handshake
    /// (mutual TLS / client authentication).  `None` → one-way TLS; the
    /// client does not present a certificate (`with_no_client_auth`).
    pub client_identity: Option<(PathBuf, PathBuf)>,
}

impl TlsConnectorConfig {
    /// Build a `rustls::ClientConfig`.
    ///
    /// When [`Self::client_identity`] is `Some`, the cert chain and key are
    /// loaded and the config is built with mutual TLS client authentication.
    /// When `None`, the client presents no certificate (`with_no_client_auth`).
    ///
    /// # Errors
    /// Returns a string error if any PEM file fails to load or parse.
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
        let cfg = if let Some((cert_pem, key_pem)) = &self.client_identity {
            let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
                rustls::pki_types::CertificateDer::pem_file_iter(cert_pem)
                    .map_err(|e| format!("client cert load {}: {e}", cert_pem.display()))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| format!("client cert parse: {e}"))?;
            let key = rustls::pki_types::PrivateKeyDer::from_pem_file(key_pem)
                .map_err(|e| format!("client key load {}: {e}", key_pem.display()))?;
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_client_auth_cert(certs, key)
                .map_err(|e| format!("client auth cert: {e}"))?
        } else {
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth()
        };
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
    /// Canonical hostname for the SASL handshake — the GSSAPI service
    /// principal host (`service_name/<sasl_host>`). Meaningful whenever
    /// the protocol [`requires_sasl`], independent of TLS: a
    /// `SASL_PLAINTEXT` listener has no `tls` to source the host from, so
    /// without this GSSAPI would fall back to `localhost` and Kerberos
    /// would reject the principal. `None` falls back to `tls.server_name`
    /// then the connection's target host. PLAIN/SCRAM ignore it.
    ///
    /// [`requires_sasl`]: ListenerProtocol::requires_sasl
    pub sasl_host: Option<String>,
}

impl ClientSecurity {
    /// Resolve the hostname handed to the SASL handshake (the GSSAPI SPN
    /// host). Prefers the explicit [`Self::sasl_host`], then the TLS SNI
    /// ([`TlsConnectorConfig::server_name`]), then the connection's target
    /// `host` if known, falling back to `"localhost"`.
    ///
    /// TLS SNI is unaffected — it is always sourced from `tls.server_name`.
    #[must_use]
    pub fn sasl_handshake_host<'a>(&'a self, target_host: Option<&'a str>) -> &'a str {
        if let Some(h) = self.sasl_host.as_deref() {
            h
        } else if let Some(tls) = self.tls.as_ref() {
            tls.server_name.as_str()
        } else if let Some(h) = target_host {
            h
        } else {
            "localhost"
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_security::ListenerProtocol;

    use super::*;

    #[test]
    fn plaintext_security_has_no_tls_or_sasl() {
        let s = ClientSecurity {
            protocol: ListenerProtocol::Plaintext,
            tls: None,
            sasl: None,
            sasl_host: None,
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
            sasl_host: None,
        };
        assert!(s.protocol.requires_sasl());
        assert!(matches!(s.sasl, Some(SaslCredentials::Plain { .. })));
    }

    #[test]
    fn sasl_handshake_host_prefers_explicit_field() {
        // SASL_PLAINTEXT (no TLS) with an explicit host: GSSAPI must get
        // the real SPN host, not "localhost" or the target host.
        let s = ClientSecurity {
            protocol: ListenerProtocol::SaslPlaintext,
            tls: None,
            sasl: None,
            sasl_host: Some("kdc-broker.example.com".into()),
        };
        assert!(s.sasl_handshake_host(Some("10.0.0.5")) == "kdc-broker.example.com");
    }

    #[test]
    fn sasl_handshake_host_falls_back_to_tls_then_target_then_localhost() {
        // No explicit sasl_host → TLS SNI wins.
        let with_tls = ClientSecurity {
            protocol: ListenerProtocol::SaslSsl,
            tls: Some(TlsConnectorConfig {
                trust_roots_pem: None,
                server_name: "tls-host".into(),
                client_identity: None,
            }),
            sasl: None,
            sasl_host: None,
        };
        assert!(with_tls.sasl_handshake_host(Some("10.0.0.5")) == "tls-host");

        // No sasl_host, no TLS → target host wins.
        let no_tls = ClientSecurity {
            protocol: ListenerProtocol::SaslPlaintext,
            tls: None,
            sasl: None,
            sasl_host: None,
        };
        assert!(no_tls.sasl_handshake_host(Some("10.0.0.5")) == "10.0.0.5");

        // Nothing set at all → localhost.
        assert!(no_tls.sasl_handshake_host(None) == "localhost");
    }

    #[test]
    fn tls_connector_config_builds_client_config() {
        // Empty trust roots → webpki defaults disabled; we only assert it builds.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cfg = TlsConnectorConfig {
            trust_roots_pem: None,
            server_name: "broker".into(),
            client_identity: None,
        };
        cfg.build().expect("client config builds with empty roots");
    }

    #[test]
    fn tls_connector_config_client_identity_none_builds_and_bogus_path_errors() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        // None path: one-way TLS builds successfully.
        let no_id = TlsConnectorConfig {
            trust_roots_pem: None,
            server_name: "broker".into(),
            client_identity: None,
        };
        no_id
            .build()
            .expect("one-way TLS builds with client_identity=None");

        // Bogus path: mTLS arm must return Err (files don't exist).
        let bogus = TlsConnectorConfig {
            trust_roots_pem: None,
            server_name: "broker".into(),
            client_identity: Some((
                "/nonexistent/cert.pem".into(),
                "/nonexistent/key.pem".into(),
            )),
        };
        assert!(
            bogus.build().is_err(),
            "bogus client-identity path returns Err"
        );
    }
}
