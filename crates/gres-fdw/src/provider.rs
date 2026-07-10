//! Establishes ONE process-global rustls `CryptoProvider` (rustcrypto) so both the
//! pgwire frontend and the crabka-client backend resolve the same default.
use std::sync::Once;

static INSTALL: Once = Once::new();

/// Install the rustcrypto provider as the process-global rustls default.
///
/// Idempotent: safe to call multiple times; the actual install runs exactly once.
/// This mirrors what the pgwire frontend does (`rustls::ServerConfig::builder_with_provider`
/// with `Arc::new(rustls_rustcrypto::provider())`), but installs it as the process
/// default so that `crabka-client-core`'s `TlsConnectorConfig::build()` — which calls
/// `rustls::ClientConfig::builder()` (no explicit provider) — picks up rustcrypto
/// instead of requiring ring.
pub fn install_default_provider() {
    INSTALL.call_once(|| {
        let _ = rustls_rustcrypto::provider().install_default();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Coexistence spike: proves that after installing the rustcrypto process default,
    /// `crabka_client_core::security::TlsConnectorConfig::build()` succeeds — meaning
    /// the kafka client TLS rides on the rustcrypto default without requiring ring.
    #[test]
    fn pgwire_and_kafka_tls_configs_build_together() {
        install_default_provider();

        // crabka-client-core builds its ClientConfig from the process default:
        let tls = crabka_client_core::security::TlsConnectorConfig {
            trust_roots_pem: None,
            server_name: "localhost".into(),
            client_identity: None,
        };
        let client_cfg = tls.build();
        assert!(
            client_cfg.is_ok(),
            "kafka client TLS must build on the rustcrypto default: {client_cfg:?}"
        );

        // Also verify a pgwire-style ServerConfig (rustcrypto) builds in the same process.
        // This exercises the same path the pgwire frontend uses.
        let provider = std::sync::Arc::new(rustls_rustcrypto::provider());
        let server_cfg = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("rustcrypto supports the default TLS versions")
            .with_no_client_auth()
            .with_cert_resolver(std::sync::Arc::new(
                rustls::server::ResolvesServerCertUsingSni::new(),
            ));
        // We just assert it doesn't panic — we don't have a cert to bind, so merely
        // constructing the final step proves the provider chain is valid.
        let _ = server_cfg;
    }
}
