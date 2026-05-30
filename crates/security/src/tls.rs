use std::path::PathBuf;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use thiserror::Error;

/// Whether the server requests and verifies a client certificate during
/// the TLS handshake (RFC 5246 §7.4.6 — Kafka's mTLS path).
///
/// `Required` rejects connections that don't present a cert chaining to
/// `client_ca_path`. `Optional` requests a cert but still accepts
/// anonymous handshakes — the dispatch layer is responsible for
/// surfacing the `Anonymous` outcome to gating logic. `Disabled` requests
/// no client cert.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientAuthMode {
    /// No client certificate requested. The handshake completes
    /// without `CertificateRequest`.
    #[default]
    Disabled,
    /// Client certificate is requested but the handshake also accepts
    /// peers that don't present one. The dispatch layer keeps such
    /// connections as ANONYMOUS.
    Optional,
    /// Client certificate is required. Handshake fails if the peer
    /// doesn't present a cert chaining to `client_ca_path`.
    Required,
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_chain_path: PathBuf,
    pub private_key_path: PathBuf,
    /// Roots used by the *client* side (this broker as an outbound
    /// inter-broker dialer) to verify server certs. Mirrors Kafka's
    /// `ssl.truststore.location` on the client.
    pub trust_roots_path: Option<PathBuf>,
    /// PEM file containing the CA(s) used to verify
    /// *incoming* client certs when `client_auth != Disabled`. Mirrors
    /// Kafka's `ssl.client.auth.truststore.location` (operator-supplied
    /// clients CA secret).
    pub client_ca_path: Option<PathBuf>,
    /// Client-cert request mode. Defaults to `Disabled`
    /// (no client cert requested).
    pub client_auth: ClientAuthMode,
}

#[derive(Debug, Error)]
pub enum TlsError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("rustls: {0}")]
    Rustls(#[from] rustls::Error),
    #[error("no private key in {0}")]
    NoPrivateKey(PathBuf),
    #[error("no certificates in {0}")]
    NoCerts(PathBuf),
    /// `client_auth` is `Optional`/`Required` but `client_ca_path` is
    /// unset. A client-cert verifier needs at least one trust root.
    #[error("client_auth is enabled but no client_ca_path configured")]
    MissingClientCa,
    /// rustls's `WebPkiClientVerifier::builder` rejected the supplied
    /// trust roots (typically: cert isn't a CA, or the public key
    /// algorithm isn't supported).
    #[error("client cert verifier build failed: {0}")]
    VerifierBuild(String),
}

impl TlsConfig {
    pub fn build_server_config(&self) -> Result<Arc<rustls::ServerConfig>, TlsError> {
        let certs = load_certs(&self.cert_chain_path)?;
        let key = load_private_key(&self.private_key_path)?;
        let builder = rustls::ServerConfig::builder();
        let cfg = match self.client_auth {
            ClientAuthMode::Disabled => {
                builder.with_no_client_auth().with_single_cert(certs, key)?
            }
            ClientAuthMode::Optional | ClientAuthMode::Required => {
                let ca_path = self
                    .client_ca_path
                    .as_ref()
                    .ok_or(TlsError::MissingClientCa)?;
                let mut roots = rustls::RootCertStore::empty();
                for cert in load_certs(ca_path)? {
                    roots.add(cert)?;
                }
                let verifier_builder =
                    rustls::server::WebPkiClientVerifier::builder(Arc::new(roots));
                let verifier = match self.client_auth {
                    ClientAuthMode::Optional => verifier_builder.allow_unauthenticated().build(),
                    ClientAuthMode::Required => verifier_builder.build(),
                    ClientAuthMode::Disabled => unreachable!(),
                }
                .map_err(|e| TlsError::VerifierBuild(e.to_string()))?;
                builder
                    .with_client_cert_verifier(verifier)
                    .with_single_cert(certs, key)?
            }
        };
        Ok(Arc::new(cfg))
    }

    pub fn build_client_config(&self) -> Result<Arc<rustls::ClientConfig>, TlsError> {
        let mut roots = rustls::RootCertStore::empty();
        if let Some(path) = &self.trust_roots_path {
            for cert in load_certs(path)? {
                roots.add(cert)?;
            }
        }
        let cfg = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Arc::new(cfg))
    }
}

fn load_certs(path: &PathBuf) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    use rustls::pki_types::pem::PemObject;
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
        .map_err(|e| TlsError::Io(std::io::Error::other(e.to_string())))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::Io(std::io::Error::other(e.to_string())))?;
    if certs.is_empty() {
        return Err(TlsError::NoCerts(path.clone()));
    }
    Ok(certs)
}

fn load_private_key(path: &PathBuf) -> Result<PrivateKeyDer<'static>, TlsError> {
    use rustls::pki_types::pem::PemObject;
    PrivateKeyDer::from_pem_file(path).map_err(|_| TlsError::NoPrivateKey(path.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use std::fs::File;
    use std::io::Write;

    fn install_provider() {
        // rustls requires an explicit CryptoProvider when no default feature is
        // compiled in.  We use ring, which is already in the workspace.
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    fn write_self_signed(dir: &std::path::Path) -> (PathBuf, PathBuf) {
        // Reuse a deterministic dev cert; for the unit test we just need
        // valid PEM. We embed pre-generated PEMs as constants.
        // (Generated with: openssl req -x509 -newkey ed25519 -nodes -days 36500 \
        //   -subj "//CN=crabka-dev" -keyout key.pem -out cert.pem)
        let cert_pem = include_str!("../tests/fixtures/dev_cert.pem");
        let key_pem = include_str!("../tests/fixtures/dev_key.pem");
        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");
        File::create(&cert_path)
            .unwrap()
            .write_all(cert_pem.as_bytes())
            .unwrap();
        File::create(&key_path)
            .unwrap()
            .write_all(key_pem.as_bytes())
            .unwrap();
        (cert_path, key_path)
    }

    fn write_client_ca(dir: &std::path::Path) -> PathBuf {
        // Self-signed dev client CA. Generated with:
        //   openssl ecparam -name prime256v1 -genkey -noout -out ca.key
        //   openssl req -x509 -new -key ca.key -days 36500 \
        //     -subj "/CN=crabka-dev-client-ca" -out ca.pem
        let pem = include_str!("../tests/fixtures/dev_client_ca.pem");
        let p = dir.join("client_ca.pem");
        File::create(&p).unwrap().write_all(pem.as_bytes()).unwrap();
        p
    }

    #[test]
    fn valid_cert_and_key_loads() {
        install_provider();
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = write_self_signed(dir.path());
        let cfg = TlsConfig {
            cert_chain_path: cert_path,
            private_key_path: key_path,
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: ClientAuthMode::Disabled,
        };
        cfg.build_server_config().expect("build server cfg");
    }

    #[test]
    fn missing_cert_errors() {
        let cfg = TlsConfig {
            cert_chain_path: PathBuf::from("/nonexistent/cert.pem"),
            private_key_path: PathBuf::from("/nonexistent/key.pem"),
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: ClientAuthMode::Disabled,
        };
        assert!(cfg.build_server_config().is_err());
    }

    #[test]
    fn client_auth_required_without_ca_errors() {
        install_provider();
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = write_self_signed(dir.path());
        let cfg = TlsConfig {
            cert_chain_path: cert_path,
            private_key_path: key_path,
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: ClientAuthMode::Required,
        };
        let err = cfg.build_server_config().unwrap_err();
        assert!(
            matches!(err, TlsError::MissingClientCa),
            "expected MissingClientCa, got {err:?}"
        );
    }

    #[test]
    fn client_auth_required_with_ca_builds() {
        install_provider();
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = write_self_signed(dir.path());
        let ca_path = write_client_ca(dir.path());
        let cfg = TlsConfig {
            cert_chain_path: cert_path,
            private_key_path: key_path,
            trust_roots_path: None,
            client_ca_path: Some(ca_path),
            client_auth: ClientAuthMode::Required,
        };
        cfg.build_server_config()
            .expect("build with client cert verifier");
    }

    #[test]
    fn client_auth_optional_with_ca_builds() {
        install_provider();
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = write_self_signed(dir.path());
        let ca_path = write_client_ca(dir.path());
        let cfg = TlsConfig {
            cert_chain_path: cert_path,
            private_key_path: key_path,
            trust_roots_path: None,
            client_ca_path: Some(ca_path),
            client_auth: ClientAuthMode::Optional,
        };
        cfg.build_server_config()
            .expect("build with optional client cert verifier");
    }
}
