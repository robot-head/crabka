use std::path::PathBuf;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_chain_path: PathBuf,
    pub private_key_path: PathBuf,
    pub trust_roots_path: Option<PathBuf>,
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
}

impl TlsConfig {
    pub fn build_server_config(&self) -> Result<Arc<rustls::ServerConfig>, TlsError> {
        let certs = load_certs(&self.cert_chain_path)?;
        let key = load_private_key(&self.private_key_path)?;
        let cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)?;
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

    #[test]
    fn valid_cert_and_key_loads() {
        install_provider();
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = write_self_signed(dir.path());
        let cfg = TlsConfig {
            cert_chain_path: cert_path,
            private_key_path: key_path,
            trust_roots_path: None,
        };
        cfg.build_server_config().expect("build server cfg");
    }

    #[test]
    fn missing_cert_errors() {
        let cfg = TlsConfig {
            cert_chain_path: PathBuf::from("/nonexistent/cert.pem"),
            private_key_path: PathBuf::from("/nonexistent/key.pem"),
            trust_roots_path: None,
        };
        assert!(cfg.build_server_config().is_err());
    }
}
