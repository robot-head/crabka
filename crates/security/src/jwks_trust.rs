//! Build a rustls `ClientConfig` whose trust roots are exclusively the
//! certificates in a user-supplied PEM bundle. Backs the
//! broker's outbound HTTPS to the JWKS endpoint when the operator
//! configures a private `IdP` CA (Strimzi-shaped tlsTrustedCertificates
//! "replace" semantic — webpki-roots are not consulted when this is
//! used).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum JwksTrustError {
    #[error("read {0}: {1}")]
    Io(PathBuf, std::io::Error),
    #[error("no certificates in {0}")]
    Empty(PathBuf),
    #[error("rustls add cert: {0}")]
    Rustls(#[from] rustls::Error),
}

/// Read a PEM bundle of one or more CA certificates and produce a
/// `rustls::ClientConfig` that trusts exactly those certificates. The
/// returned config has no client auth (the broker does not present a
/// client cert when fetching the JWKS endpoint).
pub fn build_client_config_from_pem(
    path: &Path,
) -> Result<Arc<rustls::ClientConfig>, JwksTrustError> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
        .map_err(|e| JwksTrustError::Io(path.into(), std::io::Error::other(e.to_string())))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| JwksTrustError::Io(path.into(), std::io::Error::other(e.to_string())))?;
    if certs.is_empty() {
        return Err(JwksTrustError::Empty(path.into()));
    }
    let mut roots = rustls::RootCertStore::empty();
    for cert in certs {
        roots.add(cert)?;
    }
    Ok(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use std::fs::File;
    use std::io::Write;

    fn install_provider() {
        // rustls requires an explicit CryptoProvider when no default feature
        // is compiled in. Mirrors the pattern in tls.rs.
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    fn dev_cert_pem() -> &'static str {
        include_str!("../tests/fixtures/dev_cert.pem")
    }

    #[test]
    fn build_client_config_from_pem_loads_single_cert() {
        install_provider();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ca.pem");
        File::create(&path)
            .unwrap()
            .write_all(dev_cert_pem().as_bytes())
            .unwrap();
        let cfg = build_client_config_from_pem(&path).expect("single cert PEM should load");
        // Smoke-check it's actually a ClientConfig (compile-level type
        // check is the main assertion; runtime use is exercised by HTTPS
        // integration tests).
        let _: Arc<rustls::ClientConfig> = cfg;
    }

    #[test]
    fn build_client_config_from_pem_loads_concatenated_chain() {
        install_provider();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ca-chain.pem");
        let mut f = File::create(&path).unwrap();
        f.write_all(dev_cert_pem().as_bytes()).unwrap();
        f.write_all(b"\n").unwrap();
        f.write_all(dev_cert_pem().as_bytes()).unwrap();
        let cfg = build_client_config_from_pem(&path);
        assert!(cfg.is_ok(), "concatenated chain should load: {cfg:?}");
    }

    #[test]
    fn build_client_config_from_pem_rejects_missing_file() {
        install_provider();
        let err = build_client_config_from_pem(Path::new("/nonexistent/path.pem")).unwrap_err();
        assert!(
            matches!(err, JwksTrustError::Io(_, _)),
            "expected Io, got {err:?}",
        );
    }

    #[test]
    fn build_client_config_from_pem_rejects_empty_pem() {
        install_provider();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.pem");
        File::create(&path)
            .unwrap()
            .write_all(b"# no certs in this file\n")
            .unwrap();
        let err = build_client_config_from_pem(&path).unwrap_err();
        // Either Empty (parser returned zero certs) or Io (parser
        // errored on the "comment-only" content). Both are acceptable
        // rejection shapes — both reach the right outcome (no
        // ClientConfig produced).
        assert!(
            matches!(err, JwksTrustError::Empty(_) | JwksTrustError::Io(_, _)),
            "expected Empty or Io, got {err:?}",
        );
    }

    #[test]
    fn build_client_config_from_pem_rejects_non_pem_garbage() {
        install_provider();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("garbage.bin");
        File::create(&path).unwrap().write_all(&[0u8; 64]).unwrap();
        let err = build_client_config_from_pem(&path);
        assert!(
            err.is_err(),
            "arbitrary bytes should not parse as a PEM bundle"
        );
    }
}
