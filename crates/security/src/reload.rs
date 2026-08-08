//! Hot-reloadable TLS server config.
//!
//! This module wraps a [`rustls::ServerConfig`] in an [`arc_swap::ArcSwap`], so
//! the broker can swap cert/key/client-CA without a listener restart and
//! without breaking in-flight connections. A new TLS handshake picks up the
//! latest config. An already-established TLS session continues to use the
//! `ServerConfig` it negotiated against.
//!
//! This works with the existing [`crate::TlsConfig`]. The path-and-options
//! struct stays as the source of truth, and
//! [`DynamicServerConfig::reload_from`] re-reads the files into a fresh
//! `Arc<ServerConfig>` and atomically swaps it in.

use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::tls::{TlsConfig, TlsError};

/// Atomically swappable wrapper around a [`rustls::ServerConfig`].
///
/// A clone costs one `Arc` bump. A read is lock-free. The only expensive
/// operation is [`reload_from`], which re-parses cert files.
///
/// [`reload_from`]: Self::reload_from
#[derive(Debug)]
pub struct DynamicServerConfig {
    inner: ArcSwap<rustls::ServerConfig>,
}

impl DynamicServerConfig {
    /// Build a fresh `DynamicServerConfig` from a [`TlsConfig`].
    ///
    /// This function reads the cert, the key, and the optional client-CA paths
    /// immediately.
    ///
    /// # Errors
    ///
    /// Propagates the underlying [`TlsError`] from
    /// [`TlsConfig::build_server_config`].
    // Initial dynamic-TLS-config construction. skip_all keeps cert/key material
    // out of span fields. `err` surfaces build failures (Debug).
    #[tracing::instrument(level = "info", skip_all, err)]
    pub fn from_tls_config(cfg: &TlsConfig) -> Result<Arc<Self>, TlsError> {
        let server_config = cfg.build_server_config()?;
        Ok(Arc::new(Self {
            inner: ArcSwap::new(server_config),
        }))
    }

    /// Snapshot the current `ServerConfig`.
    ///
    /// The returned `Arc` is independent of later
    /// [`reload_from`](Self::reload_from) calls. A concurrent reload does not
    /// change the handshakes that already run against this snapshot.
    #[must_use]
    pub fn current(&self) -> Arc<rustls::ServerConfig> {
        // `ArcSwap::load_full` clones the inner Arc (one atomic bump);
        // we then unwrap the outer `Guard` into a plain `Arc`.
        self.inner.load_full()
    }

    /// Re-read the cert, the key, and the optional client-CA from disk, then
    /// swap the new `ServerConfig` in atomically.
    ///
    /// On error this function keeps the previous config in place and returns
    /// the error to the caller.
    ///
    /// A reload has no semantic effect when the inputs have not changed. But
    /// rustls exposes no content-equality hash on `ServerConfig`, so this
    /// function rebuilds unconditionally. A caller that wants to skip identical
    /// reloads should diff the mtimes upstream.
    ///
    /// # Errors
    ///
    /// Propagates the underlying [`TlsError`] from
    /// [`TlsConfig::build_server_config`].
    // TLS cert/key hot-reload + atomic swap. skip_all keeps cert/key material
    // out of span fields. `err` surfaces rebuild failures (Debug); on error the
    // previous config stays in place.
    #[tracing::instrument(level = "info", skip_all, err)]
    pub fn reload_from(&self, cfg: &TlsConfig) -> Result<(), TlsError> {
        let new = cfg.build_server_config()?;
        self.inner.store(new);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Write, path::PathBuf};

    use super::*;
    use crate::tls::ClientAuthMode;

    fn install_provider() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    fn write_pair(dir: &std::path::Path, cert_pem: &str, key_pem: &str) -> (PathBuf, PathBuf) {
        let cp = dir.join("cert.pem");
        let kp = dir.join("key.pem");
        File::create(&cp)
            .unwrap()
            .write_all(cert_pem.as_bytes())
            .unwrap();
        File::create(&kp)
            .unwrap()
            .write_all(key_pem.as_bytes())
            .unwrap();
        (cp, kp)
    }

    /// The `current()` snapshot is stable across a later `reload_from`. An
    /// existing handshake that captured the old `Arc<ServerConfig>` continues
    /// to see the old certs.
    #[test]
    fn snapshot_is_stable_across_reload() {
        install_provider();
        let dir = tempfile::tempdir().unwrap();
        let (cp, kp) = write_pair(
            dir.path(),
            include_str!("../tests/fixtures/dev_cert.pem"),
            include_str!("../tests/fixtures/dev_key.pem"),
        );
        let cfg = TlsConfig {
            cert_chain_path: cp.clone(),
            private_key_path: kp.clone(),
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: ClientAuthMode::Disabled,
        };
        let dynamic = DynamicServerConfig::from_tls_config(&cfg).unwrap();
        let snap_before = dynamic.current();

        // Overwrite the cert files with the alt fixture and reload.
        std::fs::write(&cp, include_str!("../tests/fixtures/dev_cert_alt.pem")).unwrap();
        std::fs::write(&kp, include_str!("../tests/fixtures/dev_key_alt.pem")).unwrap();
        dynamic.reload_from(&cfg).expect("reload must succeed");
        let snap_after = dynamic.current();

        // The two snapshots must be distinct `Arc`s — `current()`
        // returns the latest, so `after != before` is the post-swap
        // invariant. Use raw pointer comparison: `ServerConfig` is not
        // `PartialEq`, but `Arc::ptr_eq` is exactly what we want.
        assert2::assert!(!Arc::ptr_eq(&snap_before, &snap_after));
    }

    /// A reload error leaves the prior config in place. Valid filesystem inputs
    /// make a "rebuild fails" path hard to produce, so this test points the
    /// second reload at a nonexistent cert path. The current snapshot must
    /// remain the originally-loaded config.
    #[test]
    fn reload_error_does_not_swap() {
        install_provider();
        let dir = tempfile::tempdir().unwrap();
        let (cp, kp) = write_pair(
            dir.path(),
            include_str!("../tests/fixtures/dev_cert.pem"),
            include_str!("../tests/fixtures/dev_key.pem"),
        );
        let cfg = TlsConfig {
            cert_chain_path: cp,
            private_key_path: kp,
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: ClientAuthMode::Disabled,
        };
        let dynamic = DynamicServerConfig::from_tls_config(&cfg).unwrap();
        let snap_before = dynamic.current();

        let bogus = TlsConfig {
            cert_chain_path: dir.path().join("missing.pem"),
            private_key_path: dir.path().join("missing.key"),
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: ClientAuthMode::Disabled,
        };
        let err = dynamic.reload_from(&bogus).unwrap_err();
        // Don't pattern-match on the variant — `NoCerts` vs
        // `NoPrivateKey` depends on read order. The point is that
        // `reload_from` errored.
        let _ = err;

        let snap_after = dynamic.current();
        assert2::assert!(Arc::ptr_eq(&snap_before, &snap_after));
    }
}
