//! Background JWKS refresher for SASL/OAUTHBEARER signed-token validation
//! (slice 49b).
//!
//! `crates/security` is I/O-free: it parses a JWKS *string* and verifies tokens
//! against an in-memory key set held behind a [`JwksHandle`]. This module is the
//! one place that reaches the network — it periodically GETs the identity
//! provider's JWKS endpoint, parses it, and atomically swaps the new key set
//! into the shared handle so the [`SignedJwsValidator`] picks up rotated keys
//! with no restart and no lock.
//!
//! [`SignedJwsValidator`]: crabka_security::SignedJwsValidator

use std::path::PathBuf;
use std::time::Duration;

use crabka_security::{Jwks, JwksHandle};
use tokio_util::sync::CancellationToken;

/// A JWKS fetch failure — surfaced for logging / tests; the refresher keeps the
/// previous key set on error.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FetchError {
    #[error("jwks http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("jwks document was not a valid key set")]
    Parse,
}

/// Fetch and parse a JWKS document from `endpoint` (HTTP or HTTPS). A 10s
/// timeout caps a hung identity provider; non-2xx responses are errors.
pub(crate) async fn fetch_jwks(
    client: &reqwest::Client,
    endpoint: &str,
) -> Result<Jwks, FetchError> {
    let body = client
        .get(endpoint)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    Jwks::from_json(&body).map_err(|_| FetchError::Parse)
}

/// Periodically refreshes a [`JwksHandle`] from a JWKS endpoint.
pub(crate) struct JwksRefresher {
    /// JWKS endpoint URL.
    pub endpoint: String,
    /// Shared key cell read by the validator; this task `store`s into it.
    pub handle: JwksHandle,
    /// Re-fetch cadence.
    pub interval: Duration,
    /// Cancels the task on broker shutdown.
    pub shutdown: CancellationToken,
    /// Slice 49c: optional PEM path; when `Some`, the rustls
    /// `ClientConfig` used by reqwest is built from this file and
    /// replaces the default webpki-roots trust store. When `None`,
    /// reqwest's webpki-roots default applies (slice 49b behavior).
    pub tls_trust: Option<PathBuf>,
}

impl JwksRefresher {
    /// Run until cancelled. The first fetch fires immediately (a
    /// `tokio::interval` ticks at t=0), so keys are available shortly after
    /// startup; a failed fetch logs a warning and leaves the previous key set
    /// in place — a transient identity-provider outage never crashes the broker.
    pub(crate) async fn run(self) {
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(10));
        if let Some(path) = &self.tls_trust {
            match crabka_security::build_client_config_from_pem(path) {
                Ok(cfg) => {
                    // reqwest's use_preconfigured_tls takes the rustls
                    // ClientConfig by value; clone the inner config (cheap
                    // — it's a small struct of Arc fields).
                    builder = builder.use_preconfigured_tls((*cfg).clone());
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        path = %path.display(),
                        "failed to load OAUTHBEARER JWKS TLS trust bundle; refresher will not start",
                    );
                    return;
                }
            }
        }
        let client = match builder.build() {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "failed to build JWKS HTTP client; OAUTHBEARER signed tokens will not validate");
                return;
            }
        };
        let mut tick = tokio::time::interval(self.interval);
        // Skip missed ticks rather than firing a burst after a slow fetch.
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    match fetch_jwks(&client, &self.endpoint).await {
                        Ok(jwks) => {
                            tracing::debug!(
                                endpoint = %self.endpoint,
                                keys = jwks.len(),
                                "refreshed OAUTHBEARER JWKS",
                            );
                            self.handle.store(jwks);
                        }
                        Err(e) => tracing::warn!(
                            endpoint = %self.endpoint,
                            error = %e,
                            "failed to refresh OAUTHBEARER JWKS; keeping previous key set",
                        ),
                    }
                }
                () = self.shutdown.cancelled() => return,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    /// Serve a fixed body at `/jwks` on an ephemeral port; returns the bound
    /// address and a shutdown token for the server task.
    async fn serve_jwks(body: &'static str) -> (SocketAddr, CancellationToken) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let app =
            axum::Router::new().route("/jwks", axum::routing::get(move || async move { body }));
        let srv_shutdown = shutdown.clone();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { srv_shutdown.cancelled().await })
                .await
                .unwrap();
        });
        (addr, shutdown)
    }

    const JWKS_BODY: &str = r#"{"keys":[{"kty":"EC","crv":"P-256","kid":"k1","x":"f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU","y":"x_FEzRu9m36HLN_tue659LNpXW6pCyStikYjKIWI5a0"}]}"#;

    #[tokio::test]
    async fn fetch_jwks_parses_served_keyset() {
        let (addr, shutdown) = serve_jwks(JWKS_BODY).await;
        let client = reqwest::Client::new();
        let jwks = fetch_jwks(&client, &format!("http://{addr}/jwks"))
            .await
            .unwrap();
        assert_eq!(jwks.len(), 1);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn fetch_jwks_errors_on_dead_endpoint() {
        // Nothing is listening on this port.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(500))
            .build()
            .unwrap();
        let err = fetch_jwks(&client, "http://127.0.0.1:1/jwks").await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn refresher_populates_handle_then_stops_on_shutdown() {
        let (addr, srv_shutdown) = serve_jwks(JWKS_BODY).await;
        let handle = JwksHandle::default();
        assert!(handle.load().is_empty());
        let shutdown = CancellationToken::new();
        let refresher = JwksRefresher {
            endpoint: format!("http://{addr}/jwks"),
            handle: handle.clone(),
            interval: Duration::from_millis(50),
            shutdown: shutdown.clone(),
            tls_trust: None,
        };
        let task = tokio::spawn(refresher.run());

        // Poll until the immediate first fetch lands.
        for _ in 0..100 {
            if !handle.load().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(handle.load().len(), 1);

        shutdown.cancel();
        task.await.unwrap();
        srv_shutdown.cancel();
    }

    /// Serve a fixed JSON body over TLS on an ephemeral port, using a
    /// freshly-generated self-signed cert with `127.0.0.1` as a SAN.
    /// Returns the bound address, a shutdown token, and the PEM path
    /// of the cert (suitable as a trust bundle for the client).
    async fn serve_jwks_https(
        body: &'static str,
    ) -> (std::net::SocketAddr, CancellationToken, std::path::PathBuf) {
        use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
        use std::sync::Arc;
        use tokio::io::AsyncWriteExt as _;
        use tokio_rustls::TlsAcceptor;

        // Install the rustls CryptoProvider once (idempotent — discards Err
        // on re-install). Required for rustls::ServerConfig::builder.
        let _ = rustls::crypto::ring::default_provider().install_default();

        // Generate a fresh self-signed cert with 127.0.0.1 as a SAN so the
        // client's hostname-verification accepts the loopback connection.
        let params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        let cert = params.self_signed(&key).unwrap();

        // Leak the tempdir for the test's lifetime so the PEM remains
        // readable when the refresher task fetches.
        let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        let cert_path = dir.path().join("cert.pem");
        std::fs::write(&cert_path, cert.pem()).unwrap();
        let key_path = dir.path().join("key.pem");
        std::fs::write(&key_path, key.serialize_pem()).unwrap();

        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert_path)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let priv_key = PrivateKeyDer::from_pem_file(&key_path).unwrap();
        let server_cfg = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, priv_key)
                .unwrap(),
        );
        let acceptor = TlsAcceptor::from(server_cfg);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let srv_shutdown = shutdown.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = srv_shutdown.cancelled() => break,
                    Ok((sock, _peer)) = listener.accept() => {
                        let acceptor = acceptor.clone();
                        tokio::spawn(async move {
                            use tokio::io::AsyncReadExt as _;
                            let Ok(mut tls) = acceptor.accept(sock).await else { return };
                            // Drain a minimal request line + headers (we
                            // don't parse — just ignore until empty line).
                            // Then write a fixed JSON reply.
                            let mut buf = [0u8; 1024];
                            let _ = tls.read(&mut buf).await;
                            let header = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                                body.len(),
                            );
                            let _ = tls.write_all(header.as_bytes()).await;
                            let _ = tls.write_all(body.as_bytes()).await;
                            let _ = tls.shutdown().await;
                        });
                    }
                }
            }
        });

        (addr, shutdown, cert_path)
    }

    #[tokio::test]
    async fn refresher_fetches_jwks_over_https_with_custom_trust() {
        let (addr, srv_shutdown, ca_path) = serve_jwks_https(JWKS_BODY).await;
        let handle = JwksHandle::default();
        let shutdown = CancellationToken::new();
        let refresher = JwksRefresher {
            endpoint: format!("https://127.0.0.1:{}/jwks", addr.port()),
            handle: handle.clone(),
            interval: Duration::from_millis(50),
            shutdown: shutdown.clone(),
            tls_trust: Some(ca_path),
        };
        let task = tokio::spawn(refresher.run());
        for _ in 0..100 {
            if !handle.load().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(handle.load().len(), 1);
        shutdown.cancel();
        task.await.unwrap();
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn refresher_https_fetch_fails_when_custom_trust_doesnt_match_server_cert() {
        // Server presents cert A; trust bundle is an unrelated cert B.
        // Handle stays empty because every refresh fails verification.
        let (addr, srv_shutdown, _server_cert_path) = serve_jwks_https(JWKS_BODY).await;

        let dir = tempfile::tempdir().unwrap();
        let params = rcgen::CertificateParams::new(vec!["unrelated.example".to_string()]).unwrap();
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        let cert = params.self_signed(&key).unwrap();
        let bogus_ca = dir.path().join("bogus-ca.pem");
        std::fs::write(&bogus_ca, cert.pem()).unwrap();

        let handle = JwksHandle::default();
        let shutdown = CancellationToken::new();
        let refresher = JwksRefresher {
            endpoint: format!("https://127.0.0.1:{}/jwks", addr.port()),
            handle: handle.clone(),
            interval: Duration::from_millis(50),
            shutdown: shutdown.clone(),
            tls_trust: Some(bogus_ca),
        };
        let task = tokio::spawn(refresher.run());
        // Give the refresher time for several ticks; each should fail.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            handle.load().is_empty(),
            "fetch should fail verification and leave handle empty",
        );
        shutdown.cancel();
        task.await.unwrap();
        srv_shutdown.cancel();
    }
}
