//! Background JWKS refresher for SASL/OAUTHBEARER signed-token validation.
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
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use crabka_security::{Jwks, JwksHandle};
use qubit_clock::sleep::AsyncSleeper;
use tokio::sync::mpsc;
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
/// `ignore_key_use` threads through to the JWKS parser — when
/// false, `use=enc` keys are filtered out.
pub(crate) async fn fetch_jwks(
    client: &reqwest::Client,
    endpoint: &str,
    ignore_key_use: bool,
) -> Result<Jwks, FetchError> {
    let body = client
        .get(endpoint)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    Jwks::from_json(&body, ignore_key_use).map_err(|_| FetchError::Parse)
}

/// Periodically refreshes a [`JwksHandle`] from a JWKS endpoint.
///
/// The loop additionally serves on-demand refresh requests
/// triggered by validators that encountered an unknown-kid or
/// bad-signature token (received via [`signal_rx`](Self::signal_rx)).
/// On-demand refreshes are rate-limited by
/// [`min_on_demand_pause`](Self::min_on_demand_pause) so a verify-fail
/// storm can't hammer the `IdP`. Successful refreshes update
/// [`last_successful_fetch_ms`](Self::last_successful_fetch_ms) — the
/// validator reads that to enforce hard cache expiry.
pub(crate) struct JwksRefresher {
    /// JWKS endpoint URL.
    pub endpoint: String,
    /// Shared key cell read by the validator; this task `store`s into it.
    pub handle: JwksHandle,
    /// Re-fetch cadence (periodic).
    pub interval: Duration,
    /// Cancels the task on broker shutdown.
    pub shutdown: CancellationToken,
    /// Optional PEM path; when
    /// `Some`, the rustls `ClientConfig` used by reqwest is built from
    /// this file and replaces the default webpki-roots trust store. When
    /// `None`, reqwest's webpki-roots default applies. The bundle is
    /// shared with the introspection client when
    /// configured — the operator's `tlsTrustedCertificates` flows
    /// through `idp_tls_trust` and is used for JWKS, introspection, and
    /// userinfo HTTPS.
    pub tls_trust: Option<PathBuf>,
    /// Receives signals from validators on verify-failure to
    /// trigger an on-demand refresh (subject to `min_on_demand_pause`).
    /// Capacity 1 + `try_send` on the producer side ⇒ signals coalesce.
    pub signal_rx: mpsc::Receiver<()>,
    /// Minimum pause between on-demand refreshes. Strimzi
    /// default 1 second. Periodic refresh (`interval`) is unaffected.
    pub min_on_demand_pause: Duration,
    /// Shared timestamp counter. Refresher updates after each
    /// successful fetch; validators read for cache-expiry check. Shared
    /// (`Arc<AtomicI64>`) with the paired `JwksHandle`.
    pub last_successful_fetch_ms: Arc<AtomicI64>,
    /// Tracks the last on-demand-refresh epoch ms for
    /// rate-limiting. Independent of periodic refresh.
    pub last_on_demand_refresh_ms: Arc<AtomicI64>,
    /// When true, accept JWKS keys regardless of `use` field.
    /// Default false (filter out `use=enc`). Threads to
    /// [`Jwks::from_json`].
    pub ignore_key_use: bool,
    /// Relative sleeper driving the periodic refresh cadence. Production uses
    /// [`qubit_clock::sleep::SystemSleeper`] (real time); tests inject a
    /// [`qubit_clock::sleep::MockSleeper`] so the refresh interval fires on a
    /// controlled mock timeline instead of wall-clock time.
    pub sleeper: Arc<dyn AsyncSleeper>,
}

impl JwksRefresher {
    /// Run until cancelled. The first periodic fetch fires immediately (a
    /// zero-duration first sleep on the injected [`AsyncSleeper`] reproduces
    /// `tokio::time::interval`'s t=0 tick), so keys are available shortly after
    /// startup; a failed fetch logs a warning and leaves the previous key set
    /// in place — a transient identity-provider outage never crashes the broker.
    /// On-demand refresh signals from validators race with the
    /// periodic tick in the same `select!`; the on-demand arm consults
    /// `last_on_demand_refresh_ms` against `min_on_demand_pause` and drops
    /// the signal silently when within the window.
    pub(crate) async fn run(mut self) {
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
        // Drive the periodic refresh cadence through the injected `AsyncSleeper`
        // (production: real time; tests: a controlled mock timeline). A
        // zero-duration first sleep reproduces `tokio::time::interval`'s t=0
        // tick, so the first fetch fires immediately and keys are available
        // shortly after startup. Each subsequent sleep is re-armed to
        // `self.interval` only after the fetch completes, so a slow fetch never
        // triggers a catch-up burst — this preserves the never-burst intent of
        // the original `MissedTickBehavior::Skip` (re-arm-after-work aligns the
        // next tick to `Delay` rather than `Skip`, but neither ever bursts, and
        // a JWKS fetch is far shorter than the multi-minute refresh interval).
        // The sleeper is cloned into a local so the tick future borrows it
        // rather than `self`, leaving `self` free for the arms below.
        let sleeper = self.sleeper.clone();
        let mut tick = sleeper.sleep_for_async(Duration::ZERO);
        loop {
            tokio::select! {
                () = &mut tick => {
                    self.refresh_and_swap(&client).await;
                    tick = sleeper.sleep_for_async(self.interval);
                }
                // On-demand refresh triggered by validator
                // signal. Subject to `min_on_demand_pause` rate-limit.
                // Signals coalesce via mpsc capacity 1 + `try_send`.
                Some(()) = self.signal_rx.recv() => {
                    let now_ms = current_epoch_ms();
                    let last = self.last_on_demand_refresh_ms.load(Ordering::Relaxed);
                    let elapsed_ms = now_ms.saturating_sub(last);
                    let pause_ms = i64::try_from(self.min_on_demand_pause.as_millis())
                        .unwrap_or(i64::MAX);
                    if elapsed_ms >= pause_ms {
                        self.last_on_demand_refresh_ms.store(now_ms, Ordering::Relaxed);
                        tracing::debug!(
                            endpoint = %self.endpoint,
                            elapsed_ms,
                            "on-demand JWKS refresh triggered by validator signal",
                        );
                        self.refresh_and_swap(&client).await;
                    } else {
                        tracing::debug!(
                            endpoint = %self.endpoint,
                            elapsed_ms,
                            pause_ms,
                            "on-demand JWKS refresh rate-limited; signal dropped",
                        );
                    }
                }
                () = self.shutdown.cancelled() => return,
            }
        }
    }

    /// Extracted from the loop so the periodic + on-demand arms
    /// both call it. Updates `last_successful_fetch_ms` only on success
    /// (failure leaves the timestamp untouched so the cache ages toward
    /// expiry and validators eventually start failing closed).
    async fn refresh_and_swap(&self, client: &reqwest::Client) {
        match fetch_jwks(client, &self.endpoint, self.ignore_key_use).await {
            Ok(jwks) => {
                tracing::debug!(
                    endpoint = %self.endpoint,
                    keys = jwks.len(),
                    "refreshed OAUTHBEARER JWKS",
                );
                self.handle.store(jwks);
                self.last_successful_fetch_ms
                    .store(current_epoch_ms(), Ordering::Relaxed);
            }
            Err(e) => tracing::warn!(
                endpoint = %self.endpoint,
                error = %e,
                "failed to refresh OAUTHBEARER JWKS; keeping previous key set",
            ),
        }
    }
}

/// Current Unix epoch in milliseconds, saturating on overflow
/// or pre-epoch clock skew. Used by the refresher to populate the shared
/// timestamp counter read by validators for cache-expiry checks.
fn current_epoch_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::{assert, check};
    use qubit_clock::MockWaiterKind;
    use qubit_clock::sleep::{MockSleeper, SystemSleeper};
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

    /// Pick the on-demand-refresh-relevant slots out of a
    /// `JwksRefresher` so the simple refresher tests can stay
    /// terse. `signal_rx` is given but never sent on in these tests;
    /// `min_on_demand_pause` is irrelevant; the timestamps are isolated
    /// per test.
    fn test_refresher(
        endpoint: String,
        handle: JwksHandle,
        interval: Duration,
        shutdown: CancellationToken,
        tls_trust: Option<PathBuf>,
        sleeper: Arc<dyn AsyncSleeper>,
    ) -> JwksRefresher {
        let (_tx, rx) = mpsc::channel::<()>(1);
        JwksRefresher {
            endpoint,
            handle,
            interval,
            shutdown,
            tls_trust,
            signal_rx: rx,
            min_on_demand_pause: Duration::from_secs(1),
            last_successful_fetch_ms: Arc::new(AtomicI64::new(0)),
            last_on_demand_refresh_ms: Arc::new(AtomicI64::new(0)),
            ignore_key_use: false,
            sleeper,
        }
    }

    #[tokio::test]
    async fn fetch_jwks_parses_served_keyset() {
        let (addr, shutdown) = serve_jwks(JWKS_BODY).await;
        let client = reqwest::Client::new();
        let jwks = fetch_jwks(&client, &format!("http://{addr}/jwks"), false)
            .await
            .unwrap();
        assert!(jwks.len() == 1);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn fetch_jwks_errors_on_dead_endpoint() {
        // Nothing is listening on this port.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(500))
            .build()
            .unwrap();
        let err = fetch_jwks(&client, "http://127.0.0.1:1/jwks", false).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn refresher_populates_handle_then_stops_on_shutdown() {
        let (addr, srv_shutdown) = serve_jwks(JWKS_BODY).await;
        let handle = JwksHandle::default();
        assert!(handle.load().is_empty());
        let shutdown = CancellationToken::new();
        let refresher = test_refresher(
            format!("http://{addr}/jwks"),
            handle.clone(),
            Duration::from_millis(50),
            shutdown.clone(),
            None,
            Arc::new(SystemSleeper::new()),
        );
        let task = tokio::spawn(refresher.run());

        // Poll until the immediate first fetch lands.
        for _ in 0..100 {
            if !handle.load().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(handle.load().len() == 1);

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
        let refresher = test_refresher(
            format!("https://127.0.0.1:{}/jwks", addr.port()),
            handle.clone(),
            Duration::from_millis(50),
            shutdown.clone(),
            Some(ca_path),
            Arc::new(SystemSleeper::new()),
        );
        let task = tokio::spawn(refresher.run());
        for _ in 0..100 {
            if !handle.load().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(handle.load().len() == 1);
        shutdown.cancel();
        task.await.unwrap();
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn refresher_https_fetch_fails_when_custom_trust_doesnt_match_server_cert() {
        // Server presents cert A; trust bundle is an unrelated cert B. Every
        // refresh fails TLS verification, so the handle must never populate.
        //
        // Driven on a mock timeline instead of a wall-clock sleep: the first
        // fetch fires immediately (t=0 tick), then the loop parks on the
        // refresh-interval sleep. Advancing the timeline fires each subsequent
        // fetch deterministically — exact and instant, with no flaky timing.
        let (addr, srv_shutdown, _server_cert_path) = serve_jwks_https(JWKS_BODY).await;

        let dir = tempfile::tempdir().unwrap();
        let params = rcgen::CertificateParams::new(vec!["unrelated.example".to_string()]).unwrap();
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        let cert = params.self_signed(&key).unwrap();
        let bogus_ca = dir.path().join("bogus-ca.pem");
        std::fs::write(&bogus_ca, cert.pem()).unwrap();

        let handle = JwksHandle::default();
        let shutdown = CancellationToken::new();
        let interval = Duration::from_millis(50);
        let sleeper = MockSleeper::new();
        let timeline = sleeper.timeline();
        let refresher = test_refresher(
            format!("https://127.0.0.1:{}/jwks", addr.port()),
            handle.clone(),
            interval,
            shutdown.clone(),
            Some(bogus_ca),
            Arc::new(sleeper),
        );
        let task = tokio::spawn(refresher.run());

        // Drive several refresh intervals. Before each advance, block (bounded
        // real time, hang-guard only) until the loop has parked on the interval
        // sleep — this confirms the prior fetch attempt completed — then assert
        // the failing fetch left the handle empty. `wait_for_blocked_waiters`
        // runs on a blocking thread so it never stalls the current-thread
        // runtime that must drive the refresher's HTTPS attempt.
        for _ in 0..3 {
            let tl = timeline.clone();
            let parked = tokio::task::spawn_blocking(move || {
                tl.wait_for_blocked_waiters(MockWaiterKind::Sleep, 1, Duration::from_secs(5))
            })
            .await
            .unwrap();
            assert!(
                parked,
                "refresher should park on the interval sleep between fetches",
            );
            assert!(
                handle.load().is_empty(),
                "fetch should fail verification and leave handle empty",
            );
            timeline.advance(interval);
        }

        shutdown.cancel();
        task.await.unwrap();
        srv_shutdown.cancel();
    }

    // ---- On-demand refresh + cache-expiry timestamp -------------

    /// Serve a fixed body but track how many HTTP requests have hit the
    /// `/jwks` route. Returns a shared `AtomicUsize` so the test can assert
    /// on call count after the refresher has been driven.
    async fn serve_jwks_counting(
        body: &'static str,
    ) -> (
        SocketAddr,
        CancellationToken,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter_cl = counter.clone();
        let app = axum::Router::new().route(
            "/jwks",
            axum::routing::get(move || {
                let c = counter_cl.clone();
                async move {
                    c.fetch_add(1, Ordering::Relaxed);
                    body
                }
            }),
        );
        let srv_shutdown = shutdown.clone();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { srv_shutdown.cancelled().await })
                .await
                .unwrap();
        });
        (addr, shutdown, counter)
    }

    /// Builds a refresher with a 1-hour periodic interval (so only on-demand
    /// signals matter for the test) and returns the shared signal sender +
    /// the rate-limit timestamp + the success-timestamp.
    #[allow(clippy::type_complexity)]
    fn make_signal_refresher(
        endpoint: String,
        min_on_demand_pause: Duration,
    ) -> (
        JwksRefresher,
        mpsc::Sender<()>,
        Arc<AtomicI64>, // last_successful_fetch_ms
        Arc<AtomicI64>, // last_on_demand_refresh_ms
        CancellationToken,
        JwksHandle,
    ) {
        let (signal_tx, signal_rx) = mpsc::channel::<()>(1);
        let shutdown = CancellationToken::new();
        let last_successful = Arc::new(AtomicI64::new(0));
        let last_on_demand = Arc::new(AtomicI64::new(0));
        let handle = JwksHandle::new_with_refresher_handles(
            Jwks::empty(),
            last_successful.clone(),
            signal_tx.clone(),
        );
        let refresher = JwksRefresher {
            endpoint,
            handle: handle.clone(),
            interval: Duration::from_hours(1), // disable periodic for these tests
            shutdown: shutdown.clone(),
            tls_trust: None,
            signal_rx,
            min_on_demand_pause,
            last_successful_fetch_ms: last_successful.clone(),
            last_on_demand_refresh_ms: last_on_demand.clone(),
            ignore_key_use: false,
            sleeper: Arc::new(SystemSleeper::new()),
        };
        (
            refresher,
            signal_tx,
            last_successful,
            last_on_demand,
            shutdown,
            handle,
        )
    }

    #[tokio::test]
    async fn refresher_signal_triggers_on_demand_refresh_when_pause_elapsed() {
        let (addr, srv_shutdown, count) = serve_jwks_counting(JWKS_BODY).await;
        let endpoint = format!("http://{addr}/jwks");
        let (refresher, signal_tx, _last_successful, last_on_demand, shutdown, handle) =
            make_signal_refresher(endpoint, Duration::from_millis(0));
        let task = tokio::spawn(refresher.run());

        // Drive at least one signal refresh.
        signal_tx.send(()).await.unwrap();
        // Poll until the on-demand timestamp moves or until the handle has been
        // populated by the on-demand fetch.
        for _ in 0..100 {
            if last_on_demand.load(Ordering::Relaxed) > 0 && !handle.load().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        check!(
            last_on_demand.load(Ordering::Relaxed) > 0,
            "on-demand timestamp should have advanced past sentinel 0",
        );
        check!(
            count.load(Ordering::Relaxed) >= 1,
            "server should have served the on-demand request"
        );
        check!(
            handle.load().len() == 1,
            "refresher must store the fetched key set"
        );

        shutdown.cancel();
        let _ = task.await;
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn refresher_signal_dropped_when_within_min_pause_window() {
        let (addr, srv_shutdown, count) = serve_jwks_counting(JWKS_BODY).await;
        let endpoint = format!("http://{addr}/jwks");
        // 60s pause — second signal MUST be rate-limited.
        let (refresher, signal_tx, _last_successful, last_on_demand, shutdown, _handle) =
            make_signal_refresher(endpoint, Duration::from_mins(1));
        let task = tokio::spawn(refresher.run());

        // First signal: fires. Wait on the HTTP counter (the strict
        // happens-after of refresh_and_swap) rather than the timestamp
        // store, which the select! arm performs BEFORE the HTTP call —
        // otherwise CI races between timestamp-set and request-arrival.
        signal_tx.send(()).await.unwrap();
        for _ in 0..100 {
            if count.load(Ordering::Relaxed) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let first_ts = last_on_demand.load(Ordering::Relaxed);
        assert!(first_ts > 0, "first signal must have fired a refresh");
        let count_after_first = count.load(Ordering::Relaxed);
        assert!(count_after_first >= 1);

        // Second signal within the 60s pause: dropped.
        signal_tx.send(()).await.unwrap();
        // Yield the runtime a few times to let the select! arm process.
        for _ in 0..10 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            last_on_demand.load(Ordering::Relaxed) == first_ts,
            "second signal within min_pause must not advance timestamp"
        );
        assert!(
            count.load(Ordering::Relaxed) == count_after_first,
            "server must not see a second on-demand HTTP request"
        );

        shutdown.cancel();
        let _ = task.await;
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn refresher_successful_refresh_updates_last_successful_fetch_timestamp() {
        let (addr, srv_shutdown, _count) = serve_jwks_counting(JWKS_BODY).await;
        let endpoint = format!("http://{addr}/jwks");
        let (refresher, signal_tx, last_successful, _last_on_demand, shutdown, _handle) =
            make_signal_refresher(endpoint, Duration::from_millis(0));
        let task = tokio::spawn(refresher.run());

        assert!(last_successful.load(Ordering::Relaxed) == 0);
        signal_tx.send(()).await.unwrap();
        for _ in 0..100 {
            if last_successful.load(Ordering::Relaxed) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            last_successful.load(Ordering::Relaxed) > 0,
            "last_successful_fetch_ms must advance after a successful fetch",
        );

        shutdown.cancel();
        let _ = task.await;
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn refresher_failed_refresh_does_not_advance_last_successful_fetch() {
        // Endpoint that always returns 500 ⇒ fetch fails, timestamp must stay
        // at the sentinel 0.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv_shutdown = CancellationToken::new();
        let srv_token = srv_shutdown.clone();
        let app = axum::Router::new().route(
            "/jwks",
            axum::routing::get(|| async {
                (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "boom")
            }),
        );
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { srv_token.cancelled().await })
                .await
                .unwrap();
        });

        let endpoint = format!("http://{addr}/jwks");
        let (refresher, signal_tx, last_successful, last_on_demand, shutdown, _handle) =
            make_signal_refresher(endpoint, Duration::from_millis(0));
        let task = tokio::spawn(refresher.run());

        signal_tx.send(()).await.unwrap();
        // Wait long enough for the refresh attempt to complete & log.
        for _ in 0..50 {
            if last_on_demand.load(Ordering::Relaxed) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // On-demand timestamp advances regardless (rate-limit accounting);
        // success timestamp must stay at 0.
        assert!(
            last_on_demand.load(Ordering::Relaxed) > 0,
            "on-demand rate-limit timestamp updates even when the fetch itself fails",
        );
        assert!(
            last_successful.load(Ordering::Relaxed) == 0,
            "failed fetch must leave last_successful_fetch_ms at sentinel 0"
        );

        shutdown.cancel();
        let _ = task.await;
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn refresher_passes_ignore_key_use_through_to_jwks_parser() {
        // JWKS body has an RSA key marked `use=enc`. With ignore_key_use=true
        // the refresher should still install it; with false it would be filtered
        // out (yielding an empty key set).
        const ENC_KEY_BODY: &str =
            r#"{"keys":[{"kty":"RSA","kid":"enc-kid","use":"enc","n":"AQAB","e":"AQAB"}]}"#;
        let (addr, srv_shutdown, _count) = serve_jwks_counting(ENC_KEY_BODY).await;
        let endpoint = format!("http://{addr}/jwks");
        let (mut refresher, signal_tx, _last_successful, _last_on_demand, shutdown, handle) =
            make_signal_refresher(endpoint, Duration::from_millis(0));
        refresher.ignore_key_use = true;
        let task = tokio::spawn(refresher.run());

        signal_tx.send(()).await.unwrap();
        for _ in 0..100 {
            if !handle.load().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            handle.load().len() == 1,
            "ignore_key_use=true must keep the use=enc key in the installed set"
        );

        shutdown.cancel();
        let _ = task.await;
        srv_shutdown.cancel();
    }
}
