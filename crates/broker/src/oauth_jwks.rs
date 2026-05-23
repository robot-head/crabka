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
}

impl JwksRefresher {
    /// Run until cancelled. The first fetch fires immediately (a
    /// `tokio::interval` ticks at t=0), so keys are available shortly after
    /// startup; a failed fetch logs a warning and leaves the previous key set
    /// in place — a transient identity-provider outage never crashes the broker.
    pub(crate) async fn run(self) {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
        {
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
}
