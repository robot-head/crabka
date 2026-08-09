//! Listener serving.
//!
//! The plaintext path runs through `axum::serve`. The rustls path runs through
//! a manual accept loop that hands each `TlsStream` to hyper and injects the
//! mTLS peer principal, the cert subject DN, into the request extensions. The
//! TLS material is hot-reloadable through `DynamicServerConfig`.

use std::{sync::Arc, time::Duration};

use axum::Router;
use crabka_security::{AuthMethod, DynamicServerConfig, Principal, TlsConfig};
use crabka_units::prelude::*;
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

/// Serve `app` on `listener`.
///
/// With `tls = Some(..)`, this function terminates rustls for each connection.
/// Otherwise it serves plaintext. It returns when `shutdown` is cancelled.
///
/// # Errors
/// Propagates the `std::io` error from `axum::serve` on the plaintext path, or
/// from the accept loop on the TLS path.
pub async fn serve(
    listener: TcpListener,
    app: Router,
    tls: Option<Arc<DynamicServerConfig>>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    match tls {
        None => {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { shutdown.cancelled().await })
                .await
        }
        Some(dynamic) => serve_tls(listener, app, dynamic, shutdown).await,
    }
}

async fn serve_tls(
    listener: TcpListener,
    app: Router,
    dynamic: Arc<DynamicServerConfig>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    loop {
        let (tcp, peer) = tokio::select! {
            () = shutdown.cancelled() => break,
            res = listener.accept() => match res {
                Ok(v) => v,
                Err(e) => { tracing::warn!(error = %e, "tcp accept failed"); continue; }
            },
        };
        let acceptor = tokio_rustls::TlsAcceptor::from(dynamic.current());
        let app = app.clone();
        tokio::spawn(async move {
            let tls = match acceptor.accept(tcp).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::debug!(error = %e, %peer, "tls handshake failed");
                    return;
                }
            };
            let principal = peer_principal(&tls);
            let io = TokioIo::new(tls);
            let svc = hyper::service::service_fn(
                move |mut req: hyper::Request<hyper::body::Incoming>| {
                    let app = app.clone();
                    let principal = principal.clone();
                    async move {
                        // Always inject the peer address so authz / audit handlers
                        // can do host-based ACL matching.  `peer_or_default` in
                        // `authz::auth_layer` returns `0.0.0.0:0` for plaintext
                        // connections that don't go through this TLS path.
                        req.extensions_mut().insert(peer);
                        if let Some(p) = principal {
                            req.extensions_mut().insert(p);
                        }
                        app.oneshot(req).await
                    }
                },
            );
            // HTTP/1.1 only — matches the gateway's Connect-over-h1 design (axum
            // is built with the `http1` feature; the plaintext `axum::serve`
            // path is h1 too). Connect unary + streaming work over h1; a future
            // h2/gRPC-over-TLS client would need `auto::Builder` + ALPN here.
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await
            {
                tracing::debug!(error = %e, "tls connection error");
            }
        });
    }
    Ok(())
}

/// Extract the mTLS peer principal, the cert subject DN, after a handshake.
fn peer_principal(tls: &tokio_rustls::server::TlsStream<TcpStream>) -> Option<Principal> {
    let (_, conn) = tls.get_ref();
    let cert = conn.peer_certificates()?.first()?;
    let name = crabka_security::extract_principal_from_cert(cert.as_ref())?;
    Some(Principal {
        name,
        auth_method: AuthMethod::MTls,
        groups: vec![],
    })
}

/// Build the hot-reloadable server config and spawn the reload watcher. Returns
/// the dynamic config to pass to [`serve`].
///
/// # Errors
/// Propagates `crabka_security::TlsError` if the initial config fails to build.
///
/// # Panics
///
/// Panics when `reload_interval` is not positive. Process configuration
/// validates this invariant before building the listener.
pub fn build_and_watch_tls(
    cfg: TlsConfig,
    reload_interval: Time,
    shutdown: CancellationToken,
) -> Result<Arc<DynamicServerConfig>, crabka_security::TlsError> {
    let tick = reload_tick(reload_interval);
    let dynamic = DynamicServerConfig::from_tls_config(&cfg)?;
    let watch = dynamic.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tick);
        ticker.tick().await; // skip the immediate first tick (already loaded)
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                () = shutdown.cancelled() => return,
            }
            if let Err(e) = watch.reload_from(&cfg) {
                tracing::warn!(error = %e, "tls reload failed; keeping prior config");
            }
        }
    });
    Ok(dynamic)
}

/// The `tokio` tick for a validated reload interval. `tokio::time::interval`
/// panics on a zero period, so this function checks the extent first.
fn reload_tick(reload_interval: Time) -> Duration {
    assert2::assert!(reload_interval > secs(0));
    reload_interval.to_std()
}

#[cfg(test)]
mod policy_tests {
    use assert2::{assert, check};
    use crabka_units::prelude::*;

    use super::reload_tick;

    #[test]
    fn reload_tick_rejects_a_non_positive_interval() {
        for interval in [secs(0), Time::from_secs(-1)] {
            assert!(std::panic::catch_unwind(|| reload_tick(interval)).is_err());
        }
    }

    #[test]
    fn reload_tick_hands_tokio_the_configured_extent() {
        check!(reload_tick(secs(30)) == std::time::Duration::from_secs(30));
        check!(reload_tick(millis(250)) == std::time::Duration::from_millis(250));
    }
}
