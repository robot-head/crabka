//! Listener serving: plaintext via a hyper auto h1/h2c accept loop, or rustls
//! via a manual accept loop that hands each `TlsStream` to hyper and injects
//! the mTLS peer principal (cert subject DN) into request extensions. TLS
//! material is hot-reloadable (`DynamicServerConfig`).

use std::{sync::Arc, time::Duration};

use axum::Router;
use crabka_security::{AuthMethod, DynamicServerConfig, Principal, TlsConfig};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

/// Serve `app` on `listener`. With `tls = Some(..)`, terminate rustls per
/// connection; otherwise serve plaintext. Returns when `shutdown` is cancelled.
///
/// # Errors
/// Propagates the `std::io` error from the accept loop.
pub async fn serve(
    listener: TcpListener,
    app: Router,
    tls: Option<Arc<DynamicServerConfig>>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    match tls {
        None => serve_plaintext(listener, app, shutdown).await,
        Some(dynamic) => serve_tls(listener, app, dynamic, shutdown).await,
    }
}

async fn serve_plaintext(
    listener: TcpListener,
    app: Router,
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
        let app = app.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(tcp);
            let svc = hyper::service::service_fn(
                move |mut req: hyper::Request<hyper::body::Incoming>| {
                    let app = app.clone();
                    async move {
                        req.extensions_mut().insert(peer);
                        app.oneshot(req).await
                    }
                },
            );
            // Plaintext Connect clients outside Rust use HTTP/2 prior knowledge
            // (h2c) for bidi streams; keep HTTP/1.1 on the same port for health
            // checks and existing clients.
            if let Err(e) = auto::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await
            {
                tracing::debug!(error = %e, "plaintext connection error");
            }
        });
    }
    Ok(())
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

/// Extract the mTLS peer principal (cert subject DN) after a handshake.
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

/// Build the hot-reloadable server config + spawn the reload watcher. Returns
/// the dynamic config to pass to [`serve`].
///
/// # Errors
/// Propagates `crabka_security::TlsError` if the initial config fails to build.
pub fn build_and_watch_tls(
    cfg: TlsConfig,
    reload_interval_secs: u64,
    shutdown: CancellationToken,
) -> Result<Arc<DynamicServerConfig>, crabka_security::TlsError> {
    let dynamic = DynamicServerConfig::from_tls_config(&cfg)?;
    let watch = dynamic.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(reload_interval_secs.max(1)));
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
