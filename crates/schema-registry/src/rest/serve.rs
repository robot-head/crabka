//! Serve the registry router over plain HTTP, or HTTPS (with optional mTLS
//! client-cert → `Principal`). Models `grpc-gateway/src/serve.rs`.
//!
//! The plaintext path ([`serve_http`]) is byte-for-byte the pre-security
//! `axum::serve` call, so existing tests and the no-security default are
//! unaffected. The TLS path ([`serve_https`]) terminates rustls per connection
//! in a manual accept loop and, after the handshake, extracts the peer
//! certificate's subject DN into a [`crate::auth::MtlsPrincipal`] that it injects
//! into every request's extensions for that connection. The registry's
//! `auth_layer` consumes that as the highest-precedence credential; the peer
//! `SocketAddr` is injected too, for host-scoped ACLs in `authz_layer`.

use std::sync::Arc;

use axum::Router;
use crabka_security::{AuthMethod, Principal, TlsConfig};
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use crate::auth::MtlsPrincipal;

/// Serve `app` on `listener` over plaintext HTTP. Returns when `shutdown` is
/// cancelled. Identical to the pre-security `axum::serve` call.
///
/// # Errors
/// Propagates the `std::io` error from `axum::serve`.
pub async fn serve_http(
    listener: TcpListener,
    app: Router,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
}

/// Serve `app` on `listener` over HTTPS, terminating rustls per connection.
///
/// After each handshake the peer certificate's subject DN (when present) is
/// turned into an mTLS [`Principal`] and injected — wrapped in
/// [`MtlsPrincipal`] — into every request's extensions for that connection,
/// alongside the peer [`std::net::SocketAddr`]. Returns when `shutdown` is
/// cancelled.
///
/// # Errors
/// Propagates [`crabka_security::TlsError`] if the server config fails to build,
/// or the `std::io` error from binding the accept loop.
pub async fn serve_https(
    listener: TcpListener,
    app: Router,
    tls: &TlsConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let server_config = tls.build_server_config()?;
    serve_tls(listener, app, server_config, shutdown).await?;
    Ok(())
}

async fn serve_tls(
    listener: TcpListener,
    app: Router,
    server_config: Arc<rustls::ServerConfig>,
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
        let acceptor = tokio_rustls::TlsAcceptor::from(server_config.clone());
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
                        // Inject the peer address so `authz_layer` can do
                        // host-based ACL matching; it falls back to `0.0.0.0:0`
                        // for plaintext connections that never reach this path.
                        req.extensions_mut().insert(peer);
                        if let Some(p) = principal {
                            // `auth_layer` reads `MtlsPrincipal` as the
                            // highest-precedence credential.
                            req.extensions_mut().insert(MtlsPrincipal(p));
                        }
                        app.oneshot(req).await
                    }
                },
            );
            // HTTP/1.1 only — matches the registry's plaintext `axum::serve`
            // path (axum is built with the `http1` feature) and the gateway's
            // Connect-over-h1 design.
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
