//! Per-connection request loop. Reads a frame, parses the request
//! header, looks up the handler, awaits the response, encodes the
//! response header in front of the handler's bytes, and writes the
//! result back to the client.
//!
//! Header rules (verified against Apache Kafka 4.x):
//! - Request header is v2 when the body is flexible (KIP-482), v1 otherwise.
//!   Note: `client_id` is `NULLABLE_STRING` (i16 length) in BOTH header
//!   versions — see `RequestHeader.json` schema (`flexibleVersions: none`
//!   on the field).
//! - Response header is v1 (i.e. a trailing tagged-fields byte) iff the
//!   *body* is flexible — EXCEPT for `ApiVersions` (`api_key=18`), whose
//!   response header is always v0.

use std::net::SocketAddr;

use bytes::{BufMut, Bytes, BytesMut};
use crabka_protocol::{Decode as _, api_key::ApiKey};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::Instrument as _;

use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::{ApiKeyCode, ApiVersion, CorrelationId},
    network::codec,
};

/// `ApiVersions` wire `api_key`. Named separately because it is the one API
/// whose response header is always v0 regardless of body flexibility, and
/// whose v3+ request carries the KIP-511 client software name/version.
const API_VERSIONS_KEY: ApiKeyCode = ApiKey::ApiVersions as i16;

/// `SaslHandshake` wire `api_key` — handled inline (before the handler table)
/// because it mutates the per-connection auth state.
const SASL_HANDSHAKE_KEY: ApiKeyCode = ApiKey::SaslHandshake as i16;

/// `SaslAuthenticate` wire `api_key` — handled inline (before the handler
/// table) because it mutates the per-connection auth state.
const SASL_AUTHENTICATE_KEY: ApiKeyCode = ApiKey::SaslAuthenticate as i16;

/// Process-lifetime ANONYMOUS principal. `RequestContext` only borrows
/// `&Principal`, so the defensive fallback (SASL pre-auth, where
/// `auth.principal()` is `None`) can hand out a `&'static Principal` instead of
/// allocating a fresh `String`/`Vec` per Produce/Fetch. `Principal` carries a
/// `Vec<String>` so it can't be a `const`; `LazyLock` builds it once on first
/// use.
static ANONYMOUS_PRINCIPAL: std::sync::LazyLock<crabka_security::Principal> =
    std::sync::LazyLock::new(|| crabka_security::Principal {
        name: "ANONYMOUS".to_string(),
        auth_method: crabka_security::AuthMethod::Anonymous,
        groups: vec![],
    });

/// Borrow the connection's authenticated principal, falling back to the shared
/// process-lifetime ANONYMOUS singleton when the connection has no principal
/// yet (defensive SASL pre-auth case). Avoids a per-request `Principal` clone.
fn principal_or_anonymous(
    auth: &crate::network::auth::ConnectionAuth,
) -> &crabka_security::Principal {
    auth.principal().unwrap_or(&ANONYMOUS_PRINCIPAL)
}

/// Returns a future that resolves at `deadline` if `Some`, or never resolves
/// if `None`. Used in `tokio::select!` to disarm the timer arm for non-OAuth
/// connections (which have no session expiry).
async fn sleep_until_some(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending::<()>().await,
    }
}

/// Convert an "expires-at as Unix epoch ms" into a `tokio::time::Instant`
/// suitable for `sleep_until`. Computes the delta against the current wall
/// clock and adds to `Instant::now()`; tests using `tokio::time::pause` can
/// then advance the tokio clock to fire the deadline deterministically.
fn instant_at_epoch_ms(epoch_ms: i64) -> tokio::time::Instant {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_i64, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
    // `.max(0)` ensures delta is non-negative before the unsigned cast;
    // tokens with past `exp` fire the timer on the very next poll.
    let delta_ms = (epoch_ms - now_ms).max(0);
    tokio::time::Instant::now() + std::time::Duration::from_millis(delta_ms.cast_unsigned())
}

/// Returns the principal name for an `Authenticated` connection, or for the
/// `previous` snapshot of a `Reauthenticating` connection; `None` otherwise.
/// Used by the per-connection re-auth timer's tracing log on expiry.
fn auth_principal_name(auth: &crate::network::auth::ConnectionAuth) -> Option<&str> {
    match auth {
        crate::network::auth::ConnectionAuth::Authenticated { principal, .. } => {
            Some(principal.name.as_str())
        }
        crate::network::auth::ConnectionAuth::Reauthenticating { previous, .. } => {
            Some(previous.principal.name.as_str())
        }
        _ => None,
    }
}

/// Per-listener entrypoint. Branches between TLS termination (when the
/// listener's protocol requires TLS) and the plaintext path. Both paths
/// converge on [`serve_connection_stream`] for the per-connection request
/// loop.
pub async fn serve_connection_on_listener(
    broker: std::sync::Arc<Broker>,
    stream: TcpStream,
    spec: crate::config::ListenerSpec,
) {
    // Capture the peer address from the underlying TCP socket before we
    // hand the stream off to the TLS layer / framing loop. ACL
    // handlers need this for host-based ACL matching. If `peer_addr`
    // fails (rare — socket closed mid-accept), fall back to the
    // unspecified address; ACL matchers treat it as a non-matching host.
    let peer = stream.peer_addr().unwrap_or_else(|e| {
        tracing::debug!(error = %e, "peer_addr() failed, using 0.0.0.0:0");
        SocketAddr::from(([0u8, 0, 0, 0], 0))
    });
    if spec.protocol.requires_tls() {
        let acceptor = if let Some(per_tls) = spec.tls_config.as_ref() {
            match per_tls.build_server_config() {
                Ok(sc) => tokio_rustls::TlsAcceptor::from(sc),
                Err(e) => {
                    tracing::error!(
                        listener = %spec.name,
                        error = %e,
                        "failed to build TlsAcceptor from per-listener tls_config"
                    );
                    return;
                }
            }
        } else {
            // Use DynamicServerConfig so hot-reload keeps working.
            let Some(dynamic) = broker.tls_dynamic.as_ref() else {
                tracing::error!(
                    listener = %spec.name,
                    "TLS listener without per-listener tls_config and no broker-wide tls_dynamic"
                );
                return;
            };
            // Snapshot per accept; an in-flight handshake keeps its captured config.
            tokio_rustls::TlsAcceptor::from(dynamic.current())
        };
        // Linux kTLS (Increment F): when the startup probe confirmed kTLS
        // support, terminate TLS through a `CorkStream` so `ktls` can cleanly
        // drain the rustls buffer, then hand the socket to the kernel via
        // `config_ktls_server`. The resulting `KtlsStream` is `SendfileSink`-
        // capable, so the Fetch path emits file regions and `sendfile(2)`s
        // them onto the socket — the kernel encrypts them into TLS records
        // (zero-copy over TLS). The wire bytes a client decrypts are identical
        // to the userspace path; only the encrypt locus moves kernel-side.
        #[cfg(target_os = "linux")]
        if broker.ktls_enabled {
            match acceptor.accept(ktls::CorkStream::new(stream)).await {
                Ok(tls_stream) => {
                    // Derive the mTLS principal from the peer cert BEFORE the
                    // kTLS transition consumes the stream by value. `get_ref()`
                    // reaches the rustls `ServerConnection` through the
                    // `CorkStream` wrapper exactly as for a plain `TlsStream`.
                    let mtls_principal = peer_cert_principal(&tls_stream);
                    // `config_ktls_server` consumes `tls_stream` by value; on
                    // error the stream is gone, so we cannot fall back to
                    // userspace TLS for THIS connection — we close it. This is
                    // safe precisely because the startup probe already proved
                    // kTLS works on this host, so an error here is an unexpected
                    // per-connection anomaly, not the common case.
                    match ktls::config_ktls_server(tls_stream).await {
                        Ok(ktls_stream) => {
                            // NB: any post-handshake app bytes rustls already
                            // decrypted are carried INSIDE `ktls_stream` (the
                            // `ktls` crate stores them and replays them on the
                            // first `poll_read`), so the `Framed` reader in
                            // `serve_connection_stream` sees them transparently
                            // — no manual drain plumbing needed.
                            serve_connection_stream(
                                broker,
                                ktls_stream,
                                spec,
                                peer,
                                mtls_principal,
                            )
                            .await;
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "kTLS configuration failed after handshake; closing connection \
                                 (startup probe had reported kTLS supported)"
                            );
                        }
                    }
                }
                Err(e) => tracing::debug!(error = %e, "TLS handshake failed"),
            }
            return;
        }

        match acceptor.accept(stream).await {
            Ok(tls_stream) => {
                // Derive a Principal from the peer cert
                // (mTLS). If the listener has client_auth=Required, the
                // handshake itself fails when no cert is presented, so
                // we always have one here. Optional or Disabled may
                // produce `None`.
                let mtls_principal = peer_cert_principal(&tls_stream);
                serve_connection_stream(broker, tls_stream, spec, peer, mtls_principal).await;
            }
            Err(e) => tracing::debug!(error = %e, "TLS handshake failed"),
        }
    } else {
        serve_connection_plaintext(broker, stream, spec, peer).await;
    }
}

/// Inspect the post-handshake TLS stream for a peer certificate. If
/// one is present, derive the principal name (Subject DN) via
/// [`crabka_security::extract_principal_from_cert`].
fn peer_cert_principal<S>(
    stream: &tokio_rustls::server::TlsStream<S>,
) -> Option<crabka_security::Principal> {
    let (_, server_conn) = stream.get_ref();
    let cert = server_conn.peer_certificates()?.first()?;
    let name = crabka_security::extract_principal_from_cert(cert.as_ref())?;
    Some(crabka_security::Principal {
        name,
        auth_method: crabka_security::AuthMethod::MTls,
        groups: vec![],
    })
}

/// Plaintext entry point: keeps the legacy `TcpStream`-typed signature
/// for call sites (and lets us record the peer's TCP address before we
/// hand the stream to the generic loop).
async fn serve_connection_plaintext(
    broker: std::sync::Arc<Broker>,
    stream: TcpStream,
    spec: crate::config::ListenerSpec,
    peer: SocketAddr,
) {
    serve_connection_stream(broker, stream, spec, peer, None).await;
}

fn initial_connection_auth(
    is_sasl_listener: bool,
    mtls_principal: Option<crabka_security::Principal>,
) -> crate::network::auth::ConnectionAuth {
    if is_sasl_listener {
        return crate::network::auth::ConnectionAuth::Anonymous;
    }
    let principal = mtls_principal.unwrap_or_else(|| crabka_security::Principal {
        name: "ANONYMOUS".to_string(),
        auth_method: crabka_security::AuthMethod::Anonymous,
        groups: vec![],
    });
    crate::network::auth::ConnectionAuth::Authenticated {
        principal,
        mechanism: crabka_security::SaslMechanism::Plain,
        expires_at_ms: None,
        authenticated_via_token: false,
    }
}

fn auth_deadline(auth: &crate::network::auth::ConnectionAuth) -> Option<tokio::time::Instant> {
    match auth {
        crate::network::auth::ConnectionAuth::Authenticated {
            expires_at_ms: Some(expires_at_ms),
            ..
        } => Some(instant_at_epoch_ms(*expires_at_ms)),
        crate::network::auth::ConnectionAuth::Reauthenticating { previous, .. } => {
            previous.expires_at_ms.map(instant_at_epoch_ms)
        }
        _ => None,
    }
}

async fn next_connection_frame<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    auth: &crate::network::auth::ConnectionAuth,
) -> Option<Bytes>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame_result = tokio::select! {
        biased;
        next = framed.next() => next,
        () = sleep_until_some(auth_deadline(auth)) => {
            tracing::info!(
                principal = ?auth_principal_name(auth),
                "SASL session expired, closing connection (KIP-368)"
            );
            return None;
        }
    };
    match frame_result {
        Some(Ok(bytes)) => Some(bytes.freeze()),
        Some(Err(error)) => {
            tracing::warn!(%error, "frame decode error, closing");
            None
        }
        None => None,
    }
}

fn capture_client_software(
    parsed: &crate::network::request::ParsedRequest<'_>,
    name: &mut String,
    version: &mut String,
) {
    if parsed.api_key != API_VERSIONS_KEY || parsed.api_version < 3 {
        return;
    }
    let mut body = parsed.body;
    if let Ok(request) = crabka_protocol::owned::api_versions_request::ApiVersionsRequest::decode(
        &mut body,
        parsed.api_version,
    ) && crate::handlers::api_versions::is_valid_client_info(&request.client_software_name)
        && crate::handlers::api_versions::is_valid_client_info(&request.client_software_version)
    {
        name.clone_from(&request.client_software_name);
        version.clone_from(&request.client_software_version);
    }
}

fn parse_connection_request<'a>(
    broker: &Broker,
    frame: &'a Bytes,
    peer: &SocketAddr,
) -> Option<(crate::network::request::ParsedRequest<'a>, tracing::Span)> {
    let peeked_api_key = match crate::network::request::peek_api_key(frame) {
        Ok(api_key) => api_key,
        Err(error) => {
            tracing::warn!(%error, "frame too small to peek api_key, closing");
            return None;
        }
    };
    let parsed = match crate::network::request::parse_request(frame, |api_key, version| {
        broker.handlers().body_flexible(api_key, version)
    }) {
        Ok(parsed) => parsed,
        Err(error) => {
            tracing::warn!(%error, "request parse error, closing");
            return None;
        }
    };
    debug_assert_eq!(parsed.api_key, peeked_api_key);
    let span = if tracing::enabled!(
        target: crate::telemetry::REQUEST_TARGET,
        tracing::Level::DEBUG
    ) {
        crate::telemetry::request_span(
            parsed.api_key,
            parsed.api_version,
            parsed.correlation_id,
            parsed.client_id,
            peer,
        )
    } else {
        tracing::Span::none()
    };
    Some((parsed, span))
}

fn begin_request(
    broker: &Broker,
    parsed: &crate::network::request::ParsedRequest<'_>,
) -> (std::time::Instant, InFlightGuard) {
    let started = std::time::Instant::now();
    broker.metrics.record_api_request(parsed.api_key);
    tracing::info!(
        api_key = parsed.api_key,
        api_version = parsed.api_version,
        correlation_id = parsed.correlation_id,
        body_flexible = parsed.body_flexible,
        body_len = parsed.body.len(),
        "dispatching request"
    );
    (started, InFlightGuard::new(&broker.metrics, parsed.api_key))
}

async fn send_unsupported_response<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    broker: &Broker,
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &crate::network::auth::ConnectionAuth,
    started: std::time::Instant,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    broker
        .metrics
        .record_unsupported_api_request(parsed.api_key);
    let mut body = BytesMut::with_capacity(2);
    body.put_i16(codes::UNSUPPORTED_VERSION);
    let response = encode_response(
        parsed.api_key,
        parsed.correlation_id,
        parsed.body_flexible,
        &body.freeze(),
        broker.config.socket_request_max_bytes,
    );
    let response = maybe_apply_request_quota(broker, response, parsed, auth, started).await;
    if let Err(error) = framed.send(response).await {
        tracing::warn!(%error, "framed.send error, closing");
        return false;
    }
    true
}

async fn dispatch_fetch<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    broker: &Broker,
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
    request_span: tracing::Span,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin + crate::network::fetch_writer::SendfileSink,
{
    let sendfile_capable =
        crate::network::fetch_writer::SendfileSink::is_sendfile_capable(framed.get_ref());
    match handle_fetch_frame_from_parsed(broker, parsed, auth, peer, sendfile_capable)
        .instrument(request_span)
        .await
    {
        Ok(operations) => {
            if let Err(error) = SinkExt::<Bytes>::flush(framed).await {
                tracing::warn!(%error, "framed.flush error before fetch plan, closing");
                return false;
            }
            if let Err(error) =
                crate::network::fetch_writer::write_fetch_plan(framed.get_mut(), operations).await
            {
                tracing::warn!(%error, "fetch plan write error, closing");
                return false;
            }
            true
        }
        Err(error) => {
            broker.metrics.record_request_error(parsed.api_key);
            tracing::warn!(%error, "Fetch dispatch error, closing connection");
            false
        }
    }
}

async fn send_registry_response<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    entry: crate::handlers::DispatchEntry,
    context: DispatchContext<'_, '_>,
    request_span: tracing::Span,
    started: std::time::Instant,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut response = match dispatch_registry_response(entry, context)
        .instrument(request_span)
        .await
    {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            tracing::warn!("registry entry has no ordinary dispatcher, closing connection");
            return false;
        }
        Err(error) => {
            context
                .broker
                .metrics
                .record_request_error(context.parsed.api_key);
            tracing::warn!(%error, "registry dispatch error, closing connection");
            return false;
        }
    };
    if entry.quota_policy() == crate::handlers::RequestQuotaPolicy::ApplyFallbackAccounting {
        response = maybe_apply_request_quota(
            context.broker,
            response,
            context.parsed,
            context.auth,
            started,
        )
        .await;
    }
    if let Err(error) = framed.send(response).await {
        tracing::warn!(%error, "framed.send error, closing");
        return false;
    }
    true
}

/// Generic per-connection request loop. `S` is the post-handshake byte
/// stream — `TcpStream` for plaintext listeners, `tokio_rustls::server::TlsStream<TcpStream>`
/// for TLS listeners. `spec` carries the listener's protocol so the loop
/// can initialise `ConnectionAuth` correctly and gate pre-auth requests on
/// SASL listeners.
// each api_key intercept arm adds ~15 lines.
async fn serve_connection_stream<S>(
    broker: std::sync::Arc<Broker>,
    stream: S,
    spec: crate::config::ListenerSpec,
    peer: SocketAddr,
    mtls_principal: Option<crabka_security::Principal>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static + crate::network::fetch_writer::SendfileSink,
{
    let mut framed: Framed<S, _> =
        Framed::new(stream, codec::codec(broker.config.socket_request_max_bytes));
    let is_sasl_listener = spec.protocol.requires_sasl();
    let sasl_mechanisms = crate::network::listener::resolve_sasl_mechanisms_for_listener(
        &spec,
        &broker.config.enabled_sasl_mechanisms,
    )
    .to_owned();
    let mut auth = initial_connection_auth(is_sasl_listener, mtls_principal);
    // Track live connections for the duration of this serve loop. The
    // gauge is decremented when `_conn` drops on any loop exit (EOF,
    // decode/send error, or SASL-session expiry).
    let _conn = ActiveConnectionGuard::new(&broker.metrics);
    tracing::info!(listener = %spec.name, sasl = is_sasl_listener, "connection opened");

    // KIP-714 client software identity, populated by the first ApiVersions v3+ request.
    // so `GetTelemetrySubscriptions` can be served even on connections that
    // never sent `ApiVersions` (e.g. early-version clients).
    let mut client_software = (String::new(), String::new());

    loop {
        let Some(frame) = next_connection_frame(&mut framed, &auth).await else {
            break;
        };
        let Some((parsed, req_span)) = parse_connection_request(&broker, &frame, &peer) else {
            break;
        };
        // Per-state request gate: on SASL listeners, gate every api_key
        // through `auth.allows_request(api_key)`. This covers:
        //   - Anonymous / Negotiating: only the pre-auth allowlist
        //     (ApiVersions=18, SaslHandshake=17, SaslAuthenticate=36).
        //   - Reauthenticating (KIP-368 in-band re-auth in progress): only
        //     SaslAuthenticate=36 — any other request during re-auth is a
        //     protocol violation and the connection is closed.
        //   - Authenticated: all api_keys allowed.
        // Anything blocked closes the TCP connection with no body.
        //
        // Response-shape note: every api_key has a different response body,
        // so producing a typed `error_code = 34` frame from this generic
        // dispatch layer would require a switch over every api_key. The
        // SASL path sends a *typed* SaslAuthenticate(36) response with error_code=58
        // on credential failure (its specific shape is known there). For
        // the generic pre-auth gate we close the TCP connection without
        // sending a body — JVM clients surface this to the caller as an
        // auth failure (closed connection during SASL), and this matches
        // the conservative behaviour we want for unauthenticated peers.
        if is_sasl_listener && !auth.allows_request(parsed.api_key) {
            tracing::info!(
                api_key = parsed.api_key,
                listener = %spec.name,
                "request blocked by per-state auth gate (ILLEGAL_SASL_STATE), closing connection"
            );
            let _ = codes::ILLEGAL_SASL_STATE; // referenced for docs/grep
            break;
        }
        // SASL frames (api_key 17 / 36) mutate the per-connection auth state,
        // which lives in this loop. They run *before* the regular handler
        // table because handlers receive only `&Broker` and have no way to
        // touch `auth`. Returning `Some(SaslFrameOutcome)` short-circuits
        // the normal registry path for that frame.
        if let Some(outcome) = try_handle_sasl_frame(&broker, &parsed, &mut auth, &sasl_mechanisms)
            .instrument(req_span.clone())
            .await
        {
            let SaslFrameOutcome {
                response_bytes,
                close_after,
            } = match outcome {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!(error = %e, "SASL dispatch error, closing connection");
                    break;
                }
            };
            if let Err(e) = framed.send(response_bytes).await {
                tracing::warn!(error = %e, "framed.send error during SASL, closing");
                break;
            }
            if close_after {
                tracing::info!("closing connection after failed SaslAuthenticate");
                break;
            }
            continue;
        }

        capture_client_software(&parsed, &mut client_software.0, &mut client_software.1);

        let (started, _in_flight) = begin_request(&broker, &parsed);

        let Some(entry) = broker.handlers().get(parsed.api_key) else {
            tracing::warn!(
                api_key = parsed.api_key,
                api_version = parsed.api_version,
                "unsupported api, returning error"
            );
            if !send_unsupported_response(&mut framed, &broker, &parsed, &auth, started).await {
                break;
            }
            continue;
        };

        if matches!(entry.kind(), crate::handlers::DispatchKind::Fetch) {
            if !dispatch_fetch(
                &mut framed,
                &broker,
                &parsed,
                &auth,
                &peer,
                req_span.clone(),
            )
            .await
            {
                break;
            }
            continue;
        }

        let context = DispatchContext {
            broker: &broker,
            parsed: &parsed,
            frame: &frame,
            auth: &auth,
            peer: &peer,
            listener_name: &spec.name,
            client_software_name: &client_software.0,
            client_software_version: &client_software.1,
        };
        if !send_registry_response(&mut framed, entry, context, req_span, started).await {
            break;
        }
    }
    tracing::info!("connection closed");
}

/// Outcome of intercepting a SASL frame: the bytes to write back to the
/// peer and whether the dispatcher should close the connection after the
/// send completes (used for `SaslAuthenticate` failures + illegal state).
struct SaslFrameOutcome {
    response_bytes: Bytes,
    close_after: bool,
}

/// If `frame` is a `SaslHandshake` (17) or `SaslAuthenticate` (36) request,
/// handle it inline (mutating `auth`) and return a [`SaslFrameOutcome`].
/// Returns `None` for every other `api_key` — the caller falls through to the
/// regular registry dispatch.
///
/// Errors here close the connection (protocol violations, e.g. an
/// undecodable header).
async fn try_handle_sasl_frame(
    broker: &Broker,
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &mut crate::network::auth::ConnectionAuth,
    sasl_mechanisms: &[crabka_security::SaslMechanism],
) -> Option<Result<SaslFrameOutcome, BrokerError>> {
    let api_key = parsed.api_key;
    if api_key != SASL_HANDSHAKE_KEY && api_key != SASL_AUTHENTICATE_KEY {
        return None;
    }
    Some(handle_sasl_frame(broker, parsed, auth, sasl_mechanisms).await)
}

async fn handle_sasl_frame(
    broker: &Broker,
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &mut crate::network::auth::ConnectionAuth,
    sasl_mechanisms: &[crabka_security::SaslMechanism],
) -> Result<SaslFrameOutcome, BrokerError> {
    use crabka_protocol::{Decode, Encode};

    let (resp_body, close_after) = match parsed.api_key {
        SASL_HANDSHAKE_KEY => (handle_sasl_handshake(parsed, auth, sasl_mechanisms)?, false),
        SASL_AUTHENTICATE_KEY => {
            let mut cur: &[u8] = parsed.body;
            let req =
                crabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest::decode(
                    &mut cur,
                    parsed.api_version,
                )?;
            // Must be mid-SASL: either `Negotiating` (initial auth: a
            // SaslHandshake was the previous frame) or `Reauthenticating`
            // (KIP-368 in-band re-auth: a SaslHandshake just ran on an
            // already-authenticated connection). Any other state returns
            // ILLEGAL_SASL_STATE (34) and closes.
            let mech_opt = match auth {
                crate::network::auth::ConnectionAuth::Negotiating { mechanism, .. } => {
                    Some(*mechanism)
                }
                crate::network::auth::ConnectionAuth::Reauthenticating { previous, .. } => {
                    Some(previous.mechanism)
                }
                _ => None,
            };
            let resp = if let Some(mech) = mech_opt {
                match mech {
                    crabka_security::SaslMechanism::Plain => {
                        crate::network::auth::handle_authenticate_plain(
                            &req,
                            auth,
                            &broker.config.plain_credentials,
                        )
                    }
                    crabka_security::SaslMechanism::ScramSha256
                    | crabka_security::SaslMechanism::ScramSha512 => {
                        crate::network::auth::handle_authenticate_scram(
                            &req,
                            auth,
                            &*broker.controller,
                        )
                    }
                    crabka_security::SaslMechanism::OAuthBearer => {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
                        crate::network::auth::handle_authenticate_oauthbearer(
                            &req,
                            auth,
                            &broker.config.oauthbearer_validator,
                            now_ms,
                            broker.config.oauthbearer_max_session_lifetime_seconds,
                        )
                        .await
                    }
                    crabka_security::SaslMechanism::Gssapi => {
                        let cfg = broker
                            .config
                            .gssapi
                            .as_ref()
                            .expect("GSSAPI enabled without config");
                        crate::network::auth::handle_authenticate_gssapi(&req, auth, cfg)
                    }
                }
            } else {
                crabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse {
                    error_code: codes::ILLEGAL_SASL_STATE,
                    error_message: Some("SaslAuthenticate without prior SaslHandshake".into()),
                    auth_bytes: Bytes::new(),
                    session_lifetime_ms: 0,
                    ..Default::default()
                }
            };
            // Account this SaslAuthenticate frame in the
            // per-mechanism success/failure counters. The mechanism
            // is the one selected by the preceding SaslHandshake;
            // ILLEGAL_SASL_STATE rejects (no prior handshake) land
            // under the `Unknown` sentinel so the metric stays
            // bounded.
            let mech_label = mech_opt.map_or(
                crate::metrics::UNKNOWN_LABEL,
                crabka_security::SaslMechanism::wire_name,
            );
            broker
                .metrics
                .record_authentication(mech_label, resp.error_code == 0);
            let close = resp.error_code != 0;
            let mut buf = BytesMut::with_capacity(resp.encoded_len(parsed.api_version));
            resp.encode(&mut buf, parsed.api_version)?;
            (buf.freeze(), close)
        }
        _ => unreachable!("filtered by caller to 17 / 36 only"),
    };

    let response_bytes = encode_response(
        parsed.api_key,
        parsed.correlation_id,
        parsed.body_flexible,
        &resp_body,
        broker.config.socket_request_max_bytes,
    );
    Ok(SaslFrameOutcome {
        response_bytes,
        close_after,
    })
}

fn handle_sasl_handshake(
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &mut crate::network::auth::ConnectionAuth,
    sasl_mechanisms: &[crabka_security::SaslMechanism],
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::{Decode, Encode};

    let mut body = parsed.body;
    let request = crabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest::decode(
        &mut body,
        parsed.api_version,
    )?;
    let response = crate::network::auth::handle_handshake(&request, auth, sasl_mechanisms);
    let mut encoded = BytesMut::with_capacity(response.encoded_len(parsed.api_version));
    response.encode(&mut encoded, parsed.api_version)?;
    Ok(encoded.freeze())
}

/// Decode + dispatch a `Fetch` (`api_key` 1) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// batch-authorize every topic in the request for `Read`.
/// On PLAINTEXT/SSL listeners the connection is implicitly
/// `Authenticated { ANONYMOUS / Plain }` (see the loop init), so
/// `principal()` always returns `Some` here; the `unwrap_or_else`
/// fallback covers the defensive SASL pre-auth case.
/// Build the Fetch response as an ordered [`WriteOp`] plan rather than a single
/// contiguous `Bytes`. The plan's first op carries the 4-byte frame length +
/// correlation header; subsequent ops are the response envelope interleaved
/// with each partition's records region (a refcounted view of the verbatim
/// `.log` bytes — no copy). The connection writer drains the plan directly on
/// the raw stream, bypassing `encode_response` + the `Framed` codec copies.
///
/// The legacy v0–v3 path has no canonical write-plan (it down-converts), so it
/// is encoded the old way and returned as a single `Inline` op — i.e. the
/// existing copy path, just expressed as a one-element plan.
async fn handle_fetch_frame_from_parsed(
    broker: &Broker,
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
    sendfile_capable: bool,
) -> Result<Vec<crate::network::fetch_writer::WriteOp>, BrokerError> {
    use crate::network::fetch_writer::{WriteOp, build_fetch_plan};

    debug_assert_eq!(parsed.api_key, 1);

    let principal = principal_or_anonymous(auth);
    let ctx = crate::handlers::RequestContext::new(
        principal,
        peer,
        parsed.client_id.unwrap_or(""),
        sendfile_capable && parsed.api_version >= 4,
        "",
    );

    let (resp, version) = crate::handlers::fetch::handle(
        broker,
        parsed.api_version,
        parsed.correlation_id,
        parsed.body,
        &ctx,
    )
    .await?;

    if version < 4 {
        // Legacy down-conversion path: encode the whole body the old way and
        // wrap it (plus the response header) as a single inline op.
        let body_bytes = crate::handlers::fetch::encode_fetch_response(resp, version)?;
        if crate::network::response_header_len(parsed.api_key, parsed.body_flexible)
            + body_bytes.len()
            >= broker.config.socket_request_max_bytes
        {
            return Err(BrokerError::Io(std::io::Error::other(
                "fetch response exceeds max frame size",
            )));
        }
        let framed = encode_response(
            parsed.api_key,
            parsed.correlation_id,
            parsed.body_flexible,
            &body_bytes,
            broker.config.socket_request_max_bytes,
        );
        // Prepend the 4-byte frame length so the writer path is uniform.
        let mut framed_with_len = BytesMut::with_capacity(4 + framed.len());
        framed_with_len.put_u32(u32::try_from(framed.len()).map_err(|_| {
            BrokerError::Io(std::io::Error::other(
                "fetch response exceeds max frame size",
            ))
        })?);
        framed_with_len.put_slice(&framed);
        return Ok(vec![WriteOp::Inline(framed_with_len.freeze())]);
    }

    // On plaintext connections (SENDFILE alias: Linux + Apple + FreeBSD/
    // DragonFly), drain file-backed records regions via sendfile; everywhere
    // else (TLS, Windows) use the portable vectored resolver. `do_read` only
    // ever emits `FileRegions` when `sendfile_capable`, so the resolver choice
    // and the payload kind stay in lock-step.
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    {
        if sendfile_capable && parsed.api_version >= 4 {
            return build_fetch_plan(
                &resp,
                version,
                parsed.correlation_id,
                parsed.body_flexible,
                broker.config.socket_request_max_bytes,
                crate::network::fetch_writer::resolve_records_sendfile,
            );
        }
    }

    build_fetch_plan(
        &resp,
        version,
        parsed.correlation_id,
        parsed.body_flexible,
        broker.config.socket_request_max_bytes,
        crate::network::fetch_writer::resolve_records_inline,
    )
}

#[derive(Clone, Copy)]
struct DispatchContext<'a, 'request> {
    broker: &'a Broker,
    parsed: &'a crate::network::request::ParsedRequest<'request>,
    frame: &'a Bytes,
    auth: &'a crate::network::auth::ConnectionAuth,
    peer: &'a SocketAddr,
    listener_name: &'a str,
    client_software_name: &'a str,
    client_software_version: &'a str,
}

async fn dispatch_registered_bytes(
    entry: crate::handlers::DispatchEntry,
    context: DispatchContext<'_, '_>,
) -> Option<Result<Bytes, BrokerError>> {
    let DispatchContext {
        broker,
        parsed,
        frame,
        auth,
        peer,
        listener_name,
        client_software_name,
        client_software_version,
    } = context;
    match entry.kind() {
        crate::handlers::DispatchKind::Context(handler)
        | crate::handlers::DispatchKind::DecodedContext(handler)
        | crate::handlers::DispatchKind::EncodedContext(handler) => {
            let ctx = crate::handlers::RequestContext::new(
                principal_or_anonymous(auth),
                peer,
                parsed.client_id.unwrap_or(""),
                false,
                listener_name,
            );
            Some(encode_dispatch_result(
                parsed,
                broker.config.socket_request_max_bytes,
                handler(
                    broker,
                    parsed.api_version,
                    parsed.correlation_id,
                    parsed.body,
                    &ctx,
                )
                .await,
            ))
        }
        crate::handlers::DispatchKind::Auth(handler) => Some(encode_dispatch_result(
            parsed,
            broker.config.socket_request_max_bytes,
            handler(
                broker,
                parsed.api_version,
                parsed.correlation_id,
                parsed.body,
                auth,
                peer,
            )
            .await,
        )),
        crate::handlers::DispatchKind::Produce(handler) => {
            let ctx = crate::handlers::RequestContext::new(
                principal_or_anonymous(auth),
                peer,
                parsed.client_id.unwrap_or(""),
                false,
                "",
            );
            let body_offset = frame.len() - parsed.body.len();
            let body_bytes = frame.slice(body_offset..);
            Some(encode_dispatch_result(
                parsed,
                broker.config.socket_request_max_bytes,
                handler(
                    broker,
                    parsed.api_version,
                    parsed.correlation_id,
                    parsed.body,
                    body_bytes,
                    &ctx,
                )
                .await,
            ))
        }
        crate::handlers::DispatchKind::Telemetry(handler) => {
            let ctx = crate::handlers::TelemetryContext::new(
                peer,
                parsed.client_id.unwrap_or(""),
                client_software_name,
                client_software_version,
            );
            Some(encode_dispatch_result(
                parsed,
                broker.config.socket_request_max_bytes,
                handler(
                    broker,
                    parsed.api_version,
                    parsed.correlation_id,
                    parsed.body,
                    &ctx,
                )
                .await,
            ))
        }
        crate::handlers::DispatchKind::Plain(_)
        | crate::handlers::DispatchKind::Fetch
        | crate::handlers::DispatchKind::SaslMetadata => None,
    }
}

fn encode_dispatch_result(
    parsed: &crate::network::request::ParsedRequest<'_>,
    max_frame_bytes: usize,
    result: Result<Bytes, BrokerError>,
) -> Result<Bytes, BrokerError> {
    result.map(|body| {
        encode_response(
            parsed.api_key,
            parsed.correlation_id,
            parsed.body_flexible,
            &body,
            max_frame_bytes,
        )
    })
}

async fn dispatch_registry_response(
    entry: crate::handlers::DispatchEntry,
    context: DispatchContext<'_, '_>,
) -> Result<Option<Bytes>, BrokerError> {
    let DispatchContext { broker, parsed, .. } = context;
    match dispatch_registered_bytes(entry, context).await {
        Some(result) => result.map(Some),
        None => match entry.kind() {
            crate::handlers::DispatchKind::Plain(handler) => {
                let body = handler(
                    broker,
                    parsed.api_version,
                    parsed.correlation_id,
                    parsed.body,
                )
                .await?;
                Ok(Some(encode_response(
                    parsed.api_key,
                    parsed.correlation_id,
                    parsed.body_flexible,
                    &body,
                    broker.config.socket_request_max_bytes,
                )))
            }
            crate::handlers::DispatchKind::Fetch | crate::handlers::DispatchKind::SaslMetadata => {
                Ok(None)
            }
            _ => Ok(None),
        },
    }
}

async fn maybe_apply_request_quota(
    broker: &Broker,
    mut response_bytes: Bytes,
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &crate::network::auth::ConnectionAuth,
    started: std::time::Instant,
) -> Bytes {
    let elapsed_micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    let self_accounts = matches!(
        ApiKey::from_i16(parsed.api_key),
        Some(ApiKey::Produce | ApiKey::Fetch)
    );
    if !self_accounts && let Some(principal) = auth.principal() {
        let image = broker.controller.current_image();
        let delay = crate::quota::consume_request_quota(
            &image,
            &broker.quota_buckets,
            &principal.name,
            parsed.client_id.unwrap_or(""),
            elapsed_micros,
        );
        if delay > std::time::Duration::ZERO {
            if throttle_is_leading_field(parsed.api_key, parsed.api_version) {
                let delay_ms = i32::try_from(delay.as_millis()).unwrap_or(i32::MAX);
                response_bytes = patch_leading_throttle(
                    response_bytes,
                    parsed.api_key,
                    parsed.body_flexible,
                    delay_ms,
                );
            }
            tokio::time::sleep(delay).await;
        }
    }
    response_bytes
}

/// RAII guard covering one dispatched request: bumps `in_flight_requests`
/// on construction and, on drop (any exit path — success, handler error,
/// or panic unwind), decrements it and observes the elapsed wall-clock on
/// the `request_duration_seconds{api}` histogram. Holds a cheap
/// `BrokerMetrics` clone (an `Arc`-bundle) so it does not borrow `broker`
/// across the handler `.await`.
struct InFlightGuard {
    metrics: crate::metrics::BrokerMetrics,
    api_key: i16,
    started: std::time::Instant,
}

impl InFlightGuard {
    fn new(metrics: &crate::metrics::BrokerMetrics, api_key: i16) -> Self {
        metrics.in_flight_requests.inc();
        Self {
            metrics: metrics.clone(),
            api_key,
            started: std::time::Instant::now(),
        }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.metrics.in_flight_requests.dec();
        self.metrics
            .observe_request_duration(self.api_key, self.started.elapsed().as_secs_f64());
    }
}

/// RAII guard for one live client connection: bumps `active_connections`
/// on construction, decrements it when the per-connection serve loop exits
/// (drop). Holds a cheap `BrokerMetrics` clone.
struct ActiveConnectionGuard {
    metrics: crate::metrics::BrokerMetrics,
}

impl ActiveConnectionGuard {
    fn new(metrics: &crate::metrics::BrokerMetrics) -> Self {
        metrics.active_connections.inc();
        Self {
            metrics: metrics.clone(),
        }
    }
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.metrics.active_connections.dec();
    }
}

/// Prepend the response header (`corr_id` + optional tagged-fields byte)
/// in front of the handler's body bytes.
// PERF: this copies the whole body to prepend a 4-5 byte header. A
// `bytes::Buf::chain(header, body)` would avoid the copy, but the sink is a
// `Framed<S, LengthDelimitedCodec>` and `LengthDelimitedCodec` only implements
// `Encoder<Bytes>` (a single concrete impl) — `framed.send` therefore requires
// a contiguous `Bytes` and will not accept a `Chain`/`impl Buf`. Worse, that
// `Encoder::encode` itself does `dst.extend_from_slice(&data[..])`, i.e. it
// copies the body into the codec's write buffer regardless. Eliminating the
// copy here would require swapping the codec for a custom `Encoder<impl Buf>`
// that vectored-writes header+body, which ripples through `codec.rs`, the
// roundtrip test, and all ~50 `framed.send(bytes)` call sites in this file.
// Out of scope for a single-file change; left as-is to keep wire bytes exact.
fn encode_response(
    api_key: ApiKeyCode,
    correlation_id: CorrelationId,
    body_flexible: bool,
    body: &[u8],
    max_frame_bytes: usize,
) -> Bytes {
    let header_len = crate::network::response_header_len(api_key, body_flexible);
    debug_assert!(header_len + body.len() < max_frame_bytes);
    let mut buf = BytesMut::with_capacity(header_len + body.len());
    buf.put_i32(correlation_id);
    if crate::network::response_header_v1(api_key, body_flexible) {
        buf.put_u8(0); // empty tagged fields
    }
    buf.put_slice(body);
    buf.freeze()
}

/// KIP-219 (throttle-then-respond): `true` when `api_key`'s response carries
/// `ThrottleTimeMs` as its FIRST body field at `version`, so the dispatch loop
/// can surface the request-quota throttle by patching that leading int32 in
/// place. Boundaries are verified against the 4.x response schemas. APIs absent
/// from this table keep the pre-KIP-219 behavior (throttle still enforced by the
/// channel mute, just not echoed); Produce (0) / Fetch (1) self-account and
/// never reach this path. `OffsetDelete` (47) is intentionally excluded — its
/// leading field is `ErrorCode`, so patching would corrupt it.
fn throttle_is_leading_field(api_key: ApiKeyCode, version: ApiVersion) -> bool {
    // The version bounds are the schema versions at which each API moved
    // `ThrottleTimeMs` to the front of its response (verified against the
    // 4.x response schemas); they are deliberately kept as literals here
    // and pinned by the schema-boundary tests.
    match ApiKey::from_i16(api_key) {
        Some(ApiKey::ListOffsets | ApiKey::JoinGroup | ApiKey::OffsetForLeaderEpoch) => {
            version >= 2
        }
        Some(ApiKey::Metadata | ApiKey::OffsetCommit | ApiKey::OffsetFetch) => version >= 3,
        Some(
            ApiKey::FindCoordinator
            | ApiKey::Heartbeat
            | ApiKey::LeaveGroup
            | ApiKey::SyncGroup
            | ApiKey::DescribeGroups
            | ApiKey::ListGroups,
        ) => version >= 1,
        // InitProducerId / DescribeCluster / ConsumerGroupHeartbeat (all 0+)
        Some(ApiKey::InitProducerId | ApiKey::DescribeCluster | ApiKey::ConsumerGroupHeartbeat) => {
            true
        }
        _ => false,
    }
}

/// Patch the leading `ThrottleTimeMs` (int32) of an already-encoded response in
/// place, raising it to `max(existing, delay_ms)`. The body begins right after
/// the response header, whose length mirrors `encode_response`: 5 bytes when the
/// body is flexible and the api is not `ApiVersions`, else 4. Callers must first
/// confirm `throttle_is_leading_field`.
fn patch_leading_throttle(
    resp: Bytes,
    api_key: ApiKeyCode,
    body_flexible: bool,
    delay_ms: i32,
) -> Bytes {
    let off = crate::network::response_header_len(api_key, body_flexible);
    if resp.len() < off + 4 {
        return resp;
    }
    let mut buf = BytesMut::from(resp.as_ref());
    let existing = i32::from_be_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
    let patched = existing.max(delay_ms);
    buf[off..off + 4].copy_from_slice(&patched.to_be_bytes());
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::{assert, check};

    use super::*;

    const DEFAULT_MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

    fn request_frame(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<&[u8]>,
        tagged: Option<u8>,
        body: &[u8],
    ) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_i16(api_key);
        buf.put_i16(api_version);
        buf.put_i32(correlation_id);
        match client_id {
            Some(id) => {
                buf.put_i16(i16::try_from(id.len()).expect("client id length"));
                buf.put_slice(id);
            }
            None => buf.put_i16(-1),
        }
        if let Some(tagged) = tagged {
            buf.put_u8(tagged);
        }
        buf.put_slice(body);
        buf
    }

    #[test]
    fn auth_principal_name_reads_authenticated_and_reauth_previous_only() {
        let authenticated = crate::network::auth::ConnectionAuth::Authenticated {
            principal: crabka_security::Principal {
                name: "alice".to_string(),
                auth_method: crabka_security::AuthMethod::SaslOAuthBearer,
                groups: vec![],
            },
            mechanism: crabka_security::SaslMechanism::OAuthBearer,
            expires_at_ms: Some(123),
            authenticated_via_token: false,
        };
        let reauth = crate::network::auth::ConnectionAuth::Reauthenticating {
            previous: crate::network::auth::AuthenticatedSnapshot {
                principal: crabka_security::Principal {
                    name: "bob".to_string(),
                    auth_method: crabka_security::AuthMethod::SaslOAuthBearer,
                    groups: vec![],
                },
                mechanism: crabka_security::SaslMechanism::OAuthBearer,
                expires_at_ms: Some(456),
            },
            exchange: crate::network::auth::SaslExchange::OAuthBearer,
        };
        let anonymous = crate::network::auth::ConnectionAuth::Anonymous;

        let cases = [
            ("authenticated", &authenticated, Some("alice")),
            ("reauthenticating uses previous", &reauth, Some("bob")),
            ("anonymous", &anonymous, None),
        ];
        for (case, auth, want) in cases {
            assert!(auth_principal_name(auth) == want, "{case}");
        }
    }

    // `start_paused = true` runs these on tokio's virtual clock: with no other
    // work pending, the runtime auto-advances logical time to the next timer, so
    // the `sleep_until`/`timeout` deadlines fire instantly and deterministically
    // instead of burning real wall-clock milliseconds.
    #[tokio::test(start_paused = true)]
    async fn sleep_until_some_none_remains_pending() {
        // `None` never resolves; the 10ms timeout is the only timer, so virtual
        // time jumps to it and the timeout elapses -> Err.
        let result = tokio::time::timeout(Duration::from_millis(10), sleep_until_some(None)).await;
        assert!(result.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_until_some_some_waits_until_deadline() {
        let before = tokio::time::Instant::now();
        let deadline = before + Duration::from_millis(10);
        // The inner sleep (deadline) fires before the outer 1s timeout, so the
        // timeout resolves Ok and virtual time has advanced exactly to `deadline`.
        tokio::time::timeout(Duration::from_secs(1), sleep_until_some(Some(deadline)))
            .await
            .expect("deadline should resolve");
        assert!(tokio::time::Instant::now() >= deadline);
    }

    #[test]
    fn instant_at_epoch_ms_maps_future_and_past_wall_clock_to_tokio_deadlines() {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_i64, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));

        let before = tokio::time::Instant::now();
        let future = instant_at_epoch_ms(now_ms + 250);
        let delay = future.duration_since(before);
        assert!(
            delay >= Duration::from_millis(100) && delay <= Duration::from_secs(2),
            "future epoch should become a near future tokio deadline, got {delay:?}"
        );

        let past = instant_at_epoch_ms(now_ms - 250);
        assert!(
            past <= tokio::time::Instant::now() + Duration::from_millis(50),
            "past epoch should fire immediately"
        );
    }

    #[test]
    fn encode_response_apiversions_uses_v0_header() {
        // ApiVersions response is always header v0 (no tagged byte) even
        // for flexible body versions.
        let body = [0u8, 0u8]; // error_code=0
        let out = encode_response(API_VERSIONS_KEY, 7, true, &body, DEFAULT_MAX_FRAME_BYTES);
        // 4 byte corr_id + body, no tagged byte.
        assert!(out.len() == 4 + body.len());
    }

    #[test]
    fn throttle_leading_field_table_matches_schemas() {
        // Present-and-leading version boundaries (verified vs 4.x schemas).
        // OffsetDelete (47) leads with ErrorCode — must never be patched.
        // Produce/Fetch self-account; ApiVersions is not in the table.
        let cases = [
            (11, 1, false), // JoinGroup v1: no throttle
            (11, 2, true),  // JoinGroup v2+: leading
            (3, 2, false),  // Metadata v2: no throttle
            (3, 3, true),   // Metadata v3+
            (12, 1, true),  // Heartbeat v1+
            (68, 0, true),  // ConsumerGroupHeartbeat v0+
            (47, 0, false), // OffsetDelete
            (0, 9, false),  // Produce
            (1, 13, false), // Fetch
            (18, 3, false), // ApiVersions
        ];
        for (api_key, version, want) in cases {
            assert!(
                throttle_is_leading_field(api_key, version) == want,
                "api_key {api_key} version {version}"
            );
        }
    }

    #[test]
    fn patch_leading_throttle_sets_field_flexible_and_nonflexible() {
        let read =
            |b: &[u8], off: usize| i32::from_be_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]]);

        // Flexible response header (ConsumerGroupHeartbeat, flexible v0+):
        // header = 5 bytes (corr_id + tagged byte); throttle int32 at offset 5.
        let mut body = BytesMut::new();
        body.put_i32(0); // ThrottleTimeMs = 0
        let resp = encode_response(68, 7, true, &body, DEFAULT_MAX_FRAME_BYTES);
        let patched = patch_leading_throttle(resp, 68, true, 250);
        assert!(read(&patched, 5) == 250);
        assert!(read(&patched, 0) == 7); // corr_id preserved

        // Non-flexible response header (Metadata v3): header = 4 bytes.
        let mut body = BytesMut::new();
        body.put_i32(10); // existing throttle 10 < 250
        let resp = encode_response(3, 9, false, &body, DEFAULT_MAX_FRAME_BYTES);
        let patched = patch_leading_throttle(resp, 3, false, 250);
        assert!(read(&patched, 4) == 250);
        assert!(read(&patched, 0) == 9);
    }

    #[test]
    fn patch_leading_throttle_keeps_existing_when_larger() {
        // max(existing, delay): an already-larger throttle is not lowered.
        let mut body = BytesMut::new();
        body.put_i32(500);
        let resp = encode_response(3, 1, false, &body, DEFAULT_MAX_FRAME_BYTES);
        let patched = patch_leading_throttle(resp, 3, false, 100);
        let v = i32::from_be_bytes([patched[4], patched[5], patched[6], patched[7]]);
        assert!(v == 500);
    }

    #[test]
    fn peek_api_key_reads_first_two_bytes_big_endian() {
        // api_key=18, version=3, corr_id=1 — only first 2 bytes are inspected.
        let mut buf = BytesMut::new();
        buf.put_i16(18);
        buf.put_i16(3);
        buf.put_i32(1);
        assert!(crate::network::request::peek_api_key(&buf).unwrap() == 18);
    }

    #[test]
    fn peek_api_key_rejects_short_frame() {
        let buf = [0u8; 1];
        assert!(crate::network::request::peek_api_key(&buf).is_err());
    }

    #[test]
    fn encode_response_other_flexible_inserts_tagged_byte() {
        let body = [0u8, 0u8];
        let out = encode_response(3, 7, true, &body, DEFAULT_MAX_FRAME_BYTES);
        assert!(out.len() == 5 + body.len());
        assert!(out[4] == 0); // tagged byte
    }

    /// KIP-853 RPCs (80/81/82) route through the registry path and are
    /// flexible from v0. This guards the metadata used when parsing their
    /// flexible request headers.
    #[test]
    fn raft_voter_rpcs_peek_and_flex_routing() {
        let registry = crate::handlers::registry::build_registry();

        for api_key in [80i16, 81, 82] {
            let mut buf = BytesMut::new();
            buf.put_i16(api_key);
            buf.put_i16(0); // version 0
            buf.put_i32(1); // corr_id
            assert!(crate::network::request::peek_api_key(&buf).unwrap() == api_key);
            assert!(
                registry.body_flexible(api_key, 0),
                "api_key {api_key} is flexible from v0"
            );
        }
    }

    /// The three KIP-853 controller-plane RPCs (`AddRaftVoter` 80,
    /// `RemoveRaftVoter` 81, `UpdateRaftVoter` 82) must reach their registry
    /// handlers. Drive each RPC over a real socket through the whole serve loop
    /// and assert it reaches its handler: a `DenyAll` authorizer makes every
    /// handler short-circuit at the ACL gate with `CLUSTER_AUTHORIZATION_FAILED`
    /// (31), which is observably different from the unsupported path's 35.
    #[tokio::test]
    async fn raft_voter_registry_routes_to_real_handlers() {
        use crabka_protocol::{
            Decode, Encode,
            owned::{
                add_raft_voter_request as add_req, add_raft_voter_response as add_resp,
                remove_raft_voter_request as rem_req, remove_raft_voter_response as rem_resp,
                update_raft_voter_request as upd_req, update_raft_voter_response as upd_resp,
            },
        };

        use crate::test_support::DenyAll;

        // Send a flexible (v2-header) request frame carrying `body` for
        // `api_key`/`version` and return the response body with its 5-byte
        // flexible header (corr_id + empty tagged-fields byte) stripped.
        async fn round_trip(
            framed: &mut Framed<TcpStream, tokio_util::codec::LengthDelimitedCodec>,
            api_key: i16,
            version: i16,
            body: &[u8],
        ) -> Vec<u8> {
            let frame = request_frame(api_key, version, 7, None, Some(0), body);
            framed.send(frame.freeze()).await.expect("send request");
            let resp = framed
                .next()
                .await
                .expect("a response frame")
                .expect("response decode");
            resp[5..].to_vec()
        }

        fn encode_default<T: Encode + Default>(version: i16) -> BytesMut {
            let mut body = BytesMut::new();
            T::default().encode(&mut body, version).expect("encode");
            body
        }

        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.authorizer = std::sync::Arc::new(DenyAll);
        let handle = Broker::start(cfg).await.expect("start broker");
        let broker = handle.broker_arc_for_test();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("listener addr");
        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.expect("accept");
            let spec = crate::config::ListenerSpec {
                name: "PLAINTEXT".to_string(),
                bind_addr: addr,
                advertised: "127.0.0.1:9092".to_string(),
                protocol: crabka_security::ListenerProtocol::Plaintext,
                tls_config: None,
                sasl_mechanisms: None,
            };
            serve_connection_stream(broker, stream, spec, peer, None).await;
        });

        let client = TcpStream::connect(addr).await.expect("connect");
        let mut framed = codec::frame(client, DEFAULT_MAX_FRAME_BYTES);

        let add_body = encode_default::<add_req::AddRaftVoterRequest>(add_req::MAX_VERSION);
        let raw = round_trip(&mut framed, 80, add_req::MAX_VERSION, &add_body).await;
        let add = add_resp::AddRaftVoterResponse::decode(&mut &raw[..], add_resp::MAX_VERSION)
            .expect("decode AddRaftVoterResponse");

        let rem_body = encode_default::<rem_req::RemoveRaftVoterRequest>(rem_req::MAX_VERSION);
        let raw = round_trip(&mut framed, 81, rem_req::MAX_VERSION, &rem_body).await;
        let rem = rem_resp::RemoveRaftVoterResponse::decode(&mut &raw[..], rem_resp::MAX_VERSION)
            .expect("decode RemoveRaftVoterResponse");

        let upd_body = encode_default::<upd_req::UpdateRaftVoterRequest>(upd_req::MAX_VERSION);
        let raw = round_trip(&mut framed, 82, upd_req::MAX_VERSION, &upd_body).await;
        let upd = upd_resp::UpdateRaftVoterResponse::decode(&mut &raw[..], upd_resp::MAX_VERSION)
            .expect("decode UpdateRaftVoterResponse");

        // Each real handler denies at the ACL gate; the fall-through path would
        // instead yield UNSUPPORTED_VERSION (and not even decode as this type).
        check!(add.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
        check!(rem.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
        check!(upd.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);

        drop(framed);
        server.await.expect("serve loop joins on client EOF");
        handle.shutdown().await;
    }
}
