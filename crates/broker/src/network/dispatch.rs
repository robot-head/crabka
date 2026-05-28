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

#![allow(dead_code)] // accept loop wires this up in Phase D (Task 11).

use std::net::SocketAddr;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::Instrument as _;

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::network::codec::{self, MAX_FRAME_BYTES};

const API_VERSIONS_KEY: i16 = 18;

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
    // hand the stream off to the TLS layer / framing loop. Slice-13 ACL
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
        match acceptor.accept(stream).await {
            Ok(tls_stream) => {
                // Slice 29: derive a Principal from the peer cert
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

/// Generic per-connection request loop. `S` is the post-handshake byte
/// stream — `TcpStream` for plaintext listeners, `tokio_rustls::server::TlsStream<TcpStream>`
/// for TLS listeners. `spec` carries the listener's protocol so the loop
/// can initialise `ConnectionAuth` correctly and gate pre-auth requests on
/// SASL listeners (Slice 12, T12).
#[allow(clippy::too_many_lines)] // each api_key intercept arm adds ~15 lines; T8/T9 will extract them.
async fn serve_connection_stream<S>(
    broker: std::sync::Arc<Broker>,
    stream: S,
    spec: crate::config::ListenerSpec,
    peer: SocketAddr,
    mtls_principal: Option<crabka_security::Principal>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut framed: Framed<S, _> = Framed::new(stream, codec::codec());
    let is_sasl_listener = spec.protocol.requires_sasl();
    let sasl_mechanisms = crate::network::listener::resolve_sasl_mechanisms_for_listener(
        &spec,
        &broker.config.enabled_sasl_mechanisms,
    )
    .to_owned();
    // Per-connection auth state. Mutated by the SASL handlers in T13/T14;
    // T12 only uses it to gate non-allowlisted api_keys before auth completes.
    // Slice 29: when an mTLS client cert was presented (verified by the TLS
    // layer against `client_ca_path`), the dispatch layer starts the
    // connection as Authenticated with the cert's Subject DN as the
    // principal name. SASL listeners ignore mTLS principals — Kafka's
    // SASL_SSL semantics require SASL to be the auth, even if a cert was
    // negotiated for transport.
    #[allow(unused_mut)] // T13/T14 mutate `auth` via SaslAuthenticate handlers.
    let mut auth = if is_sasl_listener {
        crate::network::auth::ConnectionAuth::Anonymous
    } else if let Some(principal) = mtls_principal {
        // Slice 49e: non-SASL connections carry an inert mechanism +
        // no-expiry; the in-band re-auth path is unreachable on these
        // listeners (handshake is only sent on SASL listeners).
        crate::network::auth::ConnectionAuth::Authenticated {
            principal,
            mechanism: crabka_security::SaslMechanism::Plain,
            expires_at_ms: None,
            // Slice 51: mTLS clients never auth via a delegation token.
            authenticated_via_token: false,
        }
    } else {
        // PLAINTEXT / SSL-without-cert: implicit anonymous, treated as
        // authenticated for gating purposes so the pre-auth allowlist
        // is a no-op.
        crate::network::auth::ConnectionAuth::Authenticated {
            principal: crabka_security::Principal {
                name: "ANONYMOUS".to_string(),
                auth_method: crabka_security::AuthMethod::Anonymous,
                groups: vec![],
            },
            mechanism: crabka_security::SaslMechanism::Plain,
            expires_at_ms: None,
            // Slice 51: anonymous never auths via a delegation token.
            authenticated_via_token: false,
        }
    };
    tracing::info!(listener = %spec.name, sasl = is_sasl_listener, "connection opened");

    loop {
        // Compute the re-auth deadline for OAUTHBEARER connections. PLAIN /
        // SCRAM / mTLS / anonymous return `None` and the timer arm is
        // effectively disabled via `std::future::pending()` inside
        // `sleep_until_some`. During `Reauthenticating`, the deadline stays
        // pinned to the `previous` session's `expires_at_ms` so a slow
        // in-band re-auth attempt cannot extend the session by sitting in
        // the in-progress state past the original expiry (KIP-368).
        let deadline: Option<tokio::time::Instant> = match &auth {
            crate::network::auth::ConnectionAuth::Authenticated {
                expires_at_ms: Some(exp_ms),
                ..
            } => Some(instant_at_epoch_ms(*exp_ms)),
            crate::network::auth::ConnectionAuth::Reauthenticating { previous, .. } => {
                previous.expires_at_ms.map(instant_at_epoch_ms)
            }
            _ => None,
        };

        // `biased;` ensures that if both `framed.next()` and the deadline
        // are ready in the same poll, the read arm wins — letting the last
        // in-flight request before expiry complete normally (KIP-368).
        let frame_result = tokio::select! {
            biased;
            next = framed.next() => next,
            () = sleep_until_some(deadline) => {
                tracing::info!(
                    principal = ?auth_principal_name(&auth),
                    "SASL session expired, closing connection (KIP-368)"
                );
                break;
            }
        };

        let frame = match frame_result {
            Some(Ok(b)) => b,
            Some(Err(e)) => {
                tracing::warn!(error = %e, "frame decode error, closing");
                break;
            }
            None => break, // EOF
        };
        // Per-request server span (slice 42). The `enabled!` guard keeps
        // this a single disabled-level check on a broker without OTLP —
        // only the OTLP layer turns on `REQUEST_TARGET` at DEBUG, so the
        // extra header parse below never runs in the common case. Each
        // handler `.await` is `.instrument`ed with this span so handler
        // events nest under it and export as one OTLP server span.
        let req_span = if tracing::enabled!(target: crate::telemetry::REQUEST_TARGET, tracing::Level::DEBUG)
        {
            match parse_request_header(&frame) {
                Ok((api_key, api_version, correlation_id, _body)) => {
                    crate::telemetry::request_span(
                        api_key,
                        api_version,
                        correlation_id,
                        peek_client_id(&frame),
                        &peer,
                    )
                }
                Err(_) => tracing::Span::none(),
            }
        } else {
            tracing::Span::none()
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
        // dispatch layer would require a switch over every api_key. T13
        // sends a *typed* SaslAuthenticate(36) response with error_code=58
        // on credential failure (its specific shape is known there). For
        // the generic pre-auth gate we close the TCP connection without
        // sending a body — JVM clients surface this to the caller as an
        // auth failure (closed connection during SASL), and this matches
        // the conservative behaviour we want for unauthenticated peers.
        if is_sasl_listener {
            match peek_api_key(&frame) {
                Ok(api_key) if !auth.allows_request(api_key) => {
                    tracing::info!(
                        api_key,
                        listener = %spec.name,
                        "request blocked by per-state auth gate (ILLEGAL_SASL_STATE), closing connection"
                    );
                    let _ = codes::ILLEGAL_SASL_STATE; // referenced for docs/grep
                    break;
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "frame too small to peek api_key, closing");
                    break;
                }
            }
        }
        // SASL frames (api_key 17 / 36) mutate the per-connection auth state,
        // which lives in this loop. They run *before* the regular handler
        // table because handlers receive only `&Broker` and have no way to
        // touch `auth`. Returning `Some(SaslFrameOutcome)` short-circuits
        // the normal dispatch_one() path for that frame.
        if let Some(outcome) = try_handle_sasl_frame(&broker, &frame, &mut auth, &sasl_mechanisms)
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
        // AlterUserScramCredentials (51) needs the connection's authenticated
        // principal and the peer `SocketAddr` so it can enforce the Cluster
        // Alter ACL gate (slice-13 T19, replacing the slice-12 super-user-name
        // equality check). The handler table signature passes only `&Broker`,
        // so this case is intercepted inline like the SASL frames are.
        // Returning `Some` short-circuits the normal `dispatch_one()` path
        // for this frame.
        // AlterReplicaLogDirs (34) needs the connection's authenticated
        // principal and the peer `SocketAddr` so it can enforce the
        // Cluster Alter ACL gate (KIP-113). Intercepted inline like
        // AlterUserScramCredentials (51).
        if peek_api_key(&frame).ok() == Some(34) {
            match handle_alter_replica_log_dirs_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during ARLD, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "ARLD dispatch error, closing connection");
                    break;
                }
            }
        }
        if peek_api_key(&frame).ok() == Some(51) {
            match handle_alter_user_scram_credentials_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during AUSCR, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "AUSCR dispatch error, closing connection");
                    break;
                }
            }
        }
        // Produce (0, slice-13 T10) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // batch-authorize every topic in the request for `Write` and
        // emit TOPIC_AUTHORIZATION_FAILED on the per-partition rows of
        // any topic that comes back `Deny`. The `&Broker`-only handler
        // table signature can't carry that context, so this api_key
        // intercepts inline.
        if peek_api_key(&frame).ok() == Some(0) {
            match handle_produce_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during Produce, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Produce dispatch error, closing connection");
                    break;
                }
            }
        }
        // Fetch (1, slice-13 T11) needs both the authenticated principal
        // AND the peer's `SocketAddr` so the handler can batch-authorize
        // every topic in the request for `Read` and emit
        // TOPIC_AUTHORIZATION_FAILED on the per-partition rows of any
        // topic that comes back `Deny`. The `&Broker`-only handler table
        // signature can't carry that context, so this api_key intercepts
        // inline.
        if peek_api_key(&frame).ok() == Some(1) {
            match handle_fetch_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during Fetch, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Fetch dispatch error, closing connection");
                    break;
                }
            }
        }
        // Metadata (3, slice-13 T12) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // batch-authorize every candidate topic for `Describe`.
        // Asymmetric behaviour: named-topic Deny surfaces
        // TOPIC_AUTHORIZATION_FAILED on the topic row; fetch-all Deny
        // silently omits the topic from the response. The
        // `&Broker`-only handler table signature can't carry the
        // principal+peer context, so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(3) {
            match handle_metadata_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during Metadata, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Metadata dispatch error, closing connection");
                    break;
                }
            }
        }
        // CreateTopics (19, slice-13 T13) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Create` on `Cluster("kafka-cluster")` and emit
        // CLUSTER_AUTHORIZATION_FAILED on every topic row on Deny. The
        // `&Broker`-only handler table signature can't carry that context,
        // so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(19) {
            match handle_create_topics_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during CreateTopics, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "CreateTopics dispatch error, closing connection");
                    break;
                }
            }
        }
        // DeleteTopics (20, slice-13 T13) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // batch-authorize every topic for `Delete` and emit
        // TOPIC_AUTHORIZATION_FAILED on denied topic rows. The
        // `&Broker`-only handler table signature can't carry that context,
        // so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(20) {
            match handle_delete_topics_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DeleteTopics, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DeleteTopics dispatch error, closing connection");
                    break;
                }
            }
        }
        // AlterConfigs (33, slice-13 T14) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `AlterConfigs` per resource: Topic resources check
        // against `ResourceType::Topic(resource_name)` and emit
        // TOPIC_AUTHORIZATION_FAILED on Deny; Broker resources check
        // against `Cluster("kafka-cluster")` and emit
        // CLUSTER_AUTHORIZATION_FAILED on Deny.
        if peek_api_key(&frame).ok() == Some(33) {
            match handle_alter_configs_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during AlterConfigs, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "AlterConfigs dispatch error, closing connection");
                    break;
                }
            }
        }
        // IncrementalAlterConfigs (44, slice-13 T14) — same shape as
        // AlterConfigs: needs both the authenticated principal and the peer
        // `SocketAddr` for per-resource ACL enforcement.
        if peek_api_key(&frame).ok() == Some(44) {
            match handle_incremental_alter_configs_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(
                            error = %e,
                            "framed.send error during IncrementalAlterConfigs, closing"
                        );
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "IncrementalAlterConfigs dispatch error, closing connection"
                    );
                    break;
                }
            }
        }
        // DeleteRecords (21, slice-13 T15) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // batch-authorize every topic in the request for `Delete` and emit
        // TOPIC_AUTHORIZATION_FAILED on the per-partition rows of any
        // topic that comes back `Deny`. The `&Broker`-only handler table
        // signature can't carry that context, so this api_key intercepts
        // inline.
        if peek_api_key(&frame).ok() == Some(21) {
            match handle_delete_records_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DeleteRecords, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DeleteRecords dispatch error, closing connection");
                    break;
                }
            }
        }
        // CreatePartitions (37, slice-13 T15) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // batch-authorize every topic in the request for `Alter` and emit
        // TOPIC_AUTHORIZATION_FAILED on the topic row of any topic that
        // comes back `Deny`. The `&Broker`-only handler table signature
        // can't carry that context, so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(37) {
            match handle_create_partitions_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during CreatePartitions, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "CreatePartitions dispatch error, closing connection");
                    break;
                }
            }
        }
        // DescribeGroups (15, slice-13 T16) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Describe` per group and emit
        // GROUP_AUTHORIZATION_FAILED on denied group rows. The
        // `&Broker`-only handler table signature can't carry that context,
        // so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(15) {
            match handle_describe_groups_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DescribeGroups, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DescribeGroups dispatch error, closing connection");
                    break;
                }
            }
        }
        // ListGroups (16, slice-13 T16) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // silently filter out groups denied `Describe`. The
        // `&Broker`-only handler table signature can't carry that context,
        // so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(16) {
            match handle_list_groups_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during ListGroups, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "ListGroups dispatch error, closing connection");
                    break;
                }
            }
        }
        // DeleteGroups (42, slice-13 T16) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Delete` per group and emit
        // GROUP_AUTHORIZATION_FAILED on denied group rows. The
        // `&Broker`-only handler table signature can't carry that context,
        // so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(42) {
            match handle_delete_groups_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DeleteGroups, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DeleteGroups dispatch error, closing connection");
                    break;
                }
            }
        }
        // JoinGroup (11, slice-13 T17) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Read` on `Group(group_id)` and emit
        // GROUP_AUTHORIZATION_FAILED on the whole response on Deny. The
        // `&Broker`-only handler table signature can't carry that context,
        // so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(11) {
            match handle_join_group_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during JoinGroup, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "JoinGroup dispatch error, closing connection");
                    break;
                }
            }
        }
        // OffsetCommit (8, slice-13 T18) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Read` on `Group(group_id)` (whole-response deny =
        // GROUP_AUTHORIZATION_FAILED) and then per-topic `Read` (per-partition
        // deny = TOPIC_AUTHORIZATION_FAILED). The `&Broker`-only handler
        // table signature can't carry that context, so this api_key
        // intercepts inline.
        if peek_api_key(&frame).ok() == Some(8) {
            match handle_offset_commit_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during OffsetCommit, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "OffsetCommit dispatch error, closing connection");
                    break;
                }
            }
        }
        // OffsetFetch (9, slice-13 T18) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Describe` on `Group(group_id)` (whole-response deny =
        // GROUP_AUTHORIZATION_FAILED) and then per-topic `Read` (per-topic
        // deny = TOPIC_AUTHORIZATION_FAILED). The `topics: None` fetch-all
        // sentinel also applies the per-topic check across committed-offsets
        // topics. The `&Broker`-only handler table signature can't carry that
        // context, so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(9) {
            match handle_offset_fetch_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during OffsetFetch, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "OffsetFetch dispatch error, closing connection");
                    break;
                }
            }
        }
        // OffsetDelete (47, KIP-496) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Delete` on `Group(group_id)` (whole-response deny =
        // GROUP_AUTHORIZATION_FAILED) and then per-topic `Read` (per-partition
        // deny = TOPIC_AUTHORIZATION_FAILED). The `&Broker`-only handler
        // table signature can't carry that context, so this api_key
        // intercepts inline.
        if peek_api_key(&frame).ok() == Some(47) {
            match handle_offset_delete_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during OffsetDelete, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "OffsetDelete dispatch error, closing connection");
                    break;
                }
            }
        }
        // DescribeCluster (60, slice-13 T19) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Describe` on `Cluster("kafka-cluster")` and emit
        // CLUSTER_AUTHORIZATION_FAILED on the whole response on Deny. The
        // `&Broker`-only handler table signature can't carry that context,
        // so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(60) {
            match handle_describe_cluster_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DescribeCluster, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DescribeCluster dispatch error, closing connection");
                    break;
                }
            }
        }
        // DescribeProducers (61, KIP-664) needs the principal + peer
        // for per-topic `Read` ACL evaluation. Intercepts inline with
        // the other principal-aware describe-style handlers.
        if peek_api_key(&frame).ok() == Some(61) {
            match handle_describe_producers_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DescribeProducers, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DescribeProducers dispatch error, closing connection");
                    break;
                }
            }
        }
        // DescribeTopicPartitions (75, KIP-966) needs the principal +
        // peer for per-topic `Describe` ACL evaluation, so it intercepts
        // inline alongside DescribeCluster / DescribeGroups.
        if peek_api_key(&frame).ok() == Some(75) {
            match handle_describe_topic_partitions_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DescribeTopicPartitions, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DescribeTopicPartitions dispatch error, closing connection");
                    break;
                }
            }
        }
        // DescribeAcls (29, slice-13 T7) needs both the authenticated
        // principal AND the peer's `SocketAddr` for host-based ACL
        // matching; neither is reachable from the `&Broker`-only handler
        // table signature, so it intercepts inline.
        if peek_api_key(&frame).ok() == Some(29) {
            match handle_describe_acls_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DescribeAcls, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DescribeAcls dispatch error, closing connection");
                    break;
                }
            }
        }
        // CreateAcls (30, slice-13 T8) — same shape as DescribeAcls: needs
        // both the authenticated principal and the peer `SocketAddr` for
        // host-based ACL matching on the `Alter` cluster gate.
        if peek_api_key(&frame).ok() == Some(30) {
            match handle_create_acls_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during CreateAcls, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "CreateAcls dispatch error, closing connection");
                    break;
                }
            }
        }
        // DeleteAcls (31, slice-13 T9) — same shape as CreateAcls: needs
        // both the authenticated principal and the peer `SocketAddr` for
        // host-based ACL matching on the `Alter` cluster gate.
        if peek_api_key(&frame).ok() == Some(31) {
            match handle_delete_acls_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DeleteAcls, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DeleteAcls dispatch error, closing connection");
                    break;
                }
            }
        }
        // ElectLeaders (43, slice-14 T5) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Alter` on `Cluster("kafka-cluster")` and emit
        // CLUSTER_AUTHORIZATION_FAILED on every per-partition row on Deny.
        // The `&Broker`-only handler table signature can't carry that
        // context, so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(43) {
            match handle_elect_leaders_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during ElectLeaders, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "ElectLeaders dispatch error, closing connection");
                    break;
                }
            }
        }
        // AlterPartitionReassignments (45, slice-15 T7) needs both the
        // authenticated principal AND the peer's `SocketAddr` so the handler
        // can authorize `Alter` on `Cluster("kafka-cluster")` and emit
        // CLUSTER_AUTHORIZATION_FAILED. The `&Broker`-only handler table
        // signature can't carry that context, so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(45) {
            match handle_alter_partition_reassignments_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during AlterPartitionReassignments, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "AlterPartitionReassignments dispatch error, closing connection");
                    break;
                }
            }
        }
        // ListPartitionReassignments (46, slice-15 T7) needs both the
        // authenticated principal AND the peer's `SocketAddr` so the handler
        // can authorize `Describe` on `Cluster("kafka-cluster")` and emit
        // CLUSTER_AUTHORIZATION_FAILED. The `&Broker`-only handler table
        // signature can't carry that context, so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(46) {
            match handle_list_partition_reassignments_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during ListPartitionReassignments, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "ListPartitionReassignments dispatch error, closing connection");
                    break;
                }
            }
        }
        // DescribeClientQuotas (48, slice-16 T7) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can authorize
        // `Describe` on `Cluster("kafka-cluster")` and emit
        // CLUSTER_AUTHORIZATION_FAILED. The `&Broker`-only handler table
        // signature can't carry that context, so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(48) {
            match handle_describe_client_quotas_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DescribeClientQuotas, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DescribeClientQuotas dispatch error, closing connection");
                    break;
                }
            }
        }
        // AlterClientQuotas (49, slice-16 T7) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can authorize
        // `Alter` on `Cluster("kafka-cluster")` and emit
        // CLUSTER_AUTHORIZATION_FAILED. The `&Broker`-only handler table
        // signature can't carry that context, so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(49) {
            match handle_alter_client_quotas_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during AlterClientQuotas, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "AlterClientQuotas dispatch error, closing connection");
                    break;
                }
            }
        }
        // DescribeUserScramCredentials (50, slice-17a T3) needs both the
        // authenticated principal AND the peer's `SocketAddr` so the handler
        // can authorize `Alter` on `Cluster("kafka-cluster")` and emit
        // CLUSTER_AUTHORIZATION_FAILED. The `&Broker`-only handler table
        // signature can't carry that context, so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(50) {
            match handle_describe_user_scram_credentials_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DescribeUserScramCredentials, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DescribeUserScramCredentials dispatch error, closing connection");
                    break;
                }
            }
        }
        // CreateDelegationToken (38, slice-51 T6) needs the broker's
        // delegation-token master HMAC key plus the per-connection
        // `auth` so the handler can enforce KIP-48's
        // "token-creating-token disallowed" rule via
        // `ConnectionAuth::Authenticated.authenticated_via_token`.
        // The `&Broker`-only handler table signature can't carry those
        // extra params, so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(38) {
            match handle_create_delegation_token_frame(&broker, &frame, &auth)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during CreateDelegationToken, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "CreateDelegationToken dispatch error, closing connection");
                    break;
                }
            }
        }
        // RenewDelegationToken (39, slice-51 T7) — needs the per-connection
        // `auth` so the handler can pull the calling principal for the
        // owner/renewer authorization check. Inline intercept matches
        // CreateDelegationToken (38) above.
        if peek_api_key(&frame).ok() == Some(39) {
            match handle_renew_delegation_token_frame(&broker, &frame, &auth)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during RenewDelegationToken, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "RenewDelegationToken dispatch error, closing connection");
                    break;
                }
            }
        }
        // ExpireDelegationToken (40, slice-51 T7) — same shape as Renew;
        // handler needs `auth` for the owner-or-renewer gate.
        if peek_api_key(&frame).ok() == Some(40) {
            match handle_expire_delegation_token_frame(&broker, &frame, &auth)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during ExpireDelegationToken, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "ExpireDelegationToken dispatch error, closing connection");
                    break;
                }
            }
        }
        // DescribeDelegationToken (41, slice-51 T6) — handler needs
        // `auth` for the visibility rules (token-authed callers see
        // only their own tokens; non-token callers see tokens they
        // own OR are listed as a renewer on).
        if peek_api_key(&frame).ok() == Some(41) {
            match handle_describe_delegation_token_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DescribeDelegationToken, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DescribeDelegationToken dispatch error, closing connection");
                    break;
                }
            }
        }
        // InitProducerId (22, slice-13 T20) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Write` on `TransactionalId` (transactional path) or
        // `IdempotentWrite` on `Cluster` (idempotent-only path). On Deny
        // the handler returns a whole-response error_code = 53 or 31.
        if peek_api_key(&frame).ok() == Some(22) {
            match handle_init_producer_id_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during InitProducerId, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "InitProducerId dispatch error, closing connection");
                    break;
                }
            }
        }
        // AddPartitionsToTxn (24, slice-13 T20) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Write` on `TransactionalId` (whole-txn deny =
        // TRANSACTIONAL_ID_AUTHORIZATION_FAILED) and per-topic `Write` on
        // `Topic` (per-row deny = TOPIC_AUTHORIZATION_FAILED).
        if peek_api_key(&frame).ok() == Some(24) {
            match handle_add_partitions_to_txn_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during AddPartitionsToTxn, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "AddPartitionsToTxn dispatch error, closing connection");
                    break;
                }
            }
        }
        // EndTxn (26, slice-13 T20) needs both the authenticated principal
        // AND the peer's `SocketAddr` so the handler can authorize
        // `Write` on `TransactionalId`. On Deny → whole-response
        // TRANSACTIONAL_ID_AUTHORIZATION_FAILED.
        if peek_api_key(&frame).ok() == Some(26) {
            match handle_end_txn_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during EndTxn, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "EndTxn dispatch error, closing connection");
                    break;
                }
            }
        }
        // TxnOffsetCommit (28, slice-13 T20) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Write` on `TransactionalId` + `Read` on `Group` +
        // per-topic `Read` on `Topic`.
        if peek_api_key(&frame).ok() == Some(28) {
            match handle_txn_offset_commit_frame(&broker, &frame, &auth, &peer)
                .instrument(req_span.clone())
                .await
            {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during TxnOffsetCommit, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "TxnOffsetCommit dispatch error, closing connection");
                    break;
                }
            }
        }
        // KIP-124 request_percentage enforcement — fallback HandlerTable path only.
        // Intercept arms (admin RPCs: ACLs, ElectLeaders, AlterPartitionReassignments,
        // ListPartitionReassignments, AlterClientQuotas, DescribeClientQuotas, etc.)
        // handle their own response write inline and are NOT subject to
        // request_percentage throttling here. Admin RPCs are low-frequency operator
        // traffic; the exemption is documented in STATUS.md.
        let started = std::time::Instant::now();
        let response_bytes = match dispatch_one(&broker, &frame)
            .instrument(req_span.clone())
            .await
        {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "dispatch error, closing connection");
                break;
            }
        };
        // Saturate at u64::MAX (≈580k years) rather than panicking on
        // extraordinarily long requests.
        #[allow(clippy::cast_possible_truncation)]
        let elapsed_micros = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;

        // Consume elapsed CPU time from the request_percentage bucket and
        // sleep before writing the response (server-side throttle only;
        // throttle_time_ms in the response is populated by Produce/Fetch
        // handlers — T9/T10 — not here).
        if let Some(principal) = auth.principal() {
            let principal_name = &principal.name;
            let client_id_str = peek_client_id(&frame).unwrap_or("");
            let image = broker.controller.current_image();
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            if let Some((entity_key, rate_pct)) = crate::quota::lookup_quota_with_key(
                &image,
                principal_name,
                client_id_str,
                "request_percentage",
            ) && rate_pct > 0.0
            {
                let rate_micros_per_sec = (rate_pct * 10_000.0) as u64;
                if rate_micros_per_sec > 0 {
                    let bucket = broker.quota_buckets.get_or_create(
                        "request_percentage",
                        &entity_key,
                        rate_micros_per_sec,
                    );
                    let granted = bucket.try_consume(elapsed_micros);
                    if granted < elapsed_micros {
                        let overage_micros = elapsed_micros - granted;
                        let delay_micros =
                            overage_micros.saturating_mul(1_000_000) / rate_micros_per_sec;
                        let delay = std::time::Duration::from_micros(delay_micros)
                            .min(std::time::Duration::from_secs(1));
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        if let Err(e) = framed.send(response_bytes).await {
            tracing::warn!(error = %e, "framed.send error, closing");
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
/// regular handler-table dispatch in [`dispatch_one`].
///
/// Errors here close the connection (protocol violations, e.g. an
/// undecodable header).
async fn try_handle_sasl_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &mut crate::network::auth::ConnectionAuth,
    sasl_mechanisms: &[crabka_security::SaslMechanism],
) -> Option<Result<SaslFrameOutcome, BrokerError>> {
    let api_key = peek_api_key(frame).ok()?;
    if api_key != 17 && api_key != 36 {
        return None;
    }
    Some(handle_sasl_frame(broker, frame, auth, api_key, sasl_mechanisms).await)
}

async fn handle_sasl_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &mut crate::network::auth::ConnectionAuth,
    api_key: i16,
    sasl_mechanisms: &[crabka_security::SaslMechanism],
) -> Result<SaslFrameOutcome, BrokerError> {
    use crabka_protocol::{Decode, Encode};

    let (parsed_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(parsed_key, api_key);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let (resp_body, close_after) = match api_key {
        17 => {
            let mut cur: &[u8] = body;
            let req = crabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest::decode(
                &mut cur,
                api_version,
            )?;
            let resp = crate::network::auth::handle_handshake(&req, auth, sasl_mechanisms);
            let mut buf = BytesMut::with_capacity(resp.encoded_len(api_version));
            resp.encode(&mut buf, api_version)?;
            (buf.freeze(), false)
        }
        36 => {
            let mut cur: &[u8] = body;
            let req =
                crabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest::decode(
                    &mut cur,
                    api_version,
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
                            &broker.controller,
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
            let close = resp.error_code != 0;
            let mut buf = BytesMut::with_capacity(resp.encoded_len(api_version));
            resp.encode(&mut buf, api_version)?;
            (buf.freeze(), close)
        }
        _ => unreachable!("filtered by caller to 17 / 36 only"),
    };

    let response_bytes = encode_response(api_key, correlation_id, body_flexible, &resp_body);
    Ok(SaslFrameOutcome {
        response_bytes,
        close_after,
    })
}

/// Decode + dispatch an `AlterReplicaLogDirs` (`api_key` 34) frame.
/// Pulls the authenticated principal + peer `SocketAddr` off the
/// connection so the handler can enforce KIP-113's Cluster Alter ACL
/// gate. On Deny, every `(topic, partition)` listed in the request
/// receives `CLUSTER_AUTHORIZATION_FAILED` in the response.
async fn handle_alter_replica_log_dirs_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use std::collections::BTreeMap;

    use crabka_protocol::owned::alter_replica_log_dirs_request::AlterReplicaLogDirsRequest;
    use crabka_protocol::owned::alter_replica_log_dirs_response::{
        AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
        AlterReplicaLogDirsResponse,
    };
    use crabka_protocol::{Decode, Encode};

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 34);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });

    let image = broker.controller.current_image();
    let authorized = broker.config.authorizer.authorize(
        &image,
        &crate::authorizer::AuthorizationRequest {
            principal: &principal,
            host: peer,
            resource_type: crabka_metadata::ResourceType::Cluster,
            resource_name: "kafka-cluster",
            operation: crabka_metadata::AclOperation::Alter,
        },
    ) == crate::authorizer::AuthorizationResult::Allow;

    if !authorized {
        // Stamp CLUSTER_AUTHORIZATION_FAILED on every partition the
        // client listed and skip the move machinery entirely.
        let mut cur: &[u8] = body;
        let req = AlterReplicaLogDirsRequest::decode(&mut cur, api_version)?;
        let mut by_topic: BTreeMap<String, Vec<AlterReplicaLogDirPartitionResult>> =
            BTreeMap::new();
        for dir in req.dirs {
            for topic in dir.topics {
                for partition_index in topic.partitions {
                    by_topic.entry(topic.name.clone()).or_default().push(
                        AlterReplicaLogDirPartitionResult {
                            partition_index,
                            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                            ..Default::default()
                        },
                    );
                }
            }
        }
        let results: Vec<_> = by_topic
            .into_iter()
            .map(|(name, partitions)| AlterReplicaLogDirTopicResult {
                topic_name: name,
                partitions,
                ..Default::default()
            })
            .collect();
        let resp = AlterReplicaLogDirsResponse {
            throttle_time_ms: 0,
            results,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(api_version));
        resp.encode(&mut buf, api_version)?;
        return Ok(encode_response(
            api_key,
            correlation_id,
            body_flexible,
            &buf.freeze(),
        ));
    }

    let resp_body =
        crate::handlers::alter_replica_log_dirs::handle(broker, api_version, correlation_id, body)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `AlterUserScramCredentials` (`api_key` 51) frame.
/// Pulls the authenticated principal off the per-connection `auth` state and
/// the peer `SocketAddr` from the accept-time capture so the handler can
/// enforce the Cluster Alter ACL gate (slice-13 T19). On a SASL listener the
/// pre-auth allowlist already rejects this frame; on PLAINTEXT/SSL listeners
/// the connection is implicitly `Authenticated { ANONYMOUS / Plain }` (see
/// the loop init), so `principal()` always returns `Some` here.
async fn handle_alter_user_scram_credentials_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::{Decode, Encode};

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 51);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req =
        crabka_protocol::owned::alter_user_scram_credentials_request::AlterUserScramCredentialsRequest::decode(
            &mut cur, api_version,
        )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp = crate::handlers::alter_user_scram_credentials::handle(broker, req, &ctx).await;
    let mut buf = BytesMut::with_capacity(resp.encoded_len(api_version));
    resp.encode(&mut buf, api_version)?;
    let resp_body = buf.freeze();
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DescribeCluster` (`api_key` 60) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Describe` on `Cluster("kafka-cluster")` (slice-13 T19).
/// On Deny the whole response receives `CLUSTER_AUTHORIZATION_FAILED`.
async fn handle_describe_cluster_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 60);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::describe_cluster::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DescribeProducers` (`api_key` 61, KIP-664) frame.
/// The handler needs the authenticated principal and peer `SocketAddr`
/// for per-topic `Read` ACL evaluation, which the `&Broker`-only handler
/// table signature can't carry; this helper builds the
/// [`crate::handlers::RequestContext`] and forwards.
async fn handle_describe_producers_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 61);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body = crate::handlers::describe_producers::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &ctx,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DescribeTopicPartitions` (`api_key` 75, KIP-966)
/// frame. Mirrors `handle_describe_cluster_frame`'s shape; the handler
/// needs the authenticated principal and peer `SocketAddr` for per-topic
/// `Describe` ACL evaluation, which the `&Broker`-only handler table
/// can't carry.
async fn handle_describe_topic_partitions_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 75);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body = crate::handlers::describe_topic_partitions::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &ctx,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `Produce` (`api_key` 0) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// batch-authorize every topic in the request for `Write` (slice-13 T10).
/// On PLAINTEXT/SSL listeners the connection is implicitly
/// `Authenticated { ANONYMOUS / Plain }` (see the loop init), so
/// `principal()` always returns `Some` here; the `unwrap_or_else`
/// fallback covers the defensive SASL pre-auth case.
async fn handle_produce_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 0);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::produce::handle(broker, api_version, correlation_id, body, &ctx).await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `Fetch` (`api_key` 1) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// batch-authorize every topic in the request for `Read` (slice-13 T11).
/// On PLAINTEXT/SSL listeners the connection is implicitly
/// `Authenticated { ANONYMOUS / Plain }` (see the loop init), so
/// `principal()` always returns `Some` here; the `unwrap_or_else`
/// fallback covers the defensive SASL pre-auth case.
async fn handle_fetch_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 1);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::fetch::handle(broker, api_version, correlation_id, body, &ctx).await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `Metadata` (`api_key` 3) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// batch-authorize every candidate topic for `Describe` (slice-13 T12).
/// Named-topic Deny surfaces `TOPIC_AUTHORIZATION_FAILED`; fetch-all Deny
/// silently omits the topic. On PLAINTEXT/SSL listeners the connection
/// is implicitly `Authenticated { ANONYMOUS / Plain }` (see the loop
/// init), so `principal()` always returns `Some` here; the
/// `unwrap_or_else` fallback covers the defensive SASL pre-auth case.
async fn handle_metadata_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 3);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::metadata::handle(broker, api_version, correlation_id, body, &ctx).await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `CreateTopics` (`api_key` 19) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Create` on `Cluster("kafka-cluster")` (slice-13 T13).
/// On PLAINTEXT/SSL listeners the connection is implicitly
/// `Authenticated { ANONYMOUS / Plain }` (see the loop init), so
/// `principal()` always returns `Some` here; the `unwrap_or_else`
/// fallback covers the defensive SASL pre-auth case.
async fn handle_create_topics_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 19);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::create_topics::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DeleteTopics` (`api_key` 20) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// batch-authorize every topic for `Delete` (slice-13 T13).
/// On PLAINTEXT/SSL listeners the connection is implicitly
/// `Authenticated { ANONYMOUS / Plain }` (see the loop init), so
/// `principal()` always returns `Some` here; the `unwrap_or_else`
/// fallback covers the defensive SASL pre-auth case.
async fn handle_delete_topics_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 20);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::delete_topics::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DescribeAcls` (`api_key` 29) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Describe` on `Cluster` with host-based ACL matching.
/// On PLAINTEXT/SSL listeners the connection is implicitly
/// `Authenticated { ANONYMOUS / Plain }` (see the loop init), so
/// `principal()` always returns `Some` here; the `unwrap_or_else`
/// fallback covers the defensive SASL pre-auth case.
async fn handle_describe_acls_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::Decode;

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 29);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::describe_acls_request::DescribeAclsRequest::decode(
        &mut cur,
        api_version,
    )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body = crate::handlers::describe_acls::handle(broker, req, &ctx, api_version).await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `CreateAcls` (`api_key` 30) frame. Mirrors
/// [`handle_describe_acls_frame`] — pulls the authenticated principal
/// off the per-connection `auth` state and the peer `SocketAddr` from
/// the accept-time capture so the handler can authorize `Alter` on
/// `Cluster` with host-based ACL matching.
async fn handle_create_acls_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::Decode;

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 30);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::create_acls_request::CreateAclsRequest::decode(
        &mut cur,
        api_version,
    )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body = crate::handlers::create_acls::handle(broker, req, &ctx, api_version).await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DeleteAcls` (`api_key` 31) frame. Mirrors
/// [`handle_create_acls_frame`] — pulls the authenticated principal
/// off the per-connection `auth` state and the peer `SocketAddr` from
/// the accept-time capture so the handler can authorize `Alter` on
/// `Cluster` with host-based ACL matching.
async fn handle_delete_acls_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::Decode;

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 31);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::delete_acls_request::DeleteAclsRequest::decode(
        &mut cur,
        api_version,
    )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body = crate::handlers::delete_acls::handle(broker, req, &ctx, api_version).await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `ElectLeaders` (`api_key` 43) frame. Mirrors
/// [`handle_delete_acls_frame`] — pulls the authenticated principal off the
/// per-connection `auth` state and the peer `SocketAddr` from the
/// accept-time capture so the handler can authorize `Alter` on
/// `Cluster("kafka-cluster")` with host-based ACL matching.
async fn handle_elect_leaders_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::Decode;

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 43);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::elect_leaders_request::ElectLeadersRequest::decode(
        &mut cur,
        api_version,
    )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body = crate::handlers::elect_leaders::handle(broker, req, &ctx, api_version).await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `AlterPartitionReassignments` (`api_key` 45) frame.
/// Mirrors [`handle_elect_leaders_frame`] — pulls the authenticated principal
/// off the per-connection `auth` state and the peer `SocketAddr` from the
/// accept-time capture so the handler can authorize `Alter` on
/// `Cluster("kafka-cluster")` with host-based ACL matching.
async fn handle_alter_partition_reassignments_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::Decode;

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 45);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::alter_partition_reassignments_request::AlterPartitionReassignmentsRequest::decode(
        &mut cur,
        api_version,
    )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::alter_partition_reassignments::handle(broker, req, &ctx, api_version)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `ListPartitionReassignments` (`api_key` 46) frame.
/// Mirrors [`handle_elect_leaders_frame`] — pulls the authenticated principal
/// off the per-connection `auth` state and the peer `SocketAddr` from the
/// accept-time capture so the handler can authorize `Describe` on
/// `Cluster("kafka-cluster")` with host-based ACL matching.
async fn handle_list_partition_reassignments_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::Decode;

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 46);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::list_partition_reassignments_request::ListPartitionReassignmentsRequest::decode(
        &mut cur,
        api_version,
    )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::list_partition_reassignments::handle(broker, req, &ctx, api_version)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DescribeClientQuotas` (`api_key` 48) frame.
/// Mirrors [`handle_alter_partition_reassignments_frame`] — pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Describe` on `Cluster("kafka-cluster")` with host-based
/// ACL matching.
async fn handle_describe_client_quotas_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::Decode;

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 48);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::describe_client_quotas_request::DescribeClientQuotasRequest::decode(
        &mut cur,
        api_version,
    )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::describe_client_quotas::handle(broker, req, &ctx, api_version).await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `AlterClientQuotas` (`api_key` 49) frame.
/// Mirrors [`handle_alter_partition_reassignments_frame`] — pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Alter` on `Cluster("kafka-cluster")` with host-based
/// ACL matching.
async fn handle_alter_client_quotas_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::Decode;

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 49);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req =
        crabka_protocol::owned::alter_client_quotas_request::AlterClientQuotasRequest::decode(
            &mut cur,
            api_version,
        )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::alter_client_quotas::handle(broker, req, &ctx, api_version).await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DescribeUserScramCredentials` (`api_key` 50) frame.
/// Mirrors [`handle_describe_client_quotas_frame`] — pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Alter` on `Cluster("kafka-cluster")` with host-based
/// ACL matching.
async fn handle_describe_user_scram_credentials_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::Decode;

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 50);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::describe_user_scram_credentials_request::DescribeUserScramCredentialsRequest::decode(
        &mut cur,
        api_version,
    )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::describe_user_scram_credentials::handle(broker, req, &ctx, api_version)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `CreateDelegationToken` (`api_key` 38) frame
/// (slice-51 T6). Threads the per-connection `auth` to the handler so
/// it can enforce the KIP-48 token-creating-token rule (via
/// `ConnectionAuth::Authenticated.authenticated_via_token`), passes
/// the broker's master HMAC key, the configured maximum-lifetime
/// ceiling (used to clamp the caller's `max_lifetime_ms`), and the
/// controller handle for appending the resulting metadata record.
async fn handle_create_delegation_token_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::{Decode, Encode};

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 38);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::create_delegation_token_request::CreateDelegationTokenRequest::decode(
        &mut cur, api_version,
    )?;

    let resp = crate::handlers::create_delegation_token::handle(
        &req,
        auth,
        broker.config.delegation_token_secret_key.as_ref(),
        broker.config.delegation_token_max_lifetime_ms,
        broker.config.delegation_token_default_renew_period_ms,
        &broker.controller,
        &broker.config.super_users,
    )
    .await;
    let mut buf = BytesMut::with_capacity(resp.encoded_len(api_version));
    resp.encode(&mut buf, api_version)?;
    let resp_body = buf.freeze();
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `RenewDelegationToken` (`api_key` 39) frame
/// (slice-51 T7). Threads the per-connection `auth` to the handler so it
/// can enforce the KIP-48 owner-or-renewer check (slice 51c added the
/// super-user bypass so the operator can renew tokens it minted via
/// act-as), and passes the configured default renew period (Kafka's
/// `delegation.token.expiry.time.ms`, 24h by default) as the fallback
/// used when the request specifies `renew_period_ms == -1` (spec §1.3
/// step 4).
async fn handle_renew_delegation_token_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::{Decode, Encode};

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 39);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::renew_delegation_token_request::RenewDelegationTokenRequest::decode(
        &mut cur, api_version,
    )?;

    let resp = crate::handlers::renew_delegation_token::handle(
        &req,
        auth,
        broker.config.delegation_token_secret_key.as_ref(),
        broker.config.delegation_token_default_renew_period_ms,
        &broker.controller,
        &broker.config.super_users,
    )
    .await;
    let mut buf = BytesMut::with_capacity(resp.encoded_len(api_version));
    resp.encode(&mut buf, api_version)?;
    let resp_body = buf.freeze();
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `ExpireDelegationToken` (`api_key` 40) frame
/// (slice-51 T7). Threads the per-connection `auth` to the handler so
/// it can enforce the KIP-48 owner-or-renewer check (slice 51c added
/// the super-user bypass so the operator's finalizer can tombstone
/// tokens it minted via act-as).
async fn handle_expire_delegation_token_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::{Decode, Encode};

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 40);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::expire_delegation_token_request::ExpireDelegationTokenRequest::decode(
        &mut cur, api_version,
    )?;

    let resp = crate::handlers::expire_delegation_token::handle(
        &req,
        auth,
        broker.config.delegation_token_secret_key.as_ref(),
        &broker.controller,
        &broker.config.super_users,
    )
    .await;
    let mut buf = BytesMut::with_capacity(resp.encoded_len(api_version));
    resp.encode(&mut buf, api_version)?;
    let resp_body = buf.freeze();
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DescribeDelegationToken` (`api_key` 41) frame
/// (slice-51 T6). Threads the per-connection `auth` so the handler can
/// apply KIP-48 visibility rules (token-authed callers see only their
/// own tokens; non-token callers see owner-or-renewer tokens), and the
/// controller handle so it can read the live image.
async fn handle_describe_delegation_token_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::{Decode, Encode};

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 41);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::describe_delegation_token_request::DescribeDelegationTokenRequest::decode(
        &mut cur, api_version,
    )?;

    let resp = crate::handlers::describe_delegation_token::handle(
        &req,
        auth,
        broker.config.delegation_token_secret_key.as_ref(),
        &broker.controller,
        peer,
        broker.config.authorizer.as_ref(),
    )
    .await;
    let mut buf = BytesMut::with_capacity(resp.encoded_len(api_version));
    resp.encode(&mut buf, api_version)?;
    let resp_body = buf.freeze();
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `AlterConfigs` (`api_key` 33) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `AlterConfigs` per resource (slice-13 T14).
/// Topic resources → `TOPIC_AUTHORIZATION_FAILED` on Deny.
/// Broker resources → `CLUSTER_AUTHORIZATION_FAILED` on Deny.
async fn handle_alter_configs_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 33);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::alter_configs::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `IncrementalAlterConfigs` (`api_key` 44) frame.
/// Pulls the authenticated principal off the per-connection `auth` state
/// and the peer `SocketAddr` from the accept-time capture so the handler
/// can authorize `AlterConfigs` per resource (slice-13 T14).
/// Topic resources → `TOPIC_AUTHORIZATION_FAILED` on Deny.
/// Broker resources → `CLUSTER_AUTHORIZATION_FAILED` on Deny.
async fn handle_incremental_alter_configs_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 44);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body = crate::handlers::incremental_alter_configs::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &ctx,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DeleteRecords` (`api_key` 21) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// batch-authorize every topic in the request for `Delete` (slice-13 T15).
/// Topics that come back `Deny` have `TOPIC_AUTHORIZATION_FAILED` set on
/// every partition row.
async fn handle_delete_records_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 21);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::delete_records::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `CreatePartitions` (`api_key` 37) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// batch-authorize every topic in the request for `Alter` (slice-13 T15).
/// Topics that come back `Deny` receive `TOPIC_AUTHORIZATION_FAILED` on
/// that topic row.
async fn handle_create_partitions_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 37);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::create_partitions::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DescribeGroups` (`api_key` 15) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Describe` per group (slice-13 T16). Denied groups receive
/// `GROUP_AUTHORIZATION_FAILED` on their per-group entry.
async fn handle_describe_groups_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 15);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::describe_groups::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `ListGroups` (`api_key` 16) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// silently filter out groups denied `Describe` (slice-13 T16). Denied
/// groups are omitted from the response without an error code.
async fn handle_list_groups_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 16);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::list_groups::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DeleteGroups` (`api_key` 42) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Delete` per group (slice-13 T16). Denied groups receive
/// `GROUP_AUTHORIZATION_FAILED` on their per-group entry.
async fn handle_delete_groups_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 42);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::delete_groups::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `JoinGroup` (`api_key` 11) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Read` on `Group(group_id)` (slice-13 T17). On Deny the
/// whole response receives `GROUP_AUTHORIZATION_FAILED`.
async fn handle_join_group_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 11);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::join_group::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `OffsetCommit` (`api_key` 8) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Read` on `Group(group_id)` (whole-response deny =
/// `GROUP_AUTHORIZATION_FAILED`) and per-topic `Read` (per-partition deny =
/// `TOPIC_AUTHORIZATION_FAILED`) (slice-13 T18).
async fn handle_offset_commit_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 8);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::offset_commit::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `OffsetFetch` (`api_key` 9) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Describe` on `Group(group_id)` (whole-response deny =
/// `GROUP_AUTHORIZATION_FAILED`) and per-topic `Read` (per-topic deny =
/// `TOPIC_AUTHORIZATION_FAILED`). The `topics: None` fetch-all sentinel runs
/// the per-topic check across discovered committed-offsets topics (slice-13 T18).
async fn handle_offset_fetch_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 9);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::offset_fetch::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `OffsetDelete` (`api_key` 47, KIP-496) frame.
/// Pulls the authenticated principal off the per-connection `auth` state
/// and the peer `SocketAddr` from the accept-time capture so the handler
/// can authorize `Delete` on `Group(group_id)` (whole-response deny =
/// `GROUP_AUTHORIZATION_FAILED`) and then per-topic `Read` (per-partition
/// deny = `TOPIC_AUTHORIZATION_FAILED`).
async fn handle_offset_delete_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 47);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::offset_delete::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `InitProducerId` (`api_key` 22) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Write` on `TransactionalId(transactional_id)` (transactional
/// path) or `IdempotentWrite` on `Cluster("kafka-cluster")` (idempotent-only
/// path) (slice-13 T20).
async fn handle_init_producer_id_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 22);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::handlers::init_producer_id::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `AddPartitionsToTxn` (`api_key` 24) frame. Pulls
/// the authenticated principal off the per-connection `auth` state and
/// the peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Write` on `TransactionalId` AND per-topic `Write` on
/// `Topic` (slice-13 T20).
async fn handle_add_partitions_to_txn_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 24);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body = crate::txn::handlers::add_partitions_to_txn::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &ctx,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `EndTxn` (`api_key` 26) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Write` on `TransactionalId` (slice-13 T20).
async fn handle_end_txn_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 26);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body =
        crate::txn::handlers::end_txn::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `TxnOffsetCommit` (`api_key` 28) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Write` on `TransactionalId` + `Read` on `Group` +
/// per-topic `Read` on `Topic` (slice-13 T20).
async fn handle_txn_offset_commit_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 28);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body = crate::txn::handlers::txn_offset_commit::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &ctx,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Read just the `api_key` (first 2 bytes) of a request frame without
/// otherwise consuming or validating it. Used by the pre-auth gate so we
/// can decide whether to even dispatch the frame.
fn peek_api_key(frame: &[u8]) -> Result<i16, BrokerError> {
    if frame.len() < 2 {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue(
                "request frame: too short to peek api_key",
            ),
        ));
    }
    Ok(i16::from_be_bytes([frame[0], frame[1]]))
}

/// Extract the `client_id` string from a request frame without fully parsing
/// it. Frame layout: `api_key(2) + api_version(2) + correlation_id(4) +
/// client_id_len(2) + client_id(n)`. Returns `None` if the frame is too short
/// or the `client_id` field is null (len == -1). Used by the
/// `request_percentage` quota enforcement path.
fn peek_client_id(frame: &[u8]) -> Option<&str> {
    // Minimum: api_key(2) + api_version(2) + corr_id(4) + cid_len(2) = 10 bytes.
    if frame.len() < 10 {
        return None;
    }
    let cid_len = i16::from_be_bytes([frame[8], frame[9]]);
    if cid_len <= 0 {
        // null (−1) or empty (0)
        return None;
    }
    // cid_len > 0 here, so the cast to usize is safe.
    #[allow(clippy::cast_sign_loss)]
    let n = cid_len as usize;
    let start = 10usize;
    let end = start.checked_add(n)?;
    if frame.len() < end {
        return None;
    }
    std::str::from_utf8(&frame[start..end]).ok()
}

/// Decode one request from the framed bytes, call the handler, build a
/// response with the right `ResponseHeader` version, return the bytes
/// ready for `framed.send` (which prepends the i32 length).
///
/// Errors here close the connection — they're protocol violations.
async fn dispatch_one(broker: &Broker, frame: &[u8]) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    let body_flexible = handler_body_flexible(api_key, api_version);
    tracing::info!(
        api_key,
        api_version,
        correlation_id,
        body_flexible,
        body_len = body.len(),
        "dispatching request"
    );

    let handler = broker
        .handlers()
        .get(api_key)
        .ok_or(BrokerError::UnsupportedApi {
            api_key,
            version: api_version,
        });

    let resp_body: Bytes = if let Ok(h) = handler {
        h(broker, api_version, correlation_id, body).await?
    } else {
        tracing::warn!(api_key, api_version, "unsupported api, returning error");
        // Build a synthetic UNSUPPORTED_VERSION response: just a 2-byte
        // error code + an empty body. Most Kafka responses begin with
        // `error_code: i16` at offset 0; clients that don't expect
        // this for some api_keys will close anyway.
        let mut buf = BytesMut::with_capacity(2);
        buf.put_i16(codes::UNSUPPORTED_VERSION);
        buf.freeze()
    };

    let out = encode_response(api_key, correlation_id, body_flexible, &resp_body);
    tracing::info!(
        api_key,
        api_version,
        correlation_id,
        resp_len = out.len(),
        "response built"
    );
    Ok(out)
}

/// Parse `RequestHeader` and return `(api_key, version, corr_id, &body)`.
fn parse_request_header(frame: &[u8]) -> Result<(i16, i16, i32, &[u8]), BrokerError> {
    if frame.len() < 8 {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("request frame < 8 bytes"),
        ));
    }
    let mut cur = frame;
    let api_key = cur.get_i16();
    let api_version = cur.get_i16();
    let correlation_id = cur.get_i32();

    let body_flexible = handler_body_flexible(api_key, api_version);
    let header_v2 = body_flexible;

    // client_id: NULLABLE_STRING (i16 length) in BOTH header versions.
    if cur.remaining() < 2 {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("request frame: missing client_id length"),
        ));
    }
    let cid_len = cur.get_i16();
    if cid_len > 0 {
        let n = usize::try_from(cid_len).expect("non-negative i16 fits usize");
        if cur.remaining() < n {
            return Err(BrokerError::Protocol(
                crabka_protocol::ProtocolError::InvalidValue(
                    "request frame: client_id length > available",
                ),
            ));
        }
        cur.advance(n);
    }
    if header_v2 {
        if cur.remaining() < 1 {
            return Err(BrokerError::Protocol(
                crabka_protocol::ProtocolError::InvalidValue(
                    "request frame: missing header tagged-fields byte",
                ),
            ));
        }
        // For the MVP we don't surface unknown header-level tagged fields.
        // Consume one UVARINT = 0 (empty). If non-zero, log + ignore.
        let tagged = cur.get_u8();
        if tagged != 0 {
            tracing::debug!(
                api_key,
                api_version,
                "non-empty header tagged fields ignored"
            );
        }
    }
    Ok((api_key, api_version, correlation_id, cur))
}

/// Returns whether the request *body* (and therefore the response body)
/// is flexible for this `(api_key, version)`. Mirrors
/// `crabka_protocol::owned::*::FLEXIBLE_MIN`.
///
/// For the handful of APIs the MVP supports, this is a small static table;
/// keep it next to the handler registry so adding a new handler updates one
/// place.
fn handler_body_flexible(api_key: i16, version: i16) -> bool {
    use crabka_protocol::owned;
    match api_key {
        0 => version >= owned::produce_request::FLEXIBLE_MIN,
        1 => version >= owned::fetch_request::FLEXIBLE_MIN,
        2 => version >= owned::list_offsets_request::FLEXIBLE_MIN,
        3 => version >= owned::metadata_request::FLEXIBLE_MIN,
        8 => version >= owned::offset_commit_request::FLEXIBLE_MIN,
        9 => version >= owned::offset_fetch_request::FLEXIBLE_MIN,
        10 => version >= owned::find_coordinator_request::FLEXIBLE_MIN,
        11 => version >= owned::join_group_request::FLEXIBLE_MIN,
        12 => version >= owned::heartbeat_request::FLEXIBLE_MIN,
        13 => version >= owned::leave_group_request::FLEXIBLE_MIN,
        14 => version >= owned::sync_group_request::FLEXIBLE_MIN,
        15 => version >= owned::describe_groups_request::FLEXIBLE_MIN,
        16 => version >= owned::list_groups_request::FLEXIBLE_MIN,
        // SaslHandshake (17) is permanently non-flexible (its
        // `FLEXIBLE_MIN` is `i16::MAX` in the upstream schema); covered
        // by the `_ => false` arm below.
        18 => version >= owned::api_versions_request::FLEXIBLE_MIN,
        19 => version >= owned::create_topics_request::FLEXIBLE_MIN,
        20 => version >= owned::delete_topics_request::FLEXIBLE_MIN,
        21 => version >= owned::delete_records_request::FLEXIBLE_MIN,
        22 => version >= owned::init_producer_id_request::FLEXIBLE_MIN,
        23 => version >= owned::offset_for_leader_epoch_request::FLEXIBLE_MIN,
        24 => version >= owned::add_partitions_to_txn_request::FLEXIBLE_MIN,
        25 => version >= owned::add_offsets_to_txn_request::FLEXIBLE_MIN,
        26 => version >= owned::end_txn_request::FLEXIBLE_MIN,
        27 => version >= owned::write_txn_markers_request::FLEXIBLE_MIN,
        28 => version >= owned::txn_offset_commit_request::FLEXIBLE_MIN,
        29 => version >= owned::describe_acls_request::FLEXIBLE_MIN,
        30 => version >= owned::create_acls_request::FLEXIBLE_MIN,
        31 => version >= owned::delete_acls_request::FLEXIBLE_MIN,
        32 => version >= owned::describe_configs_request::FLEXIBLE_MIN,
        33 => version >= owned::alter_configs_request::FLEXIBLE_MIN,
        34 => version >= owned::alter_replica_log_dirs_request::FLEXIBLE_MIN,
        35 => version >= owned::describe_log_dirs_request::FLEXIBLE_MIN,
        36 => version >= owned::sasl_authenticate_request::FLEXIBLE_MIN,
        37 => version >= owned::create_partitions_request::FLEXIBLE_MIN,
        38 => version >= owned::create_delegation_token_request::FLEXIBLE_MIN,
        39 => version >= owned::renew_delegation_token_request::FLEXIBLE_MIN,
        40 => version >= owned::expire_delegation_token_request::FLEXIBLE_MIN,
        41 => version >= owned::describe_delegation_token_request::FLEXIBLE_MIN,
        42 => version >= owned::delete_groups_request::FLEXIBLE_MIN,
        43 => version >= owned::elect_leaders_request::FLEXIBLE_MIN,
        44 => version >= owned::incremental_alter_configs_request::FLEXIBLE_MIN,
        45 => version >= owned::alter_partition_reassignments_request::FLEXIBLE_MIN,
        46 => version >= owned::list_partition_reassignments_request::FLEXIBLE_MIN,
        // 47 (OffsetDelete, KIP-496) only exists at v0, which is non-flexible.
        // `FLEXIBLE_MIN` is `i16::MAX`, so an explicit `>=` arm triggers
        // `clippy::absurd_extreme_comparisons`. Fall through to `_ => false`.
        48 => version >= owned::describe_client_quotas_request::FLEXIBLE_MIN,
        49 => version >= owned::alter_client_quotas_request::FLEXIBLE_MIN,
        50 => version >= owned::describe_user_scram_credentials_request::FLEXIBLE_MIN,
        // AlterUserScramCredentials (KIP-554, slice 12 T15) is flexible from v0.
        51 => version >= owned::alter_user_scram_credentials_request::FLEXIBLE_MIN,
        56 => version >= owned::alter_partition_request::FLEXIBLE_MIN,
        60 => version >= owned::describe_cluster_request::FLEXIBLE_MIN,
        // DescribeProducers (61, KIP-664) is flexible from v0.
        61 => version >= owned::describe_producers_request::FLEXIBLE_MIN,
        63 => version >= owned::broker_heartbeat_request::FLEXIBLE_MIN,
        // KIP-714 client-metrics push pair; both are flexible from v0.
        71 => version >= owned::get_telemetry_subscriptions_request::FLEXIBLE_MIN,
        72 => version >= owned::push_telemetry_request::FLEXIBLE_MIN,
        // DescribeTopicPartitions (75, KIP-966) is flexible from v0.
        75 => version >= owned::describe_topic_partitions_request::FLEXIBLE_MIN,
        _ => false,
    }
}

/// Prepend the response header (`corr_id` + optional tagged-fields byte)
/// in front of the handler's body bytes.
fn encode_response(api_key: i16, correlation_id: i32, body_flexible: bool, body: &[u8]) -> Bytes {
    let header_v1 = body_flexible && api_key != API_VERSIONS_KEY;
    let header_len = if header_v1 { 5 } else { 4 };
    debug_assert!(body.len() < MAX_FRAME_BYTES);
    let mut buf = BytesMut::with_capacity(header_len + body.len());
    buf.put_i32(correlation_id);
    if header_v1 {
        buf.put_u8(0); // empty tagged fields
    }
    buf.put_slice(body);
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_header_v1_no_flexible() {
        // api_key=3, version=8 (non-flexible), corr_id=42, client_id="hi"
        let mut buf = BytesMut::new();
        buf.put_i16(3);
        buf.put_i16(8);
        buf.put_i32(42);
        buf.put_i16(2);
        buf.put_slice(b"hi");
        let (k, v, c, body) = parse_request_header(&buf).unwrap();
        assert_eq!((k, v, c, body.len()), (3, 8, 42, 0));
    }

    #[test]
    fn parse_header_v2_with_tagged_byte() {
        // api_key=18 (ApiVersions), version=3 (flexible), corr_id=1, client_id="x"
        let mut buf = BytesMut::new();
        buf.put_i16(18);
        buf.put_i16(3);
        buf.put_i32(1);
        buf.put_i16(1);
        buf.put_slice(b"x");
        buf.put_u8(0); // tagged-fields byte
        let (k, v, c, body) = parse_request_header(&buf).unwrap();
        assert_eq!((k, v, c, body.len()), (18, 3, 1, 0));
    }

    #[test]
    fn encode_response_apiversions_uses_v0_header() {
        // ApiVersions response is always header v0 (no tagged byte) even
        // for flexible body versions.
        let body = [0u8, 0u8]; // error_code=0
        let out = encode_response(API_VERSIONS_KEY, 7, true, &body);
        // 4 byte corr_id + body, no tagged byte.
        assert_eq!(out.len(), 4 + body.len());
    }

    #[test]
    fn peek_api_key_reads_first_two_bytes_big_endian() {
        // api_key=18, version=3, corr_id=1 — only first 2 bytes are inspected.
        let mut buf = BytesMut::new();
        buf.put_i16(18);
        buf.put_i16(3);
        buf.put_i32(1);
        assert_eq!(peek_api_key(&buf).unwrap(), 18);
    }

    #[test]
    fn peek_api_key_rejects_short_frame() {
        let buf = [0u8; 1];
        assert!(peek_api_key(&buf).is_err());
    }

    #[test]
    fn encode_response_other_flexible_inserts_tagged_byte() {
        let body = [0u8, 0u8];
        let out = encode_response(3, 7, true, &body);
        assert_eq!(out.len(), 5 + body.len());
        assert_eq!(out[4], 0); // tagged byte
    }
}
