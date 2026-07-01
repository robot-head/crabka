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

#![allow(dead_code)] // accept loop wires this up elsewhere.

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

/// Generic per-connection request loop. `S` is the post-handshake byte
/// stream — `TcpStream` for plaintext listeners, `tokio_rustls::server::TlsStream<TcpStream>`
/// for TLS listeners. `spec` carries the listener's protocol so the loop
/// can initialise `ConnectionAuth` correctly and gate pre-auth requests on
/// SASL listeners.
#[allow(clippy::too_many_lines)] // each api_key intercept arm adds ~15 lines.
async fn serve_connection_stream<S>(
    broker: std::sync::Arc<Broker>,
    stream: S,
    spec: crate::config::ListenerSpec,
    peer: SocketAddr,
    mtls_principal: Option<crabka_security::Principal>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static + crate::network::fetch_writer::SendfileSink,
{
    let mut framed: Framed<S, _> = Framed::new(stream, codec::codec());
    let is_sasl_listener = spec.protocol.requires_sasl();
    let sasl_mechanisms = crate::network::listener::resolve_sasl_mechanisms_for_listener(
        &spec,
        &broker.config.enabled_sasl_mechanisms,
    )
    .to_owned();
    // Per-connection auth state. Mutated by the SASL handlers;
    // used to gate non-allowlisted api_keys before auth completes.
    // When an mTLS client cert was presented (verified by the TLS
    // layer against `client_ca_path`), the dispatch layer starts the
    // connection as Authenticated with the cert's Subject DN as the
    // principal name. SASL listeners ignore mTLS principals — Kafka's
    // SASL_SSL semantics require SASL to be the auth, even if a cert was
    // negotiated for transport.
    #[allow(unused_mut)] // SaslAuthenticate handlers mutate `auth`.
    let mut auth = if is_sasl_listener {
        crate::network::auth::ConnectionAuth::Anonymous
    } else if let Some(principal) = mtls_principal {
        // Non-SASL connections carry an inert mechanism +
        // no-expiry; the in-band re-auth path is unreachable on these
        // listeners (handshake is only sent on SASL listeners).
        crate::network::auth::ConnectionAuth::Authenticated {
            principal,
            mechanism: crabka_security::SaslMechanism::Plain,
            expires_at_ms: None,
            // mTLS clients never auth via a delegation token.
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
            // Anonymous never auths via a delegation token.
            authenticated_via_token: false,
        }
    };
    // Track live connections for the duration of this serve loop. The
    // gauge is decremented when `_conn` drops on any loop exit (EOF,
    // decode/send error, or SASL-session expiry).
    let _conn = ActiveConnectionGuard::new(&broker.metrics);
    tracing::info!(listener = %spec.name, sasl = is_sasl_listener, "connection opened");

    // KIP-714: per-connection client software name + version, populated from
    // the first `ApiVersions v3+` request (KIP-511). Default to empty strings
    // so `GetTelemetrySubscriptions` can be served even on connections that
    // never sent `ApiVersions` (e.g. early-version clients).
    let mut client_software_name = String::new();
    let mut client_software_version = String::new();

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
            // Freeze the codec's `BytesMut` to a refcounted `Bytes` once per
            // frame (zero-copy). The produce hot path slices each partition's
            // verbatim records bytes as a cheap refcount view of this `Bytes`
            // (see `handle_produce_frame`); all other readers deref it as
            // `&[u8]` exactly as before.
            Some(Ok(b)) => b.freeze(),
            Some(Err(e)) => {
                tracing::warn!(error = %e, "frame decode error, closing");
                break;
            }
            None => break, // EOF
        };
        // Per-request server span. The `enabled!` guard keeps
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
        // dispatch layer would require a switch over every api_key. The
        // SASL path sends a *typed* SaslAuthenticate(36) response with error_code=58
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
        // Inline-intercept api_keys. These RPCs need the connection's
        // authenticated principal and/or peer `SocketAddr` for ACL gating,
        // which the `&Broker`-only handler-table signature can't carry, so
        // each is dispatched here instead of via `dispatch_one()`. The
        // api_key is peeked ONCE; arm order is preserved exactly (the
        // data-plane keys 0/1/3 stay ahead of the admin RPCs, as before). A
        // matching arm writes its response inline and `continue`s the loop;
        // any handler or send error closes the connection. Non-intercepted
        // keys (and a frame too short to peek) fall through to the
        // `dispatch_one()` path below.
        //
        // `intercept!` factors out the identical Ok/Err send-or-close body
        // that was previously duplicated across all 47 arms. `concat!`
        // rebuilds the exact per-RPC log-message literals so tracing output
        // is byte-for-byte unchanged.
        macro_rules! intercept {
            ($call:expr, $label:literal) => {{
                match $call.instrument(req_span.clone()).await {
                    Ok(bytes) => {
                        if let Err(e) = framed.send(bytes).await {
                            tracing::warn!(error = %e, concat!("framed.send error during ", $label, ", closing"));
                            break;
                        }
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, concat!($label, " dispatch error, closing connection"));
                        break;
                    }
                }
            }};
        }
        match peek_api_key(&frame).ok() {
            Some(34) => intercept!(
                handle_alter_replica_log_dirs_frame(&broker, &frame, &auth, &peer),
                "ARLD"
            ),
            Some(51) => intercept!(
                handle_alter_user_scram_credentials_frame(&broker, &frame, &auth, &peer),
                "AUSCR"
            ),
            Some(57) => intercept!(
                handle_update_features_frame(&broker, &frame, &auth, &peer),
                "UpdateFeatures"
            ),
            Some(0) => intercept!(
                handle_produce_frame(&broker, &frame, &auth, &peer),
                "Produce"
            ),
            Some(1) => {
                // Fetch takes the zero-copy write-plan path: build an ordered
                // `WriteOp` plan, flush any buffered codec output, then drain
                // the plan directly on the raw stream (vectored write / — in
                // Increment D — sendfile). This bypasses `encode_response`'s
                // whole-body copy and the `Framed` codec's internal copy.
                //
                // `sendfile_capable` is true only for a plaintext `TcpStream` on
                // Linux (false for TLS / non-Linux); it gates whether the fetch
                // handler emits file-backed records regions for sendfile.
                let sendfile_capable =
                    crate::network::fetch_writer::SendfileSink::is_sendfile_capable(
                        framed.get_ref(),
                    );
                match handle_fetch_frame(&broker, &frame, &auth, &peer, sendfile_capable)
                    .instrument(req_span.clone())
                    .await
                {
                    Ok(ops) => {
                        // Flush the codec's write buffer first so the plan bytes
                        // don't interleave with anything the codec has pending.
                        if let Err(e) = SinkExt::<Bytes>::flush(&mut framed).await {
                            tracing::warn!(error = %e, "framed.flush error before fetch plan, closing");
                            break;
                        }
                        let stream = framed.get_mut();
                        if let Err(e) =
                            crate::network::fetch_writer::write_fetch_plan(stream, ops).await
                        {
                            tracing::warn!(error = %e, "fetch plan write error, closing");
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
            Some(3) => intercept!(
                handle_metadata_frame(&broker, &frame, &auth, &peer, &spec.name),
                "Metadata"
            ),
            Some(19) => intercept!(
                handle_create_topics_frame(&broker, &frame, &auth, &peer),
                "CreateTopics"
            ),
            Some(20) => intercept!(
                handle_delete_topics_frame(&broker, &frame, &auth, &peer),
                "DeleteTopics"
            ),
            Some(33) => intercept!(
                handle_alter_configs_frame(&broker, &frame, &auth, &peer),
                "AlterConfigs"
            ),
            Some(44) => intercept!(
                handle_incremental_alter_configs_frame(&broker, &frame, &auth, &peer),
                "IncrementalAlterConfigs"
            ),
            Some(21) => intercept!(
                handle_delete_records_frame(&broker, &frame, &auth, &peer),
                "DeleteRecords"
            ),
            Some(37) => intercept!(
                handle_create_partitions_frame(&broker, &frame, &auth, &peer),
                "CreatePartitions"
            ),
            Some(15) => intercept!(
                handle_describe_groups_frame(&broker, &frame, &auth, &peer),
                "DescribeGroups"
            ),
            Some(16) => intercept!(
                handle_list_groups_frame(&broker, &frame, &auth, &peer),
                "ListGroups"
            ),
            Some(77) => intercept!(
                handle_share_group_describe_frame(&broker, &frame, &auth, &peer),
                "ShareGroupDescribe"
            ),
            Some(78) => intercept!(
                handle_share_fetch_frame(&broker, &frame, &auth, &peer),
                "ShareFetch"
            ),
            Some(79) => intercept!(
                handle_share_acknowledge_frame(&broker, &frame, &auth, &peer),
                "ShareAcknowledge"
            ),
            Some(90) => intercept!(
                handle_describe_share_group_offsets_frame(&broker, &frame, &auth, &peer),
                "DescribeShareGroupOffsets"
            ),
            Some(91) => intercept!(
                handle_alter_share_group_offsets_frame(&broker, &frame, &auth, &peer),
                "AlterShareGroupOffsets"
            ),
            Some(92) => intercept!(
                handle_delete_share_group_offsets_frame(&broker, &frame, &auth, &peer),
                "DeleteShareGroupOffsets"
            ),
            Some(42) => intercept!(
                handle_delete_groups_frame(&broker, &frame, &auth, &peer),
                "DeleteGroups"
            ),
            Some(11) => intercept!(
                handle_join_group_frame(&broker, &frame, &auth, &peer),
                "JoinGroup"
            ),
            Some(8) => intercept!(
                handle_offset_commit_frame(&broker, &frame, &auth, &peer),
                "OffsetCommit"
            ),
            Some(9) => intercept!(
                handle_offset_fetch_frame(&broker, &frame, &auth, &peer),
                "OffsetFetch"
            ),
            Some(47) => intercept!(
                handle_offset_delete_frame(&broker, &frame, &auth, &peer),
                "OffsetDelete"
            ),
            Some(60) => intercept!(
                handle_describe_cluster_frame(&broker, &frame, &auth, &peer, &spec.name),
                "DescribeCluster"
            ),
            Some(61) => intercept!(
                handle_describe_producers_frame(&broker, &frame, &auth, &peer),
                "DescribeProducers"
            ),
            Some(65) => intercept!(
                handle_describe_transactions_frame(&broker, &frame, &auth, &peer),
                "DescribeTransactions"
            ),
            Some(66) => intercept!(
                handle_list_transactions_frame(&broker, &frame, &auth, &peer),
                "ListTransactions"
            ),
            Some(64) => intercept!(
                handle_unregister_broker_frame(&broker, &frame, &auth, &peer),
                "UnregisterBroker"
            ),
            Some(75) => intercept!(
                handle_describe_topic_partitions_frame(&broker, &frame, &auth, &peer),
                "DescribeTopicPartitions"
            ),
            Some(74) => intercept!(
                handle_list_config_resources_frame(&broker, &frame, &auth, &peer),
                "ListConfigResources"
            ),
            Some(55) => intercept!(
                handle_describe_quorum_frame(&broker, &frame, &auth, &peer),
                "DescribeQuorum"
            ),
            Some(80) => intercept!(
                handle_add_raft_voter_frame(&broker, &frame, &auth, &peer),
                "AddRaftVoter"
            ),
            Some(81) => intercept!(
                handle_remove_raft_voter_frame(&broker, &frame, &auth, &peer),
                "RemoveRaftVoter"
            ),
            Some(82) => intercept!(
                handle_update_raft_voter_frame(&broker, &frame, &auth, &peer),
                "UpdateRaftVoter"
            ),
            Some(56) => intercept!(
                handle_alter_partition_frame(&broker, &frame, &auth, &peer),
                "AlterPartition"
            ),
            Some(63) => intercept!(
                handle_broker_heartbeat_frame(&broker, &frame, &auth, &peer),
                "BrokerHeartbeat"
            ),
            Some(93) => intercept!(
                handle_get_replica_log_info_frame(&broker, &frame, &auth, &peer),
                "GetReplicaLogInfo"
            ),
            Some(12) => intercept!(
                handle_heartbeat_frame(&broker, &frame, &auth, &peer),
                "Heartbeat"
            ),
            Some(14) => intercept!(
                handle_sync_group_frame(&broker, &frame, &auth, &peer),
                "SyncGroup"
            ),
            Some(13) => intercept!(
                handle_leave_group_frame(&broker, &frame, &auth, &peer),
                "LeaveGroup"
            ),
            Some(68) => intercept!(
                handle_consumer_group_heartbeat_frame(&broker, &frame, &auth, &peer),
                "ConsumerGroupHeartbeat"
            ),
            Some(76) => intercept!(
                handle_share_group_heartbeat_frame(&broker, &frame, &auth, &peer),
                "ShareGroupHeartbeat"
            ),
            Some(88) => intercept!(
                handle_streams_group_heartbeat_frame(&broker, &frame, &auth, &peer),
                "StreamsGroupHeartbeat"
            ),
            Some(10) => intercept!(
                handle_find_coordinator_frame(&broker, &frame, &auth, &peer, &spec.name),
                "FindCoordinator"
            ),
            Some(2) => intercept!(
                handle_list_offsets_frame(&broker, &frame, &auth, &peer),
                "ListOffsets"
            ),
            Some(23) => intercept!(
                handle_offset_for_leader_epoch_frame(&broker, &frame, &auth, &peer),
                "OffsetForLeaderEpoch"
            ),
            Some(32) => intercept!(
                handle_describe_configs_frame(&broker, &frame, &auth, &peer),
                "DescribeConfigs"
            ),
            Some(35) => intercept!(
                handle_describe_log_dirs_frame(&broker, &frame, &auth, &peer),
                "DescribeLogDirs"
            ),
            Some(29) => intercept!(
                handle_describe_acls_frame(&broker, &frame, &auth, &peer),
                "DescribeAcls"
            ),
            Some(30) => intercept!(
                handle_create_acls_frame(&broker, &frame, &auth, &peer),
                "CreateAcls"
            ),
            Some(31) => intercept!(
                handle_delete_acls_frame(&broker, &frame, &auth, &peer),
                "DeleteAcls"
            ),
            Some(43) => intercept!(
                handle_elect_leaders_frame(&broker, &frame, &auth, &peer),
                "ElectLeaders"
            ),
            Some(45) => intercept!(
                handle_alter_partition_reassignments_frame(&broker, &frame, &auth, &peer),
                "AlterPartitionReassignments"
            ),
            Some(46) => intercept!(
                handle_list_partition_reassignments_frame(&broker, &frame, &auth, &peer),
                "ListPartitionReassignments"
            ),
            Some(48) => intercept!(
                handle_describe_client_quotas_frame(&broker, &frame, &auth, &peer),
                "DescribeClientQuotas"
            ),
            Some(49) => intercept!(
                handle_alter_client_quotas_frame(&broker, &frame, &auth, &peer),
                "AlterClientQuotas"
            ),
            Some(50) => intercept!(
                handle_describe_user_scram_credentials_frame(&broker, &frame, &auth, &peer),
                "DescribeUserScramCredentials"
            ),
            Some(38) => intercept!(
                handle_create_delegation_token_frame(&broker, &frame, &auth),
                "CreateDelegationToken"
            ),
            Some(39) => intercept!(
                handle_renew_delegation_token_frame(&broker, &frame, &auth),
                "RenewDelegationToken"
            ),
            Some(40) => intercept!(
                handle_expire_delegation_token_frame(&broker, &frame, &auth),
                "ExpireDelegationToken"
            ),
            Some(41) => intercept!(
                handle_describe_delegation_token_frame(&broker, &frame, &auth, &peer),
                "DescribeDelegationToken"
            ),
            Some(22) => intercept!(
                handle_init_producer_id_frame(&broker, &frame, &auth, &peer),
                "InitProducerId"
            ),
            Some(24) => intercept!(
                handle_add_partitions_to_txn_frame(&broker, &frame, &auth, &peer),
                "AddPartitionsToTxn"
            ),
            Some(26) => intercept!(
                handle_end_txn_frame(&broker, &frame, &auth, &peer),
                "EndTxn"
            ),
            Some(28) => intercept!(
                handle_txn_offset_commit_frame(&broker, &frame, &auth, &peer),
                "TxnOffsetCommit"
            ),
            Some(71) => intercept!(
                handle_get_telemetry_subscriptions_frame(
                    &broker,
                    &frame,
                    &peer,
                    &client_software_name,
                    &client_software_version,
                ),
                "GetTelemetrySubscriptions"
            ),
            Some(72) => intercept!(
                handle_push_telemetry_frame(
                    &broker,
                    &frame,
                    &peer,
                    &client_software_name,
                    &client_software_version,
                ),
                "PushTelemetry"
            ),
            _ => {}
        }
        // KIP-511: capture client software name + version from ApiVersions v3+
        // frames so they're available for telemetry subscription matching. This
        // runs on the fall-through path (key 18 is not inline-intercepted), just
        // before dispatch_one. We decode only the version field from the header
        // and the software fields from the body; the full response is built by
        // the api_versions handler inside dispatch_one as normal.
        if peek_api_key(&frame).ok() == Some(API_VERSIONS_KEY)
            && let Ok((_, api_version, _, body)) = parse_request_header(&frame)
            && api_version >= 3
        {
            use crabka_protocol::Decode;
            let mut cur: &[u8] = body;
            if let Ok(req) =
                crabka_protocol::owned::api_versions_request::ApiVersionsRequest::decode(
                    &mut cur,
                    api_version,
                )
                && crate::handlers::api_versions::is_valid_client_info(&req.client_software_name)
                && crate::handlers::api_versions::is_valid_client_info(&req.client_software_version)
            {
                client_software_name.clone_from(&req.client_software_name);
                client_software_version.clone_from(&req.client_software_version);
            }
        }
        // KIP-124 request_percentage enforcement — fallback HandlerTable path only.
        // Intercept arms (admin RPCs: ACLs, ElectLeaders, AlterPartitionReassignments,
        // ListPartitionReassignments, AlterClientQuotas, DescribeClientQuotas, etc.)
        // handle their own response write inline and are NOT subject to
        // request_percentage throttling here. Admin RPCs are low-frequency operator
        // traffic; the exemption is documented in STATUS.md.
        let started = std::time::Instant::now();
        let api_key = peek_api_key(&frame).ok();
        let mut response_bytes = match dispatch_one(&broker, &frame)
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

        // Consume elapsed handler time from the request_percentage bucket and
        // mute the channel (sleep before writing the response). Produce (0) and
        // Fetch (1) self-account inside their handlers so the request throttle
        // can be combined as max(request, byte-rate) into a single
        // throttle_time_ms + a single channel mute (KIP-219); they are skipped
        // here to avoid double-charging. For the remaining fall-through APIs we
        // surface the request throttle in the response's leading ThrottleTimeMs
        // field (where present at this version, KIP-219) before muting the
        // channel, instead of muting silently.
        let self_accounts = matches!(api_key, Some(0 | 1));
        if !self_accounts && let Some(principal) = auth.principal() {
            let client_id_str = peek_client_id(&frame).unwrap_or("");
            let image = broker.controller.current_image();
            let delay = crate::quota::consume_request_quota(
                &image,
                &broker.quota_buckets,
                &principal.name,
                client_id_str,
                elapsed_micros,
            );
            if delay > std::time::Duration::ZERO {
                // KIP-219 throttle-then-respond: echo the request-quota throttle
                // in the response's leading ThrottleTimeMs field before the
                // channel mute, for APIs that carry it at the negotiated version.
                if let (Some(key), Ok((_, version, _, _))) = (api_key, parse_request_header(&frame))
                    && throttle_is_leading_field(key, version)
                {
                    let delay_ms = i32::try_from(delay.as_millis()).unwrap_or(i32::MAX);
                    response_bytes = patch_leading_throttle(response_bytes, key, version, delay_ms);
                }
                tokio::time::sleep(delay).await;
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
            let mech_label = mech_opt.map_or("Unknown", crabka_security::SaslMechanism::wire_name);
            broker
                .metrics
                .record_authentication(mech_label, resp.error_code == 0);
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
        &*image,
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
/// enforce the Cluster Alter ACL gate. On a SASL listener the
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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

/// Decode + dispatch an `UpdateFeatures` (`api_key` 57, KIP-584) frame.
/// Pulls the authenticated principal off the per-connection `auth` state and
/// the peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Alter` on `Cluster("kafka-cluster")`.
async fn handle_update_features_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::{Decode, Encode};

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 57);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::update_features_request::UpdateFeaturesRequest::decode(
        &mut cur,
        api_version,
    )?;

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };

    let resp = crate::handlers::update_features::handle(broker, req, api_version, &ctx).await;
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
/// authorize `Describe` on `Cluster("kafka-cluster")`.
/// On Deny the whole response receives `CLUSTER_AUTHORIZATION_FAILED`.
async fn handle_describe_cluster_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
    listener_name: &str,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 60);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // DescribeCluster advertises each broker's endpoint for the listener
        // this request arrived on.
        connection_listener_name: listener_name,
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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

/// Decode + dispatch a `DescribeTransactions` (`api_key` 65, KIP-664)
/// frame. Per-tid `Describe` ACL on `TransactionalId`; the handler
/// needs the connection principal + peer.
async fn handle_describe_transactions_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 65);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };

    let resp_body = crate::handlers::describe_transactions::handle(
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

/// Decode + dispatch a `ListTransactions` (`api_key` 66, KIP-664) frame.
/// Per-tid `Describe` ACL on `TransactionalId` (silent filter on Deny).
async fn handle_list_transactions_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 66);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };

    let resp_body =
        crate::handlers::list_transactions::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `UnregisterBroker` (`api_key` 64, KIP-919) frame.
/// `Alter` on `Cluster`; Deny → `CLUSTER_AUTHORIZATION_FAILED`.
async fn handle_unregister_broker_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 64);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };

    let resp_body =
        crate::handlers::unregister_broker::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `AddRaftVoter` (`api_key` 80, KIP-853) frame.
/// `Alter` on `Cluster`; Deny → `CLUSTER_AUTHORIZATION_FAILED`.
async fn handle_add_raft_voter_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 80);
    let body_flexible = handler_body_flexible(api_key, api_version);
    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };
    let resp_body =
        crate::handlers::add_raft_voter::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `RemoveRaftVoter` (`api_key` 81, KIP-853) frame.
/// `Alter` on `Cluster`; Deny → `CLUSTER_AUTHORIZATION_FAILED`.
async fn handle_remove_raft_voter_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 81);
    let body_flexible = handler_body_flexible(api_key, api_version);
    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };
    let resp_body =
        crate::handlers::remove_raft_voter::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `UpdateRaftVoter` (`api_key` 82, KIP-853) frame.
/// `Alter` on `Cluster`; Deny → `CLUSTER_AUTHORIZATION_FAILED`.
async fn handle_update_raft_voter_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 82);
    let body_flexible = handler_body_flexible(api_key, api_version);
    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };
    let resp_body =
        crate::handlers::update_raft_voter::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `AlterPartition` (`api_key` 56) frame. Inter-broker
/// control-plane RPC: `ClusterAction` on `Cluster`; Deny → whole-response
/// `CLUSTER_AUTHORIZATION_FAILED`.
async fn handle_alter_partition_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 56);
    let body_flexible = handler_body_flexible(api_key, api_version);
    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };
    let resp_body =
        crate::handlers::alter_partition::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `BrokerHeartbeat` (`api_key` 63) frame. Inter-broker
/// control-plane RPC: `ClusterAction` on `Cluster`; Deny → whole-response
/// `CLUSTER_AUTHORIZATION_FAILED`.
async fn handle_broker_heartbeat_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 63);
    let body_flexible = handler_body_flexible(api_key, api_version);
    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };
    let resp_body =
        crate::handlers::broker_heartbeat::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `GetReplicaLogInfo` (`api_key` 93, KIP-966) frame.
/// Inter-broker control-plane RPC: `ClusterAction` on `Cluster`; Deny →
/// per-partition `CLUSTER_AUTHORIZATION_FAILED`.
async fn handle_get_replica_log_info_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 93);
    let body_flexible = handler_body_flexible(api_key, api_version);
    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };
    let resp_body = crate::handlers::get_replica_log_info::handle(
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

/// Decode + dispatch a `Heartbeat` (`api_key` 12) frame. `Read` on
/// `Group(group_id)`; Deny → whole-response `GROUP_AUTHORIZATION_FAILED`.
async fn handle_heartbeat_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 12);
    let body_flexible = handler_body_flexible(api_key, api_version);
    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };
    let resp_body =
        crate::handlers::heartbeat::handle(broker, api_version, correlation_id, body, &ctx).await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `SyncGroup` (`api_key` 14) frame. `Read` on
/// `Group(group_id)`; Deny → whole-response `GROUP_AUTHORIZATION_FAILED`.
async fn handle_sync_group_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 14);
    let body_flexible = handler_body_flexible(api_key, api_version);
    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };
    let resp_body =
        crate::handlers::sync_group::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `LeaveGroup` (`api_key` 13) frame. `Read` on
/// `Group(group_id)`; Deny → whole-response `GROUP_AUTHORIZATION_FAILED`.
async fn handle_leave_group_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 13);
    let body_flexible = handler_body_flexible(api_key, api_version);
    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };
    let resp_body =
        crate::handlers::leave_group::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `ConsumerGroupHeartbeat` (`api_key` 68, KIP-848)
/// frame. `Read` on `Group(group_id)`; Deny → whole-response
/// `GROUP_AUTHORIZATION_FAILED`.
async fn handle_consumer_group_heartbeat_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 68);
    let body_flexible = handler_body_flexible(api_key, api_version);
    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };
    let resp_body = crate::handlers::consumer_group_heartbeat::handle(
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

/// Decode + dispatch a `ShareGroupHeartbeat` (`api_key` 76, KIP-932) frame.
/// `Read` on `Group(group_id)`; Deny → whole-response
/// `GROUP_AUTHORIZATION_FAILED`.
async fn handle_share_group_heartbeat_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 76);
    let body_flexible = handler_body_flexible(api_key, api_version);
    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };
    let resp_body = crate::handlers::share_group_heartbeat::handle(
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

/// Decode + dispatch a `StreamsGroupHeartbeat` (`api_key` 88, KIP-1071)
/// frame. `Read` on `Group(group_id)`; Deny → whole-response
/// `GROUP_AUTHORIZATION_FAILED`.
async fn handle_streams_group_heartbeat_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 88);
    let body_flexible = handler_body_flexible(api_key, api_version);
    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };
    let resp_body = crate::handlers::streams_group_heartbeat::handle(
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

/// Decode + dispatch a `FindCoordinator` (`api_key` 10) frame. Per-key
/// `Describe`: GROUP → `Group(key)`, TRANSACTION → `TransactionalId(key)`.
/// Denied keys are stamped with the authorization-failed code.
async fn handle_find_coordinator_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
    listener_name: &str,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 10);
    let body_flexible = handler_body_flexible(api_key, api_version);
    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // FindCoordinator advertises the coordinator's endpoint for the
        // listener this request arrived on.
        connection_listener_name: listener_name,
    };
    let resp_body =
        crate::handlers::find_coordinator::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `ListOffsets` (`api_key` 2) frame. Per-topic
/// `Describe` on `Topic(name)`; denied topics → `TOPIC_AUTHORIZATION_FAILED`
/// per partition.
async fn handle_list_offsets_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 2);
    let body_flexible = handler_body_flexible(api_key, api_version);
    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };
    let resp_body =
        crate::handlers::list_offsets::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `OffsetForLeaderEpoch` (`api_key` 23) frame.
/// Per-topic `Describe` on `Topic(name)`; denied topics →
/// `TOPIC_AUTHORIZATION_FAILED` per partition.
async fn handle_offset_for_leader_epoch_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 23);
    let body_flexible = handler_body_flexible(api_key, api_version);
    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };
    let resp_body = crate::handlers::offset_for_leader_epoch::handle(
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

/// Decode + dispatch a `DescribeConfigs` (`api_key` 32) frame. Per-resource
/// `DescribeConfigs`: Topic → `Topic(name)`, Broker → `Cluster`. Denied
/// resources are stamped with the matching authorization-failed code.
async fn handle_describe_configs_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 32);
    let body_flexible = handler_body_flexible(api_key, api_version);
    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };
    let resp_body =
        crate::handlers::describe_configs::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DescribeLogDirs` (`api_key` 35, KIP-113) frame.
/// `Describe` on `Cluster`; Deny → whole-response
/// `CLUSTER_AUTHORIZATION_FAILED`.
async fn handle_describe_log_dirs_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 35);
    let body_flexible = handler_body_flexible(api_key, api_version);
    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };
    let resp_body =
        crate::handlers::describe_log_dirs::handle(broker, api_version, correlation_id, body, &ctx)
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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

/// Decode + dispatch a `ListConfigResources` (`api_key` 74, KIP-1142)
/// frame. Needs the authenticated principal and peer `SocketAddr` for the
/// whole-request `Cluster` `Describe` ACL gate (matches `DescribeCluster`'s
/// pattern), which the `&Broker`-only handler table signature can't carry.
async fn handle_list_config_resources_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 74);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };

    let resp_body = crate::handlers::list_config_resources::handle(
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

/// Decode + dispatch a `DescribeQuorum` (`api_key` 55, KIP-595) frame.
/// Needs the authenticated principal and peer `SocketAddr` for the
/// whole-request `Cluster` `Describe` ACL gate.
async fn handle_describe_quorum_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 55);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };

    let resp_body =
        crate::handlers::describe_quorum::handle(broker, api_version, correlation_id, body, &ctx)
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
/// batch-authorize every topic in the request for `Write`.
/// On PLAINTEXT/SSL listeners the connection is implicitly
/// `Authenticated { ANONYMOUS / Plain }` (see the loop init), so
/// `principal()` always returns `Some` here; the `unwrap_or_else`
/// fallback covers the defensive SASL pre-auth case.
async fn handle_produce_frame(
    broker: &Broker,
    frame: &Bytes,
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 0);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };

    // The produce hot path slices each partition's verbatim records bytes
    // as a zero-copy view of `frame`. `body` is a sub-slice of `frame`;
    // capture its start offset so the handler can re-slice the owning
    // `Bytes` (a refcount bump, not a copy) rather than the borrowed
    // `&[u8]`.
    let body_offset = frame.len() - body.len();
    let body_bytes = frame.slice(body_offset..);

    let resp_body = crate::handlers::produce::handle(
        broker,
        api_version,
        correlation_id,
        body,
        body_bytes,
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
async fn handle_fetch_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
    sendfile_capable: bool,
) -> Result<Vec<crate::network::fetch_writer::WriteOp>, BrokerError> {
    use crate::network::fetch_writer::{WriteOp, build_fetch_plan};

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 1);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Only the canonical v4+ plan path can sendfile; the v0–v3 legacy path
        // copy-encodes (down-conversion). Gate the FileRegions emission on both.
        sendfile_capable: sendfile_capable && api_version >= 4,
        // Fetch doesn't project broker addresses.
        connection_listener_name: "",
    };

    let (resp, version) =
        crate::handlers::fetch::handle(broker, api_version, correlation_id, body, &ctx).await?;

    if version < 4 {
        // Legacy down-conversion path: encode the whole body the old way and
        // wrap it (plus the response header) as a single inline op.
        let body_bytes = crate::handlers::fetch::encode_fetch_response(resp, version)?;
        let framed = encode_response(api_key, correlation_id, body_flexible, &body_bytes);
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
        if sendfile_capable && api_version >= 4 {
            return build_fetch_plan(
                &resp,
                version,
                correlation_id,
                body_flexible,
                crate::network::fetch_writer::resolve_records_sendfile,
            );
        }
    }

    build_fetch_plan(
        &resp,
        version,
        correlation_id,
        body_flexible,
        crate::network::fetch_writer::resolve_records_inline,
    )
}

/// Decode + dispatch a `Metadata` (`api_key` 3) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// batch-authorize every candidate topic for `Describe`.
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
    listener_name: &str,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 3);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Metadata advertises each broker's endpoint for the listener this
        // request arrived on (Apache Kafka returns the connection listener's
        // advertised address).
        connection_listener_name: listener_name,
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
/// authorize `Create` on `Cluster("kafka-cluster")`.
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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
/// batch-authorize every topic for `Delete`.
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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

/// Decode + dispatch a `CreateDelegationToken` (`api_key` 38) frame.
/// Threads the per-connection `auth` to the handler so
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
        &*broker.controller,
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

/// Decode + dispatch a `RenewDelegationToken` (`api_key` 39) frame.
/// Threads the per-connection `auth` to the handler so it
/// can enforce the KIP-48 owner-or-renewer check (with a
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
        &*broker.controller,
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

/// Decode + dispatch an `ExpireDelegationToken` (`api_key` 40) frame.
/// Threads the per-connection `auth` to the handler so
/// it can enforce the KIP-48 owner-or-renewer check (with
/// a super-user bypass so the operator's finalizer can tombstone
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
        &*broker.controller,
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

/// Decode + dispatch a `DescribeDelegationToken` (`api_key` 41) frame.
/// Threads the per-connection `auth` so the handler can
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
        &*broker.controller,
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
/// authorize `AlterConfigs` per resource.
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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
/// can authorize `AlterConfigs` per resource.
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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
/// batch-authorize every topic in the request for `Delete`.
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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
/// batch-authorize every topic in the request for `Alter`.
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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
/// authorize `Describe` per group. Denied groups receive
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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

/// Decode + dispatch a `ShareGroupDescribe` (`api_key` 77) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the peer
/// `SocketAddr` from the accept-time capture so the handler can run the
/// per-group `Describe` ACL gate.
async fn handle_share_group_describe_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 77);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };

    let resp_body = crate::handlers::share_group_describe::handle(
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

/// Decode + dispatch a `DescribeShareGroupOffsets` (`api_key` 90, KIP-932)
/// frame. Builds the [`crate::handlers::RequestContext`] the inline handler
/// needs for the per-group `Describe` ACL gate.
async fn handle_describe_share_group_offsets_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 90);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };

    let resp_body = crate::handlers::describe_share_group_offsets::handle(
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

/// Decode + dispatch an `AlterShareGroupOffsets` (`api_key` 91, KIP-932) frame.
/// Builds the [`crate::handlers::RequestContext`] for the per-group `Alter` ACL
/// gate.
async fn handle_alter_share_group_offsets_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 91);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };

    let resp_body = crate::handlers::alter_share_group_offsets::handle(
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

/// Decode + dispatch a `DeleteShareGroupOffsets` (`api_key` 92, KIP-932) frame.
/// Builds the [`crate::handlers::RequestContext`] for the per-group `Delete`
/// ACL gate.
async fn handle_delete_share_group_offsets_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 92);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };

    let resp_body = crate::handlers::delete_share_group_offsets::handle(
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

/// Decode + dispatch a `ShareFetch` (`api_key` 78, KIP-932) frame. The handler
/// needs the authenticated principal + peer `SocketAddr` for the per-topic
/// `Read` ACL gate, which the `&Broker`-only handler table signature can't
/// carry; this helper builds the [`crate::handlers::RequestContext`].
async fn handle_share_fetch_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 78);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };

    let resp_body =
        crate::handlers::share_fetch::handle(broker, api_version, correlation_id, body, &ctx)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `ShareAcknowledge` (`api_key` 79, KIP-932) frame. As
/// [`handle_share_fetch_frame`]: per-topic `Read` ACL gate needs the connection
/// principal + peer.
async fn handle_share_acknowledge_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 79);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
    };

    let resp_body =
        crate::handlers::share_acknowledge::handle(broker, api_version, correlation_id, body, &ctx)
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
/// silently filter out groups denied `Describe`. Denied
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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
/// authorize `Delete` per group. Denied groups receive
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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
/// authorize `Read` on `Group(group_id)`. On Deny the
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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
/// `TOPIC_AUTHORIZATION_FAILED`).
async fn handle_offset_commit_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 8);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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
/// the per-topic check across discovered committed-offsets topics.
async fn handle_offset_fetch_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 9);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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
/// path).
async fn handle_init_producer_id_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 22);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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
/// `Topic`.
async fn handle_add_partitions_to_txn_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 24);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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
/// authorize `Write` on `TransactionalId`.
async fn handle_end_txn_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 26);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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
/// per-topic `Read` on `Topic`.
async fn handle_txn_offset_commit_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 28);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = principal_or_anonymous(auth);
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal,
        peer,
        client_id,
        // Non-fetch handlers ignore sendfile.
        sendfile_capable: false,
        // Only the address-projecting handlers (Metadata / FindCoordinator /
        // DescribeCluster) read this; the rest leave it empty.
        connection_listener_name: "",
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

/// Decode + dispatch a `GetTelemetrySubscriptions` (`api_key` 71, KIP-714)
/// frame. Needs the peer `SocketAddr` and per-connection software
/// name/version for KIP-714 subscription matching; these are not
/// available via the `&Broker`-only handler-table signature.
async fn handle_get_telemetry_subscriptions_frame(
    broker: &Broker,
    frame: &[u8],
    peer: &SocketAddr,
    software_name: &str,
    software_version: &str,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 71);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let client_id = peek_client_id(frame).unwrap_or("");
    let tctx = crate::handlers::TelemetryContext {
        client_id,
        peer,
        software_name,
        software_version,
    };

    let resp_body = crate::handlers::get_telemetry_subscriptions::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &tctx,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `PushTelemetry` (`api_key` 72, KIP-714) frame.
/// Needs the peer `SocketAddr` and per-connection software name/version
/// for subscription authorization; these are not available via the
/// `&Broker`-only handler-table signature.
async fn handle_push_telemetry_frame(
    broker: &Broker,
    frame: &[u8],
    peer: &SocketAddr,
    software_name: &str,
    software_version: &str,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 72);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let client_id = peek_client_id(frame).unwrap_or("");
    let tctx = crate::handlers::TelemetryContext {
        client_id,
        peer,
        software_name,
        software_version,
    };

    let resp_body =
        crate::handlers::push_telemetry::handle(broker, api_version, correlation_id, body, &tctx)
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
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(
        api_key = tracing::field::Empty,
        api_version = tracing::field::Empty,
        correlation_id = tracing::field::Empty,
    ),
    err,
)]
async fn dispatch_one(broker: &Broker, frame: &[u8]) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    let body_flexible = handler_body_flexible(api_key, api_version);
    let span = tracing::Span::current();
    span.record("api_key", api_key);
    span.record("api_version", api_version);
    span.record("correlation_id", correlation_id);
    // Account this dispatched request before any handler
    // work. Counter bumps even for the UNSUPPORTED_VERSION
    // synthetic-response path below, so operators see traffic from
    // misconfigured clients alongside healthy traffic.
    broker.metrics.record_api_request(api_key);
    // Track concurrent handler occupancy + full round-trip latency. The
    // gauge is decremented and the histogram observed on every exit path
    // (including the handler-error early return below) via the RAII guard.
    let _in_flight = InFlightGuard::new(&broker.metrics, api_key);
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
        match h(broker, api_version, correlation_id, body).await {
            Ok(b) => b,
            Err(e) => {
                // Handler-level fault. Account it per-api before the
                // guard fires (which decrements in-flight + observes the
                // latency) and the connection closes upstream.
                broker.metrics.record_request_error(api_key);
                return Err(e);
            }
        }
    } else {
        tracing::warn!(api_key, api_version, "unsupported api, returning error");
        // Account this synthetic UNSUPPORTED_VERSION
        // response so operators can alert on a non-zero rate.
        broker.metrics.record_unsupported_api_request(api_key);
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
        // AlterUserScramCredentials (KIP-554) is flexible from v0.
        51 => version >= owned::alter_user_scram_credentials_request::FLEXIBLE_MIN,
        // DescribeQuorum (55, KIP-595) is flexible from v0.
        55 => version >= owned::describe_quorum_request::FLEXIBLE_MIN,
        56 => version >= owned::alter_partition_request::FLEXIBLE_MIN,
        // UpdateFeatures (57, KIP-584) is flexible from v0.
        57 => version >= owned::update_features_request::FLEXIBLE_MIN,
        // FetchSnapshot (59, KIP-630) is flexible from v0.
        59 => version >= owned::fetch_snapshot_request::FLEXIBLE_MIN,
        60 => version >= owned::describe_cluster_request::FLEXIBLE_MIN,
        // DescribeProducers (61, KIP-664) is flexible from v0.
        61 => version >= owned::describe_producers_request::FLEXIBLE_MIN,
        63 => version >= owned::broker_heartbeat_request::FLEXIBLE_MIN,
        // UnregisterBroker (64, KIP-919) — flexible from v0.
        64 => version >= owned::unregister_broker_request::FLEXIBLE_MIN,
        // DescribeTransactions (65, KIP-664) and ListTransactions (66) — both flexible from v0.
        65 => version >= owned::describe_transactions_request::FLEXIBLE_MIN,
        66 => version >= owned::list_transactions_request::FLEXIBLE_MIN,
        // KIP-848 next-gen consumer group pair; both are flexible from v0.
        68 => version >= owned::consumer_group_heartbeat_request::FLEXIBLE_MIN,
        69 => version >= owned::consumer_group_describe_request::FLEXIBLE_MIN,
        // KIP-714 client-metrics push pair; both are flexible from v0.
        71 => version >= owned::get_telemetry_subscriptions_request::FLEXIBLE_MIN,
        72 => version >= owned::push_telemetry_request::FLEXIBLE_MIN,
        // KIP-932 share-group membership pair; both are flexible from v0.
        76 => version >= owned::share_group_heartbeat_request::FLEXIBLE_MIN,
        77 => version >= owned::share_group_describe_request::FLEXIBLE_MIN,
        // KIP-1071 streams-group membership pair; both are flexible from v0.
        88 => version >= owned::streams_group_heartbeat_request::FLEXIBLE_MIN,
        89 => version >= owned::streams_group_describe_request::FLEXIBLE_MIN,
        // KIP-932 ShareFetch / ShareAcknowledge — both flexible from v0.
        78 => version >= owned::share_fetch_request::FLEXIBLE_MIN,
        79 => version >= owned::share_acknowledge_request::FLEXIBLE_MIN,
        // KIP-932 share-group admin offset RPCs — all flexible from v0.
        90 => version >= owned::describe_share_group_offsets_request::FLEXIBLE_MIN,
        91 => version >= owned::alter_share_group_offsets_request::FLEXIBLE_MIN,
        92 => version >= owned::delete_share_group_offsets_request::FLEXIBLE_MIN,
        // KIP-932 share-coordinator persister RPCs (83-87) — all flexible from v0.
        83 => version >= owned::initialize_share_group_state_request::FLEXIBLE_MIN,
        84 => version >= owned::read_share_group_state_request::FLEXIBLE_MIN,
        85 => version >= owned::write_share_group_state_request::FLEXIBLE_MIN,
        86 => version >= owned::delete_share_group_state_request::FLEXIBLE_MIN,
        87 => version >= owned::read_share_group_state_summary_request::FLEXIBLE_MIN,
        // ListConfigResources (74, KIP-1142) is flexible from v0.
        74 => version >= owned::list_config_resources_request::FLEXIBLE_MIN,
        // DescribeTopicPartitions (75, KIP-966) is flexible from v0.
        75 => version >= owned::describe_topic_partitions_request::FLEXIBLE_MIN,
        // AddRaftVoter / RemoveRaftVoter / UpdateRaftVoter (80/81/82,
        // KIP-853) — all flexible from v0.
        80 => version >= owned::add_raft_voter_request::FLEXIBLE_MIN,
        81 => version >= owned::remove_raft_voter_request::FLEXIBLE_MIN,
        82 => version >= owned::update_raft_voter_request::FLEXIBLE_MIN,
        // GetReplicaLogInfo (93, KIP-966) is flexible from v0.
        93 => version >= owned::get_replica_log_info_request::FLEXIBLE_MIN,
        // AssignReplicasToDirs (73, KIP-858) is flexible from v0.
        73 => version >= owned::assign_replicas_to_dirs_request::FLEXIBLE_MIN,
        _ => false,
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

/// KIP-219 (throttle-then-respond): `true` when `api_key`'s response carries
/// `ThrottleTimeMs` as its FIRST body field at `version`, so the dispatch loop
/// can surface the request-quota throttle by patching that leading int32 in
/// place. Boundaries are verified against the 4.x response schemas. APIs absent
/// from this table keep the pre-KIP-219 behavior (throttle still enforced by the
/// channel mute, just not echoed); Produce (0) / Fetch (1) self-account and
/// never reach this path. `OffsetDelete` (47) is intentionally excluded — its
/// leading field is `ErrorCode`, so patching would corrupt it.
fn throttle_is_leading_field(api_key: i16, version: i16) -> bool {
    match api_key {
        // ListOffsets / JoinGroup / OffsetForLeaderEpoch
        2 | 11 | 23 => version >= 2,
        // Metadata / OffsetCommit / OffsetFetch
        3 | 8 | 9 => version >= 3,
        // FindCoordinator / Heartbeat / LeaveGroup / SyncGroup / DescribeGroups / ListGroups
        10 | 12 | 13 | 14 | 15 | 16 => version >= 1,
        // InitProducerId / DescribeCluster / ConsumerGroupHeartbeat (all 0+)
        22 | 60 | 68 => true,
        _ => false,
    }
}

/// Patch the leading `ThrottleTimeMs` (int32) of an already-encoded response in
/// place, raising it to `max(existing, delay_ms)`. The body begins right after
/// the response header, whose length mirrors `encode_response`: 5 bytes when the
/// body is flexible and the api is not `ApiVersions`, else 4. Callers must first
/// confirm `throttle_is_leading_field`.
fn patch_leading_throttle(resp: Bytes, api_key: i16, version: i16, delay_ms: i32) -> Bytes {
    let header_v1 = handler_body_flexible(api_key, version) && api_key != API_VERSIONS_KEY;
    let off = if header_v1 { 5 } else { 4 };
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
    use super::*;
    use assert2::assert;

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
        assert!((k, v, c, body.len()) == (3, 8, 42, 0));
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
        assert!((k, v, c, body.len()) == (18, 3, 1, 0));
    }

    #[test]
    fn encode_response_apiversions_uses_v0_header() {
        // ApiVersions response is always header v0 (no tagged byte) even
        // for flexible body versions.
        let body = [0u8, 0u8]; // error_code=0
        let out = encode_response(API_VERSIONS_KEY, 7, true, &body);
        // 4 byte corr_id + body, no tagged byte.
        assert!(out.len() == 4 + body.len());
    }

    #[test]
    fn throttle_leading_field_table_matches_schemas() {
        // Present-and-leading version boundaries (verified vs 4.x schemas).
        assert!(!throttle_is_leading_field(11, 1)); // JoinGroup v1: no throttle
        assert!(throttle_is_leading_field(11, 2)); // JoinGroup v2+: leading
        assert!(!throttle_is_leading_field(3, 2)); // Metadata v2: no throttle
        assert!(throttle_is_leading_field(3, 3)); // Metadata v3+
        assert!(throttle_is_leading_field(12, 1)); // Heartbeat v1+
        assert!(throttle_is_leading_field(68, 0)); // ConsumerGroupHeartbeat v0+
        // OffsetDelete (47) leads with ErrorCode — must never be patched.
        assert!(!throttle_is_leading_field(47, 0));
        // Produce/Fetch self-account; ApiVersions is not in the table.
        assert!(!throttle_is_leading_field(0, 9));
        assert!(!throttle_is_leading_field(1, 13));
        assert!(!throttle_is_leading_field(18, 3));
    }

    #[test]
    fn patch_leading_throttle_sets_field_flexible_and_nonflexible() {
        let read =
            |b: &[u8], off: usize| i32::from_be_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]]);

        // Flexible response header (ConsumerGroupHeartbeat, flexible v0+):
        // header = 5 bytes (corr_id + tagged byte); throttle int32 at offset 5.
        let mut body = BytesMut::new();
        body.put_i32(0); // ThrottleTimeMs = 0
        let resp = encode_response(68, 7, true, &body);
        let patched = patch_leading_throttle(resp, 68, 0, 250);
        assert!(read(&patched, 5) == 250);
        assert!(read(&patched, 0) == 7); // corr_id preserved

        // Non-flexible response header (Metadata v3): header = 4 bytes.
        let mut body = BytesMut::new();
        body.put_i32(10); // existing throttle 10 < 250
        let resp = encode_response(3, 9, false, &body);
        let patched = patch_leading_throttle(resp, 3, 3, 250);
        assert!(read(&patched, 4) == 250);
        assert!(read(&patched, 0) == 9);
    }

    #[test]
    fn patch_leading_throttle_keeps_existing_when_larger() {
        // max(existing, delay): an already-larger throttle is not lowered.
        let mut body = BytesMut::new();
        body.put_i32(500);
        let resp = encode_response(3, 1, false, &body);
        let patched = patch_leading_throttle(resp, 3, 3, 100);
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
        assert!(peek_api_key(&buf).unwrap() == 18);
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
        assert!(out.len() == 5 + body.len());
        assert!(out[4] == 0); // tagged byte
    }

    /// KIP-853 RPCs (80/81/82) route through the inline-intercept path,
    /// which keys off `peek_api_key`, and are flexible from v0. This guards
    /// the wiring that decides whether each frame reaches its handler with
    /// the correct flexible-header treatment.
    #[test]
    fn raft_voter_rpcs_peek_and_flex_routing() {
        for api_key in [80i16, 81, 82] {
            let mut buf = BytesMut::new();
            buf.put_i16(api_key);
            buf.put_i16(0); // version 0
            buf.put_i32(1); // corr_id
            assert!(peek_api_key(&buf).unwrap() == api_key);
            assert!(
                handler_body_flexible(api_key, 0),
                "api_key {api_key} is flexible from v0"
            );
        }
    }
}
