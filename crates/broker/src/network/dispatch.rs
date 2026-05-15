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

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::network::codec::{self, MAX_FRAME_BYTES};

const API_VERSIONS_KEY: i16 = 18;

/// Per-listener entrypoint. Branches between TLS termination (when the
/// listener's protocol requires TLS) and the plaintext path. Both paths
/// converge on [`serve_connection_stream`] for the per-connection request
/// loop.
pub async fn serve_connection_on_listener(
    broker: std::sync::Arc<Broker>,
    stream: TcpStream,
    spec: crate::config::ListenerSpec,
) {
    if spec.protocol.requires_tls() {
        let Some(acceptor) = broker.tls_acceptor.clone() else {
            tracing::error!(
                listener = %spec.name,
                "TLS listener configured but broker has no TlsAcceptor"
            );
            return;
        };
        match acceptor.accept(stream).await {
            Ok(tls_stream) => serve_connection_stream(broker, tls_stream, spec).await,
            Err(e) => tracing::debug!(error = %e, "TLS handshake failed"),
        }
    } else {
        serve_connection_plaintext(broker, stream, spec).await;
    }
}

/// Plaintext entry point: keeps the legacy `TcpStream`-typed signature
/// for call sites (and lets us record the peer's TCP address before we
/// hand the stream to the generic loop).
async fn serve_connection_plaintext(
    broker: std::sync::Arc<Broker>,
    stream: TcpStream,
    spec: crate::config::ListenerSpec,
) {
    serve_connection_stream(broker, stream, spec).await;
}

/// Generic per-connection request loop. `S` is the post-handshake byte
/// stream — `TcpStream` for plaintext listeners, `tokio_rustls::server::TlsStream<TcpStream>`
/// for TLS listeners. `spec` carries the listener's protocol so the loop
/// can initialise `ConnectionAuth` correctly and gate pre-auth requests on
/// SASL listeners (Slice 12, T12).
async fn serve_connection_stream<S>(
    broker: std::sync::Arc<Broker>,
    stream: S,
    spec: crate::config::ListenerSpec,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut framed: Framed<S, _> = Framed::new(stream, codec::codec());
    let is_sasl_listener = spec.protocol.requires_sasl();
    // Per-connection auth state. Mutated by the SASL handlers in T13/T14;
    // T12 only uses it to gate non-allowlisted api_keys before auth completes.
    #[allow(unused_mut)] // T13/T14 mutate `auth` via SaslAuthenticate handlers.
    let mut auth = if is_sasl_listener {
        crate::network::auth::ConnectionAuth::Anonymous
    } else {
        // PLAINTEXT / SSL: implicit anonymous, treated as authenticated for
        // gating purposes so the pre-auth allowlist is a no-op.
        crate::network::auth::ConnectionAuth::Authenticated {
            principal: crabka_security::Principal {
                name: "ANONYMOUS".to_string(),
                mechanism: crabka_security::SaslMechanism::Plain,
            },
        }
    };
    tracing::info!(listener = %spec.name, sasl = is_sasl_listener, "connection opened");

    while let Some(frame) = framed.next().await {
        let frame = match frame {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "frame decode error, closing");
                break;
            }
        };
        // Pre-auth gate: on SASL listeners, before the connection is
        // authenticated, only api_keys on the allowlist (17/36/18) are
        // permitted. Anything else gets ILLEGAL_SASL_STATE (34).
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
        if is_sasl_listener && !auth.is_authenticated() {
            match peek_api_key(&frame) {
                Ok(api_key) if !crate::network::auth::is_pre_auth_allowed(api_key) => {
                    tracing::info!(
                        api_key,
                        listener = %spec.name,
                        "pre-auth request blocked (ILLEGAL_SASL_STATE), closing connection"
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
        if let Some(outcome) = try_handle_sasl_frame(&broker, &frame, &mut auth) {
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
        // principal so it can enforce the super-user gate; the handler table
        // signature passes only `&Broker`, so this case is intercepted inline
        // like the SASL frames are. Returning `Some` short-circuits the
        // normal `dispatch_one()` path for this frame.
        if peek_api_key(&frame).ok() == Some(51) {
            match handle_alter_user_scram_credentials_frame(&broker, &frame, &auth).await {
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
        let response_bytes = match dispatch_one(&broker, &frame).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "dispatch error, closing connection");
                break;
            }
        };
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
fn try_handle_sasl_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &mut crate::network::auth::ConnectionAuth,
) -> Option<Result<SaslFrameOutcome, BrokerError>> {
    let api_key = peek_api_key(frame).ok()?;
    if api_key != 17 && api_key != 36 {
        return None;
    }
    Some(handle_sasl_frame(broker, frame, auth, api_key))
}

fn handle_sasl_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &mut crate::network::auth::ConnectionAuth,
    api_key: i16,
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
            let resp = crate::network::auth::handle_handshake(
                &req,
                auth,
                &broker.config.enabled_sasl_mechanisms,
            );
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
            // Must be in `Negotiating` state (i.e. SaslHandshake was the
            // previous frame). Otherwise return ILLEGAL_SASL_STATE (34) and
            // close.
            let mech_opt = match auth {
                crate::network::auth::ConnectionAuth::Negotiating { mechanism, .. } => {
                    Some(*mechanism)
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
                    crabka_security::SaslMechanism::ScramSha512 => {
                        crate::network::auth::handle_authenticate_scram(
                            &req,
                            auth,
                            &broker.controller,
                        )
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

/// Decode + dispatch an `AlterUserScramCredentials` (`api_key` 51) frame.
/// Pulls the authenticated principal off the per-connection `auth` state so
/// the handler can enforce its super-user gate. On a SASL listener the
/// pre-auth allowlist already rejects this frame; on PLAINTEXT/SSL listeners
/// the connection is implicitly `Authenticated { ANONYMOUS / Plain }` (see
/// the loop init), so `principal()` always returns `Some` here.
async fn handle_alter_user_scram_credentials_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
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
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp = crate::handlers::alter_user_scram_credentials::handle(broker, req, &principal).await;
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
        36 => version >= owned::sasl_authenticate_request::FLEXIBLE_MIN,
        37 => version >= owned::create_partitions_request::FLEXIBLE_MIN,
        42 => version >= owned::delete_groups_request::FLEXIBLE_MIN,
        44 => version >= owned::incremental_alter_configs_request::FLEXIBLE_MIN,
        // AlterUserScramCredentials (KIP-554, slice 12 T15) is flexible from v0.
        51 => version >= owned::alter_user_scram_credentials_request::FLEXIBLE_MIN,
        56 => version >= owned::alter_partition_request::FLEXIBLE_MIN,
        60 => version >= owned::describe_cluster_request::FLEXIBLE_MIN,
        63 => version >= owned::broker_heartbeat_request::FLEXIBLE_MIN,
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
