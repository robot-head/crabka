//! Inbound TLS + SASL handshake for the controller listener.
//!
//! Mirror image of `network::client::InterBrokerClient`'s
//! outbound auth flow. Reuses `network::auth::handle_handshake` +
//! `handle_authenticate_*` state machines so the controller listener
//! and data plane share one source of truth.
//!
//! Frame helpers (`read_kafka_request`, `write_response`) are the
//! server-side inverse of `network::client::round_trip`. The header
//! flexibility rules match exactly:
//!   - `SaslHandshake (17)` v0+ uses a non-flexible response header
//!     (bare `correlation_id`).
//!   - `SaslAuthenticate (36)` v2+ uses a flexible response header
//!     (`correlation_id` + 1-byte tagged-fields).
//!   - `ApiVersions (18)` response header is *always* v0 by Kafka spec.

// Exercised via the runtime path (T6) and integration tests (T9). Unit
// coverage in this file is deliberately narrow — see the `tests` module
// docstring.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use crabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest;
use crabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest;
use crabka_protocol::{Decode, Encode};
use crabka_raft::{ControllerHandle, DuplexStream, RaftHandshakeError, RaftListenerHandshake};
use crabka_security::{ListenerProtocol, SaslMechanism};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::OnceCell;
use tokio_rustls::TlsAcceptor;

use crate::network::auth::{
    ConnectionAuth, SaslExchange, handle_authenticate_plain, handle_authenticate_scram,
    handle_handshake, is_pre_auth_allowed,
};

/// Late-bound handle to the broker's [`ControllerHandle`].
///
/// The handshake is constructed *before* `crabka_raft::Controller::start`
/// returns (it is moved into `ControllerConfig::handshake`), so the
/// controller is only available later. We carry an `Arc<OnceCell<…>>`
/// and `OnceCell::set` it from `Broker::start` once the controller is
/// built. SCRAM credential lookup (one round per authenticate) is the
/// only code path that touches the cell.
pub type ControllerHandleArc = Arc<OnceCell<Arc<ControllerHandle>>>;

/// API key constants — match the wire-protocol IDs used elsewhere.
const API_KEY_SASL_HANDSHAKE: i16 = 17;
const API_KEY_SASL_AUTHENTICATE: i16 = 36;
const API_KEY_API_VERSIONS: i16 = 18;

/// Per-broker handshake adapter. Constructed in `Broker::start` (T6) and
/// passed into `ControllerConfig::handshake`.
pub struct BrokerRaftHandshake {
    pub tls_acceptor: Option<TlsAcceptor>,
    pub plain_credentials: HashMap<String, String>,
    pub enabled_sasl_mechanisms: Vec<SaslMechanism>,
    pub protocol: ListenerProtocol,
    pub controller: ControllerHandleArc,
}

/// Initial per-connection auth state for an unauthenticated SASL peer.
fn pre_auth_state() -> ConnectionAuth {
    ConnectionAuth::Anonymous
}

#[async_trait::async_trait]
impl RaftListenerHandshake for BrokerRaftHandshake {
    async fn upgrade(
        &self,
        stream: TcpStream,
    ) -> Result<Box<dyn DuplexStream>, RaftHandshakeError> {
        // 1. TLS termination (if the listener protocol requires it).
        let mut stream: Box<dyn DuplexStream> = if self.protocol.requires_tls() {
            let acceptor = self.tls_acceptor.clone().ok_or_else(|| {
                RaftHandshakeError::Tls("tls_config required for TLS controller listener".into())
            })?;
            let tls = acceptor
                .accept(stream)
                .await
                .map_err(|e| RaftHandshakeError::Tls(e.to_string()))?;
            Box::new(tls)
        } else {
            Box::new(stream)
        };

        // 2. SASL termination (if the listener protocol requires it).
        if self.protocol.requires_sasl() {
            run_inbound_sasl(&mut *stream, self).await?;
        }
        Ok(stream)
    }
}

/// Drive the server-side SASL state machine until the connection is
/// authenticated or an error response has been written.
///
/// Loop invariant: every iteration reads exactly one Kafka request frame
/// and writes exactly one response frame. The `auth` state machine
/// (`network::auth::ConnectionAuth`) carries continuation state across
/// SCRAM rounds. Returns `Ok(())` once `auth.is_authenticated()` and
/// `Err(...)` if the peer sent an unexpected frame or auth failed.
async fn run_inbound_sasl(
    stream: &mut dyn DuplexStream,
    cfg: &BrokerRaftHandshake,
) -> Result<(), RaftHandshakeError> {
    let mut auth = pre_auth_state();
    loop {
        let (api_key, api_version, corr_id, body) = read_kafka_request(stream).await?;
        if !is_pre_auth_allowed(api_key) && !auth.is_authenticated() {
            return Err(RaftHandshakeError::Sasl(format!(
                "pre-auth request api_key={api_key} rejected"
            )));
        }
        match api_key {
            // ApiVersions — minimal response so peers that send it first
            // (typical JVM client pattern) can proceed. Our
            // `InterBrokerClient` outbound path skips ApiVersions, so this
            // path exists for JVM-client tolerance only.
            API_KEY_API_VERSIONS => {
                let resp_bytes = build_api_versions_response(corr_id);
                stream.write_all(&resp_bytes).await?;
            }
            API_KEY_SASL_HANDSHAKE => {
                let mut cur = body.as_slice();
                let req = SaslHandshakeRequest::decode(&mut cur, api_version)
                    .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
                let resp = handle_handshake(&req, &mut auth, &cfg.enabled_sasl_mechanisms);
                let error_code = resp.error_code;
                write_response(stream, api_key, api_version, corr_id, &resp).await?;
                if error_code != 0 {
                    return Err(RaftHandshakeError::Sasl(format!(
                        "handshake error_code={error_code}"
                    )));
                }
            }
            API_KEY_SASL_AUTHENTICATE => {
                let mut cur = body.as_slice();
                let req = SaslAuthenticateRequest::decode(&mut cur, api_version)
                    .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
                let mech = match &auth {
                    ConnectionAuth::Negotiating { mechanism, .. } => *mechanism,
                    _ => {
                        return Err(RaftHandshakeError::Sasl(
                            "authenticate before handshake".into(),
                        ));
                    }
                };
                let resp = match mech {
                    SaslMechanism::Plain => {
                        handle_authenticate_plain(&req, &mut auth, &cfg.plain_credentials)
                    }
                    SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                        let controller = cfg.controller.get().ok_or_else(|| {
                            RaftHandshakeError::Sasl(
                                "controller handle not initialised for SCRAM lookup".into(),
                            )
                        })?;
                        handle_authenticate_scram(&req, &mut auth, controller.as_ref())
                    }
                    // The controller listener authenticates peer brokers, not
                    // token-bearing clients; OAUTHBEARER is a client mechanism
                    // and is not offered for inter-broker auth.
                    SaslMechanism::OAuthBearer => {
                        return Err(RaftHandshakeError::Sasl(
                            "OAUTHBEARER is not supported on the controller listener".into(),
                        ));
                    }
                };
                let error_code = resp.error_code;
                write_response(stream, api_key, api_version, corr_id, &resp).await?;
                if error_code != 0 {
                    return Err(RaftHandshakeError::Sasl(format!(
                        "authenticate error_code={error_code}"
                    )));
                }
                if auth.is_authenticated() {
                    return Ok(());
                }
                // SCRAM second round: loop and read the next
                // SaslAuthenticate frame. Sanity-check we're still
                // mid-SCRAM and not stuck in a bad state.
                debug_assert!(
                    matches!(
                        auth,
                        ConnectionAuth::Negotiating {
                            exchange: SaslExchange::Scram(_),
                            ..
                        }
                    ),
                    "expected SCRAM continuation after non-authenticated success"
                );
            }
            other => {
                return Err(RaftHandshakeError::Protocol(format!(
                    "unexpected api_key={other} during handshake"
                )));
            }
        }
    }
}

// ────────────────────────────────────────────────────────────────────────
// Frame helpers (server-side inverse of `network::client::round_trip`).
// ────────────────────────────────────────────────────────────────────────

/// Read one length-prefixed Kafka request frame, peel off the
/// `RequestHeader` (v1 or v2), and return `(api_key, api_version,
/// correlation_id, body_bytes)`.
///
/// Header parsing matches the outbound encoder in
/// `network::client::round_trip`:
/// - v1 (non-flexible): `api_key i16 | api_version i16 | corr_id i32 |
///   client_id i16-length-prefixed bytes`.
/// - v2 (flexible, used by `SaslAuthenticate v2+`): v1 layout plus a
///   trailing `0x00` tagged-fields byte.
async fn read_kafka_request(
    stream: &mut dyn DuplexStream,
) -> Result<(i16, i16, i32, Vec<u8>), RaftHandshakeError> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let size = u32::from_be_bytes(size_buf) as usize;
    let mut frame = vec![0u8; size];
    stream.read_exact(&mut frame).await?;
    if frame.len() < 10 {
        return Err(RaftHandshakeError::Protocol("short request header".into()));
    }
    let api_key = i16::from_be_bytes([frame[0], frame[1]]);
    let api_version = i16::from_be_bytes([frame[2], frame[3]]);
    let corr_id = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
    let client_id_len = i16::from_be_bytes([frame[8], frame[9]]);
    let mut cursor: usize = 10;
    if client_id_len >= 0 {
        let cid_len = usize::try_from(client_id_len)
            .map_err(|_| RaftHandshakeError::Protocol("client_id_len overflow".into()))?;
        let cid_end = cursor
            .checked_add(cid_len)
            .ok_or_else(|| RaftHandshakeError::Protocol("client_id_len overflow".into()))?;
        if cid_end > frame.len() {
            return Err(RaftHandshakeError::Protocol(
                "client_id extends past frame".into(),
            ));
        }
        cursor = cid_end;
    }
    // Flexible request header (v2) for SaslAuthenticate v2+: a single
    // tagged-fields byte (always 0 for empty) follows client_id. Other
    // pre-auth APIs (SaslHandshake v0/v1, ApiVersions v0) use the
    // non-flexible v1 header — no extra byte.
    if is_request_header_flexible(api_key, api_version) {
        if cursor >= frame.len() {
            return Err(RaftHandshakeError::Protocol(
                "missing tagged-fields byte in flexible request header".into(),
            ));
        }
        cursor += 1;
    }
    let body = frame[cursor..].to_vec();
    Ok((api_key, api_version, corr_id, body))
}

/// Encode `resp`, prepend the `ResponseHeader` (v0 or v1 per the rules
/// below), and write the length-prefixed frame.
async fn write_response<R: Encode>(
    stream: &mut dyn DuplexStream,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    resp: &R,
) -> Result<(), RaftHandshakeError> {
    let flexible = is_response_header_flexible(api_key, api_version);
    let body_len = resp.encoded_len(api_version);
    let header_len = 4 + usize::from(flexible);
    let total = header_len + body_len;
    let total_u32 = u32::try_from(total)
        .map_err(|_| RaftHandshakeError::Protocol("response frame exceeds u32".into()))?;

    let mut out = Vec::with_capacity(4 + total);
    out.extend_from_slice(&total_u32.to_be_bytes());
    out.extend_from_slice(&corr_id.to_be_bytes());
    if flexible {
        out.push(0); // empty tagged-fields
    }
    resp.encode(&mut out, api_version)
        .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
    stream.write_all(&out).await?;
    Ok(())
}

/// Request-header flexibility rules.
///
/// Mirrors the encoder side in `network::client::round_trip` where the
/// caller passes `flexible = true` only for `SaslAuthenticate v2+`. All
/// other pre-auth APIs use the non-flexible v1 header.
fn is_request_header_flexible(api_key: i16, api_version: i16) -> bool {
    match api_key {
        API_KEY_SASL_AUTHENTICATE => api_version >= 2,
        // SaslHandshake v0/v1 — non-flexible. ApiVersions v0 — non-flexible.
        _ => false,
    }
}

/// Response-header flexibility rules.
///
/// - `SaslHandshake (17)` — non-flexible at every version we accept.
/// - `SaslAuthenticate (36)` — flexible from v2.
/// - `ApiVersions (18)` — *always* v0 response header per Kafka spec,
///   regardless of body flexibility. The Kafka clients special-case this.
fn is_response_header_flexible(api_key: i16, api_version: i16) -> bool {
    // SaslHandshake (17) and ApiVersions (18) keep the v0 response header
    // at every version we accept; only SaslAuthenticate (36) flips to a
    // flexible response header starting at v2.
    match api_key {
        API_KEY_SASL_AUTHENTICATE => api_version >= 2,
        _ => false,
    }
}

/// Minimal hand-rolled `ApiVersionsResponse v0`. Advertises only the
/// pre-auth APIs (17 / 36 / 18). Our own `InterBrokerClient` skips
/// `ApiVersions`, so this exists purely to satisfy JVM-style peers that
/// always send it first.
fn build_api_versions_response(corr_id: i32) -> Vec<u8> {
    // v0 body: error_code(i16) + api_versions array(i32 len, repeats of
    // {api_key i16, min i16, max i16}) + throttle_time_ms(i32).
    let mut body = Vec::with_capacity(2 + 4 + 3 * 6 + 4);
    body.extend_from_slice(&0i16.to_be_bytes()); // error_code
    body.extend_from_slice(&3i32.to_be_bytes()); // array length
    for k in [
        API_KEY_SASL_HANDSHAKE,
        API_KEY_SASL_AUTHENTICATE,
        API_KEY_API_VERSIONS,
    ] {
        body.extend_from_slice(&k.to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes()); // min_version
        body.extend_from_slice(&2i16.to_be_bytes()); // max_version
    }
    body.extend_from_slice(&0i32.to_be_bytes()); // throttle_time_ms

    // ApiVersions response header is always v0 — no tagged-fields byte.
    // The response body is fixed size (3 entries × 6 bytes + 10 bytes of
    // scalars = 28 bytes), so `total` is well under u32::MAX. We assert
    // this explicitly so the cast can't silently truncate if someone
    // later expands the advertised API list.
    let total = 4 + body.len();
    let total_u32 = u32::try_from(total).expect("ApiVersions response fits in u32");
    let mut out = Vec::with_capacity(4 + total);
    out.extend_from_slice(&total_u32.to_be_bytes());
    out.extend_from_slice(&corr_id.to_be_bytes());
    out.extend_from_slice(&body);
    out
}

#[cfg(test)]
mod tests {
    //! Narrow unit coverage. The richer behavioural tests (PLAIN happy
    //! path, SCRAM two-round dance, bad-creds rejection, TLS termination)
    //! live in `tests/raft_sasl.rs` (T9) where a real two-broker raft
    //! cluster is spun up. Here we just verify trait wiring + the
    //! Plaintext short-circuit predicate so a regression that flips
    //! `requires_*` would be caught at this layer.

    use super::*;

    #[test]
    fn plaintext_passthrough_short_circuits() {
        let cfg = BrokerRaftHandshake {
            tls_acceptor: None,
            plain_credentials: HashMap::new(),
            enabled_sasl_mechanisms: vec![],
            protocol: ListenerProtocol::Plaintext,
            controller: Arc::new(OnceCell::new()),
        };
        // `upgrade(TcpStream)` requires a real TCP socket, so we
        // exercise the short-circuit predicates directly here. The full
        // upgrade-path is exercised end-to-end in T9.
        assert!(!cfg.protocol.requires_tls());
        assert!(!cfg.protocol.requires_sasl());
    }

    #[test]
    fn header_flexibility_table_matches_outbound_encoder() {
        // SaslHandshake — never flexible (v0/v1).
        assert!(!is_request_header_flexible(API_KEY_SASL_HANDSHAKE, 0));
        assert!(!is_request_header_flexible(API_KEY_SASL_HANDSHAKE, 1));
        assert!(!is_response_header_flexible(API_KEY_SASL_HANDSHAKE, 0));
        assert!(!is_response_header_flexible(API_KEY_SASL_HANDSHAKE, 1));

        // SaslAuthenticate — flexible from v2.
        assert!(!is_request_header_flexible(API_KEY_SASL_AUTHENTICATE, 1));
        assert!(is_request_header_flexible(API_KEY_SASL_AUTHENTICATE, 2));
        assert!(!is_response_header_flexible(API_KEY_SASL_AUTHENTICATE, 1));
        assert!(is_response_header_flexible(API_KEY_SASL_AUTHENTICATE, 2));

        // ApiVersions — response header always v0 per Kafka spec.
        assert!(!is_response_header_flexible(API_KEY_API_VERSIONS, 0));
        assert!(!is_response_header_flexible(API_KEY_API_VERSIONS, 3));
    }
}
