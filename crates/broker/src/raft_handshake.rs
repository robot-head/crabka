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

// Exercised via the runtime path and integration tests. Unit coverage in this
// file is deliberately narrow — see the `tests` module docstring.
#![allow(dead_code)]

use std::{collections::HashMap, sync::Arc};

use crabka_protocol::{
    Decode, Encode,
    owned::{
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_handshake_request::SaslHandshakeRequest,
    },
};
use crabka_raft::{ControllerHandle, DuplexStream, RaftHandshakeError, RaftListenerHandshake};
use crabka_security::{ListenerProtocol, SaslMechanism};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::OnceCell,
};
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

/// Fixed-size prefix of a request header before the client-id bytes:
/// `api_key i16 + api_version i16 + correlation_id i32 + client_id_len i16`.
const REQUEST_HEADER_PREFIX_LEN: usize = 10;

/// `SaslAuthenticate (36)` switches to flexible (v2) request *and* response
/// headers starting at this `api_version` (KIP-482 flexible-versions cutover).
const SASL_AUTHENTICATE_FLEXIBLE_VERSION: i16 = 2;

/// Pre-auth APIs advertised in the hand-rolled `ApiVersionsResponse v0`,
/// in wire order: `SaslHandshake`, `SaslAuthenticate`, `ApiVersions`.
const ADVERTISED_PRE_AUTH_APIS: [i16; 3] = [
    API_KEY_SASL_HANDSHAKE,
    API_KEY_SASL_AUTHENTICATE,
    API_KEY_API_VERSIONS,
];

/// Version range advertised for every pre-auth API in the minimal
/// `ApiVersionsResponse v0` (covers `SaslHandshake` v0-1, `SaslAuthenticate`
/// v0-2, `ApiVersions` v0 — the versions the inbound state machine accepts).
const ADVERTISED_MIN_VERSION: i16 = 0;
/// See [`ADVERTISED_MIN_VERSION`].
const ADVERTISED_MAX_VERSION: i16 = 2;

/// Per-broker handshake adapter. Constructed in `Broker::start` and passed
/// into `ControllerConfig::handshake`.
pub struct BrokerRaftHandshake {
    pub tls_acceptor: Option<TlsAcceptor>,
    pub plain_credentials: HashMap<String, String>,
    pub enabled_sasl_mechanisms: Vec<SaslMechanism>,
    pub protocol: ListenerProtocol,
    pub controller: ControllerHandleArc,
    /// Authorizer used to gate controller RPCs after authentication
    /// (H-1). Authentication proves *who* the peer is; this enforces that
    /// the authenticated principal is allowed to drive controller/raft
    /// RPCs (`CLUSTER_ACTION` on `Cluster("kafka-cluster")`). With the
    /// default `AllowAllAuthorizer`, every principal is allowed, so
    /// dev/single-node is unaffected; `SimpleAclAuthorizer` grants
    /// super-users.
    pub authorizer: Arc<dyn crate::authorizer::Authorizer>,
}

/// Initial per-connection auth state for an unauthenticated SASL peer.
fn pre_auth_state() -> ConnectionAuth {
    ConnectionAuth::Anonymous
}

impl BrokerRaftHandshake {
    /// H-1: authorize an authenticated controller-listener peer for
    /// controller/raft RPCs. Authentication established *who* the peer is;
    /// this enforces that the principal holds `CLUSTER_ACTION` on
    /// `Cluster("kafka-cluster")` — the same gate the inter-broker
    /// control-plane RPCs (`BrokerHeartbeat`, etc.) use — evaluated against
    /// the controller's *current* metadata image so ACL changes take
    /// effect for new connections. On Deny the connection is dropped.
    fn authorize_cluster_action(
        &self,
        principal: &crabka_security::Principal,
        peer: &std::net::SocketAddr,
    ) -> Result<(), RaftHandshakeError> {
        use crabka_metadata::{AclOperation, ResourceType};

        use crate::authorizer::{AuthorizationRequest, AuthorizationResult};

        // The image is reached through the late-bound controller handle
        // (the same cell used for SCRAM lookup). If it is not yet wired the
        // controller cannot be operating, so fail closed.
        let controller = self.controller.get().ok_or_else(|| {
            RaftHandshakeError::Sasl(
                "controller handle not initialised for CLUSTER_ACTION authorization".into(),
            )
        })?;
        let image = controller.current_image();
        let decision = self.authorizer.authorize(
            &*image,
            &AuthorizationRequest {
                principal,
                host: peer,
                resource_type: ResourceType::Cluster,
                resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                operation: AclOperation::ClusterAction,
            },
        );
        if decision == AuthorizationResult::Deny {
            tracing::warn!(
                principal = %principal.name,
                peer = %peer,
                "denying controller-listener peer: principal lacks CLUSTER_ACTION on kafka-cluster"
            );
            return Err(RaftHandshakeError::Sasl(
                "principal not authorized for CLUSTER_ACTION on the controller listener".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl RaftListenerHandshake for BrokerRaftHandshake {
    async fn upgrade(
        &self,
        stream: TcpStream,
    ) -> Result<Box<dyn DuplexStream>, RaftHandshakeError> {
        // Capture the peer address before the stream is consumed by TLS
        // termination — it is the `host` of the authorization request.
        let peer = stream
            .peer_addr()
            .map_err(|e| RaftHandshakeError::Tls(e.to_string()))?;

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
        //    The SASL exchange authenticates the peer and yields its
        //    `Principal`; H-1 then authorizes that principal for
        //    controller RPCs before the connection is handed to the raft
        //    engine. A non-SASL listener (Plaintext is short-circuited to
        //    `None` upstream, so here that's TLS-only `Ssl`) has no
        //    authenticated identity to authorize at this layer — we do not
        //    extract an mTLS client-cert principal here — so the
        //    CLUSTER_ACTION gate is skipped for it (an unusual config).
        if self.protocol.requires_sasl() {
            let principal = run_inbound_sasl(&mut *stream, self).await?;
            self.authorize_cluster_action(&principal, &peer)?;
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
/// SCRAM rounds. Returns the authenticated [`Principal`] once
/// `auth.is_authenticated()` (so `upgrade` can authorize it) and
/// `Err(...)` if the peer sent an unexpected frame or auth failed.
async fn run_inbound_sasl(
    stream: &mut dyn DuplexStream,
    cfg: &BrokerRaftHandshake,
) -> Result<crabka_security::Principal, RaftHandshakeError> {
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
                    // GSSAPI server-side accept on the controller listener is
                    // wired in a later GSSAPI task.
                    SaslMechanism::Gssapi => {
                        return Err(RaftHandshakeError::Sasl(
                            "GSSAPI is not yet wired on the controller listener".into(),
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
                    // Hand the authenticated principal back to `upgrade` for
                    // the CLUSTER_ACTION authorization gate (H-1).
                    let principal = auth.principal().cloned().ok_or_else(|| {
                        RaftHandshakeError::Sasl(
                            "authenticated connection missing principal".into(),
                        )
                    })?;
                    return Ok(principal);
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
    if frame.len() < REQUEST_HEADER_PREFIX_LEN {
        return Err(RaftHandshakeError::Protocol("short request header".into()));
    }
    let api_key = i16::from_be_bytes([frame[0], frame[1]]);
    let api_version = i16::from_be_bytes([frame[2], frame[3]]);
    let corr_id = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
    let client_id_len = i16::from_be_bytes([frame[8], frame[9]]);
    let mut cursor: usize = REQUEST_HEADER_PREFIX_LEN;
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
        API_KEY_SASL_AUTHENTICATE => api_version >= SASL_AUTHENTICATE_FLEXIBLE_VERSION,
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
        API_KEY_SASL_AUTHENTICATE => api_version >= SASL_AUTHENTICATE_FLEXIBLE_VERSION,
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
    let api_count =
        i32::try_from(ADVERTISED_PRE_AUTH_APIS.len()).expect("advertised API count fits i32");
    let mut body = Vec::with_capacity(2 + 4 + ADVERTISED_PRE_AUTH_APIS.len() * 6 + 4);
    body.extend_from_slice(&0i16.to_be_bytes()); // error_code
    body.extend_from_slice(&api_count.to_be_bytes()); // array length
    for k in ADVERTISED_PRE_AUTH_APIS {
        body.extend_from_slice(&k.to_be_bytes());
        body.extend_from_slice(&ADVERTISED_MIN_VERSION.to_be_bytes()); // min_version
        body.extend_from_slice(&ADVERTISED_MAX_VERSION.to_be_bytes()); // max_version
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
    //! live in `tests/raft_sasl.rs` where a real two-broker raft
    //! cluster is spun up. Here we just verify trait wiring + the
    //! Plaintext short-circuit predicate so a regression that flips
    //! `requires_*` would be caught at this layer.

    use assert2::assert;
    use bytes::BufMut;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        time::{Duration, timeout},
    };

    use crate::test_support::DenyAll;

    struct FixedResp(&'static [u8]);

    impl Encode for FixedResp {
        fn encode<B: BufMut>(
            &self,
            buf: &mut B,
            _version: i16,
        ) -> Result<(), crabka_protocol::ProtocolError> {
            buf.put_slice(self.0);
            Ok(())
        }

        fn encoded_len(&self, _version: i16) -> usize {
            self.0.len()
        }
    }

    fn request_frame(
        api_key: i16,
        api_version: i16,
        corr_id: i32,
        client_id: Option<&[u8]>,
        flexible: bool,
        body: &[u8],
    ) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&api_key.to_be_bytes());
        frame.extend_from_slice(&api_version.to_be_bytes());
        frame.extend_from_slice(&corr_id.to_be_bytes());
        match client_id {
            Some(id) => {
                let len = i16::try_from(id.len()).expect("client id fits i16");
                frame.extend_from_slice(&len.to_be_bytes());
                frame.extend_from_slice(id);
            }
            None => frame.extend_from_slice(&(-1i16).to_be_bytes()),
        }
        if flexible {
            frame.push(0);
        }
        frame.extend_from_slice(body);

        let mut out = Vec::new();
        let len = u32::try_from(frame.len()).expect("frame fits u32");
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&frame);
        out
    }

    async fn read_request_from_frame(
        frame: Vec<u8>,
    ) -> Result<(i16, i16, i32, Vec<u8>), RaftHandshakeError> {
        let (mut client, mut server) = tokio::io::duplex(4096);
        client.write_all(&frame).await.expect("write request frame");
        read_kafka_request(&mut server).await
    }

    async fn read_response_frame(stream: &mut tokio::io::DuplexStream) -> Vec<u8> {
        timeout(Duration::from_secs(1), async {
            let mut size_buf = [0u8; 4];
            stream
                .read_exact(&mut size_buf)
                .await
                .expect("response size");
            let size = u32::from_be_bytes(size_buf) as usize;
            let mut frame = vec![0u8; size];
            stream.read_exact(&mut frame).await.expect("response frame");
            frame
        })
        .await
        .expect("timely response")
    }

    fn sasl_test_config() -> BrokerRaftHandshake {
        let mut plain_credentials = HashMap::new();
        plain_credentials.insert("broker".to_string(), "secret".to_string());
        BrokerRaftHandshake {
            tls_acceptor: None,
            plain_credentials,
            enabled_sasl_mechanisms: vec![SaslMechanism::Plain],
            protocol: ListenerProtocol::SaslPlaintext,
            controller: Arc::new(OnceCell::new()),
            authorizer: Arc::new(crate::authorizer::AllowAllAuthorizer),
        }
    }

    fn sasl_handshake_body() -> Vec<u8> {
        let mut body = bytes::BytesMut::new();
        SaslHandshakeRequest {
            mechanism: "PLAIN".to_string(),
            ..Default::default()
        }
        .encode(&mut body, 1)
        .expect("encode sasl handshake");
        body.to_vec()
    }

    fn sasl_authenticate_body() -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(0);
        payload.extend_from_slice(b"broker");
        payload.push(0);
        payload.extend_from_slice(b"secret");

        let mut body = bytes::BytesMut::new();
        SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from(payload),
            ..Default::default()
        }
        .encode(&mut body, 2)
        .expect("encode sasl authenticate");
        body.to_vec()
    }

    use super::*;

    #[test]
    fn plaintext_passthrough_short_circuits() {
        let cfg = BrokerRaftHandshake {
            tls_acceptor: None,
            plain_credentials: HashMap::new(),
            enabled_sasl_mechanisms: vec![],
            protocol: ListenerProtocol::Plaintext,
            controller: Arc::new(OnceCell::new()),
            authorizer: Arc::new(crate::authorizer::AllowAllAuthorizer),
        };
        // `upgrade(TcpStream)` requires a real TCP socket, so we
        // exercise the short-circuit predicates directly here. The full
        // upgrade-path is exercised end-to-end in integration tests.
        assert!(!cfg.protocol.requires_tls());
        assert!(!cfg.protocol.requires_sasl());
    }

    #[tokio::test]
    async fn authorize_cluster_action_denies_when_authorizer_denies() {
        let dir = tempfile::tempdir().expect("tempdir");
        let controller = Arc::new(
            crabka_raft::Controller::start(crabka_raft::ControllerConfig::for_tests(
                crabka_raft::NodeId(1),
                dir.path().to_path_buf(),
            ))
            .await
            .expect("controller"),
        );
        let controller_cell = Arc::new(OnceCell::new());
        assert!(controller_cell.set(controller.clone()).is_ok());

        let cfg = BrokerRaftHandshake {
            tls_acceptor: None,
            plain_credentials: HashMap::new(),
            enabled_sasl_mechanisms: vec![SaslMechanism::Plain],
            protocol: ListenerProtocol::SaslPlaintext,
            controller: controller_cell,
            authorizer: Arc::new(DenyAll),
        };
        let principal = crabka_security::Principal {
            name: "broker".to_string(),
            auth_method: crabka_security::AuthMethod::SaslPlain,
            groups: Vec::new(),
        };
        let peer = "127.0.0.1:9092".parse().expect("peer");

        let err = cfg
            .authorize_cluster_action(&principal, &peer)
            .expect_err("deny must reject");
        assert!(matches!(err, RaftHandshakeError::Sasl(msg) if msg.contains("not authorized")));

        drop(cfg);
        let controller = Arc::try_unwrap(controller)
            .unwrap_or_else(|_| panic!("controller handle still shared after auth test"));
        controller.shutdown().await;
    }

    #[test]
    fn header_flexibility_table_matches_outbound_encoder() {
        // SaslHandshake — never flexible (v0/v1). SaslAuthenticate —
        // flexible from v2.
        let request_cases = [
            (API_KEY_SASL_HANDSHAKE, 0, false),
            (API_KEY_SASL_HANDSHAKE, 1, false),
            (API_KEY_SASL_AUTHENTICATE, 1, false),
            (API_KEY_SASL_AUTHENTICATE, 2, true),
        ];
        for (api_key, version, want) in request_cases {
            assert!(
                is_request_header_flexible(api_key, version) == want,
                "request api_key {api_key} v{version}"
            );
        }

        // Response headers mirror the request rules for SaslHandshake /
        // SaslAuthenticate; ApiVersions — response header always v0 per
        // Kafka spec.
        let response_cases = [
            (API_KEY_SASL_HANDSHAKE, 0, false),
            (API_KEY_SASL_HANDSHAKE, 1, false),
            (API_KEY_SASL_AUTHENTICATE, 1, false),
            (API_KEY_SASL_AUTHENTICATE, 2, true),
            (API_KEY_API_VERSIONS, 0, false),
            (API_KEY_API_VERSIONS, 3, false),
        ];
        for (api_key, version, want) in response_cases {
            assert!(
                is_response_header_flexible(api_key, version) == want,
                "response api_key {api_key} v{version}"
            );
        }
    }

    #[tokio::test]
    async fn read_kafka_request_decodes_nonflex_and_flexible_headers() {
        let nonflex = request_frame(17, 1, 42, None, false, b"plain-body");
        let decoded = read_request_from_frame(nonflex)
            .await
            .expect("nonflex request");
        assert!(decoded == (17, 1, 42, b"plain-body".to_vec()));

        let flex = request_frame(36, 2, 43, Some(b"c"), true, b"auth-body");
        let decoded = read_request_from_frame(flex).await.expect("flex request");
        assert!(decoded == (36, 2, 43, b"auth-body".to_vec()));
    }

    #[tokio::test]
    async fn read_kafka_request_rejects_short_and_truncated_headers() {
        let mut short = Vec::new();
        short.extend_from_slice(&9u32.to_be_bytes());
        short.extend_from_slice(&[0; 9]);

        let mut truncated_client = Vec::new();
        truncated_client.extend_from_slice(&12u32.to_be_bytes());
        truncated_client.extend_from_slice(&17i16.to_be_bytes());
        truncated_client.extend_from_slice(&1i16.to_be_bytes());
        truncated_client.extend_from_slice(&7i32.to_be_bytes());
        truncated_client.extend_from_slice(&3i16.to_be_bytes());
        truncated_client.extend_from_slice(b"xy");

        let missing_tag = request_frame(36, 2, 44, Some(b"c"), false, b"");

        let cases = [
            (short, "short request header"),
            (truncated_client, "client_id extends past frame"),
            (missing_tag, "missing tagged-fields byte"),
        ];
        for (frame, want_msg) in cases {
            let got = read_request_from_frame(frame).await;
            assert!(
                matches!(
                    &got,
                    Err(RaftHandshakeError::Protocol(msg)) if msg.contains(want_msg)
                ),
                "want protocol error containing {want_msg:?}, got {got:?}"
            );
        }
    }

    #[tokio::test]
    async fn read_kafka_request_accepts_exact_client_id_end_boundary() {
        let frame = request_frame(17, 1, 45, Some(b"client"), false, b"");
        let decoded = read_request_from_frame(frame).await.expect("exact header");
        assert!(decoded == (17, 1, 45, Vec::new()));
    }

    #[tokio::test]
    async fn read_kafka_request_accepts_exact_header_prefix_frame() {
        // A null client id and empty body make the frame exactly
        // REQUEST_HEADER_PREFIX_LEN bytes — the minimum legal frame and the
        // only length where the short-header guard's strict `<` matters.
        let frame = request_frame(17, 1, 46, None, false, b"");
        let decoded = read_request_from_frame(frame).await.expect("exact prefix");
        assert!(decoded == (17, 1, 46, Vec::new()));
    }

    #[tokio::test]
    async fn write_response_uses_expected_header_versions() {
        let (mut client, mut server) = tokio::io::duplex(128);
        let writer = tokio::spawn(async move {
            write_response(
                &mut server,
                API_KEY_SASL_AUTHENTICATE,
                2,
                77,
                &FixedResp(&[0xaa, 0xbb]),
            )
            .await
            .expect("write flexible");
        });
        let frame = read_response_frame(&mut client).await;
        writer.await.expect("writer");
        // corr_id 77 BE + empty tagged-fields byte (flexible header) + body.
        let expected: Vec<u8> = [77i32.to_be_bytes().as_slice(), &[0x00], &[0xaa, 0xbb]].concat();
        assert!(frame == expected);

        let (mut client, mut server) = tokio::io::duplex(128);
        let writer = tokio::spawn(async move {
            write_response(
                &mut server,
                API_KEY_SASL_HANDSHAKE,
                1,
                78,
                &FixedResp(&[0xcc]),
            )
            .await
            .expect("write nonflex");
        });
        let frame = read_response_frame(&mut client).await;
        writer.await.expect("writer");
        assert!(&frame[0..4] == &78i32.to_be_bytes());
        assert!(&frame[4..] == &[0xcc]);
    }

    #[test]
    fn api_versions_response_has_expected_frame_shape() {
        let bytes = build_api_versions_response(99);
        // Byte-exact v0 frame: size(32) | corr_id(99) | error_code(0) |
        // array len(3) | {api_key, min 0, max 2} × [17, 36, 18] |
        // throttle_time_ms(0).
        let expected: Vec<u8> = [
            &32u32.to_be_bytes()[..],
            &99i32.to_be_bytes(),
            &0i16.to_be_bytes(),
            &3i32.to_be_bytes(),
            &17i16.to_be_bytes(),
            &0i16.to_be_bytes(),
            &2i16.to_be_bytes(),
            &36i16.to_be_bytes(),
            &0i16.to_be_bytes(),
            &2i16.to_be_bytes(),
            &18i16.to_be_bytes(),
            &0i16.to_be_bytes(),
            &2i16.to_be_bytes(),
            &0i32.to_be_bytes(),
        ]
        .concat();
        assert!(bytes == expected);
    }

    #[tokio::test]
    async fn run_inbound_sasl_allows_api_versions_before_plain_authentication() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let server = tokio::spawn(async move {
            let cfg = sasl_test_config();
            run_inbound_sasl(&mut server, &cfg).await
        });

        client
            .write_all(&request_frame(
                API_KEY_API_VERSIONS,
                0,
                1,
                Some(b"c"),
                false,
                b"",
            ))
            .await
            .expect("write api versions");
        let api_versions = read_response_frame(&mut client).await;
        assert!(&api_versions[0..4] == &1i32.to_be_bytes());

        client
            .write_all(&request_frame(
                API_KEY_SASL_HANDSHAKE,
                1,
                2,
                Some(b"c"),
                false,
                &sasl_handshake_body(),
            ))
            .await
            .expect("write handshake");
        let handshake = read_response_frame(&mut client).await;
        assert!(&handshake[0..4] == &2i32.to_be_bytes());
        assert!(&handshake[4..6] == &0i16.to_be_bytes());

        client
            .write_all(&request_frame(
                API_KEY_SASL_AUTHENTICATE,
                2,
                3,
                Some(b"c"),
                true,
                &sasl_authenticate_body(),
            ))
            .await
            .expect("write authenticate");
        let authenticate = read_response_frame(&mut client).await;
        // corr_id 3 BE + empty tagged-fields byte (flexible header) +
        // error_code 0.
        assert!(authenticate[0..7] == [0, 0, 0, 3, 0, 0, 0]);

        let principal = server.await.expect("server task").expect("authenticated");
        assert!(
            (principal.name.as_str(), principal.auth_method)
                == ("broker", crabka_security::AuthMethod::SaslPlain)
        );
    }

    #[tokio::test]
    async fn run_inbound_sasl_rejects_disallowed_request_before_authentication() {
        let (mut client, mut server) = tokio::io::duplex(128);
        let server = tokio::spawn(async move {
            let cfg = sasl_test_config();
            run_inbound_sasl(&mut server, &cfg).await
        });
        client
            .write_all(&request_frame(1, 0, 1, Some(b"c"), false, b""))
            .await
            .expect("write forbidden request");

        let err = server
            .await
            .expect("server task")
            .expect_err("pre-auth request rejected");
        assert!(
            matches!(err, RaftHandshakeError::Sasl(msg) if msg.contains("pre-auth request api_key=1 rejected"))
        );
    }
}
