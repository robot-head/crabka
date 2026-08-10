//! Inbound TLS + SASL handshake for the controller listener.
//!
//! This module is the mirror image of the outbound auth flow of
//! `network::client::InterBrokerClient`. It reuses the
//! `network::auth::handle_handshake` and `handle_authenticate_*` state
//! machines, so the controller listener and the data plane share one source
//! of truth.
//!
//! The frame helpers `read_kafka_request` and `write_response` are the
//! server-side inverse of `network::client::round_trip`. The header
//! flexibility rules match exactly:
//!   - `SaslHandshake (17)` v0+ uses a non-flexible response header, a bare
//!     `correlation_id`.
//!   - `SaslAuthenticate (36)` v2+ uses a flexible response header, a
//!     `correlation_id` and a 1-byte tagged-fields section.
//!   - The `ApiVersions (18)` response header is *always* v0 by Kafka spec.

use std::{collections::HashMap, sync::Arc};

use crabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::{self, ApiVersionsRequest},
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        request_header::RequestHeader,
        sasl_authenticate_request::{self, SaslAuthenticateRequest},
        sasl_handshake_request::{self, SaslHandshakeRequest},
    },
};
use crabka_raft::{
    ControllerHandle, DuplexStream, RaftConnection, RaftHandshakeError, RaftListenerHandshake,
};
use crabka_security::{ListenerProtocol, SaslMechanism};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::OnceCell,
};
use tokio_rustls::TlsAcceptor;

use crate::network::auth::{
    ConnectionAuth, SaslExchange, handle_authenticate_gssapi, handle_authenticate_plain,
    handle_authenticate_scram, handle_handshake, is_pre_auth_allowed,
};

/// Late-bound handle to the broker's [`ControllerHandle`].
///
/// The broker constructs the handshake *before* `crabka_raft::Controller::start`
/// returns, and moves it into `ControllerConfig::handshake`, so the controller
/// is only available later. This type therefore carries an
/// `Arc<OnceCell<…>>`, and `Broker::start` calls `OnceCell::set` on it once
/// the controller is built. The SCRAM credential lookup, one round for each
/// authenticate, is the only code path that touches the cell.
pub type ControllerHandleArc = Arc<OnceCell<Arc<ControllerHandle>>>;

/// API key constants. They match the wire-protocol IDs used elsewhere.
const API_KEY_SASL_HANDSHAKE: i16 = 17;
const API_KEY_SASL_AUTHENTICATE: i16 = 36;
const API_KEY_API_VERSIONS: i16 = 18;

/// `SaslAuthenticate (36)` switches to flexible (v2) request *and* response
/// headers at this `api_version`. This is the KIP-482 flexible-versions
/// cutover.
const SASL_AUTHENTICATE_FLEXIBLE_VERSION: i16 = 2;

/// Per-broker handshake adapter. `Broker::start` constructs it and passes it
/// into `ControllerConfig::handshake`.
pub struct BrokerRaftHandshake {
    pub tls_acceptor: Option<TlsAcceptor>,
    pub plain_credentials: HashMap<String, String>,
    pub enabled_sasl_mechanisms: Vec<SaslMechanism>,
    pub gssapi: Option<crabka_security::gssapi::GssapiConfig>,
    pub protocol: ListenerProtocol,
    pub controller: ControllerHandleArc,
    /// Maximum Kafka handshake frame body accepted before authentication.
    pub max_frame_bytes: usize,
    /// Authorizer that gates controller RPCs after authentication (H-1).
    ///
    /// Authentication proves *who* the peer is. This authorizer enforces that
    /// the authenticated principal may drive controller and raft RPCs, that
    /// is, `CLUSTER_ACTION` on `Cluster("kafka-cluster")`. The default
    /// `AllowAllAuthorizer` allows every principal, so it does not change
    /// dev and single-node setups. `SimpleAclAuthorizer` grants super-users.
    pub authorizer: Arc<dyn crate::authorizer::Authorizer>,
}

/// Initial per-connection auth state for an unauthenticated SASL peer.
fn pre_auth_state() -> ConnectionAuth {
    ConnectionAuth::Anonymous
}

impl BrokerRaftHandshake {
    /// H-1: authorizes an authenticated controller-listener peer for
    /// controller and raft RPCs.
    ///
    /// Authentication established *who* the peer is. This method enforces that
    /// the principal holds `CLUSTER_ACTION` on `Cluster("kafka-cluster")`.
    /// That is the same gate the inter-broker control-plane RPCs use, such as
    /// `BrokerHeartbeat`. The method evaluates it against the controller's
    /// *current* metadata image, so ACL changes take effect for new
    /// connections. On Deny, the broker drops the connection.
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

    fn authorize_cluster_alter(
        &self,
        principal: &crabka_security::Principal,
        peer: &std::net::SocketAddr,
    ) -> Result<bool, RaftHandshakeError> {
        use crabka_metadata::{AclOperation, ResourceType};

        use crate::authorizer::{AuthorizationRequest, AuthorizationResult};

        let controller = self.controller.get().ok_or_else(|| {
            RaftHandshakeError::Sasl(
                "controller handle not initialised for Alter authorization".into(),
            )
        })?;
        let image = controller.current_image();
        Ok(self.authorizer.authorize(
            &*image,
            &AuthorizationRequest {
                principal,
                host: peer,
                resource_type: ResourceType::Cluster,
                resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                operation: AclOperation::Alter,
            },
        ) == AuthorizationResult::Allow)
    }
}

#[async_trait::async_trait]
impl RaftListenerHandshake for BrokerRaftHandshake {
    async fn upgrade(&self, stream: TcpStream) -> Result<RaftConnection, RaftHandshakeError> {
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
        let mut cluster_alter_authorized = true;
        if self.protocol.requires_sasl() {
            let principal = run_inbound_sasl(&mut *stream, self).await?;
            self.authorize_cluster_action(&principal, &peer)?;
            cluster_alter_authorized = self.authorize_cluster_alter(&principal, &peer)?;
        }
        Ok(RaftConnection {
            stream,
            cluster_alter_authorized,
        })
    }
}

/// Drives the server-side SASL state machine until the connection
/// authenticates or the function writes an error response.
///
/// The loop invariant is that every iteration reads exactly one Kafka request
/// frame and writes exactly one response frame. The `auth` state machine,
/// `network::auth::ConnectionAuth`, carries continuation state across SCRAM
/// rounds.
///
/// The function returns the authenticated [`Principal`] once
/// `auth.is_authenticated()` holds, so that `upgrade` can authorize it. It
/// returns `Err(...)` if the peer sent an unexpected frame or the auth
/// failed.
async fn run_inbound_sasl(
    stream: &mut dyn DuplexStream,
    cfg: &BrokerRaftHandshake,
) -> Result<crabka_security::Principal, RaftHandshakeError> {
    let mut auth = pre_auth_state();
    loop {
        let (api_key, api_version, corr_id, body) =
            read_kafka_request(stream, cfg.max_frame_bytes).await?;
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
                let mut cur = body.as_slice();
                ApiVersionsRequest::decode(&mut cur, api_version)
                    .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
                let resp = pre_auth_api_versions_response();
                write_response(stream, api_key, api_version, corr_id, &resp).await?;
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
                    SaslMechanism::Gssapi => {
                        let config = cfg.gssapi.as_ref().ok_or_else(|| {
                            RaftHandshakeError::Sasl(
                                "GSSAPI enabled on controller listener without configuration"
                                    .into(),
                            )
                        })?;
                        handle_authenticate_gssapi(&req, &mut auth, config)
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

/// Reads one length-prefixed Kafka request frame, removes the
/// `RequestHeader` (v1 or v2), and returns `(api_key, api_version,
/// correlation_id, body_bytes)`.
///
/// The header parsing matches the outbound encoder in
/// `network::client::round_trip`:
/// - v1, non-flexible: `api_key i16 | api_version i16 | corr_id i32 |
///   client_id i16-length-prefixed bytes`.
/// - v2, flexible, which `SaslAuthenticate v2+` and `ApiVersions v3+` use:
///   the v1 layout plus a tagged-fields section.
async fn read_kafka_request(
    stream: &mut dyn DuplexStream,
    max_frame_bytes: usize,
) -> Result<(i16, i16, i32, Vec<u8>), RaftHandshakeError> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let size = u32::from_be_bytes(size_buf) as usize;
    crate::network::codec::validate_frame_length(size, max_frame_bytes)
        .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
    let mut frame = vec![0u8; size];
    stream.read_exact(&mut frame).await?;
    if frame.len() < 4 {
        return Err(RaftHandshakeError::Protocol("short request header".into()));
    }
    let api_key = i16::from_be_bytes([frame[0], frame[1]]);
    let api_version = i16::from_be_bytes([frame[2], frame[3]]);
    let header_version = if is_request_header_flexible(api_key, api_version) {
        2
    } else {
        1
    };
    let mut body = frame.as_slice();
    let header = RequestHeader::decode(&mut body, header_version)
        .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
    Ok((
        header.request_api_key,
        header.request_api_version,
        header.correlation_id,
        body.to_vec(),
    ))
}

/// Encodes `resp`, prepends the `ResponseHeader` (v0 or v1 by the rules
/// below), and writes the length-prefixed frame.
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
/// Mirrors the generated protocol schema. `SaslAuthenticate` becomes flexible
/// at v2 and `ApiVersions` at v3. `SaslHandshake` v0-v1 stays non-flexible.
fn is_request_header_flexible(api_key: i16, api_version: i16) -> bool {
    match api_key {
        API_KEY_SASL_AUTHENTICATE => api_version >= SASL_AUTHENTICATE_FLEXIBLE_VERSION,
        API_KEY_API_VERSIONS => api_version >= api_versions_request::FLEXIBLE_MIN,
        _ => false,
    }
}

/// Response-header flexibility rules.
///
/// - `SaslHandshake (17)`: non-flexible at every version this module accepts.
/// - `SaslAuthenticate (36)`: flexible from v2.
/// - `ApiVersions (18)`: *always* a v0 response header by Kafka spec,
///   whatever the body flexibility. The Kafka clients special-case it.
fn is_response_header_flexible(api_key: i16, api_version: i16) -> bool {
    // SaslHandshake (17) and ApiVersions (18) keep the v0 response header
    // at every version we accept; only SaslAuthenticate (36) flips to a
    // flexible response header starting at v2.
    match api_key {
        API_KEY_SASL_AUTHENTICATE => api_version >= SASL_AUTHENTICATE_FLEXIBLE_VERSION,
        _ => false,
    }
}

/// Builds the minimal `ApiVersionsResponse` used before SASL authentication.
///
/// Only the three APIs allowed during authentication are advertised. Each
/// entry uses its generated schema range; the ranges are not interchangeable.
/// The generated encoder also handles the v0-v4 body differences, including
/// compact arrays and tagged fields from v3.
fn pre_auth_api_versions_response() -> ApiVersionsResponse {
    ApiVersionsResponse {
        api_keys: vec![
            ApiVersion {
                api_key: API_KEY_SASL_HANDSHAKE,
                min_version: sasl_handshake_request::MIN_VERSION,
                max_version: sasl_handshake_request::MAX_VERSION,
                ..Default::default()
            },
            ApiVersion {
                api_key: API_KEY_SASL_AUTHENTICATE,
                min_version: sasl_authenticate_request::MIN_VERSION,
                max_version: sasl_authenticate_request::MAX_VERSION,
                ..Default::default()
            },
            ApiVersion {
                api_key: API_KEY_API_VERSIONS,
                min_version: api_versions_request::MIN_VERSION,
                max_version: api_versions_request::MAX_VERSION,
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    //! Narrow unit coverage.
    //!
    //! The richer behavioural tests live in `tests/raft_sasl.rs`, which starts
    //! a real two-broker raft cluster. Those cover the PLAIN happy path, the
    //! two SCRAM rounds, bad-credential rejection, and TLS termination. These
    //! tests check only the trait connections and the Plaintext
    //! short-circuit predicate, so that this layer catches a regression that
    //! flips `requires_*`.

    use assert2::assert;
    use bytes::{BufMut, Bytes};
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
        read_kafka_request(&mut server, 4096).await
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
            gssapi: None,
            protocol: ListenerProtocol::SaslPlaintext,
            controller: Arc::new(OnceCell::new()),
            max_frame_bytes: 4096,
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

    fn api_versions_body(version: i16) -> Vec<u8> {
        let mut body = bytes::BytesMut::new();
        ApiVersionsRequest {
            client_software_name: "raft-peer".to_string(),
            client_software_version: "1.0".to_string(),
            ..Default::default()
        }
        .encode(&mut body, version)
        .expect("encode api versions");
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
            gssapi: None,
            protocol: ListenerProtocol::Plaintext,
            controller: Arc::new(OnceCell::new()),
            max_frame_bytes: 4096,
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
            gssapi: None,
            protocol: ListenerProtocol::SaslPlaintext,
            controller: controller_cell,
            max_frame_bytes: 4096,
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
            (API_KEY_API_VERSIONS, 2, false),
            (API_KEY_API_VERSIONS, 3, true),
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

        let mut inner = bytes::BytesMut::new();
        RequestHeader {
            request_api_key: API_KEY_API_VERSIONS,
            request_api_version: 3,
            correlation_id: 44,
            client_id: Some("c".to_string()),
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![
                crabka_protocol::UnknownTaggedField {
                    tag: 300,
                    bytes: Bytes::from_static(b"tag-payload"),
                },
            ]),
        }
        .encode(&mut inner, 2)
        .expect("encode flexible request header with tag");
        inner.extend_from_slice(b"api-body");
        let mut tagged = Vec::new();
        tagged.extend_from_slice(
            &u32::try_from(inner.len())
                .expect("frame fits u32")
                .to_be_bytes(),
        );
        tagged.extend_from_slice(&inner);
        let decoded = read_request_from_frame(tagged)
            .await
            .expect("tagged flexible request");
        assert!(decoded == (API_KEY_API_VERSIONS, 3, 44, b"api-body".to_vec()));
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
        let oversized = 4097u32.to_be_bytes().to_vec();

        for frame in [short, truncated_client, missing_tag, oversized] {
            let got = read_request_from_frame(frame).await;
            assert!(
                matches!(got, Err(RaftHandshakeError::Protocol(_))),
                "want protocol error, got {got:?}"
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
        // A null client id and empty body make the frame exactly 10 bytes,
        // the minimum legal v1 request header.
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

    #[tokio::test]
    async fn api_versions_response_uses_schema_ranges_and_versioned_encoding() {
        let expected_ranges = [(17, 0, 1), (36, 0, 2), (18, 0, 4)];

        for version in [0, 3] {
            let (mut client, mut server) = tokio::io::duplex(256);
            let writer = tokio::spawn(async move {
                let response = pre_auth_api_versions_response();
                write_response(&mut server, API_KEY_API_VERSIONS, version, 99, &response)
                    .await
                    .expect("write api versions response");
            });
            let frame = read_response_frame(&mut client).await;
            writer.await.expect("writer");

            assert!(&frame[..4] == &99i32.to_be_bytes());
            let mut body = &frame[4..];
            let response = ApiVersionsResponse::decode(&mut body, version)
                .expect("decode api versions response");
            assert!(body.is_empty());
            let ranges: Vec<_> = response
                .api_keys
                .iter()
                .map(|api| (api.api_key, api.min_version, api.max_version))
                .collect();
            assert!(ranges == expected_ranges);

            if version == 0 {
                // v0 has no throttle_time_ms field. The old hand-rolled
                // response appended one and produced a malformed frame.
                assert!(frame.len() == 28);
            }
        }
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
                3,
                1,
                Some(b"c"),
                true,
                &api_versions_body(3),
            ))
            .await
            .expect("write api versions");
        let api_versions = read_response_frame(&mut client).await;
        assert!(&api_versions[0..4] == &1i32.to_be_bytes());
        let mut api_versions_body = &api_versions[4..];
        let response = ApiVersionsResponse::decode(&mut api_versions_body, 3)
            .expect("decode api versions v3 response");
        assert!(api_versions_body.is_empty());
        assert!(response.api_keys.len() == 3);

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
        assert!(principal.name == "broker");
        assert!(principal.auth_method == crabka_security::AuthMethod::SaslPlain);
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
