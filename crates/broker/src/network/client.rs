//! Outbound inter-broker client. Establishes TCP, optionally wraps in TLS,
//! optionally runs SASL client handshake. Returns a generic `AsyncRead +
//! `AsyncWrite` stream the caller uses for normal RPCs.
//!
//! Used (in T17) by the replicator's Fetch path, the raft transport's
//! outbound dial, and the controller-heartbeat loop. T16 only ships the
//! client itself plus a SASL/PLAIN integration test.

use bytes::{Buf, BufMut, BytesMut};
use crabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest;
use crabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse;
use crabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest;
use crabka_protocol::owned::sasl_handshake_response::SaslHandshakeResponse;
use crabka_protocol::{Decode, Encode};
use crabka_security::{ListenerProtocol, SaslMechanism, ScramClientExchange};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use std::sync::Arc;

use crate::config::InterBrokerCredentials;

/// API keys this client speaks directly (everything else is the caller's
/// problem — we return a raw stream once auth is done).
const API_KEY_SASL_HANDSHAKE: i16 = 17;
const API_KEY_SASL_AUTHENTICATE: i16 = 36;

/// `client_id` advertised in outbound Kafka request headers. Visible in
/// broker logs as the connection's reporter id.
const OUTBOUND_CLIENT_ID: &str = "crabka-inter-broker";

#[derive(Debug, Error)]
pub enum InterBrokerError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls: {0}")]
    Tls(String),
    #[error("sasl: {0}")]
    Sasl(String),
    #[error("config: {0}")]
    Config(String),
    #[error("codec: {0}")]
    Codec(String),
}

/// Trait alias for boxed duplex streams. Both `TcpStream` and
/// `tokio_rustls::client::TlsStream<TcpStream>` satisfy it.
///
/// Same shape as `crabka_client_core::ClientDuplex` so the stream
/// returned by `InterBrokerClient::connect` can be handed directly to
/// `Connection::from_stream`.
pub trait DuplexStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + ?Sized> DuplexStream for T {}

/// Constructs outbound connections to other brokers, running TLS and SASL
/// as the listener protocol demands. Cheap to clone-from / share — holds
/// just a `TlsConnector` (an `Arc` under the hood) and credentials.
pub struct InterBrokerClient {
    tls_connector: Option<TlsConnector>,
    creds: Option<InterBrokerCredentials>,
}

impl InterBrokerClient {
    #[must_use]
    pub fn new(tls_connector: Option<TlsConnector>, creds: Option<InterBrokerCredentials>) -> Self {
        Self {
            tls_connector,
            creds,
        }
    }

    /// Dial `host:port`, perform the protocol-appropriate handshakes
    /// (TLS, SASL), and return an authenticated duplex stream. Callers
    /// drive normal Kafka RPCs (Fetch, Vote, `AppendEntries`, …) through
    /// the returned stream just as if it were a fresh `TcpStream`.
    pub async fn connect(
        &self,
        host: &str,
        port: u16,
        listener_protocol: ListenerProtocol,
        server_name: &str,
    ) -> Result<Box<dyn DuplexStream>, InterBrokerError> {
        let tcp = TcpStream::connect((host, port)).await?;
        let mut stream: Box<dyn DuplexStream> = if listener_protocol.requires_tls() {
            let connector = self.tls_connector.clone().ok_or_else(|| {
                InterBrokerError::Config("TLS listener without TlsConnector".into())
            })?;
            let sni =
                tokio_rustls::rustls::pki_types::ServerName::try_from(server_name.to_string())
                    .map_err(|e| InterBrokerError::Tls(format!("invalid server name: {e}")))?;
            let tls = connector
                .connect(sni, tcp)
                .await
                .map_err(|e| InterBrokerError::Tls(e.to_string()))?;
            Box::new(tls)
        } else {
            Box::new(tcp)
        };
        if listener_protocol.requires_sasl() {
            let creds = self.creds.clone().ok_or_else(|| {
                InterBrokerError::Config("SASL listener without inter_broker_credentials".into())
            })?;
            run_outbound_sasl(&mut *stream, &creds).await?;
        }
        Ok(stream)
    }

    /// Dial `host:port` (running TLS + SASL as needed) and return a
    /// [`crabka_client_core::Connection`] over the resulting stream. The
    /// connection is fully usable for normal typed Kafka requests —
    /// `Fetch`, `OffsetForLeaderEpoch`, `BrokerHeartbeat`, raft RPCs via
    /// `raw_request`, etc.
    pub async fn connect_as_connection(
        &self,
        host: &str,
        port: u16,
        listener_protocol: ListenerProtocol,
        server_name: &str,
        options: crabka_client_core::ConnectionOptions,
    ) -> Result<crabka_client_core::Connection, InterBrokerError> {
        // Build the auth'd stream directly into a `Box<dyn ClientDuplex>`
        // (rather than `Box<dyn DuplexStream>`) so it lines up with
        // `Connection::from_stream` without an unsizing coercion that
        // Rust can't do between two equivalent-but-distinct trait
        // objects.
        let tcp = TcpStream::connect((host, port)).await?;
        let mut stream: Box<dyn crabka_client_core::ClientDuplex> =
            if listener_protocol.requires_tls() {
                let connector = self.tls_connector.clone().ok_or_else(|| {
                    InterBrokerError::Config("TLS listener without TlsConnector".into())
                })?;
                let sni =
                    tokio_rustls::rustls::pki_types::ServerName::try_from(server_name.to_string())
                        .map_err(|e| InterBrokerError::Tls(format!("invalid server name: {e}")))?;
                let tls = connector
                    .connect(sni, tcp)
                    .await
                    .map_err(|e| InterBrokerError::Tls(e.to_string()))?;
                Box::new(tls)
            } else {
                Box::new(tcp)
            };
        if listener_protocol.requires_sasl() {
            let creds = self.creds.clone().ok_or_else(|| {
                InterBrokerError::Config("SASL listener without inter_broker_credentials".into())
            })?;
            run_outbound_sasl(&mut *stream, &creds).await?;
        }
        crabka_client_core::Connection::from_stream(stream, options)
            .await
            .map_err(|e| InterBrokerError::Config(format!("Connection::from_stream: {e}")))
    }
}

// ────────────────────────────────────────────────────────────────────────
// Outbound SASL state machine.
// ────────────────────────────────────────────────────────────────────────

async fn run_outbound_sasl<S>(
    stream: &mut S,
    creds: &InterBrokerCredentials,
) -> Result<(), InterBrokerError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    // Step 1: ApiVersions. The JVM client always sends this first; we
    //         skip it for simplicity (plan §16 step 3). The broker's
    //         pre-auth allowlist tolerates skipping ApiVersions.
    // Step 2: SaslHandshake with the chosen mechanism — establishes
    //         which SASL flow the broker will run.
    let mut corr_id: i32 = 1;
    send_sasl_handshake(stream, creds.mechanism, &mut corr_id).await?;
    // Step 3: SaslAuthenticate (one round for PLAIN, two for SCRAM).
    match creds.mechanism {
        SaslMechanism::Plain => {
            send_plain_authenticate(stream, &creds.username, &creds.password, &mut corr_id).await
        }
        SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
            run_scram_client(
                stream,
                &creds.username,
                &creds.password,
                creds.mechanism,
                &mut corr_id,
            )
            .await
        }
        // OAUTHBEARER inter-broker auth would require an outbound token source;
        // not supported as an inter-broker mechanism this slice (slice 49).
        SaslMechanism::OAuthBearer => Err(InterBrokerError::Sasl(
            "OAUTHBEARER is not supported for inter-broker authentication".to_string(),
        )),
        // GSSAPI inter-broker initiate is wired in a later GSSAPI task; until
        // then it is not a usable inter-broker mechanism.
        SaslMechanism::Gssapi => Err(InterBrokerError::Sasl(
            "GSSAPI is not yet wired for inter-broker authentication".to_string(),
        )),
    }
}

/// Send `SaslHandshakeRequest v1` with the wire name for `mechanism`,
/// read `SaslHandshakeResponse v1`, fail if `error_code != 0`.
///
/// Wire framing: `SaslHandshake v1` uses the non-flexible request header
/// (v1 — no trailing tagged-fields byte) and a non-flexible response
/// header (v0 — bare `correlation_id`). This matches the server-side
/// `drive_sasl_plain_session` helper in T13's integration test.
async fn send_sasl_handshake<S>(
    stream: &mut S,
    mechanism: SaslMechanism,
    corr_id: &mut i32,
) -> Result<(), InterBrokerError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    let req = SaslHandshakeRequest {
        mechanism: mechanism.wire_name().to_string(),
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, 1)
        .map_err(|e| InterBrokerError::Codec(format!("SaslHandshake encode: {e}")))?;
    let resp_bytes = round_trip(stream, API_KEY_SASL_HANDSHAKE, 1, *corr_id, false, &body).await?;
    *corr_id += 1;
    let mut cur: &[u8] = &resp_bytes;
    let resp = SaslHandshakeResponse::decode(&mut cur, 1)
        .map_err(|e| InterBrokerError::Codec(format!("SaslHandshake decode: {e}")))?;
    if resp.error_code != 0 {
        return Err(InterBrokerError::Sasl(format!(
            "SaslHandshake error_code={} (mechanism={})",
            resp.error_code,
            mechanism.wire_name()
        )));
    }
    Ok(())
}

/// Send `SaslAuthenticate v2` with PLAIN payload `\0user\0password`, read
/// the response, fail if `error_code != 0`. PLAIN is one round-trip.
async fn send_plain_authenticate<S>(
    stream: &mut S,
    user: &str,
    pass: &str,
    corr_id: &mut i32,
) -> Result<(), InterBrokerError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    let mut payload = Vec::with_capacity(2 + user.len() + pass.len());
    payload.push(0); // authzid (empty)
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(pass.as_bytes());

    let resp = send_sasl_authenticate(stream, payload, corr_id).await?;
    if resp.error_code != 0 {
        return Err(InterBrokerError::Sasl(format!(
            "SaslAuthenticate(PLAIN) error_code={} error_message={:?}",
            resp.error_code, resp.error_message
        )));
    }
    Ok(())
}

/// Run the RFC 5802 SCRAM (SHA-256 or SHA-512) client state machine
/// over two `SaslAuthenticate v2` round-trips. Verifies the
/// server-final signature before declaring the connection
/// authenticated.
async fn run_scram_client<S>(
    stream: &mut S,
    user: &str,
    pass: &str,
    mechanism: SaslMechanism,
    corr_id: &mut i32,
) -> Result<(), InterBrokerError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    let mut exch = ScramClientExchange::new(user.to_string(), pass.as_bytes().to_vec(), mechanism);

    // Round 1: client-first → server-first.
    let client_first = exch
        .client_first()
        .map_err(|e| InterBrokerError::Sasl(format!("scram client_first: {e:?}")))?;
    let resp1 = send_sasl_authenticate(stream, client_first, corr_id).await?;
    if resp1.error_code != 0 {
        return Err(InterBrokerError::Sasl(format!(
            "SaslAuthenticate(SCRAM round 1) error_code={} error_message={:?}",
            resp1.error_code, resp1.error_message
        )));
    }
    let server_first = resp1.auth_bytes.to_vec();

    // Round 2: client-final → server-final.
    let client_final = exch
        .step(&server_first)
        .map_err(|e| InterBrokerError::Sasl(format!("scram client step: {e:?}")))?;
    let resp2 = send_sasl_authenticate(stream, client_final, corr_id).await?;
    if resp2.error_code != 0 {
        return Err(InterBrokerError::Sasl(format!(
            "SaslAuthenticate(SCRAM round 2) error_code={} error_message={:?}",
            resp2.error_code, resp2.error_message
        )));
    }
    // Server-final verification proves the broker holds the matching
    // `server_key` — not just any compatible `stored_key`.
    exch.verify_server_final(&resp2.auth_bytes)
        .map_err(|e| InterBrokerError::Sasl(format!("server-final verify: {e:?}")))?;
    Ok(())
}

/// Frame a `SaslAuthenticate v2` request carrying `auth_bytes`, send it,
/// read the response, return the decoded `SaslAuthenticateResponse v2`.
async fn send_sasl_authenticate<S>(
    stream: &mut S,
    auth_bytes: Vec<u8>,
    corr_id: &mut i32,
) -> Result<SaslAuthenticateResponse, InterBrokerError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    let req = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(auth_bytes),
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, 2)
        .map_err(|e| InterBrokerError::Codec(format!("SaslAuthenticate encode: {e}")))?;
    let resp_bytes =
        round_trip(stream, API_KEY_SASL_AUTHENTICATE, 2, *corr_id, true, &body).await?;
    *corr_id += 1;
    let mut cur: &[u8] = &resp_bytes;
    let resp = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| InterBrokerError::Codec(format!("SaslAuthenticate decode: {e}")))?;
    Ok(resp)
}

// ────────────────────────────────────────────────────────────────────────
// Framing helpers — Kafka request/response framing on the client side.
// Mirrors T13's `round_trip` helper in `crates/broker/tests/auth_handlers.rs`.
// ────────────────────────────────────────────────────────────────────────

/// Build a `RequestHeader v1` (or v2 when `flexible`), append `body`, write
/// the length-prefixed frame, read one response frame, strip the
/// `ResponseHeader`. Returns the response body bytes.
///
/// Header rules (matching Kafka and T13's helper):
/// - Request header: v1 for non-flexible, v2 for flexible (trailing 0x00
///   tagged-fields byte).
/// - Response header: v0 for non-flexible *and* for `ApiVersions(18)`
///   regardless of body flexibility; v1 (`corr_id` + 0x00 tagged byte)
///   for every other flexible response.
async fn round_trip<S>(
    stream: &mut S,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    flexible: bool,
    body: &[u8],
) -> Result<Vec<u8>, InterBrokerError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    let mut frame = BytesMut::with_capacity(16 + body.len());
    // RequestHeader: api_key + version + corr_id + client_id (i16 NULLABLE_STRING).
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(corr_id);
    frame.put_i16(
        i16::try_from(OUTBOUND_CLIENT_ID.len())
            .map_err(|_| InterBrokerError::Codec("client_id too long".into()))?,
    );
    frame.put_slice(OUTBOUND_CLIENT_ID.as_bytes());
    if flexible {
        frame.put_u8(0); // empty header tagged-fields
    }
    frame.put_slice(body);

    stream
        .write_u32(
            u32::try_from(frame.len())
                .map_err(|_| InterBrokerError::Codec("frame size exceeds u32".into()))?,
        )
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    // Read length prefix then exactly that many bytes.
    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;

    // Strip ResponseHeader: 4-byte corr_id, plus 1-byte tagged-fields for
    // v1 (flexible body AND api_key != 18). ApiVersions is special-cased
    // by the Kafka spec — its response header is always v0.
    let mut cur = &resp[..];
    if cur.len() < 4 {
        return Err(InterBrokerError::Codec("response missing corr_id".into()));
    }
    let _resp_corr_id = cur.get_i32();
    let uses_v1_header = flexible && api_key != 18;
    if uses_v1_header {
        if cur.is_empty() {
            return Err(InterBrokerError::Codec(
                "flexible response missing tagged-fields byte".into(),
            ));
        }
        let _tagged = cur.get_u8();
    }
    Ok(cur.to_vec())
}

// ────────────────────────────────────────────────────────────────────────
// OutboundDialer adapter for crabka_raft::CrabkaRaftNetworkFactory.
// ────────────────────────────────────────────────────────────────────────

/// Adapter that lets `crabka_raft` reach the broker's
/// [`InterBrokerClient`] without taking a build dependency on the
/// broker crate. Wraps an `Arc<InterBrokerClient>` plus the protocol /
/// SNI configuration once; the raft network factory clones it cheaply.
pub struct InterBrokerDialer {
    client: Arc<InterBrokerClient>,
    listener_protocol: ListenerProtocol,
    server_name: String,
}

impl InterBrokerDialer {
    #[must_use]
    pub fn new(
        client: Arc<InterBrokerClient>,
        listener_protocol: ListenerProtocol,
        server_name: String,
    ) -> Self {
        Self {
            client,
            listener_protocol,
            server_name,
        }
    }
}

#[async_trait::async_trait]
impl crabka_raft::OutboundDialer for InterBrokerDialer {
    async fn dial(
        &self,
        _target: crabka_raft::NodeId,
        addr: &str,
        options: crabka_client_core::ConnectionOptions,
    ) -> Result<crabka_client_core::Connection, crabka_client_core::ClientError> {
        // The raft transport hands us an address in `host:port` form
        // (the openraft `Node.addr` string). For SocketAddr-style
        // addresses we honour the configured `server_name` for SNI
        // separately from the literal host string.
        let (host, port) = match addr.rsplit_once(':') {
            Some((h, p)) => {
                let port: u16 = p.parse().map_err(|e: std::num::ParseIntError| {
                    crabka_client_core::ClientError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("invalid raft peer port in {addr:?}: {e}"),
                    ))
                })?;
                (h.to_string(), port)
            }
            None => {
                return Err(crabka_client_core::ClientError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("raft peer address missing port: {addr:?}"),
                )));
            }
        };
        self.client
            .connect_as_connection(
                &host,
                port,
                self.listener_protocol,
                &self.server_name,
                options,
            )
            .await
            .map_err(|e| match e {
                InterBrokerError::Io(io) => crabka_client_core::ClientError::Io(io),
                other => crabka_client_core::ClientError::Io(std::io::Error::other(format!(
                    "InterBrokerClient dial: {other}"
                ))),
            })
    }
}
