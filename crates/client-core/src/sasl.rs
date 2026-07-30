//! Transport-agnostic outbound SASL handshake for the client.
//!
//! [`outbound_sasl`] drives the client side of Kafka's
//! `SaslHandshake` + `SaslAuthenticate` exchange over any
//! `AsyncRead + AsyncWrite` stream (a plaintext `TcpStream`, a
//! `tokio_rustls` TLS stream, or an in-process duplex for tests). It
//! supports PLAIN (one round-trip), SCRAM-SHA-256/512 (two round-trips
//! with server-final verification), and GSSAPI (multi-round AP-REQ /
//! AP-REP + RFC 4752 security-layer negotiation).
//!
//! This is the shared implementation the broker's inter-broker dialer
//! and the public clients both call; the only difference is the
//! credentials value and the reporter `client_id`.

use std::path::PathBuf;

use bytes::{Buf, BufMut, BytesMut};
use crabka_ids::{ApiKey, ApiVersion};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use crabka_security::{SaslMechanism, ScramClientExchange};
use crabka_units::{ByteSize, kibibytes};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::ClientFrameMax;

const API_KEY_SASL_HANDSHAKE: i16 = 17;
const API_KEY_SASL_AUTHENTICATE: i16 = 36;

/// Maximum receive size advertised in the client's RFC 4752 security-layer
/// choice. Auth-only QOP means no data is wrapped post-handshake, so the value
/// only needs to be a sane non-zero buffer; mirror the server's offer size.
const GSSAPI_MAX_RECV: ByteSize = kibibytes(64);

/// Outbound SASL credentials. Mirrors the broker's
/// `InterBrokerCredentials`; one variant per supported mechanism.
#[derive(Debug, Clone)]
pub enum SaslCredentials {
    /// SASL/PLAIN: `\0username\0password`.
    Plain { username: String, password: String },
    /// SASL/SCRAM (SHA-256 or SHA-512).
    Scram {
        mechanism: SaslMechanism,
        username: String,
        password: String,
    },
    /// SASL/GSSAPI: authenticate as `client_principal` using the long-term
    /// key in `keytab_path` (no password).
    Gssapi {
        keytab_path: PathBuf,
        client_principal: String,
        service_name: String,
        kdc_url: String,
    },
}

impl SaslCredentials {
    /// The SASL mechanism this credential set authenticates with.
    #[must_use]
    pub fn mechanism(&self) -> SaslMechanism {
        match self {
            Self::Plain { .. } => SaslMechanism::Plain,
            Self::Scram { mechanism, .. } => *mechanism,
            Self::Gssapi { .. } => SaslMechanism::Gssapi,
        }
    }
}

/// Errors raised while running the outbound SASL handshake.
#[derive(Debug, Error)]
pub enum OutboundSaslError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("sasl: {0}")]
    Sasl(String),
    #[error("codec: {0}")]
    Codec(String),
}

/// Run the outbound SASL handshake to completion over `stream`.
///
/// Sends `SaslHandshake` with the mechanism in `creds`, then drives the
/// mechanism-specific `SaslAuthenticate` round-trips. `server_name` is the
/// broker's canonical hostname, used only by the GSSAPI path to build the
/// target SPN (`service_name/server_name`). Returns once the broker has
/// accepted the credentials; the same `stream` is then usable for normal
/// Kafka RPCs.
///
/// # Errors
///
/// Returns [`OutboundSaslError`] on I/O failure, codec failure, or a
/// non-zero broker SASL error code.
pub async fn outbound_sasl<S>(
    stream: &mut S,
    creds: &SaslCredentials,
    server_name: &str,
    client_id: &str,
    frame_max: ClientFrameMax,
) -> Result<(), OutboundSaslError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    // Step 1: ApiVersions. The JVM client always sends this first; we
    //         skip it for simplicity. The broker's pre-auth allowlist
    //         tolerates skipping ApiVersions.
    // Step 2: SaslHandshake with the chosen mechanism — establishes
    //         which SASL flow the broker will run.
    let mut corr_id: i32 = 1;
    send_sasl_handshake(
        stream,
        creds.mechanism(),
        &mut corr_id,
        client_id,
        frame_max,
    )
    .await?;
    // Step 3: SaslAuthenticate (one round for PLAIN, two for SCRAM, three
    //         for GSSAPI).
    match creds {
        SaslCredentials::Plain { username, password } => {
            send_plain_authenticate(
                stream,
                username,
                password,
                &mut corr_id,
                client_id,
                frame_max,
            )
            .await
        }
        SaslCredentials::Scram {
            mechanism,
            username,
            password,
        } => {
            run_scram_client(
                stream,
                username,
                password,
                *mechanism,
                &mut corr_id,
                client_id,
                frame_max,
            )
            .await
        }
        SaslCredentials::Gssapi {
            keytab_path,
            client_principal,
            service_name,
            kdc_url,
        } => {
            run_gssapi_client(
                stream,
                keytab_path,
                client_principal,
                service_name,
                server_name,
                kdc_url,
                &mut corr_id,
                client_id,
                frame_max,
            )
            .await
        }
    }
}

/// Send `SaslHandshakeRequest v1` with the wire name for `mechanism`,
/// read `SaslHandshakeResponse v1`, fail if `error_code != 0`.
///
/// Wire framing: `SaslHandshake v1` uses the non-flexible request header
/// (v1 — no trailing tagged-fields byte) and a non-flexible response
/// header (v0 — bare `correlation_id`).
async fn send_sasl_handshake<S>(
    stream: &mut S,
    mechanism: SaslMechanism,
    corr_id: &mut i32,
    client_id: &str,
    frame_max: ClientFrameMax,
) -> Result<(), OutboundSaslError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    let req = SaslHandshakeRequest {
        mechanism: mechanism.wire_name().to_string(),
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, 1)
        .map_err(|e| OutboundSaslError::Codec(format!("SaslHandshake encode: {e}")))?;
    let resp_bytes = round_trip(
        stream,
        ApiKey(API_KEY_SASL_HANDSHAKE),
        ApiVersion(1),
        *corr_id,
        false,
        &body,
        client_id,
        frame_max,
    )
    .await?;
    *corr_id += 1;
    let mut cur: &[u8] = &resp_bytes;
    let resp = SaslHandshakeResponse::decode(&mut cur, 1)
        .map_err(|e| OutboundSaslError::Codec(format!("SaslHandshake decode: {e}")))?;
    if resp.error_code != 0 {
        return Err(OutboundSaslError::Sasl(format!(
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
    client_id: &str,
    frame_max: ClientFrameMax,
) -> Result<(), OutboundSaslError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    let mut payload = Vec::with_capacity(2 + user.len() + pass.len());
    payload.push(0); // authzid (empty)
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(pass.as_bytes());

    let resp =
        send_sasl_authenticate(stream, payload, corr_id, client_id, frame_max).await?;
    if resp.error_code != 0 {
        return Err(OutboundSaslError::Sasl(format!(
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
    client_id: &str,
    frame_max: ClientFrameMax,
) -> Result<(), OutboundSaslError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    let exch = ScramClientExchange::new(user.to_string(), pass.as_bytes().to_vec(), mechanism);

    // Round 1: client-first → server-first.
    let (client_first, exch) = exch
        .client_first()
        .map_err(|e| OutboundSaslError::Sasl(format!("scram client_first: {e:?}")))?;
    let resp1 =
        send_sasl_authenticate(stream, client_first, corr_id, client_id, frame_max).await?;
    if resp1.error_code != 0 {
        return Err(OutboundSaslError::Sasl(format!(
            "SaslAuthenticate(SCRAM round 1) error_code={} error_message={:?}",
            resp1.error_code, resp1.error_message
        )));
    }
    let server_first = resp1.auth_bytes.to_vec();

    // Round 2: client-final → server-final.
    let (client_final, exch) = exch
        .step(&server_first)
        .map_err(|e| OutboundSaslError::Sasl(format!("scram client step: {e:?}")))?;
    let resp2 =
        send_sasl_authenticate(stream, client_final, corr_id, client_id, frame_max).await?;
    if resp2.error_code != 0 {
        return Err(OutboundSaslError::Sasl(format!(
            "SaslAuthenticate(SCRAM round 2) error_code={} error_message={:?}",
            resp2.error_code, resp2.error_message
        )));
    }
    // Server-final verification proves the broker holds the matching
    // `server_key` — not just any compatible `stored_key`.
    exch.verify_server_final(&resp2.auth_bytes)
        .map_err(|e| OutboundSaslError::Sasl(format!("server-final verify: {e:?}")))?;
    Ok(())
}

/// Run the SASL/GSSAPI (Kerberos) client state machine over
/// `SaslAuthenticate v2` round-trips.
///
/// Builds an `sspi`-backed initiator that authenticates as `client_principal`
/// using the long-term key in `keytab_path` (no password), targeting the SPN
/// `service_name/server_name` of the broker being dialed. `server_name` is the
/// broker's canonical hostname (the same value used for TLS SNI), not the
/// dialed IP — the SPN must match the service key in the broker's keytab.
/// Drives [`GssapiClientExchange`]: the GSS context establishment
/// (AP-REQ → AP-REP) followed by the RFC 4752 auth-only security-layer
/// negotiation. Each non-terminal client token is sent as request
/// `auth_bytes`; the server's reply token feeds the next step until the
/// exchange reports `Done`.
///
/// The first initiator step performs the synchronous AS/TGS exchange with the
/// KDC; subsequent steps only process tokens locally.
async fn run_gssapi_client<S>(
    stream: &mut S,
    keytab_path: &std::path::Path,
    client_principal: &str,
    service_name: &str,
    server_name: &str,
    kdc_url: &str,
    corr_id: &mut i32,
    client_id: &str,
    frame_max: ClientFrameMax,
) -> Result<(), OutboundSaslError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    use crabka_security::gssapi::{
        client::{ClientStep, GssapiClientExchange},
        provider::SspiInitiator,
    };

    let target_spn = format!("{service_name}/{server_name}");
    let keytab = keytab_path.to_string_lossy();
    let initiator = SspiInitiator::new(&keytab, client_principal, &target_spn, kdc_url)
        .map_err(|e| OutboundSaslError::Sasl(format!("GSSAPI initiator init failed: {e}")))?;
    let exchange = GssapiClientExchange::new(Box::new(initiator), GSSAPI_MAX_RECV, None);

    // Seed the exchange with no server token; this produces the AP-REQ.
    let mut step = exchange
        .step(None)
        .map_err(|e| OutboundSaslError::Sasl(format!("GSSAPI initiate failed: {e}")))?;
    loop {
        match step {
            ClientStep::Token(token, next) => {
                let resp =
                    send_sasl_authenticate(stream, token, corr_id, client_id, frame_max).await?;
                if resp.error_code != 0 {
                    return Err(OutboundSaslError::Sasl(format!(
                        "SaslAuthenticate(GSSAPI) error_code={} error_message={:?}",
                        resp.error_code, resp.error_message
                    )));
                }
                step = next
                    .step(Some(&resp.auth_bytes))
                    .map_err(|e| OutboundSaslError::Sasl(format!("GSSAPI step failed: {e}")))?;
            }
            ClientStep::Final(token) => {
                let resp =
                    send_sasl_authenticate(stream, token, corr_id, client_id, frame_max).await?;
                if resp.error_code != 0 {
                    return Err(OutboundSaslError::Sasl(format!(
                        "SaslAuthenticate(GSSAPI) error_code={} error_message={:?}",
                        resp.error_code, resp.error_message
                    )));
                }
                return Ok(());
            }
        }
    }
}

/// Frame a `SaslAuthenticate v2` request carrying `auth_bytes`, send it,
/// read the response, return the decoded `SaslAuthenticateResponse v2`.
async fn send_sasl_authenticate<S>(
    stream: &mut S,
    auth_bytes: Vec<u8>,
    corr_id: &mut i32,
    client_id: &str,
    frame_max: ClientFrameMax,
) -> Result<SaslAuthenticateResponse, OutboundSaslError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    let req = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(auth_bytes),
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, 2)
        .map_err(|e| OutboundSaslError::Codec(format!("SaslAuthenticate encode: {e}")))?;
    let resp_bytes = round_trip(
        stream,
        ApiKey(API_KEY_SASL_AUTHENTICATE),
        ApiVersion(2),
        *corr_id,
        true,
        &body,
        client_id,
        frame_max,
    )
    .await?;
    *corr_id += 1;
    let mut cur: &[u8] = &resp_bytes;
    let resp = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| OutboundSaslError::Codec(format!("SaslAuthenticate decode: {e}")))?;
    Ok(resp)
}

/// Build a `RequestHeader v1` (or v2 when `flexible`), append `body`, write
/// the length-prefixed frame, read one response frame, strip the
/// `ResponseHeader`. Returns the response body bytes.
///
/// Header rules (matching Kafka):
/// - Request header: v1 for non-flexible, v2 for flexible (trailing 0x00
///   tagged-fields byte).
/// - Response header: v0 for non-flexible *and* for `ApiVersions(18)`
///   regardless of body flexibility; v1 (`corr_id` + 0x00 tagged byte)
///   for every other flexible response.
async fn round_trip<S>(
    stream: &mut S,
    api_key: ApiKey,
    api_version: ApiVersion,
    corr_id: i32,
    flexible: bool,
    body: &[u8],
    client_id: &str,
    frame_max: ClientFrameMax,
) -> Result<Vec<u8>, OutboundSaslError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + ?Sized,
{
    let mut frame = BytesMut::with_capacity(16 + body.len());
    // RequestHeader: api_key + version + corr_id + client_id (i16 NULLABLE_STRING).
    frame.put_i16(api_key.0);
    frame.put_i16(api_version.0);
    frame.put_i32(corr_id);
    frame.put_i16(
        i16::try_from(client_id.len())
            .map_err(|_| OutboundSaslError::Codec("client_id too long".into()))?,
    );
    frame.put_slice(client_id.as_bytes());
    if flexible {
        frame.put_u8(0); // empty header tagged-fields
    }
    frame.put_slice(body);

    if frame.len() > frame_max.bytes() {
        return Err(OutboundSaslError::Codec(format!(
            "SASL request encoded {} bytes, maximum {}",
            frame.len(),
            frame_max.bytes()
        )));
    }

    stream
        .write_u32(
            u32::try_from(frame.len())
                .map_err(|_| OutboundSaslError::Codec("frame size exceeds u32".into()))?,
        )
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    // Read length prefix then exactly that many bytes.
    let resp_len = usize::try_from(stream.read_u32().await?)
        .map_err(|_| OutboundSaslError::Codec("SASL response length does not fit usize".into()))?;
    if resp_len > frame_max.bytes() {
        return Err(OutboundSaslError::Codec(format!(
            "SASL response announced {resp_len} bytes, maximum {}",
            frame_max.bytes()
        )));
    }
    let mut resp = vec![0u8; resp_len];
    stream.read_exact(&mut resp).await?;

    // Strip ResponseHeader: 4-byte corr_id, plus 1-byte tagged-fields for
    // v1 (flexible body AND api_key != 18). ApiVersions is special-cased
    // by the Kafka spec — its response header is always v0.
    let mut cur = &resp[..];
    if cur.len() < 4 {
        return Err(OutboundSaslError::Codec("response missing corr_id".into()));
    }
    let _resp_corr_id = cur.get_i32();
    let uses_v1_header = flexible && api_key != 18;
    if uses_v1_header {
        if cur.is_empty() {
            return Err(OutboundSaslError::Codec(
                "flexible response missing tagged-fields byte".into(),
            ));
        }
        let _tagged = cur.get_u8();
    }
    Ok(cur.to_vec())
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_protocol::owned::{
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_response::SaslHandshakeResponse,
    };
    use crabka_security::{ScramServerExchange, StepResult, hash_scram_password};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        time::{Duration, timeout},
    };

    use super::*;

    const TEST_CLIENT_ID: &str = "configured-sasl-client";

    // Minimal server: read one request frame, reply with a response
    // header (corr_id, plus a 0x00 tagged-fields byte when `flex_header`)
    // carrying `body`. SaslHandshake uses a v0 response header; the
    // flexible SaslAuthenticate v2 uses a v1 response header.
    async fn reply_frame<S>(stream: &mut S, body: &[u8], flex_header: bool) -> Vec<u8>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let req_len = stream.read_u32().await.unwrap();
        let mut req = vec![0u8; req_len as usize];
        stream.read_exact(&mut req).await.unwrap();
        // corr_id is at request header bytes [4..8] (api_key,version,corr_id).
        let corr_id = i32::from_be_bytes([req[4], req[5], req[6], req[7]]);
        let mut frame = BytesMut::new();
        frame.put_i32(corr_id);
        if flex_header {
            frame.put_u8(0); // empty response-header tagged-fields
        }
        frame.put_slice(body);
        stream
            .write_u32(u32::try_from(frame.len()).unwrap())
            .await
            .unwrap();
        stream.write_all(&frame).await.unwrap();
        stream.flush().await.unwrap();
        req
    }

    macro_rules! decode_sasl_authenticate_frame {
        ($req:expr, $corr_id:expr) => {{
            assert_request_header(
                &$req,
                ApiKey(API_KEY_SASL_AUTHENTICATE),
                ApiVersion(2),
                $corr_id,
                true,
            );
            let mut body = request_body(&$req, true);
            let decoded = SaslAuthenticateRequest::decode(&mut body, 2).unwrap();
            assert!(body.is_empty());
            decoded
        }};
    }

    #[tokio::test]
    async fn outbound_plain_completes() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn(async move {
            // 1. SaslHandshake v1 → error_code 0 + empty mechanisms.
            let mut hs = BytesMut::new();
            SaslHandshakeResponse {
                error_code: 0,
                ..Default::default()
            }
            .encode(&mut hs, 1)
            .unwrap();
            let hs_req = reply_frame(&mut server, &hs, false).await;
            assert_request_header(
                &hs_req,
                ApiKey(API_KEY_SASL_HANDSHAKE),
                ApiVersion(1),
                1,
                false,
            );
            let mut hs_body = request_body(&hs_req, false);
            let hs_decoded = SaslHandshakeRequest::decode(&mut hs_body, 1).unwrap();
            assert_eq!(hs_decoded.mechanism, "PLAIN");
            assert!(hs_body.is_empty());

            // 2. SaslAuthenticate v2 → error_code 0 (flexible response header).
            let mut au = BytesMut::new();
            SaslAuthenticateResponse {
                error_code: 0,
                ..Default::default()
            }
            .encode(&mut au, 2)
            .unwrap();
            let au_req = reply_frame(&mut server, &au, true).await;
            let au_decoded = decode_sasl_authenticate_frame!(au_req, 2);
            assert_eq!(au_decoded.auth_bytes.as_ref(), b"\0u\0p");
        });
        let creds = SaslCredentials::Plain {
            username: "u".into(),
            password: "p".into(),
        };
        outbound_sasl(
            &mut client,
            &creds,
            "localhost",
            TEST_CLIENT_ID,
            ClientFrameMax::default(),
        )
            .await
            .expect("PLAIN outbound handshake completes");
        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server observed both SASL client frames")
            .unwrap();
    }

    #[tokio::test]
    async fn send_sasl_authenticate_increments_correlation_id_and_sends_auth_bytes() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn(async move {
            for (expected_corr, expected_payload) in
                [(7, b"first".as_ref()), (8, b"second".as_ref())]
            {
                let mut au = BytesMut::new();
                SaslAuthenticateResponse {
                    error_code: 0,
                    ..Default::default()
                }
                .encode(&mut au, 2)
                .unwrap();
                let req = reply_frame(&mut server, &au, true).await;
                let decoded = decode_sasl_authenticate_frame!(req, expected_corr);
                assert_eq!(decoded.auth_bytes.as_ref(), expected_payload);
            }
        });

        let mut corr_id = 7;
        send_sasl_authenticate(
            &mut client,
            b"first".to_vec(),
            &mut corr_id,
            TEST_CLIENT_ID,
            ClientFrameMax::default(),
        )
        .await
        .unwrap();
        assert_eq!(corr_id, 8);
        send_sasl_authenticate(
            &mut client,
            b"second".to_vec(),
            &mut corr_id,
            TEST_CLIENT_ID,
            ClientFrameMax::default(),
        )
        .await
        .unwrap();
        assert_eq!(corr_id, 9);
        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server observed both authenticate frames")
            .unwrap();
    }

    #[tokio::test]
    async fn outbound_scram_rejects_broker_error_on_first_round() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn(async move {
            let mut hs = BytesMut::new();
            SaslHandshakeResponse {
                error_code: 0,
                ..Default::default()
            }
            .encode(&mut hs, 1)
            .unwrap();
            let hs_req = reply_frame(&mut server, &hs, false).await;
            assert_request_header(
                &hs_req,
                ApiKey(API_KEY_SASL_HANDSHAKE),
                ApiVersion(1),
                1,
                false,
            );
            let mut hs_body = request_body(&hs_req, false);
            let hs_decoded = SaslHandshakeRequest::decode(&mut hs_body, 1).unwrap();
            assert_eq!(hs_decoded.mechanism, "SCRAM-SHA-256");

            let mut au = BytesMut::new();
            SaslAuthenticateResponse {
                error_code: 42,
                error_message: Some("nope".into()),
                ..Default::default()
            }
            .encode(&mut au, 2)
            .unwrap();
            let au_req = reply_frame(&mut server, &au, true).await;
            let _ = decode_sasl_authenticate_frame!(au_req, 2);
        });

        let creds = SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha256,
            username: "u".into(),
            password: "p".into(),
        };
        let err = outbound_sasl(
            &mut client,
            &creds,
            "localhost",
            TEST_CLIENT_ID,
            ClientFrameMax::default(),
        )
            .await
            .unwrap_err();
        assert!(matches!(err, OutboundSaslError::Sasl(msg) if msg.contains("round 1")));
        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server observed SCRAM first round")
            .unwrap();
    }

    #[tokio::test]
    async fn outbound_scram_rejects_broker_error_on_second_round() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn(async move {
            let mut hs = BytesMut::new();
            SaslHandshakeResponse {
                error_code: 0,
                ..Default::default()
            }
            .encode(&mut hs, 1)
            .unwrap();
            let hs_req = reply_frame(&mut server, &hs, false).await;
            assert_request_header(
                &hs_req,
                ApiKey(API_KEY_SASL_HANDSHAKE),
                ApiVersion(1),
                1,
                false,
            );

            let cred = hash_scram_password(b"p", SaslMechanism::ScramSha256, 4096);
            let scram_server = ScramServerExchange::new("u".to_string(), cred);

            let first_req_len = server.read_u32().await.unwrap();
            let mut first_req = vec![0u8; first_req_len as usize];
            server.read_exact(&mut first_req).await.unwrap();
            let first_auth = decode_sasl_authenticate_frame!(first_req, 2);
            let server_first = match scram_server.step(&first_auth.auth_bytes) {
                StepResult::Continue(bytes, _next) => bytes,
                other => panic!("server first SCRAM step must continue, got {other:?}"),
            };

            let mut first_resp = BytesMut::new();
            SaslAuthenticateResponse {
                error_code: 0,
                auth_bytes: bytes::Bytes::from(server_first),
                ..Default::default()
            }
            .encode(&mut first_resp, 2)
            .unwrap();
            write_response_frame(&mut server, 2, &first_resp, true).await;

            let mut second_resp = BytesMut::new();
            SaslAuthenticateResponse {
                error_code: 43,
                error_message: Some("second nope".into()),
                ..Default::default()
            }
            .encode(&mut second_resp, 2)
            .unwrap();
            let second_req = reply_frame(&mut server, &second_resp, true).await;
            let _ = decode_sasl_authenticate_frame!(second_req, 3);
        });

        let creds = SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha256,
            username: "u".into(),
            password: "p".into(),
        };
        let err = outbound_sasl(
            &mut client,
            &creds,
            "localhost",
            TEST_CLIENT_ID,
            ClientFrameMax::default(),
        )
            .await
            .unwrap_err();
        assert!(matches!(err, OutboundSaslError::Sasl(msg) if msg.contains("round 2")));
        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server observed SCRAM second round")
            .unwrap();
    }

    #[tokio::test]
    async fn round_trip_rejects_response_without_correlation_id() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn(async move {
            let req_len = server.read_u32().await.unwrap();
            let mut req = vec![0u8; req_len as usize];
            server.read_exact(&mut req).await.unwrap();
            server.write_u32(3).await.unwrap();
            server.write_all(&[1, 2, 3]).await.unwrap();
            server.flush().await.unwrap();
        });

        let err = round_trip(
            &mut client,
            ApiKey(API_KEY_SASL_HANDSHAKE),
            ApiVersion(1),
            99,
            false,
            &[],
            TEST_CLIENT_ID,
            ClientFrameMax::default(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, OutboundSaslError::Codec(msg) if msg == "response missing corr_id"));
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn round_trip_rejects_flexible_response_without_tagged_fields_byte() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn(async move {
            let req_len = server.read_u32().await.unwrap();
            let mut req = vec![0u8; req_len as usize];
            server.read_exact(&mut req).await.unwrap();
            server.write_u32(4).await.unwrap();
            server.write_i32(99).await.unwrap();
            server.flush().await.unwrap();
        });

        let err = round_trip(
            &mut client,
            ApiKey(API_KEY_SASL_AUTHENTICATE),
            ApiVersion(2),
            99,
            true,
            &[],
            TEST_CLIENT_ID,
            ClientFrameMax::default(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, OutboundSaslError::Codec(msg) if msg == "flexible response missing tagged-fields byte")
        );
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn round_trip_uses_the_configured_client_id() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn(async move {
            let req = reply_frame(&mut server, &[], false).await;
            let client_len = usize::try_from(i16::from_be_bytes([req[8], req[9]])).unwrap();
            assert_eq!(&req[10..10 + client_len], b"configured-sasl-client");
        });

        round_trip(
            &mut client,
            ApiKey(API_KEY_SASL_HANDSHAKE),
            ApiVersion(1),
            99,
            false,
            &[],
            TEST_CLIENT_ID,
            crate::ClientFrameMax::default(),
        )
        .await
        .unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn round_trip_rejects_an_oversized_request_before_writing() {
        let (mut client, _server) = tokio::io::duplex(8192);
        let error = round_trip(
            &mut client,
            ApiKey(API_KEY_SASL_HANDSHAKE),
            ApiVersion(1),
            99,
            false,
            &[0],
            "",
            crate::ClientFrameMax::try_from(crabka_units::bytes(10)).unwrap(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("encoded 11"));
        assert!(error.to_string().contains("maximum 10"));
    }

    #[tokio::test]
    async fn round_trip_rejects_an_oversized_response_before_allocating() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let server_task = tokio::spawn(async move {
            let request_len = server.read_u32().await.unwrap();
            let mut request = vec![0; request_len as usize];
            server.read_exact(&mut request).await.unwrap();
            server.write_u32(17).await.unwrap();
            server.flush().await.unwrap();
        });

        let error = round_trip(
            &mut client,
            ApiKey(API_KEY_SASL_HANDSHAKE),
            ApiVersion(1),
            99,
            false,
            &[],
            "",
            crate::ClientFrameMax::try_from(crabka_units::bytes(16)).unwrap(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("announced 17"));
        assert!(error.to_string().contains("maximum 16"));
        server_task.await.unwrap();
    }

    fn assert_request_header(
        req: &[u8],
        api_key: ApiKey,
        api_version: ApiVersion,
        corr_id: i32,
        flexible: bool,
    ) {
        check!(ApiKey(i16::from_be_bytes([req[0], req[1]])) == api_key);
        check!(ApiVersion(i16::from_be_bytes([req[2], req[3]])) == api_version);
        check!(i32::from_be_bytes([req[4], req[5], req[6], req[7]]) == corr_id);
        let client_len = i16::from_be_bytes([req[8], req[9]]);
        assert_eq!(client_len, i16::try_from(TEST_CLIENT_ID.len()).unwrap());
        assert_eq!(
            &req[10..10 + TEST_CLIENT_ID.len()],
            TEST_CLIENT_ID.as_bytes()
        );
        if flexible {
            assert_eq!(req[10 + TEST_CLIENT_ID.len()], 0);
        }
    }

    fn request_body(req: &[u8], flexible: bool) -> &[u8] {
        let header_len = 10 + TEST_CLIENT_ID.len() + usize::from(flexible);
        &req[header_len..]
    }

    async fn write_response_frame<S>(stream: &mut S, corr_id: i32, body: &[u8], flex_header: bool)
    where
        S: tokio::io::AsyncWrite + Unpin,
    {
        let mut frame = BytesMut::new();
        frame.put_i32(corr_id);
        if flex_header {
            frame.put_u8(0);
        }
        frame.put_slice(body);
        stream
            .write_u32(u32::try_from(frame.len()).unwrap())
            .await
            .unwrap();
        stream.write_all(&frame).await.unwrap();
        stream.flush().await.unwrap();
    }
}
