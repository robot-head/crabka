//! Accept loop for the controller TCP listener. Receives inbound KIP-595 RPCs
//! (Fetch=1, Vote=52, BeginQuorumEpoch=53, EndQuorumEpoch=54) plus the
//! Crabka-private observer/forward RPCs and feeds them into the local
//! [`KraftController`] engine.
//!
//! Wire shape matches `crabka_client_core::Connection::raw_request`:
//!
//! - Request: `len(i32) | RequestHeader v2 (flexible) | body`
//! - Response: `len(i32) | correlation_id(i32) | tagged_fields(0u8) | body`
//!
//! `RequestHeader` v2 = `api_key(i16) api_version(i16) correlation_id(i32)
//! client_id(NULLABLE_STRING) tagged_fields(varint=0)`. We parse and discard
//! everything but `api_key`/`correlation_id` (the body is decoded by the
//! engine's transport codec / the Crabka-private wire types).

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::error::RaftError;
use crate::kraft::KraftController;
use crate::kraft::transport::{Inbound, api_key};
use crate::wire::{
    API_KEY_METADATA_FETCH, API_KEY_SUBMIT_CHANGE, CrabkaMetadataFetchRequest,
    CrabkaMetadataFetchResponse, CrabkaSubmitChangeRequest, CrabkaSubmitChangeResponse,
};

/// Kafka's `ApiVersions` API key. The controller TCP listener answers this
/// because `crabka_client_core::Connection::connect` performs an `ApiVersions`
/// handshake before any other request.
const API_KEY_API_VERSIONS: i16 = 18;

pub(crate) async fn run(
    listener: TcpListener,
    engine: KraftController,
    shutdown: CancellationToken,
    handshake: Option<Arc<dyn crate::RaftListenerHandshake>>,
) {
    match listener.local_addr() {
        Ok(addr) => info!(%addr, "controller listener started"),
        Err(e) => info!(error = %e, "controller listener started (addr unknown)"),
    }
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        let engine = engine.clone();
                        let shutdown = shutdown.clone();
                        let handshake = handshake.clone();
                        tokio::spawn(async move {
                            let boxed: Box<dyn crate::DuplexStream> = if let Some(hs) = handshake {
                                match hs.upgrade(stream).await {
                                    Ok(s) => s,
                                    Err(e) => {
                                        tracing::debug!(%peer, error = %e, "handshake failed");
                                        return;
                                    }
                                }
                            } else {
                                Box::new(stream) as Box<dyn crate::DuplexStream>
                            };
                            if let Err(e) = handle_conn(boxed, engine, shutdown).await {
                                error!(%peer, error = %e, "controller connection error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "controller listener accept failed");
                    }
                }
            }
        }
    }
}

async fn handle_conn<S>(
    mut stream: S,
    engine: KraftController,
    shutdown: CancellationToken,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            res = read_one_request(&mut stream) => {
                let (api_key_n, _api_version, correlation_id, body) = match res {
                    Ok(v) => v,
                    Err(e) => {
                        // Treat peer EOF as a clean shutdown of this conn.
                        if is_eof(&e) {
                            return Ok(());
                        }
                        return Err(e);
                    }
                };
                // ApiVersions (18) is the bootstrap handshake performed by
                // `Connection::connect`. It arrives at v0 with a header v1 (no
                // tagged-fields byte) and expects a ResponseHeader v0 reply (also
                // no tagged-fields byte) — the documented Kafka asymmetry. We
                // serialize it separately rather than poisoning the generic codec.
                if api_key_n == API_KEY_API_VERSIONS {
                    let resp = api_versions_response_body();
                    write_response_no_tagged_fields(&mut stream, correlation_id, resp).await?;
                    continue;
                }
                let resp = dispatch(api_key_n, body, &engine).await?;
                write_response(&mut stream, correlation_id, resp).await?;
            }
        }
    }
}

fn is_eof(e: &RaftError) -> bool {
    matches!(e,
        RaftError::Storage(crabka_log::LogError::Io(io))
            if io.kind() == std::io::ErrorKind::UnexpectedEof
    )
}

fn io_err(e: std::io::Error) -> RaftError {
    RaftError::Storage(crabka_log::LogError::Io(e))
}

fn truncated(needed: usize) -> RaftError {
    RaftError::Protocol(crabka_protocol::ProtocolError::UnexpectedEof { needed })
}

async fn read_one_request<S>(stream: &mut S) -> Result<(i16, i16, i32, Bytes), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.map_err(io_err)?;
    let raw_len = i32::from_be_bytes(len_buf);
    let len = usize::try_from(raw_len.max(0)).unwrap_or(0);
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame).await.map_err(io_err)?;

    // RequestHeader v2 (flexible): api_key(i16), api_version(i16),
    // correlation_id(i32), client_id(NULLABLE_STRING), tagged_fields(varint=0).
    let mut cur: &[u8] = &frame;
    let fixed = 2 + 2 + 4;
    if cur.remaining() < fixed {
        return Err(truncated(fixed - cur.remaining()));
    }
    let api_key_n = cur.get_i16();
    let api_version = cur.get_i16();
    let correlation_id = cur.get_i32();

    // Skip client_id: NULLABLE_STRING (i16 length + bytes; -1 = null).
    if cur.remaining() < 2 {
        return Err(truncated(2 - cur.remaining()));
    }
    let cs_len = cur.get_i16();
    if cs_len > 0 {
        let n = usize::try_from(cs_len).unwrap_or(0);
        if cur.remaining() < n {
            return Err(truncated(n - cur.remaining()));
        }
        cur.advance(n);
    }
    // tagged_fields: single varint zero.
    if cur.has_remaining() && cur[0] == 0 {
        cur.advance(1);
    }

    Ok((
        api_key_n,
        api_version,
        correlation_id,
        Bytes::copy_from_slice(cur),
    ))
}

async fn write_response<S>(
    stream: &mut S,
    correlation_id: i32,
    body: Bytes,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut frame = BytesMut::with_capacity(4 + 1 + body.len());
    frame.put_i32(correlation_id);
    frame.put_u8(0); // empty tagged_fields (ResponseHeader v1)
    frame.put_slice(&body);

    let mut len_prefix = [0u8; 4];
    len_prefix.copy_from_slice(&i32::try_from(frame.len()).unwrap_or(i32::MAX).to_be_bytes());
    stream.write_all(&len_prefix).await.map_err(io_err)?;
    stream.write_all(&frame).await.map_err(io_err)?;
    stream.flush().await.map_err(io_err)?;
    Ok(())
}

/// Write a response without the leading tagged-fields byte. Used only by the
/// `ApiVersions` v0 path, which decodes a `ResponseHeader v0`.
async fn write_response_no_tagged_fields<S>(
    stream: &mut S,
    correlation_id: i32,
    body: Bytes,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut frame = BytesMut::with_capacity(4 + body.len());
    frame.put_i32(correlation_id);
    frame.put_slice(&body);

    let mut len_prefix = [0u8; 4];
    len_prefix.copy_from_slice(&i32::try_from(frame.len()).unwrap_or(i32::MAX).to_be_bytes());
    stream.write_all(&len_prefix).await.map_err(io_err)?;
    stream.write_all(&frame).await.map_err(io_err)?;
    stream.flush().await.map_err(io_err)?;
    Ok(())
}

/// Minimal `ApiVersionsResponse` v0: `error_code = 0`, empty `api_keys` array.
/// The connection handshake doesn't consult the table (the engine always issues
/// `raw_request` at the captured KIP-595 versions), so an empty list is harmless.
fn api_versions_response_body() -> Bytes {
    let mut out = BytesMut::with_capacity(6);
    out.put_i16(0); // error_code
    out.put_i32(0); // api_keys: v0 non-flexible empty array
    out.freeze()
}

/// Route an inbound RPC body to the engine and produce the response body.
///
/// The KIP-595 engine RPCs (1/52/53/54) go through [`KraftController::deliver`],
/// which decodes the body, runs the core, and replies on a oneshot with the
/// encoded response body. The Crabka-private 1003/1004 keep their bespoke
/// request/response wire types.
async fn dispatch(
    api_key_n: i16,
    body: Bytes,
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    match api_key_n {
        api_key::FETCH => {
            deliver_inbound(engine, |reply| Inbound::Fetch { req: body, reply }).await
        }
        api_key::VOTE => deliver_inbound(engine, |reply| Inbound::Vote { req: body, reply }).await,
        api_key::BEGIN_QUORUM_EPOCH => {
            deliver_inbound(engine, |reply| Inbound::BeginQuorumEpoch {
                req: body,
                reply,
            })
            .await
        }
        api_key::END_QUORUM_EPOCH => {
            deliver_inbound(engine, |reply| Inbound::EndQuorumEpoch { req: body, reply }).await
        }
        API_KEY_SUBMIT_CHANGE => dispatch_submit_change(&body, engine).await,
        API_KEY_METADATA_FETCH => dispatch_metadata_fetch(&body, engine).await,
        _ => Err(RaftError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("unknown controller api key"),
        )),
    }
}

/// Deliver an [`Inbound`] to the engine and await the encoded response body.
async fn deliver_inbound<F>(engine: &KraftController, make: F) -> Result<Bytes, RaftError>
where
    F: FnOnce(oneshot::Sender<Bytes>) -> Inbound,
{
    let (reply, rx) = oneshot::channel();
    engine.deliver(make(reply)).await?;
    rx.await.map_err(|_| RaftError::Shutdown)
}

/// Handle a follower-forwarded `submit_change` (1003). The forwarder wrapped a
/// wincode-encoded `Vec<MetadataRecord>`; we submit it to the local engine
/// (presumably the leader) and translate the result into the `error_code` enum:
/// `0` applied, `1` not leader (with `leader_hint`), `2` metadata-rejected.
async fn dispatch_submit_change(body: &[u8], engine: &KraftController) -> Result<Bytes, RaftError> {
    let mut cur = body;
    let req = CrabkaSubmitChangeRequest::decode_v0(&mut cur)?;
    let records: Vec<crabka_metadata::MetadataRecord> = match <serde_wincode::SerdeCompat<
        Vec<crabka_metadata::MetadataRecord>,
    > as wincode::Deserialize>::deserialize(
        &req.records
    ) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "submit-change body decode failed");
            let resp = CrabkaSubmitChangeResponse {
                error_code: 2,
                leader_hint: -1,
            };
            let mut out = Vec::with_capacity(16);
            resp.encode_v0(&mut out);
            return Ok(Bytes::from(out));
        }
    };
    let resp = match engine.submit_change(records).await {
        Ok(()) => CrabkaSubmitChangeResponse {
            error_code: 0,
            leader_hint: -1,
        },
        Err(RaftError::Metadata(_)) => CrabkaSubmitChangeResponse {
            error_code: 2,
            leader_hint: -1,
        },
        Err(RaftError::NotLeader { current_leader }) => CrabkaSubmitChangeResponse {
            error_code: 1,
            leader_hint: current_leader
                .and_then(|l| i64::try_from(l).ok())
                .unwrap_or(-1),
        },
        Err(e) => {
            tracing::warn!(error = ?e, "submit-change failed");
            CrabkaSubmitChangeResponse {
                error_code: 3,
                leader_hint: -1,
            }
        }
    };
    let mut out = Vec::with_capacity(16);
    resp.encode_v0(&mut out);
    Ok(Bytes::from(out))
}

/// Serve a committed `__cluster_metadata` slice to a broker-only observer (1004)
/// from the engine's `KraftLog`.
async fn dispatch_metadata_fetch(
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    let mut cur = body;
    let req = CrabkaMetadataFetchRequest::decode_v0(&mut cur)?;
    let fetch_offset = req.fetch_offset.max(0);
    let max_bytes = usize::try_from(req.max_bytes.max(0)).unwrap_or(0);
    let slice = engine.metadata_fetch(fetch_offset, max_bytes).await?;
    let leader_hint: i64 = engine
        .quorum_state()
        .await
        .ok()
        .and_then(|qs| qs.leader_id)
        .and_then(|l| i64::try_from(l).ok())
        .unwrap_or(-1);

    let resp = CrabkaMetadataFetchResponse {
        error_code: 0,
        leader_hint,
        log_start_offset: slice.log_start_offset,
        high_watermark: slice.high_watermark,
        records: slice.records,
    };
    let mut out = Vec::new();
    resp.encode_v0(&mut out)?;
    Ok(Bytes::from(out))
}
