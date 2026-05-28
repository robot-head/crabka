//! Accept loop for the controller TCP listener. Receives Crabka-private
//! Raft RPCs and feeds them into the local `Raft` instance.
//!
//! Wire shape matches `crabka_client_core::Connection::raw_request`:
//!
//! - Request: `len(i32) | RequestHeader v2 (flexible) | body`
//! - Response: `len(i32) | correlation_id(i32) | tagged_fields(0u8) | body`
//!
//! `RequestHeader` v2 = `api_key(i16) api_version(i16) correlation_id(i32)
//! client_id(NULLABLE_STRING) tagged_fields(varint=0)`. We parse and
//! discard everything but `api_key` and `correlation_id`.
//!
//! The bodies are decoded by [`crate::wire`] into Crabka-private types
//! and converted into openraft's `AppendEntriesRequest` /
//! `VoteRequest`. Snapshot installation is stubbed — the response carries
//! `error_code = REJECT_NOT_IMPLEMENTED` so callers know not to retry.

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::error::RaftError;
use crate::types::{AppData, Raft, TypeConfig};
use crate::wire::{
    API_KEY_APPEND_ENTRIES, API_KEY_INSTALL_SNAPSHOT, API_KEY_METADATA_FETCH,
    API_KEY_SUBMIT_CHANGE, API_KEY_VOTE, CrabkaAppendEntriesRequest, CrabkaAppendEntriesResponse,
    CrabkaInstallSnapshotResponse, CrabkaMetadataFetchRequest, CrabkaMetadataFetchResponse,
    CrabkaSubmitChangeRequest, CrabkaSubmitChangeResponse, CrabkaVoteRequest, CrabkaVoteResponse,
};

const REJECT_NOT_IMPLEMENTED: i16 = -1;

/// Kafka's `ApiVersions` API key. The controller TCP listener has to
/// answer this because `crabka_client_core::Connection::connect`
/// performs an `ApiVersions` handshake before any other request — the
/// openraft network factory leans on that connection for the
/// Crabka-private Raft RPCs.
const API_KEY_API_VERSIONS: i16 = 18;

pub(crate) async fn run(
    listener: TcpListener,
    raft: Arc<Raft>,
    log_store: Arc<crate::log_store::RaftLogStore>,
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
                        let raft = raft.clone();
                        let log_store = log_store.clone();
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
                            if let Err(e) = handle_conn(boxed, raft, log_store, shutdown).await {
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
    raft: Arc<Raft>,
    log_store: Arc<crate::log_store::RaftLogStore>,
    shutdown: CancellationToken,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            res = read_one_request(&mut stream) => {
                let (api_key, correlation_id, body) = match res {
                    Ok(v) => v,
                    Err(e) => {
                        // Treat peer EOF as a clean shutdown of this conn,
                        // not an error to bubble up.
                        if is_eof(&e) {
                            return Ok(());
                        }
                        return Err(e);
                    }
                };
                // ApiVersions (api_key 18) is the bootstrap handshake
                // performed by `crabka_client_core::Connection::connect`. It
                // arrives at v0 with a header v1 (no tagged-fields byte) and
                // expects a ResponseHeader v0 reply (also no tagged-fields
                // byte). The Crabka-private Raft RPCs use flexible headers
                // and prepend a tagged-fields byte to the response — but
                // the ApiVersions response is the documented Kafka
                // asymmetry that must NOT include it. We dispatch and
                // serialise this path separately rather than poisoning the
                // generic codec.
                if api_key == API_KEY_API_VERSIONS {
                    let resp = api_versions_response_body();
                    write_response_no_tagged_fields(&mut stream, correlation_id, resp).await?;
                    continue;
                }
                let resp = dispatch(api_key, &body, &raft, &log_store).await?;
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

async fn read_one_request<S>(stream: &mut S) -> Result<(i16, i32, Bytes), RaftError>
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
    let api_key = cur.get_i16();
    let _api_version = cur.get_i16();
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

    Ok((api_key, correlation_id, Bytes::copy_from_slice(cur)))
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
    frame.put_u8(0); // empty tagged_fields
    frame.put_slice(&body);

    let mut len_prefix = [0u8; 4];
    len_prefix.copy_from_slice(&i32::try_from(frame.len()).unwrap_or(i32::MAX).to_be_bytes());
    stream.write_all(&len_prefix).await.map_err(io_err)?;
    stream.write_all(&frame).await.map_err(io_err)?;
    stream.flush().await.map_err(io_err)?;
    Ok(())
}

/// Write a response without the leading tagged-fields byte. Used only
/// by the `ApiVersions` v0 path, which decodes a `ResponseHeader v0`.
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

/// Minimal `ApiVersionsResponse` v0: `error_code = 0`, empty
/// `api_keys` array. The openraft network adapter doesn't consult the
/// returned table — it always issues `raw_request` at version 0 against
/// the Crabka-private api keys — so an empty list is harmless.
fn api_versions_response_body() -> Bytes {
    let mut out = BytesMut::with_capacity(6);
    // error_code: INT16
    out.put_i16(0);
    // api_keys: ARRAY (v0 is non-flexible → length-prefixed by INT32, items omitted).
    out.put_i32(0);
    out.freeze()
}

async fn dispatch(
    api_key: i16,
    body: &[u8],
    raft: &Raft,
    log_store: &Arc<crate::log_store::RaftLogStore>,
) -> Result<Bytes, RaftError> {
    match api_key {
        API_KEY_APPEND_ENTRIES => {
            let mut cur = body;
            let req = CrabkaAppendEntriesRequest::decode_v0(&mut cur)?;
            let req_term = req.term;
            let openraft_req = convert_append_entries(req);
            let res = raft
                .append_entries(openraft_req)
                .await
                .map_err(|e| RaftError::Openraft(format!("{e:?}")))?;
            // Map openraft's response variants onto the v0 wire shape so
            // the leader's decoder can distinguish HigherVote (back off
            // and rediscover) from Conflict (walk back prev_log_id) from
            // Success. Returning a flat success=false with term=0 makes
            // the leader interpret every failure as a Conflict, which
            // panics openraft when prev_log_id was None.
            let (success, term) = match &res {
                openraft::raft::AppendEntriesResponse::Success
                | openraft::raft::AppendEntriesResponse::PartialSuccess(_) => (true, 0i64),
                openraft::raft::AppendEntriesResponse::HigherVote(v) => {
                    (false, i64::try_from(v.leader_id.term).unwrap_or(i64::MAX))
                }
                openraft::raft::AppendEntriesResponse::Conflict => (false, req_term),
            };
            let mut out = Vec::with_capacity(32);
            CrabkaAppendEntriesResponse {
                success,
                term,
                last_log_index: 0,
            }
            .encode_v0(&mut out)?;
            Ok(Bytes::from(out))
        }
        API_KEY_VOTE => {
            let mut cur = body;
            let req = CrabkaVoteRequest::decode_v0(&mut cur)?;
            // Reconstruct the candidate's `last_log_id` from the wire
            // form so the receiver's "log up-to-date" check evaluates
            // against the real candidate state. Sentinel `-1` (or any
            // negative) means "no log entries"; anything else maps
            // straight back to `LogId::new(...)`.
            let last_log_id = if req.last_log_index < 0 || req.last_log_term < 0 {
                None
            } else {
                Some(openraft::LogId {
                    leader_id: openraft::LeaderId::new(
                        u64::try_from(req.last_log_term).unwrap_or(0),
                        u64::try_from(req.last_log_node_id.max(0)).unwrap_or(0),
                    ),
                    index: u64::try_from(req.last_log_index).unwrap_or(0),
                })
            };
            let openraft_req = openraft::raft::VoteRequest {
                vote: openraft::Vote::new(u64::try_from(req.term).unwrap_or(0), req.candidate_id),
                last_log_id,
            };
            let res = raft
                .vote(openraft_req)
                .await
                .map_err(|e| RaftError::Openraft(format!("{e:?}")))?;
            let mut out = Vec::with_capacity(16);
            CrabkaVoteResponse {
                vote_granted: res.vote_granted,
                term: i64::try_from(res.vote.leader_id.term).unwrap_or(i64::MAX),
            }
            .encode_v0(&mut out)?;
            Ok(Bytes::from(out))
        }
        API_KEY_INSTALL_SNAPSHOT => {
            let mut out = Vec::with_capacity(4);
            CrabkaInstallSnapshotResponse {
                error_code: REJECT_NOT_IMPLEMENTED,
            }
            .encode_v0(&mut out)?;
            Ok(Bytes::from(out))
        }
        API_KEY_SUBMIT_CHANGE => dispatch_submit_change(body, raft).await,
        API_KEY_METADATA_FETCH => dispatch_metadata_fetch(body, raft, log_store).await,
        _ => Err(RaftError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("unknown controller api key"),
        )),
    }
}

/// Handle a follower-forwarded `Controller::submit_change` request. The
/// follower has wrapped the bincode-encoded `Vec<MetadataRecord>` in a
/// `CrabkaSubmitChangeRequest`; we hand the records to the local raft
/// (which is presumably the leader) via `client_write`, then translate
/// the openraft response into the slice-7 `error_code` enum:
///
/// - `0`: applied cleanly (no per-record rejections).
/// - `1`: not leader (the response carries `leader_hint` so the
///   follower can retry against a different peer).
/// - `2`: applied to the log, but the state machine rejected one or
///   more records at apply time (e.g., a `CreateTopics` race).
/// - `3`: opaque openraft failure — surface as `RaftError::NotLeader`
///   on the caller side and let the higher layer translate.
async fn dispatch_submit_change(body: &[u8], raft: &Raft) -> Result<Bytes, RaftError> {
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
    let data = AppData { records };
    let resp = match raft.client_write(data).await {
        Ok(r) if r.data.rejected.is_empty() => CrabkaSubmitChangeResponse {
            error_code: 0,
            leader_hint: -1,
        },
        Ok(_) => CrabkaSubmitChangeResponse {
            error_code: 2,
            leader_hint: -1,
        },
        Err(openraft::error::RaftError::APIError(
            openraft::error::ClientWriteError::ForwardToLeader(f),
        )) => CrabkaSubmitChangeResponse {
            error_code: 1,
            leader_hint: f.leader_id.map_or(-1, |l| i64::try_from(l).unwrap_or(-1)),
        },
        Err(e) => {
            tracing::warn!(error = ?e, "submit-change client_write failed");
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

/// Serve a slice of committed `__cluster_metadata` entries to a
/// broker-only observer. Reads `[fetch_offset, high_watermark]` from the
/// log store, encodes each entry as a Kafka record batch, and returns
/// them plus `log_start_offset`, `high_watermark`, and a `leader_hint`.
async fn dispatch_metadata_fetch(
    body: &[u8],
    raft: &Raft,
    log_store: &Arc<crate::log_store::RaftLogStore>,
) -> Result<Bytes, RaftError> {
    let mut cur = body;
    let req = CrabkaMetadataFetchRequest::decode_v0(&mut cur)?;
    let metrics = raft.metrics().borrow().clone();
    let high_watermark = metrics.last_applied.as_ref().map_or(0, |l| l.index);
    let leader_hint = metrics
        .current_leader
        .map_or(-1, |l| i64::try_from(l).unwrap_or(-1));
    let log_start_offset = log_store.log_start_index().await;

    let fetch_offset = u64::try_from(req.fetch_offset.max(0)).unwrap_or(0);
    let max_bytes = usize::try_from(req.max_bytes.max(0)).unwrap_or(0);
    let entries = if fetch_offset > high_watermark {
        Vec::new()
    } else {
        log_store.read_range(fetch_offset..=high_watermark).await
    };
    let records = crate::metadata_fetch::encode_committed_records(&entries, max_bytes);

    let resp = CrabkaMetadataFetchResponse {
        error_code: 0,
        leader_hint,
        log_start_offset: i64::try_from(log_start_offset).unwrap_or(i64::MAX),
        high_watermark: i64::try_from(high_watermark).unwrap_or(i64::MAX),
        records,
    };
    let mut out = Vec::new();
    resp.encode_v0(&mut out)?;
    Ok(Bytes::from(out))
}

fn convert_append_entries(
    req: CrabkaAppendEntriesRequest,
) -> openraft::raft::AppendEntriesRequest<TypeConfig> {
    use serde_wincode::SerdeCompat;
    use wincode::Deserialize as _;
    let leader_node = u64::try_from(req.leader_id.max(0)).unwrap_or(0);
    let entries = req
        .entries
        .into_iter()
        .map(|e| {
            let payload: openraft::EntryPayload<TypeConfig> =
                <SerdeCompat<openraft::EntryPayload<TypeConfig>>>::deserialize(&e.payload)
                    .unwrap_or(openraft::EntryPayload::Blank);
            openraft::Entry {
                log_id: openraft::LogId {
                    leader_id: openraft::LeaderId::new(
                        u64::try_from(e.log_term).unwrap_or(0),
                        u64::try_from(e.log_node_id.max(0)).unwrap_or(0),
                    ),
                    index: u64::try_from(e.log_index).unwrap_or(0),
                },
                payload,
            }
        })
        .collect();
    openraft::raft::AppendEntriesRequest {
        // AppendEntries can only be issued by an elected leader, so the
        // wire-side vote must be reconstructed as `committed`. `Vote::new`
        // yields an uncommitted vote, which trips openraft's
        // `update_accepted` debug-assert (`vote must be committed`).
        vote: openraft::Vote::new_committed(u64::try_from(req.term).unwrap_or(0), leader_node),
        prev_log_id: (req.prev_log_index >= 0).then(|| openraft::LogId {
            leader_id: openraft::LeaderId::new(
                u64::try_from(req.prev_log_term).unwrap_or(0),
                u64::try_from(req.prev_log_node_id.max(0)).unwrap_or(0),
            ),
            index: u64::try_from(req.prev_log_index).unwrap_or(0),
        }),
        entries,
        leader_commit: (req.leader_commit >= 0).then(|| openraft::LogId {
            leader_id: openraft::LeaderId::new(
                u64::try_from(req.leader_commit_term.max(0)).unwrap_or(0),
                u64::try_from(req.leader_commit_node_id.max(0)).unwrap_or(0),
            ),
            index: u64::try_from(req.leader_commit).unwrap_or(0),
        }),
    }
}
