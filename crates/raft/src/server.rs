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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::error::RaftError;
use crate::types::{Raft, TypeConfig};
use crate::wire::{
    API_KEY_APPEND_ENTRIES, API_KEY_INSTALL_SNAPSHOT, API_KEY_VOTE, CrabkaAppendEntriesRequest,
    CrabkaAppendEntriesResponse, CrabkaInstallSnapshotResponse, CrabkaVoteRequest,
    CrabkaVoteResponse,
};

const REJECT_NOT_IMPLEMENTED: i16 = -1;

#[allow(dead_code)]
pub(crate) async fn run(listener: TcpListener, raft: Arc<Raft>, shutdown: CancellationToken) {
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
                        let shutdown = shutdown.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_conn(stream, raft, shutdown).await {
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

async fn handle_conn(
    mut stream: TcpStream,
    raft: Arc<Raft>,
    shutdown: CancellationToken,
) -> Result<(), RaftError> {
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
                let resp = dispatch(api_key, &body, &raft).await?;
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

async fn read_one_request(stream: &mut TcpStream) -> Result<(i16, i32, Bytes), RaftError> {
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

async fn write_response(
    stream: &mut TcpStream,
    correlation_id: i32,
    body: Bytes,
) -> Result<(), RaftError> {
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

async fn dispatch(api_key: i16, body: &[u8], raft: &Raft) -> Result<Bytes, RaftError> {
    match api_key {
        API_KEY_APPEND_ENTRIES => {
            let mut cur = body;
            let req = CrabkaAppendEntriesRequest::decode_v0(&mut cur)?;
            let openraft_req = convert_append_entries(req);
            let res = raft
                .append_entries(openraft_req)
                .await
                .map_err(|e| RaftError::Openraft(format!("{e:?}")))?;
            let mut out = Vec::with_capacity(32);
            CrabkaAppendEntriesResponse {
                success: matches!(res, openraft::raft::AppendEntriesResponse::Success),
                term: 0,
                last_log_index: 0,
            }
            .encode_v0(&mut out)?;
            Ok(Bytes::from(out))
        }
        API_KEY_VOTE => {
            let mut cur = body;
            let req = CrabkaVoteRequest::decode_v0(&mut cur)?;
            let openraft_req = openraft::raft::VoteRequest {
                vote: openraft::Vote::new(u64::try_from(req.term).unwrap_or(0), req.candidate_id),
                last_log_id: None,
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
        _ => Err(RaftError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("unknown controller api key"),
        )),
    }
}

fn convert_append_entries(
    req: CrabkaAppendEntriesRequest,
) -> openraft::raft::AppendEntriesRequest<TypeConfig> {
    use bincode::config::standard;
    let leader_node = u64::try_from(req.leader_id.max(0)).unwrap_or(0);
    let entries = req
        .entries
        .into_iter()
        .map(|e| {
            let payload: openraft::EntryPayload<TypeConfig> =
                bincode::serde::decode_from_slice(&e.payload, standard())
                    .map_or(openraft::EntryPayload::Blank, |(v, _)| v);
            openraft::Entry {
                log_id: openraft::LogId {
                    leader_id: openraft::LeaderId::new(
                        u64::try_from(e.log_term).unwrap_or(0),
                        leader_node,
                    ),
                    index: u64::try_from(e.log_index).unwrap_or(0),
                },
                payload,
            }
        })
        .collect();
    openraft::raft::AppendEntriesRequest {
        vote: openraft::Vote::new(u64::try_from(req.term).unwrap_or(0), leader_node),
        prev_log_id: (req.prev_log_index >= 0).then(|| openraft::LogId {
            leader_id: openraft::LeaderId::new(
                u64::try_from(req.prev_log_term).unwrap_or(0),
                leader_node,
            ),
            index: u64::try_from(req.prev_log_index).unwrap_or(0),
        }),
        entries,
        leader_commit: (req.leader_commit >= 0).then(|| openraft::LogId {
            leader_id: openraft::LeaderId::new(0, 0),
            index: u64::try_from(req.leader_commit).unwrap_or(0),
        }),
    }
}
