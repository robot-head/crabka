//! openraft `RaftNetwork` over Kafka TCP framing using the existing
//! `crabka-client-core::Connection`. One cached connection per peer.
//!
//! Wire shape: `RequestHeader v2` (flexible) carries a Crabka-
//! private api key (1000 = `AppendEntries`, 1001 = `Vote`) at version 0.
//! Bodies are encoded by [`crate::wire`] and travel as opaque bytes
//! through [`crabka_client_core::Connection::raw_request`]. The response
//! body decodes back into the openraft response types.
//!
//! Naming follows the `state_machine` precedent: the local types are
//! prefixed `Crabka` to avoid colliding with the openraft trait names
//! when both are imported into the same scope.
//!
//! Scope:
//!
//! - `AppendEntries`: full Raft semantics, except the `Conflict` /
//!   `PartialSuccess` paths collapse onto `HigherVote` for now — the
//!   v0 response codec only carries `success/term/last_log_index`, which
//!   is enough to make progress in a healthy 3-node quorum with no
//!   network faults.
//! - `Vote`: full semantics with the caveat that the peer's
//!   `last_log_id` is not returned (the v0 response carries only
//!   `vote_granted` + `term`).
//! - `InstallSnapshot`: ships checkpoint chunks to a follower whose log
//!   prefix has been compacted; the response carries the follower's
//!   `vote` so the leader can detect a higher term.
//!
//! `wire::Crabka*Response` can evolve to carry richer fields without
//! breaking the `RaftNetworkFactory` interface.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{LogId, Vote};
use tracing::warn;

use crabka_client_core::{ClientError, Connection, ConnectionOptions};

use crate::error::RaftError as CrabkaRaftError;
use crate::types::{Node, NodeId, TypeConfig};

/// Outbound dialer the controller hands to the raft network factory.
///
/// `crabka-raft` cannot depend on `crabka-broker` (that would be a
/// cycle), so the broker provides an impl wrapping its
/// `InterBrokerClient` (TLS + SASL) and injects it via
/// [`ControllerConfig::dialer`]. When no dialer is injected, the
/// factory falls back to a plain `Connection::connect(addr)` — the
/// PLAINTEXT path used when no TLS/SASL dialer is injected.
#[async_trait::async_trait]
pub trait OutboundDialer: Send + Sync {
    /// Open a `Connection` to the raft peer at `target` reachable on
    /// `addr`. The returned connection has already negotiated
    /// `ApiVersions` and is usable for `raw_request` immediately.
    async fn dial(
        &self,
        target: NodeId,
        addr: &str,
        options: ConnectionOptions,
    ) -> Result<Connection, ClientError>;
}

/// Default no-op dialer: opens a raw `TcpStream` via
/// `Connection::connect`. Used when the broker hasn't injected a
/// `InterBrokerClient`-backed dialer (legacy PLAINTEXT path).
pub(crate) struct PlaintextDialer;

#[async_trait::async_trait]
impl OutboundDialer for PlaintextDialer {
    async fn dial(
        &self,
        _target: NodeId,
        addr: &str,
        options: ConnectionOptions,
    ) -> Result<Connection, ClientError> {
        let sock: SocketAddr = addr.parse().map_err(|e: std::net::AddrParseError| {
            ClientError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid raft peer address {addr:?}: {e}"),
            ))
        })?;
        Connection::connect(sock, options).await
    }
}
use crate::wire::{
    API_KEY_APPEND_ENTRIES, API_KEY_INSTALL_SNAPSHOT, API_KEY_VOTE, CrabkaAppendEntriesRequest,
    CrabkaAppendEntriesResponse, CrabkaLogEntry, CrabkaVoteRequest, CrabkaVoteResponse,
};

/// Factory of per-peer `RaftNetwork` adapters. Maintains a `DashMap` of
/// cached connections keyed by `NodeId`.
pub(crate) struct CrabkaRaftNetworkFactory {
    connections: Arc<DashMap<NodeId, Arc<Connection>>>,
    client_id: String,
    dialer: Arc<dyn OutboundDialer>,
}

impl CrabkaRaftNetworkFactory {
    pub(crate) fn new(client_id: String, dialer: Arc<dyn OutboundDialer>) -> Self {
        Self {
            connections: Arc::new(DashMap::new()),
            client_id,
            dialer,
        }
    }

    /// Look up or open a connection to `target` at `addr`.
    ///
    /// The cache holds an `Arc<Connection>`; cloning is cheap. Connection
    /// establishment is delegated to the injected [`OutboundDialer`] so
    /// inter-broker TLS + SASL can transparently slot in at the broker
    /// layer.
    async fn connect(
        &self,
        target: NodeId,
        addr: &str,
    ) -> Result<Arc<Connection>, CrabkaRaftError> {
        if let Some(c) = self.connections.get(&target) {
            return Ok(c.value().clone());
        }
        let opts = ConnectionOptions {
            client_id: self.client_id.clone(),
            ..ConnectionOptions::default()
        };
        let conn = Arc::new(self.dialer.dial(target, addr, opts).await?);
        self.connections.insert(target, conn.clone());
        Ok(conn)
    }
}

impl Clone for CrabkaRaftNetworkFactory {
    fn clone(&self) -> Self {
        Self {
            connections: self.connections.clone(),
            client_id: self.client_id.clone(),
            dialer: self.dialer.clone(),
        }
    }
}

impl openraft::network::RaftNetworkFactory<TypeConfig> for CrabkaRaftNetworkFactory {
    type Network = CrabkaRaftNetworkConn;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        // KIP-853 voter nodes carry their listener endpoints; openraft dials
        // the CONTROLLER endpoint. A node with no resolvable controller
        // endpoint yields an empty addr — `connect` then fails to parse it
        // and surfaces `Unreachable`, which is the correct backoff behavior.
        let addr = if let Some(a) = node.controller_addr() {
            a.to_string()
        } else {
            warn!(
                target_node = target,
                directory_id = %node.directory_id,
                endpoints = ?node.endpoints,
                "raft node has no resolvable controller endpoint; connection will fail \
                 and openraft will back off"
            );
            String::new()
        };
        CrabkaRaftNetworkConn {
            target,
            addr,
            factory: self.clone(),
        }
    }
}

/// Per-peer adapter created by the factory's `new_client`.
pub(crate) struct CrabkaRaftNetworkConn {
    target: NodeId,
    addr: String,
    factory: CrabkaRaftNetworkFactory,
}

impl openraft::network::RaftNetwork<TypeConfig> for CrabkaRaftNetworkConn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let conn = self
            .factory
            .connect(self.target, &self.addr)
            .await
            .map_err(|e| map_connect_err(&e))?;
        let body = encode_append_entries(&rpc).map_err(|e| map_encode_err(&e))?;
        let resp_body = conn
            .raw_request(API_KEY_APPEND_ENTRIES, 0, body)
            .await
            .map_err(|e| map_client_err(&e))?;
        decode_append_entries_resp(&resp_body, &rpc).map_err(|e| map_encode_err(&e))
    }

    /// Ship one `InstallSnapshot` chunk to the peer: serialize `vote` +
    /// `meta` as bincode, frame the chunk via [`crate::wire`], and decode
    /// the peer's returned `vote`. A far-behind follower whose log prefix
    /// has been compacted behind a checkpoint catches up through this
    /// path rather than append-entries.
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>>,
    > {
        use serde_wincode::SerdeCompat;
        use wincode::{Deserialize as _, Serialize as _};

        let conn = self
            .factory
            .connect(self.target, &self.addr)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let vote = <SerdeCompat<Vote<NodeId>>>::serialize(&rpc.vote).map_err(snapshot_net_err)?;
        let meta = <SerdeCompat<openraft::SnapshotMeta<NodeId, Node>>>::serialize(&rpc.meta)
            .map_err(snapshot_net_err)?;
        let mut body = Vec::new();
        crate::wire::CrabkaInstallSnapshotRequest {
            vote: Bytes::from(vote),
            meta: Bytes::from(meta),
            offset: i64::try_from(rpc.offset).unwrap_or(i64::MAX),
            data: Bytes::from(rpc.data),
            done: rpc.done,
        }
        .encode_v0(&mut body)
        .map_err(snapshot_net_err)?;
        let resp_body = conn
            .raw_request(API_KEY_INSTALL_SNAPSHOT, 0, Bytes::from(body))
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mut cur: &[u8] = &resp_body;
        let resp = crate::wire::CrabkaInstallSnapshotResponse::decode_v0(&mut cur)
            .map_err(snapshot_net_err)?;
        let vote: Vote<NodeId> =
            <SerdeCompat<Vote<NodeId>>>::deserialize(&resp.vote).map_err(snapshot_net_err)?;
        Ok(InstallSnapshotResponse { vote })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let conn = self
            .factory
            .connect(self.target, &self.addr)
            .await
            .map_err(|e| map_connect_err(&e))?;
        let body = encode_vote(&rpc).map_err(|e| map_encode_err(&e))?;
        let resp_body = conn
            .raw_request(API_KEY_VOTE, 0, body)
            .await
            .map_err(|e| map_client_err(&e))?;
        decode_vote_resp(&resp_body, &rpc).map_err(|e| map_encode_err(&e))
    }
}

/// Map a connection-establishment failure to openraft's `Unreachable`
/// variant so the engine backs off rather than spinning.
fn map_connect_err(e: &CrabkaRaftError) -> RPCError<NodeId, Node, RaftError<NodeId>> {
    RPCError::Unreachable(Unreachable::new(e))
}

/// Map a per-request transport failure to `Network` so openraft retries
/// immediately.
fn map_client_err(e: &ClientError) -> RPCError<NodeId, Node, RaftError<NodeId>> {
    RPCError::Network(NetworkError::new(e))
}

/// Map a codec failure (encode body or decode response) to `Network`.
/// These are programmer errors in practice but we surface them as
/// transient so openraft logs + retries.
fn map_encode_err(e: &CrabkaRaftError) -> RPCError<NodeId, Node, RaftError<NodeId>> {
    RPCError::Network(NetworkError::new(e))
}

/// Map a codec failure on the `InstallSnapshot` path to `Network`. The
/// install path carries `InstallSnapshotError` in its `RPCError`, a
/// different generic param than the append/vote mappers, so it needs its
/// own helper.
fn snapshot_net_err<E: std::error::Error + 'static>(
    e: E,
) -> RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>> {
    RPCError::Network(NetworkError::new(&e))
}

fn encode_append_entries(rpc: &AppendEntriesRequest<TypeConfig>) -> Result<Bytes, CrabkaRaftError> {
    use openraft::EntryPayload;
    use serde_wincode::SerdeCompat;
    use wincode::Serialize as _;

    let mut entries = Vec::with_capacity(rpc.entries.len());
    for e in &rpc.entries {
        let payload_kind: i8 = match &e.payload {
            EntryPayload::Blank => 0,
            EntryPayload::Normal(_) => 1,
            EntryPayload::Membership(_) => 2,
        };
        let payload = <SerdeCompat<EntryPayload<TypeConfig>>>::serialize(&e.payload)?;
        entries.push(CrabkaLogEntry {
            log_index: i64::try_from(e.log_id.index).unwrap_or(i64::MAX),
            log_term: i64::try_from(e.log_id.leader_id.term).unwrap_or(i64::MAX),
            log_node_id: i64::try_from(e.log_id.leader_id.node_id).unwrap_or(i64::MAX),
            payload_kind,
            payload: Bytes::from(payload),
        });
    }

    // `Vote::leader_id` carries the leader node id plus the term. With
    // openraft's default (non-`single-term-leader`) feature set, `LeaderId`
    // exposes `node_id` directly rather than `Option<voted_for>`.
    let leader_node = rpc.vote.leader_id.node_id;
    let req = CrabkaAppendEntriesRequest {
        node_id: i32::try_from(leader_node).unwrap_or(-1),
        term: i64::try_from(rpc.vote.leader_id.term).unwrap_or(i64::MAX),
        leader_id: i32::try_from(leader_node).unwrap_or(-1),
        prev_log_index: rpc
            .prev_log_id
            .map_or(-1, |l| i64::try_from(l.index).unwrap_or(i64::MAX)),
        prev_log_term: rpc
            .prev_log_id
            .map_or(-1, |l| i64::try_from(l.leader_id.term).unwrap_or(i64::MAX)),
        prev_log_node_id: rpc.prev_log_id.map_or(-1, |l| {
            i64::try_from(l.leader_id.node_id).unwrap_or(i64::MAX)
        }),
        leader_commit: rpc
            .leader_commit
            .map_or(-1, |l| i64::try_from(l.index).unwrap_or(i64::MAX)),
        leader_commit_term: rpc
            .leader_commit
            .map_or(-1, |l| i64::try_from(l.leader_id.term).unwrap_or(i64::MAX)),
        leader_commit_node_id: rpc.leader_commit.map_or(-1, |l| {
            i64::try_from(l.leader_id.node_id).unwrap_or(i64::MAX)
        }),
        entries,
    };
    let mut out = Vec::with_capacity(64);
    req.encode_v0(&mut out)?;
    Ok(Bytes::from(out))
}

/// Decode an `AppendEntriesResponse`.
///
/// Response mapping (see module doc for context):
///
/// - `success == true` => `Success`.
/// - `success == false` and `resp.term > req_vote.term` => `HigherVote`
///   carrying a fresh uncommitted vote at the higher term. The sender
///   node id field is unknown over the wire, so we reuse the local
///   candidate id; openraft only uses the term ordering for backoff.
/// - `success == false` and `resp.term == req_vote.term` => `Conflict`.
///   This is the "log mismatch" path; the leader will walk back its
///   `prev_log_id` and retry. `last_log_index` is informational only;
///   openraft 0.9's `Conflict` variant carries no fields.
fn decode_append_entries_resp(
    body: &[u8],
    req: &AppendEntriesRequest<TypeConfig>,
) -> Result<AppendEntriesResponse<NodeId>, CrabkaRaftError> {
    let mut cur = body;
    let resp = CrabkaAppendEntriesResponse::decode_v0(&mut cur)?;
    if resp.success {
        return Ok(AppendEntriesResponse::Success);
    }
    let resp_term = u64::try_from(resp.term).unwrap_or(0);
    if resp_term > req.vote.leader_id.term {
        let voter = req.vote.leader_id.node_id;
        Ok(AppendEntriesResponse::HigherVote(Vote::new(
            resp_term, voter,
        )))
    } else if req.prev_log_id.is_none() {
        // openraft's `Conflict` variant assumes `prev_log_id.is_some()`
        // — returning it for an empty-prev request trips the
        // "prev_log_id=None never conflict" debug-assert on the leader
        // (openraft 0.9.24 src/replication/mod.rs:486). An empty-prev
        // AppendEntries cannot meaningfully conflict on log entries.
        Ok(AppendEntriesResponse::Success)
    } else {
        Ok(AppendEntriesResponse::Conflict)
    }
}

fn encode_vote(rpc: &VoteRequest<NodeId>) -> Result<Bytes, CrabkaRaftError> {
    let candidate = rpc.vote.leader_id.node_id;
    let req = CrabkaVoteRequest {
        term: i64::try_from(rpc.vote.leader_id.term).unwrap_or(i64::MAX),
        candidate_id: candidate,
        last_log_index: rpc
            .last_log_id
            .map_or(-1, |l| i64::try_from(l.index).unwrap_or(i64::MAX)),
        last_log_term: rpc
            .last_log_id
            .map_or(-1, |l| i64::try_from(l.leader_id.term).unwrap_or(i64::MAX)),
        last_log_node_id: rpc.last_log_id.map_or(-1, |l| {
            i64::try_from(l.leader_id.node_id).unwrap_or(i64::MAX)
        }),
    };
    let mut out = Vec::with_capacity(32);
    req.encode_v0(&mut out)?;
    Ok(Bytes::from(out))
}

/// Decode a `VoteResponse`. The v0 wire form carries only
/// `vote_granted + term`, so we synthesize the openraft response with
/// `last_log_id = None`. Openraft tolerates a missing `last_log_id` —
/// it's an optimization hint, not a correctness input.
fn decode_vote_resp(
    body: &[u8],
    req: &VoteRequest<NodeId>,
) -> Result<VoteResponse<NodeId>, CrabkaRaftError> {
    let mut cur = body;
    let resp = CrabkaVoteResponse::decode_v0(&mut cur)?;
    let resp_term = u64::try_from(resp.term).unwrap_or(0);
    // Reconstruct the peer's vote: if granted, the peer voted for our
    // candidate at the request term. If not granted, the peer reports a
    // (possibly higher) term but the voted-for is unknown over the wire,
    // so we fall back to the candidate id — openraft only consults the
    // term for backoff decisions.
    let voter = req.vote.leader_id.node_id;
    let vote = Vote::new(resp_term, voter);
    let last_log_id: Option<LogId<NodeId>> = None;
    Ok(VoteResponse::new(vote, last_log_id, resp.vote_granted))
}
