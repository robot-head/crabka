//! Transport seam for the [`KraftController`](crate::kraft::controller::KraftController):
//! outbound peer RPCs go through [`PeerSender`] (real TCP in prod, in-memory in
//! tests); inbound KIP-595 RPCs arrive as [`Inbound`] carrying a oneshot reply
//! channel; handle-facing requests arrive as [`Command`].
//!
//! This module is the wire-agnostic boundary: the event loop never touches
//! sockets directly. Tasks 6/7 supply the in-memory and real-TCP `PeerSender`
//! impls; Task 1/2 only need the trait + the command/inbound plumbing.

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::error::RaftError;
use crate::kraft::event::Event;
use crate::kraft::types::NodeId;

/// A decoded inbound KIP-595 RPC plus a oneshot to reply on. The event loop
/// decodes the body into a core [`Event`], runs it, and encodes the produced
/// response (e.g. `ReplyVote`) back onto `reply`.
#[derive(Debug)]
pub enum Inbound {
    Vote {
        req: Bytes,
        reply: oneshot::Sender<Bytes>,
    },
    BeginQuorumEpoch {
        req: Bytes,
        reply: oneshot::Sender<Bytes>,
    },
    EndQuorumEpoch {
        req: Bytes,
        reply: oneshot::Sender<Bytes>,
    },
    Fetch {
        req: Bytes,
        reply: oneshot::Sender<Bytes>,
    },
}

/// Everything that arrives on the engine's mpsc and drives one turn of the
/// loop. The full handle-facing variants (`SubmitChange`, `TriggerSnapshot`,
/// ...) are fleshed out in Task 4; Task 1/2 need the inbound-RPC path, the
/// raw-event injection used by the driver/contract tests, and shutdown.
#[derive(Debug)]
pub enum Command {
    /// An inbound peer RPC with a oneshot to reply on.
    Inbound(Inbound),
    /// Inject a core [`Event`] directly (test-only driver entrypoint; also how
    /// the loop feeds peer-RPC responses back to itself as the matching
    /// `Receive*Response` event in later tasks).
    Event(Event),
    /// Test-only: append a metadata batch to the log (as the leader's
    /// `submit_change` will in Task 4) and drive commit through the real
    /// apply pipeline. Exercises the Task 2 `AppendLeaderChange`/`advance_hwm`/
    /// decode/`validate`/`apply`/publish path without the network or Task 4's
    /// submit machinery. Replies with the appended base offset.
    #[cfg(test)]
    TestAppendAndCommit {
        records: Vec<crabka_metadata::MetadataRecord>,
        reply: oneshot::Sender<i64>,
    },
    /// Stop the loop.
    Shutdown,
}

/// Outbound peer RPC sender. Encodes nothing itself — the event loop hands it
/// the already-encoded KIP-595 request body and the destination peer; the impl
/// dials/sends and returns the raw response body.
///
/// Matches the `async_trait` mechanism used by
/// [`OutboundDialer`](crate::network::OutboundDialer).
#[async_trait::async_trait]
pub trait PeerSender: Send + Sync {
    /// Send `body` (a KIP-595 request for `api_key`) to `peer` and return the
    /// raw response body.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the peer is unreachable or the RPC fails.
    async fn send(&self, peer: NodeId, api_key: i16, body: Bytes) -> Result<Bytes, RaftError>;
}

/// A no-op `PeerSender` for single-voter / no-network tests: every send fails
/// as unreachable. A single voter never sends peer RPCs (it wins its own
/// election immediately), so this lets the contract tests run without wiring a
/// real transport.
pub struct NullPeerSender;

#[async_trait::async_trait]
impl PeerSender for NullPeerSender {
    async fn send(&self, peer: NodeId, _api_key: i16, _body: Bytes) -> Result<Bytes, RaftError> {
        Err(RaftError::NotLeader {
            current_leader: Some(peer),
        })
    }
}
