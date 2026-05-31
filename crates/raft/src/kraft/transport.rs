//! Transport seam for the [`KraftController`](crate::kraft::controller::KraftController):
//! outbound peer RPCs go through [`PeerSender`] (real TCP in prod, in-memory in
//! tests); inbound KIP-595 RPCs arrive as [`Inbound`] carrying a oneshot reply
//! channel; handle-facing requests arrive as [`Command`].
//!
//! This module is the wire-agnostic boundary: the event loop never touches
//! sockets directly. Tasks 6/7 supply the in-memory and real-TCP `PeerSender`
//! impls; Task 1/2 only need the trait + the command/inbound plumbing.
//!
//! ## Node-local peer codec (Tasks 3–6)
//!
//! Until Task 7 wires the generated KIP-595 codecs, peer RPC request and
//! response *bodies* are encoded with the deterministic node-local
//! [`wire`] codec defined here. The engine encodes a [`PeerRequest`] into the
//! body it hands [`PeerSender::send`]; the in-memory transport (Task 6) decodes
//! it, drives the receiving engine, and encodes a [`PeerResponse`] back. The
//! sending engine then decodes that [`PeerResponse`] into the matching core
//! [`Event`] (`ReceiveVoteResponse` / `ReceiveFetchResponse`). This keeps the
//! send path fire-and-forget: the engine never `.await`s a peer RPC inline.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::sync::oneshot;

use crate::error::RaftError;
use crate::kraft::event::Event;
use crate::kraft::types::{LeaderEpoch, LogOffsetMetadata, NodeId};

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
/// loop.
pub enum Command {
    /// An inbound peer RPC with a oneshot to reply on.
    Inbound(Inbound),
    /// Inject a core [`Event`] directly (test/driver entrypoint; also how the
    /// loop feeds peer-RPC responses back to itself as the matching
    /// `Receive*Response` event — the fire-and-forget feedback path).
    Event(Event),
    /// A Fetch RESPONSE the follower received from the leader. Unlike other peer
    /// responses (which decode to a pure core event), a Fetch response carries
    /// log records the follower must truncate/append/apply BEFORE feeding the
    /// `ReceiveFetchResponse` event to the core — so it gets its own command
    /// rather than going through the pure `Event` feedback path.
    FetchResponse {
        /// The leader that answered (the responder peer).
        from: NodeId,
        /// The raw encoded [`wire::PeerResponse::Fetch`] body.
        body: Bytes,
    },
    /// A timer (election / fetch / heartbeat) fired. The loop maps it to the
    /// right core event after consulting liveness state (a fetch tick re-polls
    /// rather than electing unless the leader has been missed enough times).
    Timer(TimerTick),
    /// Handle op: append + commit a metadata batch as the leader, replying once
    /// it is committed and applied (or with a rejection).
    SubmitChange {
        records: Vec<crabka_metadata::MetadataRecord>,
        reply: oneshot::Sender<Result<(), RaftError>>,
    },
    /// Handle op: snapshot the current image to a checkpoint.
    TriggerSnapshot {
        reply: oneshot::Sender<Result<(), RaftError>>,
    },
    /// Handle op: read a structured snapshot of consensus state for
    /// `DescribeQuorum`.
    QuorumStateSnapshot {
        reply: oneshot::Sender<QuorumStateSnapshot>,
    },
    /// Test-only: append a metadata batch to the log (as the leader's
    /// `submit_change` will) and drive commit through the real apply pipeline.
    /// Replies with the appended base offset.
    #[cfg(test)]
    TestAppendAndCommit {
        records: Vec<crabka_metadata::MetadataRecord>,
        reply: oneshot::Sender<i64>,
    },
    /// Stop the loop.
    Shutdown,
}

/// Which timer fired. The loop interprets the tick against current role/liveness
/// state rather than mapping 1:1 to a core event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerTick {
    /// The election timer's deadline passed.
    Election,
    /// The fetch timer's deadline passed (follower/observer poll watchdog).
    Fetch,
    /// The leader heartbeat interval ticked.
    Heartbeat,
}

/// A structured, node-local snapshot of consensus state surfaced to the handle
/// for the broker's `DescribeQuorum` admin view. (The handle-level
/// `crate::controller::QuorumState` translation lands in Task 8; this is the
/// engine's own view, free of openraft types.)
#[derive(Debug, Clone)]
pub struct QuorumStateSnapshot {
    pub leader_id: Option<NodeId>,
    pub leader_epoch: LeaderEpoch,
    pub high_watermark: i64,
    pub log_end_offset: i64,
    pub voters: Vec<NodeId>,
    /// Per-follower fetch offset, populated only on the leader.
    pub per_voter_fetch_offset: std::collections::BTreeMap<NodeId, i64>,
}

/// Outbound peer RPC sender. Encodes nothing itself — the event loop hands it
/// the already-encoded request body (see [`wire`]) and the destination peer;
/// the impl dials/sends and returns the raw response body.
///
/// Matches the `async_trait` mechanism used by
/// [`OutboundDialer`](crate::network::OutboundDialer).
#[async_trait::async_trait]
pub trait PeerSender: Send + Sync {
    /// Send `body` (a request for `api_key`) to `peer` and return the raw
    /// response body.
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

/// KIP-595 api keys used by the engine's peer sends.
pub mod api_key {
    pub const FETCH: i16 = 1;
    pub const VOTE: i16 = 52;
    pub const BEGIN_QUORUM_EPOCH: i16 = 53;
    pub const END_QUORUM_EPOCH: i16 = 54;
}

/// Node-local peer-RPC request/response body codec (Tasks 3–6). NOT the wire
/// format — Task 7 replaces these with the generated KIP-595 codecs. The shape
/// only has to round-trip between two in-process engines, so it is a flat,
/// deterministic little-endian encoding.
pub mod wire {
    use super::{Buf, BufMut, Bytes, BytesMut, LeaderEpoch, LogOffsetMetadata, NodeId};

    /// A peer RPC request body, as encoded by the sending engine and decoded by
    /// the receiving (in-memory) transport.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum PeerRequest {
        Vote {
            candidate_epoch: LeaderEpoch,
            candidate: NodeId,
            last_epoch: LeaderEpoch,
            last_offset: i64,
            pre_vote: bool,
        },
        BeginQuorumEpoch {
            leader_id: NodeId,
            leader_epoch: LeaderEpoch,
        },
        EndQuorumEpoch {
            leader_id: NodeId,
            leader_epoch: LeaderEpoch,
        },
        Fetch {
            from: NodeId,
            fetch_epoch: LeaderEpoch,
            fetch_offset: i64,
        },
    }

    /// A peer RPC response body, as encoded by the receiving engine and decoded
    /// by the sending engine back into a `Receive*Response` event.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum PeerResponse {
        Vote {
            epoch: LeaderEpoch,
            granted: bool,
            pre_vote: bool,
        },
        /// Begin/End acks carry the responder's epoch (used by the sender to
        /// fence itself if it is behind); no core event is produced for them.
        Ack { epoch: LeaderEpoch },
        Fetch {
            leader_id: NodeId,
            leader_epoch: LeaderEpoch,
            diverging: Option<LogOffsetMetadata>,
            /// Leader's high watermark at serve time. The follower advances its
            /// own HWM to `min(hwm, own log_end)` after appending the records.
            hwm: i64,
            /// Verbatim `RecordBatch`-encoded bytes for the batches at/after the
            /// follower's `fetch_offset`, up to the leader's log end (a
            /// length-prefixed run of `RecordBatch::encode` blobs). Empty when
            /// the follower is already caught up or the fetch diverged.
            records: Bytes,
        },
    }

    const TAG_VOTE: u8 = 1;
    const TAG_BEGIN: u8 = 2;
    const TAG_END: u8 = 3;
    const TAG_FETCH: u8 = 4;
    const TAG_ACK: u8 = 5;

    impl PeerRequest {
        #[must_use]
        pub fn encode(&self) -> Bytes {
            let mut b = BytesMut::new();
            match *self {
                PeerRequest::Vote {
                    candidate_epoch,
                    candidate,
                    last_epoch,
                    last_offset,
                    pre_vote,
                } => {
                    b.put_u8(TAG_VOTE);
                    b.put_u32(candidate_epoch);
                    b.put_u64(candidate);
                    b.put_u32(last_epoch);
                    b.put_i64(last_offset);
                    b.put_u8(u8::from(pre_vote));
                }
                PeerRequest::BeginQuorumEpoch {
                    leader_id,
                    leader_epoch,
                } => {
                    b.put_u8(TAG_BEGIN);
                    b.put_u64(leader_id);
                    b.put_u32(leader_epoch);
                }
                PeerRequest::EndQuorumEpoch {
                    leader_id,
                    leader_epoch,
                } => {
                    b.put_u8(TAG_END);
                    b.put_u64(leader_id);
                    b.put_u32(leader_epoch);
                }
                PeerRequest::Fetch {
                    from,
                    fetch_epoch,
                    fetch_offset,
                } => {
                    b.put_u8(TAG_FETCH);
                    b.put_u64(from);
                    b.put_u32(fetch_epoch);
                    b.put_i64(fetch_offset);
                }
            }
            b.freeze()
        }

        /// Decode a request body.
        #[must_use]
        pub fn decode(mut buf: &[u8]) -> Option<Self> {
            if buf.is_empty() {
                return None;
            }
            let tag = buf.get_u8();
            match tag {
                TAG_VOTE => {
                    let candidate_epoch = buf.get_u32();
                    let candidate = buf.get_u64();
                    let last_epoch = buf.get_u32();
                    let last_offset = buf.get_i64();
                    let pre_vote = buf.get_u8() != 0;
                    Some(PeerRequest::Vote {
                        candidate_epoch,
                        candidate,
                        last_epoch,
                        last_offset,
                        pre_vote,
                    })
                }
                TAG_BEGIN => Some(PeerRequest::BeginQuorumEpoch {
                    leader_id: buf.get_u64(),
                    leader_epoch: buf.get_u32(),
                }),
                TAG_END => Some(PeerRequest::EndQuorumEpoch {
                    leader_id: buf.get_u64(),
                    leader_epoch: buf.get_u32(),
                }),
                TAG_FETCH => Some(PeerRequest::Fetch {
                    from: buf.get_u64(),
                    fetch_epoch: buf.get_u32(),
                    fetch_offset: buf.get_i64(),
                }),
                _ => None,
            }
        }
    }

    impl PeerResponse {
        #[must_use]
        pub fn encode(&self) -> Bytes {
            let mut b = BytesMut::new();
            match self {
                PeerResponse::Vote {
                    epoch,
                    granted,
                    pre_vote,
                } => {
                    b.put_u8(TAG_VOTE);
                    b.put_u32(*epoch);
                    b.put_u8(u8::from(*granted));
                    b.put_u8(u8::from(*pre_vote));
                }
                PeerResponse::Ack { epoch } => {
                    b.put_u8(TAG_ACK);
                    b.put_u32(*epoch);
                }
                PeerResponse::Fetch {
                    leader_id,
                    leader_epoch,
                    diverging,
                    hwm,
                    records,
                } => {
                    b.put_u8(TAG_FETCH);
                    b.put_u64(*leader_id);
                    b.put_u32(*leader_epoch);
                    b.put_i64(*hwm);
                    match diverging {
                        Some(p) => {
                            b.put_u8(1);
                            b.put_i64(p.offset);
                            b.put_u32(p.epoch);
                        }
                        None => b.put_u8(0),
                    }
                    b.put_u32(u32::try_from(records.len()).unwrap_or(u32::MAX));
                    b.extend_from_slice(records);
                }
            }
            b.freeze()
        }

        /// Decode a response body.
        #[must_use]
        pub fn decode(mut buf: &[u8]) -> Option<Self> {
            if buf.is_empty() {
                return None;
            }
            let tag = buf.get_u8();
            match tag {
                TAG_VOTE => Some(PeerResponse::Vote {
                    epoch: buf.get_u32(),
                    granted: buf.get_u8() != 0,
                    pre_vote: buf.get_u8() != 0,
                }),
                TAG_ACK => Some(PeerResponse::Ack {
                    epoch: buf.get_u32(),
                }),
                TAG_FETCH => {
                    let leader_id = buf.get_u64();
                    let leader_epoch = buf.get_u32();
                    let hwm = buf.get_i64();
                    let diverging = if buf.get_u8() != 0 {
                        Some(LogOffsetMetadata {
                            offset: buf.get_i64(),
                            epoch: buf.get_u32(),
                        })
                    } else {
                        None
                    };
                    let rec_len = buf.get_u32() as usize;
                    if buf.remaining() < rec_len {
                        return None;
                    }
                    let records = Bytes::copy_from_slice(&buf[..rec_len]);
                    Some(PeerResponse::Fetch {
                        leader_id,
                        leader_epoch,
                        diverging,
                        hwm,
                        records,
                    })
                }
                _ => None,
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use assert2::assert;

        #[test]
        fn request_round_trips() {
            for req in [
                PeerRequest::Vote {
                    candidate_epoch: 3,
                    candidate: 7,
                    last_epoch: 2,
                    last_offset: 42,
                    pre_vote: true,
                },
                PeerRequest::BeginQuorumEpoch {
                    leader_id: 5,
                    leader_epoch: 9,
                },
                PeerRequest::EndQuorumEpoch {
                    leader_id: 1,
                    leader_epoch: 4,
                },
                PeerRequest::Fetch {
                    from: 2,
                    fetch_epoch: 1,
                    fetch_offset: 11,
                },
            ] {
                let enc = req.encode();
                assert!(PeerRequest::decode(&enc) == Some(req));
            }
        }

        #[test]
        fn response_round_trips() {
            for resp in [
                PeerResponse::Vote {
                    epoch: 3,
                    granted: true,
                    pre_vote: false,
                },
                PeerResponse::Ack { epoch: 8 },
                PeerResponse::Fetch {
                    leader_id: 2,
                    leader_epoch: 5,
                    diverging: Some(LogOffsetMetadata {
                        offset: 5,
                        epoch: 1,
                    }),
                    hwm: 0,
                    records: Bytes::new(),
                },
                PeerResponse::Fetch {
                    leader_id: 2,
                    leader_epoch: 5,
                    diverging: None,
                    hwm: 7,
                    records: Bytes::from_static(b"\x01\x02\x03"),
                },
            ] {
                let enc = resp.encode();
                assert!(PeerResponse::decode(&enc) == Some(resp));
            }
        }
    }
}
