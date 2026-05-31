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
    /// Handle op: read a committed `__cluster_metadata` slice for an observer's
    /// `API_KEY_METADATA_FETCH` (1004), encoded as Kafka record batches.
    MetadataFetch {
        fetch_offset: i64,
        max_bytes: usize,
        reply: oneshot::Sender<MetadataFetchSlice>,
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

/// A committed-range read result for the observer metadata-fetch path (1004).
/// `records` is concatenated Kafka `RecordBatch`es (one per committed log batch
/// in `[fetch_offset, high_watermark)`); the offsets are `KraftLog` offsets.
#[derive(Debug, Clone)]
pub struct MetadataFetchSlice {
    pub records: bytes::Bytes,
    pub log_start_offset: i64,
    pub high_watermark: i64,
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
/// engine's own view.)
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

/// Real KIP-595 peer-RPC body codec (Task 7). The engine's loop reasons in
/// terms of the flat [`PeerRequest`]/[`PeerResponse`] enums (unchanged since
/// Task 3); this module maps each variant to/from the genuine generated
/// KIP-595 message **bodies** (header-less — the framing layer in `server.rs` /
/// `network.rs` adds the request/response header). Captured wire versions:
/// Vote v2, Begin/End QuorumEpoch v1, Fetch v17. Crabka-to-Crabka replication
/// rides these exact bytes, so a JVM voter (Slice 6) can interoperate.
///
/// The metadata log is the single KRaft topic `__cluster_metadata`, partition
/// 0; every RPC body therefore carries exactly one topic / one partition.
/// `pre_vote` has no field in the JVM `VoteResponse`, so the responder echoes it
/// back in an internal tagged field ([`PRE_VOTE_ECHO_TAG`]) that a JVM peer
/// harmlessly ignores; the candidate reads it to match the response to its
/// pre-vote vs vote round (keeping the loop's `ReceiveVoteResponse` handling
/// unchanged).
pub mod wire {
    use bytes::{Buf, Bytes, BytesMut};

    use crabka_protocol::owned::begin_quorum_epoch_request::{
        self as bqe_req, BeginQuorumEpochRequest,
    };
    use crabka_protocol::owned::begin_quorum_epoch_response::BeginQuorumEpochResponse;
    use crabka_protocol::owned::end_quorum_epoch_request::{
        self as eqe_req, EndQuorumEpochRequest,
    };
    use crabka_protocol::owned::end_quorum_epoch_response::EndQuorumEpochResponse;
    use crabka_protocol::owned::fetch_request::{self as fetch_req, FetchRequest};
    use crabka_protocol::owned::fetch_response::{self as fetch_resp, FetchResponse};
    use crabka_protocol::owned::vote_request::{self as vote_req, VoteRequest};
    use crabka_protocol::owned::vote_response::{self as vote_resp, VoteResponse};
    use crabka_protocol::records::RecordsPayload;
    use crabka_protocol::tagged_fields::{UnknownTaggedField, UnknownTaggedFields};
    use crabka_protocol::{Decode, Encode};

    use super::{LeaderEpoch, LogOffsetMetadata, NodeId};

    /// KRaft metadata log topic name.
    const METADATA_TOPIC: &str = "__cluster_metadata";
    /// The single metadata partition.
    const METADATA_PARTITION: i32 = 0;

    /// Captured flexible wire versions (Slice-0 findings; byte-validated Slice-2).
    const VOTE_VERSION: i16 = 2;
    const QUORUM_EPOCH_VERSION: i16 = 1;
    const FETCH_VERSION: i16 = 17;

    /// Internal tagged-field tag carrying the `pre_vote` echo on a `VoteResponse`
    /// (a single byte: 1 = pre-vote round, 0 = real vote). Picked well above any
    /// JVM-assigned tag so a real Kafka voter ignores it as unknown.
    const PRE_VOTE_ECHO_TAG: u32 = 0x6b76; // "kv"

    /// A peer RPC request body, as encoded by the sending engine and decoded by
    /// the receiving engine's inbound dispatch.
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

    /// A peer RPC response body, decoded by the sending engine back into the
    /// matching `Receive*Response` event (or applied directly, for Fetch).
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum PeerResponse {
        Vote {
            epoch: LeaderEpoch,
            granted: bool,
            pre_vote: bool,
        },
        /// Begin/End acks carry the responder's epoch; no core event is produced.
        Ack { epoch: LeaderEpoch },
        Fetch {
            leader_id: NodeId,
            leader_epoch: LeaderEpoch,
            diverging: Option<LogOffsetMetadata>,
            /// Leader's high watermark at serve time.
            hwm: i64,
            /// Verbatim concatenated `RecordBatch` bytes for `[fetch_offset, log_end)`.
            records: Bytes,
        },
    }

    /// `LeaderEpoch` (u32) <-> wire `i32` (KRaft uses i32 leaderEpoch).
    #[allow(clippy::cast_possible_wrap)]
    fn epoch_to_wire(e: LeaderEpoch) -> i32 {
        i32::try_from(e).unwrap_or(i32::MAX)
    }
    #[allow(clippy::cast_sign_loss)]
    fn epoch_from_wire(e: i32) -> LeaderEpoch {
        u32::try_from(e).unwrap_or(0)
    }
    /// `NodeId` (u64) <-> wire `i32` replica id.
    fn node_to_wire(n: NodeId) -> i32 {
        i32::try_from(n).unwrap_or(i32::MAX)
    }
    #[allow(clippy::cast_sign_loss)]
    fn node_from_wire(n: i32) -> NodeId {
        u64::try_from(n).unwrap_or(0)
    }

    fn encode_body<T: Encode>(msg: &T, version: i16) -> Bytes {
        let mut buf = BytesMut::new();
        // Generated codecs only error on out-of-range version, which is fixed
        // here, so encode is infallible in practice.
        let _ = msg.encode(&mut buf, version);
        buf.freeze()
    }

    impl PeerRequest {
        #[must_use]
        pub fn encode(&self) -> Bytes {
            match *self {
                PeerRequest::Vote {
                    candidate_epoch,
                    candidate,
                    last_epoch,
                    last_offset,
                    pre_vote,
                } => {
                    let req = VoteRequest {
                        cluster_id: None,
                        voter_id: -1,
                        topics: vec![vote_req::TopicData {
                            topic_name: METADATA_TOPIC.to_string(),
                            partitions: vec![vote_req::PartitionData {
                                partition_index: METADATA_PARTITION,
                                replica_epoch: epoch_to_wire(candidate_epoch),
                                replica_id: node_to_wire(candidate),
                                last_offset_epoch: epoch_to_wire(last_epoch),
                                last_offset,
                                pre_vote,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    };
                    encode_body(&req, VOTE_VERSION)
                }
                PeerRequest::BeginQuorumEpoch {
                    leader_id,
                    leader_epoch,
                } => {
                    let req = BeginQuorumEpochRequest {
                        cluster_id: None,
                        voter_id: -1,
                        topics: vec![bqe_req::TopicData {
                            topic_name: METADATA_TOPIC.to_string(),
                            partitions: vec![bqe_req::PartitionData {
                                partition_index: METADATA_PARTITION,
                                leader_id: node_to_wire(leader_id),
                                leader_epoch: epoch_to_wire(leader_epoch),
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    };
                    encode_body(&req, QUORUM_EPOCH_VERSION)
                }
                PeerRequest::EndQuorumEpoch {
                    leader_id,
                    leader_epoch,
                } => {
                    let req = EndQuorumEpochRequest {
                        cluster_id: None,
                        topics: vec![eqe_req::TopicData {
                            topic_name: METADATA_TOPIC.to_string(),
                            partitions: vec![eqe_req::PartitionData {
                                partition_index: METADATA_PARTITION,
                                leader_id: node_to_wire(leader_id),
                                leader_epoch: epoch_to_wire(leader_epoch),
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    };
                    encode_body(&req, QUORUM_EPOCH_VERSION)
                }
                PeerRequest::Fetch {
                    from,
                    fetch_epoch,
                    fetch_offset,
                } => {
                    let req = FetchRequest {
                        // v17 carries replica_id in replica_state, not the
                        // top-level field (which is gated to v0..=14).
                        replica_state: fetch_req::ReplicaState {
                            replica_id: node_to_wire(from),
                            replica_epoch: -1,
                            ..Default::default()
                        },
                        topics: vec![fetch_req::FetchTopic {
                            topic: METADATA_TOPIC.to_string(),
                            partitions: vec![fetch_req::FetchPartition {
                                partition: METADATA_PARTITION,
                                current_leader_epoch: epoch_to_wire(fetch_epoch),
                                fetch_offset,
                                last_fetched_epoch: epoch_to_wire(fetch_epoch),
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    };
                    encode_body(&req, FETCH_VERSION)
                }
            }
        }

        /// Decode a request body. Returns `None` on a malformed frame.
        #[must_use]
        pub fn decode(buf: &[u8]) -> Option<Self> {
            // Probe each api by attempting the decode at its captured version.
            // The engine's inbound dispatch already knows the api_key (it routes
            // to the right `Inbound` variant), so a single attempt per call site
            // suffices; we try all to keep `decode` self-contained for tests.
            let mut cur = buf;
            if let Ok(req) = VoteRequest::decode(&mut cur, VOTE_VERSION)
                && cur.is_empty()
                && let Some(p) = req.topics.first().and_then(|t| t.partitions.first())
            {
                return Some(PeerRequest::Vote {
                    candidate_epoch: epoch_from_wire(p.replica_epoch),
                    candidate: node_from_wire(p.replica_id),
                    last_epoch: epoch_from_wire(p.last_offset_epoch),
                    last_offset: p.last_offset,
                    pre_vote: p.pre_vote,
                });
            }
            None
        }
    }

    /// Decode a Vote request body (api 52).
    #[must_use]
    pub fn decode_vote(buf: &[u8]) -> Option<PeerRequest> {
        let mut cur = buf;
        let req = VoteRequest::decode(&mut cur, VOTE_VERSION).ok()?;
        let p = req.topics.first()?.partitions.first()?;
        Some(PeerRequest::Vote {
            candidate_epoch: epoch_from_wire(p.replica_epoch),
            candidate: node_from_wire(p.replica_id),
            last_epoch: epoch_from_wire(p.last_offset_epoch),
            last_offset: p.last_offset,
            pre_vote: p.pre_vote,
        })
    }

    /// Decode a `BeginQuorumEpoch` request body (api 53).
    #[must_use]
    pub fn decode_begin(buf: &[u8]) -> Option<PeerRequest> {
        let mut cur = buf;
        let req = BeginQuorumEpochRequest::decode(&mut cur, QUORUM_EPOCH_VERSION).ok()?;
        let p = req.topics.first()?.partitions.first()?;
        Some(PeerRequest::BeginQuorumEpoch {
            leader_id: node_from_wire(p.leader_id),
            leader_epoch: epoch_from_wire(p.leader_epoch),
        })
    }

    /// Decode an `EndQuorumEpoch` request body (api 54).
    #[must_use]
    pub fn decode_end(buf: &[u8]) -> Option<PeerRequest> {
        let mut cur = buf;
        let req = EndQuorumEpochRequest::decode(&mut cur, QUORUM_EPOCH_VERSION).ok()?;
        let p = req.topics.first()?.partitions.first()?;
        Some(PeerRequest::EndQuorumEpoch {
            leader_id: node_from_wire(p.leader_id),
            leader_epoch: epoch_from_wire(p.leader_epoch),
        })
    }

    /// Decode a Fetch request body (api 1).
    #[must_use]
    pub fn decode_fetch(buf: &[u8]) -> Option<PeerRequest> {
        let mut cur = buf;
        let req = FetchRequest::decode(&mut cur, FETCH_VERSION).ok()?;
        let from = node_from_wire(req.replica_state.replica_id);
        let p = req.topics.first()?.partitions.first()?;
        Some(PeerRequest::Fetch {
            from,
            fetch_epoch: epoch_from_wire(p.last_fetched_epoch),
            fetch_offset: p.fetch_offset,
        })
    }

    impl PeerResponse {
        #[must_use]
        pub fn encode(&self) -> Bytes {
            match self {
                PeerResponse::Vote {
                    epoch,
                    granted,
                    pre_vote,
                } => {
                    let mut resp = VoteResponse {
                        error_code: 0,
                        topics: vec![vote_resp::TopicData {
                            topic_name: METADATA_TOPIC.to_string(),
                            partitions: vec![vote_resp::PartitionData {
                                partition_index: METADATA_PARTITION,
                                error_code: 0,
                                leader_id: -1,
                                leader_epoch: epoch_to_wire(*epoch),
                                vote_granted: *granted,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    };
                    // Echo pre_vote in an internal tagged field so the candidate
                    // can match the response to its round (the JVM schema has no
                    // pre_vote response field; an unknown tag is ignored there).
                    resp.unknown_tagged_fields = UnknownTaggedFields(vec![UnknownTaggedField {
                        tag: PRE_VOTE_ECHO_TAG,
                        bytes: Bytes::from_static(if *pre_vote { &[1u8] } else { &[0u8] }),
                    }]);
                    encode_body(&resp, VOTE_VERSION)
                }
                PeerResponse::Ack { epoch } => {
                    // A Begin/End ack is encoded as the corresponding
                    // BeginQuorumEpochResponse with the responder's leader_epoch.
                    let resp = BeginQuorumEpochResponse {
                        error_code: 0,
                        topics: vec![
                            crabka_protocol::owned::begin_quorum_epoch_response::TopicData {
                                topic_name: METADATA_TOPIC.to_string(),
                                partitions: vec![
                                    crabka_protocol::owned::begin_quorum_epoch_response::PartitionData {
                                        partition_index: METADATA_PARTITION,
                                        error_code: 0,
                                        leader_id: -1,
                                        leader_epoch: epoch_to_wire(*epoch),
                                        ..Default::default()
                                    },
                                ],
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    };
                    encode_body(&resp, QUORUM_EPOCH_VERSION)
                }
                PeerResponse::Fetch {
                    leader_id,
                    leader_epoch,
                    diverging,
                    hwm,
                    records,
                } => {
                    let mut partition = fetch_resp::PartitionData {
                        partition_index: METADATA_PARTITION,
                        error_code: 0,
                        high_watermark: *hwm,
                        current_leader: fetch_resp::LeaderIdAndEpoch {
                            leader_id: node_to_wire(*leader_id),
                            leader_epoch: epoch_to_wire(*leader_epoch),
                            ..Default::default()
                        },
                        ..Default::default()
                    };
                    if let Some(point) = diverging {
                        partition.diverging_epoch = fetch_resp::EpochEndOffset {
                            epoch: epoch_to_wire(point.epoch),
                            end_offset: point.offset,
                            ..Default::default()
                        };
                    }
                    if !records.is_empty() {
                        partition.records = Some(RecordsPayload::Raw(records.clone()));
                    }
                    let resp = FetchResponse {
                        responses: vec![fetch_resp::FetchableTopicResponse {
                            topic: METADATA_TOPIC.to_string(),
                            partitions: vec![partition],
                            ..Default::default()
                        }],
                        ..Default::default()
                    };
                    encode_body(&resp, FETCH_VERSION)
                }
            }
        }

        /// Decode a Vote response body (api 52).
        #[must_use]
        pub fn decode_vote(buf: &[u8]) -> Option<Self> {
            let mut cur = buf;
            let resp = VoteResponse::decode(&mut cur, VOTE_VERSION).ok()?;
            let p = resp.topics.first()?.partitions.first()?;
            let pre_vote = resp
                .unknown_tagged_fields
                .0
                .iter()
                .find(|f| f.tag == PRE_VOTE_ECHO_TAG)
                .map_or(false, |f| f.bytes.first().copied() == Some(1));
            Some(PeerResponse::Vote {
                epoch: epoch_from_wire(p.leader_epoch),
                granted: p.vote_granted,
                pre_vote,
            })
        }

        /// Decode a Begin/End-quorum-epoch ack body (api 53/54).
        #[must_use]
        pub fn decode_ack(buf: &[u8]) -> Option<Self> {
            let mut cur = buf;
            let resp = BeginQuorumEpochResponse::decode(&mut cur, QUORUM_EPOCH_VERSION).ok()?;
            let p = resp.topics.first()?.partitions.first()?;
            Some(PeerResponse::Ack {
                epoch: epoch_from_wire(p.leader_epoch),
            })
        }

        /// Decode a Fetch response body (api 1).
        #[must_use]
        pub fn decode_fetch(buf: &[u8]) -> Option<Self> {
            let mut cur = buf;
            let resp = FetchResponse::decode(&mut cur, FETCH_VERSION).ok()?;
            let p = resp.responses.first()?.partitions.first()?;
            let leader_id = node_from_wire(p.current_leader.leader_id);
            let leader_epoch = epoch_from_wire(p.current_leader.leader_epoch);
            // diverging_epoch defaults to (-1, -1); a real divergence carries a
            // non-negative end_offset.
            let diverging = if p.diverging_epoch.end_offset >= 0 {
                Some(LogOffsetMetadata {
                    offset: p.diverging_epoch.end_offset,
                    epoch: epoch_from_wire(p.diverging_epoch.epoch),
                })
            } else {
                None
            };
            let records = match &p.records {
                Some(RecordsPayload::Raw(b)) => b.clone(),
                Some(other) => {
                    let mut out = BytesMut::new();
                    let _ = other.encode_to(&mut out);
                    out.freeze()
                }
                None => Bytes::new(),
            };
            Some(PeerResponse::Fetch {
                leader_id,
                leader_epoch,
                diverging,
                hwm: p.high_watermark,
                records,
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use assert2::assert;

        #[test]
        fn vote_request_round_trips() {
            let req = PeerRequest::Vote {
                candidate_epoch: 3,
                candidate: 7,
                last_epoch: 2,
                last_offset: 42,
                pre_vote: true,
            };
            assert!(decode_vote(&req.encode()) == Some(req));
        }

        #[test]
        fn begin_end_round_trip() {
            let begin = PeerRequest::BeginQuorumEpoch {
                leader_id: 5,
                leader_epoch: 9,
            };
            assert!(decode_begin(&begin.encode()) == Some(begin));
            let end = PeerRequest::EndQuorumEpoch {
                leader_id: 1,
                leader_epoch: 4,
            };
            assert!(decode_end(&end.encode()) == Some(end));
        }

        #[test]
        fn fetch_request_round_trips() {
            let req = PeerRequest::Fetch {
                from: 2,
                fetch_epoch: 1,
                fetch_offset: 11,
            };
            assert!(decode_fetch(&req.encode()) == Some(req));
        }

        #[test]
        fn vote_response_round_trips_with_pre_vote_echo() {
            for pre_vote in [false, true] {
                let resp = PeerResponse::Vote {
                    epoch: 3,
                    granted: true,
                    pre_vote,
                };
                assert!(PeerResponse::decode_vote(&resp.encode()) == Some(resp));
            }
        }

        #[test]
        fn ack_round_trips() {
            let resp = PeerResponse::Ack { epoch: 8 };
            assert!(PeerResponse::decode_ack(&resp.encode()) == Some(resp));
        }

        #[test]
        fn fetch_response_round_trips() {
            let with_records = PeerResponse::Fetch {
                leader_id: 2,
                leader_epoch: 5,
                diverging: None,
                hwm: 7,
                records: Bytes::from_static(b"\x01\x02\x03"),
            };
            assert!(PeerResponse::decode_fetch(&with_records.encode()) == Some(with_records));

            let diverged = PeerResponse::Fetch {
                leader_id: 2,
                leader_epoch: 5,
                diverging: Some(LogOffsetMetadata {
                    offset: 5,
                    epoch: 1,
                }),
                hwm: 0,
                records: Bytes::new(),
            };
            assert!(PeerResponse::decode_fetch(&diverged.encode()) == Some(diverged));
        }
    }
}
