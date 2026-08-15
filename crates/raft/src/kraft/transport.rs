//! Transport seam for the [`KraftController`](crate::kraft::controller::KraftController).
//!
//! Outbound peer RPCs go through [`PeerSender`], which is real TCP in
//! production and in-memory in tests. Inbound KIP-595 RPCs arrive as
//! [`Inbound`], which carries a oneshot reply channel. Handle-facing requests
//! arrive as [`Command`].
//!
//! This module is the wire-agnostic boundary: the event loop never touches
//! sockets directly. In-memory tests and real TCP both implement `PeerSender`
//! against the same command and inbound plumbing.
//!
//! ## Peer codec
//!
//! Peer RPC request and response bodies are encoded with the generated KIP-595
//! message codecs in [`wire`]. The engine encodes a `PeerRequest` into the body
//! it hands to [`PeerSender::send`]. The receiving transport drives the peer
//! engine and returns a `PeerResponse`. The sending engine then decodes that
//! response into the matching core [`Event`], which is `ReceiveVoteResponse` or
//! `ReceiveFetchResponse`. This keeps the send path fire-and-forget: the engine
//! never `.await`s a peer RPC inline.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::sync::oneshot;

use crate::{
    error::RaftError,
    kraft::{
        event::Event,
        types::{Epoch, LogOffsetMetadata, NodeId},
    },
};

/// A decoded inbound KIP-595 RPC plus a oneshot to reply on.
///
/// The event loop decodes the body into a core [`Event`], runs it, and encodes
/// the produced response, for example `ReplyVote`, back onto `reply`.
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
    FetchSnapshot {
        req: Bytes,
        reply: oneshot::Sender<Bytes>,
    },
}

/// Everything that arrives on the engine's mpsc and drives one turn of the
/// loop.
pub enum Command {
    /// An inbound peer RPC with a oneshot to reply on.
    Inbound(Inbound),
    /// Injects a core [`Event`] directly. This is the test and driver entry
    /// point. The loop also uses it to feed peer-RPC responses back to itself
    /// as the matching `Receive*Response` event, which is the fire-and-forget
    /// feedback path.
    Event(Event),
    /// A Fetch RESPONSE the follower received from the leader. Other peer
    /// responses decode to a pure core event, but a Fetch response carries log
    /// records. The follower must truncate, append, and apply those records
    /// BEFORE it feeds the `ReceiveFetchResponse` event to the core. This
    /// response therefore gets its own command instead of the pure `Event`
    /// feedback path.
    FetchResponse {
        /// The leader that answered, which is the responder peer.
        from: NodeId,
        /// The raw encoded [`wire::PeerResponse::Fetch`] body.
        body: Bytes,
    },
    /// A `FetchSnapshot` RESPONSE the follower received from the leader. It
    /// carries snapshot bytes that the follower reassembles before it resumes.
    /// This mirrors the dedicated command path of `FetchResponse`.
    FetchSnapshotResponse { from: NodeId, body: Bytes },
    /// An election, fetch, or heartbeat timer fired. The loop maps it to the
    /// right core event after it reads the liveness state. A fetch tick
    /// re-polls instead of electing, unless the leader has been missed enough
    /// times.
    Timer(TimerTick),
    /// Handle op: append and commit a metadata batch as the leader. It replies
    /// once the batch is committed and applied, or it replies with a
    /// rejection.
    SubmitChange {
        records: Vec<crabka_metadata::MetadataRecord>,
        reply: oneshot::Sender<Result<crate::SubmitChangeResult, RaftError>>,
    },
    /// Handle op: append a KIP-853 control batch and optionally wait for it to
    /// commit under the new voter set.
    Reconfigure {
        change: crate::reconfig::VoterChange,
        reply: oneshot::Sender<Result<crate::reconfig::ReconfigOutcome, RaftError>>,
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
        max_size: crabka_units::ByteSize,
        reply: oneshot::Sender<MetadataFetchSlice>,
    },
    /// Test-only: append a metadata batch to the log, the same way the
    /// leader's `submit_change` does, and drive the commit through the real
    /// apply pipeline. Replies with the appended base offset.
    #[cfg(test)]
    TestAppendAndCommit {
        records: Vec<crabka_metadata::MetadataRecord>,
        reply: oneshot::Sender<i64>,
    },
    /// Stop the loop.
    Shutdown,
}

/// A committed-range read result for the observer metadata-fetch path (1004).
///
/// `records` is concatenated Kafka `RecordBatch`es, one for each committed log
/// batch in `[fetch_offset, high_watermark)`. The offsets are `KraftLog`
/// offsets.
#[derive(Debug, Clone)]
pub struct MetadataFetchSlice {
    pub records: bytes::Bytes,
    pub log_start_offset: i64,
    pub high_watermark: i64,
}

/// Which timer fired. The loop interprets the tick against the current role
/// and liveness state, and does not map it one-to-one to a core event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerTick {
    /// The election timer's deadline passed.
    Election,
    /// The fetch timer's deadline passed. This is the follower and observer
    /// poll watchdog.
    Fetch,
    /// The leader heartbeat interval ticked.
    Heartbeat,
}

/// A structured, node-local snapshot of consensus state for the handle, which
/// serves the broker's `DescribeQuorum` admin view.
///
/// This is the engine's own view. The handle maps it into the public
/// `crate::controller::QuorumState`.
#[derive(Debug, Clone)]
pub struct QuorumStateSnapshot {
    pub leader_id: Option<NodeId>,
    pub leader_epoch: Epoch,
    pub high_watermark: i64,
    pub log_end_offset: i64,
    /// Log-start offset. It rises past 0 once the log has been pruned below a
    /// snapshot under KIP-630.
    pub log_start_offset: i64,
    pub voters: crabka_metadata::VoterSet,
    /// Directory identity voted for in the current epoch, if any.
    pub voted_directory_id: Option<uuid::Uuid>,
    /// Replicas that have fetched from the leader but are not current voters.
    pub observers: Vec<NodeId>,
    /// Per-replica fetch offset, populated on the leader for voters and
    /// observers.
    pub per_replica_fetch_offset: std::collections::BTreeMap<NodeId, i64>,
}

/// Outbound peer RPC sender.
///
/// It encodes nothing itself. The event loop hands it the already-encoded
/// request body (see [`wire`]) and the destination peer. The impl then dials
/// the peer, sends the body, and returns the raw response body.
///
/// This matches the `async_trait` mechanism that
/// [`OutboundDialer`](crate::network::OutboundDialer) uses.
#[async_trait::async_trait]
pub trait PeerSender: Send + Sync {
    /// Sends `body`, a request for `api_key`, to `peer` and returns the raw
    /// response body.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the peer is unreachable or the RPC fails.
    async fn send(&self, peer: NodeId, api_key: i16, body: Bytes) -> Result<Bytes, RaftError>;

    /// Probe an advertised candidate endpoint through the configured outbound
    /// security dialer and verify its finalized `kraft.version` support.
    async fn probe_kraft_version(
        &self,
        _address: &str,
        _finalized_version: u16,
    ) -> Result<bool, RaftError> {
        Err(RaftError::ChangeRejected(
            "candidate probing is unavailable on this transport".into(),
        ))
    }

    /// Replace the peer endpoint table after applying a `VotersRecord`.
    fn update_voters(&self, _voters: &crabka_metadata::VoterSet) {}

    /// Transport-only bootstrap peers used by an observer with no voter view.
    fn discovery_peers(&self) -> Vec<NodeId> {
        Vec::new()
    }

    /// Associate a leader id with the endpoint used for its discovery reply.
    fn remember_peer(&self, _source: NodeId, _actual: NodeId) {}
}

/// A no-op `PeerSender` for single-voter and no-network tests. Every send fails
/// as unreachable.
///
/// A single voter never sends peer RPCs, because it wins its own election
/// immediately. This sender therefore lets the contract tests run without a
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

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[tokio::test]
    async fn null_peer_sender_reports_target_as_current_leader() {
        let err = NullPeerSender
            .send(NodeId(7), api_key::FETCH, Bytes::new())
            .await
            .expect_err("null sender should reject peer sends");

        assert2::assert!(matches!(
            err,
            RaftError::NotLeader {
                current_leader: Some(NodeId(7))
            }
        ));
    }
}

/// KIP-595 api keys used by the engine's peer sends.
pub mod api_key {
    pub const FETCH: i16 = 1;
    pub const VOTE: i16 = 52;
    pub const BEGIN_QUORUM_EPOCH: i16 = 53;
    pub const END_QUORUM_EPOCH: i16 = 54;
    pub const FETCH_SNAPSHOT: i16 = 59;
}

/// Real KIP-595 peer-RPC body codec.
///
/// The engine's loop reasons in terms of the flat `PeerRequest` and
/// `PeerResponse` enums. This module maps each variant to and from the genuine
/// generated KIP-595 message bodies. Those bodies are header-less, because the
/// framing layer in `server.rs` and `network.rs` adds the request header and
/// the response header. The captured wire versions are Vote v2,
/// `BeginQuorumEpoch` v1, `EndQuorumEpoch` v1, and Fetch v17. Crabka-to-Crabka
/// replication rides these exact bytes.
///
/// The metadata log is the single `KRaft` topic `__cluster_metadata`, partition
/// 0, so every RPC body carries exactly one topic and exactly one partition.
/// Kafka's `VoteResponse` carries no pre-vote field. A candidate matches a
/// reply to its round from its own `Prospective` or `Candidate` role, so Crabka
/// encodes a byte-faithful `VoteResponse` and the core infers the round itself
/// (KIP-996).
pub mod wire {
    use bytes::{Buf, Bytes, BytesMut};
    use crabka_protocol::{
        Decode, Encode,
        owned::{
            begin_quorum_epoch_request::{self as bqe_req, BeginQuorumEpochRequest},
            begin_quorum_epoch_response::BeginQuorumEpochResponse,
            end_quorum_epoch_request::{self as eqe_req, EndQuorumEpochRequest},
            end_quorum_epoch_response::EndQuorumEpochResponse,
            fetch_request::{self as fetch_req, FetchRequest},
            fetch_response::{self as fetch_resp, FetchResponse},
            fetch_snapshot_request::{self as fs_req, FetchSnapshotRequest},
            fetch_snapshot_response::{self as fs_resp, FetchSnapshotResponse},
            vote_request::{self as vote_req, VoteRequest},
            vote_response::{self as vote_resp, VoteResponse},
        },
        primitives::uuid::Uuid as MetaUuid,
        records::RecordsPayload,
    };

    use super::{Epoch, LogOffsetMetadata, NodeId};

    /// `KRaft` metadata log topic name.
    const METADATA_TOPIC: &str = "__cluster_metadata";
    /// The single metadata partition.
    const METADATA_PARTITION: i32 = 0;
    /// The fixed `KRaft` `__cluster_metadata` topic id (KIP-595). Fetch v13 and
    /// above key the topic by this id, not by name.
    const METADATA_TOPIC_ID: MetaUuid = MetaUuid([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);

    /// Captured flexible wire versions, byte-validated against fixture frames.
    const VOTE_VERSION: i16 = 2;
    const QUORUM_EPOCH_VERSION: i16 = 1;
    const FETCH_VERSION: i16 = 17;
    const FETCH_SNAPSHOT_VERSION: i16 = 1;
    /// Kafka `NOT_LEADER_OR_FOLLOWER`.
    pub(crate) const NOT_LEADER_OR_FOLLOWER: i16 = 6;

    fn records_payload_to_bytes(payload: &RecordsPayload) -> Bytes {
        match payload {
            RecordsPayload::Raw(bytes) => bytes.clone(),
            other => {
                let mut out = BytesMut::new();
                let _ = other.encode_to(&mut out);
                out.freeze()
            }
        }
    }

    /// A peer RPC request body, as encoded by the sending engine and decoded by
    /// the receiving engine's inbound dispatch.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum PeerRequest {
        Vote {
            /// The recipient voter this request is addressed to. This is the
            /// wire top-level `voterId`. The JVM validates that an incoming
            /// Vote is addressed to it before it considers the grant, and it
            /// silently rejects a stale `voterId` or a `voterId` of `-1`.
            /// `broadcast_vote` builds this field for each recipient.
            voter_id: NodeId,
            candidate_epoch: Epoch,
            candidate: NodeId,
            last_epoch: Epoch,
            last_offset: i64,
            pre_vote: bool,
        },
        BeginQuorumEpoch {
            leader_id: NodeId,
            leader_epoch: Epoch,
        },
        EndQuorumEpoch {
            leader_id: NodeId,
            leader_epoch: Epoch,
        },
        Fetch {
            from: NodeId,
            fetch_epoch: Epoch,
            fetch_offset: i64,
        },
        FetchSnapshot {
            from: NodeId,
            snapshot_id: (i64, i32),
            position: i64,
            max_bytes: i32,
        },
    }

    /// A peer RPC response body. The sending engine decodes it back into the
    /// matching `Receive*Response` event, or applies it directly for Fetch.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum PeerResponse {
        Vote {
            epoch: Epoch,
            granted: bool,
        },
        /// `BeginQuorumEpoch` and `EndQuorumEpoch` acks carry the responder's
        /// epoch. They produce no core event.
        Ack {
            epoch: Epoch,
        },
        Fetch {
            leader_id: NodeId,
            leader_epoch: Epoch,
            diverging: Option<LogOffsetMetadata>,
            /// When set, the follower's fetch offset is below the leader's
            /// pruned log-start, and the follower must `FetchSnapshot` this
            /// snapshot instead. The tuple is `(end_offset, epoch)`.
            snapshot_id: Option<(i64, i32)>,
            /// Leader's high watermark at serve time.
            hwm: i64,
            /// Verbatim concatenated `RecordBatch` bytes for `[fetch_offset, log_end)`.
            records: Bytes,
        },
        /// Fetch could not identify a leader. The requester keeps its fetch
        /// watchdog armed instead of treating this as a successful heartbeat.
        FetchError {
            leader_epoch: Epoch,
            error_code: i16,
        },
        FetchSnapshot {
            snapshot_id: (i64, i32),
            size: i64,
            position: i64,
            bytes: Bytes,
            error_code: i16,
        },
    }

    /// Converts between the consensus `Epoch` (u32) and the wire `i32`.
    /// `KRaft` uses an i32 `leaderEpoch`. The KIP-595 wire carries the leader
    /// epoch as a raw `int32` and stays raw here. The consensus epoch is always
    /// non-negative.
    fn epoch_to_wire(e: Epoch) -> i32 {
        i32::try_from(e).unwrap_or(i32::MAX)
    }
    fn epoch_from_wire(e: i32) -> Epoch {
        u32::try_from(e).unwrap_or(0)
    }
    /// Converts between the `NodeId` (u64) and the wire `i32` replica id.
    fn node_to_wire(n: NodeId) -> i32 {
        i32::try_from(n.0).unwrap_or(i32::MAX)
    }
    fn node_from_wire(n: i32) -> NodeId {
        NodeId(u64::try_from(n).unwrap_or(0))
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
                    voter_id,
                    candidate_epoch,
                    candidate,
                    last_epoch,
                    last_offset,
                    pre_vote,
                } => {
                    let req = VoteRequest {
                        voter_id: node_to_wire(voter_id),
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
                            topic_id: METADATA_TOPIC_ID,
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
                PeerRequest::FetchSnapshot {
                    from,
                    snapshot_id,
                    position,
                    max_bytes,
                } => encode_fetch_snapshot_request(from, snapshot_id, position, max_bytes),
            }
        }

        /// Decodes a request body. Returns `None` on a malformed frame.
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
                    voter_id: node_from_wire(req.voter_id),
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

    /// Decodes a Vote request body (api 52).
    #[must_use]
    pub fn decode_vote(buf: &[u8]) -> Option<PeerRequest> {
        let mut cur = buf;
        let req = VoteRequest::decode(&mut cur, VOTE_VERSION).ok()?;
        let p = req.topics.first()?.partitions.first()?;
        Some(PeerRequest::Vote {
            voter_id: node_from_wire(req.voter_id),
            candidate_epoch: epoch_from_wire(p.replica_epoch),
            candidate: node_from_wire(p.replica_id),
            last_epoch: epoch_from_wire(p.last_offset_epoch),
            last_offset: p.last_offset,
            pre_vote: p.pre_vote,
        })
    }

    /// Decodes a `BeginQuorumEpoch` request body (api 53).
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

    /// Decodes an `EndQuorumEpoch` request body (api 54).
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

    /// Decodes a Fetch request body (api 1).
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

    /// Encodes a `FetchSnapshot` request body (api 59).
    fn encode_fetch_snapshot_request(
        from: NodeId,
        snapshot_id: (i64, i32),
        position: i64,
        max_bytes: i32,
    ) -> Bytes {
        let (end_offset, epoch) = snapshot_id;
        let req = FetchSnapshotRequest {
            replica_id: node_to_wire(from),
            max_bytes,
            topics: vec![fs_req::TopicSnapshot {
                name: METADATA_TOPIC.to_string(),
                partitions: vec![fs_req::PartitionSnapshot {
                    partition: METADATA_PARTITION,
                    current_leader_epoch: epoch,
                    snapshot_id: fs_req::SnapshotId {
                        end_offset,
                        epoch,
                        ..Default::default()
                    },
                    position,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        encode_body(&req, FETCH_SNAPSHOT_VERSION)
    }

    /// Encodes a `FetchSnapshot` response body (api 59).
    fn encode_fetch_snapshot_response(
        snapshot_id: (i64, i32),
        size: i64,
        position: i64,
        bytes: &Bytes,
        error_code: i16,
    ) -> Bytes {
        let (end_offset, epoch) = snapshot_id;
        let resp = FetchSnapshotResponse {
            topics: vec![fs_resp::TopicSnapshot {
                name: METADATA_TOPIC.to_string(),
                partitions: vec![fs_resp::PartitionSnapshot {
                    index: METADATA_PARTITION,
                    error_code,
                    snapshot_id: fs_resp::SnapshotId {
                        end_offset,
                        epoch,
                        ..Default::default()
                    },
                    size,
                    position,
                    unaligned_records: RecordsPayload::Raw(bytes.clone()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        encode_body(&resp, FETCH_SNAPSHOT_VERSION)
    }

    /// Decodes a `FetchSnapshot` request body (api 59).
    #[must_use]
    pub fn decode_fetch_snapshot(buf: &[u8]) -> Option<PeerRequest> {
        let mut cur = buf;
        let req = FetchSnapshotRequest::decode(&mut cur, FETCH_SNAPSHOT_VERSION).ok()?;
        let p = req.topics.first()?.partitions.first()?;
        Some(PeerRequest::FetchSnapshot {
            from: node_from_wire(req.replica_id),
            snapshot_id: (p.snapshot_id.end_offset, p.snapshot_id.epoch),
            position: p.position,
            max_bytes: req.max_bytes,
        })
    }

    impl PeerResponse {
        #[must_use]
        pub fn encode(&self) -> Bytes {
            match self {
                PeerResponse::Vote { epoch, granted } => {
                    let resp = VoteResponse {
                        topics: vec![vote_resp::TopicData {
                            topic_name: METADATA_TOPIC.to_string(),
                            partitions: vec![vote_resp::PartitionData {
                                partition_index: METADATA_PARTITION,
                                leader_id: -1,
                                leader_epoch: epoch_to_wire(*epoch),
                                vote_granted: *granted,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    };
                    encode_body(&resp, VOTE_VERSION)
                }
                PeerResponse::Ack { epoch } => {
                    // A Begin/End ack is encoded as the corresponding
                    // BeginQuorumEpochResponse with the responder's leader_epoch.
                    let resp = BeginQuorumEpochResponse {
                        topics: vec![
                            crabka_protocol::owned::begin_quorum_epoch_response::TopicData {
                                topic_name: METADATA_TOPIC.to_string(),
                                partitions: vec![
                                    crabka_protocol::owned::begin_quorum_epoch_response::PartitionData {
                                        partition_index: METADATA_PARTITION,
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
                    snapshot_id,
                    hwm,
                    records,
                } => {
                    let mut partition = fetch_resp::PartitionData {
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
                    if let Some((end_offset, epoch)) = snapshot_id {
                        partition.snapshot_id = fetch_resp::SnapshotId {
                            end_offset: *end_offset,
                            epoch: *epoch,
                            ..Default::default()
                        };
                    }
                    if !records.is_empty() {
                        partition.records = Some(RecordsPayload::Raw(records.clone()));
                    }
                    let resp = FetchResponse {
                        responses: vec![fetch_resp::FetchableTopicResponse {
                            topic: METADATA_TOPIC.to_string(),
                            topic_id: METADATA_TOPIC_ID,
                            partitions: vec![partition],
                            ..Default::default()
                        }],
                        ..Default::default()
                    };
                    encode_body(&resp, FETCH_VERSION)
                }
                PeerResponse::FetchError {
                    leader_epoch,
                    error_code,
                } => {
                    let resp = FetchResponse {
                        responses: vec![fetch_resp::FetchableTopicResponse {
                            topic: METADATA_TOPIC.to_string(),
                            topic_id: METADATA_TOPIC_ID,
                            partitions: vec![fetch_resp::PartitionData {
                                partition_index: METADATA_PARTITION,
                                error_code: *error_code,
                                high_watermark: -1,
                                current_leader: fetch_resp::LeaderIdAndEpoch {
                                    leader_id: -1,
                                    leader_epoch: epoch_to_wire(*leader_epoch),
                                    ..Default::default()
                                },
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    };
                    encode_body(&resp, FETCH_VERSION)
                }
                PeerResponse::FetchSnapshot {
                    snapshot_id,
                    size,
                    position,
                    bytes,
                    error_code,
                } => encode_fetch_snapshot_response(
                    *snapshot_id,
                    *size,
                    *position,
                    bytes,
                    *error_code,
                ),
            }
        }

        /// Decodes a Vote response body (api 52). The round, pre-vote or
        /// real, is not on the wire. The engine infers it from the candidate's
        /// role.
        #[must_use]
        pub fn decode_vote(buf: &[u8]) -> Option<Self> {
            let mut cur = buf;
            let resp = VoteResponse::decode(&mut cur, VOTE_VERSION).ok()?;
            let p = resp.topics.first()?.partitions.first()?;
            Some(PeerResponse::Vote {
                epoch: epoch_from_wire(p.leader_epoch),
                granted: p.vote_granted,
            })
        }

        /// Decodes a `BeginQuorumEpoch` or `EndQuorumEpoch` ack body
        /// (api 53 and api 54).
        #[must_use]
        pub fn decode_ack(buf: &[u8]) -> Option<Self> {
            let mut cur = buf;
            let resp = BeginQuorumEpochResponse::decode(&mut cur, QUORUM_EPOCH_VERSION).ok()?;
            let p = resp.topics.first()?.partitions.first()?;
            Some(PeerResponse::Ack {
                epoch: epoch_from_wire(p.leader_epoch),
            })
        }

        /// Decodes a Fetch response body (api 1).
        #[must_use]
        pub fn decode_fetch(buf: &[u8]) -> Option<Self> {
            let mut cur = buf;
            let resp = FetchResponse::decode(&mut cur, FETCH_VERSION).ok()?;
            let p = resp.responses.first()?.partitions.first()?;
            let leader_epoch = epoch_from_wire(p.current_leader.leader_epoch);
            if p.error_code != 0 && p.current_leader.leader_id < 0 {
                return Some(PeerResponse::FetchError {
                    leader_epoch,
                    error_code: p.error_code,
                });
            }
            let leader_id = node_from_wire(p.current_leader.leader_id);
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
            let snapshot_id = if p.snapshot_id.end_offset >= 0 {
                Some((p.snapshot_id.end_offset, p.snapshot_id.epoch))
            } else {
                None
            };
            let records = p
                .records
                .as_ref()
                .map_or_else(Bytes::new, records_payload_to_bytes);
            Some(PeerResponse::Fetch {
                leader_id,
                leader_epoch,
                diverging,
                snapshot_id,
                hwm: p.high_watermark,
                records,
            })
        }

        /// Decodes a `FetchSnapshot` response body (api 59).
        #[must_use]
        pub fn decode_fetch_snapshot(buf: &[u8]) -> Option<Self> {
            let mut cur = buf;
            let resp = FetchSnapshotResponse::decode(&mut cur, FETCH_SNAPSHOT_VERSION).ok()?;
            let p = resp.topics.first()?.partitions.first()?;
            let bytes = records_payload_to_bytes(&p.unaligned_records);
            Some(PeerResponse::FetchSnapshot {
                snapshot_id: (p.snapshot_id.end_offset, p.snapshot_id.epoch),
                size: p.size,
                position: p.position,
                bytes,
                error_code: p.error_code,
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use assert2::{assert, check};

        use super::*;

        #[test]
        fn vote_request_round_trips() {
            let req = PeerRequest::Vote {
                voter_id: NodeId(9),
                candidate_epoch: 3,
                candidate: NodeId(7),
                last_epoch: 2,
                last_offset: 42,
                pre_vote: true,
            };
            assert2::assert!(decode_vote(&req.encode()) == Some(req));
        }

        #[test]
        fn generic_request_decode_accepts_vote_request() {
            let req = PeerRequest::Vote {
                voter_id: NodeId(9),
                candidate_epoch: 3,
                candidate: NodeId(7),
                last_epoch: 2,
                last_offset: 42,
                pre_vote: true,
            };
            assert2::assert!(PeerRequest::decode(&req.encode()) == Some(req));
        }

        #[test]
        fn encoded_vote_request_carries_target_voter_and_empty_cluster_id() {
            use crabka_protocol::Decode;

            let req = PeerRequest::Vote {
                voter_id: NodeId(9),
                candidate_epoch: 3,
                candidate: NodeId(7),
                last_epoch: 2,
                last_offset: 42,
                pre_vote: true,
            };
            let mut cur = &req.encode()[..];
            let raw = VoteRequest::decode(&mut cur, VOTE_VERSION).expect("decode vote request");
            let partition = &raw.topics[0].partitions[0];
            check!(
                (
                    raw.cluster_id.as_ref(),
                    raw.voter_id,
                    partition.replica_epoch,
                    partition.replica_id,
                    partition.last_offset_epoch,
                    partition.last_offset,
                    partition.pre_vote,
                ) == (None, 9, 3, 7, 2, 42, true)
            );
        }

        #[test]
        fn begin_end_round_trip() {
            let begin = PeerRequest::BeginQuorumEpoch {
                leader_id: NodeId(5),
                leader_epoch: 9,
            };
            assert2::assert!(decode_begin(&begin.encode()) == Some(begin));
            let end = PeerRequest::EndQuorumEpoch {
                leader_id: NodeId(1),
                leader_epoch: 4,
            };
            assert2::assert!(decode_end(&end.encode()) == Some(end));
        }

        #[test]
        fn encoded_begin_and_end_requests_carry_quorum_defaults_and_leader() {
            use crabka_protocol::Decode;

            let begin = PeerRequest::BeginQuorumEpoch {
                leader_id: NodeId(5),
                leader_epoch: 9,
            };
            let mut begin_cur = &begin.encode()[..];
            let raw_begin = BeginQuorumEpochRequest::decode(&mut begin_cur, QUORUM_EPOCH_VERSION)
                .expect("decode begin request");
            let begin_partition = &raw_begin.topics[0].partitions[0];
            assert2::assert!(raw_begin.cluster_id.as_ref() == None);
            assert2::assert!(raw_begin.voter_id == -1);
            assert2::assert!(begin_partition.leader_id == 5);
            assert2::assert!(begin_partition.leader_epoch == 9);

            let end = PeerRequest::EndQuorumEpoch {
                leader_id: NodeId(1),
                leader_epoch: 4,
            };
            let mut end_cur = &end.encode()[..];
            let raw_end = EndQuorumEpochRequest::decode(&mut end_cur, QUORUM_EPOCH_VERSION)
                .expect("decode end request");
            let end_partition = &raw_end.topics[0].partitions[0];
            assert2::assert!(raw_end.cluster_id.as_ref() == None);
            assert2::assert!(end_partition.leader_id == 1);
            assert2::assert!(end_partition.leader_epoch == 4);
        }

        #[test]
        fn fetch_request_round_trips() {
            let req = PeerRequest::Fetch {
                from: NodeId(2),
                fetch_epoch: 1,
                fetch_offset: 11,
            };
            assert2::assert!(decode_fetch(&req.encode()) == Some(req));
        }

        #[test]
        fn encoded_fetch_request_carries_replica_state_epoch_sentinel() {
            use crabka_protocol::{Decode, owned::fetch_request::FetchRequest};

            let req = PeerRequest::Fetch {
                from: NodeId(2),
                fetch_epoch: 1,
                fetch_offset: 11,
            };
            let mut cur = &req.encode()[..];
            let raw = FetchRequest::decode(&mut cur, FETCH_VERSION).expect("decode fetch request");
            let partition = &raw.topics[0].partitions[0];
            check!(
                (
                    raw.replica_state.replica_id,
                    raw.replica_state.replica_epoch,
                    partition.current_leader_epoch,
                    partition.last_fetched_epoch,
                    partition.fetch_offset,
                ) == (2, -1, 1, 1, 11)
            );
        }

        #[test]
        fn vote_response_round_trips() {
            let resp = PeerResponse::Vote {
                epoch: 3,
                granted: true,
            };
            assert2::assert!(PeerResponse::decode_vote(&resp.encode()) == Some(resp));
        }

        #[test]
        fn encoded_vote_response_carries_success_error_codes() {
            use crabka_protocol::Decode;

            let resp = PeerResponse::Vote {
                epoch: 3,
                granted: true,
            };
            let mut cur = &resp.encode()[..];
            let raw = VoteResponse::decode(&mut cur, VOTE_VERSION).expect("decode vote response");
            let partition = &raw.topics[0].partitions[0];
            check!(
                (
                    raw.error_code,
                    partition.partition_index,
                    partition.error_code,
                    partition.leader_epoch,
                    partition.vote_granted,
                ) == (0, METADATA_PARTITION, 0, 3, true)
            );
        }

        #[test]
        fn decodes_jvm_style_response_without_echo_tag() {
            // A real JVM `VoteResponse` is byte-faithful Kafka v2 with no Crabka
            // echo tag. Build one straight from the generated protocol type
            // (bypassing `PeerResponse::Vote::encode`) and confirm `decode_vote`
            // tolerates it — the regression guard for the removed
            // `PRE_VOTE_ECHO_TAG`.
            let resp = VoteResponse {
                error_code: 0,
                topics: vec![vote_resp::TopicData {
                    topic_name: METADATA_TOPIC.to_string(),
                    partitions: vec![vote_resp::PartitionData {
                        partition_index: METADATA_PARTITION,
                        error_code: 0,
                        leader_id: -1,
                        leader_epoch: epoch_to_wire(7),
                        vote_granted: true,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
            let bytes = encode_body(&resp, VOTE_VERSION);
            let decoded = PeerResponse::decode_vote(&bytes).unwrap();
            assert2::assert!(
                decoded
                    == PeerResponse::Vote {
                        epoch: 7,
                        granted: true
                    }
            );
        }

        #[test]
        fn ack_round_trips() {
            let resp = PeerResponse::Ack { epoch: 8 };
            assert2::assert!(PeerResponse::decode_ack(&resp.encode()) == Some(resp));
        }

        #[test]
        fn encoded_ack_response_carries_success_error_codes() {
            use crabka_protocol::Decode;

            let resp = PeerResponse::Ack { epoch: 8 };
            let mut cur = &resp.encode()[..];
            let raw = BeginQuorumEpochResponse::decode(&mut cur, QUORUM_EPOCH_VERSION)
                .expect("decode ack");
            let partition = &raw.topics[0].partitions[0];
            check!(
                (
                    raw.error_code,
                    partition.partition_index,
                    partition.error_code,
                    partition.leader_id,
                    partition.leader_epoch,
                ) == (0, METADATA_PARTITION, 0, -1, 8)
            );
        }

        #[test]
        fn fetch_response_carries_snapshot_id() {
            let resp = PeerResponse::Fetch {
                leader_id: NodeId(1),
                leader_epoch: 4,
                diverging: None,
                snapshot_id: Some((42, 3)),
                hwm: 0,
                records: Bytes::new(),
            };
            assert2::assert!(PeerResponse::decode_fetch(&resp.encode()) == Some(resp));
        }

        #[test]
        fn fetch_snapshot_request_round_trips() {
            let req = PeerRequest::FetchSnapshot {
                from: NodeId(2),
                snapshot_id: (42, 3),
                position: 128,
                max_bytes: 4096,
            };
            assert2::assert!(decode_fetch_snapshot(&req.encode()) == Some(req));
        }

        #[test]
        fn encoded_fetch_snapshot_request_carries_empty_cluster_id() {
            use crabka_protocol::Decode;

            let req = PeerRequest::FetchSnapshot {
                from: NodeId(2),
                snapshot_id: (42, 3),
                position: 128,
                max_bytes: 4096,
            };
            let mut cur = &req.encode()[..];
            let raw = FetchSnapshotRequest::decode(&mut cur, FETCH_SNAPSHOT_VERSION)
                .expect("decode fetch snapshot request");
            let partition = &raw.topics[0].partitions[0];
            check!(
                (
                    raw.cluster_id.as_ref(),
                    raw.replica_id,
                    raw.max_bytes,
                    partition.current_leader_epoch,
                    partition.snapshot_id.end_offset,
                    partition.snapshot_id.epoch,
                    partition.position,
                ) == (None, 2, 4096, 3, 42, 3, 128)
            );
        }

        #[test]
        fn fetch_snapshot_response_round_trips() {
            let resp = PeerResponse::FetchSnapshot {
                snapshot_id: (42, 3),
                size: 9,
                position: 0,
                bytes: Bytes::from_static(b"snapshotX"),
                error_code: 0,
            };
            assert2::assert!(PeerResponse::decode_fetch_snapshot(&resp.encode()) == Some(resp));
        }

        #[test]
        fn fetch_response_round_trips() {
            let with_records = PeerResponse::Fetch {
                leader_id: NodeId(2),
                leader_epoch: 5,
                diverging: None,
                snapshot_id: None,
                hwm: 7,
                records: Bytes::from_static(b"\x01\x02\x03"),
            };
            assert2::assert!(
                PeerResponse::decode_fetch(&with_records.encode()) == Some(with_records)
            );

            let diverged = PeerResponse::Fetch {
                leader_id: NodeId(2),
                leader_epoch: 5,
                diverging: Some(LogOffsetMetadata {
                    offset: 5,
                    epoch: 1,
                }),
                snapshot_id: None,
                hwm: 0,
                records: Bytes::new(),
            };
            assert2::assert!(PeerResponse::decode_fetch(&diverged.encode()) == Some(diverged));
        }

        #[test]
        fn fetch_error_round_trips_with_unknown_leader() {
            use crabka_protocol::{Decode, owned::fetch_response::FetchResponse};

            let resp = PeerResponse::FetchError {
                leader_epoch: 5,
                error_code: NOT_LEADER_OR_FOLLOWER,
            };
            let encoded = resp.encode();
            assert2::assert!(PeerResponse::decode_fetch(&encoded) == Some(resp));

            let mut cur = &encoded[..];
            let raw = FetchResponse::decode(&mut cur, FETCH_VERSION).expect("decode Fetch error");
            let partition = &raw.responses[0].partitions[0];
            check!(
                (
                    raw.error_code,
                    partition.error_code,
                    partition.high_watermark,
                    partition.current_leader.leader_id,
                    partition.current_leader.leader_epoch,
                ) == (0, NOT_LEADER_OR_FOLLOWER, -1, -1, 5)
            );
        }

        #[test]
        fn fetch_error_with_zero_leader_preserves_redirect() {
            use crabka_protocol::{Decode, Encode, owned::fetch_response::FetchResponse};

            let success = PeerResponse::Fetch {
                leader_id: NodeId(0),
                leader_epoch: 5,
                diverging: None,
                snapshot_id: None,
                hwm: -1,
                records: Bytes::new(),
            }
            .encode();
            let mut cur = &success[..];
            let mut raw = FetchResponse::decode(&mut cur, FETCH_VERSION).expect("decode Fetch");
            raw.responses[0].partitions[0].error_code = NOT_LEADER_OR_FOLLOWER;
            let mut encoded = BytesMut::new();
            raw.encode(&mut encoded, FETCH_VERSION)
                .expect("encode Fetch error");

            assert2::assert!(matches!(
                PeerResponse::decode_fetch(&encoded),
                Some(PeerResponse::Fetch {
                    leader_id: NodeId(0),
                    leader_epoch: 5,
                    ..
                })
            ));
        }

        #[test]
        fn encoded_fetch_response_carries_partition_success_fields() {
            use crabka_protocol::{Decode, owned::fetch_response::FetchResponse};

            let resp = PeerResponse::Fetch {
                leader_id: NodeId(2),
                leader_epoch: 5,
                diverging: None,
                snapshot_id: None,
                hwm: 7,
                records: Bytes::new(),
            };
            let mut cur = &resp.encode()[..];
            let raw =
                FetchResponse::decode(&mut cur, FETCH_VERSION).expect("decode fetch response");
            let partition = &raw.responses[0].partitions[0];
            check!(
                (
                    partition.partition_index,
                    partition.error_code,
                    partition.high_watermark,
                    partition.current_leader.leader_id,
                    partition.current_leader.leader_epoch,
                ) == (METADATA_PARTITION, 0, 7, 2, 5)
            );
        }

        #[test]
        fn fetch_wire_carries_metadata_topic_id() {
            use crabka_protocol::{
                Decode,
                owned::{fetch_request::FetchRequest, fetch_response::FetchResponse},
            };
            let req = PeerRequest::Fetch {
                from: NodeId(2),
                fetch_epoch: 1,
                fetch_offset: 5,
            };
            let mut c = &req.encode()[..];
            let dreq = FetchRequest::decode(&mut c, FETCH_VERSION).unwrap();
            assert2::assert!(dreq.topics[0].topic_id == METADATA_TOPIC_ID);

            let resp = PeerResponse::Fetch {
                leader_id: NodeId(1),
                leader_epoch: 4,
                diverging: None,
                snapshot_id: None,
                hwm: 0,
                records: Bytes::new(),
            };
            let mut c2 = &resp.encode()[..];
            let dresp = FetchResponse::decode(&mut c2, FETCH_VERSION).unwrap();
            assert2::assert!(dresp.responses[0].topic_id == METADATA_TOPIC_ID);
        }
    }
}
