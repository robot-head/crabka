//! `Controller` is the public entry point. It owns the async
//! [`KraftController`] consensus engine, the controller TCP listener, and the
//! `submit_change` leader-aware forwarding logic, behind a stable
//! [`ControllerHandle`] API the broker depends on.
//!
//! Cluster formation is driven by `BootstrapMode`: a fresh `Bootstrap`/`Join`
//! node seeds its quorum state from configured static voters or a dynamic
//! bootstrap snapshot; a restarted `Rejoin` node recovers from its on-disk
//! metadata log, checkpoint, and quorum-state file (handled inside
//! [`KraftController::open`]).
//!
//! KIP-853 voter changes are serialized by the same single-owner engine that
//! appends, commits, truncates, and snapshots the metadata log.

use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

use crabka_metadata::{MetadataImage, MetadataRecord};
use crabka_units::prelude::{ByteSize, ByteSizeExt as _};
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::info;
use uuid::Uuid;

use crate::{
    config::{BootstrapMode, ControllerConfig},
    error::RaftError,
    kraft::KraftController,
    network::{OutboundDialer, PlaintextDialer, RealPeerSender},
    server,
    types::{Node, NodeId, controller_endpoint_addr as endpoint_addr_from_endpoints},
};

/// Crabka-native view of the controller's current quorum state. Surfaced by
/// [`ControllerHandle::quorum_state`] for the broker's `DescribeQuorum` admin
/// handler so callers don't depend on engine internals directly.
#[derive(Debug, Clone)]
pub struct QuorumState {
    /// `KRaft` leader epoch (on the wire as `leader_epoch`).
    pub current_term: u64,
    /// High watermark — the last committed/applied offset on this node. `0`
    /// until the first commit.
    pub last_applied_index: u64,
    /// Current cluster leader. `None` mid-election.
    pub current_leader: Option<NodeId>,
    /// Voter ids in the current (static) membership.
    pub voters: Vec<NodeId>,
    /// Full voter node identities (directory id + endpoints + kraft.version)
    /// keyed by node id. Mirrors `voters`; carries the KIP-853 voter metadata
    /// the `DescribeQuorum` path needs.
    pub voter_nodes: BTreeMap<NodeId, Node>,
    /// Per-replica fetch offset (matched index), including observers known to
    /// the leader. Empty on a follower; callers use Kafka's unknown sentinel.
    pub per_voter_matched_index: BTreeMap<NodeId, u64>,
}

/// A contiguous byte window of the latest metadata `.checkpoint`, returned by
/// [`ControllerHandle::read_snapshot_range`] to back the broker's
/// `FetchSnapshot` handler.
#[derive(Debug, PartialEq)]
pub struct SnapshotSlice {
    pub end_offset: i64,
    pub epoch: i32,
    pub total_size: i64,
    pub bytes: bytes::Bytes,
}

/// Outcome of [`ControllerHandle::read_snapshot_range`]. The broker's
/// `FetchSnapshot` handler maps each variant to its Kafka error code:
/// `NoSnapshot` → `SNAPSHOT_NOT_FOUND`, `OutOfRange` → `POSITION_OUT_OF_RANGE`.
pub enum SnapshotRange {
    /// No `.checkpoint` exists yet.
    NoSnapshot,
    /// `position` is strictly past the snapshot's end byte. A `position`
    /// exactly at the end is valid and yields an empty `Slice`.
    OutOfRange,
    /// The requested byte window.
    Slice(SnapshotSlice),
}

/// Handle returned by [`Controller::start`]. Owns the live [`KraftController`]
/// engine and the listener task. Drop is NOT a clean shutdown — call
/// [`Self::shutdown`] (or [`Self::cancel`]) to drain the listener + stop the
/// engine before the runtime is torn down.
pub struct ControllerHandle {
    engine: KraftController,
    leader: watch::Receiver<Option<NodeId>>,
    shutdown: CancellationToken,
    listener_task: Mutex<Option<JoinHandle<()>>>,
    /// Directory holding the metadata log + KIP-630 `.checkpoint` artifacts.
    data_dir: std::path::PathBuf,
    client_id: String,
    client_dispatch_queue_capacity: crabka_client_core::ConnectionDispatchQueueCapacity,
    client_frame_max: crabka_client_core::ClientFrameMax,
    /// This node's own id, used for leader and membership checks.
    self_node_id: NodeId,
    /// Configured bootstrap voter set. Dynamic membership comes from the
    /// engine snapshot; this remains only as an address fallback during
    /// initial discovery at kraft.version 0.
    voters: crabka_metadata::VoterSet,
    /// Compatibility staging area for callers that still separate observer
    /// registration from promotion. Membership itself is changed only by an
    /// engine-owned KIP-853 command.
    staged_learners: std::sync::Mutex<BTreeMap<NodeId, Node>>,
    /// Outbound dialer; `forward_submit_to`/`fetch_metadata_from` reach a peer's
    /// controller listener with the same TLS/SASL handshake the engine's RPCs
    /// ride on.
    dialer: Arc<dyn OutboundDialer>,
    /// The address the controller listener actually bound to (resolved port when
    /// `controller_listen_addr` requested port 0).
    controller_bound_addr: SocketAddr,
}

impl ControllerHandle {
    /// Current metadata snapshot (cheap; `Arc` clone).
    #[must_use]
    pub fn current_image(&self) -> Arc<MetadataImage> {
        self.engine.current_image()
    }

    /// The address the controller listener actually bound to.
    #[must_use]
    pub fn controller_bound_addr(&self) -> SocketAddr {
        self.controller_bound_addr
    }

    /// Read up to `max_bytes` of the latest metadata snapshot starting at
    /// `position`. Reads the engine's `.checkpoint` artifacts directly (the
    /// engine writes a bare KIP-630 checkpoint, no `.meta` sidecar).
    ///
    /// `position` and `max_bytes` stay the raw KIP-595 `FetchSnapshot` `int64`
    /// and `int32`: both are byte offsets into an on-disk checkpoint that the
    /// broker's handler forwards straight off the wire, so there is no domain
    /// layer between the decode and the slice index for a quantity to occupy.
    #[must_use]
    pub fn read_snapshot_range(&self, position: i64, max_bytes: i32) -> SnapshotRange {
        let Some((id, bytes)) =
            load_latest_checkpoint(&crate::kraft::checkpoint_dir(&self.data_dir))
        else {
            return SnapshotRange::NoSnapshot;
        };
        let pos = usize::try_from(position.max(0)).unwrap_or(0);
        if pos > bytes.len() {
            return SnapshotRange::OutOfRange;
        }
        let max = usize::try_from(max_bytes.max(0)).unwrap_or(0);
        let slice = crate::snapshot::SnapshotReader::byte_range(&bytes, pos, max);
        SnapshotRange::Slice(SnapshotSlice {
            end_offset: id.0,
            epoch: id.1,
            total_size: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
            bytes: bytes::Bytes::copy_from_slice(slice),
        })
    }

    /// Subscribe to leader-id changes.
    #[must_use]
    pub fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.leader.clone()
    }

    /// Subscribe to metadata-image changes.
    #[must_use]
    pub fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.engine.watch_image()
    }

    /// Snapshot the controller's current quorum state. Used by the broker's
    /// `DescribeQuorum` (`api_key=55`, KIP-595) handler. Cheap — a `watch`
    /// borrow of the engine's published snapshot.
    #[must_use]
    pub fn quorum_state(&self) -> QuorumState {
        let snap = self.engine.quorum_snapshot();
        let voter_nodes: BTreeMap<NodeId, Node> = snap
            .voters
            .iter()
            .map(|v| {
                (
                    v.id,
                    Node {
                        directory_id: v.directory_id,
                        endpoints: v.endpoints.clone(),
                        kraft_version: v.kraft_version,
                    },
                )
            })
            .collect();
        let per_voter_matched_index: BTreeMap<NodeId, u64> = snap
            .per_replica_fetch_offset
            .iter()
            .map(|(id, off)| (*id, u64::try_from((*off).max(0)).unwrap_or(0)))
            .collect();
        QuorumState {
            current_term: u64::from(snap.leader_epoch),
            last_applied_index: u64::try_from(snap.high_watermark.max(0)).unwrap_or(0),
            current_leader: snap.leader_id,
            voters: snap.voters.ids().into_iter().collect(),
            voter_nodes,
            per_voter_matched_index,
        }
    }

    /// Directory identity voted for in the current leader epoch, if any.
    #[must_use]
    pub fn voted_directory_id(&self) -> Option<Uuid> {
        self.engine.quorum_snapshot().voted_directory_id
    }

    /// Manually trigger a metadata snapshot (KIP-630 checkpoint) on this node.
    ///
    /// # Errors
    /// Returns [`RaftError`] if serialization or the file write fails.
    pub async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        self.engine.trigger_snapshot().await
    }

    /// Read committed `__cluster_metadata` entries starting at `fetch_offset`,
    /// encoded as Kafka record batches for an observer.
    #[must_use]
    pub async fn metadata_records(
        &self,
        fetch_offset: u64,
        max_size: ByteSize,
    ) -> crate::kraft::MetadataFetchSlice {
        let off = i64::try_from(fetch_offset).unwrap_or(i64::MAX);
        self.engine.metadata_fetch(off, max_size).await.unwrap_or(
            crate::kraft::MetadataFetchSlice {
                records: bytes::Bytes::new(),
                log_start_offset: 0,
                high_watermark: 0,
            },
        )
    }

    /// Submit a batch of metadata records. Returns `Ok(())` once committed AND
    /// applied on the leader. Pre-validation lives in the engine. On a follower
    /// (`NotLeader` with a known leader), forwards directly to the leader's
    /// controller listener via `API_KEY_SUBMIT_CHANGE`.
    ///
    /// # Errors
    /// Returns an error if validation, replication, or forwarding fails.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.self_node_id.0, records = records.len())
    )]
    pub async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<crate::SubmitChangeResult, RaftError> {
        match self.engine.submit_change(records.clone()).await {
            Ok(result) => Ok(result),
            Err(RaftError::NotLeader {
                current_leader: Some(leader),
            }) => {
                if let Some(addr) = self.voter_addr(leader) {
                    self.forward_submit_to(leader, &addr, &records).await
                } else {
                    Err(RaftError::NotLeader {
                        current_leader: Some(leader),
                    })
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Reconcile a single-node voter-set delta through the KIP-853 engine.
    ///
    /// # Errors
    /// Rejects multi-node batch changes; KIP-642 is a separate operation.
    pub async fn change_membership(
        &self,
        new_voters: std::collections::BTreeSet<NodeId>,
    ) -> Result<(), RaftError> {
        let current = self.engine.quorum_snapshot().voters;
        let current_ids: std::collections::BTreeSet<NodeId> = current.ids().into_iter().collect();
        let added: Vec<NodeId> = new_voters.difference(&current_ids).copied().collect();
        let removed: Vec<NodeId> = current_ids.difference(&new_voters).copied().collect();
        if added.len() + removed.len() > 1 {
            return Err(RaftError::ReconfigRejected(
                "only one voter change may be submitted at a time".into(),
            ));
        }
        let outcome = if let Some(id) = removed.first() {
            let voter = current.get(*id).ok_or_else(|| {
                RaftError::ReconfigRejected(format!(
                    "voter {id} disappeared while preparing removal"
                ))
            })?;
            self.remove_voter(crate::reconfig::RemoveVoter {
                id: *id,
                directory_id: voter.directory_id,
            })
            .await?
        } else if let Some(id) = added.first() {
            let node = self
                .staged_learners
                .lock()
                .map_err(|_| RaftError::ReconfigRejected("staged learner lock poisoned".into()))?
                .get(id)
                .cloned()
                .ok_or_else(|| {
                    RaftError::ReconfigRejected(format!(
                        "voter {id} must be staged with add_learner first"
                    ))
                })?;
            self.add_voter(crate::reconfig::AddVoter {
                voter: crabka_metadata::Voter {
                    id: *id,
                    directory_id: node.directory_id,
                    endpoints: node.endpoints,
                    kraft_version: node.kraft_version,
                },
                ack_when_committed: true,
            })
            .await?
        } else {
            return Ok(());
        };
        match outcome {
            crate::reconfig::ReconfigOutcome::Committed => Ok(()),
            crate::reconfig::ReconfigOutcome::NotLeader { leader } => Err(RaftError::NotLeader {
                current_leader: leader,
            }),
        }
    }

    /// Stage a caught-up observer identity for later voter promotion.
    ///
    /// # Errors
    /// The observer catches up by fetching from the leader; this call does not
    /// alter quorum membership.
    pub fn add_learner(
        &self,
        node_id: NodeId,
        node: Node,
    ) -> std::future::Ready<Result<(), RaftError>> {
        let result = self
            .staged_learners
            .lock()
            .map_err(|_| RaftError::ReconfigRejected("staged learner lock poisoned".into()))
            .map(|mut learners| {
                learners.insert(node_id, node);
            });
        std::future::ready(result)
    }

    /// Add a caught-up controller voter through the Raft control log.
    ///
    /// # Errors
    /// Returns a validation, leadership, timeout, or storage error from Raft.
    pub async fn add_voter(
        &self,
        req: crate::reconfig::AddVoter,
    ) -> Result<crate::reconfig::ReconfigOutcome, RaftError> {
        self.engine
            .reconfigure(crate::reconfig::VoterChange::Add(req))
            .await
    }

    /// Remove the exact node/directory pair through the Raft control log.
    ///
    /// # Errors
    /// Returns a validation, leadership, timeout, or storage error from Raft.
    pub async fn remove_voter(
        &self,
        req: crate::reconfig::RemoveVoter,
    ) -> Result<crate::reconfig::ReconfigOutcome, RaftError> {
        self.engine
            .reconfigure(crate::reconfig::VoterChange::Remove(req))
            .await
    }

    /// Update the exact voter's endpoint and supported feature range.
    ///
    /// # Errors
    /// Returns a validation, leadership, timeout, or storage error from Raft.
    pub async fn update_voter(
        &self,
        req: crate::reconfig::UpdateVoter,
    ) -> Result<crate::reconfig::ReconfigOutcome, RaftError> {
        self.engine
            .reconfigure(crate::reconfig::VoterChange::Update(req))
            .await
    }

    /// Atomically append `KRaftVersionRecord` and the initial `VotersRecord`.
    ///
    /// # Errors
    /// Returns an unsupported-version, leadership, timeout, or storage error.
    pub async fn finalize_kraft_version(
        &self,
        version: u16,
    ) -> Result<crate::reconfig::ReconfigOutcome, RaftError> {
        self.engine.finalize_kraft_version(version).await
    }

    /// Resolve a voter's controller listener `<host>:<port>` from the static
    /// voter set's CONTROLLER endpoint. See [`controller_endpoint_addr`].
    fn voter_addr(&self, node_id: NodeId) -> Option<String> {
        let voters = self.engine.quorum_snapshot().voters;
        controller_endpoint_addr(&voters, node_id)
            .or_else(|| controller_endpoint_addr(&self.voters, node_id))
    }

    /// Open a one-shot authenticated connection to the leader's controller
    /// listener, send a wincode-encoded `Vec<MetadataRecord>` as
    /// `API_KEY_SUBMIT_CHANGE`, and translate the response into a `RaftError`.
    ///
    /// Decomposed into three killable steps: [`encode_submit_change_body`] builds
    /// the exact wire bytes, the [`SubmitChangeTransport`] seam performs the
    /// (un-mockable) dial→`raw_request`→close round trip and hands back the raw
    /// response body, and [`translate_submit_change_response`] decodes that body
    /// and maps the transport `error_code` into a `RaftError`.
    // cargo-mutants: thin wrapper; builds the live DialerSubmitTransport, needs a real dialer
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.self_node_id.0, leader = leader.0, addr, records = records.len()),
        err
    )]
    async fn forward_submit_to(
        &self,
        leader: NodeId,
        addr: &str,
        records: &[crabka_metadata::MetadataRecord],
    ) -> Result<crate::SubmitChangeResult, RaftError> {
        let transport = DialerSubmitTransport {
            dialer: self.dialer.as_ref(),
            client_id: &self.client_id,
            client_dispatch_queue_capacity: self.client_dispatch_queue_capacity,
            client_frame_max: self.client_frame_max,
        };
        forward_submit_via(&transport, leader, addr, records).await
    }

    /// Dial a controller-listener `addr` and issue one `API_KEY_METADATA_FETCH`.
    /// Used by broker-only observers to pull committed `__cluster_metadata`.
    ///
    /// # Errors
    /// - [`RaftError::Network`] if the dial or request fails.
    /// - [`RaftError::Protocol`] if the response cannot be decoded.
    pub async fn fetch_metadata_from(
        &self,
        addr: SocketAddr,
        fetch_offset: u64,
        max_size: ByteSize,
    ) -> Result<crate::wire::CrabkaMetadataFetchResponse, RaftError> {
        let req = crate::wire::CrabkaMetadataFetchRequest {
            fetch_offset: i64::try_from(fetch_offset).unwrap_or(i64::MAX),
            // `max_bytes` is the KIP-595-shaped `int32` on the Crabka observer
            // wire; the quantity converts here and nowhere deeper.
            max_bytes: max_size.bytes_i32(),
        };
        let mut body = Vec::with_capacity(12);
        req.encode_v0(&mut body);

        let opts = crabka_client_core::ConnectionOptions {
            client_id: self.client_id.clone(),
            dispatch_queue_capacity: self.client_dispatch_queue_capacity,
            frame_max: self.client_frame_max,
            ..crabka_client_core::ConnectionOptions::default()
        };
        let conn = self
            .dialer
            .dial(NodeId(1), &addr.to_string(), opts)
            .await
            .map_err(RaftError::Network)?;
        let resp_body = conn
            .raw_request(
                crate::wire::API_KEY_METADATA_FETCH,
                0,
                bytes::Bytes::from(body),
            )
            .await
            .map_err(RaftError::Network)?;
        conn.close();

        let mut cur: &[u8] = &resp_body;
        crate::wire::CrabkaMetadataFetchResponse::decode_v0(&mut cur).map_err(RaftError::Protocol)
    }

    /// Drain the listener and stop the engine. Idempotent in practice.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        self.engine.shutdown().await;
        if let Some(h) = self.listener_task.lock().await.take() {
            let _ = h.await;
        }
    }

    /// Stop the engine and cancel the controller listener without consuming
    /// `self`. Used by `BrokerHandle::shutdown` where the controller is behind
    /// an `Arc`. Idempotent. Awaits the listener task so the OS port is released
    /// before returning.
    pub async fn cancel(&self) {
        self.shutdown.cancel();
        self.engine.shutdown().await;
        if let Some(h) = self.listener_task.lock().await.take() {
            let _ = h.await;
        }
    }
}

/// Resolve a voter's CONTROLLER-listener `<host>:<port>` from the voter set,
/// preferring the endpoint named `CONTROLLER` and falling back to the first.
///
/// The host is returned VERBATIM (a DNS name), never pre-resolved to a
/// `SocketAddr`. The dialer re-resolves it per connect (`TcpStream::connect`),
/// so a peer that restarts on a new pod IP stays reachable. Parsing to a
/// `SocketAddr` here would (a) freeze a restarted peer's boot-time IP and
/// (b) fail outright on a non-literal hostname — which silently disabled
/// leader-forwarding of `submit_change` (e.g. broker self-registration), since
/// `parse()` returned `None` and the forward was skipped.
fn controller_endpoint_addr(voters: &crabka_metadata::VoterSet, node_id: NodeId) -> Option<String> {
    let voter = voters.get(node_id)?;
    endpoint_addr_from_endpoints(&voter.endpoints)
}

/// The single un-mockable step of leader-forwarding a `submit_change`: dial the
/// leader's controller listener, issue one `API_KEY_SUBMIT_CHANGE` request with
/// the already-encoded `body`, and return the raw response body bytes. The
/// concrete [`crabka_client_core::Connection`] is opaque (it cannot be built in
/// a test), so this seam returns plain `Bytes` — every serialize/translate
/// decision around it stays unit-testable against a mock.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
trait SubmitChangeTransport: Send + Sync {
    /// Round-trip the encoded `API_KEY_SUBMIT_CHANGE` `body` to the leader and
    /// return the raw response body.
    async fn send_submit_change(
        &self,
        leader: NodeId,
        addr: &str,
        body: Vec<u8>,
    ) -> Result<bytes::Bytes, crabka_client_core::ClientError>;
}

/// Live [`SubmitChangeTransport`] over the injected [`OutboundDialer`]: dials a
/// one-shot authenticated connection, sends the request at `API_KEY_SUBMIT_CHANGE`
/// version 0, closes the connection, and returns the response body. This is the
/// only part of the forward path that touches a real socket.
struct DialerSubmitTransport<'a> {
    dialer: &'a dyn OutboundDialer,
    client_id: &'a str,
    client_dispatch_queue_capacity: crabka_client_core::ConnectionDispatchQueueCapacity,
    client_frame_max: crabka_client_core::ClientFrameMax,
}

#[async_trait::async_trait]
impl SubmitChangeTransport for DialerSubmitTransport<'_> {
    // The only un-mockable step: dial + one `API_KEY_SUBMIT_CHANGE` + close, with
    // no offline signal (a `crabka_client_core::Connection` cannot be built in a
    // test). `#[mutants::skip]` rather than an `exclude_re` because cargo-mutants'
    // name-regex exclusions do not reliably match the struct-field-deletion mutant
    // this method's `ConnectionOptions { .. }` literal generates.
    #[cfg_attr(test, mutants::skip)]
    async fn send_submit_change(
        &self,
        leader: NodeId,
        addr: &str,
        body: Vec<u8>,
    ) -> Result<bytes::Bytes, crabka_client_core::ClientError> {
        let opts = crabka_client_core::ConnectionOptions {
            client_id: self.client_id.to_owned(),
            dispatch_queue_capacity: self.client_dispatch_queue_capacity,
            frame_max: self.client_frame_max,
            ..crabka_client_core::ConnectionOptions::default()
        };
        let conn = self.dialer.dial(leader, addr, opts).await?;
        let resp_body = conn
            .raw_request(
                crate::wire::API_KEY_SUBMIT_CHANGE,
                0,
                bytes::Bytes::from(body),
            )
            .await?;
        conn.close();
        Ok(resp_body)
    }
}

/// `forward_submit_to`'s testable core: serialize → send (via the injected
/// [`SubmitChangeTransport`]) → translate. The real path supplies a
/// [`DialerSubmitTransport`]; tests supply a mock so the serialize/translate
/// decisions carry mutation signal without a live quorum.
async fn forward_submit_via(
    transport: &dyn SubmitChangeTransport,
    leader: NodeId,
    addr: &str,
    records: &[crabka_metadata::MetadataRecord],
) -> Result<crate::SubmitChangeResult, RaftError> {
    let body = encode_submit_change_body(records)?;
    let resp_body = transport
        .send_submit_change(leader, addr, body)
        .await
        .map_err(RaftError::Network)?;
    translate_submit_change_response(&resp_body, leader)
}

/// Build the exact `API_KEY_SUBMIT_CHANGE` v0 request body for `records`:
/// wincode-encode the `Vec<MetadataRecord>`, then frame it with the
/// length-prefixed [`crate::wire::CrabkaSubmitChangeRequest`] codec. Kept
/// byte-for-byte identical to the inlined path so the wire stays exact.
fn encode_submit_change_body(
    records: &[crabka_metadata::MetadataRecord],
) -> Result<Vec<u8>, RaftError> {
    let body_bytes = <serde_wincode::SerdeCompat<Vec<crabka_metadata::MetadataRecord>> as wincode::Serialize>::serialize(
        &records.to_vec(),
    )
    .map_err(RaftError::from)?;
    let payload = crate::wire::CrabkaSubmitChangeRequest {
        records: bytes::Bytes::from(body_bytes),
    };
    let mut body = Vec::with_capacity(payload.records.len() + 4);
    payload.encode_v0(&mut body)?;
    Ok(body)
}

/// Decode a `CrabkaSubmitChangeResponse` from the leader's `resp_body` and map
/// its transport `error_code` into the caller's `Result`:
/// - `0` → applied (`Ok`).
/// - `2` → the leader rejected at apply-time (topic already exists). The wire
///   carries only a code; the topic name is what the caller had in hand.
/// - anything else → collapse to `NotLeader` (`CreateTopics` maps that to the
///   retryable `NOT_CONTROLLER`), preferring the response's `leader_hint` when
///   non-negative and falling back to the dialed `leader`.
fn translate_submit_change_response(
    resp_body: &[u8],
    leader: NodeId,
) -> Result<crate::SubmitChangeResult, RaftError> {
    let mut cur: &[u8] = resp_body;
    let resp = crate::wire::CrabkaSubmitChangeResponse::decode_v0(&mut cur)?;
    match resp.error_code {
        0 => <serde_wincode::SerdeCompat<crate::SubmitChangeResult> as wincode::Deserialize>::deserialize(
            &resp.result,
        )
        .map_err(RaftError::from),
        2 => Err(RaftError::Metadata(
            crabka_metadata::MetadataError::TopicExists(String::new()),
        )),
        _ => Err(RaftError::NotLeader {
            current_leader: (resp.leader_hint >= 0)
                .then(|| NodeId(u64::try_from(resp.leader_hint).unwrap_or(leader.0))),
        }),
    }
}

/// Scan `dir` for `<end_offset>-<epoch>.checkpoint` artifacts and return the
/// highest `(end_offset, epoch)` plus its raw bytes. Matches the bare-checkpoint
/// format the engine writes (no `.meta` sidecar).
fn load_latest_checkpoint(dir: &std::path::Path) -> Option<((i64, i32), Vec<u8>)> {
    let ((off, ep), path) = std::fs::read_dir(dir)
        .ok()?
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_str()?;
            let id = crate::kraft::controller::parse_checkpoint_name(name)?;
            Some((id, entry.path()))
        })
        .max_by_key(|(id, _)| *id)?;
    let bytes = std::fs::read(&path).ok()?;
    Some(((off, ep), bytes))
}

/// Zero-sized factory for [`ControllerHandle`]s.
pub struct Controller;

impl Controller {
    /// Start a controller node, open the listener, and begin participating in
    /// the quorum.
    ///
    /// `bootstrap_mode` governs cluster formation: `Bootstrap` seeds a fresh
    /// quorum from `initial_voters`; `Join`/`Rejoin` recover or wait. Mismatches
    /// between mode and on-disk log state return [`RaftError::Startup`].
    ///
    /// # Errors
    /// Returns an error if configuration, storage recovery, or startup fails.
    pub async fn start(config: ControllerConfig) -> Result<ControllerHandle, RaftError> {
        Self::start_with_listener(config, None).await
    }

    /// Like [`Self::start`], but adopts a caller-supplied, already-bound
    /// controller listener instead of binding `controller_listen_addr` itself.
    /// The supplied listener's local address MUST equal
    /// `config.controller_listen_addr`.
    ///
    /// # Errors
    /// Returns an error if the listener, storage, or controller cannot start.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = config.node_id.0, mode = ?config.bootstrap_mode),
        err
    )]
    pub async fn start_with_listener(
        config: ControllerConfig,
        prebound: Option<tokio::net::TcpListener>,
    ) -> Result<ControllerHandle, RaftError> {
        let metadata_snapshot_fetch_max =
            crabka_kraft_core::snapshot_fetch::MetadataSnapshotFetchMax::new(
                config.metadata_snapshot_fetch_max,
            )
            .map_err(RaftError::Startup)?;

        // First-boot orchestration validates mode against on-disk log state. The
        // metadata log lives directly under `log_dir` for the KraftLog engine.
        let data_dir = config.log_dir.clone();
        let log_exists = metadata_log_nonempty(&data_dir);
        let snapshot_voters = load_latest_checkpoint(&crate::kraft::checkpoint_dir(&data_dir))
            .and_then(|(_, bytes)| crate::snapshot::SnapshotReader::read(&bytes).ok())
            .and_then(|snapshot| snapshot.control_state.map(|state| state.voters))
            .unwrap_or_default();
        let voters = if config.initial_voters.is_empty() {
            snapshot_voters
        } else {
            config.initial_voters.clone()
        };
        let bootstrap_mode = if config.bootstrap_mode == BootstrapMode::Bootstrap
            && voters.is_empty()
            && config.auto_join
        {
            BootstrapMode::Join
        } else {
            config.bootstrap_mode
        };
        match (bootstrap_mode, log_exists) {
            (BootstrapMode::Bootstrap, false) => {
                if voters.is_empty() {
                    return Err(RaftError::Startup(
                        "Bootstrap mode requires a non-empty initial_voters set".into(),
                    ));
                }
            }
            (BootstrapMode::Join, false) | (BootstrapMode::Rejoin, true) => {}
            (BootstrapMode::Bootstrap, true) => {
                return Err(RaftError::Startup(
                    "Bootstrap mode requires empty raft log; existing log indicates an already-initialized broker — use Rejoin".into(),
                ));
            }
            (BootstrapMode::Rejoin, false) => {
                return Err(RaftError::Startup(
                    "Rejoin mode requires non-empty raft log; this broker has no on-disk state — use Bootstrap or Join".into(),
                ));
            }
            (BootstrapMode::Join, true) => {
                return Err(RaftError::Startup(
                    "Join mode requires empty raft log; this broker has on-disk state — use Rejoin"
                        .into(),
                ));
            }
        }

        let cluster_id = config.cluster_id.unwrap_or_else(Uuid::nil);
        let dialer: Arc<dyn OutboundDialer> = config
            .dialer
            .clone()
            .unwrap_or_else(|| Arc::new(PlaintextDialer));

        // The peer sender starts from the bootstrap view. The engine replaces
        // it immediately when it replays a dynamic voter control record.
        let peers = Arc::new(RealPeerSender::new(
            voters.clone(),
            &config.bootstrap_servers,
            config.client_id.clone(),
            Arc::clone(&dialer),
            config.client_dispatch_queue_capacity,
            config.client_frame_max,
        ));

        // Build / recover the engine. `Join` nodes with an empty log + empty
        // voter set sit unattached; `Bootstrap` seeds the static voter set.
        let engine = KraftController::open(
            data_dir.clone(),
            config.node_id,
            cluster_id,
            voters.clone(),
            config.election_timeout,
            config.heartbeat_interval,
            config.controller_fetch_miss_limit,
            config.metadata_raft_command_queue_capacity,
            config.metadata_raft_fetch_max,
            peers,
            config.snapshot_interval_records,
            metadata_snapshot_fetch_max,
        )?;

        // Controller listener.
        let listener = match prebound {
            Some(l) => l,
            None => tokio::net::TcpListener::bind(config.controller_listen_addr)
                .await
                .map_err(|e| RaftError::Storage(crabka_log::LogError::Io(e)))?,
        };
        let actual_addr = listener
            .local_addr()
            .map_err(|e| RaftError::Storage(crabka_log::LogError::Io(e)))?;
        let shutdown = CancellationToken::new();
        let leader_rx = engine.watch_leader();
        let listener_task = tokio::spawn(server::run(
            listener,
            engine.clone(),
            shutdown.clone(),
            config.handshake.clone(),
            config.shard_router.clone(),
            config.admin_router.clone(),
        ));
        info!(
            node_id = config.node_id.0,
            addr = %actual_addr,
            "controller started"
        );

        Ok(ControllerHandle {
            engine,
            leader: leader_rx,
            shutdown,
            listener_task: Mutex::new(Some(listener_task)),
            data_dir,
            client_id: config.client_id.clone(),
            client_dispatch_queue_capacity: config.client_dispatch_queue_capacity,
            client_frame_max: config.client_frame_max,
            self_node_id: config.node_id,
            voters,
            staged_learners: std::sync::Mutex::new(BTreeMap::new()),
            dialer,
            controller_bound_addr: actual_addr,
        })
    }
}

/// True when the metadata log under `dir` already holds durable raft state (a
/// previously-running node). Detects either a quorum-state file or any log
/// segment, indicating a node that has persisted state.
///
/// `dir` is the controller data dir (`<log_dir>/__cluster_metadata`). The
/// broker binary's `detect_bootstrap_mode` calls this so its Bootstrap/Rejoin
/// choice can never disagree with [`Controller::start_with_listener`]'s mode
/// validation — a node killed mid-election (segment dir created but no
/// `quorum-state` yet) reads as un-formatted and re-Bootstraps rather than
/// dying with "Rejoin requires non-empty raft log".
#[must_use]
pub fn metadata_log_nonempty(dir: &std::path::Path) -> bool {
    let qs = dir.join("quorum-state");
    if qs.exists() {
        return true;
    }
    // Any `*.log` segment indicates prior state.
    std::fs::read_dir(dir).is_ok_and(|entries| {
        entries
            .flatten()
            .any(|e| e.path().extension().is_some_and(|ext| ext == "log"))
    })
}

#[cfg(test)]
mod bootstrap_mode_tests {
    use assert2::check;
    use crabka_units::prelude::{Time, TimeExt as _, gibibytes, mebibytes, millis, secs};
    use tempfile::TempDir;

    use super::*;

    const TEST_OP_TIMEOUT: Time = secs(2);

    /// Election timeout used by tests that want a leader elected promptly.
    const FAST_ELECTION_TIMEOUT: Time = millis(200);

    /// A fetch budget large enough that no test log is truncated by it.
    const UNBOUNDED_FETCH: ByteSize = gibibytes(1);

    #[test]
    fn controller_endpoint_addr_keeps_dns_hostname_not_parsed_socketaddr() {
        // Regression: a voter endpoint host is a per-pod DNS FQDN, NOT a
        // pre-resolved IP. The resolver must return "<host>:<port>" verbatim so
        // the dialer re-resolves it per connect. Parsing to a `SocketAddr`
        // returns None for a hostname, which silently disabled leader-forwarding
        // of `submit_change` — broker self-registration then failed with "not
        // leader" and RF=3 topics could not be placed.
        let host = "demo-broker-2-0.demo-broker-headless.default.svc.cluster.local";
        let voters = crabka_metadata::VoterSet::from_voters([crabka_metadata::Voter {
            id: NodeId(2),
            directory_id: Uuid::nil(),
            endpoints: vec![crabka_metadata::VoterEndpoint {
                name: "CONTROLLER".to_string(),
                host: host.to_string(),
                port: 9093,
            }],
            kraft_version: crabka_metadata::KRaftVersionRange::default(),
        }]);
        for (_name, node_id, expected) in [
            ("registered voter", NodeId(2), Some(format!("{host}:9093"))),
            ("unknown voter", NodeId(99), None),
        ] {
            assert2::assert!(controller_endpoint_addr(&voters, node_id) == expected);
        }
    }

    #[test]
    fn controller_endpoint_addr_prefers_controller_endpoint_over_others() {
        // A voter advertises several listeners. The resolver must pick the one
        // named CONTROLLER even when it is not first in the list — submit_change
        // must be forwarded to the controller listener, not (e.g.) the
        // inter-broker REPLICATION listener on a different port. The non-
        // CONTROLLER endpoint is placed FIRST so a flipped `name == "CONTROLLER"`
        // predicate (matching the first NON-controller endpoint instead) returns
        // the wrong address.
        let voters = crabka_metadata::VoterSet::from_voters([crabka_metadata::Voter {
            id: NodeId(7),
            directory_id: Uuid::nil(),
            endpoints: vec![
                crabka_metadata::VoterEndpoint {
                    name: "REPLICATION".to_string(),
                    host: "replication-host".to_string(),
                    port: 9092,
                },
                crabka_metadata::VoterEndpoint {
                    name: "CONTROLLER".to_string(),
                    host: "controller-host".to_string(),
                    port: 9093,
                },
            ],
            kraft_version: crabka_metadata::KRaftVersionRange::default(),
        }]);
        assert2::assert!(
            controller_endpoint_addr(&voters, NodeId(7))
                == Some("controller-host:9093".to_string())
        );
    }

    fn topic_record(name: &str) -> crabka_metadata::MetadataRecord {
        crabka_metadata::MetadataRecord::V1Topic(crabka_metadata::TopicRecord {
            name: name.into(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: 1,
        })
    }

    fn committable_topic_record(name: &str) -> crabka_metadata::MetadataRecord {
        crabka_metadata::MetadataRecord::V1Topic(crabka_metadata::TopicRecord {
            name: name.into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        })
    }

    async fn wait_for_leader(ctrl: &ControllerHandle) {
        let mut leader_rx = ctrl.watch_leader();
        tokio::time::timeout(
            TEST_OP_TIMEOUT.to_std(),
            leader_rx.wait_for(Option::is_some),
        )
        .await
        .expect("no leader elected within 2s")
        .expect("leader watch channel closed");
    }

    async fn submit_change_with_timeout(
        ctrl: &ControllerHandle,
        records: Vec<crabka_metadata::MetadataRecord>,
        context: &str,
    ) -> Result<(), RaftError> {
        tokio::time::timeout(TEST_OP_TIMEOUT.to_std(), ctrl.submit_change(records))
            .await
            .unwrap_or_else(|_| panic!("{context} submit_change timed out"))
            .map(|_| ())
    }

    async fn bind_eventually(addr: SocketAddr) -> tokio::net::TcpListener {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => return listener,
                Err(err) if tokio::time::Instant::now() < deadline => {
                    let _ = err;
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(err) => panic!("listener address {addr} was not released: {err}"),
            }
        }
    }

    #[derive(Clone)]
    struct RecordingDialer {
        client_ids: Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl OutboundDialer for RecordingDialer {
        async fn dial(
            &self,
            _target: NodeId,
            addr: &str,
            options: crabka_client_core::ConnectionOptions,
        ) -> Result<crabka_client_core::Connection, crabka_client_core::ClientError> {
            self.client_ids
                .lock()
                .unwrap()
                .push(options.client_id.clone());
            let sock = tokio::net::lookup_host(addr)
                .await
                .map_err(crabka_client_core::ClientError::Io)?
                .next()
                .ok_or_else(|| {
                    crabka_client_core::ClientError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "test address resolved to no sockets",
                    ))
                })?;
            crabka_client_core::Connection::connect(sock, options).await
        }
    }

    fn submit_change_response_bytes(error_code: i16, leader_hint: i64) -> bytes::Bytes {
        let mut out = Vec::new();
        let result = <serde_wincode::SerdeCompat<crate::SubmitChangeResult> as wincode::Serialize>::serialize(
            &crate::SubmitChangeResult::default(),
        )
        .expect("serialize result");
        crate::wire::CrabkaSubmitChangeResponse {
            error_code,
            leader_hint,
            result: bytes::Bytes::from(result),
        }
        .encode_v0(&mut out)
        .unwrap();
        bytes::Bytes::from(out)
    }

    #[test]
    fn load_latest_checkpoint_picks_highest_offset_then_epoch() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("1-9.checkpoint"), b"old-offset").unwrap();
        std::fs::write(dir.path().join("2-1.checkpoint"), b"old-epoch").unwrap();
        std::fs::write(dir.path().join("2-3.checkpoint"), b"best").unwrap();
        std::fs::write(dir.path().join("9-9.txt"), b"ignored suffix").unwrap();
        std::fs::write(
            dir.path().join("not-a-checkpoint.checkpoint"),
            b"ignored name",
        )
        .unwrap();

        let latest = load_latest_checkpoint(dir.path()).expect("checkpoint");
        assert2::assert!(latest == ((2, 3), b"best".to_vec()));
    }

    #[tokio::test]
    async fn read_snapshot_range_allows_exact_end_but_rejects_past_end() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Join,
            initial_voters: crabka_metadata::VoterSet::from_voters(std::iter::empty()),
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("join start");
        let checkpoint_dir = crate::kraft::checkpoint_dir(dir.path());
        std::fs::create_dir_all(&checkpoint_dir).unwrap();
        std::fs::write(checkpoint_dir.join("10-4.checkpoint"), b"abc").unwrap();

        match ctrl.read_snapshot_range(3, 10) {
            SnapshotRange::Slice(slice) => {
                assert2::assert!(
                    slice
                        == SnapshotSlice {
                            end_offset: 10,
                            epoch: 4,
                            total_size: 3,
                            bytes: bytes::Bytes::new(),
                        }
                );
            }
            other => panic!(
                "position exactly at snapshot end should yield an empty slice, got {:?}",
                std::mem::discriminant(&other)
            ),
        }

        assert2::assert!(matches!(
            ctrl.read_snapshot_range(4, 10),
            SnapshotRange::OutOfRange
        ));
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn quorum_view_reflects_live_single_voter_state_and_submitted_records() {
        let dir = TempDir::new().unwrap();
        let mut cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
        cfg.election_timeout = FAST_ELECTION_TIMEOUT;
        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        wait_for_leader(&ctrl).await;

        let quorum = ctrl.quorum_state();
        check!(
            (
                quorum.voter_nodes.contains_key(&NodeId(1)),
                quorum.current_leader,
                quorum.current_leader == Some(NodeId(1)),
            ) == (true, Some(NodeId(1)), true)
        );

        tokio::time::timeout(
            TEST_OP_TIMEOUT.to_std(),
            ctrl.submit_change(vec![committable_topic_record("ops-a")]),
        )
        .await
        .expect("submit ops-a timed out")
        .expect("submit ops-a");
        tokio::time::timeout(
            TEST_OP_TIMEOUT.to_std(),
            ctrl.submit_change(vec![committable_topic_record("ops-b")]),
        )
        .await
        .expect("submit ops-b timed out")
        .expect("submit ops-b");

        assert2::assert!(
            (
                ctrl.current_image().topic("ops-a").is_some(),
                ctrl.current_image().topic("ops-b").is_some(),
            ) == (true, true)
        );
        let quorum = ctrl.quorum_state();
        let leader_last = quorum.last_applied_index;
        assert2::assert!(leader_last >= 2);
        assert2::assert!(quorum.per_voter_matched_index.get(&NodeId(1)) == Some(&leader_last));
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn quorum_view_reports_join_node_is_not_leader() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Join,
            initial_voters: crabka_metadata::VoterSet::from_voters(std::iter::empty()),
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("join start");

        assert2::assert!(ctrl.quorum_state().current_leader.is_none());
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_releases_bound_listener_addr() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        let addr = ctrl.controller_bound_addr();

        ctrl.shutdown().await;

        let rebound = bind_eventually(addr).await;
        drop(rebound);
    }

    #[tokio::test]
    async fn cancel_releases_bound_listener_addr_without_consuming_handle() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        let addr = ctrl.controller_bound_addr();

        ctrl.cancel().await;

        let rebound = bind_eventually(addr).await;
        drop(rebound);
        ctrl.cancel().await;
    }

    #[test]
    fn metadata_log_nonempty_detects_quorum_state_and_log_segments_only() {
        for (_case, file, expected) in [
            ("empty directory", None, false),
            (
                "quorum state file",
                Some(("quorum-state", b"state".as_slice())),
                true,
            ),
            (
                "log segment",
                Some(("00000000000000000000.log", b"log".as_slice())),
                true,
            ),
            (
                "non-log extension",
                Some(("00000000000000000000.txt", b"log".as_slice())),
                false,
            ),
        ] {
            let dir = TempDir::new().unwrap();
            if let Some((name, contents)) = file {
                std::fs::write(dir.path().join(name), contents).unwrap();
            }
            assert2::assert!(metadata_log_nonempty(dir.path()) == expected);
        }
    }

    #[test]
    fn encode_submit_change_body_frames_wincode_records_with_i32_length_prefix() {
        // The forward path must produce the exact `CrabkaSubmitChangeRequest` v0
        // wire bytes: a 4-byte big-endian length prefix followed by the
        // wincode-encoded `Vec<MetadataRecord>`. Decoding the framed body back
        // and re-deserializing must round-trip to the original records, proving
        // the prefix length matches the payload length (a mutated length or a
        // dropped wincode step fails to decode or yields different records).
        let records = vec![topic_record("alpha"), topic_record("beta")];
        let body = encode_submit_change_body(&records).expect("encode");

        let expected_wincode = <serde_wincode::SerdeCompat<
            Vec<crabka_metadata::MetadataRecord>,
        > as wincode::Serialize>::serialize(&records)
        .expect("wincode");
        assert2::assert!(body.len() == expected_wincode.len() + 4);

        let mut cur: &[u8] = &body;
        let req =
            crate::wire::CrabkaSubmitChangeRequest::decode_v0(&mut cur).expect("decode frame");
        assert2::assert!(req.records.as_ref() == expected_wincode.as_slice());
        // The framed payload IS the wincode encoding of the original records, so
        // it deserializes back to them — proving no double-framing / corruption.
        let decoded = <serde_wincode::SerdeCompat<
            Vec<crabka_metadata::MetadataRecord>,
        > as wincode::Deserialize>::deserialize(&req.records)
        .expect("wincode decode");
        assert2::assert!(decoded == records);
    }

    #[test]
    fn translate_submit_change_response_maps_each_error_code() {
        // 0 => applied.
        assert2::assert!(
            translate_submit_change_response(&submit_change_response_bytes(0, -1), NodeId(5))
                .is_ok()
        );

        // 2 => leader rejected at apply-time: a TopicExists metadata error.
        let err = translate_submit_change_response(&submit_change_response_bytes(2, -1), NodeId(5))
            .expect_err("code 2 is an error");
        assert2::assert!(matches!(
            err,
            RaftError::Metadata(crabka_metadata::MetadataError::TopicExists(_))
        ));

        // Any other code collapses to NotLeader, taking the response's
        // leader_hint when non-negative.
        let err = translate_submit_change_response(&submit_change_response_bytes(1, 9), NodeId(5))
            .expect_err("code 1 is an error");
        assert2::assert!(matches!(
            err,
            RaftError::NotLeader {
                current_leader: Some(NodeId(9))
            }
        ));

        // A negative leader_hint falls back to None (unknown), NOT to the dialed
        // leader id — distinguishing the `>= 0` guard.
        let err = translate_submit_change_response(&submit_change_response_bytes(3, -1), NodeId(5))
            .expect_err("code 3 is an error");
        assert2::assert!(matches!(
            err,
            RaftError::NotLeader {
                current_leader: None
            }
        ));
    }

    #[test]
    fn translate_submit_change_response_propagates_decode_error() {
        // A truncated body (fewer than the fixed 10 response bytes) must surface
        // as a protocol error rather than being silently treated as success.
        let err = translate_submit_change_response(&[0u8; 3], NodeId(5))
            .expect_err("truncated decodes err");
        assert2::assert!(matches!(err, RaftError::Protocol(_)));
    }

    #[tokio::test]
    async fn forward_submit_via_sends_encoded_body_and_returns_ok_on_applied() {
        // End-to-end of the testable core: the transport must receive the exact
        // framed body for `records` (the wincode + length-prefix encoding) at the
        // dialed leader/addr, and an `error_code = 0` response yields Ok.
        let records = vec![topic_record("gamma")];
        let expected_body = encode_submit_change_body(&records).expect("encode");

        let mut transport = MockSubmitChangeTransport::new();
        transport
            .expect_send_submit_change()
            .withf(move |leader, addr, body| {
                *leader == 7 && addr == "leader-host:9093" && body == &expected_body
            })
            .times(1)
            .returning(|_, _, _| Ok(submit_change_response_bytes(0, -1)));

        forward_submit_via(&transport, NodeId(7), "leader-host:9093", &records)
            .await
            .expect("applied");
    }

    #[tokio::test]
    async fn forward_submit_via_translates_not_leader_hint() {
        // The transport's response leader_hint must flow through translation to
        // the caller's NotLeader error.
        let mut transport = MockSubmitChangeTransport::new();
        transport
            .expect_send_submit_change()
            .returning(|_, _, _| Ok(submit_change_response_bytes(1, 4)));

        let err = forward_submit_via(
            &transport,
            NodeId(7),
            "leader-host:9093",
            &[topic_record("z")],
        )
        .await
        .expect_err("not leader");
        assert2::assert!(matches!(
            err,
            RaftError::NotLeader {
                current_leader: Some(NodeId(4))
            }
        ));
    }

    #[tokio::test]
    async fn forward_submit_via_maps_transport_error_to_network() {
        // A dial/send failure surfaces as RaftError::Network (so CreateTopics
        // retries), not a panic or a swallowed success.
        let mut transport = MockSubmitChangeTransport::new();
        transport.expect_send_submit_change().returning(|_, _, _| {
            Err(crabka_client_core::ClientError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "refused",
            )))
        });

        let err = forward_submit_via(
            &transport,
            NodeId(7),
            "leader-host:9093",
            &[topic_record("z")],
        )
        .await
        .expect_err("network error");
        assert2::assert!(matches!(err, RaftError::Network(_)));
    }

    #[tokio::test]
    async fn bootstrap_on_non_empty_log_errors() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("first bootstrap ok");
        // Drive a commit so the log is non-empty on the second boot.
        wait_for_leader(&ctrl).await;
        submit_change_with_timeout(
            &ctrl,
            vec![crabka_metadata::MetadataRecord::V1Topic(
                crabka_metadata::TopicRecord {
                    name: "seed".into(),
                    topic_id: Uuid::new_v4(),
                    partitions: 1,
                    replication_factor: 1,
                },
            )],
            "bootstrap seed",
        )
        .await
        .expect("submit");
        ctrl.shutdown().await;

        let cfg2 = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        match Controller::start(cfg2).await {
            Err(err) => assert2::assert!(matches!(err, RaftError::Startup(_))),
            Ok(ctrl) => {
                ctrl.shutdown().await;
                panic!("Bootstrap on existing log must error but succeeded");
            }
        }
    }

    #[tokio::test]
    async fn rejoin_on_empty_log_errors() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Rejoin,
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        match Controller::start(cfg).await {
            Err(err) => assert2::assert!(matches!(err, RaftError::Startup(_))),
            Ok(ctrl) => {
                ctrl.shutdown().await;
                panic!("Rejoin on empty log must error but succeeded");
            }
        }
    }

    #[tokio::test]
    async fn metadata_records_serves_committed_topic() {
        use crabka_metadata::{MetadataImage, MetadataRecord, TopicRecord, from_kraft_value};
        use crabka_protocol::records::RecordBatch;

        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        wait_for_leader(&ctrl).await;
        submit_change_with_timeout(
            &ctrl,
            vec![MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id: Uuid::new_v4(),
                partitions: 1,
                replication_factor: 1,
            })],
            "metadata_records seed",
        )
        .await
        .expect("submit");

        let slice = tokio::time::timeout(
            TEST_OP_TIMEOUT.to_std(),
            ctrl.metadata_records(0, UNBOUNDED_FETCH),
        )
        .await
        .expect("metadata_records timed out");
        assert2::assert!(slice.high_watermark >= 1);
        let image = MetadataImage::new(Uuid::nil());
        let mut buf: &[u8] = &slice.records;
        let mut found = false;
        while !buf.is_empty() {
            let batch = RecordBatch::decode(&mut buf).expect("decode");
            if batch.attributes.is_control_batch() {
                continue;
            }
            for r in &batch.records {
                let Some(value) = r.value.as_ref() else {
                    continue;
                };
                if let Ok(MetadataRecord::V1Topic(t)) = from_kraft_value(value, &image)
                    && t.name == "t"
                {
                    found = true;
                }
            }
        }
        assert2::assert!(found);
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn fetch_metadata_from_returns_committed_records() {
        use crabka_metadata::{MetadataImage, MetadataRecord, TopicRecord, from_kraft_value};
        use crabka_protocol::records::RecordBatch;

        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        wait_for_leader(&ctrl).await;
        submit_change_with_timeout(
            &ctrl,
            vec![MetadataRecord::V1Topic(TopicRecord {
                name: "fetched".into(),
                topic_id: Uuid::new_v4(),
                partitions: 1,
                replication_factor: 1,
            })],
            "fetch_metadata seed",
        )
        .await
        .expect("submit");

        let addr = ctrl.controller_bound_addr();
        let resp = tokio::time::timeout(
            TEST_OP_TIMEOUT.to_std(),
            ctrl.fetch_metadata_from(addr, 0, mebibytes(1)),
        )
        .await
        .expect("fetch_metadata_from timed out")
        .expect("fetch");
        assert2::assert!(resp.error_code == 0);
        assert2::assert!(resp.high_watermark >= 1);

        let image = MetadataImage::new(Uuid::nil());
        let mut buf: &[u8] = &resp.records;
        let mut found = false;
        while !buf.is_empty() {
            let batch = RecordBatch::decode(&mut buf).expect("decode");
            if batch.attributes.is_control_batch() {
                continue;
            }
            for r in &batch.records {
                let Some(value) = r.value.as_ref() else {
                    continue;
                };
                if let Ok(MetadataRecord::V1Topic(t)) = from_kraft_value(value, &image)
                    && t.name == "fetched"
                {
                    found = true;
                }
            }
        }
        assert2::assert!(found);
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn fetch_metadata_from_passes_configured_client_id_to_dialer() {
        let dir = TempDir::new().unwrap();
        let client_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
        let dialer = RecordingDialer {
            client_ids: Arc::clone(&client_ids),
        };
        let mut cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
        cfg.election_timeout = FAST_ELECTION_TIMEOUT;
        cfg.client_id = "metadata-fetch-client".into();
        cfg.dialer = Some(Arc::new(dialer));

        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        wait_for_leader(&ctrl).await;
        submit_change_with_timeout(
            &ctrl,
            vec![committable_topic_record("client-id-check")],
            "client-id fetch seed",
        )
        .await
        .expect("submit");

        let resp = tokio::time::timeout(
            TEST_OP_TIMEOUT.to_std(),
            ctrl.fetch_metadata_from(ctrl.controller_bound_addr(), 0, mebibytes(1)),
        )
        .await
        .expect("fetch_metadata_from timed out")
        .expect("fetch");

        assert2::assert!(resp.error_code == 0);
        assert2::assert!(client_ids.lock().unwrap().as_slice() == ["metadata-fetch-client"]);
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn join_on_empty_log_starts_unattached() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Join,
            initial_voters: crabka_metadata::VoterSet::from_voters(std::iter::empty()),
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg)
            .await
            .expect("Join on empty log starts ok");
        // Without voters this node never elects.
        assert2::assert!(ctrl.watch_leader().borrow().is_none());
        ctrl.shutdown().await;
    }
}
