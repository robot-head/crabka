//! `Controller` is the public entry point. It owns the async
//! [`KraftController`] consensus engine, the controller TCP listener, and the
//! `submit_change` leader-aware forwarding logic, behind a stable
//! [`ControllerHandle`] API the broker depends on.
//!
//! Cluster formation is driven by `BootstrapMode`: a fresh `Bootstrap`/`Join`
//! node seeds its quorum state from `initial_voters`; a restarted `Rejoin` node
//! recovers from its on-disk metadata log + checkpoint + quorum-state file
//! (handled inside [`KraftController::open`]).
//!
//! Static voters only this slice — `add_voter`/`remove_voter`/`update_voter`/
//! `change_membership`/`add_learner` return [`RaftError::Unsupported`]
//! ("dynamic reconfig: Slice 5").

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;
use uuid::Uuid;

use crabka_metadata::{MetadataImage, MetadataRecord};

use crate::config::{BootstrapMode, ControllerConfig};
use crate::error::RaftError;
use crate::kraft::KraftController;
use crate::network::{OutboundDialer, PlaintextDialer, RealPeerSender};
use crate::server;
use crate::types::{Node, NodeId};

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
    /// Per-voter fetch offset (matched index), populated ONLY on the leader
    /// (the engine only tracks per-follower progress while leading). Empty on a
    /// follower; callers fall back to the JVM `-1` ("Unknown") sentinel.
    pub per_voter_matched_index: BTreeMap<NodeId, u64>,
}

/// A contiguous byte window of the latest metadata `.checkpoint`, returned by
/// [`ControllerHandle::read_snapshot_range`] to back the broker's
/// `FetchSnapshot` handler.
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
    /// This node's own id. Used by [`ReconfigOps::is_leader`].
    self_node_id: NodeId,
    /// Static voter set (this slice; KIP-853 dynamic membership is Slice 5).
    /// Used to resolve voter endpoints for `quorum_state().voter_nodes` and
    /// `forward_submit_to`.
    voters: crabka_metadata::VoterSet,
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
    /// borrow of the engine's published snapshot plus a clone of the static
    /// voter metadata.
    #[must_use]
    pub fn quorum_state(&self) -> QuorumState {
        let snap = self.engine.quorum_snapshot();
        let voter_nodes: BTreeMap<NodeId, Node> = self
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
            .per_voter_fetch_offset
            .iter()
            .map(|(id, off)| (*id, u64::try_from((*off).max(0)).unwrap_or(0)))
            .collect();
        QuorumState {
            current_term: u64::from(snap.leader_epoch),
            last_applied_index: u64::try_from(snap.high_watermark.max(0)).unwrap_or(0),
            current_leader: snap.leader_id,
            voters: snap.voters,
            voter_nodes,
            per_voter_matched_index,
        }
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
        max_bytes: usize,
    ) -> crate::kraft::MetadataFetchSlice {
        let off = i64::try_from(fetch_offset).unwrap_or(i64::MAX);
        self.engine.metadata_fetch(off, max_bytes).await.unwrap_or(
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
    pub async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), RaftError> {
        match self.engine.submit_change(records.clone()).await {
            Ok(()) => Ok(()),
            Err(RaftError::NotLeader {
                current_leader: Some(leader),
            }) => {
                if let Some(addr) = self.voter_addr(leader) {
                    self.forward_submit_to(leader, addr, &records).await
                } else {
                    Err(RaftError::NotLeader {
                        current_leader: Some(leader),
                    })
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Reconfiguration is static this slice. (KIP-853 dynamic voters: Slice 5.)
    ///
    /// # Errors
    /// Always [`RaftError::Unsupported`].
    #[allow(clippy::unused_async)] // async to match the stable handle API
    pub async fn change_membership(
        &self,
        _new_voters: std::collections::BTreeSet<NodeId>,
    ) -> Result<(), RaftError> {
        Err(RaftError::Unsupported("dynamic reconfig: Slice 5"))
    }

    /// Reconfiguration is static this slice. (KIP-853 dynamic voters: Slice 5.)
    ///
    /// # Errors
    /// Always [`RaftError::Unsupported`].
    #[allow(clippy::unused_async)] // async to match the stable handle API
    pub async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
        Err(RaftError::Unsupported("dynamic reconfig: Slice 5"))
    }

    /// Reconfiguration is static this slice. (KIP-853 dynamic voters: Slice 5.)
    ///
    /// # Errors
    /// Always [`RaftError::Unsupported`].
    #[allow(clippy::unused_async)] // async to match the stable handle API
    pub async fn add_voter(
        &self,
        _req: crate::reconfig::AddVoter,
    ) -> Result<crate::reconfig::ReconfigOutcome, RaftError> {
        Err(RaftError::Unsupported("dynamic reconfig: Slice 5"))
    }

    /// Reconfiguration is static this slice. (KIP-853 dynamic voters: Slice 5.)
    ///
    /// # Errors
    /// Always [`RaftError::Unsupported`].
    #[allow(clippy::unused_async)] // async to match the stable handle API
    pub async fn remove_voter(
        &self,
        _req: crate::reconfig::RemoveVoter,
    ) -> Result<crate::reconfig::ReconfigOutcome, RaftError> {
        Err(RaftError::Unsupported("dynamic reconfig: Slice 5"))
    }

    /// Reconfiguration is static this slice. (KIP-853 dynamic voters: Slice 5.)
    ///
    /// # Errors
    /// Always [`RaftError::Unsupported`].
    #[allow(clippy::unused_async)] // async to match the stable handle API
    pub async fn update_voter(
        &self,
        _req: crate::reconfig::UpdateVoter,
    ) -> Result<crate::reconfig::ReconfigOutcome, RaftError> {
        Err(RaftError::Unsupported("dynamic reconfig: Slice 5"))
    }

    /// Resolve a voter's controller listener address from the static voter set's
    /// CONTROLLER endpoint.
    fn voter_addr(&self, node_id: NodeId) -> Option<SocketAddr> {
        let voter = self.voters.get(node_id)?;
        let endpoint = voter
            .endpoints
            .iter()
            .find(|e| e.name == "CONTROLLER")
            .or_else(|| voter.endpoints.first())?;
        format!("{}:{}", endpoint.host, endpoint.port).parse().ok()
    }

    /// Open a one-shot authenticated connection to the leader's controller
    /// listener, send a wincode-encoded `Vec<MetadataRecord>` as
    /// `API_KEY_SUBMIT_CHANGE`, and translate the response into a `RaftError`.
    async fn forward_submit_to(
        &self,
        leader: NodeId,
        addr: SocketAddr,
        records: &[crabka_metadata::MetadataRecord],
    ) -> Result<(), RaftError> {
        let body_bytes = <serde_wincode::SerdeCompat<Vec<crabka_metadata::MetadataRecord>> as wincode::Serialize>::serialize(
            &records.to_vec(),
        )
        .map_err(crate::error::RaftError::from)?;
        let payload = crate::wire::CrabkaSubmitChangeRequest {
            records: bytes::Bytes::from(body_bytes),
        };
        let mut body = Vec::with_capacity(payload.records.len() + 4);
        payload.encode_v0(&mut body)?;

        let opts = crabka_client_core::ConnectionOptions {
            client_id: self.client_id.clone(),
            ..crabka_client_core::ConnectionOptions::default()
        };
        let conn = self
            .dialer
            .dial(leader, &addr.to_string(), opts)
            .await
            .map_err(RaftError::Network)?;

        let resp_body = conn
            .raw_request(
                crate::wire::API_KEY_SUBMIT_CHANGE,
                0,
                bytes::Bytes::from(body),
            )
            .await
            .map_err(RaftError::Network)?;
        conn.close();

        let mut cur: &[u8] = &resp_body;
        let resp = crate::wire::CrabkaSubmitChangeResponse::decode_v0(&mut cur)?;
        match resp.error_code {
            0 => Ok(()),
            // The leader rejected at apply-time. The wire carries only an error
            // code; the topic name is what the caller had in hand.
            2 => Err(RaftError::Metadata(
                crabka_metadata::MetadataError::TopicExists(String::new()),
            )),
            // not-leader / other collapse to NotLeader — CreateTopics maps that
            // to NOT_CONTROLLER (retryable).
            _ => Err(RaftError::NotLeader {
                current_leader: (resp.leader_hint >= 0)
                    .then(|| u64::try_from(resp.leader_hint).unwrap_or(leader)),
            }),
        }
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
        max_bytes: u32,
    ) -> Result<crate::wire::CrabkaMetadataFetchResponse, RaftError> {
        let req = crate::wire::CrabkaMetadataFetchRequest {
            fetch_offset: i64::try_from(fetch_offset).unwrap_or(i64::MAX),
            max_bytes: i32::try_from(max_bytes).unwrap_or(i32::MAX),
        };
        let mut body = Vec::with_capacity(12);
        req.encode_v0(&mut body);

        let opts = crabka_client_core::ConnectionOptions {
            client_id: self.client_id.clone(),
            ..crabka_client_core::ConnectionOptions::default()
        };
        let conn = self
            .dialer
            .dial(1, &addr.to_string(), opts)
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

#[async_trait::async_trait]
impl crate::reconfig::ReconfigOps for ControllerHandle {
    fn current_voters(&self) -> crabka_metadata::VoterSet {
        let voters = self
            .quorum_state()
            .voter_nodes
            .into_iter()
            .map(|(id, node)| crabka_metadata::Voter {
                id,
                directory_id: node.directory_id,
                endpoints: node.endpoints,
                kraft_version: node.kraft_version,
            });
        crabka_metadata::VoterSet::from_voters(voters)
    }

    fn leader(&self) -> Option<NodeId> {
        self.quorum_state().current_leader
    }

    fn is_leader(&self) -> bool {
        self.quorum_state().current_leader == Some(self.self_node_id)
    }

    fn leader_last_index(&self) -> u64 {
        self.quorum_state().last_applied_index
    }

    fn observer_index(&self, id: NodeId) -> Option<u64> {
        self.quorum_state()
            .per_voter_matched_index
            .get(&id)
            .copied()
    }

    async fn add_learner(&self, id: NodeId, node: crate::Node) -> Result<(), RaftError> {
        ControllerHandle::add_learner(self, id, node).await
    }

    async fn change_membership(
        &self,
        ids: std::collections::BTreeSet<NodeId>,
    ) -> Result<(), RaftError> {
        ControllerHandle::change_membership(self, ids).await
    }

    async fn submit_records(
        &self,
        records: Vec<crabka_metadata::MetadataRecord>,
    ) -> Result<(), RaftError> {
        ControllerHandle::submit_change(self, records).await
    }
}

/// Scan `dir` for `<end_offset>-<epoch>.checkpoint` artifacts and return the
/// highest `(end_offset, epoch)` plus its raw bytes. Matches the bare-checkpoint
/// format the engine writes (no `.meta` sidecar).
fn load_latest_checkpoint(dir: &std::path::Path) -> Option<((i64, i32), Vec<u8>)> {
    let entries = std::fs::read_dir(dir).ok()?;
    let mut best: Option<((i64, i32), std::path::PathBuf)> = None;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(".checkpoint") else {
            continue;
        };
        let Some((off, ep)) = stem.split_once('-') else {
            continue;
        };
        let (Ok(off), Ok(ep)) = (off.parse::<i64>(), ep.parse::<i32>()) else {
            continue;
        };
        if best.as_ref().is_none_or(|(cur, _)| (off, ep) > *cur) {
            best = Some(((off, ep), entry.path()));
        }
    }
    let ((off, ep), path) = best?;
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
    pub async fn start(config: ControllerConfig) -> Result<ControllerHandle, RaftError> {
        Self::start_with_listener(config, None).await
    }

    /// Like [`Self::start`], but adopts a caller-supplied, already-bound
    /// controller listener instead of binding `controller_listen_addr` itself.
    /// The supplied listener's local address MUST equal
    /// `config.controller_listen_addr`.
    #[allow(clippy::too_many_lines)]
    pub async fn start_with_listener(
        config: ControllerConfig,
        prebound: Option<tokio::net::TcpListener>,
    ) -> Result<ControllerHandle, RaftError> {
        // The static voter set for this node. Bootstrap/Join nodes carry it in
        // `initial_voters`; a Rejoin node recovers the quorum-state file but the
        // voter set is reconstructed from config (static voters this slice).
        let voters = config.initial_voters.clone();

        // First-boot orchestration validates mode against on-disk log state. The
        // metadata log lives directly under `log_dir` for the KraftLog engine.
        let data_dir = config.log_dir.clone();
        let log_exists = metadata_log_nonempty(&data_dir);
        match (config.bootstrap_mode, log_exists) {
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

        // The peer sender resolves voter endpoints from the static set.
        let peers = Arc::new(RealPeerSender::new(
            voters.clone(),
            config.client_id.clone(),
            dialer.clone(),
        ));

        let election_ms = u64::try_from(config.election_timeout.as_millis()).unwrap_or(1_000);

        // Build / recover the engine. `Join` nodes with an empty log + empty
        // voter set sit unattached until Slice 5's dynamic join; `Bootstrap`
        // seeds the static voter set.
        let engine = KraftController::open(
            data_dir.clone(),
            config.node_id,
            cluster_id,
            voters.clone(),
            election_ms,
            peers,
            config.snapshot_interval_records,
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
        ));
        info!(
            node_id = config.node_id,
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
            self_node_id: config.node_id,
            voters,
            dialer,
            controller_bound_addr: actual_addr,
        })
    }
}

/// True when the metadata log under `dir` already holds committed records (a
/// previously-formatted node). Detects either a quorum-state file or any log
/// segment, indicating a previously-formatted node.
fn metadata_log_nonempty(dir: &std::path::Path) -> bool {
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
    use super::*;
    use assert2::assert;
    use tempfile::TempDir;

    #[tokio::test]
    async fn bootstrap_on_non_empty_log_errors() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("first bootstrap ok");
        // Drive a commit so the log is non-empty on the second boot.
        let mut leader_rx = ctrl.watch_leader();
        while leader_rx.borrow().is_none() {
            leader_rx.changed().await.unwrap();
        }
        ctrl.submit_change(vec![crabka_metadata::MetadataRecord::V1Topic(
            crabka_metadata::TopicRecord {
                name: "seed".into(),
                topic_id: Uuid::new_v4(),
                partitions: 1,
                replication_factor: 1,
            },
        )])
        .await
        .expect("submit");
        ctrl.shutdown().await;

        let cfg2 = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        match Controller::start(cfg2).await {
            Err(err) => assert!(
                matches!(err, RaftError::Startup(_)),
                "Bootstrap on existing log must return Startup; got: {err:?}"
            ),
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
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        match Controller::start(cfg).await {
            Err(err) => assert!(
                matches!(err, RaftError::Startup(_)),
                "Rejoin on empty log must return Startup; got: {err:?}"
            ),
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
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        let mut leader_rx = ctrl.watch_leader();
        while leader_rx.borrow().is_none() {
            leader_rx.changed().await.unwrap();
        }
        ctrl.submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        })])
        .await
        .expect("submit");

        let slice = ctrl.metadata_records(0, usize::MAX).await;
        assert!(slice.high_watermark >= 1);
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
        assert!(found, "topic 't' must appear in fetched metadata records");
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn fetch_metadata_from_returns_committed_records() {
        use crabka_metadata::{MetadataImage, MetadataRecord, TopicRecord, from_kraft_value};
        use crabka_protocol::records::RecordBatch;

        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        let mut leader_rx = ctrl.watch_leader();
        while leader_rx.borrow().is_none() {
            leader_rx.changed().await.unwrap();
        }
        ctrl.submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
            name: "fetched".into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        })])
        .await
        .expect("submit");

        let addr = ctrl.controller_bound_addr();
        let resp = ctrl
            .fetch_metadata_from(addr, 0, 1_048_576)
            .await
            .expect("fetch");
        assert!(resp.error_code == 0);
        assert!(resp.high_watermark >= 1);

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
        assert!(
            found,
            "topic 'fetched' must appear in fetched metadata records"
        );
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn join_on_empty_log_starts_unattached() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Join,
            initial_voters: crabka_metadata::VoterSet::from_voters(std::iter::empty()),
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg)
            .await
            .expect("Join on empty log starts ok");
        // Without voters this node never elects.
        assert!(ctrl.watch_leader().borrow().is_none());
        ctrl.shutdown().await;
    }
}
