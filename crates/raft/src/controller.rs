//! `Controller` is the public entry point. Owns the openraft node, the
//! state machine watcher, the controller listener task, and the
//! `submit_change` leader-aware forwarding logic.
//!
//! Cluster formation is driven by `BootstrapMode`: one broker boots as
//! the singleton voter (`Bootstrap`), remaining fresh brokers skip
//! `initialize` (`Join`), and restarted brokers replay their on-disk log
//! (`Rejoin`), seeding state from the newest metadata checkpoint when one
//! exists before replaying the log entries that follow it.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;
use uuid::Uuid;

use crabka_metadata::{MetadataImage, MetadataRecord};

use crate::config::{BootstrapMode, ControllerConfig};
use crate::error::RaftError;
use crate::log_store::RaftLogStore;
use crate::network::{CrabkaRaftNetworkFactory, OutboundDialer, PlaintextDialer};
use crate::server;
use crate::state_machine::CrabkaStateMachine;
use crate::types::{AppData, Node, NodeId, Raft};

/// Crabka-native view of the openraft node's current quorum state.
/// Surfaced by [`ControllerHandle::quorum_state`] for the broker's
/// `DescribeQuorum` admin handler so callers don't have to depend on
/// openraft types directly.
#[derive(Debug, Clone)]
pub struct QuorumState {
    /// Raft term — used as the `KRaft` `leader_epoch` on the wire.
    pub current_term: u64,
    /// Index of the last log entry applied to this node's state machine.
    /// Used as `KRaft` `high_watermark` on the wire. `0` until the first
    /// commit (the same value openraft treats as "no log applied yet").
    pub last_applied_index: u64,
    /// Current cluster leader. `None` mid-election.
    pub current_leader: Option<NodeId>,
    /// Voter ids in the current membership config.
    pub voters: Vec<NodeId>,
    /// Full voter node identities (directory id + endpoints + kraft.version)
    /// in the current membership config, keyed by node id. Mirrors `voters`
    /// but carries the KIP-853 voter metadata the `DescribeQuorum` /
    /// dynamic-reconfiguration paths need.
    pub voter_nodes: BTreeMap<NodeId, Node>,
    /// Per-voter `matched` log index from openraft's `replication` map.
    /// Populated ONLY on the leader — openraft only knows peers'
    /// progress when this node is acknowledging their `AppendEntries`
    /// replies. On followers (and during elections) the map is empty
    /// and callers should fall back to the JVM `-1` ("Unknown")
    /// sentinel for each voter.
    pub per_voter_matched_index: BTreeMap<NodeId, u64>,
}

/// A contiguous byte window of the latest metadata `.checkpoint`,
/// returned by [`ControllerHandle::read_snapshot_range`] to back the
/// broker's `FetchSnapshot` handler. `end_offset` / `epoch` identify the
/// snapshot, and `total_size` is its full byte length so the handler can
/// drive paging.
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

/// Handle returned by [`Controller::start`]. Carries the live openraft
/// node, the state machine, a leader-id watcher, and the background task
/// join handles. Drop is NOT a clean shutdown — call [`Self::shutdown`]
/// to drain the listener + leader-pump tasks before the runtime is torn
/// down.
pub struct ControllerHandle {
    raft: Arc<Raft>,
    state_machine: Arc<CrabkaStateMachine>,
    leader: watch::Receiver<Option<NodeId>>,
    shutdown: CancellationToken,
    listener_task: Mutex<Option<JoinHandle<()>>>,
    leader_pump_task: Mutex<Option<JoinHandle<()>>>,
    snapshot_task: Mutex<Option<JoinHandle<()>>>,
    /// Directory holding the latest KIP-630 metadata `.checkpoint`. Read
    /// by [`Self::read_snapshot_range`] to serve the broker's
    /// `FetchSnapshot` handler.
    snapshot_dir: std::path::PathBuf,
    client_id: String,
    /// This node's own raft id. Used by [`ReconfigOps::is_leader`] to compare
    /// against the leader reported by `quorum_state`.
    self_node_id: NodeId,
    /// Max allowed observer lag (in log entries) before an `AddVoter`
    /// candidate may be promoted. Cloned from `ControllerConfig`.
    observer_lag_bound: u64,
    /// Serializes KIP-853 reconfigurations. A single in-flight add/remove/
    /// update at a time so the membership change and the authoritative
    /// `V1Voters` record stay in lockstep.
    reconfig_lock: Mutex<()>,
    /// Outbound dialer cloned from the factory at construction time.
    /// `forward_submit_to` uses it to reach the leader's controller
    /// listener with the same TLS / SASL handshake that openraft's
    /// `AppendEntries` / `Vote` RPCs ride on top of. For the PLAINTEXT
    /// path the broker doesn't inject a dialer, and
    /// `Controller::start` substitutes `PlaintextDialer` — equivalent
    /// to a bare `Connection::connect`.
    dialer: Arc<dyn OutboundDialer>,
    /// Clone of the openraft storage adapter. Used by
    /// [`Self::metadata_records`] to serve committed log entries to
    /// broker-only observers over `API_KEY_METADATA_FETCH`.
    log_store: Arc<RaftLogStore>,
    /// The address the controller listener actually bound to. When
    /// `ControllerConfig::controller_listen_addr` uses port 0 (OS-assigned,
    /// the norm in tests) this carries the resolved port. KIP-853 auto-join
    /// advertises this in the `AddRaftVoter` request so the leader's
    /// `add_learner` can dial the joiner back. Also used by tests and
    /// broker-only observers to dial the live listener.
    controller_bound_addr: SocketAddr,
}

impl ControllerHandle {
    /// Current metadata snapshot (cheap; `Arc` clone).
    #[must_use]
    pub fn current_image(&self) -> Arc<MetadataImage> {
        self.state_machine.current_image()
    }

    /// The address the controller listener actually bound to (the
    /// resolved port when `controller_listen_addr` requested port 0).
    /// KIP-853 auto-join advertises this so the leader can dial the
    /// joiner back to replicate the log.
    #[must_use]
    pub fn controller_bound_addr(&self) -> SocketAddr {
        self.controller_bound_addr
    }

    /// Read up to `max_bytes` of the latest metadata snapshot starting at
    /// `position`.
    #[must_use]
    pub fn read_snapshot_range(&self, position: i64, max_bytes: i32) -> SnapshotRange {
        let Some((id, bytes, _meta)) = crate::snapshot::load_latest(&self.snapshot_dir)
            .ok()
            .flatten()
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
            end_offset: id.end_offset,
            epoch: id.epoch,
            total_size: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
            bytes: bytes::Bytes::copy_from_slice(slice),
        })
    }

    /// Subscribe to leader-id changes. The receiver's initial value is
    /// `None` until the first metrics tick arrives from openraft.
    #[must_use]
    pub fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.leader.clone()
    }

    /// Subscribe to metadata-image changes. The receiver holds the
    /// current image immediately; callers use
    /// `rx.borrow()` + `rx.changed().await` to track updates.
    #[must_use]
    pub fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.state_machine.watch_image()
    }

    /// Snapshot the openraft node's current quorum state in
    /// Crabka-native terms. Used by the broker's `DescribeQuorum`
    /// (`api_key=55`, KIP-595) handler to fill `leader_epoch`,
    /// `high_watermark`, and per-voter `log_end_offset` for
    /// `kafka-metadata-quorum --describe`. Cheap — a clone of
    /// openraft's `RaftMetrics` watch value.
    #[must_use]
    pub fn quorum_state(&self) -> QuorumState {
        let m = self.raft.metrics().borrow().clone();
        let membership = m.membership_config.membership();
        let voters: Vec<NodeId> = membership.voter_ids().collect();
        // openraft's `nodes()` yields `(&NodeId, &Node)` for every member
        // (voters + learners). Restrict to the voter ids so `voter_nodes`
        // mirrors `voters`.
        let voter_set: std::collections::BTreeSet<NodeId> = voters.iter().copied().collect();
        let voter_nodes: BTreeMap<NodeId, Node> = membership
            .nodes()
            .filter(|(nid, _)| voter_set.contains(nid))
            .map(|(nid, node)| (*nid, node.clone()))
            .collect();
        // openraft populates `replication` only on the current leader;
        // on a follower the map is empty and per-voter `log_end_offset`
        // stays at the `Unknown` sentinel for each peer.
        let per_voter_matched_index: BTreeMap<NodeId, u64> = m
            .replication
            .as_ref()
            .map(|repl| {
                repl.iter()
                    .map(|(nid, log_id_opt)| (*nid, log_id_opt.as_ref().map_or(0, |lid| lid.index)))
                    .collect()
            })
            .unwrap_or_default();
        QuorumState {
            current_term: m.current_term,
            last_applied_index: m.last_applied.as_ref().map_or(0, |lid| lid.index),
            current_leader: m.current_leader,
            voters,
            voter_nodes,
            per_voter_matched_index,
        }
    }

    /// Manually trigger a metadata snapshot on this node. openraft's
    /// snapshot policy is [`SnapshotPolicy::Never`], so checkpoints are
    /// produced either through this path or the config-driven background
    /// trigger task. Returns once openraft has accepted the request; the
    /// build runs asynchronously in the engine. After the build completes
    /// openraft purges the log behind the snapshot (we keep zero
    /// in-snapshot logs).
    ///
    /// # Errors
    ///
    /// `RaftError::Openraft` if the raft engine is shut down or in a
    /// fatal state.
    pub async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| RaftError::Openraft(format!("{e:?}")))
    }

    /// Read committed `__cluster_metadata` entries starting at
    /// `fetch_offset` (an openraft log index), encoded as Kafka record
    /// batches for an observer. Entries beyond the current high watermark
    /// (last applied/committed index) are never served. `max_bytes` caps
    /// the encoded payload (at least one batch is always emitted so the
    /// observer makes progress).
    #[must_use]
    pub async fn metadata_records(
        &self,
        fetch_offset: u64,
        max_bytes: usize,
    ) -> crate::metadata_fetch::MetadataFetchSlice {
        crate::metadata_fetch::read_committed_slice(
            &self.raft,
            &self.log_store,
            fetch_offset,
            max_bytes,
        )
        .await
    }

    /// Submit a batch of metadata records.
    ///
    /// Returns `Ok(())` once the records are committed AND applied on
    /// this node. Pre-validates against the current image so we don't
    /// spam Raft with records the local state already rejects.
    ///
    /// On `ForwardToLeader`, retries up to three times with a 100ms
    /// backoff. After the third failure, surfaces the last reported
    /// leader id as `RaftError::NotLeader`. Any other openraft error is
    /// returned as `RaftError::Openraft` (debug-formatted; callers
    /// should treat it as a transient cluster fault).
    pub async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), RaftError> {
        // Pre-validation: cheap, local, and avoids waking openraft when
        // the change is doomed (e.g., duplicate topic). Validate against a
        // local clone that we apply records into as we go — otherwise a
        // batch containing `V1Topic(t)` followed by `V1Partition{topic: t}`
        // would reject the partition because the topic isn't yet in the
        // committed image.
        let mut scratch: MetadataImage = (*self.state_machine.current_image()).clone();
        for r in &records {
            scratch.validate(r)?;
            scratch.apply(r);
        }
        let data = AppData { records };

        let mut last_known_leader: Option<NodeId> = None;
        for attempt in 1u32..=3 {
            match self.raft.client_write(data.clone()).await {
                Ok(resp) => {
                    // The leader's state machine re-validates each record
                    // at apply time; the first rejection is the canonical
                    // error for the caller. `MetadataError` doesn't
                    // derive `serde`, so we carry the rendered string
                    // through `AppDataResponse` and reconstruct here.
                    // The "topic '<name>' already exists" prefix is the
                    // only signal we need for the
                    // `TopicExists`-vs-`InvalidRecord` discrimination.
                    if let Some(msg) = resp.data.rejected.into_iter().next() {
                        let err = if let Some(rest) = msg.strip_prefix("topic '")
                            && let Some(name) = rest.strip_suffix("' already exists")
                        {
                            crabka_metadata::MetadataError::TopicExists(name.to_string())
                        } else {
                            crabka_metadata::MetadataError::InvalidRecord("validation failed")
                        };
                        return Err(RaftError::Metadata(err));
                    }
                    return Ok(());
                }
                Err(openraft::error::RaftError::APIError(
                    openraft::error::ClientWriteError::ForwardToLeader(f),
                )) => {
                    last_known_leader = f.leader_id;
                    // If openraft tells us who the leader is and it isn't
                    // us, forward the change directly to the leader's
                    // controller listener via the
                    // `API_KEY_SUBMIT_CHANGE` RPC. Otherwise (transient
                    // `leader_id: None` during election) fall through to
                    // the retry loop.
                    if let Some(leader) = last_known_leader
                        && let Some(addr) = self.voter_addr(leader)
                    {
                        return self.forward_submit_to(leader, addr, &data.records).await;
                    }
                    if attempt == 3 {
                        return Err(RaftError::NotLeader {
                            current_leader: last_known_leader,
                        });
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => return Err(RaftError::Openraft(format!("{e:?}"))),
            }
        }
        // Unreachable: the loop above always returns inside attempt == 3.
        Err(RaftError::NotLeader {
            current_leader: last_known_leader,
        })
    }

    /// Mutate the openraft voter set. `new_voters` is the **complete** desired set
    /// (not a delta). Any voter in the current set but not in `new_voters` is
    /// removed entirely (`retain=false`). Any voter in `new_voters` that isn't
    /// already in the cluster must have been registered via [`Self::add_learner`]
    /// first — openraft refuses to promote unknown ids.
    ///
    /// Two-phase joint config: openraft commits a joint membership log entry
    /// (old ∪ new), then a uniform log entry (new only). If the leader crashes
    /// between the two, the cluster is left in joint config and a future call
    /// completes the transition.
    ///
    /// # Errors
    ///
    /// - `RaftError::NotLeader` if this node isn't the openraft leader.
    /// - `RaftError::ChangeRejected` if openraft rejects (e.g., the new voter set
    ///   would leave the cluster without quorum, or a promoted node isn't a learner).
    /// - `RaftError::Shutdown` if the raft engine has been shut down.
    pub async fn change_membership(
        &self,
        new_voters: std::collections::BTreeSet<NodeId>,
    ) -> Result<(), RaftError> {
        use openraft::error::ClientWriteError;
        use openraft::error::RaftError as ORE;
        match self.raft.change_membership(new_voters, false).await {
            Ok(_) => Ok(()),
            Err(ORE::APIError(ClientWriteError::ForwardToLeader(f))) => Err(RaftError::NotLeader {
                current_leader: f.leader_id,
            }),
            Err(ORE::APIError(ClientWriteError::ChangeMembershipError(e))) => {
                Err(RaftError::ChangeRejected(format!("{e:?}")))
            }
            Err(e) => Err(RaftError::Openraft(format!("{e:?}"))),
        }
    }

    /// Register a non-voting raft learner with id `node_id` and the KIP-853
    /// voter identity `node` (directory id + endpoints + kraft.version range).
    /// Blocks until the leader has replicated up to its current commit index
    /// to the new node (so a subsequent [`Self::change_membership`] promotion
    /// won't stall waiting for catch-up). Pair with [`Self::change_membership`]
    /// to turn a learner into a voter:
    ///
    /// ```ignore
    /// let node = Node { directory_id, endpoints, kraft_version };
    /// controller.add_learner(4, node).await?;
    /// controller.change_membership([1, 2, 3, 4].into_iter().collect()).await?;
    /// ```
    ///
    /// # Errors
    ///
    /// - `RaftError::NotLeader` if this node isn't the openraft leader.
    /// - `RaftError::ChangeRejected` if openraft rejects (e.g., the learner
    ///   never catches up within openraft's internal deadline).
    /// - `RaftError::Shutdown` if the raft engine has been shut down.
    pub async fn add_learner(&self, node_id: NodeId, node: Node) -> Result<(), RaftError> {
        use openraft::error::ClientWriteError;
        use openraft::error::RaftError as ORE;
        match self.raft.add_learner(node_id, node, true).await {
            Ok(_) => Ok(()),
            Err(ORE::APIError(ClientWriteError::ForwardToLeader(f))) => Err(RaftError::NotLeader {
                current_leader: f.leader_id,
            }),
            Err(ORE::APIError(ClientWriteError::ChangeMembershipError(e))) => {
                Err(RaftError::ChangeRejected(format!("{e:?}")))
            }
            Err(e) => Err(RaftError::Openraft(format!("{e:?}"))),
        }
    }

    /// Add a single voter (KIP-853 `AddVoter`). The candidate must already be
    /// reachable as a learner; the coordinator registers it, waits for it to
    /// catch up within `observer_lag_bound`, promotes it, and writes the
    /// authoritative `V1Voters` record. Serialized against other
    /// reconfigurations by the per-handle lock.
    ///
    /// # Errors
    ///
    /// Surfaces the coordinator's guard errors ([`RaftError::ReconfigInProgress`],
    /// [`RaftError::VoterNotCaughtUp`]) and any underlying raft failure.
    pub async fn add_voter(
        &self,
        req: crate::reconfig::AddVoter,
    ) -> Result<crate::reconfig::ReconfigOutcome, RaftError> {
        crate::reconfig::Coordinator::new(self, &self.reconfig_lock, self.observer_lag_bound)
            .add_voter(req)
            .await
    }

    /// Remove a single voter (KIP-853 `RemoveVoter`), refusing to drop the
    /// last voter. Serialized against other reconfigurations.
    ///
    /// # Errors
    ///
    /// Surfaces the coordinator's guard errors ([`RaftError::ReconfigInProgress`],
    /// [`RaftError::ReconfigRejected`]) and any underlying raft failure.
    pub async fn remove_voter(
        &self,
        req: crate::reconfig::RemoveVoter,
    ) -> Result<crate::reconfig::ReconfigOutcome, RaftError> {
        crate::reconfig::Coordinator::new(self, &self.reconfig_lock, self.observer_lag_bound)
            .remove_voter(req)
            .await
    }

    /// Update a voter's endpoints / supported version range (KIP-853
    /// `UpdateVoter`). Serialized against other reconfigurations.
    ///
    /// # Errors
    ///
    /// Surfaces the coordinator's guard errors ([`RaftError::ReconfigInProgress`],
    /// [`RaftError::ReconfigRejected`]) and any underlying raft failure.
    pub async fn update_voter(
        &self,
        req: crate::reconfig::UpdateVoter,
    ) -> Result<crate::reconfig::ReconfigOutcome, RaftError> {
        crate::reconfig::Coordinator::new(self, &self.reconfig_lock, self.observer_lag_bound)
            .update_voter(req)
            .await
    }

    /// Resolve a voter's controller listener address from openraft's current
    /// membership config. KIP-853 voters carry their endpoints in the `Node`
    /// payload, so the address is read directly from the replicated
    /// membership rather than a static config-time list.
    fn voter_addr(&self, node_id: NodeId) -> Option<SocketAddr> {
        let m = self.raft.metrics().borrow().clone();
        m.membership_config
            .membership()
            .get_node(&node_id)
            .and_then(Node::controller_addr)
    }

    /// Open a one-shot authenticated connection to the leader's
    /// controller listener, send a bincode-encoded `Vec<MetadataRecord>`
    /// as `API_KEY_SUBMIT_CHANGE`, and translate the response back into a
    /// `RaftError`.
    ///
    /// Routes through [`OutboundDialer::dial`] so the same TLS / SASL
    /// handshake openraft's `AppendEntries` / `Vote` RPCs ride on top
    /// of applies here too. For the PLAINTEXT path,
    /// `Controller::start` substitutes `PlaintextDialer`, which is
    /// byte-equivalent to a bare `Connection::connect`.
    ///
    /// A fresh connection per call mirrors a raw
    /// `TcpStream::connect` — `submit_change` forwarding is
    /// rare (only on follower-side writes) and reusing the openraft
    /// network factory's cache from here would complicate ownership for
    /// negligible gain.
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

        // Dial through the injected dialer so SASL / TLS terminates
        // before the first raft frame leaves this host. The dialer
        // returns a `Connection` that has already negotiated
        // `ApiVersions` and (when applicable) completed SASL auth.
        let opts = crabka_client_core::ConnectionOptions {
            client_id: self.client_id.clone(),
            ..crabka_client_core::ConnectionOptions::default()
        };
        let conn = self
            .dialer
            .dial(leader, &addr.to_string(), opts)
            .await
            .map_err(RaftError::Network)?;

        // `raw_request` builds the v2 RequestHeader, writes the frame,
        // reads the response, and strips the v1 ResponseHeader's
        // leading tagged-fields byte. The server in `server.rs` returns
        // a v1 ResponseHeader for every Crabka-private api key, so the
        // returned bytes are the bare `CrabkaSubmitChangeResponse` body.
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
            // `error_code = 2` => leader rejected at apply-time. We
            // collapse the typed `MetadataError` into a generic
            // `TopicExists` here since the wire only carries an error
            // code; the topic name is what the caller had in hand.
            2 => Err(RaftError::Metadata(
                crabka_metadata::MetadataError::TopicExists(String::new()),
            )),
            // `error_code = 1` (not leader) and `3` (other) collapse to
            // `NotLeader` — the test of record (CreateTopics) maps that
            // to `NOT_CONTROLLER`, which the client treats as retryable.
            _ => Err(RaftError::NotLeader {
                current_leader: (resp.leader_hint >= 0)
                    .then(|| u64::try_from(resp.leader_hint).unwrap_or(leader)),
            }),
        }
    }

    /// Dial a controller-listener `addr` and issue one
    /// `API_KEY_METADATA_FETCH`. Used by broker-only observers (and the
    /// in-crate integration test) to pull committed `__cluster_metadata`
    /// entries. Routes through the same [`OutboundDialer`] as
    /// `forward_submit_to`, so TLS/SASL terminates before the first frame.
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

    /// Drain all background tasks and shut down the inner openraft node.
    /// Idempotent in practice — `CancellationToken::cancel` is, and the
    /// task join handles are taken under the mutex.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        // Stop the openraft engine first so the listener stops getting
        // fresh work; then drain the spawned tasks.
        let _ = self.raft.shutdown().await;
        if let Some(h) = self.listener_task.lock().await.take() {
            let _ = h.await;
        }
        if let Some(h) = self.leader_pump_task.lock().await.take() {
            let _ = h.await;
        }
        if let Some(h) = self.snapshot_task.lock().await.take() {
            let _ = h.await;
        }
    }

    /// Stop the openraft engine and cancel the controller listener without
    /// consuming `self`. Used by `BrokerHandle::shutdown` where the
    /// controller is behind an `Arc` and cannot be moved out.
    ///
    /// Idempotent — calling multiple times is safe (cancellation token and
    /// `raft.shutdown()` are both idempotent).
    ///
    /// Awaits the listener task so that the OS port is guaranteed to be
    /// released before this method returns. This is important for tests
    /// that immediately rebind the same port after `BrokerHandle::shutdown`.
    pub async fn cancel(&self) {
        self.shutdown.cancel();
        let _ = self.raft.shutdown().await;
        // Drain the listener task so its `TcpListener` is dropped (and the
        // port is released by the OS) before we return.
        if let Some(h) = self.listener_task.lock().await.take() {
            let _ = h.await;
        }
        if let Some(h) = self.snapshot_task.lock().await.take() {
            let _ = h.await;
        }
    }
}

#[async_trait::async_trait]
impl crate::reconfig::ReconfigOps for ControllerHandle {
    fn current_voters(&self) -> crabka_metadata::VoterSet {
        // Rebuild a `VoterSet` from the replicated membership's voter nodes.
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
        // openraft's metrics don't expose a separate `last_log_index` we can
        // read cheaply here; `last_applied_index` is the high-watermark this
        // node has committed and is a safe (never-overshooting) basis for the
        // observer-lag check.
        self.quorum_state().last_applied_index
    }

    fn observer_index(&self, id: NodeId) -> Option<u64> {
        // openraft only populates `replication` on the leader, and learners
        // may be absent. Returning `None` is conservative — the lag check
        // then treats the candidate as fully behind (lag == leader_last_index)
        // and refuses promotion until catch-up is observed.
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

/// Zero-sized factory for [`ControllerHandle`]s. Kept as a unit struct
/// (rather than a free function) so callers and rustdoc can hang
/// trait-style documentation off a stable type name.
pub struct Controller;

impl Controller {
    /// Start an openraft node, open the controller listener, and begin
    /// participating in the quorum.
    ///
    /// Cluster formation is governed by [`ControllerConfig::bootstrap_mode`]:
    /// `Bootstrap` initializes a singleton-voter cluster on an empty log;
    /// `Join` skips initialize and waits for an external `add_learner`;
    /// `Rejoin` skips initialize and relies on the on-disk raft log.
    /// Mismatches between mode and log state return `RaftError::Startup`.
    pub async fn start(config: ControllerConfig) -> Result<ControllerHandle, RaftError> {
        Self::start_with_listener(config, None).await
    }

    /// Like [`Self::start`], but adopts a caller-supplied, already-bound
    /// controller listener instead of binding `controller_listen_addr`
    /// itself.
    ///
    /// Test harnesses use this to defeat the bind-and-drop TOCTOU race:
    /// the test binds an ephemeral port, hands the live `TcpListener`
    /// here (never dropping it), so no other process can claim the port
    /// in the gap between probe and bind. The supplied listener's local
    /// address MUST equal `config.controller_listen_addr` — the bootstrap
    /// membership record and the voter map are built from the config
    /// value, so a mismatch would advertise an unreachable dial address.
    #[allow(clippy::too_many_lines)]
    pub async fn start_with_listener(
        config: ControllerConfig,
        prebound: Option<tokio::net::TcpListener>,
    ) -> Result<ControllerHandle, RaftError> {
        // 1. Log + state machine. The cluster UUID is injected from the
        //    operator (via `BrokerConfig::cluster_id`) so every broker in
        //    the same `KafkaCluster` reports a matching `MetadataImage`
        //    cluster id. Falls back to `Uuid::nil()` for legacy
        //    single-node tests that don't set it.
        let log_store = Arc::new(RaftLogStore::open(config.log_dir.clone()).await?);
        let snapshot_dir = config.log_dir.join("@metadata-0");
        let state_machine = Arc::new(CrabkaStateMachine::new(
            config.cluster_id.unwrap_or_else(Uuid::nil),
            snapshot_dir.clone(),
        ));

        // 2. openraft engine config. Times are millis; we widen the
        //    election window to `[t, 2t]` to keep the standard openraft
        //    jitter behavior.
        let election_min: u64 = u64::try_from(config.election_timeout.as_millis()).unwrap_or(1_000);
        let election_max = election_min.saturating_mul(2);
        let heartbeat: u64 = u64::try_from(config.heartbeat_interval.as_millis()).unwrap_or(200);
        let raft_config = openraft::Config {
            cluster_name: "crabka-metadata".to_string(),
            election_timeout_min: election_min,
            election_timeout_max: election_max,
            heartbeat_interval: heartbeat,
            install_snapshot_timeout: 5_000,
            // Never auto-snapshot — Crabka drives checkpoints itself via
            // `trigger_snapshot` and the config-driven background trigger
            // task. Keep zero in-snapshot logs so a completed snapshot
            // immediately drives `purge`, compacting the metadata log
            // behind the checkpoint.
            snapshot_policy: openraft::SnapshotPolicy::Never,
            max_in_snapshot_log_to_keep: 0,
            ..Default::default()
        };

        // 3. Network factory. Resolves each peer addr from the KIP-853
        //    voter `Node` surfaced by openraft membership (the CONTROLLER
        //    endpoint via `Node::controller_addr`).
        // Use the injected dialer if the broker provided one
        // (inter-broker TLS / SASL), otherwise fall back to plain
        // `TcpStream::connect` for the PLAINTEXT path.
        let dialer: Arc<dyn OutboundDialer> = config
            .dialer
            .clone()
            .unwrap_or_else(|| Arc::new(PlaintextDialer));
        let network = CrabkaRaftNetworkFactory::new(config.client_id.clone(), dialer.clone());

        // 4. Spawn openraft. `Raft::new` consumes the log store and state
        //    machine; we keep our own `Arc` clones so the controller
        //    handle can read the image and probe the log on shutdown.
        let raft = Arc::new(
            openraft::Raft::new(
                config.node_id,
                Arc::new(raft_config),
                network,
                log_store.clone(),
                state_machine.clone(),
            )
            .await
            .map_err(|e| RaftError::Openraft(format!("{e:?}")))?,
        );

        // 5. First-boot orchestration. The bootstrap_mode tells us which
        //    role this broker plays in cluster formation. Misuse is fatal —
        //    a Bootstrap on top of an existing log would re-seed the
        //    cluster, and a Rejoin on an empty log would never converge.
        let log_is_empty = log_store.last_log_id().await.is_none();
        match (config.bootstrap_mode, log_is_empty) {
            (BootstrapMode::Bootstrap, true) => {
                // Seed-membership init from the operator-supplied initial
                // voter set (KIP-853 dynamic). The bootstrap node holds the
                // initial `VotersRecord`; openraft replicates it as the first
                // membership log entry. A single-voter seed self-elects on the
                // first election timeout with no contention.
                if config.initial_voters.is_empty() {
                    return Err(RaftError::Startup(
                        "Bootstrap mode requires a non-empty initial_voters set".into(),
                    ));
                }
                let members: BTreeMap<NodeId, Node> = config
                    .initial_voters
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
                raft.initialize(members)
                    .await
                    .map_err(|e| RaftError::Openraft(format!("bootstrap initialize: {e:?}")))?;
            }
            (BootstrapMode::Join, true) | (BootstrapMode::Rejoin, false) => {
                // Join: don't initialize — engine waits in Learner state
                //   until the bootstrap broker's add_learner arrives.
                // Rejoin: existing log carries membership; openraft
                //   replayed it during Raft::new. Nothing to do.
            }
            (BootstrapMode::Bootstrap, false) => {
                return Err(RaftError::Startup(
                    "Bootstrap mode requires empty raft log; existing log indicates an already-initialized broker — use Rejoin".into(),
                ));
            }
            (BootstrapMode::Rejoin, true) => {
                return Err(RaftError::Startup(
                    "Rejoin mode requires non-empty raft log; this broker has no on-disk state — use Bootstrap or Join".into(),
                ));
            }
            (BootstrapMode::Join, false) => {
                return Err(RaftError::Startup(
                    "Join mode requires empty raft log; this broker has on-disk state — use Rejoin"
                        .into(),
                ));
            }
        }

        // 6. Controller listener. Adopt the caller-supplied listener when
        //    present (test harness handoff); otherwise bind here so we
        //    surface a clear error if the port is taken, then hand it off
        //    to the accept loop.
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
        let listener_task = tokio::spawn(server::run(
            listener,
            raft.clone(),
            log_store.clone(),
            shutdown.clone(),
            config.handshake.clone(),
        ));
        info!(
            node_id = config.node_id,
            addr = %actual_addr,
            "controller started"
        );

        // 7. Leader-watch pump. Republish openraft's `current_leader`
        //    metric into a public `watch::Receiver<Option<NodeId>>` so
        //    callers don't need to depend on openraft directly.
        let (leader_tx, leader_rx) = watch::channel::<Option<NodeId>>(None);
        let mut metrics_rx = raft.metrics();
        let shutdown_for_pump = shutdown.clone();
        let leader_pump_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = shutdown_for_pump.cancelled() => break,
                    res = metrics_rx.changed() => {
                        if res.is_err() {
                            // Metrics sender dropped — engine is gone.
                            break;
                        }
                        let leader = metrics_rx.borrow().current_leader;
                        // `send` errors only when there are no receivers; that's fine.
                        let _ = leader_tx.send(leader);
                    }
                }
            }
        });

        // 8. Snapshot trigger pump. openraft's policy is
        //    `SnapshotPolicy::Never`, so the Kafka-faithful heuristics live
        //    here: only the current leader fires, and only when the
        //    metadata-log has grown by `max_bytes_between_snapshots` since the
        //    last snapshot, or the interval elapses. The byte signal is a
        //    delta against a baseline captured at the previous trigger —
        //    purge only deletes sealed segments, so the active segment keeps
        //    the absolute size above the threshold and a raw comparison would
        //    re-fire on every tick.
        let raft_for_snap = raft.clone();
        let shutdown_for_snap = shutdown.clone();
        let log_store_for_snap = log_store.clone();
        let max_bytes = config.max_bytes_between_snapshots;
        let interval = config.max_snapshot_interval;
        let snapshot_task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(500));
            let mut last_snapshot_at = tokio::time::Instant::now();
            let mut bytes_at_last_snapshot = log_store_for_snap.size_bytes().await;
            loop {
                tokio::select! {
                    () = shutdown_for_snap.cancelled() => break,
                    _ = tick.tick() => {
                        let m = raft_for_snap.metrics().borrow().clone();
                        if m.current_leader != Some(m.id) {
                            continue;
                        }
                        let log_bytes = log_store_for_snap.size_bytes().await;
                        let bytes_grown = log_bytes.saturating_sub(bytes_at_last_snapshot);
                        let interval_elapsed =
                            interval > Duration::ZERO && last_snapshot_at.elapsed() >= interval;
                        if (bytes_grown >= max_bytes || interval_elapsed)
                            && raft_for_snap.trigger().snapshot().await.is_ok()
                        {
                            last_snapshot_at = tokio::time::Instant::now();
                            bytes_at_last_snapshot = log_bytes;
                        }
                    }
                }
            }
        });

        Ok(ControllerHandle {
            raft,
            state_machine,
            leader: leader_rx,
            shutdown,
            listener_task: Mutex::new(Some(listener_task)),
            leader_pump_task: Mutex::new(Some(leader_pump_task)),
            snapshot_task: Mutex::new(Some(snapshot_task)),
            snapshot_dir,
            client_id: config.client_id.clone(),
            self_node_id: config.node_id,
            observer_lag_bound: config.observer_lag_bound,
            reconfig_lock: Mutex::new(()),
            dialer,
            log_store: log_store.clone(),
            controller_bound_addr: actual_addr,
        })
    }
}

#[cfg(test)]
mod bootstrap_mode_tests {
    use super::*;
    use assert2::assert;
    use tempfile::TempDir;

    #[tokio::test]
    async fn bootstrap_on_non_empty_log_errors() {
        let dir = TempDir::new().unwrap();
        // First boot: bootstrap fresh.
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("first bootstrap ok");
        ctrl.shutdown().await;

        // Second boot: log is non-empty, Bootstrap must error.
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
        use crabka_metadata::{MetadataRecord, TopicRecord, from_kafka_record};
        use crabka_protocol::records::RecordBatch;
        use uuid::Uuid;

        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        // Wait to become leader.
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
        // Decode the batches and confirm topic "t" is present somewhere.
        let mut buf: &[u8] = &slice.records;
        let mut found = false;
        while !buf.is_empty() {
            let batch = RecordBatch::decode(&mut buf).expect("decode");
            for r in &batch.records {
                if let Ok(MetadataRecord::V1Topic(t)) = from_kafka_record(r)
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
        use crabka_metadata::{MetadataRecord, TopicRecord, from_kafka_record};
        use crabka_protocol::records::RecordBatch;
        use uuid::Uuid;

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

        // `voter_addr(1)` returns the pre-bind port-0 addr from for_tests;
        // use `controller_bound_addr()` instead to get the actual OS-assigned port.
        let addr = ctrl.controller_bound_addr();
        let resp = ctrl
            .fetch_metadata_from(addr, 0, 1_048_576)
            .await
            .expect("fetch");
        assert!(resp.error_code == 0);
        assert!(resp.high_watermark >= 1);

        let mut buf: &[u8] = &resp.records;
        let mut found = false;
        while !buf.is_empty() {
            let batch = RecordBatch::decode(&mut buf).expect("decode");
            for r in &batch.records {
                if let Ok(MetadataRecord::V1Topic(t)) = from_kafka_record(r)
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
    async fn join_on_empty_log_starts_in_learner_state() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Join,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg)
            .await
            .expect("Join on empty log starts ok");
        // Without an external add_learner the watch_leader stays None.
        assert!(ctrl.watch_leader().borrow().is_none());
        ctrl.shutdown().await;
    }
}
