//! `MetadataSource` — the metadata authority a broker reads from and
//! writes through. Combined/controller nodes back it with a live
//! `ControllerHandle` (openraft voter); broker-only nodes back it with a
//! `MetadataObserver` (true `KRaft` observer) plus a write-forwarding path
//! to the controller quorum. Handlers depend only on this trait.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::watch;

use crabka_metadata::{MetadataImage, MetadataRecord};
use crabka_raft::{
    AddVoter, ControllerHandle, Node, NodeId, OutboundDialer, QuorumState, RaftError,
    ReconfigOutcome, RemoveVoter, SnapshotRange, UpdateVoter,
};

use crate::metadata_observer::MetadataObserver;

#[async_trait::async_trait]
pub trait MetadataSource: Send + Sync {
    fn current_image(&self) -> Arc<MetadataImage>;
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>>;
    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>>;
    fn quorum_state(&self) -> QuorumState;
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), RaftError>;
    async fn change_membership(&self, new_voters: BTreeSet<NodeId>) -> Result<(), RaftError>;
    async fn add_learner(&self, node_id: NodeId, node: Node) -> Result<(), RaftError>;
    /// The controller listener's bound address. Meaningful only on
    /// controller/combined nodes; broker-only observers have no controller
    /// listener and report an unspecified address.
    fn controller_bound_addr(&self) -> SocketAddr;
    /// Read a byte window of the latest metadata snapshot to serve
    /// `FetchSnapshot`. Controller/combined nodes back this with their
    /// on-disk checkpoint; broker-only observers have none to serve.
    fn read_snapshot_range(&self, position: i64, max_bytes: i32) -> SnapshotRange;
    /// Schedule a metadata snapshot. Meaningful only on controller/combined
    /// nodes; broker-only observers have no log of their own to snapshot.
    async fn trigger_snapshot(&self) -> Result<(), RaftError>;
    async fn add_voter(&self, req: AddVoter) -> Result<ReconfigOutcome, RaftError>;
    async fn remove_voter(&self, req: RemoveVoter) -> Result<ReconfigOutcome, RaftError>;
    async fn update_voter(&self, req: UpdateVoter) -> Result<ReconfigOutcome, RaftError>;
    async fn cancel(&self);
}

#[async_trait::async_trait]
impl MetadataSource for ControllerHandle {
    fn current_image(&self) -> Arc<MetadataImage> {
        ControllerHandle::current_image(self)
    }
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        ControllerHandle::watch_image(self)
    }
    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        ControllerHandle::watch_leader(self)
    }
    fn quorum_state(&self) -> QuorumState {
        ControllerHandle::quorum_state(self)
    }
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), RaftError> {
        ControllerHandle::submit_change(self, records).await
    }
    async fn change_membership(&self, new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
        ControllerHandle::change_membership(self, new_voters).await
    }
    async fn add_learner(&self, node_id: NodeId, node: Node) -> Result<(), RaftError> {
        ControllerHandle::add_learner(self, node_id, node).await
    }
    fn controller_bound_addr(&self) -> SocketAddr {
        ControllerHandle::controller_bound_addr(self)
    }
    fn read_snapshot_range(&self, position: i64, max_bytes: i32) -> SnapshotRange {
        ControllerHandle::read_snapshot_range(self, position, max_bytes)
    }
    async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        ControllerHandle::trigger_snapshot(self).await
    }
    async fn add_voter(&self, req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
        ControllerHandle::add_voter(self, req).await
    }
    async fn remove_voter(&self, req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
        ControllerHandle::remove_voter(self, req).await
    }
    async fn update_voter(&self, req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
        ControllerHandle::update_voter(self, req).await
    }
    async fn cancel(&self) {
        ControllerHandle::cancel(self).await;
    }
}

/// Broker-only metadata source: reads from a [`MetadataObserver`], writes
/// by forwarding to the controller quorum.
pub struct ObserverSource {
    observer: Arc<MetadataObserver>,
    writer: Arc<dyn MetadataWriter>,
}

/// Write side for broker-only nodes: forward a batch to the controller
/// quorum leader.
#[async_trait::async_trait]
pub trait MetadataWriter: Send + Sync {
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), RaftError>;
}

impl ObserverSource {
    #[must_use]
    pub fn new(observer: Arc<MetadataObserver>, writer: Arc<dyn MetadataWriter>) -> Self {
        Self { observer, writer }
    }
}

#[async_trait::async_trait]
impl MetadataSource for ObserverSource {
    fn current_image(&self) -> Arc<MetadataImage> {
        self.observer.current_image()
    }
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.observer.watch_image()
    }
    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.observer.watch_leader()
    }
    fn quorum_state(&self) -> QuorumState {
        // A broker-only node is not a voter and has no openraft state of its
        // own, so only `current_leader` is meaningful here. `current_term` /
        // `last_applied_index` / per-voter progress are unknown — DescribeQuorum
        // on a broker-only node forwards to a controller in a later component.
        QuorumState {
            current_term: 0,
            last_applied_index: 0,
            current_leader: *self.observer.watch_leader().borrow(),
            voters: Vec::new(),
            voter_nodes: std::collections::BTreeMap::new(),
            per_voter_matched_index: std::collections::BTreeMap::new(),
        }
    }
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), RaftError> {
        self.writer.submit_change(records).await
    }
    async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
        Err(RaftError::NotLeader {
            current_leader: None,
        })
    }
    async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
        Err(RaftError::NotLeader {
            current_leader: None,
        })
    }
    fn controller_bound_addr(&self) -> SocketAddr {
        // A broker-only node runs no controller listener. The only callers
        // (DescribeQuorum / KIP-853 reconfiguration) live on controllers, so
        // this is never reached in practice; report an unspecified address.
        SocketAddr::from(([0, 0, 0, 0], 0))
    }
    fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
        // A broker-only observer holds no checkpoint of its own to serve;
        // FetchSnapshot is answered by the controller quorum.
        SnapshotRange::NoSnapshot
    }
    async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        Err(RaftError::NotLeader {
            current_leader: None,
        })
    }
    async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
        Err(RaftError::NotLeader {
            current_leader: None,
        })
    }
    async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
        Err(RaftError::NotLeader {
            current_leader: None,
        })
    }
    async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
        Err(RaftError::NotLeader {
            current_leader: None,
        })
    }
    async fn cancel(&self) {
        self.observer.cancel().await;
    }
}

/// Forwards metadata writes from a broker-only node to the controller
/// quorum. Tries the leader hint first (from the observer), then walks the
/// voter list. Mirrors the `API_KEY_SUBMIT_CHANGE` request the controller
/// already serves.
pub struct QuorumForwarder {
    pub(crate) voters: Vec<(NodeId, SocketAddr)>,
    pub(crate) dialer: Arc<dyn OutboundDialer>,
    pub(crate) client_id: String,
    pub(crate) leader: watch::Receiver<Option<NodeId>>,
}

impl QuorumForwarder {
    async fn try_submit(
        &self,
        target: NodeId,
        addr: SocketAddr,
        body: &[u8],
    ) -> Result<crabka_raft::CrabkaSubmitChangeResponse, RaftError> {
        let opts = crabka_client_core::ConnectionOptions {
            client_id: self.client_id.clone(),
            ..crabka_client_core::ConnectionOptions::default()
        };
        let conn = self
            .dialer
            .dial(target, &addr.to_string(), opts)
            .await
            .map_err(RaftError::Network)?;
        let resp_body = conn
            .raw_request(
                crabka_raft::API_KEY_SUBMIT_CHANGE,
                0,
                bytes::Bytes::copy_from_slice(body),
            )
            .await
            .map_err(RaftError::Network)?;
        conn.close();
        let mut cur: &[u8] = &resp_body;
        crabka_raft::CrabkaSubmitChangeResponse::decode_v0(&mut cur).map_err(RaftError::Protocol)
    }
}

#[async_trait::async_trait]
impl MetadataWriter for QuorumForwarder {
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), RaftError> {
        let payload =
            <serde_wincode::SerdeCompat<Vec<MetadataRecord>> as wincode::Serialize>::serialize(
                &records,
            )
            .map_err(RaftError::from)?;
        let req = crabka_raft::CrabkaSubmitChangeRequest {
            records: bytes::Bytes::from(payload),
        };
        // + 4 for the length-prefix encode_v0 writes ahead of the records.
        let mut body = Vec::with_capacity(req.records.len() + 4);
        req.encode_v0(&mut body).map_err(RaftError::Protocol)?;

        let hint = *self.leader.borrow();
        let mut order: Vec<(NodeId, SocketAddr)> = Vec::new();
        if let Some(l) = hint
            && let Some(t) = self.voters.iter().find(|(id, _)| *id == l)
        {
            order.push(*t);
        }
        for v in &self.voters {
            if Some(v.0) != hint {
                order.push(*v);
            }
        }

        let mut last_err = RaftError::NotLeader {
            current_leader: hint,
        };
        for (target, addr) in order {
            match self.try_submit(target, addr, &body).await {
                Ok(resp) if resp.error_code == 0 => return Ok(()),
                // error_code 2 => leader rejected at apply-time. Match the
                // controller's own forward path (`forward_submit_to`), which
                // collapses the typed `MetadataError` into `TopicExists` since
                // the wire carries only an error code and the forwarded write
                // of record is CreateTopics (-> Kafka TOPIC_ALREADY_EXISTS).
                Ok(resp) if resp.error_code == 2 => {
                    return Err(RaftError::Metadata(
                        crabka_metadata::MetadataError::TopicExists(String::new()),
                    ));
                }
                Ok(resp) => {
                    last_err = RaftError::NotLeader {
                        current_leader: (resp.leader_hint >= 0)
                            .then(|| u64::try_from(resp.leader_hint).unwrap_or(0)),
                    };
                }
                Err(e) => last_err = e,
            }
        }
        Err(last_err)
    }
}
