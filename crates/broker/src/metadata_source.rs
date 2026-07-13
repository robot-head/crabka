//! `MetadataSource` — the metadata authority a broker reads from and
//! writes through. Combined/controller nodes back it with a live
//! `ControllerHandle` (openraft voter); broker-only nodes back it with a
//! `MetadataObserver` (true `KRaft` observer) plus a write-forwarding path
//! to the controller quorum. Handlers depend only on this trait.

use std::{collections::BTreeSet, net::SocketAddr, sync::Arc};

use crabka_metadata::{MetadataImage, MetadataRecord};
use crabka_raft::{
    AddVoter, ControllerHandle, Node, NodeId, OutboundDialer, QuorumState, RaftError,
    ReconfigOutcome, RemoveVoter, SnapshotRange, SubmitChangeResult, UpdateVoter,
};
use tokio::sync::watch;

use crate::metadata_observer::MetadataObserver;

#[async_trait::async_trait]
pub trait MetadataSource: Send + Sync {
    fn current_image(&self) -> Arc<MetadataImage>;
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>>;
    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>>;
    fn quorum_state(&self) -> QuorumState;
    async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError>;
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
    async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError> {
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
    async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError>;
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
    async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError> {
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
    /// Voter map `(id, "<host>:<port>")` — host carried verbatim; the dialer
    /// re-resolves it per connect so a rejoining peer's new pod IP is reached.
    pub(crate) voters: Vec<(NodeId, String)>,
    pub(crate) dialer: Arc<dyn OutboundDialer>,
    pub(crate) client_id: String,
    pub(crate) leader: watch::Receiver<Option<NodeId>>,
}

impl QuorumForwarder {
    async fn try_submit(
        &self,
        target: NodeId,
        addr: &str,
        body: &[u8],
    ) -> Result<crabka_raft::CrabkaSubmitChangeResponse, RaftError> {
        let opts = crabka_client_core::ConnectionOptions {
            client_id: self.client_id.clone(),
            ..crabka_client_core::ConnectionOptions::default()
        };
        let conn = self
            .dialer
            .dial(target, addr, opts)
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

/// Order the voters to try when forwarding `submit_change`: the hinted leader
/// first (when known and present in the set), then every OTHER voter as a
/// fallback. Pure so the ordering — especially the "every voter except the
/// hint" fallback — is unit-tested without standing up a live quorum.
fn build_forward_order(voters: &[(NodeId, String)], hint: Option<NodeId>) -> Vec<(NodeId, String)> {
    let mut order: Vec<(NodeId, String)> = Vec::new();
    if let Some(l) = hint
        && let Some(t) = voters.iter().find(|(id, _)| *id == l)
    {
        order.push(t.clone());
    }
    for v in voters {
        if Some(v.0) != hint {
            order.push(v.clone());
        }
    }
    order
}

#[async_trait::async_trait]
impl MetadataWriter for QuorumForwarder {
    async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError> {
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
        let order = build_forward_order(&self.voters, hint);

        let mut last_err = RaftError::NotLeader {
            current_leader: hint,
        };
        for (target, addr) in order {
            match self.try_submit(target, &addr, &body).await {
                Ok(resp) if resp.error_code == 0 => {
                    return <serde_wincode::SerdeCompat<SubmitChangeResult> as wincode::Deserialize>::deserialize(
                        &resp.result,
                    )
                    .map_err(RaftError::from);
                }
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
                            .then(|| NodeId(u64::try_from(resp.leader_hint).unwrap_or(0))),
                    };
                }
                Err(e) => last_err = e,
            }
        }
        Err(last_err)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        net::SocketAddr,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use bytes::{Bytes, BytesMut};
    use crabka_metadata::{MetadataRecord, TopicRecord};
    use crabka_protocol::{
        Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
        },
    };
    use crabka_raft::{
        BootstrapMode, Controller, ControllerConfig, Node, NodeId, OutboundDialer, RaftError,
        SnapshotRange, SubmitChangeResult,
    };
    use tempfile::TempDir;
    use tokio::sync::watch;
    use uuid::Uuid;

    use super::{
        MetadataSource, MetadataWriter, ObserverSource, QuorumForwarder, build_forward_order,
    };

    fn voters() -> Vec<(crabka_raft::NodeId, String)> {
        vec![
            (crabka_audit::NodeId(1), "h1:9093".to_string()),
            (crabka_audit::NodeId(2), "h2:9093".to_string()),
            (crabka_audit::NodeId(3), "h3:9093".to_string()),
        ]
    }

    fn topic_record(name: &str) -> MetadataRecord {
        MetadataRecord::V1Topic(TopicRecord {
            name: name.into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        })
    }

    fn api_versions_response_v0() -> Vec<u8> {
        let resp = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![ApiVersion {
                api_key: api_versions_request::API_KEY,
                min_version: 0,
                max_version: 3,
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    fn submit_change_response_body(error_code: i16, leader_hint: i64) -> Vec<u8> {
        let mut out = vec![0u8]; // flexible ResponseHeader v1 tagged-fields
        let result =
            <serde_wincode::SerdeCompat<SubmitChangeResult> as wincode::Serialize>::serialize(
                &SubmitChangeResult::default(),
            )
            .expect("serialize submit result");
        crabka_raft::CrabkaSubmitChangeResponse {
            error_code,
            leader_hint,
            result: Bytes::from(result),
        }
        .encode_v0(&mut out)
        .unwrap();
        out
    }

    #[derive(Clone)]
    struct RecordingDialer {
        client_ids: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl OutboundDialer for RecordingDialer {
        async fn dial(
            &self,
            target: NodeId,
            addr: &str,
            options: crabka_client_core::ConnectionOptions,
        ) -> Result<crabka_client_core::Connection, crabka_client_core::ClientError> {
            self.client_ids
                .lock()
                .unwrap()
                .push(options.client_id.clone());
            crabka_raft::PlaintextDialer
                .dial(target, addr, options)
                .await
        }
    }

    fn forwarder(
        addr: SocketAddr,
        client_ids: Arc<Mutex<Vec<String>>>,
        leader_hint: Option<NodeId>,
    ) -> QuorumForwarder {
        let (_leader_tx, leader_rx) = watch::channel(leader_hint);
        QuorumForwarder {
            voters: vec![(NodeId(1), addr.to_string())],
            dialer: Arc::new(RecordingDialer { client_ids }),
            client_id: "forwarder-client".into(),
            leader: leader_rx,
        }
    }

    struct RecordingWriter {
        calls: Mutex<Vec<Vec<MetadataRecord>>>,
    }

    #[async_trait::async_trait]
    impl MetadataWriter for RecordingWriter {
        async fn submit_change(
            &self,
            records: Vec<MetadataRecord>,
        ) -> Result<SubmitChangeResult, RaftError> {
            self.calls.lock().unwrap().push(records);
            Ok(SubmitChangeResult::default())
        }
    }

    fn not_leader_none<T>(result: &Result<T, RaftError>) {
        assert!(matches!(
            result,
            Err(RaftError::NotLeader {
                current_leader: None
            })
        ));
    }

    async fn wait_for_controller_leader(ctrl: &crabka_raft::ControllerHandle) {
        let mut leader_rx = ctrl.watch_leader();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while leader_rx.borrow().is_none() {
                leader_rx.changed().await.unwrap();
            }
        })
        .await
        .expect("controller should elect itself");
    }

    async fn bind_eventually(addr: SocketAddr) -> tokio::net::TcpListener {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
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

    #[test]
    fn forward_order_hinted_leader_first_then_every_other_voter() {
        // Hint = 2 → try the leader first, then the OTHER voters as fallback.
        // A flipped `Some(v.0) != hint` (i.e. `== hint`) would re-push only the
        // hinted voter and drop the fallbacks, leaving no peer to retry when the
        // hint is stale.
        let order = build_forward_order(&voters(), Some(crabka_audit::NodeId(2)));
        assert!(
            order
                == vec![
                    (crabka_raft::NodeId(2), "h2:9093".to_string()),
                    (crabka_raft::NodeId(1), "h1:9093".to_string()),
                    (crabka_raft::NodeId(3), "h3:9093".to_string()),
                ]
        );
    }

    #[test]
    fn forward_order_no_hint_tries_all_voters() {
        // No leader hint → fall back to trying every voter. A flipped predicate
        // (`== None`) would push nothing, so the forward could reach no peer.
        let order = build_forward_order(&voters(), None);
        assert!(order == voters());
    }

    #[test]
    fn forward_order_unknown_hint_still_tries_all_voters() {
        // Hint names a voter not in the set → no leader-first entry, but every
        // voter is still tried (hint 9 != each id).
        let order = build_forward_order(&voters(), Some(crabka_audit::NodeId(9)));
        assert!(order == voters());
    }

    #[tokio::test]
    async fn controller_handle_metadata_source_forwards_snapshot_reconfig_and_cancel() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("controller");
        wait_for_controller_leader(&ctrl).await;
        let source: &dyn MetadataSource = &ctrl;

        assert!(matches!(
            source.add_learner(NodeId(2), Node::default()).await,
            Err(RaftError::Unsupported(_))
        ));
        source
            .submit_change(vec![topic_record("snapshot-topic")])
            .await
            .expect("submit metadata");
        source.trigger_snapshot().await.expect("snapshot");
        assert!(matches!(
            source.read_snapshot_range(0, 1),
            SnapshotRange::Slice(_)
        ));

        let addr = source.controller_bound_addr();
        source.cancel().await;
        let listener = bind_eventually(addr).await;
        drop(listener);
    }

    #[tokio::test]
    async fn observer_source_uses_observer_writer_and_denies_controller_only_ops() {
        let cluster_id = Uuid::new_v4();
        let observer = crate::metadata_observer::MetadataObserver::start(
            crate::metadata_observer::ObserverConfig {
                voters: vec![],
                dialer: Arc::new(crabka_raft::PlaintextDialer),
                client_id: "observer-source-test".into(),
                cluster_id,
                max_bytes: 1_048_576,
                poll_interval: std::time::Duration::from_mins(1),
                sleeper: Arc::new(qubit_clock::sleep::SystemSleeper::new()),
            },
        );
        let writer = Arc::new(RecordingWriter {
            calls: Mutex::new(Vec::new()),
        });
        let source = ObserverSource::new(observer.clone(), writer.clone());

        assert!(source.current_image().cluster_id() == cluster_id);
        source
            .submit_change(vec![topic_record("forwarded-topic")])
            .await
            .expect("submit via writer");
        {
            let calls = writer.calls.lock().unwrap();
            assert!(calls.len() == 1);
            assert!(
                matches!(&calls[0][0], MetadataRecord::V1Topic(t) if t.name == "forwarded-topic")
            );
        }

        not_leader_none(&source.change_membership(BTreeSet::new()).await);
        not_leader_none(&source.add_learner(NodeId(2), Node::default()).await);
        not_leader_none(&source.trigger_snapshot().await);
        source.cancel().await;
        assert!(observer.task_drained_for_test().await);
    }

    #[tokio::test]
    async fn quorum_forwarder_applied_response_returns_ok_and_sends_client_id() {
        let submit_requests = Arc::new(AtomicUsize::new(0));
        let submit_requests_for_mock = submit_requests.clone();
        let mock =
            crabka_client_core::MockBroker::start(move |api_key, _version, _corr_id, _body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(api_versions_response_v0());
                }
                if api_key == crabka_raft::API_KEY_SUBMIT_CHANGE {
                    submit_requests_for_mock.fetch_add(1, Ordering::SeqCst);
                    return Some(submit_change_response_body(0, -1));
                }
                None
            })
            .await;
        let client_ids = Arc::new(Mutex::new(Vec::new()));
        let forwarder = forwarder(mock.addr, client_ids.clone(), Some(NodeId(1)));

        forwarder
            .submit_change(vec![topic_record("applied")])
            .await
            .expect("applied");

        assert!(submit_requests.load(Ordering::SeqCst) == 1);
        assert!(
            client_ids
                .lock()
                .unwrap()
                .iter()
                .any(|id| id == "forwarder-client")
        );
        mock.stop();
    }

    #[tokio::test]
    async fn quorum_forwarder_error_code_two_maps_to_topic_exists() {
        let mock =
            crabka_client_core::MockBroker::start(move |api_key, _version, _corr_id, _body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(api_versions_response_v0());
                }
                if api_key == crabka_raft::API_KEY_SUBMIT_CHANGE {
                    return Some(submit_change_response_body(2, -1));
                }
                None
            })
            .await;
        let forwarder = forwarder(mock.addr, Arc::new(Mutex::new(Vec::new())), Some(NodeId(1)));

        let err = forwarder
            .submit_change(vec![topic_record("already-exists")])
            .await
            .expect_err("metadata error");

        assert!(matches!(
            err,
            RaftError::Metadata(crabka_metadata::MetadataError::TopicExists(_))
        ));
        mock.stop();
    }

    #[tokio::test]
    async fn quorum_forwarder_not_leader_response_preserves_positive_hint() {
        let mock =
            crabka_client_core::MockBroker::start(move |api_key, _version, _corr_id, _body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(api_versions_response_v0());
                }
                if api_key == crabka_raft::API_KEY_SUBMIT_CHANGE {
                    return Some(submit_change_response_body(1, 7));
                }
                None
            })
            .await;
        let forwarder = forwarder(mock.addr, Arc::new(Mutex::new(Vec::new())), Some(NodeId(1)));

        let err = forwarder
            .submit_change(vec![topic_record("redirect")])
            .await
            .expect_err("not leader");

        assert!(matches!(
            err,
            RaftError::NotLeader {
                current_leader: Some(NodeId(7))
            }
        ));
        mock.stop();
    }

    #[tokio::test]
    async fn quorum_forwarder_negative_leader_hint_is_unknown() {
        let mock =
            crabka_client_core::MockBroker::start(move |api_key, _version, _corr_id, _body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(api_versions_response_v0());
                }
                if api_key == crabka_raft::API_KEY_SUBMIT_CHANGE {
                    return Some(submit_change_response_body(3, -1));
                }
                None
            })
            .await;
        let forwarder = forwarder(mock.addr, Arc::new(Mutex::new(Vec::new())), Some(NodeId(1)));

        let err = forwarder
            .submit_change(vec![topic_record("unknown-leader")])
            .await
            .expect_err("not leader");

        assert!(matches!(
            err,
            RaftError::NotLeader {
                current_leader: None
            }
        ));
        mock.stop();
    }
}
