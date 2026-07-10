//! `BrokerHeartbeat` (`api_key=63`). KIP-500 controller-side heartbeat handler.
//!
//! Only the openraft leader handles heartbeats. Non-leaders return
//! `NOT_CONTROLLER` so the broker client can redirect.

use std::sync::Arc;

use bytes::Bytes;
use crabka_metadata::{AclOperation, MetadataImage, MetadataRecord, ResourceType};
use crabka_protocol::{
    Decode,
    owned::{
        broker_heartbeat_request::BrokerHeartbeatRequest,
        broker_heartbeat_response::BrokerHeartbeatResponse,
    },
};
use crabka_raft::NodeId;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
    heartbeat::controller_state::ControllerLivenessState,
    leader_election::select_replacement_leader_for_shutdown,
};

#[tracing::instrument(
    name = "handle_broker_heartbeat",
    level = "info",
    skip_all,
    fields(api = "BrokerHeartbeat", version, req_bytes = req_bytes.len()),
    err,
)]
// cargo-mutants: controller RPC orchestration over live broker state; helper
// functions and handler tests cover the deterministic decision points.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let liveness = broker.liveness.clone();
    let controller = broker.controller.clone();
    let node_id = broker.config.node_id;
    let metrics = broker.metrics.clone();
    let recovery = broker.unclean_recovery.clone();
    // Check leadership: this broker is the controller leader iff the
    // watch channel reports a leader id equal to our own node_id.
    let is_leader = controller
        .watch_leader()
        .borrow()
        .is_some_and(|n| is_controller_leader(Some(n), node_id));
    {
        let mut cur: &[u8] = req_bytes;
        let req = BrokerHeartbeatRequest::decode(&mut cur, version)?;

        // ── ACL preamble ────────────────────────────────────────────
        // Inter-broker control-plane RPC: `ClusterAction` on
        // `Cluster("kafka-cluster")`. On Deny → whole-response
        // `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
        {
            let image = controller.current_image();
            if cluster_action_denied(
                broker.config.authorizer.as_ref(),
                &image,
                ctx.principal,
                ctx.peer,
            ) {
                return denied_response(version);
            }
        }

        // Only the openraft leader handles heartbeats. NOT_CONTROLLER
        // tells the broker client to redirect.
        if !is_leader {
            return encode_response(version, &not_controller_response());
        }

        let broker_id_u64 = u64::try_from(req.broker_id).unwrap_or(0);

        // Record the heartbeat. If it's a revival, the liveness ticker
        // will pick up the transition next cycle and the heartbeat-side
        // wakeup is a no-op; the controlled-shutdown path handles
        // explicit on-revival handling.
        let _transition = liveness.record_heartbeat(broker_id_u64).await;

        // Track want_shut_down state and drive leader transfer.
        liveness
            .set_wants_shutdown(broker_id_u64, req.want_shut_down)
            .await;

        let should_shut_down = if req.want_shut_down {
            drain_leaderships_for_shutdown(&controller, &liveness, NodeId(broker_id_u64)).await?
        } else {
            false
        };

        // KIP-112: a broker that reports offline log dirs is still alive, so
        // the liveness `alive→dead` failover never fires. Map the reported
        // offline dir UUIDs to the reporting broker's affected partitions and
        // fail them over (elect from surviving alive ISR, drop the offline
        // replica). Only the controller leader reaches here (NOT_CONTROLLER
        // early-return above), and it's idempotent across repeated heartbeats.
        //
        // Validate the reporting broker id independently of `broker_id_u64`
        // (which falls back to 0 for the liveness path): failing over the
        // wrong broker on a malformed negative id would be harmful.
        if has_offline_log_dirs(&req)
            && let Ok(reporting_broker) = u64::try_from(req.broker_id)
        {
            let offline: std::collections::HashSet<uuid::Uuid> = req
                .offline_log_dirs
                .iter()
                .map(|u| uuid::Uuid::from_bytes(u.0))
                .collect();
            let recoveries = failover_offline_dirs(
                &controller,
                NodeId(reporting_broker),
                &offline,
                &liveness,
                &metrics,
            )
            .await;
            // Fire-and-forget: enqueue logs internally if the recovery manager is gone.
            for (topic, partition, strategy) in recoveries {
                recovery
                    .enqueue(crate::unclean_recovery::RecoveryJob {
                        topic,
                        partition,
                        strategy,
                        reply: None,
                    })
                    .await;
            }
        }

        encode_response(version, &success_response(should_shut_down))
    }
}

fn is_controller_leader(leader: Option<NodeId>, node_id: NodeId) -> bool {
    leader == Some(node_id)
}

fn has_offline_log_dirs(req: &BrokerHeartbeatRequest) -> bool {
    !req.offline_log_dirs.is_empty()
}

fn not_controller_response() -> BrokerHeartbeatResponse {
    BrokerHeartbeatResponse {
        error_code: codes::NOT_CONTROLLER,
        ..Default::default()
    }
}

fn success_response(should_shut_down: bool) -> BrokerHeartbeatResponse {
    BrokerHeartbeatResponse {
        is_caught_up: true,
        is_fenced: false,
        should_shut_down,
        ..Default::default()
    }
}

fn denied_response_body() -> BrokerHeartbeatResponse {
    BrokerHeartbeatResponse {
        error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
        ..Default::default()
    }
}

fn encode_response(version: i16, resp: &BrokerHeartbeatResponse) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(resp, version)
}

/// `ClusterAction` on `Cluster("kafka-cluster")` gate. Returns `true`
/// when the principal is denied (inter-broker control-plane RPC).
fn cluster_action_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &MetadataImage,
    principal: &crabka_security::Principal,
    host: &std::net::SocketAddr,
) -> bool {
    authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal,
            host,
            resource_type: ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: AclOperation::ClusterAction,
        },
    ) == AuthorizationResult::Deny
}

/// Whole-response `CLUSTER_AUTHORIZATION_FAILED (31)` response built on Deny.
fn denied_response(version: i16) -> Result<Bytes, BrokerError> {
    encode_response(version, &denied_response_body())
}

/// Run the KIP-112 offline-dir failover for `broker`'s reported offline dirs:
/// compute the partition changes, submit them, and return any offset-aware
/// recovery jobs (KIP-966) for the caller to enqueue. Controller-leader only;
/// the caller gates on leadership. Submit failure is logged, not propagated.
pub(crate) async fn failover_offline_dirs(
    controller: &std::sync::Arc<dyn crate::metadata_source::MetadataSource>,
    broker: crabka_raft::NodeId,
    offline: &std::collections::HashSet<uuid::Uuid>,
    liveness: &crate::heartbeat::controller_state::ControllerLivenessState,
    metrics: &crate::metrics::BrokerMetrics,
) -> Vec<(String, i32, crate::config_keys::RecoveryStrategy)> {
    let image = controller.current_image();
    let plan = crate::leader_election::compute_offline_dir_failover_changes(
        &image, broker, offline, liveness, metrics,
    )
    .await;
    if !plan.changes.is_empty()
        && let Err(e) = controller.submit_change(plan.changes).await
    {
        tracing::warn!(error = %e, "offline-dir failover submit_change failed");
    }
    plan.recoveries
}

/// Scan partitions where `shutting_down` is currently leader, submit a
/// replacement-leader record for each one where a live ISR alternative
/// exists, and return `true` once every *transferable* partition has been
/// re-led (i.e. the broker is safe to shut down). Partitions with no other
/// live replica — single-replica internal topics like `__consumer_offsets`
/// or `__crabka_audit` — cannot transfer leadership anywhere and are not
/// counted; counting them would block controlled shutdown forever. Returns
/// `false` while transferable leadership is still moving; the client retries
/// on the next heartbeat tick.
///
/// Pure-by-construction: `MetadataImage` is read-only, the controller
/// is the only side-effect channel. On submit failure we log and
/// return `Ok(false)` so the client will retry rather than crash.
async fn drain_leaderships_for_shutdown(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    liveness: &Arc<ControllerLivenessState>,
    shutting_down: NodeId,
) -> Result<bool, BrokerError> {
    let image: Arc<MetadataImage> = controller.current_image();

    let mut leader_count: usize = 0;
    let mut changes: Vec<MetadataRecord> = Vec::new();
    for topic in image.topics() {
        for pr in image.partitions_of(&topic.name) {
            if pr.leader != shutting_down {
                continue;
            }
            if let Ok(new_pr) = select_replacement_leader_for_shutdown(
                &image,
                liveness,
                &pr.topic,
                pr.partition,
                shutting_down,
            )
            .await
            {
                // A live replica can take over: transfer leadership and keep
                // the broker waiting until the new leadership is visible.
                leader_count += 1;
                changes.push(MetadataRecord::V1Partition(new_pr));
            }
            // Else: no live alternative ISR member to transfer to — e.g. the
            // single-replica internal topics (__consumer_offsets,
            // __transaction_state, __crabka_audit), of which every broker
            // leads its own copy. Leadership cannot move anywhere, so counting
            // it would block controlled shutdown forever; and the broker is
            // stopping regardless (the partition has no other replica to serve
            // it either way). Do NOT count it toward the drain gate.
        }
    }

    if !changes.is_empty()
        && let Err(e) = controller.submit_change(changes).await
    {
        tracing::warn!(error = %e, "controlled shutdown: submit_change failed");
        return Ok(false);
    }

    // `leader_count` was computed against the pre-submit image and counts
    // only transferable partitions. The submit above (if any) only takes
    // effect on a subsequent heartbeat once the new image is visible — so we
    // report `should_shut_down=true` only when this broker was already not
    // leading any transferable partition.
    Ok(leader_count == 0)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        net::SocketAddr,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use assert2::assert;
    use bytes::BytesMut;
    use crabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord};
    use crabka_protocol::{Encode, primitives::uuid::Uuid as ProtocolUuid};
    use crabka_raft::{
        AddVoter, Node, QuorumState, RaftError, ReconfigOutcome, RemoveVoter, SnapshotRange,
        UpdateVoter,
    };
    use tokio::sync::watch;
    use uuid::Uuid;

    use super::*;

    /// Minimal `MetadataSource` that captures `submit_change` calls for
    /// inspection. Returns a fixed image; `watch_leader` always reports
    /// `Some(1)` (this node is the leader).
    struct MockSource {
        leader_rx: watch::Receiver<Option<NodeId>>,
        _leader_tx: watch::Sender<Option<NodeId>>,
        image: Arc<MetadataImage>,
        captured: Arc<Mutex<Vec<MetadataRecord>>>,
    }

    impl MockSource {
        fn new(image: MetadataImage) -> (Self, Arc<Mutex<Vec<MetadataRecord>>>) {
            let (tx, rx) = watch::channel(Some(NodeId(1)));
            let captured = Arc::new(Mutex::new(Vec::new()));
            let source = Self {
                leader_rx: rx,
                _leader_tx: tx,
                image: Arc::new(image),
                captured: captured.clone(),
            };
            (source, captured)
        }
    }

    #[async_trait::async_trait]
    impl crate::metadata_source::MetadataSource for MockSource {
        fn current_image(&self) -> Arc<MetadataImage> {
            self.image.clone()
        }
        fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
            let (_tx, rx) = watch::channel(self.image.clone());
            rx
        }
        fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
            self.leader_rx.clone()
        }
        fn quorum_state(&self) -> QuorumState {
            QuorumState {
                current_term: 0,
                last_applied_index: 0,
                current_leader: *self.leader_rx.borrow(),
                voters: Vec::new(),
                voter_nodes: std::collections::BTreeMap::new(),
                per_voter_matched_index: std::collections::BTreeMap::new(),
            }
        }
        async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), RaftError> {
            self.captured.lock().unwrap().extend(records);
            Ok(())
        }
        async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
            panic!("broker heartbeat MockSource does not support membership changes")
        }
        async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
            panic!("broker heartbeat MockSource does not support learner changes")
        }
        fn controller_bound_addr(&self) -> SocketAddr {
            SocketAddr::from(([0, 0, 0, 0], 0))
        }
        fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
            SnapshotRange::NoSnapshot
        }
        async fn trigger_snapshot(&self) -> Result<(), RaftError> {
            panic!("broker heartbeat MockSource does not support snapshots")
        }
        async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("broker heartbeat MockSource does not support adding voters")
        }
        async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("broker heartbeat MockSource does not support removing voters")
        }
        async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("broker heartbeat MockSource does not support updating voters")
        }
        async fn cancel(&self) {}
    }

    fn image_with_dir_partition(
        leader: NodeId,
        replicas: &[NodeId],
        isr: &[NodeId],
        dirs: &[Uuid],
    ) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: i16::try_from(replicas.len()).unwrap(),
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader,
            replicas: replicas.to_vec(),
            isr: isr.to_vec(),
            leader_epoch: crabka_metadata::LeaderEpoch(5),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: dirs.to_vec(),
            partition_epoch: 0,
        }));
        img
    }

    async fn liveness_with(alive: &[NodeId]) -> Arc<ControllerLivenessState> {
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for &n in alive {
            l.record_heartbeat(n.0).await;
        }
        Arc::new(l)
    }

    fn request(offline_log_dirs: Vec<uuid::Uuid>) -> Bytes {
        let req = BrokerHeartbeatRequest {
            broker_id: 1,
            broker_epoch: -1,
            current_metadata_offset: 0,
            want_fence: false,
            want_shut_down: false,
            offline_log_dirs: offline_log_dirs
                .into_iter()
                .map(|u| ProtocolUuid(u.into_bytes()))
                .collect(),
            cordoned_log_dirs: None,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(
            req.encoded_len(crabka_protocol::owned::broker_heartbeat_request::MAX_VERSION),
        );
        req.encode(
            &mut buf,
            crabka_protocol::owned::broker_heartbeat_request::MAX_VERSION,
        )
        .expect("encode BrokerHeartbeatRequest");
        buf.freeze()
    }

    crate::test_support::response_helpers!(
        BrokerHeartbeatResponse,
        client_id = "broker-heartbeat-test"
    );

    use crate::test_support::start_broker_with_authorizer as start_broker;

    async fn wait_for_leader(broker: &Broker) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if broker
                .controller
                .watch_leader()
                .borrow()
                .is_some_and(|n| n == broker.config.node_id)
            {
                return;
            }
            assert!(
                std::time::Instant::now() <= deadline,
                "broker did not become controller leader"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    #[test]
    fn leader_predicate_matches_current_node_only() {
        let cases = [
            (Some(NodeId(1)), true),
            (Some(NodeId(2)), false),
            (None, false),
        ];
        for (leader, want) in cases {
            assert!(
                is_controller_leader(leader, NodeId(1)) == want,
                "leader {leader:?}"
            );
        }
    }

    #[test]
    fn heartbeat_response_builders_preserve_non_default_fields() {
        let expected_not_controller = BrokerHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: codes::NOT_CONTROLLER,
            is_caught_up: false,
            is_fenced: true,
            should_shut_down: false,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(not_controller_response() == expected_not_controller);

        let expected_success = BrokerHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            is_caught_up: true,
            is_fenced: false,
            should_shut_down: true,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(success_response(true) == expected_success);

        let expected_denied = BrokerHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            is_caught_up: false,
            is_fenced: true,
            should_shut_down: false,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(denied_response_body() == expected_denied);
    }

    #[test]
    fn offline_dir_gate_tracks_reported_directories() {
        let empty = BrokerHeartbeatRequest {
            offline_log_dirs: vec![],
            ..Default::default()
        };
        assert!(!has_offline_log_dirs(&empty));

        let reported = BrokerHeartbeatRequest {
            offline_log_dirs: vec![ProtocolUuid(uuid::Uuid::from_u128(0xD1).into_bytes())],
            ..Default::default()
        };
        assert!(has_offline_log_dirs(&reported));
    }

    #[tokio::test]
    async fn failover_offline_dirs_submits_change_for_offline_leader() {
        let bad = Uuid::from_u128(0xBAD);
        let good = Uuid::from_u128(0x600D);
        // leader=1, replicas=[1,2], isr=[1,2]; broker 1's dir is `bad`.
        let img = image_with_dir_partition(
            crabka_audit::NodeId(1),
            &[crabka_audit::NodeId(1), crabka_audit::NodeId(2)],
            &[crabka_audit::NodeId(1), crabka_audit::NodeId(2)],
            &[bad, good],
        );
        let (source, captured) = MockSource::new(img);
        let controller: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(source);
        let liveness = liveness_with(&[crabka_audit::NodeId(1), crabka_audit::NodeId(2)]).await;
        let metrics = crate::metrics::BrokerMetrics::new();
        let offline: std::collections::HashSet<Uuid> = [bad].into_iter().collect();

        let recoveries = failover_offline_dirs(
            &controller,
            crabka_audit::NodeId(1),
            &offline,
            &liveness,
            &metrics,
        )
        .await;

        // Exactly one change must have been submitted (the new leader record):
        // broker 2 is elected (broker 1's dir is offline), the offline replica
        // is dropped from the ISR, and both epochs are bumped.
        let changes = captured.lock().unwrap();
        let expected_changes = vec![MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: crabka_audit::NodeId(2),
            replicas: vec![crabka_audit::NodeId(1), crabka_audit::NodeId(2)],
            isr: vec![crabka_audit::NodeId(2)],
            leader_epoch: crabka_metadata::LeaderEpoch(6),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![bad, good],
            partition_epoch: 1,
        })];
        assert!(*changes == expected_changes);
        // No unclean recovery needed (broker 2 is alive and in ISR).
        assert!(recoveries == vec![]);
    }

    #[tokio::test]
    async fn failover_offline_dirs_no_change_when_dir_healthy() {
        let bad = Uuid::from_u128(0xBAD);
        let good = Uuid::from_u128(0x600D);
        // Both replicas are on `good` dir; reporting `bad` as offline is a no-op.
        let img = image_with_dir_partition(
            crabka_audit::NodeId(1),
            &[crabka_audit::NodeId(1), crabka_audit::NodeId(2)],
            &[crabka_audit::NodeId(1), crabka_audit::NodeId(2)],
            &[good, good],
        );
        let (source, captured) = MockSource::new(img);
        let controller: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(source);
        let liveness = liveness_with(&[crabka_audit::NodeId(1), crabka_audit::NodeId(2)]).await;
        let metrics = crate::metrics::BrokerMetrics::new();
        let offline: std::collections::HashSet<Uuid> = [bad].into_iter().collect();

        let recoveries = failover_offline_dirs(
            &controller,
            crabka_audit::NodeId(1),
            &offline,
            &liveness,
            &metrics,
        )
        .await;

        // No change submitted and no recovery needed.
        let changes = captured.lock().unwrap();
        assert!(changes.is_empty());
        assert!(recoveries.is_empty());
    }

    #[tokio::test]
    async fn single_replica_partition_does_not_block_controlled_shutdown() {
        // Broker 1 leads an RF=1 partition (replicas=[1], isr=[1]) — exactly
        // the shape of the broker-affinity internal topics __consumer_offsets
        // / __crabka_audit. There is nowhere to transfer leadership, so the
        // drain gate must still report "safe to shut down" (regression: this
        // used to count the partition forever and time out controlled
        // shutdown at 30s).
        let img = image_with_dir_partition(
            crabka_audit::NodeId(1),
            &[crabka_audit::NodeId(1)],
            &[crabka_audit::NodeId(1)],
            &[Uuid::nil()],
        );
        let (source, captured) = MockSource::new(img);
        let controller: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(source);
        let liveness = liveness_with(&[crabka_audit::NodeId(1)]).await;

        let drained =
            drain_leaderships_for_shutdown(&controller, &liveness, crabka_audit::NodeId(1))
                .await
                .unwrap();

        assert!(drained); // untransferable partition is not counted
        assert!(captured.lock().unwrap().is_empty()); // nothing to transfer
    }

    #[tokio::test]
    async fn transferable_partition_blocks_until_leadership_moves() {
        // Broker 1 leads an RF=2 partition with broker 2 alive in ISR: it can
        // and must transfer, so the broker is not yet safe to shut down.
        let img = image_with_dir_partition(
            crabka_audit::NodeId(1),
            &[crabka_audit::NodeId(1), crabka_audit::NodeId(2)],
            &[crabka_audit::NodeId(1), crabka_audit::NodeId(2)],
            &[Uuid::nil(), Uuid::nil()],
        );
        let (source, captured) = MockSource::new(img);
        let controller: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(source);
        let liveness = liveness_with(&[crabka_audit::NodeId(1), crabka_audit::NodeId(2)]).await;

        let drained =
            drain_leaderships_for_shutdown(&controller, &liveness, crabka_audit::NodeId(1))
                .await
                .unwrap();

        assert!(!drained); // still leading a transferable partition pre-submit
        let changes = captured.lock().unwrap();
        assert!(changes.len() == 1);
        let MetadataRecord::V1Partition(pr) = &changes[0] else {
            panic!("expected V1Partition change")
        };
        assert!(pr.leader == crabka_audit::NodeId(2)); // leadership handed to the live ISR replica
    }

    /// Empty ACLs + no super-users → every principal is denied
    /// `ClusterAction`, so the denied response carries
    /// `CLUSTER_AUTHORIZATION_FAILED`.
    #[test]
    fn cluster_action_denied_yields_cluster_authorization_failed() {
        use crabka_protocol::owned::broker_heartbeat_response::{self, BrokerHeartbeatResponse};

        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = MetadataImage::new(uuid::Uuid::nil());
        let principal = crabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));

        assert!(cluster_action_denied(
            &authorizer,
            &image,
            &principal,
            &peer
        ));

        let bytes = denied_response(broker_heartbeat_response::MAX_VERSION).expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp =
            BrokerHeartbeatResponse::decode(&mut cur, broker_heartbeat_response::MAX_VERSION)
                .unwrap();
        assert!(resp.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
        assert!(resp.is_fenced);
    }

    #[test]
    fn cluster_action_allowed_by_allow_all_authorizer() {
        let image = MetadataImage::new(uuid::Uuid::nil());
        let principal = crabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));

        assert!(!cluster_action_denied(
            &crate::authorizer::AllowAllAuthorizer,
            &image,
            &principal,
            &peer
        ));
    }

    #[tokio::test]
    async fn handle_leader_success_preserves_response_shape() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        let principal = crabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = test_context(&principal, &peer);
        let version = crabka_protocol::owned::broker_heartbeat_request::MAX_VERSION;
        let req = request(vec![]);

        let bytes = handle(&broker, version, 11, &req, &ctx)
            .await
            .expect("BrokerHeartbeat handler");
        let resp = decode_response(&bytes, version);

        let expected = BrokerHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            is_caught_up: true,
            is_fenced: false,
            should_shut_down: false,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected, "{resp:?}");

        broker_handle.shutdown().await;
    }
}
