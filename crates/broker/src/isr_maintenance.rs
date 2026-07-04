//! Per-leader-partition ISR maintenance. Compares each follower's
//! last-fetch time vs `replica_lag_time_max_ms` and proposes
//! `AlterPartition` shrink/expand to the controller leader.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use crabka_protocol::owned::alter_partition_request::AlterPartitionRequest;
use crabka_raft::NodeId;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::partition::Partition;
use crate::partition_registry::PartitionRegistry;

/// Cadence of the ISR maintenance scan: every leader partition's follower
/// lag is re-evaluated once per tick.
const ISR_SCAN_INTERVAL: Duration = Duration::from_secs(1);

/// KIP-903 sentinel for an unknown broker epoch. Stamped when the metadata
/// image has no epoch for a broker; tells the controller to skip the
/// stale-replica epoch fence for that entry.
const UNKNOWN_BROKER_EPOCH: i64 = -1;

pub(crate) struct Config {
    pub node_id: NodeId,
    pub partitions: Arc<PartitionRegistry>,
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
    pub replica_lag_time_max: Duration,
    pub broker_id: i32,
    pub shutdown: CancellationToken,
    /// Bumped on each proposed shrink / expand.
    pub metrics: crate::metrics::BrokerMetrics,
}

pub(crate) async fn run(cfg: Config) {
    let mut tick = tokio::time::interval(ISR_SCAN_INTERVAL);
    // Reused across ticks to avoid re-allocating the snapshot Vec each second.
    // Holds cheap `Arc<Partition>` clones (no String allocation, no second
    // registry lookup). Cleared and refilled each tick.
    let mut snapshot: Vec<Arc<Partition>> = Vec::new();
    loop {
        tokio::select! {
            _ = tick.tick() => {},
            () = cfg.shutdown.cancelled() => return,
        }
        // Snapshot the partition values as cheap Arc clones in a single
        // iteration, then iterate the owned `Vec` so we never hold a shard
        // guard across a yield point.
        snapshot.clear();
        snapshot.extend(cfg.partitions.arcs());
        for part in snapshot.drain(..) {
            if part
                .current_leader
                .load(std::sync::atomic::Ordering::Acquire)
                != cfg.node_id
            {
                continue;
            }
            let Some(proposal) = compute_proposal(&part, cfg.replica_lag_time_max).await else {
                continue;
            };
            // Classify the proposal as shrink/expand using the ISRs captured
            // inside `compute_proposal`'s single lock scope. `compute_proposal`
            // already filtered for "actually changed", so at least one of these
            // bumps fires. Reusing its captured `prev_isr` avoids a second
            // `replica_state` lock and closes the TOCTOU window where the ISR
            // could change between the two acquisitions.
            let prev_isr: std::collections::HashSet<NodeId> =
                proposal.prev_isr.iter().copied().collect();
            let next_isr: std::collections::HashSet<NodeId> =
                proposal.new_isr.iter().copied().collect();
            if prev_isr.difference(&next_isr).next().is_some() {
                cfg.metrics.isr_shrinks_total.inc();
            }
            if next_isr.difference(&prev_isr).next().is_some() {
                cfg.metrics.isr_expands_total.inc();
            }
            if let Err(e) = send_alter_partition(
                &cfg.controller,
                cfg.broker_id,
                &part.topic,
                part.partition_id,
                proposal.new_isr,
                proposal.leader_epoch,
            )
            .await
            {
                warn!(topic = %part.topic, partition = part.partition_id, error = %e,
                    "AlterPartition propose failed");
            }
        }
    }
}

/// A computed ISR change proposal. All fields are captured within
/// `compute_proposal`'s single `replica_state` lock scope so the caller
/// can classify shrink/expand and submit the proposal without re-locking
/// (and without a TOCTOU window where the ISR shifts between locks).
#[derive(Debug, PartialEq)]
struct Proposal {
    /// The pre-proposal ISR (sorted), used by the caller for shrink/expand
    /// metric classification.
    prev_isr: Vec<NodeId>,
    /// The proposed new ISR (sorted). Guaranteed `!= prev_isr`.
    new_isr: Vec<NodeId>,
    /// Leader epoch to stamp on the `AlterPartition` request.
    leader_epoch: i32,
}

/// Returns `Some(Proposal)` if the ISR should change, else `None`.
async fn compute_proposal(part: &Partition, lag_max: Duration) -> Option<Proposal> {
    let st = part.replica_state.lock().await;
    let now = Instant::now();
    // Capture the pre-proposal ISR (sorted) once, inside this lock scope.
    let mut prev_isr: Vec<NodeId> = st.isr.iter().copied().collect();
    prev_isr.sort_unstable();
    let mut new_isr: Vec<NodeId> = prev_isr.clone();
    // Shrink: drop followers lagging > lag_max.
    new_isr.retain(|n| {
        st.per_follower
            .get(n)
            .is_none_or(|stats| now.duration_since(stats.last_fetch) <= lag_max)
    });
    // Expand: add followers in per_follower not in current ISR that have
    // been recently caught up.
    for (n, stats) in &st.per_follower {
        if !st.isr.contains(n)
            && now.duration_since(stats.last_caught_up) <= lag_max
            && !new_isr.contains(n)
        {
            new_isr.push(*n);
        }
    }
    new_isr.sort_unstable();
    let no_change = new_isr == prev_isr;
    if no_change {
        None
    } else {
        Some(Proposal {
            prev_isr,
            new_isr,
            leader_epoch: st.current_leader_epoch,
        })
    }
}

#[tracing::instrument(
    name = "isr_send_alter_partition",
    level = "info",
    skip_all,
    fields(topic = %topic, partition, leader_epoch, new_isr_len = new_isr.len()),
    err,
)]
async fn send_alter_partition(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    broker_id: i32,
    topic: &str,
    partition: i32,
    new_isr: Vec<NodeId>,
    leader_epoch: i32,
) -> Result<(), String> {
    let image = controller.current_image();
    let leader_id = *controller.watch_leader().borrow();
    let targets = alter_partition_targets(&image, leader_id);
    if targets.is_empty() {
        return match leader_id {
            Some(_) => Err("controller leader not in image".into()),
            None => Err("no controller leader".into()),
        };
    }

    let req =
        build_alter_partition_request(&image, broker_id, topic, partition, &new_isr, leader_epoch);
    let mut last_err = String::new();
    for (target_id, addr) in targets {
        match send_alter_partition_to(broker_id, &addr, req.clone()).await {
            Ok(()) => {
                debug!(
                    topic = topic,
                    partition = partition,
                    new_isr_len = new_isr.len(),
                    controller_target = target_id,
                    "AlterPartition proposed"
                );
                return Ok(());
            }
            Err(AlterPartitionSendError::NotController) => {
                last_err = format!("target {target_id} is not controller");
            }
            Err(AlterPartitionSendError::Rejected {
                global_err,
                part_err,
            }) => {
                warn!(
                    topic = topic,
                    partition = partition,
                    new_isr_len = new_isr.len(),
                    controller_target = target_id,
                    global_error_code = global_err,
                    partition_error_code = part_err,
                    "AlterPartition rejected by controller"
                );
                return Err(format!(
                    "AlterPartition rejected: global={global_err} partition={part_err}"
                ));
            }
            Err(AlterPartitionSendError::Transport(e)) => {
                last_err = format!("target {target_id} ({addr}): {e}");
            }
        }
    }
    Err(last_err)
}

fn build_alter_partition_request(
    image: &crabka_metadata::MetadataImage,
    broker_id: i32,
    topic: &str,
    partition: i32,
    new_isr: &[NodeId],
    leader_epoch: i32,
) -> AlterPartitionRequest {
    use crabka_protocol::owned::alter_partition_request::{BrokerState, PartitionData, TopicData};

    // Look up topic_id from the metadata image and convert to the protocol Uuid type.
    let topic_id = {
        let raw: [u8; 16] = image
            .topic(topic)
            .map_or([0u8; 16], |t| *t.topic_id.as_bytes());
        crabka_protocol::primitives::uuid::Uuid(raw)
    };

    // `new_isr` is the v2 field (versions 2 only on the wire).
    // `new_isr_with_epochs` is the v3 field; the client negotiates MAX_VERSION
    // (= 3), so we must populate both so that whichever version is selected
    // carries the correct ISR.  The handler side reads `new_isr_with_epochs`
    // when `new_isr` is empty (i.e. version 3).
    // KIP-903: per-member epochs come from the metadata image; unknown brokers fall back to -1.
    let new_isr_i32: Vec<i32> = new_isr
        .iter()
        .map(|n| i32::try_from(*n).unwrap_or(i32::MAX))
        .collect();
    let new_isr_with_epochs: Vec<BrokerState> = new_isr_i32
        .iter()
        .map(|&bid| BrokerState {
            broker_id: bid,
            broker_epoch: image
                .broker_epoch(u64::try_from(bid).unwrap_or(0))
                .unwrap_or(UNKNOWN_BROKER_EPOCH),
            ..Default::default()
        })
        .collect();

    AlterPartitionRequest {
        broker_id,
        // KIP-903: the partition leader stamps its own broker epoch and each
        // ISR member's epoch from the metadata image so the controller can
        // fence stale replicas. Unknown brokers fall back to -1 (skip-check).
        broker_epoch: image
            .broker_epoch(u64::try_from(broker_id).unwrap_or(0))
            .unwrap_or(UNKNOWN_BROKER_EPOCH),
        topics: vec![TopicData {
            topic_id,
            partitions: vec![PartitionData {
                partition_index: partition,
                leader_epoch,
                new_isr: new_isr_i32,
                new_isr_with_epochs,
                leader_recovery_state: 0,
                partition_epoch: 0,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn alter_partition_targets(
    image: &crabka_metadata::MetadataImage,
    leader_id: Option<NodeId>,
) -> Vec<(NodeId, String)> {
    let mut out = Vec::new();
    if let Some(id) = leader_id
        && let Some(b) = image.broker(id)
    {
        out.push((id, format!("{}:{}", b.host, b.port)));
    }
    let mut others: Vec<(NodeId, String)> = image
        .brokers()
        .filter(|b| Some(b.node_id) != leader_id)
        .map(|b| (b.node_id, format!("{}:{}", b.host, b.port)))
        .collect();
    others.sort_by_key(|(id, _)| *id);
    out.extend(others);
    out
}

#[derive(Debug, PartialEq, Eq)]
enum AlterPartitionSendError {
    NotController,
    Rejected { global_err: i16, part_err: i16 },
    Transport(String),
}

async fn send_alter_partition_to(
    broker_id: i32,
    addr: &str,
    req: AlterPartitionRequest,
) -> Result<(), AlterPartitionSendError> {
    let client = crabka_client_core::Client::builder()
        .bootstrap(addr.to_string())
        .client_id(format!("crabka-broker-{broker_id}-isr"))
        .build()
        .await
        .map_err(|e| AlterPartitionSendError::Transport(format!("connect: {e}")))?;

    let resp = client
        .send(req)
        .await
        .map_err(|e| AlterPartitionSendError::Transport(format!("send: {e}")))?;
    let global_err = resp.error_code;
    let part_err = resp
        .topics
        .first()
        .and_then(|t| t.partitions.first())
        .map_or(0, |p| p.error_code);
    classify_alter_partition_response(global_err, part_err)
}

fn classify_alter_partition_response(
    global_err: i16,
    part_err: i16,
) -> Result<(), AlterPartitionSendError> {
    if is_not_controller_response(global_err, part_err) {
        return Err(AlterPartitionSendError::NotController);
    }
    if global_err != 0 || part_err != 0 {
        return Err(AlterPartitionSendError::Rejected {
            global_err,
            part_err,
        });
    }
    Ok(())
}

fn is_not_controller_response(global_err: i16, part_err: i16) -> bool {
    global_err == crate::codes::NOT_CONTROLLER || part_err == crate::codes::NOT_CONTROLLER
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::atomic::Ordering;

    use crabka_metadata::{BrokerRegistrationRecord, MetadataImage, MetadataRecord, TopicRecord};
    use tempfile::tempdir;
    use tokio::sync::watch;

    fn reg(id: NodeId) -> MetadataRecord {
        MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: id,
            broker_epoch: i64::try_from(id).unwrap(),
            incarnation_id: uuid::Uuid::nil(),
            host: format!("b{id}"),
            port: 9092,
            rack: None,
            endpoints: vec![],
        })
    }

    fn topic(name: &str, topic_id: uuid::Uuid) -> MetadataRecord {
        MetadataRecord::V1Topic(TopicRecord {
            name: name.to_string(),
            topic_id,
            partitions: 1,
            replication_factor: 3,
        })
    }

    fn fixture_partition(log_dir: &Path, topic: &str, partition: i32) -> Arc<Partition> {
        let part_dir = crate::log_dir::partition_dir(log_dir, topic, partition);
        std::fs::create_dir_all(&part_dir).unwrap();
        let log = crabka_log::Log::open(&part_dir, crabka_log::LogConfig::default()).unwrap();
        crate::broker::spawn_partition(
            topic.to_string(),
            partition,
            log_dir.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
        )
    }

    async fn set_replica_state(
        part: &Partition,
        isr: &[NodeId],
        replicas: &[NodeId],
        leader: NodeId,
        leader_epoch: i32,
        follower_ages: &[(NodeId, Duration, Duration)],
    ) {
        let now = Instant::now();
        let mut st = part.replica_state.lock().await;
        st.install_isr(isr, replicas, leader, now);
        st.current_leader_epoch = leader_epoch;
        for &(follower, last_fetch_age, last_caught_up_age) in follower_ages {
            st.per_follower.insert(
                follower,
                crate::replica_state::FollowerStats {
                    leo: 0,
                    last_fetch: now
                        .checked_sub(last_fetch_age)
                        .expect("test fetch age is representable"),
                    last_caught_up: now
                        .checked_sub(last_caught_up_age)
                        .expect("test caught-up age is representable"),
                },
            );
        }
    }

    struct TestMetadataSource {
        image_tx: watch::Sender<Arc<MetadataImage>>,
        leader_tx: watch::Sender<Option<NodeId>>,
    }

    impl TestMetadataSource {
        fn new(image: MetadataImage, leader: Option<NodeId>) -> Self {
            let (image_tx, _) = watch::channel(Arc::new(image));
            let (leader_tx, _) = watch::channel(leader);
            Self {
                image_tx,
                leader_tx,
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::metadata_source::MetadataSource for TestMetadataSource {
        fn current_image(&self) -> Arc<MetadataImage> {
            self.image_tx.borrow().clone()
        }

        fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
            self.image_tx.subscribe()
        }

        fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
            self.leader_tx.subscribe()
        }

        fn quorum_state(&self) -> crabka_raft::QuorumState {
            unimplemented!("unused in isr_maintenance tests")
        }

        async fn submit_change(
            &self,
            _records: Vec<MetadataRecord>,
        ) -> Result<(), crabka_raft::RaftError> {
            unimplemented!("unused in isr_maintenance tests")
        }

        async fn change_membership(
            &self,
            _new_voters: std::collections::BTreeSet<NodeId>,
        ) -> Result<(), crabka_raft::RaftError> {
            unimplemented!("unused in isr_maintenance tests")
        }

        async fn add_learner(
            &self,
            _node_id: NodeId,
            _node: crabka_raft::Node,
        ) -> Result<(), crabka_raft::RaftError> {
            unimplemented!("unused in isr_maintenance tests")
        }

        fn controller_bound_addr(&self) -> std::net::SocketAddr {
            unimplemented!("unused in isr_maintenance tests")
        }

        fn read_snapshot_range(
            &self,
            _position: i64,
            _max_bytes: i32,
        ) -> crabka_raft::SnapshotRange {
            unimplemented!("unused in isr_maintenance tests")
        }

        async fn trigger_snapshot(&self) -> Result<(), crabka_raft::RaftError> {
            unimplemented!("unused in isr_maintenance tests")
        }

        async fn add_voter(
            &self,
            _req: crabka_raft::AddVoter,
        ) -> Result<crabka_raft::ReconfigOutcome, crabka_raft::RaftError> {
            unimplemented!("unused in isr_maintenance tests")
        }

        async fn remove_voter(
            &self,
            _req: crabka_raft::RemoveVoter,
        ) -> Result<crabka_raft::ReconfigOutcome, crabka_raft::RaftError> {
            unimplemented!("unused in isr_maintenance tests")
        }

        async fn update_voter(
            &self,
            _req: crabka_raft::UpdateVoter,
        ) -> Result<crabka_raft::ReconfigOutcome, crabka_raft::RaftError> {
            unimplemented!("unused in isr_maintenance tests")
        }

        async fn cancel(&self) {}
    }

    #[tokio::test]
    async fn compute_proposal_shrinks_lagging_isr_member() {
        let log_dir = tempdir().unwrap();
        let part = fixture_partition(log_dir.path(), "t", 0);
        set_replica_state(
            &part,
            &[1, 2, 3],
            &[1, 2, 3],
            1,
            7,
            &[
                (2, Duration::from_secs(1), Duration::from_secs(1)),
                (3, Duration::from_secs(30), Duration::from_secs(30)),
            ],
        )
        .await;

        let proposal = compute_proposal(&part, Duration::from_secs(5))
            .await
            .expect("lagging ISR member should produce a shrink proposal");

        let expected = Proposal {
            prev_isr: vec![1, 2, 3],
            new_isr: vec![1, 2],
            leader_epoch: 7,
        };
        assert_eq!(proposal, expected);
    }

    #[tokio::test]
    async fn compute_proposal_expands_recently_caught_up_replica() {
        let log_dir = tempdir().unwrap();
        let part = fixture_partition(log_dir.path(), "t", 0);
        set_replica_state(
            &part,
            &[1, 2],
            &[1, 2, 3],
            1,
            8,
            &[
                (2, Duration::from_secs(1), Duration::from_secs(1)),
                (3, Duration::from_secs(1), Duration::from_secs(1)),
            ],
        )
        .await;

        let proposal = compute_proposal(&part, Duration::from_secs(5))
            .await
            .expect("caught-up replica should produce an expand proposal");

        let expected = Proposal {
            prev_isr: vec![1, 2],
            new_isr: vec![1, 2, 3],
            leader_epoch: 8,
        };
        assert_eq!(proposal, expected);
    }

    #[tokio::test]
    async fn compute_proposal_ignores_stale_non_isr_replica() {
        let log_dir = tempdir().unwrap();
        let part = fixture_partition(log_dir.path(), "t", 0);
        set_replica_state(
            &part,
            &[1, 2],
            &[1, 2, 3],
            1,
            9,
            &[
                (2, Duration::from_secs(1), Duration::from_secs(1)),
                (3, Duration::from_secs(1), Duration::from_secs(30)),
            ],
        )
        .await;

        assert!(
            compute_proposal(&part, Duration::from_secs(5))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn run_bumps_shrink_metric_for_leader_partition() {
        let log_dir = tempdir().unwrap();
        let part = fixture_partition(log_dir.path(), "t", 0);
        part.current_leader.store(1, Ordering::Release);
        set_replica_state(
            &part,
            &[1, 2],
            &[1, 2],
            1,
            10,
            &[(2, Duration::from_secs(30), Duration::from_secs(30))],
        )
        .await;

        let partitions = Arc::new(PartitionRegistry::new());
        partitions.insert("t".to_string(), 0, part);
        let controller: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(
            TestMetadataSource::new(MetadataImage::new(uuid::Uuid::nil()), None),
        );
        let metrics = crate::metrics::BrokerMetrics::default();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(Config {
            node_id: 1,
            partitions,
            controller,
            replica_lag_time_max: Duration::from_secs(5),
            broker_id: 1,
            shutdown: shutdown.clone(),
            metrics: metrics.clone(),
        }));

        tokio::time::timeout(Duration::from_millis(500), async {
            while metrics.isr_shrinks_total.get() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("leader partition should be scanned and classified as a shrink");

        shutdown.cancel();
        task.await.unwrap();
        assert!(metrics.isr_shrinks_total.get() == 1);
        assert!(metrics.isr_expands_total.get() == 0);
    }

    #[test]
    fn build_request_preserves_topic_broker_epochs_and_isr_fields() {
        use crabka_protocol::UnknownTaggedFields;
        use crabka_protocol::owned::alter_partition_request::{
            BrokerState, PartitionData, TopicData,
        };

        let topic_id = uuid::Uuid::from_u128(0xA11CE);
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&topic("orders", topic_id));
        image.apply(&reg(1));

        let req = build_alter_partition_request(&image, 4, "orders", 6, &[1, 9], 12);

        let expected = AlterPartitionRequest {
            broker_id: 4,
            broker_epoch: -1,
            topics: vec![TopicData {
                topic_id: crabka_protocol::primitives::uuid::Uuid(*topic_id.as_bytes()),
                partitions: vec![PartitionData {
                    partition_index: 6,
                    leader_epoch: 12,
                    new_isr: vec![1, 9],
                    new_isr_with_epochs: vec![
                        BrokerState {
                            broker_id: 1,
                            broker_epoch: 1,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                        BrokerState {
                            broker_id: 9,
                            broker_epoch: -1,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                    ],
                    leader_recovery_state: 0,
                    partition_epoch: 0,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert_eq!(req, expected);
    }

    #[tokio::test]
    async fn send_alter_partition_errors_without_controller_target() {
        let controller: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(
            TestMetadataSource::new(MetadataImage::new(uuid::Uuid::nil()), None),
        );

        let err = send_alter_partition(&controller, 1, "orders", 0, vec![1], 3)
            .await
            .expect_err("missing controller leader should reject the send");

        assert!(err == "no controller leader");
    }

    #[tokio::test]
    async fn send_alter_partition_to_reports_transport_error_for_closed_port() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let err = send_alter_partition_to(1, &addr.to_string(), AlterPartitionRequest::default())
            .await
            .expect_err("closed local port should fail as transport");

        assert!(matches!(err, AlterPartitionSendError::Transport(_)));
    }

    #[test]
    fn alter_partition_targets_try_hint_first_then_remaining_brokers() {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&reg(2));
        image.apply(&reg(0));
        image.apply(&reg(1));

        let targets = alter_partition_targets(&image, Some(2));

        assert!(
            targets
                == vec![
                    (2, "b2:9092".to_string()),
                    (0, "b0:9092".to_string()),
                    (1, "b1:9092".to_string()),
                ]
        );
    }

    #[test]
    fn alter_partition_targets_fall_back_when_hint_missing() {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&reg(1));
        image.apply(&reg(0));

        let targets = alter_partition_targets(&image, Some(9));

        assert!(targets == vec![(0, "b0:9092".to_string()), (1, "b1:9092".to_string())]);
    }

    #[test]
    fn not_controller_classification_covers_global_and_partition_codes() {
        let cases = [
            (crate::codes::NOT_CONTROLLER, 0, true),
            (0, crate::codes::NOT_CONTROLLER, true),
            (0, 0, false),
            (crate::codes::UNKNOWN_SERVER_ERROR, 0, false),
        ];
        for (global_err, part_err, want) in cases {
            assert_eq!(
                is_not_controller_response(global_err, part_err),
                want,
                "global_err={global_err} part_err={part_err}"
            );
        }
    }

    #[test]
    fn alter_partition_response_classifies_all_error_surfaces() {
        let cases = [
            (0, 0, Ok(())),
            (
                crate::codes::NOT_CONTROLLER,
                0,
                Err(AlterPartitionSendError::NotController),
            ),
            (
                0,
                crate::codes::NOT_CONTROLLER,
                Err(AlterPartitionSendError::NotController),
            ),
            (
                crate::codes::UNKNOWN_SERVER_ERROR,
                0,
                Err(AlterPartitionSendError::Rejected {
                    global_err: crate::codes::UNKNOWN_SERVER_ERROR,
                    part_err: 0,
                }),
            ),
            (
                0,
                crate::codes::UNKNOWN_SERVER_ERROR,
                Err(AlterPartitionSendError::Rejected {
                    global_err: 0,
                    part_err: crate::codes::UNKNOWN_SERVER_ERROR,
                }),
            ),
            (
                crate::codes::UNKNOWN_SERVER_ERROR,
                crate::codes::UNKNOWN_TOPIC_OR_PARTITION,
                Err(AlterPartitionSendError::Rejected {
                    global_err: crate::codes::UNKNOWN_SERVER_ERROR,
                    part_err: crate::codes::UNKNOWN_TOPIC_OR_PARTITION,
                }),
            ),
        ];
        for (global_err, part_err, want) in cases {
            assert_eq!(
                classify_alter_partition_response(global_err, part_err),
                want,
                "global_err={global_err} part_err={part_err}"
            );
        }
    }
}
