//! Subscribes to the controller's metadata-image watch channel and
//! on each apply:
//!
//! 1. **Materializes the local on-disk partition** for any
//!    `(topic, partition)` where this broker is in `replicas`,
//!    regardless of leader/follower role. With round-robin replica
//!    placement, the broker that handles a `CreateTopics` request is
//!    usually not the partition leader. The lazy supervisor-driven path
//!    is therefore the only one that materializes the partition on the
//!    leader broker reliably.
//!
//! 2. **Spawns a `replicator::run` task** per `(topic, partition)`
//!    where this broker is in `replicas` but is NOT the leader, and
//!    cancels tasks for partitions removed from the image.

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crabka_ids::PartitionIndex;
use crabka_log::{Log, LogConfig};
use crabka_metadata::MetadataImage;
use crabka_raft::NodeId;
use crabka_units::Time;
use dashmap::DashMap;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    config::ReplicationRuntimeConfig, partition_registry::PartitionRegistry, replicator,
    throttle::ThrottleState, txn::coordinator::TxnCoordinator,
};

/// A `(topic, partition)` pair. The supervisor keys follower tasks, local
/// materialization, and dir-assignment reports on this pair.
pub(crate) type TopicPartition = (String, i32);

#[derive(Debug, Clone, PartialEq, Eq)]
struct WalFollowerSpec {
    topic: String,
    leader: NodeId,
    leader_epoch: crabka_metadata::LeaderEpoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReplicatorTaskTarget {
    topic_id: uuid::Uuid,
    leader: NodeId,
    leader_epoch: crabka_metadata::LeaderEpoch,
}

#[derive(Debug)]
struct ReplicatorTask {
    shutdown: CancellationToken,
    target: ReplicatorTaskTarget,
    handle: JoinHandle<()>,
}

#[derive(Debug)]
struct WalFollowerTask {
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
}

/// `(topic, partition)` pairs where `node_id` is in `replicas` AND
/// `leader != node_id`. For each such pair the broker should run a follower
/// replicator task. This is a single O(P) walk. It runs on every
/// metadata-image change, so it must stay proportional to total partitions.
pub(crate) fn desired_follower_set(
    node_id: NodeId,
    image: &MetadataImage,
) -> HashSet<TopicPartition> {
    image
        .all_partitions()
        .filter(|p| p.replicas.contains(&node_id) && p.leader != node_id)
        .map(|p| (p.topic.clone(), p.partition))
        .collect()
}

/// `(topic, partition)` pairs where `node_id` is in `replicas`,
/// regardless of leader/follower role. Every entry here means this
/// broker hosts partition data on disk and must materialize the
/// on-disk `Partition` locally. This is a single O(P) walk, the same as
/// [`desired_follower_set`].
pub(crate) fn desired_local_set(node_id: NodeId, image: &MetadataImage) -> HashSet<TopicPartition> {
    image
        .all_partitions()
        .filter(|p| p.replicas.contains(&node_id))
        .map(|p| (p.topic.clone(), p.partition))
        .collect()
}

/// WAL voter placement for every diskless partition in one metadata image.
/// The partition leader is first, then rack-distinct registered brokers are
/// preferred by the placement policy.
pub(crate) fn desired_wal_placements(
    image: &MetadataImage,
    voter_count: usize,
) -> HashMap<crate::wal::quorum::registry::ShardId, Vec<NodeId>> {
    let brokers = image.brokers().cloned().collect::<Vec<_>>();
    image
        .all_partitions()
        .filter(|partition| {
            crate::broker::diskless_topic_config(image.topic_config(&partition.topic))
        })
        .filter(|partition| image.broker(partition.leader).is_some())
        .filter_map(|partition| {
            let topic_id = image.topic(&partition.topic)?.topic_id;
            Some((
                crate::wal::quorum::registry::ShardId {
                    topic_id,
                    partition: PartitionIndex(partition.partition),
                },
                crate::wal::quorum::placement::select_voters(
                    brokers.iter().cloned(),
                    partition.leader,
                    voter_count,
                ),
            ))
        })
        .collect()
}

/// Open (or recover) the on-disk `Partition` for `(topic, partition)` and
/// insert it into `partitions` with
/// `PartitionRegistry::materialize_if_vacant`.
///
/// This is the canonical, race-free materialization helper. Both the
/// `ReplicatorSupervisor` reconcile loop and the `InitProducerId` handler
/// (first-touch path) call this function. `materialize_if_vacant` runs the
/// build closure under the per-key lock, so two concurrent callers for the
/// same key can never both spawn independent writer tasks.
///
/// Returns `Ok(())` if the partition is already present, which is a no-op, or
/// if the function opened it. Returns `Err(String)` on I/O failure.
pub(crate) struct MaterializePartitionConfig<'a> {
    pub partitions: &'a PartitionRegistry,
    pub topic: &'a str,
    pub topic_id: Option<uuid::Uuid>,
    pub partition: i32,
    pub log_dirs: &'a [PathBuf],
    pub log_config: &'a LogConfig,
    pub log_dir_status: &'a crate::log_dir_status::LogDirRegistry,
    pub producer_state: &'a Arc<crate::producer_state::ProducerState>,
    pub producer_id_expiration: Time,
    pub max_produce_group: usize,
    pub partition_writer_queue_depth: usize,
    pub diskless_wal_local_replica_count: usize,
    pub diskless: bool,
    pub hot_tail: Option<Arc<crate::diskless::hot_tail::HotTailCache>>,
    pub wal_shards: Option<Arc<crate::wal::quorum::registry::WalShardRegistry>>,
    pub sequencer: Option<Arc<dyn crate::wal::OffsetSequencer>>,
}

pub(crate) fn materialize_partition(config: MaterializePartitionConfig<'_>) -> Result<(), String> {
    let MaterializePartitionConfig {
        partitions,
        topic,
        topic_id,
        partition,
        log_dirs,
        log_config,
        log_dir_status,
        producer_state,
        producer_id_expiration,
        max_produce_group,
        partition_writer_queue_depth,
        diskless_wal_local_replica_count,
        diskless,
        hot_tail,
        wal_shards,
        sequencer,
    } = config;
    // `materialize_if_vacant` runs `build` under the per-key write lock —
    // only one thread can be inside it for a given key at a time,
    // eliminating the TOCTOU race that existed with the old
    // `contains_key` + `insert` pattern. JBOD placement (KIP-113) happens
    // under this lock too, so two concurrent materializations of the same
    // partition can never pick two different log dirs.
    partitions.materialize_if_vacant(topic, PartitionIndex(partition), || {
        let dir = crate::log_dir::place_partition_dir(log_dirs, topic, partition);
        std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
        let open_config = crate::diskless::recovery::open_config(log_config, diskless);
        let mut log = Log::open(&dir, open_config).map_err(|e| format!("Log::open: {e}"))?;
        if let Some(stamp_source) = partitions.stamp_source() {
            log.set_stamp_source(stamp_source)
                .map_err(|e| format!("set stamp source: {e}"))?;
        }
        if diskless && let (Some(topic_id), Some(registry)) = (topic_id, wal_shards.as_ref()) {
            let shard = crate::wal::quorum::registry::ShardId {
                topic_id,
                partition: PartitionIndex(partition),
            };
            if registry.local_is_leader(shard) {
                crate::wal::quorum::follower::hydrate_on_promotion(
                    log_dirs,
                    topic,
                    shard,
                    registry.local_node_id(),
                    log_config,
                    &mut log,
                )
                .map_err(|e| format!("hydrate promoted WAL follower: {e}"))?;
            }
        }
        if diskless {
            producer_state.install_snapshot_before_materialization(
                topic,
                PartitionIndex(partition),
                log.producer_state_snapshot(),
            );
        }
        let owning_dir = dir
            .parent()
            .expect("placed partition dir always has a parent log.dir")
            .to_path_buf();
        crate::broker::try_spawn_partition_with_sequencer(crate::broker::PartitionSpawnConfig {
            topic: topic.to_string(),
            topic_id,
            partition_id: PartitionIndex(partition),
            log_dir: owning_dir,
            log,
            log_dir_status: log_dir_status.clone(),
            producer_state: producer_state.clone(),
            producer_id_expiration,
            max_produce_group,
            partition_writer_queue_depth,
            diskless_wal_local_replica_count,
            diskless,
            hot_tail,
            wal_shards,
            sequencer,
        })
        .map_err(|e| format!("spawn partition: {e}"))
    })
}

/// Push topic-config overrides onto every locally-hosted partition in
/// `desired`. The call is idempotent, because the same `LogConfig` sent twice
/// is a cheap noop write inside `Log::set_config`. Errors on individual
/// partitions log through `warn!` and do not propagate.
pub(crate) async fn push_topic_configs(
    desired: &HashSet<TopicPartition>,
    partitions: &PartitionRegistry,
    image: &MetadataImage,
) {
    let empty: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for (topic, partition) in desired {
        let Some(part) = partitions.get(topic, PartitionIndex(*partition)) else {
            continue;
        };
        let overrides = image.topic_config(topic).unwrap_or(&empty);
        if let Err(e) = part.apply_log_config_overrides(overrides).await {
            warn!(
                topic = %topic, partition = partition, error = %e,
                "supervisor: apply_log_config_overrides failed"
            );
        }
    }
}

/// Compute the dir-assignment reports that changed since last reported.
///
/// Returns `(wire_assignments, tracker_updates)`:
/// - `wire_assignments`: `(topic_id, partition, dir_uuid)` for `build_request`.
/// - `tracker_updates`: `(topic_name, partition, dir_uuid)` to write into
///   `reported_dirs` on a successful send.
///
/// This function is pure. It reads each partition's current owning dir exactly
/// once and does not load again after the change-check. That removes both the
/// TOCTOU race and the O(n²) `Vec::contains` scan of a double-iteration
/// approach.
type WireDirAssignment = (uuid::Uuid, i32, uuid::Uuid);
type ReportedDirUpdate = (String, i32, uuid::Uuid);
type ChangedAssignments = (Vec<WireDirAssignment>, Vec<ReportedDirUpdate>);

pub(crate) fn collect_changed_assignments(
    local_set: &HashSet<TopicPartition>,
    partitions: &PartitionRegistry,
    log_dir_ids: &crate::log_dir_id::LogDirIds,
    image: &MetadataImage,
    reported_dirs: &dashmap::DashMap<TopicPartition, uuid::Uuid>,
) -> ChangedAssignments {
    let mut wire = Vec::new();
    let mut updates = Vec::new();
    for (topic, partition) in local_set {
        let Some(part) = partitions.get(topic, PartitionIndex(*partition)) else {
            continue;
        };
        let dir = part.log_dir.load();
        let Some(dir_uuid) = log_dir_ids.id_for(&dir) else {
            continue;
        };
        let Some(topic_rec) = image.topic(topic) else {
            continue;
        };
        let key = (topic.clone(), *partition);
        if reported_dirs.get(&key).map(|e| *e.value()) == Some(dir_uuid) {
            continue; // unchanged since last report
        }
        wire.push((topic_rec.topic_id, *partition, dir_uuid));
        updates.push((topic.clone(), *partition, dir_uuid));
    }
    (wire, updates)
}

fn resolve_leader_endpoint(
    broker: &crabka_metadata::BrokerRegistrationRecord,
    listener_name: &str,
) -> (String, u16) {
    broker
        .endpoints
        .iter()
        .find(|e| e.name == listener_name)
        .map_or_else(
            || (broker.host.clone(), broker.port),
            |e| (e.host.clone(), e.port),
        )
}

#[async_trait::async_trait]
trait AssignDirsReporter: Send + Sync {
    async fn send(
        &self,
        controller: &Arc<dyn crate::metadata_source::MetadataSource>,
        client_id: &str,
        req: crabka_protocol::owned::assign_replicas_to_dirs_request::AssignReplicasToDirsRequest,
    ) -> Result<(), String>;
}

#[derive(Default)]
struct NetworkAssignDirsReporter {
    dispatch_queue_capacity: crabka_client_core::ConnectionDispatchQueueCapacity,
    frame_max: crabka_client_core::ClientFrameMax,
}

#[async_trait::async_trait]
impl AssignDirsReporter for NetworkAssignDirsReporter {
    async fn send(
        &self,
        controller: &Arc<dyn crate::metadata_source::MetadataSource>,
        client_id: &str,
        req: crabka_protocol::owned::assign_replicas_to_dirs_request::AssignReplicasToDirsRequest,
    ) -> Result<(), String> {
        crate::assign_dirs::send_assignments_with_policy(
            controller,
            client_id,
            req,
            self.dispatch_queue_capacity,
            self.frame_max,
        )
        .await
    }
}

pub(crate) struct ReplicatorSupervisor {
    node_id: NodeId,
    broker_id: i32,
    controller: Arc<dyn crate::metadata_source::MetadataSource>,
    partitions: Arc<PartitionRegistry>,
    log_dirs: Vec<PathBuf>,
    log_config: LogConfig,
    client_id: String,
    tasks: DashMap<TopicPartition, ReplicatorTask>,
    wal_tasks: DashMap<crate::wal::quorum::registry::ShardId, WalFollowerTask>,
    wal_task_targets: DashMap<crate::wal::quorum::registry::ShardId, WalFollowerSpec>,
    shutdown: CancellationToken,
    txn_coordinator: Option<Arc<TxnCoordinator>>,
    /// KIP-932 share coordinator. Each reconcile refreshes its view of
    /// locally-led `__share_group_state` partitions, the same as for the
    /// txn coordinator.
    share_coordinator: Option<Arc<crate::share_coordinator::coordinator::ShareCoordinator>>,
    /// Shared outbound dialer. It uses TLS and SASL when configured, and raw
    /// TCP otherwise. Each spawned replicator clones this Arc.
    inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
    /// Listener protocol used for inter-broker dials. It decides whether
    /// the dialer runs TLS and SASL.
    inter_broker_listener_protocol: crabka_security::ListenerProtocol,
    inter_broker_server_name: String,
    replication: ReplicationRuntimeConfig,
    /// Name of the listener whose endpoint the supervisor resolves from the
    /// metadata image when it dials peers.
    inter_broker_listener_name: String,
    /// KIP-73: broker-wide throttle state forwarded to each spawned
    /// replicator so they can consult the follower-in token bucket.
    throttle_state: Arc<ThrottleState>,
    /// KIP-113 runtime offline-dir registry. Forwarded into each
    /// `materialize_partition` and each spawned `Replicator::Config`, so the
    /// partition writer's storage-failure path can flip the dir
    /// offline broker-wide.
    log_dir_status: crate::log_dir_status::LogDirRegistry,
    /// Broker-wide idempotent/transactional producer-sequence tracker.
    /// Forwarded into each `materialize_partition` so the partition
    /// writer's `Compact` handler can snapshot active producers for
    /// KIP-534 `RETAIN_EMPTY`.
    producer_state: Arc<crate::producer_state::ProducerState>,
    producer_id_expiration: Time,
    max_produce_group: usize,
    partition_writer_queue_depth: usize,
    diskless_wal_local_replica_count: usize,
    /// Broker-wide metrics handle. Each spawned replicator
    /// clones this so it can increment `replication_bytes_in` after a
    /// successful follower-side append.
    metrics: crate::metrics::BrokerMetrics,
    /// KIP-858: stable UUID per configured log.dir. The reconcile loop uses
    /// these to build `AssignReplicasToDirs` reports.
    log_dir_ids: crate::log_dir_id::LogDirIds,
    /// Shared advisory cache for quorum-committed diskless WAL tails.
    hot_tail: Arc<crate::diskless::hot_tail::HotTailCache>,
    /// Registry exposed through the KIP-595 shard router for diskless WAL
    /// fetches to newly materialized partitions.
    wal_shards: Arc<crate::wal::quorum::registry::WalShardRegistry>,
    /// KIP-858: tracks the last-reported dir UUID per (topic, partition), so
    /// the supervisor sends `AssignReplicasToDirs` only on first
    /// materialization or after a KIP-113 log-dir swap.
    reported_dirs: dashmap::DashMap<TopicPartition, uuid::Uuid>,
    /// Topic identities observed by the preceding reconcile. A comparison of
    /// UUIDs, rather than names alone, also detects a delete followed by a
    /// same-name recreation, and it does not treat startup-only on-disk logs
    /// as tombstoned.
    known_topic_ids: Mutex<HashMap<String, uuid::Uuid>>,
    assign_dirs_reporter: Arc<dyn AssignDirsReporter>,
}

pub(crate) struct ReplicatorSupervisorConfig {
    pub client_dispatch_queue_capacity: crabka_client_core::ConnectionDispatchQueueCapacity,
    pub client_frame_max: crabka_client_core::ClientFrameMax,
    pub node_id: NodeId,
    pub broker_id: i32,
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
    pub partitions: Arc<PartitionRegistry>,
    pub log_dirs: Vec<PathBuf>,
    pub log_config: LogConfig,
    pub client_id: String,
    pub shutdown: CancellationToken,
    pub txn_coordinator: Option<Arc<TxnCoordinator>>,
    pub share_coordinator: Option<Arc<crate::share_coordinator::coordinator::ShareCoordinator>>,
    pub inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
    pub inter_broker_listener_protocol: crabka_security::ListenerProtocol,
    pub inter_broker_server_name: String,
    pub inter_broker_listener_name: String,
    pub replication: ReplicationRuntimeConfig,
    pub throttle_state: Arc<ThrottleState>,
    pub log_dir_status: crate::log_dir_status::LogDirRegistry,
    pub producer_state: Arc<crate::producer_state::ProducerState>,
    pub producer_id_expiration: Time,
    pub max_produce_group: usize,
    pub partition_writer_queue_depth: usize,
    pub diskless_wal_local_replica_count: usize,
    pub metrics: crate::metrics::BrokerMetrics,
    pub log_dir_ids: crate::log_dir_id::LogDirIds,
    pub hot_tail: Arc<crate::diskless::hot_tail::HotTailCache>,
    pub wal_shards: Arc<crate::wal::quorum::registry::WalShardRegistry>,
}

impl ReplicatorSupervisor {
    pub(crate) fn new(config: ReplicatorSupervisorConfig) -> Self {
        let ReplicatorSupervisorConfig {
            client_dispatch_queue_capacity,
            client_frame_max,
            node_id,
            broker_id,
            controller,
            partitions,
            log_dirs,
            log_config,
            client_id,
            shutdown,
            txn_coordinator,
            share_coordinator,
            inter_broker_client,
            inter_broker_listener_protocol,
            inter_broker_server_name,
            inter_broker_listener_name,
            replication,
            throttle_state,
            log_dir_status,
            producer_state,
            producer_id_expiration,
            max_produce_group,
            partition_writer_queue_depth,
            diskless_wal_local_replica_count,
            metrics,
            log_dir_ids,
            hot_tail,
            wal_shards,
        } = config;
        let known_topic_ids = controller
            .current_image()
            .topics()
            .map(|topic| (topic.name.clone(), topic.topic_id))
            .collect();
        Self {
            node_id,
            broker_id,
            controller,
            partitions,
            log_dirs,
            log_config,
            client_id,
            tasks: DashMap::new(),
            wal_tasks: DashMap::new(),
            wal_task_targets: DashMap::new(),
            shutdown,
            txn_coordinator,
            share_coordinator,
            inter_broker_client,
            inter_broker_listener_protocol,
            inter_broker_server_name,
            inter_broker_listener_name,
            replication,
            throttle_state,
            log_dir_status,
            producer_state,
            producer_id_expiration,
            max_produce_group,
            partition_writer_queue_depth,
            diskless_wal_local_replica_count,
            metrics,
            log_dir_ids,
            hot_tail,
            wal_shards,
            reported_dirs: dashmap::DashMap::new(),
            known_topic_ids: Mutex::new(known_topic_ids),
            assign_dirs_reporter: Arc::new(NetworkAssignDirsReporter {
                dispatch_queue_capacity: client_dispatch_queue_capacity,
                frame_max: client_frame_max,
            }),
        }
    }

    fn replicator_config(
        &self,
        key: TopicPartition,
        topic: &crabka_metadata::TopicRecord,
        partition: &crabka_metadata::PartitionRecord,
        broker: &crabka_metadata::BrokerRegistrationRecord,
        shutdown: CancellationToken,
    ) -> replicator::Config {
        let (leader_host, leader_port) =
            resolve_leader_endpoint(broker, &self.inter_broker_listener_name);
        replicator::Config {
            node_id: self.node_id,
            topic: key.0,
            topic_id: crabka_protocol::primitives::uuid::Uuid(topic.topic_id.into_bytes()),
            partition: crabka_ids::PartitionIndex(key.1),
            leader_node_id: partition.leader,
            leader_epoch: partition.leader_epoch,
            leader_host,
            leader_port,
            partitions: self.partitions.clone(),
            log_dirs: self.log_dirs.clone(),
            log_settings: self.log_config.clone(),
            client_id: self.client_id.clone(),
            shutdown,
            inter_broker_client: self.inter_broker_client.clone(),
            inter_broker_listener_protocol: self.inter_broker_listener_protocol,
            inter_broker_server_name: self.inter_broker_server_name.clone(),
            replication: self.replication.clone(),
            throttle_state: self.throttle_state.clone(),
            controller: self.controller.clone(),
            log_dir_status: self.log_dir_status.clone(),
            producer_state: self.producer_state.clone(),
            metrics: self.metrics.clone(),
        }
    }

    fn desired_wal_followers(
        &self,
        image: &MetadataImage,
        placements: &HashMap<crate::wal::quorum::registry::ShardId, Vec<NodeId>>,
    ) -> HashMap<crate::wal::quorum::registry::ShardId, WalFollowerSpec> {
        image
            .all_partitions()
            .filter(|partition| {
                crate::broker::diskless_topic_config(image.topic_config(&partition.topic))
            })
            .filter_map(|partition| {
                let topic = image.topic(&partition.topic)?;
                let shard = crate::wal::quorum::registry::ShardId {
                    topic_id: topic.topic_id,
                    partition: PartitionIndex(partition.partition),
                };
                let voters = placements.get(&shard)?;
                (voters.len() == self.diskless_wal_local_replica_count
                    && voters.first() == Some(&partition.leader)
                    && partition.leader != self.node_id
                    && voters.contains(&self.node_id))
                .then(|| {
                    (
                        shard,
                        WalFollowerSpec {
                            topic: partition.topic.clone(),
                            leader: partition.leader,
                            leader_epoch: partition.leader_epoch,
                        },
                    )
                })
            })
            .collect()
    }

    async fn stop_obsolete_wal_followers(
        &self,
        desired: &HashMap<crate::wal::quorum::registry::ShardId, WalFollowerSpec>,
    ) {
        let current = self
            .wal_tasks
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for shard in current {
            let target_matches = match (desired.get(&shard), self.wal_task_targets.get(&shard)) {
                (Some(desired), Some(current)) => {
                    desired == current.value()
                        && self
                            .wal_tasks
                            .get(&shard)
                            .is_some_and(|task| !task.handle.is_finished())
                }
                _ => false,
            };
            if target_matches {
                continue;
            }
            let Some((_, task)) = self.wal_tasks.remove(&shard) else {
                continue;
            };
            let target = self
                .wal_task_targets
                .remove(&shard)
                .map(|(_, target)| target);
            task.shutdown.cancel();
            let _ = task.handle.await;
            if !self.wal_shards.local_is_voter(shard)
                && let Some(target) = target
            {
                for log_dir in &self.log_dirs {
                    if let Err(error) = crate::wal::quorum::remove_shard(
                        self.wal_shards.as_ref(),
                        log_dir,
                        &target.topic,
                        shard.topic_id,
                        shard.partition,
                    ) {
                        warn!(
                            topic = %target.topic,
                            partition = shard.partition.0,
                            path = %log_dir.display(),
                            error = %error,
                            "failed to remove obsolete follower WAL shard"
                        );
                    }
                }
            }
        }
    }

    fn spawn_wal_followers(
        &self,
        image: &MetadataImage,
        desired: HashMap<crate::wal::quorum::registry::ShardId, WalFollowerSpec>,
    ) {
        for (shard, spec) in desired {
            if self.wal_tasks.contains_key(&shard) {
                continue;
            }
            let Some(broker) = image.broker(spec.leader) else {
                warn!(
                    topic = %spec.topic,
                    partition = shard.partition.0,
                    leader = spec.leader.0,
                    "diskless WAL leader broker is not registered; deferring follower"
                );
                continue;
            };
            let (leader_host, leader_port) =
                resolve_leader_endpoint(broker, &self.inter_broker_listener_name);
            let token = self.shutdown.child_token();
            self.wal_task_targets.insert(shard, spec.clone());
            let handle = tokio::spawn(crate::wal::quorum::follower::run(
                crate::wal::quorum::follower::Config {
                    node_id: self.node_id,
                    topic: spec.topic,
                    shard,
                    leader_node_id: spec.leader,
                    leader_epoch: spec.leader_epoch.0,
                    leader_host,
                    leader_port,
                    log_dirs: self.log_dirs.clone(),
                    storage: self.log_config.clone(),
                    client_id: self.client_id.clone(),
                    shutdown: token.clone(),
                    inter_broker_client: self.inter_broker_client.clone(),
                    inter_broker_listener_protocol: self.inter_broker_listener_protocol,
                    inter_broker_server_name: self.inter_broker_server_name.clone(),
                    replication: self.replication.clone(),
                },
            ));
            self.wal_tasks.insert(
                shard,
                WalFollowerTask {
                    shutdown: token,
                    handle,
                },
            );
        }
    }

    pub(crate) async fn reconcile(&self, image: &MetadataImage) {
        let wal_placements = desired_wal_placements(image, self.diskless_wal_local_replica_count);
        for (shard, voters) in &wal_placements {
            let shortfall = self
                .diskless_wal_local_replica_count
                .saturating_sub(voters.len());
            if let Some(shortfall) = std::num::NonZeroUsize::new(shortfall) {
                warn!(
                    topic_id = %shard.topic_id,
                    partition = shard.partition.0,
                    available = voters.len(),
                    required = self.diskless_wal_local_replica_count,
                    shortfall = shortfall.get(),
                    "diskless WAL placement lacks enough distinct-rack registered brokers"
                );
            }
        }
        self.wal_shards.replace_placements(&wal_placements);
        let desired_wal_followers = self.desired_wal_followers(image, &wal_placements);

        // A promoted WAL voter must have exclusive access to its follower log
        // before the canonical partition adopts that durable prefix.
        self.stop_obsolete_wal_followers(&desired_wal_followers)
            .await;
        let wal_dirs_to_keep = image
            .all_partitions()
            .filter(|partition| {
                crate::broker::diskless_topic_config(image.topic_config(&partition.topic))
            })
            .filter_map(|partition| {
                let topic_id = image.topic(&partition.topic)?.topic_id;
                let shard = crate::wal::quorum::registry::ShardId {
                    topic_id,
                    partition: PartitionIndex(partition.partition),
                };
                wal_placements
                    .get(&shard)
                    .is_some_and(|voters| voters.contains(&self.node_id))
                    .then_some((partition.topic.clone(), shard))
            })
            .flat_map(|(topic, shard)| {
                self.log_dirs.iter().map(move |log_dir| {
                    crate::wal::quorum::shard_dir(
                        log_dir,
                        &topic,
                        Some(shard.topic_id),
                        shard.partition,
                    )
                })
            })
            .collect();
        if let Err(error) =
            crate::wal::quorum::prune_orphaned_shard_dirs(&self.log_dirs, &wal_dirs_to_keep)
        {
            warn!(error = %error, "failed to prune orphaned WAL shard directories");
        }

        let local_set = desired_local_set(self.node_id, image);

        // A DeleteTopics handler tears down its local partition immediately
        // after the metadata commit. A reconcile that already captured the
        // preceding image can race that teardown and materialize the deleted
        // partition again. Re-prune from the authoritative new image before
        // materializing its desired set so that stale-image resurrection is
        // idempotently repaired on the next watch delivery.
        self.prune_deleted_topic_partitions(image);

        // A reassignment can remove this broker from a live partition's
        // replica set without deleting the topic. Stop and remove that local
        // runtime before materializing the new desired set. Partitions that
        // are absent from metadata are left alone here: startup recovery can
        // discover an on-disk partition before its topic record arrives, and
        // `prune_deleted_topic_partitions` handles known topic tombstones.
        self.prune_unassigned_partitions(&local_set, image);

        // 0. Materialize the on-disk partition for every assignment where
        //    self is in `replicas`, regardless of leader/follower role.
        //    Additionally: sync the partition's cached leader + epoch
        //    (idempotent), and for partitions where self is leader,
        //    install the ISR into ReplicaState for HW computation.
        self.reconcile_local_partitions(&local_set, image).await;

        // Start WAL followers only after reassignment pruning completes. A
        // broker that just stopped hosting the ordinary partition deletes its
        // old shard directory during pruning; starting first would race that
        // deletion against the new follower log.
        self.spawn_wal_followers(image, desired_wal_followers);

        // Push topic-config overrides onto every locally-hosted partition.
        // Pushes are idempotent — sending the same `LogConfig` is a cheap
        // noop write inside `Log::set_config`. The metadata-watch reconcile
        // loop fires on every image change, so AlterConfigs propagation is
        // bounded to one reconcile tick.
        push_topic_configs(&local_set, &self.partitions, image).await;

        let desired = desired_follower_set(self.node_id, image);

        // 1. Cancel removed, retargeted, or exited tasks. Topic identity is
        // part of the target: a delete/recreate with the same name, leader,
        // and epoch must not reuse the old topic's task.
        let current: Vec<TopicPartition> = self.tasks.iter().map(|e| e.key().clone()).collect();
        for k in current {
            let desired_target = desired.contains(&k).then(|| {
                let topic = image.topic(&k.0)?;
                let partition = image.partition(&k.0, k.1)?;
                Some(ReplicatorTaskTarget {
                    topic_id: topic.topic_id,
                    leader: partition.leader,
                    leader_epoch: partition.leader_epoch,
                })
            });
            let desired_target = desired_target.flatten();
            let keep = self.tasks.get(&k).is_some_and(|task| {
                !task.handle.is_finished() && Some(task.target) == desired_target
            });
            if !keep && let Some((_, task)) = self.tasks.remove(&k) {
                task.shutdown.cancel();
            }
        }

        // 2. Spawn new follower replicators.
        for k in desired {
            if self.tasks.contains_key(&k) {
                continue;
            }
            let part = image.partition(&k.0, k.1).cloned();
            let Some(part) = part else { continue };
            let leader = part.leader;
            let Some(broker) = image.broker(leader).cloned() else {
                warn!(
                    topic = %k.0, partition = k.1, leader = leader.0,
                    "leader broker not yet registered in MetadataImage; deferring"
                );
                continue;
            };
            // Resolve the topic's `topic_id` from the same image we're
            // reconciling against. The replicator needs it for the v13+
            // Fetch wire format; without it the leader's handler can't
            // resolve the topic name and returns UNKNOWN_TOPIC_OR_PARTITION.
            let Some(topic_rec) = image.topic(&k.0).cloned() else {
                warn!(
                    topic = %k.0, partition = k.1,
                    "topic record missing from MetadataImage; deferring"
                );
                continue;
            };
            let token = self.shutdown.child_token();
            let target = ReplicatorTaskTarget {
                topic_id: topic_rec.topic_id,
                leader,
                leader_epoch: part.leader_epoch,
            };
            let config =
                self.replicator_config(k.clone(), &topic_rec, &part, &broker, token.clone());
            let handle = tokio::spawn(replicator::run(config));
            self.tasks.insert(
                k,
                ReplicatorTask {
                    shutdown: token,
                    target,
                    handle,
                },
            );
        }

        // 3. Refresh the txn coordinator's view of locally-led
        //    __transaction_state partitions. Cheap (Arc clone + lock).
        if let Some(coord) = &self.txn_coordinator {
            coord.refresh_leader_partitions(image).await;
        }

        // 3b. Refresh the share coordinator's view of locally-led
        //     __share_group_state partitions (KIP-932). Same shape as txn.
        if let Some(coord) = &self.share_coordinator {
            coord.refresh_leader_partitions(image).await;
        }

        // 4. KIP-858: report any (topic, partition) whose owning log-dir UUID
        //    has changed since the last successful report (first materialization
        //    or after a KIP-113 dir swap). Only sends if there is at least one
        //    change; on error the tracker is NOT updated so we retry next tick.
        //    The report submits a non-clobbering V1PartitionDirAssignment delta
        //    (merges one replica's `directories` slot), so it can no longer
        //    revert a concurrent reassignment.
        self.report_dir_assignments(&local_set, image).await;
    }

    fn prune_deleted_topic_partitions(&self, image: &MetadataImage) {
        let current_topic_ids = image
            .topics()
            .map(|topic| (topic.name.clone(), topic.topic_id))
            .collect::<HashMap<_, _>>();
        let obsolete_topics = {
            let mut known_topic_ids = self
                .known_topic_ids
                .lock()
                .expect("replicator supervisor topic identities poisoned");
            let obsolete = known_topic_ids
                .iter()
                .filter(|(name, id)| current_topic_ids.get(*name) != Some(*id))
                .map(|(name, id)| (name.clone(), *id))
                .collect::<HashMap<_, _>>();
            *known_topic_ids = current_topic_ids;
            obsolete
        };
        for partition in self.partitions.arcs() {
            let Some(&topic_id) = obsolete_topics.get(&partition.topic) else {
                continue;
            };
            self.prune_partition(&partition, topic_id, false);
        }
    }

    fn prune_unassigned_partitions(
        &self,
        local_set: &HashSet<TopicPartition>,
        image: &MetadataImage,
    ) {
        for partition in self.partitions.arcs() {
            let key = (partition.topic.clone(), partition.index.get());
            let Some(topic_id) = image.topic(&key.0).map(|topic| topic.topic_id) else {
                continue;
            };
            if image.partition(&key.0, key.1).is_none() || local_set.contains(&key) {
                continue;
            }
            let shard = crate::wal::quorum::registry::ShardId {
                topic_id,
                partition: partition.index,
            };
            self.prune_partition(&partition, topic_id, self.wal_shards.local_is_voter(shard));
        }
    }

    fn prune_partition(
        &self,
        partition: &crate::partition::Partition,
        topic_id: uuid::Uuid,
        preserve_follower: bool,
    ) {
        let topic = &partition.topic;
        let index = partition.index;
        let Some(removed) = self.partitions.remove(topic, index) else {
            return;
        };
        if let Some(writer) = removed.take_writer_handle() {
            writer.abort();
        }
        self.reported_dirs.remove(&(topic.clone(), index.get()));
        let owning_dir = removed.log_dir.load_full();
        let remove = if preserve_follower {
            crate::wal::quorum::remove_leader_shard(
                self.wal_shards.as_ref(),
                &owning_dir,
                topic,
                topic_id,
                index,
            )
        } else {
            crate::wal::quorum::remove_shard(
                self.wal_shards.as_ref(),
                &owning_dir,
                topic,
                topic_id,
                index,
            )
        };
        if let Err(error) = remove {
            warn!(
                topic = %topic,
                partition = index.get(),
                error = %error,
                "failed to prune WAL shard"
            );
        }
        let partition_dir = crate::log_dir::partition_dir(&owning_dir, topic, index.get());
        if let Err(error) = remove_partition_dir(&partition_dir) {
            warn!(
                topic = %topic,
                partition = index.get(),
                path = %partition_dir.display(),
                error = %error,
                "failed to prune partition directory"
            );
        }
    }

    async fn reconcile_local_partitions(
        &self,
        local_set: &HashSet<TopicPartition>,
        image: &MetadataImage,
    ) {
        for key in local_set {
            if let Err(e) = self.materialize_local_partition(image, &key.0, key.1) {
                warn!(
                    topic = %key.0, partition = key.1, error = %e,
                    "failed to materialize local partition"
                );
                continue;
            }
            let Some(part_record) = image.partition(&key.0, key.1).cloned() else {
                continue;
            };
            let Some(part) = self.partitions.get(&key.0, PartitionIndex(key.1)) else {
                continue;
            };
            // Always sync the partition's cached leader + epoch.
            // `Partition::install_leader_change` is idempotent (atomic stores
            // no-op on equal writes).
            let topic_id = image.topic(&key.0).map(|topic| topic.topic_id);
            let promoting_diskless = part.diskless
                && part_record.leader == self.node_id
                && part
                    .current_leader
                    .load(std::sync::atomic::Ordering::Acquire)
                    != self.node_id.0;
            if promoting_diskless {
                let Some(topic_id) = topic_id else {
                    warn!(
                        topic = %key.0,
                        partition = key.1,
                        "cannot prepare diskless promotion without topic identity"
                    );
                    continue;
                };
                let shard = crate::wal::quorum::registry::ShardId {
                    topic_id,
                    partition: PartitionIndex(key.1),
                };
                let engine = self.wal_shards.get(shard);
                let result = part
                    .install_replication_target_after_log_prepare(
                        Some(topic_id),
                        part_record.leader.0,
                        part_record.leader_epoch.0,
                        |log| {
                            let durable = crate::wal::quorum::follower::hydrate_on_promotion(
                                &self.log_dirs,
                                &key.0,
                                shard,
                                self.node_id,
                                &self.log_config,
                                log,
                            )?;
                            if let (Some(durable), Some(engine)) = (durable, engine.as_ref()) {
                                engine.adopt_local_durable_prefix(
                                    durable,
                                    log.log_start_offset(),
                                    log.log_end_offset(),
                                );
                            }
                            Ok(log.producer_state_snapshot())
                        },
                        |snapshot| {
                            self.producer_state.rebuild_from_snapshot(
                                &key.0,
                                PartitionIndex(key.1),
                                snapshot,
                            )
                        },
                    )
                    .await;
                if let Err(error) = result {
                    warn!(
                        topic = %key.0,
                        partition = key.1,
                        error = %error,
                        "failed to prepare diskless promotion"
                    );
                    continue;
                }
            } else {
                part.install_replication_target(
                    topic_id,
                    part_record.leader.0,
                    part_record.leader_epoch.0,
                )
                .await;
            }
            if part_record.leader == self.node_id {
                // Install the *current* ISR from the metadata image (not the
                // full replica set) as ISR membership: using `replicas` would
                // undo any shrink applied via AlterPartition, so
                // isr_maintenance's shrink would never stick (and producers
                // with acks=-1 would stay blocked on lagging followers). The
                // replica set is passed separately so follower-progress
                // tracking survives across reconciles for replicas catching
                // up toward ISR re-admission.
                part.install_isr(&part_record.isr, &part_record.replicas, part_record.leader)
                    .await;
            }
        }
    }

    /// Collect changed `(topic_id, partition, dir_uuid)` assignments from
    /// `local_set` and send `AssignReplicasToDirs` to the controller leader
    /// when at least one assignment has changed since the last successful send.
    async fn report_dir_assignments(
        &self,
        local_set: &HashSet<TopicPartition>,
        image: &MetadataImage,
    ) {
        let (wire, updates) = collect_changed_assignments(
            local_set,
            &self.partitions,
            &self.log_dir_ids,
            image,
            &self.reported_dirs,
        );
        if wire.is_empty() {
            return;
        }
        let req = crate::assign_dirs::build_request(self.broker_id, &wire);
        match self
            .assign_dirs_reporter
            .send(&self.controller, &self.client_id, req)
            .await
        {
            Ok(()) => {
                for (topic, partition, dir_uuid) in updates {
                    self.reported_dirs.insert((topic, partition), dir_uuid);
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "assign_replicas_to_dirs report failed; will retry next reconcile"
                );
            }
        }
    }

    /// Open (or recover) the on-disk `Partition` for `(topic, partition)`
    /// and insert it into the broker's shared `partitions` map.
    /// Idempotent: a no-op if the partition is already present.
    fn materialize_local_partition(
        &self,
        image: &MetadataImage,
        topic: &str,
        partition: i32,
    ) -> Result<(), String> {
        let diskless = crate::broker::diskless_topic_config(image.topic_config(topic));
        materialize_partition(MaterializePartitionConfig {
            partitions: &self.partitions,
            topic,
            topic_id: image.topic(topic).map(|topic| topic.topic_id),
            partition,
            log_dirs: &self.log_dirs,
            log_config: &self.log_config,
            log_dir_status: &self.log_dir_status,
            producer_state: &self.producer_state,
            producer_id_expiration: self.producer_id_expiration,
            max_produce_group: self.max_produce_group,
            partition_writer_queue_depth: self.partition_writer_queue_depth,
            diskless_wal_local_replica_count: self.diskless_wal_local_replica_count,
            diskless,
            hot_tail: Some(self.hot_tail.clone()),
            wal_shards: Some(self.wal_shards.clone()),
            sequencer: diskless.then(|| {
                Arc::new(crate::wal::ControllerSequencer::new(
                    self.controller.clone(),
                )) as Arc<dyn crate::wal::OffsetSequencer>
            }),
        })
    }

    pub(crate) async fn run(self) {
        let mut rx = self.controller.watch_image();
        let mut first_reconcile = true;
        loop {
            let image = rx.borrow().clone();
            if first_reconcile {
                self.reconcile(&image).await;
                first_reconcile = false;
            } else {
                tokio::select! {
                    biased;
                    () = self.shutdown.cancelled() => break,
                    () = self.reconcile(&image) => {}
                }
            }
            tokio::select! {
                () = self.shutdown.cancelled() => break,
                res = rx.changed() => {
                    if res.is_err() {
                        break;
                    }
                }
            }
        }
        let task_keys = self
            .tasks
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in task_keys {
            if let Some((_, task)) = self.tasks.remove(&key) {
                task.shutdown.cancel();
                task.handle.abort();
                let _ = task.handle.await;
            }
        }
        let wal_task_keys = self
            .wal_tasks
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for shard in wal_task_keys {
            if let Some((_, task)) = self.wal_tasks.remove(&shard) {
                task.shutdown.cancel();
                task.handle.abort();
                let _ = task.handle.await;
            }
        }
    }

    pub(crate) fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(self.run())
    }
}

fn remove_partition_dir(path: &std::path::Path) -> std::io::Result<()> {
    match std::fs::remove_dir_all(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        net::SocketAddr,
        sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    };

    use assert2::{assert, check};
    use crabka_metadata::{
        BrokerEndpoint, BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord,
        TopicRecord,
    };
    use crabka_raft::{
        AddVoter, Node, QuorumState, RaftError, ReconfigOutcome, RemoveVoter, SnapshotRange,
        UpdateVoter,
    };
    use crabka_units::{bytes, hours, millis};
    use tokio::sync::watch;
    use uuid::Uuid;

    use super::*;

    #[derive(Debug)]
    struct TestStampSource(AtomicU64);

    impl crabka_log::StampSource for TestStampSource {
        fn next_stamp(&self) -> u64 {
            self.0.fetch_add(1, Ordering::Relaxed)
        }
    }

    fn materialize_test_partition(
        partitions: &Arc<PartitionRegistry>,
        log_dir: &std::path::Path,
        topic: &str,
    ) {
        materialize_partition(MaterializePartitionConfig {
            partitions,
            topic,
            topic_id: None,
            partition: 0,
            log_dirs: &[log_dir.to_path_buf()],
            log_config: &LogConfig::default(),
            log_dir_status: &crate::log_dir_status::LogDirRegistry::default(),
            producer_state: &Arc::new(crate::producer_state::ProducerState::new()),
            producer_id_expiration: hours(24),
            max_produce_group: 1_024,
            partition_writer_queue_depth: 64,
            diskless_wal_local_replica_count: 3,
            diskless: false,
            hot_tail: None,
            wal_shards: None,
            sequencer: None,
        })
        .expect("materialize partition");
    }

    fn append_one(partition: &crate::partition::Partition) -> crabka_log::Offset {
        use crabka_protocol::records::{Record, RecordBatch};

        let mut batch = RecordBatch {
            records: vec![Record::default()],
            ..RecordBatch::default()
        };
        partition
            .log
            .lock()
            .expect("partition log")
            .append(&mut batch)
            .expect("append")
    }

    /// Yield-poll until `cond` holds, with a bounded hang-guard. A real
    /// stall then fails the test deterministically instead of spinning
    /// forever.
    async fn await_until(what: &str, mut cond: impl FnMut() -> bool) {
        for _ in 0..200_000 {
            if cond() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition never held: {what}");
    }

    fn image_with(records: &[MetadataRecord]) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        for r in records {
            img.apply(r);
        }
        img
    }

    fn topic_record(name: &str, partitions: i32) -> MetadataRecord {
        MetadataRecord::V1Topic(TopicRecord {
            name: name.into(),
            topic_id: Uuid::new_v4(),
            partitions,
            replication_factor: 3,
        })
    }

    fn partition_record(
        topic: &str,
        partition: i32,
        leader: NodeId,
        replicas: Vec<NodeId>,
        leader_epoch: i32,
    ) -> MetadataRecord {
        MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.into(),
            partition,
            leader,
            replicas: replicas.clone(),
            isr: replicas,
            leader_epoch: crabka_metadata::LeaderEpoch(leader_epoch),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        })
    }

    fn broker_record(node_id: NodeId) -> BrokerRegistrationRecord {
        BrokerRegistrationRecord {
            node_id,
            broker_epoch: 0,
            incarnation_id: Uuid::new_v4(),
            host: "legacy-host".into(),
            port: 9092,
            rack: None,
            log_dirs: vec![],
            endpoints: vec![BrokerEndpoint {
                name: "INTERNAL".into(),
                host: "internal-host".into(),
                port: 19092,
                protocol: crabka_security::ListenerProtocol::Plaintext,
            }],
            features: std::collections::BTreeMap::new(),
        }
    }

    fn running_replicator_task(
        target: ReplicatorTaskTarget,
        shutdown: CancellationToken,
    ) -> ReplicatorTask {
        let child_shutdown = shutdown.clone();
        ReplicatorTask {
            shutdown,
            target,
            handle: tokio::spawn(async move { child_shutdown.cancelled().await }),
        }
    }

    struct StaticMetadataSource {
        image: Arc<MetadataImage>,
        image_rx: watch::Receiver<Arc<MetadataImage>>,
        leader_rx: watch::Receiver<Option<NodeId>>,
    }

    impl StaticMetadataSource {
        fn new(image: MetadataImage) -> Self {
            let image = Arc::new(image);
            let (_image_tx, image_rx) = watch::channel(image.clone());
            let (_leader_tx, leader_rx) = watch::channel(None);
            Self {
                image,
                image_rx,
                leader_rx,
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::metadata_source::MetadataSource for StaticMetadataSource {
        fn current_image(&self) -> Arc<MetadataImage> {
            self.image.clone()
        }

        fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
            self.image_rx.clone()
        }

        fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
            self.leader_rx.clone()
        }

        fn quorum_state(&self) -> QuorumState {
            QuorumState {
                current_term: 0,
                last_applied_index: 0,
                current_leader: None,
                voters: Vec::new(),
                voter_nodes: std::collections::BTreeMap::new(),
                per_voter_matched_index: std::collections::BTreeMap::new(),
            }
        }

        async fn submit_change(
            &self,
            _records: Vec<MetadataRecord>,
        ) -> Result<crabka_raft::SubmitChangeResult, RaftError> {
            panic!("unused in replicator supervisor tests")
        }

        async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
            panic!("unused in replicator supervisor tests")
        }

        async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
            panic!("unused in replicator supervisor tests")
        }

        fn controller_bound_addr(&self) -> SocketAddr {
            SocketAddr::from(([127, 0, 0, 1], 0))
        }

        fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
            SnapshotRange::NoSnapshot
        }

        async fn trigger_snapshot(&self) -> Result<(), RaftError> {
            panic!("unused in replicator supervisor tests")
        }

        async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("unused in replicator supervisor tests")
        }

        async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("unused in replicator supervisor tests")
        }

        async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("unused in replicator supervisor tests")
        }

        async fn cancel(&self) {}
    }

    #[derive(Default)]
    struct CountingAssignDirsReporter {
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl AssignDirsReporter for CountingAssignDirsReporter {
        async fn send(
            &self,
            _controller: &Arc<dyn crate::metadata_source::MetadataSource>,
            _client_id: &str,
            req: crabka_protocol::owned::assign_replicas_to_dirs_request::AssignReplicasToDirsRequest,
        ) -> Result<(), String> {
            assert!(!req.directories.is_empty());
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn supervisor_fixture(
        image: MetadataImage,
    ) -> (
        ReplicatorSupervisor,
        Arc<PartitionRegistry>,
        Arc<CountingAssignDirsReporter>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let partitions = Arc::new(PartitionRegistry::new());
        let reporter = Arc::new(CountingAssignDirsReporter::default());
        let mut supervisor = ReplicatorSupervisor::new(ReplicatorSupervisorConfig {
            client_dispatch_queue_capacity:
                crabka_client_core::ConnectionDispatchQueueCapacity::default(),
            client_frame_max: crabka_client_core::ClientFrameMax::default(),
            node_id: NodeId(2),
            broker_id: 2,
            controller: Arc::new(StaticMetadataSource::new(image)),
            partitions: partitions.clone(),
            log_dirs: vec![dir.path().to_path_buf()],
            log_config: LogConfig::default(),
            client_id: "supervisor-test".into(),
            shutdown: CancellationToken::new(),
            txn_coordinator: None,
            share_coordinator: None,
            inter_broker_client: Arc::new(crate::network::client::InterBrokerClient::new(
                None, None,
            )),
            inter_broker_listener_protocol: crabka_security::ListenerProtocol::Plaintext,
            inter_broker_server_name: "localhost".into(),
            inter_broker_listener_name: "INTERNAL".into(),
            replication: ReplicationRuntimeConfig::default(),
            throttle_state: Arc::new(ThrottleState::new()),
            log_dir_status: crate::log_dir_status::LogDirRegistry::default(),
            producer_state: Arc::new(crate::producer_state::ProducerState::new()),
            producer_id_expiration: hours(24),
            max_produce_group: 1_024,
            partition_writer_queue_depth: 64,
            diskless_wal_local_replica_count: 3,
            metrics: crate::metrics::BrokerMetrics::default(),
            log_dir_ids: crate::log_dir_id::LogDirIds::resolve(&[dir.path().to_path_buf()]),
            hot_tail: Arc::new(crate::diskless::hot_tail::HotTailCache::default()),
            wal_shards: Arc::new(crate::wal::quorum::registry::WalShardRegistry::new(
                crabka_raft::NodeId(2),
            )),
        });
        supervisor.assign_dirs_reporter = reporter.clone();
        (supervisor, partitions, reporter, dir)
    }

    #[tokio::test]
    async fn network_reporter_send_propagates_controller_resolution_errors() {
        // The real network reporter must surface send_assignments' error
        // (here: no controller leader elected), not swallow it into Ok(()).
        let source: Arc<dyn crate::metadata_source::MetadataSource> =
            Arc::new(StaticMetadataSource::new(MetadataImage::new(Uuid::nil())));
        let err = NetworkAssignDirsReporter::default()
            .send(
                &source,
                "test",
                crabka_protocol::owned::assign_replicas_to_dirs_request::AssignReplicasToDirsRequest::default(),
            )
            .await
            .expect_err("no controller leader must fail");
        assert!(err == "no controller leader");
    }

    #[test]
    fn includes_partition_where_self_is_follower() {
        let img = image_with(&[
            topic_record("t", 1),
            partition_record("t", 0, NodeId(1), vec![NodeId(1), NodeId(2), NodeId(3)], 0),
        ]);
        let d = desired_follower_set(NodeId(2), &img);
        assert!(d.contains(&("t".into(), 0)));
        assert!(d.len() == 1);
    }

    #[test]
    fn desired_follower_set_includes_followers_excludes_leader_and_non_replicas() {
        let img = image_with(&[
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id: Uuid::new_v4(),
                partitions: 1,
                replication_factor: 3,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "t".into(),
                partition: 0,
                leader: crabka_audit::NodeId(1),
                replicas: vec![
                    crabka_audit::NodeId(1),
                    crabka_audit::NodeId(2),
                    crabka_audit::NodeId(3),
                ],
                isr: vec![
                    crabka_audit::NodeId(1),
                    crabka_audit::NodeId(2),
                    crabka_audit::NodeId(3),
                ],
                leader_epoch: crabka_metadata::LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }),
        ]);
        let cases = [
            // Self is a follower replica → included.
            (NodeId(2), HashSet::from_iter([("t".to_string(), 0)])),
            // Self is the leader → excluded.
            (NodeId(1), HashSet::new()),
            // Self is not a replica at all → excluded.
            (NodeId(99), HashSet::new()),
        ];
        for (node_id, want) in cases {
            assert!(
                desired_follower_set(node_id, &img) == want,
                "node {}",
                node_id.0
            );
        }
    }

    #[test]
    fn desired_local_set_exactly_includes_all_local_replicas() {
        let img = image_with(&[
            topic_record("a", 2),
            partition_record("a", 0, NodeId(1), vec![NodeId(1), NodeId(2), NodeId(3)], 0),
            partition_record("a", 1, NodeId(2), vec![NodeId(1), NodeId(2), NodeId(3)], 0),
            topic_record("b", 1),
            partition_record("b", 0, NodeId(3), vec![NodeId(1), NodeId(3)], 0),
            topic_record("c", 1),
            partition_record("c", -1, NodeId(1), vec![NodeId(2), NodeId(4)], 0),
        ]);

        let local = desired_local_set(NodeId(2), &img);

        assert!(
            local
                == HashSet::from_iter([
                    ("a".to_string(), 0),
                    ("a".to_string(), 1),
                    ("c".to_string(), -1),
                ])
        );
    }

    #[test]
    fn desired_wal_placements_cover_only_diskless_topics_and_prefer_distinct_racks() {
        use std::collections::BTreeMap;

        let topic_id = Uuid::from_u128(17);
        let mut overrides = BTreeMap::new();
        overrides.insert("crabka.diskless".into(), "true".into());
        let mut broker1 = broker_record(NodeId(1));
        broker1.rack = Some("a".into());
        let mut broker2 = broker_record(NodeId(2));
        broker2.rack = Some("b".into());
        let mut broker3 = broker_record(NodeId(3));
        broker3.rack = Some("c".into());
        let mut broker4 = broker_record(NodeId(4));
        broker4.rack = Some("a".into());
        let image = image_with(&[
            MetadataRecord::V1BrokerRegistration(broker4),
            MetadataRecord::V1BrokerRegistration(broker2),
            MetadataRecord::V1BrokerRegistration(broker1),
            MetadataRecord::V1BrokerRegistration(broker3),
            MetadataRecord::V1Topic(TopicRecord {
                name: "diskless".into(),
                topic_id,
                partitions: 1,
                replication_factor: 3,
            }),
            partition_record(
                "diskless",
                0,
                NodeId(2),
                vec![NodeId(1), NodeId(2), NodeId(3)],
                0,
            ),
            MetadataRecord::V1TopicConfig(crabka_metadata::TopicConfigRecord {
                topic: "diskless".into(),
                overrides,
            }),
            topic_record("classic", 1),
            partition_record("classic", 0, NodeId(1), vec![NodeId(1)], 0),
        ]);

        let placements = desired_wal_placements(&image, 3);

        assert!(placements.len() == 1);
        assert!(
            placements.get(&crate::wal::quorum::registry::ShardId {
                topic_id,
                partition: PartitionIndex(0),
            }) == Some(&vec![NodeId(2), NodeId(1), NodeId(3)])
        );
    }

    #[test]
    fn desired_wal_followers_include_only_complete_nonleader_placements() {
        use std::collections::BTreeMap;

        let topic_id = Uuid::from_u128(18);
        let mut overrides = BTreeMap::new();
        overrides.insert("crabka.diskless".into(), "true".into());
        let image = image_with(&[
            MetadataRecord::V1BrokerRegistration(broker_record(NodeId(1))),
            MetadataRecord::V1BrokerRegistration(broker_record(NodeId(2))),
            MetadataRecord::V1BrokerRegistration(broker_record(NodeId(3))),
            MetadataRecord::V1Topic(TopicRecord {
                name: "diskless".into(),
                topic_id,
                partitions: 1,
                replication_factor: 1,
            }),
            partition_record("diskless", 0, NodeId(1), vec![NodeId(1)], 7),
            MetadataRecord::V1TopicConfig(crabka_metadata::TopicConfigRecord {
                topic: "diskless".into(),
                overrides,
            }),
        ]);
        let (supervisor, _, _, _) = supervisor_fixture(image.clone());
        let shard = crate::wal::quorum::registry::ShardId {
            topic_id,
            partition: PartitionIndex(0),
        };
        let complete = HashMap::from([(shard, vec![NodeId(1), NodeId(2), NodeId(3)])]);

        let desired = supervisor.desired_wal_followers(&image, &complete);

        assert!(
            desired.get(&shard)
                == Some(&WalFollowerSpec {
                    topic: "diskless".into(),
                    leader: NodeId(1),
                    leader_epoch: crabka_metadata::LeaderEpoch(7),
                })
        );
        let short = HashMap::from([(shard, vec![NodeId(1), NodeId(2)])]);
        assert!(supervisor.desired_wal_followers(&image, &short).is_empty());
    }

    #[tokio::test]
    async fn reconcile_wal_followers_retains_only_the_current_target() {
        use std::collections::BTreeMap;

        let topic_id = Uuid::from_u128(19);
        let image_at_epoch = |leader_epoch| {
            let mut overrides = BTreeMap::new();
            overrides.insert("crabka.diskless".into(), "true".into());
            image_with(&[
                MetadataRecord::V1Topic(TopicRecord {
                    name: "diskless".into(),
                    topic_id,
                    partitions: 1,
                    replication_factor: 1,
                }),
                partition_record("diskless", 0, NodeId(1), vec![NodeId(1)], leader_epoch),
                MetadataRecord::V1TopicConfig(crabka_metadata::TopicConfigRecord {
                    topic: "diskless".into(),
                    overrides,
                }),
            ])
        };
        let image = image_at_epoch(7);
        let (supervisor, _, _, _) = supervisor_fixture(image.clone());
        let shard = crate::wal::quorum::registry::ShardId {
            topic_id,
            partition: PartitionIndex(0),
        };
        let placements = HashMap::from([(shard, vec![NodeId(1), NodeId(2), NodeId(3)])]);
        let target = WalFollowerSpec {
            topic: "diskless".into(),
            leader: NodeId(1),
            leader_epoch: crabka_metadata::LeaderEpoch(7),
        };
        let current = CancellationToken::new();
        let current_task = current.clone();
        supervisor.wal_tasks.insert(
            shard,
            WalFollowerTask {
                shutdown: current.clone(),
                handle: tokio::spawn(async move { current_task.cancelled().await }),
            },
        );
        supervisor.wal_task_targets.insert(shard, target);

        let desired = supervisor.desired_wal_followers(&image, &placements);
        supervisor.stop_obsolete_wal_followers(&desired).await;
        supervisor.spawn_wal_followers(&image, desired);

        assert!(!current.is_cancelled());
        assert!(supervisor.wal_tasks.contains_key(&shard));
        assert!(supervisor.wal_task_targets.contains_key(&shard));

        let next = image_at_epoch(8);
        let desired = supervisor.desired_wal_followers(&next, &placements);
        supervisor.stop_obsolete_wal_followers(&desired).await;
        supervisor.spawn_wal_followers(&next, desired);

        assert!(current.is_cancelled());
        assert!(!supervisor.wal_tasks.contains_key(&shard));
        assert!(!supervisor.wal_task_targets.contains_key(&shard));

        let removed = CancellationToken::new();
        let removed_task = removed.clone();
        supervisor.wal_tasks.insert(
            shard,
            WalFollowerTask {
                shutdown: removed.clone(),
                handle: tokio::spawn(async move { removed_task.cancelled().await }),
            },
        );
        supervisor.wal_task_targets.insert(
            shard,
            WalFollowerSpec {
                topic: "diskless".into(),
                leader: NodeId(1),
                leader_epoch: crabka_metadata::LeaderEpoch(8),
            },
        );
        let follower_shard = crate::wal::quorum::shard_dir(
            &supervisor.log_dirs[0],
            "diskless",
            Some(topic_id),
            PartitionIndex(0),
        );
        std::fs::create_dir_all(follower_shard.join("voter-2")).unwrap();
        std::fs::write(follower_shard.join("voter-2/checkpoint"), b"durable").unwrap();

        let empty = MetadataImage::new(Uuid::nil());
        let desired = supervisor.desired_wal_followers(&empty, &HashMap::new());
        supervisor.stop_obsolete_wal_followers(&desired).await;
        supervisor.spawn_wal_followers(&empty, desired);

        assert!(removed.is_cancelled());
        assert!(!supervisor.wal_tasks.contains_key(&shard));
        assert!(!supervisor.wal_task_targets.contains_key(&shard));
        assert!(!follower_shard.exists());
    }

    #[tokio::test]
    async fn materialize_partition_helper_supports_isr_install() {
        use crabka_log::LogConfig;
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let partitions = Arc::new(PartitionRegistry::new());
        materialize_partition(MaterializePartitionConfig {
            partitions: &partitions,
            topic: "t",
            topic_id: None,
            partition: 0,
            log_dirs: &[dir.path().to_path_buf()],
            log_config: &LogConfig::default(),
            log_dir_status: &crate::log_dir_status::LogDirRegistry::default(),
            producer_state: &Arc::new(crate::producer_state::ProducerState::new()),
            producer_id_expiration: hours(24),
            max_produce_group: 1_024,
            partition_writer_queue_depth: 64,
            diskless_wal_local_replica_count: 3,
            diskless: false,
            hot_tail: None,
            wal_shards: None,
            sequencer: None,
        })
        .expect("materialize");
        let part = partitions.get("t", PartitionIndex(0)).expect("part");
        // Mirror what reconcile does for leader partitions.
        part.install_isr(
            &[
                crabka_audit::NodeId(1),
                crabka_audit::NodeId(2),
                crabka_audit::NodeId(3),
            ],
            &[
                crabka_audit::NodeId(1),
                crabka_audit::NodeId(2),
                crabka_audit::NodeId(3),
            ],
            crabka_audit::NodeId(1),
        )
        .await;
        let st = part.replica_state.lock().await;
        assert!(st.isr.len() == 3);
    }

    #[tokio::test]
    async fn materialized_partition_stamps_only_when_source_is_configured() {
        let disabled_dir = tempfile::tempdir().expect("disabled tempdir");
        let disabled = Arc::new(PartitionRegistry::new());
        materialize_test_partition(&disabled, disabled_dir.path(), "disabled");
        let disabled_part = disabled
            .get("disabled", PartitionIndex(0))
            .expect("disabled partition");
        let disabled_offset = append_one(&disabled_part);
        assert!(disabled_offset == crabka_log::Offset(0));
        assert!(
            disabled_part.stamp_for_offset(disabled_offset).is_none(),
            "Kafka-only partitions must not create internal stamps"
        );

        let enabled_dir = tempfile::tempdir().expect("enabled tempdir");
        let source: Arc<dyn crabka_log::StampSource> =
            Arc::new(TestStampSource(AtomicU64::new(100)));
        let enabled = Arc::new(PartitionRegistry::with_stamp_source(Some(source)));
        materialize_test_partition(&enabled, enabled_dir.path(), "enabled");
        let enabled_part = enabled
            .get("enabled", PartitionIndex(0))
            .expect("enabled partition");
        let enabled_offset = append_one(&enabled_part);
        assert!(enabled_offset == crabka_log::Offset(0));
        assert!(enabled_part.stamp_for_offset(enabled_offset) == Some(100));
    }

    #[tokio::test]
    async fn recovered_partition_installs_source_before_new_appends() {
        use crabka_protocol::records::{Record, RecordBatch};

        let dir = tempfile::tempdir().expect("tempdir");
        let partition_dir = crate::log_dir::partition_dir(dir.path(), "recovered", 0);
        std::fs::create_dir_all(&partition_dir).expect("partition dir");
        let mut existing = Log::open(&partition_dir, LogConfig::default()).expect("open existing");
        existing
            .append(&mut RecordBatch {
                records: vec![Record::default()],
                ..RecordBatch::default()
            })
            .expect("append existing");
        drop(existing);

        let source: Arc<dyn crabka_log::StampSource> =
            Arc::new(TestStampSource(AtomicU64::new(500)));
        let partitions = Arc::new(PartitionRegistry::with_stamp_source(Some(source)));
        materialize_test_partition(&partitions, dir.path(), "recovered");
        let partition = partitions
            .get("recovered", PartitionIndex(0))
            .expect("recovered partition");

        let new_offset = append_one(&partition);
        assert!(new_offset == crabka_log::Offset(1));
        assert!(partition.stamp_for_offset(crabka_log::Offset(0)).is_none());
        assert!(partition.stamp_for_offset(new_offset) == Some(500));
    }

    #[tokio::test]
    async fn materialize_diskless_partition_registers_wal_shard() {
        use crabka_log::LogConfig;
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let partitions = Arc::new(PartitionRegistry::new());
        let topic_id = uuid::Uuid::from_u128(0xD15C);
        let hot_tail = Arc::new(crate::diskless::hot_tail::HotTailCache::default());
        let wal_shards = Arc::new(crate::wal::quorum::registry::WalShardRegistry::new(
            crabka_raft::NodeId(0),
        ));

        materialize_partition(MaterializePartitionConfig {
            partitions: &partitions,
            topic: "diskless",
            topic_id: Some(topic_id),
            partition: 0,
            log_dirs: &[dir.path().to_path_buf()],
            log_config: &LogConfig::default(),
            log_dir_status: &crate::log_dir_status::LogDirRegistry::default(),
            producer_state: &Arc::new(crate::producer_state::ProducerState::new()),
            producer_id_expiration: hours(24),
            max_produce_group: 1_024,
            partition_writer_queue_depth: 64,
            diskless_wal_local_replica_count: 3,
            diskless: true,
            hot_tail: Some(hot_tail),
            wal_shards: Some(wal_shards.clone()),
            sequencer: None,
        })
        .expect("materialize");

        assert!(
            wal_shards
                .get(crate::wal::quorum::registry::ShardId {
                    topic_id,
                    partition: PartitionIndex(0),
                })
                .is_some()
        );
    }

    #[test]
    fn multiple_topics_aggregated() {
        let img = image_with(&[
            MetadataRecord::V1Topic(TopicRecord {
                name: "a".into(),
                topic_id: Uuid::new_v4(),
                partitions: 1,
                replication_factor: 3,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "a".into(),
                partition: 0,
                leader: crabka_audit::NodeId(1),
                replicas: vec![
                    crabka_audit::NodeId(1),
                    crabka_audit::NodeId(2),
                    crabka_audit::NodeId(3),
                ],
                isr: vec![
                    crabka_audit::NodeId(1),
                    crabka_audit::NodeId(2),
                    crabka_audit::NodeId(3),
                ],
                leader_epoch: crabka_metadata::LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }),
            MetadataRecord::V1Topic(TopicRecord {
                name: "b".into(),
                topic_id: Uuid::new_v4(),
                partitions: 2,
                replication_factor: 3,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "b".into(),
                partition: 0,
                leader: crabka_audit::NodeId(3),
                replicas: vec![
                    crabka_audit::NodeId(1),
                    crabka_audit::NodeId(2),
                    crabka_audit::NodeId(3),
                ],
                isr: vec![
                    crabka_audit::NodeId(1),
                    crabka_audit::NodeId(2),
                    crabka_audit::NodeId(3),
                ],
                leader_epoch: crabka_metadata::LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "b".into(),
                partition: 1,
                leader: crabka_audit::NodeId(2),
                replicas: vec![
                    crabka_audit::NodeId(1),
                    crabka_audit::NodeId(2),
                    crabka_audit::NodeId(3),
                ],
                isr: vec![
                    crabka_audit::NodeId(1),
                    crabka_audit::NodeId(2),
                    crabka_audit::NodeId(3),
                ],
                leader_epoch: crabka_metadata::LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }),
        ]);
        let d = desired_follower_set(NodeId(2), &img);
        // b/1 is excluded: self is leader for it.
        assert!(d == HashSet::from_iter([("a".to_string(), 0), ("b".to_string(), 0)]));
        assert!(d.contains(&("a".into(), 0)));
        assert!(d.contains(&("b".into(), 0)));
        assert!(!d.contains(&("b".into(), 1))); // self is leader for b/1
        assert!(d.len() == 2);
    }

    #[test]
    fn resolve_leader_endpoint_prefers_matching_listener() {
        let broker = broker_record(NodeId(1));
        assert!(resolve_leader_endpoint(&broker, "INTERNAL") == ("internal-host".into(), 19092));
        assert!(resolve_leader_endpoint(&broker, "EXTERNAL") == ("legacy-host".into(), 9092));
    }

    #[test]
    fn replicator_task_config_receives_runtime_policy_and_tls_server_name() {
        let image = image_with(&[
            topic_record("t", 1),
            partition_record("t", 0, NodeId(1), vec![NodeId(1), NodeId(2)], 7),
            MetadataRecord::V1BrokerRegistration(broker_record(NodeId(1))),
        ]);
        let (mut supervisor, _partitions, _reporter, _dir) = supervisor_fixture(image.clone());
        supervisor.replication.fetch_max = bytes(2_345_678);
        supervisor.replication.send_error_backoff = millis(37);
        supervisor.inter_broker_server_name = "broker.internal".into();
        let broker = image.broker(NodeId(1)).expect("leader broker");
        let topic = image.topic("t").expect("topic");

        let config = supervisor.replicator_config(
            ("t".into(), 0),
            topic,
            image.partition("t", 0).expect("partition"),
            broker,
            CancellationToken::new(),
        );

        assert!(config.replication.fetch_max == bytes(2_345_678));
        assert!(config.replication.send_error_backoff == millis(37));
        assert!(config.inter_broker_server_name == "broker.internal");
    }

    #[tokio::test]
    async fn reconcile_materializes_leader_partition_and_installs_isr() {
        let img = image_with(&[
            topic_record("t", 1),
            partition_record("t", 0, NodeId(2), vec![NodeId(1), NodeId(2), NodeId(3)], 7),
        ]);
        let (supervisor, partitions, _reporter, _dir) = supervisor_fixture(img.clone());

        supervisor.reconcile(&img).await;

        let part = partitions
            .get("t", PartitionIndex(0))
            .expect("local leader materialized");
        assert!(
            part.current_leader
                .load(std::sync::atomic::Ordering::Acquire)
                == 2,
            "leader cache updated"
        );
        assert!(
            part.current_leader_epoch
                .load(std::sync::atomic::Ordering::Acquire)
                == 7,
            "leader epoch cache updated"
        );
        let state = part.replica_state.lock().await;
        assert!(state.isr == [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect());
    }

    #[tokio::test]
    async fn reconcile_does_not_treat_reserved_diskless_offset_as_durable() {
        use std::collections::BTreeMap;

        use crabka_metadata::PartitionOffsetAdvanceRecord;

        let mut overrides = BTreeMap::new();
        overrides.insert("crabka.diskless".into(), "true".into());
        let img = image_with(&[
            topic_record("diskless", 1),
            partition_record("diskless", 0, NodeId(2), vec![NodeId(2)], 0),
            MetadataRecord::V1TopicConfig(crabka_metadata::TopicConfigRecord {
                topic: "diskless".into(),
                overrides,
            }),
            MetadataRecord::V1PartitionOffsetAdvance(PartitionOffsetAdvanceRecord {
                topic: "diskless".into(),
                partition: 0,
                count: 7,
            }),
        ]);
        let (supervisor, partitions, _reporter, _dir) = supervisor_fixture(img.clone());

        supervisor.reconcile(&img).await;

        let partition = partitions
            .get("diskless", PartitionIndex(0))
            .expect("diskless leader materialized");
        assert!(partition.high_watermark().await == crabka_log::Offset(0));
    }

    #[tokio::test]
    async fn reconcile_materializes_follower_but_does_not_install_isr() {
        let img = image_with(&[
            topic_record("t", 1),
            partition_record("t", 0, NodeId(1), vec![NodeId(1), NodeId(2), NodeId(3)], 7),
        ]);
        let (supervisor, partitions, _reporter, _dir) = supervisor_fixture(img.clone());

        supervisor.reconcile(&img).await;

        let part = partitions
            .get("t", PartitionIndex(0))
            .expect("local follower materialized");
        let state = part.replica_state.lock().await;
        assert!(state.isr.is_empty());
    }

    #[tokio::test]
    async fn reconcile_prunes_deleted_topic_partitions_but_keeps_live_topics() {
        #[derive(Debug, PartialEq, Eq)]
        struct PartitionState {
            topic: &'static str,
            registered: bool,
            directory_exists: bool,
            runtime_reused: Option<bool>,
        }

        let live_topic = topic_record("live", 1);
        let live_partition = partition_record("live", 0, NodeId(2), vec![NodeId(2)], 0);
        let active = image_with(&[
            topic_record("deleted", 1),
            partition_record("deleted", 0, NodeId(2), vec![NodeId(2)], 0),
            live_topic.clone(),
            live_partition.clone(),
            topic_record("recreated", 1),
            partition_record("recreated", 0, NodeId(2), vec![NodeId(2)], 0),
        ]);
        let after_delete = image_with(&[
            live_topic,
            live_partition,
            topic_record("recreated", 1),
            partition_record("recreated", 0, NodeId(2), vec![NodeId(2)], 0),
        ]);
        let (supervisor, partitions, _reporter, dir) = supervisor_fixture(active.clone());
        supervisor
            .materialize_local_partition(&active, "startup-only", 0)
            .expect("startup-only partition");
        supervisor.reconcile(&active).await;
        let original = ["deleted", "live", "recreated", "startup-only"]
            .into_iter()
            .map(|topic| {
                (
                    topic,
                    partitions
                        .get(topic, PartitionIndex(0))
                        .expect("original partition"),
                )
            })
            .collect::<HashMap<_, _>>();
        let deleted_topic_id = active.topic("deleted").expect("deleted topic").topic_id;
        let deleted_shard = crate::wal::quorum::registry::ShardId {
            topic_id: deleted_topic_id,
            partition: PartitionIndex(0),
        };
        supervisor.wal_shards.insert(
            deleted_shard,
            Arc::new(crate::wal::quorum::engine::WalShardEngine::for_logs(
                std::collections::BTreeMap::from([(NodeId(2), original["deleted"].log.clone())]),
            )),
        );
        let deleted_wal_dir = crate::wal::quorum::shard_dir(
            dir.path(),
            "deleted",
            Some(deleted_topic_id),
            PartitionIndex(0),
        );
        std::fs::create_dir_all(&deleted_wal_dir).expect("deleted WAL shard directory");

        supervisor.reconcile(&after_delete).await;

        assert!(supervisor.wal_shards.get(deleted_shard).is_none());
        assert!(!deleted_wal_dir.exists());

        let actual = ["deleted", "live", "recreated", "startup-only"]
            .into_iter()
            .map(|topic| PartitionState {
                topic,
                registered: partitions.contains(topic, PartitionIndex(0)),
                directory_exists: dir.path().join(format!("{topic}-0")).exists(),
                runtime_reused: partitions
                    .get(topic, PartitionIndex(0))
                    .map(|current| Arc::ptr_eq(&original[topic], &current)),
            })
            .collect::<Vec<_>>();
        let expected = vec![
            PartitionState {
                topic: "deleted",
                registered: false,
                directory_exists: false,
                runtime_reused: None,
            },
            PartitionState {
                topic: "live",
                registered: true,
                directory_exists: true,
                runtime_reused: Some(true),
            },
            PartitionState {
                topic: "recreated",
                registered: true,
                directory_exists: true,
                runtime_reused: Some(false),
            },
            PartitionState {
                topic: "startup-only",
                registered: true,
                directory_exists: true,
                runtime_reused: Some(true),
            },
        ];
        assert!(actual == expected);
    }

    #[tokio::test]
    async fn reconcile_prunes_partition_after_local_replica_is_reassigned() {
        let topic = topic_record("moved", 1);
        let assigned = image_with(&[
            topic.clone(),
            partition_record("moved", 0, NodeId(2), vec![NodeId(2)], 0),
        ]);
        let reassigned = image_with(&[
            topic,
            partition_record("moved", 0, NodeId(1), vec![NodeId(1)], 1),
        ]);
        let (supervisor, partitions, _reporter, dir) = supervisor_fixture(assigned.clone());
        supervisor.reconcile(&assigned).await;

        let original = partitions
            .get("moved", PartitionIndex(0))
            .expect("assigned partition");
        let topic_id = assigned.topic("moved").expect("topic").topic_id;
        let shard = crate::wal::quorum::registry::ShardId {
            topic_id,
            partition: PartitionIndex(0),
        };
        supervisor.wal_shards.insert(
            shard,
            Arc::new(crate::wal::quorum::engine::WalShardEngine::for_logs(
                std::collections::BTreeMap::from([(NodeId(2), original.log.clone())]),
            )),
        );
        let wal_dir =
            crate::wal::quorum::shard_dir(dir.path(), "moved", Some(topic_id), PartitionIndex(0));
        std::fs::create_dir_all(&wal_dir).expect("WAL shard directory");

        supervisor.reconcile(&reassigned).await;

        assert!(!partitions.contains("moved", PartitionIndex(0)));
        assert!(!dir.path().join("moved-0").exists());
        assert!(supervisor.wal_shards.get(shard).is_none());
        assert!(!wal_dir.exists());
        assert!(original.take_writer_handle().is_none());
    }

    #[tokio::test]
    async fn reconcile_keeps_partition_until_its_metadata_record_arrives() {
        let image = image_with(&[topic_record("pending", 1)]);
        let (supervisor, partitions, _reporter, dir) = supervisor_fixture(image.clone());
        supervisor
            .materialize_local_partition(&image, "pending", 0)
            .expect("startup partition");
        let original = partitions
            .get("pending", PartitionIndex(0))
            .expect("startup partition");

        supervisor.reconcile(&image).await;

        let current = partitions
            .get("pending", PartitionIndex(0))
            .expect("pending metadata must not delete local storage");
        assert!(Arc::ptr_eq(&original, &current));
        assert!(dir.path().join("pending-0").exists());
    }

    #[test]
    fn remove_partition_dir_is_idempotent_but_reports_other_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("missing");
        remove_partition_dir(&missing).expect("missing directory is already removed");

        let file = dir.path().join("file");
        std::fs::write(&file, b"not a directory").expect("test file");
        assert!(remove_partition_dir(&file).is_err());
    }

    #[tokio::test]
    async fn reconcile_cancels_tasks_for_removed_partitions() {
        let img = MetadataImage::new(Uuid::nil());
        let (supervisor, _partitions, _reporter, _dir) = supervisor_fixture(img.clone());
        let token = CancellationToken::new();
        supervisor.tasks.insert(
            ("stale".into(), 0),
            running_replicator_task(
                ReplicatorTaskTarget {
                    topic_id: Uuid::new_v4(),
                    leader: NodeId(1),
                    leader_epoch: crabka_metadata::LeaderEpoch(0),
                },
                token.clone(),
            ),
        );

        supervisor.reconcile(&img).await;

        check!(token.is_cancelled());
        check!(supervisor.tasks.len() == 0);
    }

    #[tokio::test]
    async fn reconcile_cancels_task_when_target_leader_or_epoch_changes() {
        let img = image_with(&[
            topic_record("t", 1),
            partition_record("t", 0, NodeId(1), vec![NodeId(1), NodeId(2)], 8),
        ]);
        let (supervisor, _partitions, _reporter, _dir) = supervisor_fixture(img.clone());
        let token = CancellationToken::new();
        supervisor.tasks.insert(
            ("t".into(), 0),
            running_replicator_task(
                ReplicatorTaskTarget {
                    topic_id: img.topic("t").expect("topic").topic_id,
                    leader: NodeId(9),
                    leader_epoch: crabka_metadata::LeaderEpoch(7),
                },
                token.clone(),
            ),
        );

        supervisor.reconcile(&img).await;

        check!(token.is_cancelled());
        check!(supervisor.tasks.len() == 0);
    }

    #[tokio::test]
    async fn reconcile_cancels_task_for_recreated_topic_with_same_generation() {
        let new_topic_id = Uuid::new_v4();
        let img = image_with(&[
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id: new_topic_id,
                partitions: 1,
                replication_factor: 2,
            }),
            partition_record("t", 0, NodeId(1), vec![NodeId(1), NodeId(2)], 8),
        ]);
        let (supervisor, _partitions, _reporter, _dir) = supervisor_fixture(img.clone());
        let token = CancellationToken::new();
        supervisor.tasks.insert(
            ("t".into(), 0),
            running_replicator_task(
                ReplicatorTaskTarget {
                    topic_id: Uuid::new_v4(),
                    leader: NodeId(1),
                    leader_epoch: crabka_metadata::LeaderEpoch(8),
                },
                token.clone(),
            ),
        );

        supervisor.reconcile(&img).await;

        assert!(token.is_cancelled());
        assert!(supervisor.tasks.is_empty());
    }

    #[tokio::test]
    async fn reconcile_respawns_exited_replicator_task() {
        let img = image_with(&[
            topic_record("t", 1),
            partition_record("t", 0, NodeId(1), vec![NodeId(1), NodeId(2)], 8),
            MetadataRecord::V1BrokerRegistration(broker_record(NodeId(1))),
        ]);
        let (supervisor, _partitions, _reporter, _dir) = supervisor_fixture(img.clone());
        let stale_shutdown = CancellationToken::new();
        supervisor.tasks.insert(
            ("t".into(), 0),
            ReplicatorTask {
                shutdown: stale_shutdown.clone(),
                target: ReplicatorTaskTarget {
                    topic_id: img.topic("t").expect("topic").topic_id,
                    leader: NodeId(1),
                    leader_epoch: crabka_metadata::LeaderEpoch(8),
                },
                handle: tokio::spawn(async {}),
            },
        );
        await_until("old replicator exits", || {
            supervisor
                .tasks
                .get(&("t".into(), 0))
                .is_some_and(|task| task.handle.is_finished())
        })
        .await;

        supervisor.reconcile(&img).await;

        assert!(stale_shutdown.is_cancelled());
        let replacement = supervisor.tasks.get(&("t".into(), 0)).expect("respawned");
        assert!(!replacement.handle.is_finished());
        replacement.shutdown.cancel();
    }

    #[tokio::test]
    async fn report_dir_assignments_sends_and_records_successful_updates() {
        let topic_id = Uuid::new_v4();
        let img = image_with(&[
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id,
                partitions: 1,
                replication_factor: 1,
            }),
            partition_record("t", 0, NodeId(2), vec![NodeId(2)], 0),
        ]);
        let (supervisor, partitions, reporter, _dir) = supervisor_fixture(img.clone());
        supervisor
            .materialize_local_partition(&img, "t", 0)
            .unwrap();
        let mut local_set = HashSet::new();
        local_set.insert(("t".to_string(), 0));

        supervisor.report_dir_assignments(&local_set, &img).await;

        assert!(reporter.calls.load(Ordering::SeqCst) == 1);
        assert!(supervisor.reported_dirs.contains_key(&("t".to_string(), 0)));

        let part = partitions
            .get("t", PartitionIndex(0))
            .expect("materialized");
        let dir = part.log_dir.load();
        let expected = supervisor.log_dir_ids.id_for(&dir).expect("dir id");
        assert!(
            supervisor
                .reported_dirs
                .get(&("t".to_string(), 0))
                .map(|e| *e)
                == Some(expected)
        );
    }

    #[tokio::test]
    async fn materialize_local_partition_inserts_partition() {
        let img = MetadataImage::new(Uuid::nil());
        let (supervisor, partitions, _reporter, _dir) = supervisor_fixture(img.clone());

        supervisor
            .materialize_local_partition(&img, "t", 0)
            .unwrap();

        assert!(partitions.contains("t", PartitionIndex(0)));
    }

    #[tokio::test]
    async fn run_reconciles_initial_image_before_shutdown() {
        let img = image_with(&[
            topic_record("t", 1),
            partition_record("t", 0, NodeId(2), vec![NodeId(2)], 0),
        ]);
        let (supervisor, partitions, _reporter, _dir) = supervisor_fixture(img);
        supervisor.shutdown.cancel();

        supervisor.run().await;

        assert!(partitions.contains("t", PartitionIndex(0)));
    }

    #[tokio::test]
    async fn push_topic_configs_pushes_overrides_to_local_partition() {
        use std::collections::BTreeMap;

        use crabka_log::LogConfig;
        use crabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
        use tempfile::tempdir;
        use uuid::Uuid;

        // Build an image with a topic + partition record + V1TopicConfig.
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: crabka_audit::NodeId(1),
            replicas: vec![crabka_audit::NodeId(1)],
            isr: vec![crabka_audit::NodeId(1)],
            leader_epoch: crabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
        let mut overrides = BTreeMap::new();
        overrides.insert("retention.ms".to_string(), "60000".to_string());
        img.apply(&MetadataRecord::V1TopicConfig(
            crabka_metadata::TopicConfigRecord {
                topic: "t".into(),
                overrides,
            },
        ));

        // Materialize the partition on disk.
        let dir = tempdir().expect("tempdir");
        let partitions = Arc::new(PartitionRegistry::new());
        materialize_partition(MaterializePartitionConfig {
            partitions: &partitions,
            topic: "t",
            topic_id: None,
            partition: 0,
            log_dirs: &[dir.path().to_path_buf()],
            log_config: &LogConfig::default(),
            log_dir_status: &crate::log_dir_status::LogDirRegistry::default(),
            producer_state: &Arc::new(crate::producer_state::ProducerState::new()),
            producer_id_expiration: hours(24),
            max_produce_group: 1_024,
            partition_writer_queue_depth: 64,
            diskless_wal_local_replica_count: 3,
            diskless: false,
            hot_tail: None,
            wal_shards: None,
            sequencer: None,
        })
        .expect("materialize");

        // Call push_topic_configs directly.
        let mut desired = HashSet::new();
        desired.insert(("t".to_string(), 0));
        push_topic_configs(&desired, &partitions, &img).await;

        // Wait until the writer actor applies the SetLogConfig message and the
        // partition's Log reports retention.ms=60s.
        let part = partitions
            .get("t", PartitionIndex(0))
            .expect("partition materialized");
        await_until("retention.ms=60s applied to partition log", || {
            part.log
                .lock()
                .expect("log lock")
                .config_snapshot()
                .retention
                == Some(crabka_units::minutes(1))
        })
        .await;
        let snap = part.log.lock().expect("log lock").config_snapshot();
        assert!(snap.retention == Some(crabka_units::minutes(1)));
    }

    #[tokio::test]
    async fn push_topic_configs_with_no_overrides_uses_defaults() {
        use crabka_log::LogConfig;
        use crabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
        use tempfile::tempdir;
        use uuid::Uuid;

        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: crabka_audit::NodeId(1),
            replicas: vec![crabka_audit::NodeId(1)],
            isr: vec![crabka_audit::NodeId(1)],
            leader_epoch: crabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));

        let dir = tempdir().expect("tempdir");
        let partitions = Arc::new(PartitionRegistry::new());
        materialize_partition(MaterializePartitionConfig {
            partitions: &partitions,
            topic: "t",
            topic_id: None,
            partition: 0,
            log_dirs: &[dir.path().to_path_buf()],
            log_config: &LogConfig::default(),
            log_dir_status: &crate::log_dir_status::LogDirRegistry::default(),
            producer_state: &Arc::new(crate::producer_state::ProducerState::new()),
            producer_id_expiration: hours(24),
            max_produce_group: 1_024,
            partition_writer_queue_depth: 64,
            diskless_wal_local_replica_count: 3,
            diskless: false,
            hot_tail: None,
            wal_shards: None,
            sequencer: None,
        })
        .expect("materialize");

        let mut desired = HashSet::new();
        desired.insert(("t".to_string(), 0));
        push_topic_configs(&desired, &partitions, &img).await;

        // No overrides → default retention applies. Wait until the writer actor
        // has processed the push (the log already carries the default, so this
        // resolves as soon as the config snapshot matches).
        let part = partitions.get("t", PartitionIndex(0)).expect("partition");
        await_until("default retention applied to partition log", || {
            part.log
                .lock()
                .expect("log lock")
                .config_snapshot()
                .retention
                == LogConfig::default().retention
        })
        .await;
        let snap = part.log.lock().expect("log lock").config_snapshot();
        assert!(snap.retention == LogConfig::default().retention);
    }

    #[tokio::test]
    async fn collect_changed_assignments_reports_new_then_skips_unchanged() {
        use crabka_log::LogConfig;
        use crabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
        use tempfile::tempdir;
        use uuid::Uuid;

        // Build image with a single topic+partition.
        let topic_id = Uuid::new_v4();
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id,
            partitions: 1,
            replication_factor: 1,
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: crabka_audit::NodeId(1),
            replicas: vec![crabka_audit::NodeId(1)],
            isr: vec![crabka_audit::NodeId(1)],
            leader_epoch: crabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));

        // Materialize the partition under a temp dir.
        let dir = tempdir().expect("tempdir");
        let partitions = Arc::new(PartitionRegistry::new());
        materialize_partition(MaterializePartitionConfig {
            partitions: &partitions,
            topic: "t",
            topic_id: None,
            partition: 0,
            log_dirs: &[dir.path().to_path_buf()],
            log_config: &LogConfig::default(),
            log_dir_status: &crate::log_dir_status::LogDirRegistry::default(),
            producer_state: &Arc::new(crate::producer_state::ProducerState::new()),
            producer_id_expiration: hours(24),
            max_produce_group: 1_024,
            partition_writer_queue_depth: 64,
            diskless_wal_local_replica_count: 3,
            diskless: false,
            hot_tail: None,
            wal_shards: None,
            sequencer: None,
        })
        .expect("materialize");

        // Resolve LogDirIds over the same temp dir.
        let log_dir_ids = crate::log_dir_id::LogDirIds::resolve(&[dir.path().to_path_buf()]);

        // Confirm the partition's log_dir equals the temp dir (the parent of
        // the placed partition sub-dir).
        let part = partitions
            .get("t", PartitionIndex(0))
            .expect("part present");
        let loaded_dir = part.log_dir.load();
        assert!(**loaded_dir == dir.path().to_path_buf());

        let dir_uuid = log_dir_ids.id_for(dir.path()).expect("dir uuid resolvable");

        let mut local_set = HashSet::new();
        local_set.insert(("t".to_string(), 0));
        let reported_dirs: dashmap::DashMap<(String, i32), uuid::Uuid> = dashmap::DashMap::new();

        // First call: nothing reported yet → one wire entry + one update entry.
        let (wire, updates) = collect_changed_assignments(
            &local_set,
            &partitions,
            &log_dir_ids,
            &img,
            &reported_dirs,
        );
        assert!(wire == vec![(topic_id, 0, dir_uuid)]);
        assert!(updates == vec![("t".to_string(), 0, dir_uuid)]);

        // Simulate a successful send: insert the tracker update.
        for (topic, partition, uuid) in updates {
            reported_dirs.insert((topic, partition), uuid);
        }

        // Second call: already reported → both vecs empty.
        let (wire2, updates2) = collect_changed_assignments(
            &local_set,
            &partitions,
            &log_dir_ids,
            &img,
            &reported_dirs,
        );
        assert!(wire2.is_empty());
        assert!(updates2.is_empty());
    }
}
