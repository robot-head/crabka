//! Subscribes to the controller's metadata-image watch channel and
//! on each apply:
//!
//! 1. **Materializes the local on-disk partition** for any
//!    `(topic, partition)` where this broker is in `replicas`,
//!    regardless of leader/follower role. The `CreateTopics` handler
//!    used to do this itself, but with slice-8 round-robin placement
//!    the broker that handles the request usually isn't the partition
//!    leader — so the lazy supervisor-driven path is the only one
//!    that materializes the partition on the leader broker reliably.
//!
//! 2. **Spawns a `replicator::run` task** per `(topic, partition)`
//!    where this broker is in `replicas` but is NOT the leader, and
//!    cancels tasks for partitions removed from the image.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crabka_log::{Log, LogConfig};
use crabka_metadata::MetadataImage;
use crabka_raft::{ControllerHandle, NodeId};

use crate::broker::spawn_partition;
use crate::partition::Partition;
use crate::replicator;
use crate::txn::coordinator::TxnCoordinator;

/// `(topic, partition)` pairs where `node_id` is in `replicas` AND
/// `leader != node_id` — i.e., the broker should run a follower
/// replicator task.
pub(crate) fn desired_follower_set(
    node_id: NodeId,
    image: &MetadataImage,
) -> HashSet<(String, i32)> {
    let mut out = HashSet::new();
    for t in image.topics() {
        for p in image.partitions_of(&t.name) {
            if p.replicas.contains(&node_id) && p.leader != node_id {
                out.insert((p.topic.clone(), p.partition));
            }
        }
    }
    out
}

/// `(topic, partition)` pairs where `node_id` is in `replicas`,
/// regardless of leader/follower role — every entry here means this
/// broker hosts partition data on disk and must materialize the
/// on-disk `Partition` locally.
pub(crate) fn desired_local_set(node_id: NodeId, image: &MetadataImage) -> HashSet<(String, i32)> {
    let mut out = HashSet::new();
    for t in image.topics() {
        for p in image.partitions_of(&t.name) {
            if p.replicas.contains(&node_id) {
                out.insert((p.topic.clone(), p.partition));
            }
        }
    }
    out
}

/// Open (or recover) the on-disk `Partition` for `(topic, partition)` and
/// insert it into `partitions` using `DashMap::entry().or_insert_with()`.
///
/// This is the canonical, race-free materialization helper. Both the
/// `ReplicatorSupervisor` reconcile loop and the `InitProducerId` handler
/// (first-touch path) call this function — the `DashMap` entry API ensures
/// that two concurrent callers for the same key can never both spawn
/// independent writer tasks.
///
/// Returns `Ok(())` if the partition is already present (no-op) or was
/// successfully opened. Returns `Err(String)` on I/O failure.
pub(crate) fn materialize_partition(
    partitions: &DashMap<(String, i32), Arc<Partition>>,
    topic: &str,
    partition: i32,
    log_dir: &std::path::Path,
    log_config: &LogConfig,
) -> Result<(), String> {
    use dashmap::mapref::entry::Entry;

    // `entry()` takes a write-lock on the shard for this key for the
    // duration of the closure — only one thread can be inside
    // `or_try_insert_with` for a given key at a time, eliminating the
    // TOCTOU race that existed with the old `contains_key` + `insert`
    // pattern.
    match partitions.entry((topic.to_string(), partition)) {
        Entry::Occupied(_) => Ok(()),
        Entry::Vacant(slot) => {
            let dir = log_dir.join(format!("{topic}-{partition}"));
            std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
            let log = Log::open(&dir, log_config.clone()).map_err(|e| format!("Log::open: {e}"))?;
            let part = spawn_partition(topic.to_string(), partition, log);
            slot.insert(part);
            Ok(())
        }
    }
}

pub(crate) struct ReplicatorSupervisor {
    node_id: NodeId,
    controller: Arc<ControllerHandle>,
    partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    log_dir: PathBuf,
    log_config: LogConfig,
    client_id: String,
    tasks: DashMap<(String, i32), CancellationToken>,
    shutdown: CancellationToken,
    txn_coordinator: Option<Arc<TxnCoordinator>>,
}

impl ReplicatorSupervisor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        node_id: NodeId,
        controller: Arc<ControllerHandle>,
        partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
        log_dir: PathBuf,
        log_config: LogConfig,
        client_id: String,
        shutdown: CancellationToken,
        txn_coordinator: Option<Arc<TxnCoordinator>>,
    ) -> Self {
        Self {
            node_id,
            controller,
            partitions,
            log_dir,
            log_config,
            client_id,
            tasks: DashMap::new(),
            shutdown,
            txn_coordinator,
        }
    }

    pub(crate) async fn reconcile(&self, image: &MetadataImage) {
        // 0. Materialize the on-disk partition for every assignment where
        //    self is in `replicas`, regardless of leader/follower role.
        //    Additionally: for every partition where self is leader,
        //    install the static ISR (= replicas) into the partition's
        //    ReplicaState so the HW computation has the correct membership.
        for key in desired_local_set(self.node_id, image) {
            if let Err(e) = self.materialize_local_partition(&key.0, key.1) {
                warn!(
                    topic = %key.0, partition = key.1, error = %e,
                    "failed to materialize local partition"
                );
                continue;
            }
            let Some(part_record) = image.partition(&key.0, key.1).cloned() else {
                continue;
            };
            if part_record.leader != self.node_id {
                continue;
            }
            let Some(part) = self
                .partitions
                .get(&(key.0.clone(), key.1))
                .map(|e| e.value().clone())
            else {
                continue;
            };
            part.install_isr(&part_record.replicas, part_record.leader).await;
        }

        let desired = desired_follower_set(self.node_id, image);

        // 1. Cancel removed.
        let current: Vec<(String, i32)> = self.tasks.iter().map(|e| e.key().clone()).collect();
        for k in current {
            if !desired.contains(&k)
                && let Some((_, token)) = self.tasks.remove(&k)
            {
                token.cancel();
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
                    topic = %k.0, partition = k.1, leader,
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
            let token = CancellationToken::new();
            self.tasks.insert(k.clone(), token.clone());
            tokio::spawn(replicator::run(replicator::Config {
                node_id: self.node_id,
                topic: k.0,
                topic_id: crabka_protocol::primitives::uuid::Uuid(topic_rec.topic_id.into_bytes()),
                partition: k.1,
                leader_node_id: leader,
                leader_addr: format!("{}:{}", broker.host, broker.port),
                partitions: self.partitions.clone(),
                log_dir: self.log_dir.clone(),
                log_config: self.log_config.clone(),
                client_id: self.client_id.clone(),
                shutdown: token,
            }));
        }

        // 3. Refresh the txn coordinator's view of locally-led
        //    __transaction_state partitions. Cheap (Arc clone + lock).
        if let Some(coord) = &self.txn_coordinator {
            coord.refresh_leader_partitions(image).await;
        }
    }

    /// Open (or recover) the on-disk `Partition` for `(topic, partition)`
    /// and insert it into the broker's shared `partitions` map.
    /// Idempotent: a no-op if the partition is already present.
    fn materialize_local_partition(&self, topic: &str, partition: i32) -> Result<(), String> {
        materialize_partition(
            &self.partitions,
            topic,
            partition,
            &self.log_dir,
            &self.log_config,
        )
    }

    pub(crate) async fn run(self) {
        let mut rx = self.controller.watch_image();
        loop {
            let image = rx.borrow().clone();
            self.reconcile(&image).await;
            tokio::select! {
                () = self.shutdown.cancelled() => break,
                res = rx.changed() => {
                    if res.is_err() {
                        break;
                    }
                }
            }
        }
        for entry in &self.tasks {
            entry.value().cancel();
        }
    }

    pub(crate) fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(self.run())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
    use uuid::Uuid;

    fn image_with(records: &[MetadataRecord]) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        for r in records {
            img.apply(r);
        }
        img
    }

    #[test]
    fn includes_partition_where_self_is_follower() {
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
                leader: 1,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
            }),
        ]);
        let d = desired_follower_set(2, &img);
        assert!(d.contains(&("t".into(), 0)));
        assert_eq!(d.len(), 1);
    }

    #[test]
    fn excludes_partition_where_self_is_leader() {
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
                leader: 1,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
            }),
        ]);
        assert!(desired_follower_set(1, &img).is_empty());
    }

    #[test]
    fn excludes_partition_where_self_is_not_a_replica() {
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
                leader: 1,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
            }),
        ]);
        assert!(desired_follower_set(99, &img).is_empty());
    }

    #[tokio::test]
    async fn materialize_partition_helper_supports_isr_install() {
        use crabka_log::LogConfig;
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let partitions = Arc::new(DashMap::new());
        materialize_partition(&partitions, "t", 0, dir.path(), &LogConfig::default())
            .expect("materialize");
        let part = partitions
            .get(&("t".to_string(), 0))
            .expect("part")
            .value()
            .clone();
        // Mirror what reconcile does for leader partitions.
        part.install_isr(&[1, 2, 3], 1).await;
        let st = part.replica_state.lock().await;
        assert_eq!(st.isr.len(), 3);
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
                leader: 1,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
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
                leader: 3,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "b".into(),
                partition: 1,
                leader: 2,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
            }),
        ]);
        let d = desired_follower_set(2, &img);
        assert!(d.contains(&("a".into(), 0)));
        assert!(d.contains(&("b".into(), 0)));
        assert!(!d.contains(&("b".into(), 1))); // self is leader for b/1
        assert_eq!(d.len(), 2);
    }
}
