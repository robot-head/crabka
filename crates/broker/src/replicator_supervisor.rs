//! Subscribes to the controller's metadata-image watch channel and
//! diffs the desired follower-replication assignments on each apply.
//! Spawns a `replicator::run` task per new (topic, partition); cancels
//! tasks for partitions removed from the image.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crabka_log::LogConfig;
use crabka_metadata::MetadataImage;
use crabka_raft::{ControllerHandle, NodeId};

use crate::partition::Partition;
use crate::replicator;

/// Free-function shape used both inside the supervisor and by tests.
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

pub(crate) struct ReplicatorSupervisor {
    node_id: NodeId,
    controller: Arc<ControllerHandle>,
    partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    log_dir: PathBuf,
    log_config: LogConfig,
    client_id: String,
    tasks: DashMap<(String, i32), CancellationToken>,
    shutdown: CancellationToken,
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
        }
    }

    #[allow(clippy::unused_async)] // tokio::spawn inside; async needed for Task 9 test harness
    pub(crate) async fn reconcile(&self, image: &MetadataImage) {
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

        // 2. Spawn new.
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
            let token = CancellationToken::new();
            self.tasks.insert(k.clone(), token.clone());
            tokio::spawn(replicator::run(replicator::Config {
                node_id: self.node_id,
                topic: k.0,
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
