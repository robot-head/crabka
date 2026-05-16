//! KIP-460 auto preferred-replica rebalance. A background task on the
//! controller leader periodically scans every partition; for each
//! where `select_new_leader_for_partition(Preferred)` succeeds, queues
//! a `V1Partition` update. Submits in one batch per tick when the
//! cluster-wide imbalance ratio crosses the configured threshold.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use crabka_metadata::{MetadataImage, MetadataRecord};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::heartbeat::controller_state::ControllerLivenessState;
use crate::leader_election::{ElectionType, select_new_leader_for_partition};

/// Minimal trait for the controller surface we use. Lets tests inject
/// a mock without spinning up real raft.
#[async_trait]
pub(crate) trait ControllerLike: Send + Sync {
    fn is_leader(&self) -> bool;
    fn current_image(&self) -> Arc<MetadataImage>;
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String>;
}

#[derive(Debug, Clone)]
pub(crate) struct AutoRebalanceConfig {
    pub check_interval: Duration,
    pub imbalance_threshold_pct: u32,
}

/// Spawned task entry point.
pub(crate) async fn run(
    controller: Arc<dyn ControllerLike>,
    liveness: Arc<ControllerLivenessState>,
    cfg: AutoRebalanceConfig,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(cfg.check_interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => {},
            () = shutdown.cancelled() => {
                info!("auto-rebalance task shutting down");
                return;
            }
        }
        if !controller.is_leader() {
            debug!("auto-rebalance tick skipped: not controller leader");
            continue;
        }
        rebalance_tick(&*controller, &liveness, &cfg).await;
    }
}

pub(crate) async fn rebalance_tick(
    controller: &dyn ControllerLike,
    liveness: &ControllerLivenessState,
    cfg: &AutoRebalanceConfig,
) {
    let image = controller.current_image();
    let mut to_submit: Vec<MetadataRecord> = Vec::new();
    let mut total: u64 = 0;
    for topic in image.topics() {
        for p in image.partitions_of(&topic.name) {
            total += 1;
            if let Ok(new_pr) = select_new_leader_for_partition(
                &image,
                liveness,
                &topic.name,
                p.partition,
                ElectionType::Preferred,
            )
            .await
            {
                // PreferredAlreadyLeader and any other Err are silently skipped this tick.
                to_submit.push(MetadataRecord::V1Partition(new_pr));
            }
        }
    }
    let imbalanced = to_submit.len() as u64;
    if total == 0 {
        return;
    }
    let pct = (imbalanced * 100) / total;
    if pct < u64::from(cfg.imbalance_threshold_pct) {
        debug!(imbalanced, total, pct, "auto-rebalance: below threshold");
        return;
    }
    info!(count = imbalanced, "auto-rebalance: submitting elections");
    if let Err(e) = controller.submit_change(to_submit).await {
        warn!(error = %e, "auto-rebalance submit failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{PartitionRecord, TopicRecord};
    use std::sync::Mutex;
    use uuid::Uuid;

    struct MockController {
        image: Arc<MetadataImage>,
        is_leader: bool,
        submitted: Mutex<Vec<MetadataRecord>>,
    }

    #[async_trait]
    impl ControllerLike for MockController {
        fn is_leader(&self) -> bool {
            self.is_leader
        }
        fn current_image(&self) -> Arc<MetadataImage> {
            self.image.clone()
        }
        async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String> {
            self.submitted.lock().unwrap().extend(records);
            Ok(())
        }
    }

    fn img_with_n_partitions(imbalanced: usize, balanced: usize) -> Arc<MetadataImage> {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "foo".into(),
            topic_id: Uuid::nil(),
            partitions: i32::try_from(imbalanced + balanced).expect("partition count fits i32"),
            replication_factor: 3,
        }));
        let mut p = 0i32;
        // Imbalanced: leader = 2 (not preferred). ISR has all three.
        for _ in 0..imbalanced {
            img.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: "foo".into(),
                partition: p,
                leader: 2,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
                leader_epoch: 5,
                adding_replicas: vec![],
                removing_replicas: vec![],
            }));
            p += 1;
        }
        // Balanced: leader = 1 (preferred).
        for _ in 0..balanced {
            img.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: "foo".into(),
                partition: p,
                leader: 1,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
                leader_epoch: 5,
                adding_replicas: vec![],
                removing_replicas: vec![],
            }));
            p += 1;
        }
        Arc::new(img)
    }

    async fn liveness_all_alive() -> ControllerLivenessState {
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for n in [1, 2, 3] {
            l.record_heartbeat(n).await;
        }
        l
    }

    #[tokio::test]
    async fn below_threshold_skips_submit() {
        // 5 imbalanced out of 100 → 5%; threshold 10% → no submit.
        let mock = MockController {
            image: img_with_n_partitions(5, 95),
            is_leader: true,
            submitted: Mutex::new(Vec::new()),
        };
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: Duration::from_mins(5),
            imbalance_threshold_pct: 10,
        };
        rebalance_tick(&mock, &liveness, &cfg).await;
        assert!(mock.submitted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn above_threshold_submits_imbalanced_set() {
        // 20 imbalanced out of 100 → 20%; threshold 10% → submit 20.
        let mock = MockController {
            image: img_with_n_partitions(20, 80),
            is_leader: true,
            submitted: Mutex::new(Vec::new()),
        };
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: Duration::from_mins(5),
            imbalance_threshold_pct: 10,
        };
        rebalance_tick(&mock, &liveness, &cfg).await;
        let submitted = mock.submitted.lock().unwrap();
        assert_eq!(submitted.len(), 20);
        // Every submitted record must promote preferred (replicas[0] = 1).
        for record in submitted.iter() {
            match record {
                MetadataRecord::V1Partition(p) => assert_eq!(p.leader, 1),
                _ => panic!("unexpected record type"),
            }
        }
    }
}
