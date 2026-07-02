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
    // Single O(P) walk over every partition instead of the quadratic
    // topics() × partitions_of() scan.
    for ((topic_name, partition), _pr) in image.all_partitions() {
        total += 1;
        if let Ok(new_pr) = select_new_leader_for_partition(
            &image,
            liveness,
            topic_name,
            *partition,
            ElectionType::Preferred,
        )
        .await
        {
            // PreferredAlreadyLeader and any other Err are silently skipped this tick.
            to_submit.push(MetadataRecord::V1Partition(new_pr));
        }
    }
    let imbalanced = to_submit.len() as u64;
    if total == 0 {
        return;
    }
    // Nothing imbalanced → nothing to do. Submitting an empty batch still
    // writes a raft entry and re-broadcasts the metadata image, churning
    // every broker's reconcile loop once per tick — which, at a 0%
    // threshold + 1s interval, starves ISR re-admission of catching-up
    // replicas. Bail before the threshold math (which can't gate the
    // empty case at threshold 0, since `0 < 0` is false).
    if to_submit.is_empty() {
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
    use assert2::assert;
    use crabka_metadata::{PartitionRecord, TopicRecord};
    use std::sync::Mutex;
    use uuid::Uuid;

    struct MockController {
        image: Arc<MetadataImage>,
        is_leader: bool,
        submitted: Mutex<Vec<MetadataRecord>>,
        submit_calls: std::sync::atomic::AtomicUsize,
    }

    impl MockController {
        fn new(image: Arc<MetadataImage>, is_leader: bool) -> Self {
            Self {
                image,
                is_leader,
                submitted: Mutex::new(Vec::new()),
                submit_calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }
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
            self.submit_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
                directories: vec![],
                partition_epoch: 0,
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
                directories: vec![],
                partition_epoch: 0,
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
        let mock = MockController::new(img_with_n_partitions(5, 95), true);
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: Duration::from_mins(5),
            imbalance_threshold_pct: 10,
        };
        rebalance_tick(&mock, &liveness, &cfg).await;
        assert!(mock.submitted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn exact_threshold_submits_imbalanced_set() {
        // 10 imbalanced out of 100 is exactly 10%; threshold 10% should submit.
        let mock = MockController::new(img_with_n_partitions(10, 90), true);
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: Duration::from_mins(5),
            imbalance_threshold_pct: 10,
        };
        rebalance_tick(&mock, &liveness, &cfg).await;
        assert!(mock.submitted.lock().unwrap().len() == 10);
    }

    #[tokio::test]
    async fn zero_imbalance_does_not_submit_empty_batch() {
        // Every partition is already balanced (leader == preferred). Even
        // with threshold 0% the tick must NOT call submit_change: an empty
        // batch still writes a spurious raft entry, which broadcasts the
        // metadata image and churns every broker's reconcile loop once per
        // tick (starving ISR re-admission of catching-up replicas).
        let mock = MockController::new(img_with_n_partitions(0, 5), true);
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: Duration::from_secs(1),
            imbalance_threshold_pct: 0,
        };
        rebalance_tick(&mock, &liveness, &cfg).await;
        assert!(
            mock.submit_calls.load(std::sync::atomic::Ordering::SeqCst) == 0,
            "must not submit when there is nothing to rebalance"
        );
    }

    #[tokio::test]
    async fn above_threshold_submits_imbalanced_set() {
        // 20 imbalanced out of 100 → 20%; threshold 10% → submit 20.
        let mock = MockController::new(img_with_n_partitions(20, 80), true);
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: Duration::from_mins(5),
            imbalance_threshold_pct: 10,
        };
        rebalance_tick(&mock, &liveness, &cfg).await;
        let submitted = mock.submitted.lock().unwrap();
        assert!(submitted.len() == 20);
        // Every submitted record must promote preferred (replicas[0] = 1).
        for record in submitted.iter() {
            match record {
                MetadataRecord::V1Partition(p) => assert!(p.leader == 1),
                _ => panic!("unexpected record type"),
            }
        }
    }

    #[tokio::test]
    async fn run_submits_when_controller_is_leader() {
        let controller = Arc::new(MockController::new(img_with_n_partitions(1, 0), true));
        let controller_for_run: Arc<dyn ControllerLike> = controller.clone();
        let liveness = Arc::new(liveness_all_alive().await);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            controller_for_run,
            liveness,
            AutoRebalanceConfig {
                check_interval: Duration::from_millis(10),
                imbalance_threshold_pct: 0,
            },
            shutdown.clone(),
        ));

        tokio::time::timeout(Duration::from_millis(500), async {
            while controller
                .submit_calls
                .load(std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("leader auto-rebalance loop should submit");

        shutdown.cancel();
        task.await.unwrap();
        assert!(!controller.submitted.lock().unwrap().is_empty());
    }
}
