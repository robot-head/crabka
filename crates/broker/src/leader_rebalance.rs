//! KIP-460 auto preferred-replica rebalance.
//!
//! A background task on the controller leader scans every partition
//! periodically. For each partition where
//! `select_new_leader_for_partition(Preferred)` succeeds, the task queues a
//! `V1Partition` update. The task submits one batch per tick when the
//! cluster-wide imbalance ratio crosses the configured threshold.

#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;
use crabka_metadata::{MetadataImage, MetadataRecord};
use crabka_units::{
    Ratio, Time,
    convert::{RatioExt, TimeExt as _},
    fraction,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    heartbeat::controller_state::ControllerLivenessState,
    leader_election::{ElectionType, select_new_leader_for_partition},
};

/// Minimal trait for the controller surface this module uses. It lets tests
/// inject a mock without a real raft cluster.
#[async_trait]
pub(crate) trait ControllerLike: Send + Sync {
    fn is_leader(&self) -> bool;
    fn current_image(&self) -> Arc<MetadataImage>;
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String>;
}

#[derive(Debug, Clone)]
pub(crate) struct AutoRebalanceConfig {
    pub check_interval: Time,
    pub imbalance_threshold: Ratio,
}

/// Spawned task entry point.
pub(crate) async fn run(
    controller: Arc<dyn ControllerLike>,
    liveness: Arc<ControllerLivenessState>,
    cfg: AutoRebalanceConfig,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(cfg.check_interval.to_std());
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
    // Single O(P) walk over every partition.
    for pr in image.all_partitions() {
        total += 1;
        if let Ok(new_pr) = select_new_leader_for_partition(
            &image,
            liveness,
            &pr.topic,
            pr.partition,
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
    // A dimensioned ratio, not a truncated integer percentage: at 100 total
    // partitions with 10.9% imbalanced, the old `(imbalanced * 100) / total`
    // read 10, sat on a 10% threshold, and declared the cluster balanced.
    // `u32` keeps both widenings lossless; a broker with more than 4 billion
    // partitions has lost long before the threshold matters.
    let imbalanced_f64 = f64::from(u32::try_from(imbalanced).unwrap_or(u32::MAX));
    let total_f64 = f64::from(u32::try_from(total).unwrap_or(u32::MAX));
    let ratio = fraction(imbalanced_f64 / total_f64);
    if ratio < cfg.imbalance_threshold {
        let pct = ratio.percent_f64();
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
    use std::{sync::Mutex, time::Duration};

    use assert2::assert;
    use crabka_metadata::{PartitionRecord, TopicRecord};
    use crabka_units::{millis, minutes, percent, secs};
    use uuid::Uuid;

    use super::*;

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
                leader_epoch: crabka_metadata::LeaderEpoch(5),
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
                leader_epoch: crabka_metadata::LeaderEpoch(5),
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
        let l = ControllerLivenessState::new(secs(10));
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
            check_interval: minutes(5),
            imbalance_threshold: percent(10),
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
            check_interval: minutes(5),
            imbalance_threshold: percent(10),
        };
        rebalance_tick(&mock, &liveness, &cfg).await;
        assert!(mock.submitted.lock().unwrap().len() == 10);
    }

    /// The threshold is a [`Ratio`], so the code compares a fraction that
    /// falls between two whole percentages at full precision.
    /// `floor(100 * r) < T` and `r < T / 100` agree for every integer `T`.
    /// This test therefore pins that the move away from the old truncating
    /// `(imbalanced * 100) / total` left the KIP-460 decision boundary
    /// exactly where it was.
    #[tokio::test]
    async fn fractional_percentages_compare_against_the_threshold_exactly() {
        // 200 partitions gives half-percent granularity either side of 10%.
        for (imbalanced, balanced, want_submitted) in
            [(19_usize, 181_usize, 0_usize), (21, 179, 21)]
        {
            let mock = MockController::new(img_with_n_partitions(imbalanced, balanced), true);
            let liveness = liveness_all_alive().await;
            let cfg = AutoRebalanceConfig {
                check_interval: minutes(5),
                imbalance_threshold: percent(10),
            };

            rebalance_tick(&mock, &liveness, &cfg).await;

            assert!(
                mock.submitted.lock().unwrap().len() == want_submitted,
                "{imbalanced}/{} imbalanced",
                imbalanced + balanced
            );
        }
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
            check_interval: secs(1),
            imbalance_threshold: <Ratio as RatioExt>::ZERO,
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
            check_interval: minutes(5),
            imbalance_threshold: percent(10),
        };
        rebalance_tick(&mock, &liveness, &cfg).await;
        let submitted = mock.submitted.lock().unwrap();
        assert!(submitted.len() == 20);
        // Every submitted record must promote preferred (replicas[0] = 1).
        for record in submitted.iter() {
            match record {
                MetadataRecord::V1Partition(p) => assert!(p.leader == crabka_audit::NodeId(1)),
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
                check_interval: millis(10),
                imbalance_threshold: <Ratio as RatioExt>::ZERO,
            },
            shutdown.clone(),
        ));

        tokio::time::timeout(Duration::from_millis(500), async {
            while controller
                .submit_calls
                .load(std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("leader auto-rebalance loop should submit");

        shutdown.cancel();
        task.await.unwrap();
        assert!(!controller.submitted.lock().unwrap().is_empty());
    }
}
