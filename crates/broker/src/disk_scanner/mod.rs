//! Periodic per-partition disk-usage scanner. Spawned by
//! `Broker::start` when `--partition-disk-scan-interval-secs > 0`.
//! Each tick walks the log directory for every known
//! (topic, partition), sums regular file sizes, and updates the
//! `partition_disk_bytes` gauge.

pub mod scan;

use std::path::PathBuf;

use crabka_units::{Time, convert::TimeExt as _};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    log_dir,
    metrics::{BrokerMetrics, PartitionLabel},
};

pub struct DiskScanner {
    pub log_dirs: Vec<PathBuf>,
    pub interval: Time,
    pub metrics: BrokerMetrics,
    pub shutdown: CancellationToken,
}

impl DiskScanner {
    pub async fn run(self) {
        info!(
            interval_secs = self.interval.secs_i64(),
            "disk scanner started"
        );
        let mut ticker = interval(self.interval.to_std());
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                () = self.shutdown.cancelled() => {
                    info!("disk scanner shutting down");
                    return;
                }
            }
            self.tick_once();
        }
    }

    fn tick_once(&self) {
        let partitions = match log_dir::scan_all(&self.log_dirs) {
            Ok(p) => p,
            Err(e) => {
                warn!(error = %e, "disk scanner: log_dir::scan_all failed; skipping tick");
                return;
            }
        };
        for (topic, partition, owning_dir) in partitions {
            let path = log_dir::partition_dir(&owning_dir, &topic, partition);
            match scan::sum_partition_dir(&path) {
                Ok(bytes) => {
                    let lbl = PartitionLabel { topic, partition };
                    self.metrics
                        .partition_disk_bytes
                        .get_or_create(&lbl)
                        .set(i64::try_from(bytes).unwrap_or(i64::MAX));
                }
                Err(e) => {
                    warn!(?topic, partition, error = %e, "disk scanner: sum_partition_dir failed; skipping partition");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, time::Duration};

    use assert2::assert;
    use crabka_units::{hours, millis, minutes};

    use super::*;

    #[test]
    fn tick_once_sets_gauge_for_each_partition() {
        let tmp = tempfile::tempdir().unwrap();
        // Materialize two partition dirs the way the broker would.
        let p0 = tmp.path().join("t-0");
        let p1 = tmp.path().join("t-1");
        std::fs::create_dir_all(&p0).unwrap();
        std::fs::create_dir_all(&p1).unwrap();
        // Scope handles so they close before tick_once walks the dir
        // (Windows reports stale dir metadata while files are still open).
        {
            let mut f0 = std::fs::File::create(p0.join("00.log")).unwrap();
            f0.write_all(&[0u8; 1234]).unwrap();
            let mut f1 = std::fs::File::create(p1.join("00.log")).unwrap();
            f1.write_all(&[0u8; 5678]).unwrap();
        }

        let metrics = BrokerMetrics::new();
        let scanner = DiskScanner {
            log_dirs: vec![tmp.path().to_path_buf()],
            interval: minutes(1),
            metrics: metrics.clone(),
            shutdown: CancellationToken::new(),
        };
        scanner.tick_once();

        let g0 = metrics
            .partition_disk_bytes
            .get_or_create(&PartitionLabel {
                topic: "t".into(),
                partition: 0,
            })
            .get();
        let g1 = metrics
            .partition_disk_bytes
            .get_or_create(&PartitionLabel {
                topic: "t".into(),
                partition: 1,
            })
            .get();
        assert!(g0 == 1234);
        assert!(g1 == 5678);
    }

    #[tokio::test]
    async fn run_ticks_until_shutdown() {
        let tmp = tempfile::tempdir().unwrap();
        let p0 = tmp.path().join("run-0");
        std::fs::create_dir_all(&p0).unwrap();
        {
            let mut f0 = std::fs::File::create(p0.join("00.log")).unwrap();
            f0.write_all(&[0u8; 321]).unwrap();
        }

        let metrics = BrokerMetrics::new();
        let shutdown = CancellationToken::new();
        let scanner = DiskScanner {
            log_dirs: vec![tmp.path().to_path_buf()],
            interval: millis(10),
            metrics: metrics.clone(),
            shutdown: shutdown.clone(),
        };
        let handle = tokio::spawn(scanner.run());
        let label = PartitionLabel {
            topic: "run".into(),
            partition: 0,
        };

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if metrics.partition_disk_bytes.get_or_create(&label).get() == 321 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn run_waits_for_shutdown_between_ticks() {
        let tmp = tempfile::tempdir().unwrap();
        let metrics = BrokerMetrics::new();
        let shutdown = CancellationToken::new();
        let scanner = DiskScanner {
            log_dirs: vec![tmp.path().to_path_buf()],
            interval: hours(1),
            metrics,
            shutdown: shutdown.clone(),
        };
        let mut handle = tokio::spawn(scanner.run());

        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut handle)
                .await
                .is_err(),
            "disk scanner run loop exited before shutdown"
        );

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("disk scanner should observe shutdown")
            .expect("disk scanner should not panic");
    }
}
