//! Periodic per-partition disk-usage scanner. Spawned by
//! `Broker::start` when `--partition-disk-scan-interval-secs > 0`.
//! Each tick walks the log directory for every known
//! (topic, partition), sums regular file sizes, and updates the
//! `partition_disk_bytes` gauge.

pub mod scan;

use std::path::PathBuf;
use std::time::Duration;

use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::log_dir;
use crate::metrics::{BrokerMetrics, PartitionLabel};

pub struct DiskScanner {
    pub log_dir: PathBuf,
    pub interval: Duration,
    pub metrics: BrokerMetrics,
    pub shutdown: CancellationToken,
}

impl DiskScanner {
    pub async fn run(self) {
        info!(
            interval_secs = self.interval.as_secs(),
            "disk scanner started"
        );
        let mut ticker = interval(self.interval);
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
        let partitions = match log_dir::scan(&self.log_dir) {
            Ok(p) => p,
            Err(e) => {
                warn!(error = %e, "disk scanner: log_dir::scan failed; skipping tick");
                return;
            }
        };
        for (topic, partition) in partitions {
            let path = log_dir::partition_dir(&self.log_dir, &topic, partition);
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
    use super::*;
    use std::io::Write;

    #[test]
    fn tick_once_sets_gauge_for_each_partition() {
        let tmp = tempfile::tempdir().unwrap();
        // Materialize two partition dirs the way the broker would.
        let p0 = tmp.path().join("t-0");
        let p1 = tmp.path().join("t-1");
        std::fs::create_dir_all(&p0).unwrap();
        std::fs::create_dir_all(&p1).unwrap();
        let mut f0 = std::fs::File::create(p0.join("00.log")).unwrap();
        f0.write_all(&[0u8; 1234]).unwrap();
        let mut f1 = std::fs::File::create(p1.join("00.log")).unwrap();
        f1.write_all(&[0u8; 5678]).unwrap();

        let metrics = BrokerMetrics::new();
        let scanner = DiskScanner {
            log_dir: tmp.path().to_path_buf(),
            interval: Duration::from_mins(1),
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
        assert_eq!(g0, 1234);
        assert_eq!(g1, 5678);
    }
}
