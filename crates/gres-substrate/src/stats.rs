//! Authoritative range-statistics snapshots for planning consumers.
//!
//! This module deliberately has no integration with checkpoint trigger counters.
//! Those counters describe work since the last successful checkpoint and reset on
//! success. They are not live range metrics.

use std::{
    sync::{Arc, RwLock},
    time::SystemTime,
};

/// Statistics for a single range at one sampling instant.
///
/// `row_count`, `store_bytes`, and `replication_lag_bytes` are gauges. `write_rate`
/// and `read_rate` are rates calculated over a provider-defined non-reset interval.
/// A missing value means that the source is not authoritative for that metric.
/// A caller must not interpret it as zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeStats {
    /// Tenant owning the range.
    pub tenant_name: String,
    /// Stable range identifier within the tenant.
    pub range_id: u32,
    /// Current authoritative row count, when available.
    pub row_count: Option<u64>,
    /// A rowid the owner observed with about half of the range's rows below it,
    /// when it measured one.
    ///
    /// Splitting a range needs an *observed* key. Its rowid interval says
    /// nothing about where the rows sit inside it: `CLUSTER` rewrites every live
    /// row at a fresh rowid and permanently vacates the block it came from,
    /// deletes leave arbitrary holes, and a timestamp-sharded table's rowids are
    /// packed clock readings spread across most of the domain. Only the owner
    /// can see that distribution, so only the owner can name a boundary that
    /// actually divides it.
    pub median_rowid: Option<u64>,
    /// Current authoritative stored bytes, when available.
    pub store_bytes: Option<u64>,
    /// Authoritative write rate for the sample interval, when available.
    pub write_rate: Option<u64>,
    /// Authoritative read rate for the sample interval, when available.
    pub read_rate: Option<u64>,
    /// Current authoritative replication lag in bytes, when available.
    pub replication_lag_bytes: Option<u64>,
}

/// An atomically published range-statistics sample.
///
/// `version` is monotonically increasing for a provider. `sampled_at` identifies
/// when gauges and interval rates were observed. This snapshot contains neither
/// reset-on-checkpoint counters nor inferred zero values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeStatsSnapshot {
    /// Monotonically increasing provider version.
    pub version: u64,
    /// Time at which the values were sampled.
    pub sampled_at: SystemTime,
    /// The ranges sampled atomically at `sampled_at`.
    pub ranges: Vec<RangeStats>,
}

impl RangeStatsSnapshot {
    /// Return whether this snapshot is newer than a version already consumed.
    #[must_use]
    pub const fn is_newer_than(&self, version: u64) -> bool {
        self.version > version
    }
}

/// Narrow source seam for range-statistics consumers.
///
/// Implementations only publish metrics they measure authoritatively. Callers must
/// treat every `None` metric as unknown and must not take metric-dependent action.
pub trait RangeStatsProvider: Send + Sync {
    /// Return the provider's latest atomically published sample.
    fn snapshot(&self) -> RangeStatsSnapshot;
}

/// In-memory snapshot provider for tests and explicit in-process publishers.
///
/// It is intentionally a publisher seam, not a live metric collector.
#[derive(Debug, Clone)]
pub struct InMemoryRangeStatsProvider {
    snapshot: Arc<RwLock<RangeStatsSnapshot>>,
}

impl InMemoryRangeStatsProvider {
    /// Build a provider with an initial snapshot.
    #[must_use]
    pub fn new(snapshot: RangeStatsSnapshot) -> Self {
        Self {
            snapshot: Arc::new(RwLock::new(snapshot)),
        }
    }

    /// Publish a strictly newer complete snapshot.
    ///
    /// Returns the rejected snapshot when its version is not newer than the
    /// currently published version or its sampling time regresses.
    /// # Panics
    ///
    /// Panics if an internal invariant is violated.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn publish(&self, snapshot: RangeStatsSnapshot) -> Result<(), RangeStatsSnapshot> {
        let mut current = self.snapshot.write().expect("range stats lock poisoned");
        if !snapshot.is_newer_than(current.version) || snapshot.sampled_at < current.sampled_at {
            return Err(snapshot);
        }
        *current = snapshot;
        Ok(())
    }
}

impl RangeStatsProvider for InMemoryRangeStatsProvider {
    fn snapshot(&self) -> RangeStatsSnapshot {
        self.snapshot
            .read()
            .expect("range stats lock poisoned")
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use assert2::assert;

    use super::*;

    fn snapshot(version: u64, store_bytes: Option<u64>) -> RangeStatsSnapshot {
        RangeStatsSnapshot {
            version,
            sampled_at: SystemTime::UNIX_EPOCH + Duration::from_secs(version),
            ranges: vec![RangeStats {
                tenant_name: "blue".to_string(),
                range_id: 7,
                row_count: None,
                median_rowid: None,
                store_bytes,
                write_rate: None,
                read_rate: None,
                replication_lag_bytes: None,
            }],
        }
    }

    #[test]
    fn provider_accepts_only_newer_snapshots_and_preserves_unknown_metrics() {
        let provider = InMemoryRangeStatsProvider::new(snapshot(4, Some(99)));

        assert!(provider.publish(snapshot(4, Some(0))).is_err());
        provider.publish(snapshot(5, None)).expect("new snapshot");

        let published = provider.snapshot();
        assert!(published.is_newer_than(4));
        assert!(published.sampled_at == SystemTime::UNIX_EPOCH + Duration::from_secs(5));
        assert!(published.ranges[0].store_bytes.is_none());
    }

    #[test]
    fn provider_rejects_newer_version_with_regressed_sampling_time() {
        let provider = InMemoryRangeStatsProvider::new(snapshot(4, Some(99)));
        let regressed_sample = RangeStatsSnapshot {
            version: 5,
            sampled_at: SystemTime::UNIX_EPOCH + Duration::from_secs(3),
            ranges: Vec::new(),
        };

        assert!(provider.publish(regressed_sample).is_err());
        assert!(provider.snapshot() == snapshot(4, Some(99)));
    }
}
