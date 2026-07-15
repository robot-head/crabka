//! Poll-style counters for timestamp-oracle load observation.
//!
//! Both stat structs follow the substrate's hand-rolled counter pattern
//! (shared through an [`Arc`], recorded at the seam, snapshotted by a
//! poller), but use relaxed atomics instead of a mutex so recording never
//! reintroduces a lock on the oracle's lock-free grant fast path. A snapshot
//! reads each counter independently and may therefore be momentarily torn
//! across fields under concurrent grants; rates computed from successive
//! snapshots are unaffected.

use std::sync::atomic::{AtomicU64, Ordering};

/// Grant-serving counters recorded by the range-0 timestamp oracle.
#[derive(Debug, Default)]
pub struct TsoOracleStats {
    grants_served: AtomicU64,
    timestamps_granted: AtomicU64,
    horizon_persists: AtomicU64,
    heartbeats: AtomicU64,
}

/// Point-in-time copy of [`TsoOracleStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TsoOracleStatsSnapshot {
    /// Successful grants served.
    pub grants_served: u64,
    /// Timestamps handed out across all served grants.
    pub timestamps_granted: u64,
    /// Durable horizon advances committed through range 0.
    pub horizon_persists: u64,
    /// Epoch-liveness heartbeats issued.
    pub heartbeats: u64,
}

impl TsoOracleStats {
    /// Record one served grant of `count` timestamps.
    pub fn record_grant(&self, count: u64) {
        self.grants_served.fetch_add(1, Ordering::Relaxed);
        self.timestamps_granted.fetch_add(count, Ordering::Relaxed);
    }

    /// Record one durable horizon advance.
    pub fn record_horizon_persist(&self) {
        self.horizon_persists.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one epoch-liveness heartbeat.
    pub fn record_heartbeat(&self) {
        self.heartbeats.fetch_add(1, Ordering::Relaxed);
    }

    /// Return the current counters.
    #[must_use]
    pub fn snapshot(&self) -> TsoOracleStatsSnapshot {
        TsoOracleStatsSnapshot {
            grants_served: self.grants_served.load(Ordering::Relaxed),
            timestamps_granted: self.timestamps_granted.load(Ordering::Relaxed),
            horizon_persists: self.horizon_persists.load(Ordering::Relaxed),
            heartbeats: self.heartbeats.load(Ordering::Relaxed),
        }
    }
}

/// Batch-fill counters recorded by the conveyor timestamp client.
#[derive(Debug, Default)]
pub struct TsoClientStats {
    rpcs_issued: AtomicU64,
    requests_coalesced: AtomicU64,
}

/// Point-in-time copy of [`TsoClientStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TsoClientStatsSnapshot {
    /// Upstream grant RPCs actually issued.
    pub rpcs_issued: u64,
    /// Caller requests coalesced into those RPCs; average batch fill is
    /// `requests_coalesced / rpcs_issued`.
    pub requests_coalesced: u64,
}

impl TsoClientStats {
    /// Record one issued upstream RPC carrying `requests` coalesced callers.
    pub fn record_flush(&self, requests: u64) {
        self.rpcs_issued.fetch_add(1, Ordering::Relaxed);
        self.requests_coalesced
            .fetch_add(requests, Ordering::Relaxed);
    }

    /// Return the current counters.
    #[must_use]
    pub fn snapshot(&self) -> TsoClientStatsSnapshot {
        TsoClientStatsSnapshot {
            rpcs_issued: self.rpcs_issued.load(Ordering::Relaxed),
            requests_coalesced: self.requests_coalesced.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn oracle_stats_accumulate_and_snapshot() {
        let stats = TsoOracleStats::default();

        stats.record_grant(3);
        stats.record_grant(2);
        stats.record_horizon_persist();
        stats.record_heartbeat();
        stats.record_heartbeat();

        assert!(
            stats.snapshot()
                == TsoOracleStatsSnapshot {
                    grants_served: 2,
                    timestamps_granted: 5,
                    horizon_persists: 1,
                    heartbeats: 2,
                }
        );
    }

    #[test]
    fn client_stats_accumulate_and_snapshot() {
        let stats = TsoClientStats::default();

        stats.record_flush(1);
        stats.record_flush(5);

        assert!(
            stats.snapshot()
                == TsoClientStatsSnapshot {
                    rpcs_issued: 2,
                    requests_coalesced: 6,
                }
        );
    }
}
