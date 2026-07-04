//! Shared, lock-free counters the writer updates and the broker scrapes into
//! Prometheus.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::ids::{RecordCount, SpoolBytes};

/// Cumulative + current spool statistics.
#[derive(Debug, Default)]
pub struct AuditStats {
    spooled: AtomicU64,
    replayed: AtomicU64,
    dropped: AtomicU64,
    depth: AtomicU64,
    spool_bytes: AtomicU64,
}

impl AuditStats {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn inc_spooled(&self) {
        self.spooled.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn inc_replayed_by(&self, n: u64) {
        self.replayed.fetch_add(n, Ordering::Relaxed);
    }
    pub(crate) fn inc_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn set_depth(&self, count: RecordCount, bytes: SpoolBytes) {
        self.depth.store(count.0, Ordering::Relaxed);
        self.spool_bytes.store(bytes.0, Ordering::Relaxed);
    }

    #[must_use]
    pub fn spooled(&self) -> u64 {
        self.spooled.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn replayed(&self) -> u64 {
        self.replayed.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn depth(&self) -> u64 {
        self.depth.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn spool_bytes(&self) -> u64 {
        self.spool_bytes.load(Ordering::Relaxed)
    }
}
