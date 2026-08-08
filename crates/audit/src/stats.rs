//! Shared, lock-free counters for the audit spool.
//!
//! The writer updates these counters. The broker scrapes them into Prometheus.

use std::sync::atomic::{AtomicU64, Ordering};

use crabka_units::prelude::{ByteSize, ByteSizeExt as _};

use crate::ids::RecordCount;

/// Cumulative and current spool statistics.
///
/// The counters are `AtomicU64`, which cannot hold a quantity. The byte gauge
/// converts to and from [`ByteSize`] in its accessors.
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
    pub(crate) fn set_depth(&self, count: RecordCount, size: ByteSize) {
        self.depth.store(count.0, Ordering::Relaxed);
        self.spool_bytes.store(size.bytes_u64(), Ordering::Relaxed);
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
    /// Bytes currently held in the spool.
    ///
    /// The broker exports this value as the `audit_spool_bytes` gauge, and the
    /// method takes its name from that gauge.
    #[must_use]
    pub fn spool_bytes(&self) -> ByteSize {
        ByteSize::from_bytes(self.spool_bytes.load(Ordering::Relaxed))
    }
}
