//! Tunables for `Log`. Defaults match Apache Kafka 4.2.

use std::time::Duration;

/// Per-topic policy for what to do with old log segments.
///
/// `Delete` (default): age- or size-based segment deletion via
/// [`crate::retention`]. `Compact`: newest-wins dedup-by-key,
/// implemented in [`crate::compact`] and invoked through
/// [`crate::Log::compact`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CleanupPolicy {
    #[default]
    Delete,
    Compact,
}

/// Tunables for [`Log`](crate::Log) behavior.
///
/// Defaults match Apache Kafka 4.2 (`segment.bytes`, `segment.ms`,
/// `retention.ms`, `index.interval.bytes`, etc.). The
/// [`Default`](Self::default) impl is the recommended starting point;
/// most production deployments will only override `retention_ms` and
/// `retention_bytes`.
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Roll the active segment when it exceeds this many bytes. Kafka default: 1 GiB.
    pub segment_bytes: u64,

    /// Roll the active segment when its first record is older than this. Kafka default: 7 days.
    pub segment_ms: Duration,

    /// Delete sealed segments older than this. `None` = unlimited. Kafka default: 7 days.
    pub retention_ms: Option<Duration>,

    /// Delete oldest sealed segments until the total `.log` size fits. `None` = unlimited.
    pub retention_bytes: Option<u64>,

    /// Write one `.index`/`.timeindex` entry per N bytes of `.log`. Kafka default: 4 KiB.
    pub index_interval_bytes: u32,

    /// fsync after every `append`. Default off; broker manages fsync separately.
    pub flush_on_append: bool,

    /// On open, CRC every batch in the active segment from the last index entry to EOF.
    pub validate_on_open: bool,

    /// Cleanup policy. Defaults to `Delete`. See [`CleanupPolicy`].
    pub cleanup_policy: CleanupPolicy,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 1024 * 1024 * 1024,
            segment_ms: Duration::from_hours(7 * 24),
            retention_ms: Some(Duration::from_hours(7 * 24)),
            retention_bytes: None,
            index_interval_bytes: 4096,
            flush_on_append: false,
            validate_on_open: true,
            cleanup_policy: CleanupPolicy::Delete,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_kafka_4x() {
        let c = LogConfig::default();
        assert_eq!(c.segment_bytes, 1 << 30);
        assert_eq!(c.index_interval_bytes, 4096);
        assert!(!c.flush_on_append);
        assert!(c.validate_on_open);
    }

    #[test]
    fn default_cleanup_policy_is_delete() {
        let c = LogConfig::default();
        assert_eq!(c.cleanup_policy, CleanupPolicy::Delete);
    }
}
