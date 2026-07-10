//! Tunables for `Log`. Defaults match Apache Kafka 4.2.

use std::time::Duration;

use crabka_compression::CompressionType;

/// Per-topic policy for what to do with old log segments.
///
/// `Delete` (default): age- or size-based segment deletion via
/// `crate::retention`. `Compact`: newest-wins dedup-by-key,
/// implemented in `crate::compact` and invoked through
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
#[derive(Debug, Clone, PartialEq)]
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

    /// Broker-side recompression target. `None` is Kafka's
    /// `compression.type=producer` (pass-through — store the batch
    /// exactly as the producer sent it). `Some(c)` forces every batch
    /// the broker accepts on this partition to be re-encoded to `c`
    /// before write. Matches Kafka's per-topic `compression.type`
    /// config: `gzip` / `snappy` / `lz4` / `zstd` / `uncompressed` map
    /// to `Some(_)`; `producer` (the default) maps to `None`.
    pub compression_type: Option<CompressionType>,

    /// When `true`, this partition's sealed segments (KIP-405)
    /// are eligible to be copied to the remote tier by the broker's
    /// `RemoteLogManager`. Maps to Kafka's per-topic `remote.storage.enable`.
    /// Default `false` (Kafka's default — tiered storage is opt-in per topic).
    pub remote_storage_enable: bool,

    /// Local-disk time-retention window for tiered
    /// partitions (KIP-405). `None` inherits `retention_ms`. Default `None`.
    pub local_retention_ms: Option<Duration>,

    /// Local-disk size budget for tiered partitions (KIP-405).
    /// `None` inherits `retention_bytes`. Default `None`.
    pub local_retention_bytes: Option<u64>,

    /// KIP-534. After a tombstone or transaction marker first becomes
    /// compaction-eligible, retain it for at least this long before deletion
    /// (the delete-horizon grace window). Default 24h.
    pub delete_retention_ms: Duration,
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
            // Pass-through: producers' compression choice wins. Kafka's
            // default. Operators flip this to a specific codec on
            // topics where they want broker-side enforcement.
            compression_type: None,
            // Tiered storage is opt-in per topic (Kafka default false).
            remote_storage_enable: false,
            local_retention_ms: None,
            local_retention_bytes: None,
            delete_retention_ms: Duration::from_hours(24),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn defaults_match_kafka_4x() {
        assert2::assert!(
            LogConfig::default()
                == LogConfig {
                    segment_bytes: 1 << 30,
                    segment_ms: Duration::from_hours(7 * 24),
                    retention_ms: Some(Duration::from_hours(7 * 24)),
                    retention_bytes: None,
                    index_interval_bytes: 4096,
                    flush_on_append: false,
                    validate_on_open: true,
                    cleanup_policy: CleanupPolicy::Delete,
                    compression_type: None,
                    remote_storage_enable: false,
                    local_retention_ms: None,
                    local_retention_bytes: None,
                    delete_retention_ms: Duration::from_hours(24),
                }
        );
    }

    #[test]
    fn default_cleanup_policy_is_delete() {
        let c = LogConfig::default();
        assert2::assert!(c.cleanup_policy == CleanupPolicy::Delete);
    }

    #[test]
    fn default_compression_is_producer_passthrough() {
        let c = LogConfig::default();
        assert2::assert!(c.compression_type == None);
    }

    #[test]
    fn default_local_retention_is_none() {
        let c = LogConfig::default();
        assert2::assert!(c.local_retention_ms == None);
        assert2::assert!(c.local_retention_bytes == None);
    }

    #[test]
    fn default_delete_retention_is_24h() {
        assert2::assert!(
            LogConfig::default().delete_retention_ms == std::time::Duration::from_hours(24)
        );
    }
}
