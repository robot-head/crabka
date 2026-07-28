//! Tunables for `Log`. Defaults match Apache Kafka 4.2.

use crabka_compression::CompressionType;
use crabka_units::prelude::{ByteSize, Time, days, gibibytes, hours, kibibytes};

/// Kafka's `segment.bytes` default: roll the active segment at 1 GiB.
const DEFAULT_SEGMENT_SIZE: ByteSize = gibibytes(1);

/// Kafka's `segment.ms` default: roll the active segment once its first
/// record is a week old.
const DEFAULT_SEGMENT_ROLL_INTERVAL: Time = days(7);

/// Kafka's `retention.ms` default: delete sealed segments a week after their
/// newest record.
const DEFAULT_RETENTION: Time = days(7);

/// Kafka's `index.interval.bytes` default: one sparse `.index`/`.timeindex`
/// entry per 4 KiB of `.log`.
const DEFAULT_INDEX_INTERVAL: ByteSize = kibibytes(4);

/// Kafka's `delete.retention.ms` default: a tombstone or transaction marker
/// stays readable for a day after it first becomes compaction-eligible.
const DEFAULT_DELETE_RETENTION: Time = hours(24);

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
/// most production deployments will only override [`Self::retention`] and
/// [`Self::retention_size`].
#[derive(Debug, Clone, PartialEq)]
pub struct LogConfig {
    /// Roll the active segment once it grows past this. Kafka's
    /// `segment.bytes`; default 1 GiB.
    pub segment_size: ByteSize,

    /// Roll the active segment when its first record is older than this.
    /// Kafka's `segment.ms`; default 7 days.
    pub segment_roll_interval: Time,

    /// Delete sealed segments older than this. `None` = unlimited. Kafka's
    /// `retention.ms`; default 7 days.
    pub retention: Option<Time>,

    /// Delete oldest sealed segments until the total `.log` size fits.
    /// `None` = unlimited. Kafka's `retention.bytes`.
    pub retention_size: Option<ByteSize>,

    /// Write one `.index`/`.timeindex` entry per this much `.log`. Kafka's
    /// `index.interval.bytes`; default 4 KiB.
    pub index_interval: ByteSize,

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
    /// partitions (KIP-405). `None` inherits [`Self::retention`]. Default `None`.
    pub local_retention: Option<Time>,

    /// Local-disk size budget for tiered partitions (KIP-405).
    /// `None` inherits [`Self::retention_size`]. Default `None`.
    pub local_retention_size: Option<ByteSize>,

    /// KIP-534. After a tombstone or transaction marker first becomes
    /// compaction-eligible, retain it for at least this long before deletion
    /// (the delete-horizon grace window). Default 24h.
    pub delete_retention: Time,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            segment_size: DEFAULT_SEGMENT_SIZE,
            segment_roll_interval: DEFAULT_SEGMENT_ROLL_INTERVAL,
            retention: Some(DEFAULT_RETENTION),
            retention_size: None,
            index_interval: DEFAULT_INDEX_INTERVAL,
            flush_on_append: false,
            validate_on_open: true,
            cleanup_policy: CleanupPolicy::Delete,
            // Pass-through: producers' compression choice wins. Kafka's
            // default. Operators flip this to a specific codec on
            // topics where they want broker-side enforcement.
            compression_type: None,
            // Tiered storage is opt-in per topic (Kafka default false).
            remote_storage_enable: false,
            local_retention: None,
            local_retention_size: None,
            delete_retention: DEFAULT_DELETE_RETENTION,
        }
    }
}

#[cfg(test)]
mod tests {

    use crabka_units::prelude::{ByteSizeExt as _, TimeExt, bytes, secs};

    use super::*;

    #[test]
    fn defaults_match_kafka_4x() {
        assert2::assert!(
            LogConfig::default()
                == LogConfig {
                    segment_size: bytes(1 << 30),
                    segment_roll_interval: days(7),
                    retention: Some(days(7)),
                    retention_size: None,
                    index_interval: bytes(4096),
                    flush_on_append: false,
                    validate_on_open: true,
                    cleanup_policy: CleanupPolicy::Delete,
                    compression_type: None,
                    remote_storage_enable: false,
                    local_retention: None,
                    local_retention_size: None,
                    delete_retention: secs(24 * 60 * 60),
                }
        );
    }

    #[test]
    fn defaults_cross_the_raw_seams_as_kafkas_documented_numbers() {
        // The quantities exist to be handed to `.index` sizing, retention
        // arithmetic, and Kafka config reporting as plain integers; a
        // scale slip in a constructor would show up here.
        let c = LogConfig::default();
        assert2::check!(c.segment_size.bytes_u64() == 1_073_741_824);
        assert2::check!(c.index_interval.bytes_u64() == 4_096);
        assert2::check!(c.segment_roll_interval.millis_i64() == 604_800_000);
        assert2::check!(c.retention.map(TimeExt::millis_i64) == Some(604_800_000));
        assert2::check!(c.delete_retention.millis_i64() == 86_400_000);
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
        assert2::assert!(c.local_retention == None);
        assert2::assert!(c.local_retention_size == None);
    }
}
