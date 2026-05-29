//! `DescribeLogDirs` (`api_key=35`, KIP-113). Reports, per configured log
//! directory, the partitions it physically holds and their on-disk sizes.
//! Backs the `kafka-log-dirs --describe` admin tool.
//!
//! Surfaces both current logs and in-progress future logs (KIP-113
//! intra-broker moves): a future-log entry is reported under the
//! destination dir with `is_future_key = true` and an `offset_lag`
//! equal to `current_log.LEO − future_log.LEO`.

use std::collections::BTreeMap;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::describe_log_dirs_request::DescribeLogDirsRequest;
use crabka_protocol::owned::describe_log_dirs_response::{
    DescribeLogDirsPartition, DescribeLogDirsResponse, DescribeLogDirsResult, DescribeLogDirsTopic,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::disk_scanner::scan::sum_partition_dir;
use crate::error::BrokerError;
use crate::log_dir;

/// Filter derived from the request `topics` field:
/// - `None`  → report every partition (admin-client default).
/// - `Some`  → report only listed topics; an empty partition list for a
///   topic means "all partitions of that topic".
enum Filter {
    All,
    Topics(BTreeMap<String, Vec<i32>>),
}

impl Filter {
    fn allows(&self, topic: &str, partition: i32) -> bool {
        match self {
            Filter::All => true,
            Filter::Topics(map) => match map.get(topic) {
                None => false,
                Some(parts) => parts.is_empty() || parts.contains(&partition),
            },
        }
    }
}

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let log_dirs = broker.config.all_log_dirs();
    let partitions = broker.partitions.clone();
    let future_logs = broker.future_logs.clone();
    let log_dir_status = broker.log_dir_status.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = DescribeLogDirsRequest::decode(&mut cur, version)?;

        let filter = match req.topics {
            None => Filter::All,
            Some(topics) => Filter::Topics(
                topics
                    .into_iter()
                    .map(|t| (t.topic, t.partitions))
                    .collect(),
            ),
        };

        let mut results = Vec::with_capacity(log_dirs.len());
        for dir in &log_dirs {
            // KIP-113 offline-dir handling: a dir the startup probe
            // flagged unwritable is reported with
            // `error_code = KAFKA_STORAGE_ERROR`, no partition scan,
            // and `-1` for capacity. The JVM `kafka-log-dirs` tool
            // expects this shape — it prints the dir as
            // "OFFLINE: …" rather than a row of zeros.
            if log_dir_status.is_offline(dir) {
                results.push(DescribeLogDirsResult {
                    error_code: codes::KAFKA_STORAGE_ERROR,
                    log_dir: absolute_path(dir),
                    topics: Vec::new(),
                    total_bytes: -1,
                    usable_bytes: -1,
                    ..Default::default()
                });
                continue;
            }
            // Group the partitions physically present in this dir by topic.
            let mut by_topic: BTreeMap<String, Vec<DescribeLogDirsPartition>> = BTreeMap::new();
            let discovered = log_dir::scan(dir).unwrap_or_default();
            for (topic, partition) in discovered {
                if !filter.allows(&topic, partition) {
                    continue;
                }
                let part_dir = log_dir::partition_dir(dir, &topic, partition);
                let size = sum_partition_dir(&part_dir).unwrap_or(0);
                let offset_lag = offset_lag_for(&partitions, &topic, partition).await;
                by_topic
                    .entry(topic)
                    .or_default()
                    .push(DescribeLogDirsPartition {
                        partition_index: partition,
                        partition_size: i64::try_from(size).unwrap_or(i64::MAX),
                        offset_lag,
                        is_future_key: false,
                        ..Default::default()
                    });
            }

            // KIP-113: surface in-progress future logs (one per
            // `<topic>-<partition>-future` subdir) with
            // `is_future_key = true`. `offset_lag` is the gap between
            // the future log and the source log; while the move is
            // running this shrinks toward zero, then the directory
            // rename turns the entry into a regular current log.
            let future_discovered = log_dir::scan_future(dir).unwrap_or_default();
            for (topic, partition) in future_discovered {
                if !filter.allows(&topic, partition) {
                    continue;
                }
                let future_path = log_dir::future_partition_dir(dir, &topic, partition);
                let size = sum_partition_dir(&future_path).unwrap_or(0);
                let offset_lag = future_offset_lag(&partitions, &future_logs, &topic, partition);
                by_topic
                    .entry(topic)
                    .or_default()
                    .push(DescribeLogDirsPartition {
                        partition_index: partition,
                        partition_size: i64::try_from(size).unwrap_or(i64::MAX),
                        offset_lag,
                        is_future_key: true,
                        ..Default::default()
                    });
            }

            let topics = by_topic
                .into_iter()
                .map(|(name, partitions)| DescribeLogDirsTopic {
                    name,
                    partitions,
                    ..Default::default()
                })
                .collect();

            let (total_bytes, usable_bytes) = log_dir_capacity(dir);

            results.push(DescribeLogDirsResult {
                error_code: codes::NONE,
                log_dir: absolute_path(dir),
                topics,
                // KIP-827 (Kafka 3.3+): v4 surfaces per-dir filesystem
                // capacity. We query the underlying filesystem via
                // `statvfs` on unix and report `-1` (Kafka's "unknown"
                // sentinel) on non-unix; the JVM admin tools tolerate
                // `-1` and skip the column.
                total_bytes,
                usable_bytes,
                ..Default::default()
            });
        }

        let resp = DescribeLogDirsResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            results,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

/// `LEO − HW`, clamped to ≥ 0, for a loaded current log; 0 when the
/// partition isn't materialized on this broker.
async fn offset_lag_for(
    partitions: &crate::partition_registry::PartitionRegistry,
    topic: &str,
    partition: i32,
) -> i64 {
    let Some(part) = partitions.get(topic, partition) else {
        return 0;
    };
    let leo = part.log_end_offset();
    let hw = part.high_watermark().await;
    (leo - hw).max(0)
}

/// `current_log.LEO − future_log.LEO`, clamped to ≥ 0, for an
/// in-progress KIP-113 move. Returns 0 if the partition isn't
/// materialized locally; falls back to 0 if the future-log registry
/// has no entry (broker just started and the resume task hasn't yet
/// opened the future log).
fn future_offset_lag(
    partitions: &crate::partition_registry::PartitionRegistry,
    future_logs: &dashmap::DashMap<
        (String, i32),
        std::sync::Arc<crate::future_log::FutureLogState>,
    >,
    topic: &str,
    partition: i32,
) -> i64 {
    let Some(part) = partitions.get(topic, partition) else {
        return 0;
    };
    let current_leo = part.log_end_offset();
    let future_leo = future_logs
        .get(&(topic.to_string(), partition))
        .map_or(0, |e| {
            e.value()
                .future_log
                .lock()
                .expect("future log mutex poisoned")
                .log_end_offset()
        });
    (current_leo - future_leo).max(0)
}

/// Best-effort absolute path string for a log dir, matching Kafka's
/// "absolute log directory path" contract. Falls back to the lexical path
/// when canonicalization fails (e.g. the dir was removed out from under us).
fn absolute_path(dir: &std::path::Path) -> String {
    std::fs::canonicalize(dir)
        .unwrap_or_else(|_| dir.to_path_buf())
        .display()
        .to_string()
}

/// `(total_bytes, usable_bytes)` for the filesystem hosting `dir`,
/// matching the KIP-827 `DescribeLogDirsResult` v4 fields. `total_bytes`
/// is the filesystem capacity; `usable_bytes` is what's available to a
/// non-root caller (i.e. respects the typical 5 % root reserve).
///
/// Returns `(-1, -1)` — the Kafka "unknown" sentinel — when the platform
/// has no `statvfs` (Windows) or the syscall fails (path vanished mid-
/// reconfigure, permissions). The JVM admin tools tolerate `-1` and
/// just skip the column.
fn log_dir_capacity(dir: &std::path::Path) -> (i64, i64) {
    disk_stats(dir).unwrap_or((-1, -1))
}

#[cfg(unix)]
fn disk_stats(dir: &std::path::Path) -> Option<(i64, i64)> {
    let stat = rustix::fs::statvfs(dir).ok()?;
    // `f_frsize` is the fragment size in bytes; multiplying by the
    // block counts yields capacity in bytes. Both fields come back as
    // `u64`; clamp to `i64::MAX` rather than overflow on a hypothetical
    // exabyte-scale volume.
    let frsize = i64::try_from(stat.f_frsize).unwrap_or(i64::MAX);
    let total = i64::try_from(stat.f_blocks)
        .unwrap_or(i64::MAX)
        .saturating_mul(frsize);
    let usable = i64::try_from(stat.f_bavail)
        .unwrap_or(i64::MAX)
        .saturating_mul(frsize);
    Some((total, usable))
}

#[cfg(not(unix))]
fn disk_stats(_dir: &std::path::Path) -> Option<(i64, i64)> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_all_allows_everything() {
        let f = Filter::All;
        assert!(f.allows("any", 0));
        assert!(f.allows("other", 99));
    }

    #[test]
    fn filter_topics_respects_partition_list() {
        let mut m = BTreeMap::new();
        m.insert("t".to_string(), vec![0, 2]);
        let f = Filter::Topics(m);
        assert!(f.allows("t", 0));
        assert!(!f.allows("t", 1));
        assert!(f.allows("t", 2));
        assert!(!f.allows("other", 0));
    }

    #[test]
    fn filter_topics_empty_partition_list_means_all() {
        let mut m = BTreeMap::new();
        m.insert("t".to_string(), vec![]);
        let f = Filter::Topics(m);
        assert!(f.allows("t", 0));
        assert!(f.allows("t", 7));
        assert!(!f.allows("u", 0));
    }

    /// On unix, `statvfs` against any tempdir must return positive,
    /// sensible numbers — `total_bytes >= usable_bytes > 0`. Catches
    /// fragment-size vs block-count multiplication regressions, which
    /// would otherwise silently report zeros (Kafka tools then
    /// display "0 B free" and operators chase a ghost).
    #[cfg(unix)]
    #[test]
    fn log_dir_capacity_returns_sensible_unix_numbers() {
        let tmp = tempfile::tempdir().unwrap();
        let (total, usable) = log_dir_capacity(tmp.path());
        assert!(
            total > 0,
            "total_bytes must be positive on unix tempdir, got {total}"
        );
        assert!(
            usable > 0,
            "usable_bytes must be positive on unix tempdir, got {usable}"
        );
        assert!(
            total >= usable,
            "total_bytes ({total}) must be ≥ usable_bytes ({usable})",
        );
    }

    /// Vanished path yields the Kafka "unknown" sentinel rather than
    /// propagating the syscall error. Operators see `-1` and the JVM
    /// tool skips the column; the alternative — a 500-like
    /// `KafkaStorageException` — would block the whole describe.
    #[cfg(unix)]
    #[test]
    fn log_dir_capacity_returns_minus_one_for_missing_path() {
        let phantom = std::path::Path::new("/nonexistent/crabka/test/dir/should/not/exist");
        assert_eq!(log_dir_capacity(phantom), (-1, -1));
    }
}
