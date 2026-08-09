//! `DescribeLogDirs` (`api_key=35`, KIP-113).
//!
//! The handler reports, for each configured log directory, the partitions that
//! the directory physically holds and their on-disk sizes. It backs the
//! `kafka-log-dirs --describe` admin tool.
//!
//! The handler reports both current logs and the future logs of in-progress
//! KIP-113 intra-broker moves. It reports a future-log entry under the
//! destination dir, with `is_future_key = true` and an `offset_lag` equal to
//! `current_log.LEO − future_log.LEO`.

use std::collections::BTreeMap;

use bytes::Bytes;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode,
    owned::{
        describe_log_dirs_request::DescribeLogDirsRequest,
        describe_log_dirs_response::{
            DescribeLogDirsPartition, DescribeLogDirsResponse, DescribeLogDirsResult,
            DescribeLogDirsTopic,
        },
    },
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    disk_scanner::scan::sum_partition_dir,
    error::BrokerError,
    log_dir,
};

/// Filter that the handler derives from the request `topics` field.
///
/// - `None`  → report every partition. This is the admin-client default.
/// - `Some`  → report only the listed topics. An empty partition list for a
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

#[tracing::instrument(
    name = "handle_describe_log_dirs",
    level = "info",
    skip_all,
    fields(api = "DescribeLogDirs", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let log_dirs = broker.config.all_log_dirs();
    let partitions = broker.partitions.clone();
    let future_logs = broker.future_logs.clone();
    let log_dir_status = broker.log_dir_status.clone();
    {
        let mut cur: &[u8] = req_bytes;
        let req = DescribeLogDirsRequest::decode(&mut cur, version)?;

        // ── ACL preamble ────────────────────────────────────────────
        // `Describe` on `Cluster("kafka-cluster")`. On Deny → whole-response
        // `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
        {
            let image = broker.controller.current_image();
            if cluster_describe_denied(
                broker.config.authorizer.as_ref(),
                &image,
                ctx.principal,
                ctx.peer,
            ) {
                return denied_response(version);
            }
        }

        let filter = request_filter(req);

        let mut results = Vec::with_capacity(log_dirs.len());
        for dir in &log_dirs {
            // KIP-113 offline-dir handling: a dir the startup probe
            // flagged unwritable is reported with
            // `error_code = KAFKA_STORAGE_ERROR`, no partition scan,
            // and `-1` for capacity. The JVM `kafka-log-dirs` tool
            // expects this shape — it prints the dir as
            // "OFFLINE: …" rather than a row of zeros.
            if log_dir_status.is_offline(dir) {
                results.push(offline_result(dir));
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
                let offset_lag = future_offset_lag(
                    &partitions,
                    &future_logs,
                    &topic,
                    crabka_ids::PartitionIndex(partition),
                );
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
        crate::handlers::encode_response(&resp, version)
    }
}

fn request_filter(req: DescribeLogDirsRequest) -> Filter {
    req.topics.map_or(Filter::All, |topics| {
        Filter::Topics(
            topics
                .into_iter()
                .map(|topic| (topic.topic, topic.partitions))
                .collect(),
        )
    })
}

fn offline_result(dir: &std::path::Path) -> DescribeLogDirsResult {
    DescribeLogDirsResult {
        error_code: codes::KAFKA_STORAGE_ERROR,
        log_dir: absolute_path(dir),
        topics: Vec::new(),
        total_bytes: -1,
        usable_bytes: -1,
        ..Default::default()
    }
}

/// Gate for `Describe` on `Cluster("kafka-cluster")`.
///
/// Returns `true` when the authorizer denies the operation.
fn cluster_describe_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &crabka_metadata::MetadataImage,
    principal: &crabka_security::Principal,
    host: &std::net::SocketAddr,
) -> bool {
    authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal,
            host,
            resource_type: ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: AclOperation::Describe,
        },
    ) == AuthorizationResult::Deny
}

/// Whole-response `CLUSTER_AUTHORIZATION_FAILED (31)` response for a Deny.
fn denied_response(version: i16) -> Result<Bytes, BrokerError> {
    let resp = DescribeLogDirsResponse {
        throttle_time_ms: 0,
        error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
        results: Vec::new(),
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

/// `LEO − HW` for a loaded current log, with a clamp at 0.
///
/// Returns 0 when the partition is not materialized on this broker.
async fn offset_lag_for(
    partitions: &crate::partition_registry::PartitionRegistry,
    topic: &str,
    partition: i32,
) -> i64 {
    let Some(part) = partitions.get(topic, crabka_ids::PartitionIndex(partition)) else {
        return 0;
    };
    let leo = part.log_end_offset();
    let hw = part.high_watermark().await;
    // Lag is a record-count delta between two offsets, not an offset.
    (leo.0 - hw.0).max(0)
}

/// `current_log.LEO − future_log.LEO` for an in-progress KIP-113 move, with a
/// clamp at 0.
///
/// Returns 0 if the partition is not materialized locally. Also returns 0 if
/// the future-log registry has no entry. The registry has no entry when the
/// broker has just started and the resume task has not opened the future log
/// yet.
fn future_offset_lag(
    partitions: &crate::partition_registry::PartitionRegistry,
    future_logs: &dashmap::DashMap<
        (String, crabka_ids::PartitionIndex),
        std::sync::Arc<crate::future_log::FutureLogState>,
    >,
    topic: &str,
    partition: crabka_ids::PartitionIndex,
) -> i64 {
    let Some(part) = partitions.get(topic, partition) else {
        return 0;
    };
    let current_leo = part.log_end_offset();
    let future_leo =
        future_logs
            .get(&(topic.to_string(), partition))
            .map_or(crabka_log::Offset(0), |e| {
                e.value()
                    .future_log
                    .lock()
                    .expect("future log mutex poisoned")
                    .log_end_offset()
            });
    // Lag is a record-count delta between two offsets, not an offset.
    (current_leo.0 - future_leo.0).max(0)
}

/// Best-effort absolute path string for a log dir.
///
/// The result matches the "absolute log directory path" contract of Kafka. The
/// function falls back to the lexical path when the canonicalization fails, for
/// example after another process removes the dir.
fn absolute_path(dir: &std::path::Path) -> String {
    std::fs::canonicalize(dir)
        .unwrap_or_else(|_| dir.to_path_buf())
        .display()
        .to_string()
}

/// `(total_bytes, usable_bytes)` for the filesystem that hosts `dir`.
///
/// The pair matches the KIP-827 `DescribeLogDirsResult` v4 fields.
/// `total_bytes` is the capacity of the filesystem. `usable_bytes` is the space
/// available to a non-root caller, so it respects the typical 5 % root reserve.
///
/// Returns `(-1, -1)`, the Kafka "unknown" sentinel, when the platform has no
/// `statvfs`, as on Windows, or when the syscall fails. The syscall fails when
/// the path vanishes during a reconfigure, or on a permission error. The JVM
/// admin tools tolerate `-1` and skip the column.
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
    use assert2::assert;

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
        for (topic, partition, want) in [
            ("t", 0, true),
            ("t", 1, false),
            ("t", 2, true),
            ("other", 0, false),
        ] {
            assert!(f.allows(topic, partition) == want, "{topic}-{partition}");
        }
    }

    #[test]
    fn filter_topics_empty_partition_list_means_all() {
        let mut m = BTreeMap::new();
        m.insert("t".to_string(), vec![]);
        let f = Filter::Topics(m);
        for (topic, partition, want) in [("t", 0, true), ("t", 7, true), ("u", 0, false)] {
            assert!(f.allows(topic, partition) == want, "{topic}-{partition}");
        }
    }

    /// On unix, `statvfs` against any tempdir must return sensible positive
    /// numbers.
    ///
    /// The rule is `total_bytes >= usable_bytes > 0`. This test catches a
    /// regression in the multiplication of the fragment size by the block
    /// count. Such a regression reports zeros silently. The Kafka tools then
    /// display "0 B free", and operators chase a problem that does not exist.
    #[cfg(unix)]
    #[test]
    fn log_dir_capacity_returns_sensible_unix_numbers() {
        use assert2::check;
        let tmp = tempfile::tempdir().unwrap();
        let (total, usable) = log_dir_capacity(tmp.path());
        check!(
            total > 0,
            "total_bytes must be positive on unix tempdir, got {total}"
        );
        check!(
            usable > 0,
            "usable_bytes must be positive on unix tempdir, got {usable}"
        );
        check!(
            total >= usable,
            "total_bytes ({total}) must be ≥ usable_bytes ({usable})",
        );
    }

    /// A vanished path gives the Kafka "unknown" sentinel and not the syscall
    /// error.
    ///
    /// Operators see `-1`, and the JVM tool skips the column. The alternative
    /// is a `KafkaStorageException`, much like a 500, which would block the
    /// whole describe.
    #[cfg(unix)]
    #[test]
    fn log_dir_capacity_returns_minus_one_for_missing_path() {
        let phantom = std::path::Path::new("/nonexistent/crabka/test/dir/should/not/exist");
        assert!(log_dir_capacity(phantom) == (-1, -1));
    }

    /// Builds a `Partition` rooted at `<log_dir>/<topic>-<partition>`.
    ///
    /// The function uses the real `spawn_partition` path and mirrors the
    /// `future_log` and registry test fixtures. It appends `count` records, so
    /// the LEO of the partition advances to `count`.
    fn partition_with_leo(
        log_dir: &std::path::Path,
        topic: &str,
        partition: crabka_ids::PartitionIndex,
        count: i32,
    ) -> std::sync::Arc<crate::partition::Partition> {
        let part_dir = log_dir::partition_dir(log_dir, topic, partition.get());
        std::fs::create_dir_all(&part_dir).unwrap();
        let log = crabka_log::Log::open(&part_dir, crabka_log::LogConfig::default()).unwrap();
        let part = crate::broker::spawn_partition(
            topic.to_string(),
            partition,
            log_dir.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            std::sync::Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );
        if count > 0 {
            append_n(&part.log, count);
        }
        part
    }

    /// Appends one batch of `count` records to a `Log` behind a mutex.
    ///
    /// The LEO of the log advances by `count`.
    fn append_n(log: &std::sync::Mutex<crabka_log::Log>, count: i32) {
        use bytes::Bytes;
        use crabka_protocol::records::{Attributes, Record, RecordBatch};
        let mut batch = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: -1,
            attributes: Attributes::default(),
            last_offset_delta: count - 1,
            base_timestamp: 1_700_000_000,
            max_timestamp: 1_700_000_000,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: (0..count)
                .map(|i| Record {
                    attributes: 0,
                    offset_delta: i,
                    timestamp_delta: 0,
                    key: None,
                    value: Some(Bytes::from_static(b"v")),
                    headers: vec![],
                })
                .collect(),
        };
        log.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .append(&mut batch)
            .expect("append records");
    }

    /// A partition that is not materialized locally reports lag `0`.
    ///
    /// It does not report the `-1` that a whole-function replacement mutant
    /// returns.
    #[tokio::test]
    async fn offset_lag_missing_partition_is_zero() {
        let reg = crate::partition_registry::PartitionRegistry::new();
        assert!(offset_lag_for(&reg, "ghost", 0).await == 0);
    }

    /// A materialized partition with LEO ahead of HW reports `LEO - HW`.
    ///
    /// A fresh HW is 0. This test pins the real subtraction against the
    /// whole-function `-> -1` replacement.
    #[tokio::test]
    async fn offset_lag_uses_leo_minus_hw() {
        let dir = tempfile::tempdir().unwrap();
        let reg = crate::partition_registry::PartitionRegistry::new();
        let part = partition_with_leo(dir.path(), "t", crabka_ids::PartitionIndex(0), 5);
        assert!(part.log_end_offset() == crabka_log::Offset(5));
        reg.insert("t".to_string(), crabka_ids::PartitionIndex(0), part);
        // Fresh partition HW is 0 → lag == LEO == 5 (not -1, not 0).
        assert!(offset_lag_for(&reg, "t", 0).await == 5);
    }

    /// Builds a `FutureLogState` whose future log has LEO `future_count`.
    fn future_state_with_leo(
        dir: &std::path::Path,
        future_count: i32,
    ) -> std::sync::Arc<crate::future_log::FutureLogState> {
        let future_path = dir.join("future");
        std::fs::create_dir_all(&future_path).unwrap();
        let flog = crabka_log::Log::open(&future_path, crabka_log::LogConfig::default()).unwrap();
        let future_log = std::sync::Arc::new(std::sync::Mutex::new(flog));
        if future_count > 0 {
            append_n(&future_log, future_count);
        }
        std::sync::Arc::new(crate::future_log::FutureLogState {
            target_log_dir: dir.to_path_buf(),
            future_path,
            future_log,
            cancel: tokio_util::sync::CancellationToken::new(),
            task: std::sync::Mutex::new(None::<tokio::task::JoinHandle<()>>),
        })
    }

    /// With no local partition, the future-log lag is `0`.
    ///
    /// It is not the `1` that a whole-function `-> 1` replacement mutant
    /// returns.
    #[tokio::test]
    async fn future_offset_lag_missing_partition_is_zero() {
        let reg = crate::partition_registry::PartitionRegistry::new();
        let future_logs = dashmap::DashMap::new();
        let lag = future_offset_lag(&reg, &future_logs, "ghost", crabka_ids::PartitionIndex(0));
        assert!(lag == 0);
    }

    /// `future_offset_lag` is `current_log.LEO − future_log.LEO`, clamped at 0.
    ///
    /// With current LEO 5 and future LEO 2 the answer is 3. This value
    /// separates the real subtraction from every mutant: `-> 0` gives 0,
    /// `-> 1` gives 1, `-` → `+` gives 7, and `-` → `/` gives 2.
    #[tokio::test]
    async fn future_offset_lag_is_current_minus_future_leo() {
        let cur_dir = tempfile::tempdir().unwrap();
        let fut_dir = tempfile::tempdir().unwrap();
        let reg = crate::partition_registry::PartitionRegistry::new();
        let part = partition_with_leo(cur_dir.path(), "t", crabka_ids::PartitionIndex(3), 5);
        assert!(part.log_end_offset() == crabka_log::Offset(5));
        reg.insert("t".to_string(), crabka_ids::PartitionIndex(3), part);

        let future_logs = dashmap::DashMap::new();
        future_logs.insert(
            ("t".to_string(), crabka_ids::PartitionIndex(3)),
            future_state_with_leo(fut_dir.path(), 2),
        );

        let lag = future_offset_lag(&reg, &future_logs, "t", crabka_ids::PartitionIndex(3));
        assert!(lag == 3, "current LEO 5 − future LEO 2 == 3, got {lag}");
    }

    #[test]
    fn cluster_describe_denied_yields_cluster_authorization_failed() {
        use crabka_protocol::owned::describe_log_dirs_response::{self, DescribeLogDirsResponse};

        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let principal = crabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));

        assert!(cluster_describe_denied(
            &authorizer,
            &image,
            &principal,
            &peer
        ));

        let bytes = denied_response(describe_log_dirs_response::MAX_VERSION).expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp =
            DescribeLogDirsResponse::decode(&mut cur, describe_log_dirs_response::MAX_VERSION)
                .unwrap();
        assert!(resp.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
    }
}
