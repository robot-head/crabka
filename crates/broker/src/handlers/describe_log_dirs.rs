//! `DescribeLogDirs` (`api_key=35`, KIP-113). Reports, per configured log
//! directory, the partitions it physically holds and their on-disk sizes.
//! Backs the `kafka-log-dirs --describe` admin tool.
//!
//! This is the JBOD read side. The intra-broker move side
//! (`AlterReplicaLogDirs`, api key 34) is a follow-up slice, so every log
//! reported here is a current log: `is_future_key` is always `false`.

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
use crate::partition::Partition;

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

            let topics = by_topic
                .into_iter()
                .map(|(name, partitions)| DescribeLogDirsTopic {
                    name,
                    partitions,
                    ..Default::default()
                })
                .collect();

            results.push(DescribeLogDirsResult {
                error_code: codes::NONE,
                log_dir: absolute_path(dir),
                topics,
                // total_bytes / usable_bytes (v4+) require a portable
                // statvfs we don't depend on yet; the generated default of
                // -1 means "unknown", which the JVM tooling tolerates.
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
    partitions: &dashmap::DashMap<(String, i32), std::sync::Arc<Partition>>,
    topic: &str,
    partition: i32,
) -> i64 {
    let Some(part) = partitions
        .get(&(topic.to_string(), partition))
        .map(|e| e.value().clone())
    else {
        return 0;
    };
    let leo = part.log_end_offset();
    let hw = part.high_watermark().await;
    (leo - hw).max(0)
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
}
