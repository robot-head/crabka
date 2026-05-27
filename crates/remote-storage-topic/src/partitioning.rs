//! Deterministic `TopicIdPartition` → metadata-topic-partition mapping.
//!
//! Every metadata event for one user partition must land in the same
//! `__remote_log_metadata` partition so its lifecycle ordering survives
//! the round trip through Kafka. The hash inputs (`topic_id` +
//! `partition`) match the identity of `TopicIdPartition`, which means a
//! topic rename does not move the bucket.

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use crabka_remote_storage::TopicIdPartition;

/// Pick the metadata-topic partition for `tp`, given the
/// `partition_count` of `__remote_log_metadata`.
///
/// # Panics
///
/// Panics when `partition_count <= 0`.
#[must_use]
pub fn metadata_partition_for(tp: &TopicIdPartition, partition_count: i32) -> i32 {
    assert!(partition_count > 0, "partition_count must be positive");
    let mut h = DefaultHasher::new();
    for byte in tp.topic_id.as_bytes() {
        h.write_u8(*byte);
    }
    h.write_i32(tp.partition);
    // Strip sign bit so `%` yields a non-negative bucket; the mask
    // keeps the result in [0, i64::MAX] so the i32 truncation after
    // the `%` is safe (bucket < partition_count < i32::MAX).
    #[allow(clippy::cast_possible_wrap)] // masked to i64::MAX
    let bucket = (h.finish() & i64::MAX as u64) as i64;
    #[allow(clippy::cast_possible_truncation)] // bucket % count fits in i32
    let p = (bucket % i64::from(partition_count)) as i32;
    p
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn tp(name: &str, partition: i32) -> TopicIdPartition {
        TopicIdPartition::new(
            Uuid::from_u128(0x1234_5678_9ABC_DEF0_0011_2233_4455_6677),
            name,
            partition,
        )
    }

    #[test]
    fn is_in_range() {
        for p in 0..10 {
            let bucket = metadata_partition_for(&tp("orders", p), 50);
            assert!((0..50).contains(&bucket), "bucket {bucket} out of [0,50)");
        }
    }

    #[test]
    fn is_deterministic_across_calls() {
        let a = metadata_partition_for(&tp("orders", 7), 50);
        let b = metadata_partition_for(&tp("orders", 7), 50);
        assert_eq!(a, b);
    }

    #[test]
    fn ignores_topic_name() {
        // Identity is (topic_id, partition); name is informational.
        let a = metadata_partition_for(&tp("orders", 3), 50);
        let b = metadata_partition_for(&tp("renamed", 3), 50);
        assert_eq!(a, b, "renaming a topic must not re-bucket its metadata");
    }

    #[test]
    fn different_partitions_distribute() {
        let mut seen = std::collections::HashSet::new();
        for p in 0..200 {
            seen.insert(metadata_partition_for(&tp("orders", p), 50));
        }
        // Sanity: a hash that always returned 0 would land only one bucket.
        assert!(
            seen.len() > 5,
            "hash should spread across buckets, got {}",
            seen.len()
        );
    }

    #[test]
    fn single_partition_count_collapses_to_zero() {
        for p in 0..20 {
            assert_eq!(metadata_partition_for(&tp("t", p), 1), 0);
        }
    }

    #[test]
    #[should_panic(expected = "partition_count must be positive")]
    fn rejects_zero_partition_count() {
        let _ = metadata_partition_for(&tp("t", 0), 0);
    }
}
