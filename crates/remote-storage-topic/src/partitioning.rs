//! Deterministic `TopicIdPartition` → metadata-topic-partition mapping.
//!
//! Every metadata event for one user partition must land in the same
//! `__remote_log_metadata` partition so its lifecycle ordering survives
//! the round trip through Kafka. The hash inputs (`topic_id` +
//! `partition`) match the identity of `TopicIdPartition`, which means a
//! topic rename does not move the bucket.

use std::{collections::hash_map::DefaultHasher, hash::Hasher};

use crabka_remote_storage::TopicIdPartition;

/// Pick the metadata-topic partition for `tp`, given the
/// `partition_count` of `__remote_log_metadata`.
///
/// # Panics
///
/// Panics when `partition_count <= 0`.
#[must_use]
pub fn metadata_partition_for(tp: &TopicIdPartition, partition_count: i32) -> i32 {
    assert2::assert!(partition_count > 0);
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

/// Deduped, sorted set of `__remote_log_metadata` partitions that carry
/// metadata for the given user-topic-partitions, given the metadata topic's
/// `partition_count`. This is the set a broker must consume to serve remote
/// reads for the partitions it leads or follows.
///
/// # Panics
///
/// Panics when `partition_count <= 0` (via [`metadata_partition_for`]).
#[must_use]
pub fn metadata_partitions_for<'a, I>(tps: I, partition_count: i32) -> Vec<i32>
where
    I: IntoIterator<Item = &'a TopicIdPartition>,
{
    let mut set: Vec<i32> = tps
        .into_iter()
        .map(|tp| metadata_partition_for(tp, partition_count))
        .collect();
    set.sort_unstable();
    set.dedup();
    set
}

#[cfg(test)]
mod tests {

    use uuid::Uuid;

    use super::*;

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
            assert2::assert!((0..50).contains(&bucket));
        }
    }

    #[test]
    fn is_deterministic_across_calls() {
        let a = metadata_partition_for(&tp("orders", 7), 50);
        let b = metadata_partition_for(&tp("orders", 7), 50);
        assert2::assert!(a == b);
    }

    #[test]
    fn ignores_topic_name() {
        // Identity is (topic_id, partition); name is informational.
        let a = metadata_partition_for(&tp("orders", 3), 50);
        let b = metadata_partition_for(&tp("renamed", 3), 50);
        assert2::assert!(a == b);
    }

    #[test]
    fn different_partitions_distribute() {
        let mut seen = std::collections::HashSet::new();
        for p in 0..200 {
            seen.insert(metadata_partition_for(&tp("orders", p), 50));
        }
        // Sanity: a hash that always returned 0 would land only one bucket.
        assert2::assert!(seen.len() > 5);
    }

    #[test]
    fn single_partition_count_collapses_to_zero() {
        for p in 0..20 {
            assert2::assert!(metadata_partition_for(&tp("t", p), 1) == 0);
        }
    }

    #[test]
    #[should_panic(expected = "partition_count must be positive")]
    fn rejects_zero_partition_count() {
        let _ = metadata_partition_for(&tp("t", 0), 0);
    }

    #[test]
    fn metadata_partitions_for_dedupes_and_sorts() {
        // Two user-partitions that hash to the same metadata partition must
        // collapse to one entry; the result is sorted ascending.
        let a = tp("orders", 0);
        let b = tp("orders", 1);
        let pa = metadata_partition_for(&a, 50);
        let pb = metadata_partition_for(&b, 50);
        let got = metadata_partitions_for([a.clone(), b.clone(), a.clone()].iter(), 50);
        let mut expected: Vec<i32> = vec![pa, pb];
        expected.sort_unstable();
        expected.dedup();
        assert2::assert!(got == expected);
    }

    #[test]
    fn metadata_partitions_for_empty_is_empty() {
        let none: [TopicIdPartition; 0] = [];
        assert2::assert!(metadata_partitions_for(none.iter(), 50).is_empty());
    }
}
