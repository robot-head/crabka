//! `murmur2("{group}:{topicId}:{partition}") % num_partitions`.
//!
//! This is Apache Kafka's share-coordinator key form. The module hashes it
//! with the same `Utils.abs(murmur2(...)) % numPartitions` convention as
//! `__transaction_state`. A share key therefore resolves to the same
//! `__share_group_state` partition on Crabka as on Apache Kafka.

use crate::kafka_hash::murmur2_partition;

/// Map a share-coordinator key `(group_id, topic_id, partition)` to a
/// partition index in `__share_group_state`.
///
/// The function builds Kafka's key string
/// `"{group_id}:{topic_id}:{partition}"` and hashes it with murmur2. It then
/// applies the JVM `Utils.abs(int)` semantics, which return 0 for `i32::MIN`.
#[must_use]
pub fn partition_for_share_key(
    group_id: &str,
    topic_id: &uuid::Uuid,
    partition: i32,
    num: i32,
) -> i32 {
    let key = format!("{group_id}:{topic_id}:{partition}");
    murmur2_partition(key.as_bytes(), num)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn deterministic_for_same_key() {
        let id = Uuid::from_bytes([5; 16]);
        let a = partition_for_share_key("g", &id, 0, 50);
        let b = partition_for_share_key("g", &id, 0, 50);
        assert!(a == b);
    }

    #[test]
    fn distinct_keys_differ_somewhere() {
        let id = Uuid::from_bytes([5; 16]);
        // Not all distinct keys must differ, but the partition must depend on
        // every component for at least some inputs.
        let p0 = partition_for_share_key("g", &id, 0, 50);
        let p1 = partition_for_share_key("g", &id, 1, 50);
        let pg = partition_for_share_key("h", &id, 0, 50);
        assert!(p0 != p1 || p0 != pg);
    }

    #[test]
    fn always_in_bounds() {
        let ids = [
            Uuid::nil(),
            Uuid::from_bytes([255; 16]),
            Uuid::from_bytes([1; 16]),
        ];
        for id in ids {
            for g in ["", "group", "a-very-long-share-group-id-with-symbols-!@#"] {
                for p in [0, 7, 49, i32::MAX] {
                    for num in [1, 3, 50, 256] {
                        let idx = partition_for_share_key(g, &id, p, num);
                        assert!(
                            (0..num).contains(&idx),
                            "g={g:?} id={id} p={p} num={num} produced {idx}"
                        );
                    }
                }
            }
        }
    }
}
