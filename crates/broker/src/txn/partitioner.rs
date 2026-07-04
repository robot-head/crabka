//! `murmur2(transactional_id) % num_partitions` — Apache Kafka's
//! `Utils.abs(murmur2(...)) % numPartitions` convention. Matches the
//! JVM client so a tid hashes to the same `__transaction_state`
//! partition on Crabka as it does on Apache Kafka.

use crate::kafka_hash::murmur2_partition;

/// Map a `transactional_id` to a partition index in
/// `__transaction_state`. Uses `i32`-cast then `abs` to match the JVM
/// `Utils.abs(int)` semantics, which returns 0 for `Integer.MIN_VALUE`
/// to avoid arithmetic overflow.
pub fn partition_for_tid(transactional_id: &str, num_partitions: i32) -> i32 {
    murmur2_partition(transactional_id.as_bytes(), num_partitions)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    // Reference vectors generated from the canonical JVM implementation:
    //   Utils.abs(Utils.murmur2(tid.getBytes(StandardCharsets.UTF_8))) % 50
    // where Utils is org.apache.kafka.common.utils.Utils.
    #[test]
    fn matches_jvm_for_canonical_tids() {
        let cases: &[(&str, i32)] = &[("my-tid", 43), ("producer-1", 45), ("tx-orders-prod", 26)];
        for (tid, expected) in cases {
            assert!(
                partition_for_tid(tid, 50) == *expected,
                "tid `{tid}` should hash to partition {expected}"
            );
        }
    }

    #[test]
    fn always_in_bounds() {
        for s in [
            "",
            "a",
            "really-long-transactional-id-with-many-bytes-and-symbols-!@#$%",
        ] {
            for n in [1, 50, 256] {
                let p = partition_for_tid(s, n);
                assert!((0..n).contains(&p));
            }
        }
    }

    #[test]
    fn min_value_input_does_not_break_bounds() {
        // Sanity test verifying the i32::MIN guard in Utils.abs semantics.
        // We can't easily construct a tid that murmur2's to exactly i32::MIN,
        // but verify partition_for_tid never returns negative for a diverse set of inputs.
        let long_repeated = "x".repeat(64);
        let inputs: &[&str] = &[
            "",
            "a",
            "tid",
            "transactional-id-123",
            &long_repeated,
            "00000000",
            "1111111111111111",
            "deadbeef",
            "very-long-string-that-might-trigger-edge-cases-in-murmur2-mixing",
        ];
        for s in inputs {
            for num_partitions in [1, 3, 50, 256] {
                let p = partition_for_tid(s, num_partitions);
                assert!(
                    (0..num_partitions).contains(&p),
                    "tid={s:?}, num_partitions={num_partitions} produced p={p} (out of bounds)"
                );
            }
        }
    }
}
