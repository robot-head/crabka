//! `murmur2("{group}:{topicId}:{partition}") % num_partitions` — Apache
//! Kafka's share-coordinator key form, hashed with the same
//! `Utils.abs(murmur2(...)) % numPartitions` convention as
//! `__transaction_state` so a share key resolves to the same
//! `__share_group_state` partition on Crabka as on Apache Kafka.

// `murmur2` is exercised by `partition_for_share_key`; the constants and the
// helper become live once the share coordinator / FindCoordinator path call
// it in later tasks.

const SEED: u32 = 0x9747_b28c;
const M: u32 = 0x5bd1_e995;
const R: u32 = 24;

// Intentional truncation: murmur2 operates on the low 32 bits of the
// length, matching the JVM int-cast semantics.
#[allow(clippy::cast_possible_truncation)]
fn murmur2(data: &[u8]) -> u32 {
    let length = data.len();
    let mut h: u32 = SEED ^ (length as u32);
    let chunks = data.chunks_exact(4);
    let rem = chunks.remainder();
    for chunk in chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    match rem.len() {
        3 => {
            h ^= u32::from(rem[2]) << 16;
            h ^= u32::from(rem[1]) << 8;
            h ^= u32::from(rem[0]);
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= u32::from(rem[1]) << 8;
            h ^= u32::from(rem[0]);
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= u32::from(rem[0]);
            h = h.wrapping_mul(M);
        }
        _ => {}
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

/// Map a share-coordinator key `(group_id, topic_id, partition)` to a
/// partition index in `__share_group_state`. Builds Kafka's key string
/// `"{group_id}:{topic_id}:{partition}"`, hashes it with murmur2, and applies
/// the JVM `Utils.abs(int)` semantics (returns 0 for `i32::MIN`).
#[must_use]
pub fn partition_for_share_key(
    group_id: &str,
    topic_id: &uuid::Uuid,
    partition: i32,
    num: i32,
) -> i32 {
    let key = format!("{group_id}:{topic_id}:{partition}");
    let h = murmur2(key.as_bytes()).cast_signed();
    let abs = if h == i32::MIN { 0 } else { h.abs() };
    abs % num
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use uuid::Uuid;

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
