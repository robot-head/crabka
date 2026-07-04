//! Kafka-compatible hashing helpers.

const MURMUR2_SEED: u32 = 0x9747_b28c;
const MURMUR2_M: u32 = 0x5bd1_e995;
const MURMUR2_R: u32 = 24;

// Intentional truncation: Kafka's murmur2 uses a 32-bit length, matching
// JVM int-cast semantics for byte arrays longer than i32::MAX.
#[allow(clippy::cast_possible_truncation)]
#[must_use]
pub(crate) fn murmur2(data: &[u8]) -> u32 {
    let length = data.len();
    let mut h: u32 = MURMUR2_SEED ^ (length as u32);
    let chunks = data.chunks_exact(4);
    let rem = chunks.remainder();

    for chunk in chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(MURMUR2_M);
        k ^= k >> MURMUR2_R;
        k = k.wrapping_mul(MURMUR2_M);
        h = h.wrapping_mul(MURMUR2_M);
        h ^= k;
    }

    match rem {
        [a] => {
            h ^= u32::from(*a);
            h = h.wrapping_mul(MURMUR2_M);
        }
        [a, b] => {
            h ^= u32::from(*b) << 8;
            h ^= u32::from(*a);
            h = h.wrapping_mul(MURMUR2_M);
        }
        [a, b, c] => {
            h ^= u32::from(*c) << 16;
            h ^= u32::from(*b) << 8;
            h ^= u32::from(*a);
            h = h.wrapping_mul(MURMUR2_M);
        }
        [] => {}
        _ => unreachable!("chunks_exact(4) leaves at most three remainder bytes"),
    }

    h ^= h >> 13;
    h = h.wrapping_mul(MURMUR2_M);
    h ^= h >> 15;
    h
}

/// Apply Kafka's `Utils.abs(murmur2(data)) % numPartitions` convention.
#[must_use]
pub(crate) fn murmur2_partition(data: &[u8], num_partitions: i32) -> i32 {
    let hash = murmur2(data).cast_signed();
    let positive = if hash == i32::MIN { 0 } else { hash.abs() };
    positive % num_partitions
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn murmur2_matches_kafka_vectors_for_remainder_lengths() {
        let cases: &[(&[u8], u32)] = &[
            (b"", 0x106e_08d9),
            (b"a", 0xa2d0_b27c),
            (b"ab", 0x12d8_262a),
            (b"abc", 0x1c94_221b),
            (b"abcd", 0xb11a_b5f4),
            (b"abcde", 0x1b89_7edd),
        ];

        for (input, expected) in cases {
            assert!(murmur2(input) == *expected);
        }
    }

    #[test]
    fn murmur2_partition_matches_kafka_utils_abs_mod() {
        let cases: &[(&[u8], i32, i32)] = &[
            (b"my-tid", 50, 43),
            (b"producer-1", 50, 45),
            (b"tx-orders-prod", 50, 26),
            (b"abcde", 7, 4),
        ];

        for (input, partitions, expected) in cases {
            assert!(murmur2_partition(input, *partitions) == *expected);
        }
    }
}
