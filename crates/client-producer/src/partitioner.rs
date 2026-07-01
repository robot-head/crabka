//! `UniformStickyPartitioner` — Java 3.0+ default. Hash-on-key for keyed
//! records; sticky-per-topic for null-key records, rotating only when the
//! current accumulator drains.

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Default)]
pub struct UniformStickyPartitioner {
    sticky: Mutex<HashMap<String, i32>>,
}

impl UniformStickyPartitioner {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Pick the partition for a record.
    ///
    /// `num_partitions` must be > 0.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(topic = %topic, keyed = key.is_some(), num_partitions),
    )]
    pub fn pick(&self, topic: &str, key: Option<&[u8]>, num_partitions: i32) -> i32 {
        assert!(num_partitions > 0, "num_partitions must be > 0");
        if let Some(k) = key {
            let h = murmur2(k);
            // Result is always in [0, num_partitions) which fits i32
            // because num_partitions: i32 and unsigned_abs() % u32 ≤ i32::MAX.
            #[allow(clippy::cast_possible_wrap)]
            let result = (h.unsigned_abs() % num_partitions.cast_unsigned()) as i32;
            result
        } else {
            let mut s = self.sticky.lock().expect("sticky mutex poisoned");
            *s.entry(topic.to_string()).or_insert(0) % num_partitions
        }
    }

    /// Rotate the sticky partition for `topic` to a new one (called by the
    /// sender after a batch flushes). Not yet wired in — the sender will
    /// invoke this on linger expiry in a follow-up; documented and
    /// tested already.
    #[allow(dead_code)]
    pub fn rotate(&self, topic: &str, num_partitions: i32) {
        if num_partitions <= 0 {
            return;
        }
        let mut s = self.sticky.lock().expect("sticky mutex poisoned");
        let entry = s.entry(topic.to_string()).or_insert(0);
        *entry = (*entry + 1) % num_partitions;
    }
}

/// `MurmurHash2` — Kafka's `DefaultPartitioner` key hash.
/// Reference implementation; length cast to u32 matches the canonical spec.
fn murmur2(data: &[u8]) -> i32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let length = data.len();
    // Reference MurmurHash2 impl truncates length to u32 as part of the spec.
    #[allow(clippy::cast_possible_truncation)]
    let mut h: u32 = SEED ^ (length as u32);

    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();
    for chunk in chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }

    match remainder.len() {
        3 => {
            h ^= u32::from(remainder[2]) << 16;
            h ^= u32::from(remainder[1]) << 8;
            h ^= u32::from(remainder[0]);
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= u32::from(remainder[1]) << 8;
            h ^= u32::from(remainder[0]);
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= u32::from(remainder[0]);
            h = h.wrapping_mul(M);
        }
        _ => {}
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;

    // Reinterpret bits as i32 — intentional per MurmurHash2 reference spec.
    h.cast_signed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn key_hash_is_stable_across_calls() {
        let p = UniformStickyPartitioner::new();
        let a = p.pick("t", Some(b"my-key"), 12);
        let b = p.pick("t", Some(b"my-key"), 12);
        assert!(a == b);
        assert!(a == 9);
        assert!((0..12).contains(&a));
    }

    #[test]
    fn null_key_uses_sticky_partition() {
        let p = UniformStickyPartitioner::new();
        let a = p.pick("t", None, 4);
        let b = p.pick("t", None, 4);
        let c = p.pick("t", None, 4);
        assert!(a == b);
        assert!(b == c);
    }

    #[test]
    fn rotate_moves_sticky_to_next() {
        let p = UniformStickyPartitioner::new();
        let a = p.pick("t", None, 4);
        p.rotate("t", 4);
        let b = p.pick("t", None, 4);
        assert!(a != b);
        assert!(b == (a + 1) % 4);
    }

    #[test]
    fn rotate_wraps_at_partition_count() {
        let p = UniformStickyPartitioner::new();

        assert!(p.pick("t", None, 3) == 0);
        p.rotate("t", 3);
        assert!(p.pick("t", None, 3) == 1);
        p.rotate("t", 3);
        assert!(p.pick("t", None, 3) == 2);
        p.rotate("t", 3);
        assert!(p.pick("t", None, 3) == 0);
        assert!(*p.sticky.lock().unwrap().get("t").unwrap() == 0);
    }

    #[test]
    fn distinct_topics_have_distinct_sticky_state() {
        let p = UniformStickyPartitioner::new();
        let _ = p.pick("a", None, 4);
        p.rotate("a", 4);
        // Topic "b"'s sticky is still 0.
        assert!(p.pick("b", None, 4) == 0);
    }

    #[test]
    fn murmur2_matches_kafka_golden_vectors() {
        assert!(murmur2(b"") == 275_646_681);
        assert!(murmur2(b"a") == -1_563_381_124);
        assert!(murmur2(b"ab") == 316_155_434);
        assert!(murmur2(b"abc") == 479_470_107);
        assert!(murmur2(b"abcd") == -1_323_649_548);
        assert!(murmur2(b"abcde") == 461_995_741);
        assert!(murmur2(b"kafka") == -798_503_068);
        assert!(murmur2(b"my-key") == 1_748_425_209);
    }
}
