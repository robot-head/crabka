//! FNV-sharded trace-id bloom filter for index-less trace lookup.

use serde::{Deserialize, Serialize};

/// FNV-1 32-bit hash.
#[must_use]
pub fn fnv1_32(bytes: &[u8]) -> u32 {
    const OFFSET: u32 = 2_166_136_261;
    const PRIME: u32 = 16_777_619;

    let mut hash = OFFSET;
    for &b in bytes {
        hash = hash.wrapping_mul(PRIME);
        hash ^= u32::from(b);
    }
    hash
}

fn fnv1a_32(bytes: &[u8]) -> u32 {
    const OFFSET: u32 = 2_166_136_261;
    const PRIME: u32 = 16_777_619;

    let mut hash = OFFSET;
    for &b in bytes {
        hash ^= u32::from(b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BloomShard {
    bits: Vec<u64>,
    num_bits: u64,
    k: u32,
}

impl BloomShard {
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss
    )]
    fn new(expected_items: usize, fp_rate: f64) -> Self {
        let n = expected_items.max(1) as f64;
        let fp_rate = fp_rate.clamp(0.000_001, 0.5);
        let m = (-(n * fp_rate.ln()) / (std::f64::consts::LN_2 * std::f64::consts::LN_2))
            .ceil()
            .max(64.0);
        let k = ((m / n) * std::f64::consts::LN_2).round().max(1.0) as u32;
        let num_bits = m as u64;
        let words = usize::try_from(num_bits.div_ceil(64)).unwrap_or(usize::MAX);
        Self {
            bits: vec![0_u64; words],
            num_bits,
            k,
        }
    }

    fn probes(&self, trace_id: &[u8; 16]) -> impl Iterator<Item = u64> + '_ {
        let h1 = u64::from(fnv1_32(trace_id));
        let h2 = u64::from(fnv1a_32(trace_id)) | 1;
        let num_bits = self.num_bits;
        (0..u64::from(self.k)).map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % num_bits)
    }

    fn insert(&mut self, trace_id: &[u8; 16]) {
        let probes: Vec<u64> = self.probes(trace_id).collect();
        for bit in probes {
            self.bits[(bit / 64) as usize] |= 1_u64 << (bit % 64);
        }
    }

    fn maybe_contains(&self, trace_id: &[u8; 16]) -> bool {
        self.probes(trace_id)
            .all(|bit| self.bits[(bit / 64) as usize] & (1_u64 << (bit % 64)) != 0)
    }
}

/// Sharded trace-id bloom filter.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardedTraceBloom {
    shards: Vec<BloomShard>,
}

impl ShardedTraceBloom {
    #[must_use]
    pub fn new(shard_count: usize, expected_items_per_shard: usize, fp_rate: f64) -> Self {
        let shard_count = shard_count.max(1);
        Self {
            shards: (0..shard_count)
                .map(|_| BloomShard::new(expected_items_per_shard, fp_rate))
                .collect(),
        }
    }

    #[must_use]
    pub fn with_tempo_defaults(expected_items: usize) -> Self {
        const ITEMS_PER_100_KIB_SHARD: usize = 85_000;
        let shard_count = expected_items.div_ceil(ITEMS_PER_100_KIB_SHARD).max(1);
        let per_shard = expected_items.div_ceil(shard_count).max(1);
        Self::new(shard_count, per_shard, 0.01)
    }

    #[must_use]
    pub fn shard_of(&self, trace_id: &[u8; 16]) -> usize {
        (fnv1_32(trace_id) as usize) % self.shards.len()
    }

    pub fn insert(&mut self, trace_id: &[u8; 16]) {
        let shard = self.shard_of(trace_id);
        self.shards[shard].insert(trace_id);
    }

    #[must_use]
    pub fn maybe_contains(&self, trace_id: &[u8; 16]) -> bool {
        let shard = self.shard_of(trace_id);
        self.shards[shard].maybe_contains(trace_id)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn tid(n: u8) -> [u8; 16] {
        let mut t = [0u8; 16];
        t[0] = n;
        t[15] = n.wrapping_mul(7);
        t
    }

    #[test]
    fn no_false_negatives() {
        let mut b = ShardedTraceBloom::new(8, 64, 0.01);
        for n in 0..64_u8 {
            b.insert(&tid(n));
        }
        for n in 0..64_u8 {
            assert!(b.maybe_contains(&tid(n)));
        }
    }

    #[test]
    fn false_positive_rate_is_bounded() {
        let mut b = ShardedTraceBloom::new(16, 256, 0.01);
        for n in 0..=255_u8 {
            b.insert(&tid(n));
        }
        let mut fp = 0_u32;
        let mut probes = 0_u32;
        for n in 256_u32..4352 {
            let mut t = [0_u8; 16];
            t[0..4].copy_from_slice(&n.to_le_bytes());
            t[15] = 0xAB;
            probes += 1;
            if b.maybe_contains(&t) {
                fp += 1;
            }
        }
        let rate = f64::from(fp) / f64::from(probes);
        assert!(rate < 0.05);
    }

    #[test]
    fn shard_is_fnv_mod_count() {
        let b = ShardedTraceBloom::new(16, 64, 0.01);
        let t = tid(42);
        assert!(b.shard_of(&t) == (fnv1_32(&t) as usize) % 16);
    }

    #[test]
    fn fnv1_32_is_stable() {
        let h = fnv1_32(&[0_u8]);
        let expected = 2_166_136_261_u32.wrapping_mul(16_777_619);
        assert!(h == expected);
    }

    #[test]
    fn snapshot_round_trips() {
        let mut b = ShardedTraceBloom::new(4, 32, 0.01);
        b.insert(&tid(1));
        let json = serde_json::to_vec(&b).unwrap();
        let back: ShardedTraceBloom = serde_json::from_slice(&json).unwrap();
        assert!(back.maybe_contains(&tid(1)));
    }
}
