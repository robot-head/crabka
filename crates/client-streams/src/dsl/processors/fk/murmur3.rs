//! `MurmurHash3` x64 128-bit — JVM `org.apache.kafka.streams.state.internals.Murmur3.hash128`.
//! Consumed by the FK-join subscription-send / resolver processors (hashing the
//! left value to staleness-check responses).

const C1: u64 = 0x87c3_7b91_1142_53d5;
const C2: u64 = 0x4cf5_ad43_2745_937f;
const DEFAULT_SEED: u32 = 104_729;

#[must_use]
pub(crate) fn hash128(data: &[u8]) -> [u8; 16] {
    let (h1, h2) = hash128_longs(data, DEFAULT_SEED);
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&h1.to_be_bytes());
    out[8..].copy_from_slice(&h2.to_be_bytes());
    out
}

fn hash128_longs(data: &[u8], seed: u32) -> (u64, u64) {
    let mut h1 = u64::from(seed);
    let mut h2 = u64::from(seed);
    let nblocks = data.len() / 16;
    for i in 0..nblocks {
        let base = i * 16;
        let mut k1 = u64::from_le_bytes(data[base..base + 8].try_into().unwrap());
        let mut k2 = u64::from_le_bytes(data[base + 8..base + 16].try_into().unwrap());
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dc_e729);
        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;
        h2 = h2.rotate_left(31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x3849_5ab5);
    }
    let tail = &data[nblocks * 16..];
    let mut k1: u64 = 0;
    let mut k2: u64 = 0;
    let len = tail.len();
    if len > 8 {
        for j in (8..len).rev() {
            k2 ^= u64::from(tail[j]) << ((j - 8) * 8);
        }
        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;
    }
    if len > 0 {
        for j in (0..len.min(8)).rev() {
            k1 ^= u64::from(tail[j]) << (j * 8);
        }
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }
    h1 ^= data.len() as u64;
    h2 ^= data.len() as u64;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    (h1, h2)
}

fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input() {
        // Murmur3.hash128("", seed=104729) — captured value (non-zero; seed != 0).
        assert_eq!(
            hex_bytes("9d2764a018e329428c3cf3b035938518"),
            hash128(b"").to_vec()
        );
    }

    #[test]
    fn matches_jvm_capture() {
        let v: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/testdata/fk_join/behavior.json"
            ))
            .unwrap(),
        )
        .unwrap();
        for e in v["murmur3"].as_array().unwrap() {
            let input = hex_bytes(e["input_hex"].as_str().unwrap());
            let want = hex_bytes(e["hash_hex"].as_str().unwrap());
            assert_eq!(
                hash128(&input).as_slice(),
                want.as_slice(),
                "murmur3 mismatch for input {}",
                e["input_hex"]
            );
        }
    }

    fn hex_bytes(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }
}
