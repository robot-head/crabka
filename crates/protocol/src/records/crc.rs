//! CRC-32C (Castagnoli) wrapping the `crc32c` crate. Kafka v2 record batches
//! use this CRC over everything after the `crc` field of the header.

/// CRC-32C of the input.
#[must_use]
pub(crate) fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Continue a CRC-32C computation over additional `data`, starting from `seed`.
#[must_use]
pub(crate) fn crc32c_append(seed: u32, data: &[u8]) -> u32 {
    crc32c::crc32c_append(seed, data)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Standard CRC-32C reference vectors.
    /// "123456789" -> 0xE3069283 (RFC 3720 / iSCSI).
    const VECTORS: &[(&[u8], u32)] = &[
        (b"", 0x0000_0000),
        (b"a", 0xC1D0_4330),
        (b"123456789", 0xE306_9283),
        (b"The quick brown fox jumps over the lazy dog", 0x2262_0404),
    ];

    #[test]
    fn known_vectors() {
        for (input, expected) in VECTORS {
            let got = crc32c(input);
            assert_eq!(
                got, *expected,
                "input={input:?}: expected {expected:#010x}, got {got:#010x}",
            );
        }
    }
}
