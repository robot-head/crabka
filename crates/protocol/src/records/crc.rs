//! CRC-32C (Castagnoli) wrapping the `crc32c` crate. Kafka v2 record batches
//! use this CRC over everything after the `crc` field of the header.

/// CRC-32C of the input.
#[must_use]
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Standard CRC-32C reference vectors.
    /// "123456789" -> 0xE3069283 (RFC 3720 / iSCSI).
    const VECTORS: &[(&[u8], u32)] = &[
        (b"", 0x0000_0000),
        (b"a", 0xC1D04330),
        (b"123456789", 0xE306_9283),
        (b"The quick brown fox jumps over the lazy dog", 0x22620404),
    ];

    #[test]
    fn known_vectors() {
        for (input, expected) in VECTORS {
            let got = crc32c(input);
            assert_eq!(
                got, *expected,
                "input={:?}: expected {:#010x}, got {:#010x}",
                input, expected, got
            );
        }
    }
}
