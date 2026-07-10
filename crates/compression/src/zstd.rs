//! Zstd via the `zstd` crate (wraps libzstd).

use bytes::Bytes;

use crate::CompressionError;

/// Match Kafka's default zstd level.
const DEFAULT_LEVEL: i32 = 3;

pub fn compress(data: &[u8]) -> Result<Bytes, CompressionError> {
    let out = zstd::bulk::compress(data, DEFAULT_LEVEL)?;
    Ok(Bytes::from(out))
}

pub fn decompress(data: &[u8], max_output: usize) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        return Err(CompressionError::InvalidData("empty zstd payload".into()));
    }
    let decoder = zstd::stream::Decoder::new(data)
        .map_err(|e| CompressionError::InvalidData(format!("zstd open: {e}")))?;
    // Read at most `max_output + 1` bytes so we can detect overflow without
    // materializing the oversized output.
    let mut limited = std::io::Read::take(decoder, (max_output as u64).saturating_add(1));
    let mut out = Vec::with_capacity(data.len().saturating_mul(2).min(max_output));
    std::io::Read::read_to_end(&mut limited, &mut out)
        .map_err(|e| CompressionError::InvalidData(format!("zstd decode: {e}")))?;
    if out.len() > max_output {
        return Err(CompressionError::TooLarge { limit: max_output });
    }
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {

    use super::*;

    const HELLO: &[u8] = b"hello kafka, this is a moderately repetitive payload to compress";
    const BIG_CAP: usize = 256 * 1024 * 1024;

    #[test]
    fn roundtrip() {
        let z = compress(HELLO).unwrap();
        let back = decompress(&z, BIG_CAP).unwrap();
        assert2::assert!(back.as_ref() == HELLO);
    }

    #[test]
    fn decompress_empty_rejected() {
        assert2::assert!(matches!(
            decompress(b"", BIG_CAP),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn decompress_garbage_rejected() {
        assert2::assert!(matches!(
            decompress(b"this is not zstd", BIG_CAP),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn larger_payload_roundtrips() {
        let big = vec![0xABu8; 128 * 1024];
        let z = compress(&big).unwrap();
        let back = decompress(&z, BIG_CAP).unwrap();
        assert2::assert!(back.as_ref() == big.as_slice());
    }

    #[test]
    fn decompression_bomb_rejected() {
        let bomb = vec![0u8; 64 * 1024 * 1024];
        let z = compress(&bomb).unwrap();
        assert2::assert!(matches!(
            decompress(&z, 1024),
            Err(CompressionError::TooLarge { limit: 1024 })
        ));
        let back = decompress(&z, BIG_CAP).unwrap();
        assert2::assert!(back.as_ref() == bomb.as_slice());
    }

    #[test]
    fn decompress_at_exact_cap_succeeds() {
        let z = compress(HELLO).unwrap();
        // Output of exactly `max_output` bytes is allowed (cap check is
        // `len > max_output`, not `>=`).
        let back = decompress(&z, HELLO.len()).unwrap();
        assert2::assert!(back.as_ref() == HELLO);
        // One byte under the exact size is rejected.
        assert2::assert!(matches!(
            decompress(&z, HELLO.len() - 1),
            Err(CompressionError::TooLarge { limit }) if limit == HELLO.len() - 1
        ));
    }
}
