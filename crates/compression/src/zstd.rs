//! Zstd via the `zstd` crate (wraps libzstd).

use bytes::Bytes;

use crate::CompressionError;

/// Match Kafka's default zstd level.
const DEFAULT_LEVEL: i32 = 3;

pub fn compress(data: &[u8]) -> Result<Bytes, CompressionError> {
    let out = zstd::bulk::compress(data, DEFAULT_LEVEL)?;
    Ok(Bytes::from(out))
}

pub fn decompress(data: &[u8]) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        return Err(CompressionError::InvalidData("empty zstd payload".into()));
    }
    // The decompressor needs an upper bound on the output size. We don't
    // know it ahead of time, so use `bulk::Decompressor` and feed the
    // input through a growable buffer.
    let mut decoder = zstd::stream::Decoder::new(data)
        .map_err(|e| CompressionError::InvalidData(format!("zstd open: {e}")))?;
    let mut out = Vec::with_capacity(data.len() * 4);
    std::io::Read::read_to_end(&mut decoder, &mut out)
        .map_err(|e| CompressionError::InvalidData(format!("zstd decode: {e}")))?;
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    const HELLO: &[u8] = b"hello kafka, this is a moderately repetitive payload to compress";

    #[test]
    fn roundtrip() {
        let z = compress(HELLO).unwrap();
        let back = decompress(&z).unwrap();
        assert_eq!(back.as_ref(), HELLO);
    }

    #[test]
    fn decompress_empty_rejected() {
        assert!(matches!(
            decompress(b""),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn decompress_garbage_rejected() {
        assert!(matches!(
            decompress(b"this is not zstd"),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn larger_payload_roundtrips() {
        let big = vec![0xABu8; 128 * 1024];
        let z = compress(&big).unwrap();
        let back = decompress(&z).unwrap();
        assert_eq!(back.as_ref(), big.as_slice());
    }
}
