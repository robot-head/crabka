//! Gzip (RFC-1952), via `flate2` with the pure-Rust `miniz_oxide` backend.

use std::io::{Read, Write};

use bytes::Bytes;
use flate2::Compression as GzipLevel;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;

use crate::CompressionError;

pub fn compress(data: &[u8]) -> Result<Bytes, CompressionError> {
    let mut encoder = GzEncoder::new(Vec::with_capacity(data.len()), GzipLevel::default());
    encoder.write_all(data)?;
    let out = encoder.finish()?;
    Ok(Bytes::from(out))
}

pub fn decompress(data: &[u8], max_output: usize) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        return Err(CompressionError::InvalidData("empty gzip payload".into()));
    }
    let decoder = GzDecoder::new(data);
    // Read at most `max_output + 1` bytes: the extra byte lets us detect that
    // the real output exceeds the cap without ever materializing it.
    let mut limited = decoder.take((max_output as u64).saturating_add(1));
    let mut out = Vec::with_capacity(data.len().saturating_mul(2).min(max_output));
    limited
        .read_to_end(&mut out)
        .map_err(|e| CompressionError::InvalidData(format!("gzip decode: {e}")))?;
    if out.len() > max_output {
        return Err(CompressionError::TooLarge { limit: max_output });
    }
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    const HELLO: &[u8] = b"hello kafka, this is a moderately repetitive payload to compress";
    const BIG_CAP: usize = 256 * 1024 * 1024;

    #[test]
    fn roundtrip() {
        let z = compress(HELLO).unwrap();
        assert!(z.len() < HELLO.len() + 32, "z={:?}", z.len());
        let back = decompress(&z, BIG_CAP).unwrap();
        assert!(back.as_ref() == HELLO);
    }

    #[test]
    fn decompress_empty_rejected() {
        assert!(matches!(
            decompress(b"", BIG_CAP),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn decompress_garbage_rejected() {
        assert!(matches!(
            decompress(b"this is not gzip", BIG_CAP),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn compress_empty_produces_valid_frame() {
        let z = compress(b"").unwrap();
        assert!(!z.is_empty(), "empty input still requires a gzip header");
        let back = decompress(&z, BIG_CAP).unwrap();
        assert!(back.as_ref() == b"");
    }

    #[test]
    fn decompression_bomb_rejected() {
        // 64 MiB of zeros compresses tiny but expands hugely.
        let bomb = vec![0u8; 64 * 1024 * 1024];
        let z = compress(&bomb).unwrap();
        assert!(matches!(
            decompress(&z, 1024),
            Err(CompressionError::TooLarge { limit: 1024 })
        ));
        let back = decompress(&z, BIG_CAP).unwrap();
        assert!(back.len() == bomb.len());
    }
}
