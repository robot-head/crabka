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

pub fn decompress(data: &[u8]) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        return Err(CompressionError::InvalidData("empty gzip payload".into()));
    }
    let mut decoder = GzDecoder::new(data);
    let mut out = Vec::with_capacity(data.len() * 2);
    decoder
        .read_to_end(&mut out)
        .map_err(|e| CompressionError::InvalidData(format!("gzip decode: {e}")))?;
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    const HELLO: &[u8] = b"hello kafka, this is a moderately repetitive payload to compress";

    #[test]
    fn roundtrip() {
        let z = compress(HELLO).unwrap();
        assert!(z.len() < HELLO.len() + 32, "z={:?}", z.len());
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
            decompress(b"this is not gzip"),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn compress_empty_produces_valid_frame() {
        let z = compress(b"").unwrap();
        assert!(!z.is_empty(), "empty input still requires a gzip header");
        let back = decompress(&z).unwrap();
        assert_eq!(back.as_ref(), b"");
    }
}
