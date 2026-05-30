//! LZ4 frame format (LZ4F), independent blocks.
//!
//! Kafka writes LZ4 in the frame format (magic `0x04 22 4D 18`) with these
//! choices: 64 KiB block size, independent blocks, no block checksum, no
//! content-size in the header. We match those defaults so produced bytes
//! line up with `KafkaLZ4BlockOutputStream`'s output for differential
//! testing.

use std::io::{Read, Write};

use bytes::Bytes;
use lz4_flex::frame::{BlockMode, BlockSize, FrameDecoder, FrameEncoder, FrameInfo};

use crate::CompressionError;

fn frame_info() -> FrameInfo {
    FrameInfo::new()
        .block_size(BlockSize::Max64KB)
        .block_mode(BlockMode::Independent)
        .block_checksums(false)
        .content_checksum(false)
}

pub fn compress(data: &[u8]) -> Result<Bytes, CompressionError> {
    let mut encoder = FrameEncoder::with_frame_info(frame_info(), Vec::with_capacity(data.len()));
    encoder.write_all(data)?;
    let out = encoder
        .finish()
        .map_err(|e| CompressionError::InvalidData(format!("lz4 finish: {e}")))?;
    Ok(Bytes::from(out))
}

pub fn decompress(data: &[u8]) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        return Err(CompressionError::InvalidData("empty lz4 payload".into()));
    }
    let mut decoder = FrameDecoder::new(data);
    let mut out = Vec::with_capacity(data.len() * 2);
    decoder
        .read_to_end(&mut out)
        .map_err(|e| CompressionError::InvalidData(format!("lz4 decode: {e}")))?;
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    const HELLO: &[u8] = b"hello kafka, this is a moderately repetitive payload to compress";

    #[test]
    fn roundtrip() {
        let z = compress(HELLO).unwrap();
        let back = decompress(&z).unwrap();
        assert!(back.as_ref() == HELLO);
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
            decompress(b"this is not lz4"),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn larger_payload_roundtrips() {
        let big = vec![0xABu8; 128 * 1024]; // 128 KiB -> multiple 64 KiB blocks
        let z = compress(&big).unwrap();
        let back = decompress(&z).unwrap();
        assert!(back.as_ref() == big.as_slice());
    }
}
