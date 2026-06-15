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

pub fn decompress(data: &[u8], max_output: usize) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        return Err(CompressionError::InvalidData("empty lz4 payload".into()));
    }
    let decoder = FrameDecoder::new(data);
    // Read at most `max_output + 1` bytes so we can detect overflow without
    // materializing the oversized output.
    let mut limited = decoder.take((max_output as u64).saturating_add(1));
    let mut out = Vec::with_capacity(data.len().saturating_mul(2).min(max_output));
    limited
        .read_to_end(&mut out)
        .map_err(|e| CompressionError::InvalidData(format!("lz4 decode: {e}")))?;
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
            decompress(b"this is not lz4", BIG_CAP),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn larger_payload_roundtrips() {
        let big = vec![0xABu8; 128 * 1024]; // 128 KiB -> multiple 64 KiB blocks
        let z = compress(&big).unwrap();
        let back = decompress(&z, BIG_CAP).unwrap();
        assert!(back.as_ref() == big.as_slice());
    }

    #[test]
    fn decompression_bomb_rejected() {
        let bomb = vec![0u8; 64 * 1024 * 1024];
        let z = compress(&bomb).unwrap();
        assert!(matches!(
            decompress(&z, 1024),
            Err(CompressionError::TooLarge { limit: 1024 })
        ));
        let back = decompress(&z, BIG_CAP).unwrap();
        assert!(back.len() == bomb.len());
    }

    #[test]
    fn decompress_at_exact_cap_succeeds() {
        let z = compress(HELLO).unwrap();
        // Output of exactly `max_output` bytes is allowed (cap check is
        // `len > max_output`, not `>=`).
        let back = decompress(&z, HELLO.len()).unwrap();
        assert!(back.as_ref() == HELLO);
        // One byte under the exact size is rejected.
        assert!(matches!(
            decompress(&z, HELLO.len() - 1),
            Err(CompressionError::TooLarge { limit }) if limit == HELLO.len() - 1
        ));
    }

    #[test]
    fn frame_uses_64kib_independent_blocks() {
        // Compress a payload larger than 64 KiB so the block-size choice is
        // observable in the frame header: our explicit `Max64KB` must stay
        // 64 KiB rather than grow to an auto-selected larger block. This pins
        // the `frame_info()` settings (a `Default::default()` FrameInfo would
        // auto-pick a 256 KiB block for a payload this size).
        let big = vec![0xCDu8; 128 * 1024];
        let z = compress(&big).unwrap();
        // LZ4 frame layout: [magic:4][FLG][BD]...
        assert!(z[0..4] == [0x04, 0x22, 0x4D, 0x18]);
        let flg = z[4];
        let bd = z[5];
        // BD bits 4..6 encode the block max size; value 4 == 64 KiB.
        assert!((bd >> 4) & 0x7 == 4, "BD={bd:#04x}");
        // FLG bit 5 = block-independence flag (we request Independent).
        assert!((flg >> 5) & 1 == 1, "FLG={flg:#04x}");
        // FLG bit 4 = per-block checksum, bit 2 = content checksum: both off.
        assert!((flg >> 4) & 1 == 0, "FLG={flg:#04x}");
        assert!((flg >> 2) & 1 == 0, "FLG={flg:#04x}");
    }
}
