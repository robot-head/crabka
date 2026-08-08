//! Xerial-snappy framing over `snap` raw blocks.
//!
//! Kafka does not use Google's official Snappy stream format. It uses
//! "xerial-snappy", a Java-library convention with this layout:
//!
//! ```text
//! [\x82 'S' 'N' 'A' 'P' 'P' 'Y' \x00]                 # 8-byte magic
//! [\x00 \x00 \x00 \x01]                               # version       (BE u32)
//! [\x00 \x00 \x00 \x01]                               # minCompatibleVersion (BE u32)
//! ( [BE u32 chunk length] [raw snappy block ...] )*   # zero or more chunks
//! ```
//!
//! There is no end-of-stream marker. Chunks run until EOF.
//!
//! The differential test suite verifies JVM xerial-snappy byte equality after
//! the oracle gets a `compress` op.

use bytes::{BufMut, Bytes, BytesMut};
use crabka_units::prelude::{ByteSize, ByteSizeExt as _, kibibytes};

use crate::CompressionError;

/// Xerial framing header. 16 bytes total.
const XERIAL_HEADER: [u8; 16] = [
    0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0x00, 0x00, 0x00, 0x00, 0x01, // version = 1
    0x00, 0x00, 0x00, 0x01, // minCompatibleVersion = 1
];

/// Largest single chunk Kafka writes.
///
/// Kafka's `SnappyOutputStream` writes chunks up to 32 KiB by default. The same
/// size keeps our output byte-identical with the JVM for differential-equal
/// cases.
const XERIAL_CHUNK: ByteSize = kibibytes(32);

pub fn compress(data: &[u8]) -> Result<Bytes, CompressionError> {
    let mut out = BytesMut::with_capacity(XERIAL_HEADER.len() + data.len());
    out.put_slice(&XERIAL_HEADER);

    let mut encoder = snap::raw::Encoder::new();
    // `slice::chunks` is a primitive-typed substrate: hand it a raw count.
    for chunk in data.chunks(XERIAL_CHUNK.bytes_usize()) {
        let max = snap::raw::max_compress_len(chunk.len());
        let mut buf = vec![0u8; max];
        let n = encoder
            .compress(chunk, &mut buf)
            .map_err(|e| CompressionError::InvalidData(format!("snappy encode: {e}")))?;
        out.put_u32(u32::try_from(n).expect("chunk size fits u32"));
        out.put_slice(&buf[..n]);
    }
    Ok(out.freeze())
}

pub fn decompress(data: &[u8], max_output: usize) -> Result<Bytes, CompressionError> {
    if data.len() < XERIAL_HEADER.len() {
        return Err(CompressionError::InvalidData(
            "snappy payload too short for xerial header".into(),
        ));
    }
    if data[..8] != XERIAL_HEADER[..8] {
        return Err(CompressionError::InvalidData(
            "snappy missing xerial magic".into(),
        ));
    }
    // Ignore version fields (bytes 8..16); Kafka never bumped them.
    let mut rest = &data[XERIAL_HEADER.len()..];

    let mut out = BytesMut::with_capacity(data.len().saturating_mul(2).min(max_output));
    let mut decoder = snap::raw::Decoder::new();
    while !rest.is_empty() {
        if rest.len() < 4 {
            return Err(CompressionError::InvalidData(
                "snappy chunk header truncated".into(),
            ));
        }
        let len = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
        rest = &rest[4..];
        if rest.len() < len {
            return Err(CompressionError::InvalidData(
                "snappy chunk body truncated".into(),
            ));
        }
        let (block, tail) = rest.split_at(len);
        rest = tail;

        let max_out = snap::raw::decompress_len(block)
            .map_err(|e| CompressionError::InvalidData(format!("snappy decode_len: {e}")))?;
        // Reject before allocating this chunk if it would push us past the cap.
        // `decompress_len` reads the block's stored uncompressed size, so this
        // bounds allocation without materializing the oversized output.
        if out.len().saturating_add(max_out) > max_output {
            return Err(CompressionError::TooLarge { limit: max_output });
        }
        let mut buf = vec![0u8; max_out];
        let n = decoder
            .decompress(block, &mut buf)
            .map_err(|e| CompressionError::InvalidData(format!("snappy decode: {e}")))?;
        out.put_slice(&buf[..n]);
    }
    Ok(out.freeze())
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
    fn decompress_truncated_header() {
        assert2::assert!(matches!(
            decompress(&XERIAL_HEADER[..4], BIG_CAP),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn decompress_missing_magic() {
        let bytes = [0u8; 20];
        assert2::assert!(matches!(
            decompress(&bytes, BIG_CAP),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn decompress_truncated_chunk() {
        let mut bytes = XERIAL_HEADER.to_vec();
        bytes.extend_from_slice(&[0, 0, 0, 100]); // claim 100-byte chunk
        bytes.push(0); // only 1 byte present
        assert2::assert!(matches!(
            decompress(&bytes, BIG_CAP),
            Err(CompressionError::InvalidData(_))
        ));
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
        // The cumulative per-chunk cap check is `out + chunk > max_output`
        // (not `>=`), so a cap equal to the exact output size must pass.
        let back = decompress(&z, HELLO.len()).unwrap();
        assert2::assert!(back.as_ref() == HELLO);
        // One byte under the exact size is rejected.
        assert2::assert!(matches!(
            decompress(&z, HELLO.len() - 1),
            Err(CompressionError::TooLarge { limit }) if limit == HELLO.len() - 1
        ));
    }

    /// Count the xerial length-prefixed chunks in a compressed payload.
    fn chunk_count(compressed: &[u8]) -> usize {
        let mut rest = &compressed[XERIAL_HEADER.len()..];
        let mut chunks = 0;
        while rest.len() >= 4 {
            let len = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
            rest = &rest[4 + len..];
            chunks += 1;
        }
        chunks
    }

    #[test]
    fn compress_splits_exactly_at_the_xerial_chunk_boundary() {
        // The chunk size is what keeps our framing byte-identical with the JVM's
        // `SnappyOutputStream`, so pin it by behaviour rather than by reading the
        // constant: a payload of exactly one chunk emits one length-prefixed
        // chunk, and a single byte more emits two. A mis-scaled `ByteSize` (a
        // stray factor of 1024 either way) moves that boundary and fails here.
        let chunk = XERIAL_CHUNK.bytes_usize();
        for (payload_len, expected_chunks) in [
            (4096, 1),
            (chunk - 1, 1),
            (chunk, 1),
            (chunk + 1, 2),
            (2 * chunk, 2),
            (2 * chunk + 1, 3),
        ] {
            // Incompressible bytes, so no chunk collapses to nothing.
            let payload: Vec<u8> = (0..payload_len)
                .map(|i| u8::try_from(i % 251).unwrap())
                .collect();
            let z = compress(&payload).unwrap();
            assert2::check!(
                chunk_count(&z) == expected_chunks,
                "payload of {payload_len} bytes"
            );
        }
    }

    #[test]
    fn decompress_bare_header_is_empty() {
        // compress("") emits exactly the 16-byte xerial header and no chunks;
        // decompressing it must succeed and yield empty. Boundary on the
        // `data.len() < header` guard: a length equal to the header is valid.
        let z = compress(b"").unwrap();
        assert2::assert!(z.len() == XERIAL_HEADER.len());
        let back = decompress(&z, 1024).unwrap();
        assert2::assert!(back.is_empty());
    }

    #[test]
    fn decompress_four_byte_chunk_header_reads_length() {
        // With exactly a 4-byte chunk header present and no body, we must get
        // PAST the "header truncated" guard (`rest.len() < 4` is false at 4)
        // and fail on the missing *body* instead. Pins the `< 4` boundary.
        let mut bytes = XERIAL_HEADER.to_vec();
        bytes.extend_from_slice(&[0, 0, 0, 5]); // claim a 5-byte chunk, supply none
        let Err(CompressionError::InvalidData(msg)) = decompress(&bytes, 1024) else {
            panic!("expected InvalidData");
        };
        assert2::assert!(msg.contains("body"));
    }
}
