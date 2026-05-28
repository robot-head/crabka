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
//! There is no end-of-stream marker; chunks run until EOF.
//!
//! JVM xerial-snappy byte equality is verified by the differential test
//! suite once the oracle gains a `compress` op.

use bytes::{BufMut, Bytes, BytesMut};

use crate::CompressionError;

/// Xerial framing header. 16 bytes total.
const XERIAL_HEADER: [u8; 16] = [
    0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0x00, 0x00, 0x00, 0x00, 0x01, // version = 1
    0x00, 0x00, 0x00, 0x01, // minCompatibleVersion = 1
];

/// Largest single chunk Kafka writes. Kafka's `SnappyOutputStream` writes
/// chunks up to 32 KiB by default; using the same size keeps our output
/// byte-identical with the JVM for differential-equal cases.
const XERIAL_CHUNK_SIZE: usize = 32 * 1024;

pub fn compress(data: &[u8]) -> Result<Bytes, CompressionError> {
    let mut out = BytesMut::with_capacity(XERIAL_HEADER.len() + data.len());
    out.put_slice(&XERIAL_HEADER);

    let mut encoder = snap::raw::Encoder::new();
    for chunk in data.chunks(XERIAL_CHUNK_SIZE) {
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

pub fn decompress(data: &[u8]) -> Result<Bytes, CompressionError> {
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

    let mut out = BytesMut::with_capacity(data.len() * 2);
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

    #[test]
    fn roundtrip() {
        let z = compress(HELLO).unwrap();
        let back = decompress(&z).unwrap();
        assert_eq!(back.as_ref(), HELLO);
    }

    #[test]
    fn decompress_truncated_header() {
        assert!(matches!(
            decompress(&XERIAL_HEADER[..4]),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn decompress_missing_magic() {
        let bytes = [0u8; 20];
        assert!(matches!(
            decompress(&bytes),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn decompress_truncated_chunk() {
        let mut bytes = XERIAL_HEADER.to_vec();
        bytes.extend_from_slice(&[0, 0, 0, 100]); // claim 100-byte chunk
        bytes.push(0); // only 1 byte present
        assert!(matches!(
            decompress(&bytes),
            Err(CompressionError::InvalidData(_))
        ));
    }
}
