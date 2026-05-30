//! v0/v1 `Message` wire format.
//!
//! Wire layout (sit inside a [`MessageSet`](crate::set) entry, after the
//! per-entry `(offset:i64, message_size:i32)` framing):
//!
//! ```text
//! v0: crc:u32 | magic:i8=0 | attrs:i8 |                 | key | value
//! v1: crc:u32 | magic:i8=1 | attrs:i8 | timestamp:i64   | key | value
//! ```
//!
//! `key` and `value` are nullable bytes (i32 length; -1 means null). CRC-32
//! (IEEE polynomial) is computed over the bytes from `magic` through the
//! end of `value` — i.e. everything inside the message except the CRC
//! field itself.

use bytes::{Buf, BufMut, Bytes};
use crabka_compression::CompressionType;

use crate::error::LegacyRecordsError;

/// Magic byte (i.e. legacy message format version) — 0 or 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Magic {
    V0,
    V1,
}

impl Magic {
    #[must_use]
    pub const fn as_i8(self) -> i8 {
        match self {
            Self::V0 => 0,
            Self::V1 => 1,
        }
    }

    pub fn from_i8(b: i8) -> Result<Self, LegacyRecordsError> {
        match b {
            0 => Ok(Self::V0),
            1 => Ok(Self::V1),
            other => Err(LegacyRecordsError::UnsupportedMagic { found: other }),
        }
    }
}

/// Bit layout of the legacy `attributes` byte.
pub mod attrs {
    pub const COMPRESSION_MASK: i8 = 0x07;
    /// v1-only: 0 = `CreateTime`, 1 = `LogAppendTime`.
    pub const TIMESTAMP_TYPE_BIT: i8 = 1 << 3;
}

/// Map a v0/v1 compression-code (low 3 bits of `attributes`) to a
/// [`CompressionType`].
///
/// `0` => `None`, `1` => `Gzip`, `2` => `Snappy`, `3` => `Lz4`. v0/v1
/// never carried Zstd on the wire (KIP-110 was v2-only).
pub fn compression_from_attrs(byte: i8) -> Result<CompressionType, LegacyRecordsError> {
    match byte & attrs::COMPRESSION_MASK {
        0 => Ok(CompressionType::None),
        1 => Ok(CompressionType::Gzip),
        2 => Ok(CompressionType::Snappy),
        3 => Ok(CompressionType::Lz4),
        other => Err(LegacyRecordsError::Malformed(format!(
            "legacy compression code {other} not supported (v0/v1 carries 0..=3)"
        ))),
    }
}

#[must_use]
pub fn attrs_with_compression(byte: i8, codec: CompressionType) -> i8 {
    let code: i8 = match codec {
        CompressionType::None => 0,
        CompressionType::Gzip => 1,
        CompressionType::Snappy => 2,
        CompressionType::Lz4 => 3,
        CompressionType::Zstd => panic!("legacy v0/v1 cannot carry zstd"),
        _ => panic!("unrecognised compression codec {codec:?} for v0/v1"),
    };
    (byte & !attrs::COMPRESSION_MASK) | code
}

/// Owned, decoded legacy message (post-frame parse).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub magic: Magic,
    pub attributes: i8,
    /// `Some` iff `magic == V1`.
    pub timestamp: Option<i64>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
}

impl Message {
    /// Bytes the message occupies inside the per-entry frame (i.e. starting
    /// at the CRC field, up to and including the value).
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        // crc(4) + magic(1) + attrs(1) [+ ts(8) if v1] + key + value
        let mut n = 4 + 1 + 1;
        if matches!(self.magic, Magic::V1) {
            n += 8;
        }
        n += nullable_bytes_len(self.key.as_ref());
        n += nullable_bytes_len(self.value.as_ref());
        n
    }

    /// Encode the message into `buf`, including the CRC field. `buf` is
    /// extended; nothing is written before the CRC.
    pub fn encode_into<B: BufMut>(&self, buf: &mut B) {
        // Build the CRC-covered payload first, then prefix it with the CRC.
        let body_len = self.encoded_len() - 4;
        let mut body = Vec::with_capacity(body_len);
        body.push(self.magic.as_i8() as u8);
        body.push(self.attributes as u8);
        if matches!(self.magic, Magic::V1) {
            let ts = self.timestamp.unwrap_or(-1);
            body.extend_from_slice(&ts.to_be_bytes());
        }
        put_nullable_bytes(&mut body, self.key.as_ref());
        put_nullable_bytes(&mut body, self.value.as_ref());

        let crc = crc32fast::hash(&body);
        buf.put_u32(crc);
        buf.put_slice(&body);
    }

    /// Decode a message from `buf`. `buf` must be positioned at the CRC and
    /// must contain at least `frame_size` bytes; `frame_size` is the
    /// `message_size` from the outer MessageSet frame.
    pub fn decode_from<B: Buf>(buf: &mut B, frame_size: usize) -> Result<Self, LegacyRecordsError> {
        if buf.remaining() < frame_size {
            return Err(LegacyRecordsError::Truncated {
                needed: frame_size - buf.remaining(),
            });
        }
        if frame_size < 6 {
            return Err(LegacyRecordsError::Malformed(format!(
                "message frame {frame_size} bytes < 6 minimum"
            )));
        }
        let mut frame = vec![0u8; frame_size];
        buf.copy_to_slice(&mut frame);

        let expected_crc = u32::from_be_bytes(frame[0..4].try_into().unwrap());
        let computed = crc32fast::hash(&frame[4..]);
        if expected_crc != computed {
            return Err(LegacyRecordsError::CrcMismatch {
                expected: expected_crc,
                computed,
            });
        }

        let mut cur = &frame[4..];
        let magic = Magic::from_i8(cur.get_i8())?;
        let attributes = cur.get_i8();
        let timestamp = match magic {
            Magic::V0 => None,
            Magic::V1 => {
                if cur.remaining() < 8 {
                    return Err(LegacyRecordsError::Truncated {
                        needed: 8 - cur.remaining(),
                    });
                }
                Some(cur.get_i64())
            }
        };
        let key = get_nullable_bytes(&mut cur, "key")?;
        let value = get_nullable_bytes(&mut cur, "value")?;
        if !cur.is_empty() {
            return Err(LegacyRecordsError::Malformed(format!(
                "trailing {} byte(s) inside message frame",
                cur.len()
            )));
        }
        Ok(Self {
            magic,
            attributes,
            timestamp,
            key,
            value,
        })
    }

    #[must_use]
    pub fn compression(&self) -> CompressionType {
        compression_from_attrs(self.attributes).unwrap_or(CompressionType::None)
    }
}

fn nullable_bytes_len(b: Option<&Bytes>) -> usize {
    4 + b.map_or(0, Bytes::len)
}

fn put_nullable_bytes(buf: &mut Vec<u8>, b: Option<&Bytes>) {
    match b {
        None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
        Some(data) => {
            let len = i32::try_from(data.len()).unwrap_or(i32::MAX);
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(data);
        }
    }
}

fn get_nullable_bytes(
    buf: &mut &[u8],
    label: &'static str,
) -> Result<Option<Bytes>, LegacyRecordsError> {
    if buf.remaining() < 4 {
        return Err(LegacyRecordsError::Truncated {
            needed: 4 - buf.remaining(),
        });
    }
    let len = buf.get_i32();
    if len < 0 {
        if len == -1 {
            return Ok(None);
        }
        return Err(LegacyRecordsError::NegativeLength { label, len });
    }
    let n = len as usize;
    if buf.remaining() < n {
        return Err(LegacyRecordsError::Truncated {
            needed: n - buf.remaining(),
        });
    }
    let data = Bytes::copy_from_slice(&buf[..n]);
    buf.advance(n);
    Ok(Some(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use bytes::BytesMut;

    fn fixture_v0() -> Message {
        Message {
            magic: Magic::V0,
            attributes: 0,
            timestamp: None,
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"v")),
        }
    }

    fn fixture_v1() -> Message {
        Message {
            magic: Magic::V1,
            attributes: 0,
            timestamp: Some(1_700_000_000),
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"value")),
        }
    }

    fn fixture_v1_null() -> Message {
        Message {
            magic: Magic::V1,
            attributes: 0,
            timestamp: Some(42),
            key: None,
            value: None,
        }
    }

    #[test]
    fn v0_roundtrip() {
        let m = fixture_v0();
        let mut buf = BytesMut::new();
        m.encode_into(&mut buf);
        assert!(buf.len() == m.encoded_len());
        let mut cur: &[u8] = &buf[..];
        let decoded = Message::decode_from(&mut cur, m.encoded_len()).unwrap();
        assert!(decoded == m);
    }

    #[test]
    fn v1_roundtrip() {
        let m = fixture_v1();
        let mut buf = BytesMut::new();
        m.encode_into(&mut buf);
        assert!(buf.len() == m.encoded_len());
        let mut cur: &[u8] = &buf[..];
        let decoded = Message::decode_from(&mut cur, m.encoded_len()).unwrap();
        assert!(decoded == m);
    }

    #[test]
    fn v1_null_key_and_value() {
        let m = fixture_v1_null();
        let mut buf = BytesMut::new();
        m.encode_into(&mut buf);
        let mut cur: &[u8] = &buf[..];
        let decoded = Message::decode_from(&mut cur, m.encoded_len()).unwrap();
        assert!(decoded == m);
        assert!(decoded.key.is_none());
        assert!(decoded.value.is_none());
    }

    #[test]
    fn rejects_bad_crc() {
        let m = fixture_v1();
        let mut buf = BytesMut::new();
        m.encode_into(&mut buf);
        buf[0] ^= 0xFF;
        let mut cur: &[u8] = &buf[..];
        assert!(matches!(
            Message::decode_from(&mut cur, m.encoded_len()),
            Err(LegacyRecordsError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn rejects_unsupported_magic() {
        let mut buf = BytesMut::new();
        // Build a fake message with magic = 2.
        let mut body = vec![2u8, 0u8]; // magic, attrs
        body.extend_from_slice(&(-1i32).to_be_bytes()); // key=null
        body.extend_from_slice(&(-1i32).to_be_bytes()); // value=null
        let crc = crc32fast::hash(&body);
        buf.extend_from_slice(&crc.to_be_bytes());
        buf.extend_from_slice(&body);
        let frame_size = buf.len();
        let mut cur: &[u8] = &buf[..];
        assert!(matches!(
            Message::decode_from(&mut cur, frame_size),
            Err(LegacyRecordsError::UnsupportedMagic { found: 2 })
        ));
    }

    #[test]
    fn attrs_codec_roundtrip() {
        for c in [
            CompressionType::None,
            CompressionType::Gzip,
            CompressionType::Snappy,
            CompressionType::Lz4,
        ] {
            let bits = attrs_with_compression(0, c);
            assert!(compression_from_attrs(bits).unwrap() == c);
        }
    }
}
