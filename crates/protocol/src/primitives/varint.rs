use bytes::{Buf, BufMut};

use crate::ProtocolError;

const MAX_VARINT_BYTES: usize = 5; // 32-bit
const MAX_VARLONG_BYTES: usize = 10; // 64-bit

pub fn put_uvarint<B: BufMut>(buf: &mut B, mut v: u32) {
    while (v & !0x7F) != 0 {
        buf.put_u8((v & 0x7F).to_le_bytes()[0] | 0x80);
        v >>= 7;
    }
    buf.put_u8(v.to_le_bytes()[0]);
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_uvarint<B: Buf>(buf: &mut B) -> Result<u32, ProtocolError> {
    let mut result: u32 = 0;
    let mut shift = 0;
    for _ in 0..MAX_VARINT_BYTES {
        if buf.remaining() == 0 {
            return Err(ProtocolError::UnexpectedEof { needed: 1 });
        }
        let b = buf.get_u8();
        result |= u32::from(b & 0x7F) << shift;
        if (b & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(ProtocolError::VarintTooLong {
        max: MAX_VARINT_BYTES,
    })
}

#[must_use]
pub fn uvarint_len(v: u32) -> usize {
    if v == 0 {
        return 1;
    }
    let bits = usize::from((u32::BITS - v.leading_zeros()).to_le_bytes()[0]);
    bits.div_ceil(7)
}

pub fn put_varint<B: BufMut>(buf: &mut B, v: i32) {
    let zz = ((v << 1) ^ (v >> 31)).cast_unsigned();
    put_uvarint(buf, zz);
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_varint<B: Buf>(buf: &mut B) -> Result<i32, ProtocolError> {
    let zz = get_uvarint(buf)?;
    Ok((zz >> 1).cast_signed() ^ -(zz & 1).cast_signed())
}

#[must_use]
pub fn varint_len(v: i32) -> usize {
    let zz = ((v << 1) ^ (v >> 31)).cast_unsigned();
    uvarint_len(zz)
}

pub fn put_uvarlong<B: BufMut>(buf: &mut B, mut v: u64) {
    while (v & !0x7F) != 0 {
        buf.put_u8((v & 0x7F).to_le_bytes()[0] | 0x80);
        v >>= 7;
    }
    buf.put_u8(v.to_le_bytes()[0]);
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_uvarlong<B: Buf>(buf: &mut B) -> Result<u64, ProtocolError> {
    let mut result: u64 = 0;
    let mut shift = 0;
    for _ in 0..MAX_VARLONG_BYTES {
        if buf.remaining() == 0 {
            return Err(ProtocolError::UnexpectedEof { needed: 1 });
        }
        let b = buf.get_u8();
        result |= u64::from(b & 0x7F) << shift;
        if (b & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(ProtocolError::VarintTooLong {
        max: MAX_VARLONG_BYTES,
    })
}

pub fn put_varlong<B: BufMut>(buf: &mut B, v: i64) {
    let zz = ((v << 1) ^ (v >> 63)).cast_unsigned();
    put_uvarlong(buf, zz);
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_varlong<B: Buf>(buf: &mut B) -> Result<i64, ProtocolError> {
    let zz = get_uvarlong(buf)?;
    Ok((zz >> 1).cast_signed() ^ -(zz & 1).cast_signed())
}

#[must_use]
pub fn uvarlong_len(v: u64) -> usize {
    if v == 0 {
        return 1;
    }
    let bits = usize::from((u64::BITS - v.leading_zeros()).to_le_bytes()[0]);
    bits.div_ceil(7)
}

#[must_use]
pub fn varlong_len(v: i64) -> usize {
    let zz = ((v << 1) ^ (v >> 63)).cast_unsigned();
    uvarlong_len(zz)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn uvarint_known_vectors() {
        // (value, expected bytes) — pulled from KIP-482 / protobuf reference.
        let cases: &[(u32, &[u8])] = &[
            (0, &[0x00]),
            (1, &[0x01]),
            (127, &[0x7F]),
            (128, &[0x80, 0x01]),
            (16_383, &[0xFF, 0x7F]),
            (16_384, &[0x80, 0x80, 0x01]),
            (u32::MAX, &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]),
        ];
        for (v, expected) in cases {
            let mut buf = BytesMut::new();
            put_uvarint(&mut buf, *v);
            assert2::assert!(&buf[..] == *expected);
            let mut cur = *expected;
            check!(get_uvarint(&mut cur).unwrap() == *v);
            check!(cur.is_empty());
            check!(uvarint_len(*v) == expected.len());
        }
    }

    #[test]
    fn varint_zigzag_sample() {
        // (value, expected bytes) — protobuf zig-zag examples.
        let cases: &[(i32, &[u8])] = &[
            (0, &[0x00]),
            (-1, &[0x01]),
            (1, &[0x02]),
            (-2, &[0x03]),
            (2, &[0x04]),
            (i32::MAX, &[0xFE, 0xFF, 0xFF, 0xFF, 0x0F]),
            (i32::MIN, &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]),
        ];
        for (v, expected) in cases {
            let mut buf = BytesMut::new();
            put_varint(&mut buf, *v);
            assert2::assert!(&buf[..] == *expected);
            let mut cur = *expected;
            assert2::assert!(get_varint(&mut cur).unwrap() == *v);
            assert2::assert!(varint_len(*v) == expected.len());
        }
    }

    #[test]
    fn uvarint_rejects_overlong() {
        let too_long = [0x80u8, 0x80, 0x80, 0x80, 0x80, 0x01];
        let mut cur = &too_long[..];
        assert2::assert!(matches!(
            get_uvarint(&mut cur),
            Err(ProtocolError::VarintTooLong { .. })
        ));
    }

    #[test]
    fn uvarint_eof() {
        let truncated = [0x80u8];
        let mut cur = &truncated[..];
        assert2::assert!(matches!(
            get_uvarint(&mut cur),
            Err(ProtocolError::UnexpectedEof { .. })
        ));
    }

    #[test]
    fn uvarlong_len_known_values() {
        // (value, expected byte length on wire)
        let cases: &[(u64, usize)] = &[
            (0, 1),
            (1, 1),
            (127, 1),
            (128, 2),
            (u64::from(u32::MAX), 5),
            (u64::MAX, 10),
        ];
        for (v, expected) in cases {
            assert2::assert!(uvarlong_len(*v) == *expected);
        }
    }

    #[test]
    fn varlong_len_known_values() {
        // Zigzag: 0->0, -1->1, 1->2, i64::MIN has max bits
        let cases: &[(i64, usize)] = &[
            (0, 1),
            (-1, 1),
            (1, 1),
            (63, 1),
            (64, 2),
            (-64, 1),
            (-65, 2),
            (i64::MAX, 10),
            (i64::MIN, 10),
        ];
        for (v, expected) in cases {
            assert2::assert!(varlong_len(*v) == *expected);
        }
    }
}
