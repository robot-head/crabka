//! Wire-level helpers for `[]<elem>` and nullable `[]<elem>` arrays.
//!
//! Non-flexible arrays use a 4-byte big-endian `INT32` length prefix (−1 for
//! null).  Flexible (compact) arrays use an unsigned varint whose value is
//! `len + 1` (0 means null).

use bytes::{Buf, BufMut};

use crate::ProtocolError;
use crate::primitives::fixed::{get_i32, put_i32};
use crate::primitives::varint::{get_uvarint, put_uvarint, uvarint_len};

/// Write a non-nullable array-length prefix.
pub fn put_array_len<B: BufMut>(buf: &mut B, n: usize, flexible: bool) {
    if flexible {
        put_uvarint(buf, u32::try_from(n + 1).expect("array too large"));
    } else {
        put_i32(buf, i32::try_from(n).expect("array too large"));
    }
}

/// Write a nullable array-length prefix.  `None` encodes as −1 (non-flex) or
/// 0 (flex).
pub fn put_nullable_array_len<B: BufMut>(buf: &mut B, len: Option<usize>, flexible: bool) {
    match (flexible, len) {
        (false, None) => put_i32(buf, -1),
        (false, Some(n)) => put_i32(buf, i32::try_from(n).expect("array too large")),
        (true, None) => put_uvarint(buf, 0),
        (true, Some(n)) => put_uvarint(buf, u32::try_from(n + 1).expect("array too large")),
    }
}

/// Number of bytes consumed by a non-nullable array-length prefix.
#[must_use]
pub fn array_len_prefix_len(n: usize, flexible: bool) -> usize {
    if flexible {
        uvarint_len(u32::try_from(n + 1).unwrap())
    } else {
        4
    }
}

/// Number of bytes consumed by a nullable array-length prefix.
#[must_use]
pub fn nullable_array_len_prefix_len(len: Option<usize>, flexible: bool) -> usize {
    match (flexible, len) {
        (false, _) => 4,
        (true, None) => uvarint_len(0),
        (true, Some(n)) => uvarint_len(u32::try_from(n + 1).unwrap()),
    }
}

/// Read a non-nullable array length.  Returns an error if the encoded value is
/// null (−1 / 0).
pub fn get_array_len<B: Buf>(buf: &mut B, flexible: bool) -> Result<usize, ProtocolError> {
    if flexible {
        let raw = get_uvarint(buf)?;
        if raw == 0 {
            return Err(ProtocolError::InvalidValue(
                "non-nullable array was null (compact encoding)",
            ));
        }
        Ok((raw - 1) as usize)
    } else {
        let n = get_i32(buf)?;
        if n < 0 {
            return Err(ProtocolError::InvalidValue(
                "non-nullable array had negative length",
            ));
        }
        Ok(usize::try_from(n).expect("n is non-negative"))
    }
}

/// Read a nullable array length.  Returns `None` when the encoded value is
/// null (−1 non-flex, 0 flex).
pub fn get_nullable_array_len<B: Buf>(
    buf: &mut B,
    flexible: bool,
) -> Result<Option<usize>, ProtocolError> {
    if flexible {
        let raw = get_uvarint(buf)?;
        if raw == 0 {
            Ok(None)
        } else {
            Ok(Some((raw - 1) as usize))
        }
    } else {
        let n = get_i32(buf)?;
        if n < 0 {
            Ok(None)
        } else {
            Ok(Some(usize::try_from(n).expect("n is non-negative")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use bytes::BytesMut;

    // --- non-nullable, non-flexible -----------------------------------------

    #[test]
    fn non_flex_empty_array_roundtrip() {
        let mut buf = BytesMut::new();
        put_array_len(&mut buf, 0, false);
        assert!(buf.len() == 4, "prefix must be 4 bytes");
        let mut cur = &buf[..];
        assert!(get_array_len(&mut cur, false).unwrap() == 0);
        assert!(cur.is_empty());
    }

    #[test]
    fn non_flex_three_element_array_roundtrip() {
        let mut buf = BytesMut::new();
        put_array_len(&mut buf, 3, false);
        assert!(buf.len() == 4);
        let mut cur = &buf[..];
        assert!(get_array_len(&mut cur, false).unwrap() == 3);
        assert!(cur.is_empty());
    }

    // --- non-nullable, flexible (compact) -----------------------------------

    #[test]
    fn flex_empty_array_roundtrip() {
        let mut buf = BytesMut::new();
        put_array_len(&mut buf, 0, true);
        // len=0 → encode 1 → single byte 0x01
        assert!(&buf[..] == &[0x01]);
        let mut cur = &buf[..];
        assert!(get_array_len(&mut cur, true).unwrap() == 0);
        assert!(cur.is_empty());
    }

    #[test]
    fn flex_three_element_array_roundtrip() {
        let mut buf = BytesMut::new();
        put_array_len(&mut buf, 3, true);
        // len=3 → encode 4 → single byte 0x04
        assert!(&buf[..] == &[0x04]);
        let mut cur = &buf[..];
        assert!(get_array_len(&mut cur, true).unwrap() == 3);
        assert!(cur.is_empty());
    }

    // --- nullable, non-flexible ---------------------------------------------

    #[test]
    fn non_flex_nullable_null_roundtrip() {
        let mut buf = BytesMut::new();
        put_nullable_array_len(&mut buf, None, false);
        assert!(&buf[..] == &[0xFF, 0xFF, 0xFF, 0xFF]); // -1 in big-endian
        let mut cur = &buf[..];
        assert!(get_nullable_array_len(&mut cur, false).unwrap() == None);
        assert!(cur.is_empty());
    }

    #[test]
    fn non_flex_nullable_some_roundtrip() {
        let mut buf = BytesMut::new();
        put_nullable_array_len(&mut buf, Some(3), false);
        let mut cur = &buf[..];
        assert!(get_nullable_array_len(&mut cur, false).unwrap() == Some(3));
        assert!(cur.is_empty());
    }

    // --- nullable, flexible -------------------------------------------------

    #[test]
    fn flex_nullable_null_roundtrip() {
        let mut buf = BytesMut::new();
        put_nullable_array_len(&mut buf, None, true);
        assert!(&buf[..] == &[0x00]); // 0 = null in compact encoding
        let mut cur = &buf[..];
        assert!(get_nullable_array_len(&mut cur, true).unwrap() == None);
        assert!(cur.is_empty());
    }

    #[test]
    fn flex_nullable_some_roundtrip() {
        let mut buf = BytesMut::new();
        put_nullable_array_len(&mut buf, Some(3), true);
        // Some(3) → encode 4 → 0x04
        assert!(&buf[..] == &[0x04]);
        let mut cur = &buf[..];
        assert!(get_nullable_array_len(&mut cur, true).unwrap() == Some(3));
        assert!(cur.is_empty());
    }

    // --- prefix_len helpers -------------------------------------------------

    #[test]
    fn array_len_prefix_len_non_flex() {
        assert!(array_len_prefix_len(0, false) == 4);
        assert!(array_len_prefix_len(100, false) == 4);
    }

    #[test]
    fn array_len_prefix_len_flex() {
        // len=0 → varint(1) = 1 byte; len=126 → varint(127) = 1 byte;
        // len=127 → varint(128) = 2 bytes.
        assert!(array_len_prefix_len(0, true) == 1);
        assert!(array_len_prefix_len(126, true) == 1);
        assert!(array_len_prefix_len(127, true) == 2);
    }

    #[test]
    fn nullable_prefix_len_non_flex_always_4() {
        assert!(nullable_array_len_prefix_len(None, false) == 4);
        assert!(nullable_array_len_prefix_len(Some(3), false) == 4);
    }

    #[test]
    fn nullable_prefix_len_flex_null_is_1() {
        // null → varint(0) = 1 byte
        assert!(nullable_array_len_prefix_len(None, true) == 1);
    }

    // --- error cases --------------------------------------------------------

    #[test]
    fn non_nullable_rejects_null_non_flex() {
        let bytes = (-1i32).to_be_bytes();
        let mut cur = &bytes[..];
        assert!(matches!(
            get_array_len(&mut cur, false),
            Err(ProtocolError::InvalidValue(_))
        ));
    }

    #[test]
    fn non_nullable_rejects_null_flex() {
        // varint 0 = null in compact encoding
        let bytes = [0x00u8];
        let mut cur = &bytes[..];
        assert!(matches!(
            get_array_len(&mut cur, true),
            Err(ProtocolError::InvalidValue(_))
        ));
    }
}
