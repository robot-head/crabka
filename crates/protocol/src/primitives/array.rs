//! Wire-level helpers for `[]<elem>` and nullable `[]<elem>` arrays.
//!
//! Non-flexible arrays use a 4-byte big-endian `INT32` length prefix (−1 for
//! null).  Flexible (compact) arrays use an unsigned varint whose value is
//! `len + 1` (0 means null).

use bytes::{Buf, BufMut};

use crate::{
    ProtocolError,
    primitives::{
        fixed::{get_i32, put_i32},
        varint::{get_uvarint, put_uvarint, uvarint_len},
    },
};

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
    let n = if flexible {
        let raw = get_uvarint(buf)?;
        if raw == 0 {
            return Err(ProtocolError::InvalidValue(
                "non-nullable array was null (compact encoding)",
            ));
        }
        (raw - 1) as usize
    } else {
        let n = get_i32(buf)?;
        if n < 0 {
            return Err(ProtocolError::InvalidValue(
                "non-nullable array had negative length",
            ));
        }
        usize::try_from(n).expect("n is non-negative")
    };
    // Every array element occupies at least one byte on the wire, so a
    // legitimate array of `n` elements always has at least `n` bytes left in
    // the buffer. Any larger `n` is impossible and indicates a malformed or
    // hostile frame; reject before a caller can `Vec::with_capacity(n)`.
    if n > buf.remaining() {
        return Err(ProtocolError::InvalidValue(
            "array length exceeds remaining buffer",
        ));
    }
    Ok(n)
}

/// Read a nullable array length.  Returns `None` when the encoded value is
/// null (−1 non-flex, 0 flex).
pub fn get_nullable_array_len<B: Buf>(
    buf: &mut B,
    flexible: bool,
) -> Result<Option<usize>, ProtocolError> {
    let n = if flexible {
        let raw = get_uvarint(buf)?;
        if raw == 0 {
            return Ok(None);
        }
        (raw - 1) as usize
    } else {
        let n = get_i32(buf)?;
        if n < 0 {
            return Ok(None);
        }
        usize::try_from(n).expect("n is non-negative")
    };
    // See `get_array_len`: a length larger than the remaining bytes cannot
    // describe a real array and must be rejected before pre-allocation.
    if n > buf.remaining() {
        return Err(ProtocolError::InvalidValue(
            "array length exceeds remaining buffer",
        ));
    }
    Ok(Some(n))
}

#[cfg(test)]
mod tests {

    use bytes::BytesMut;

    use super::*;

    #[test]
    fn array_roundtrip_cases() {
        for (_case, len, flexible, expected_prefix) in [
            ("non-flex empty", 0, false, &[0, 0, 0, 0][..]),
            ("non-flex three", 3, false, &[0, 0, 0, 3][..]),
            ("flex empty", 0, true, &[0x01][..]),
            ("flex three", 3, true, &[0x04][..]),
        ] {
            let mut buf = BytesMut::new();
            put_array_len(&mut buf, len, flexible);
            assert2::assert!(&buf[..] == expected_prefix);
            buf.extend_from_slice(&vec![0; len]);
            let mut cur = &buf[..];
            assert2::assert!((get_array_len(&mut cur, flexible).unwrap(), cur.len()) == (len, len));
        }
    }

    #[test]
    fn nullable_array_roundtrip_cases() {
        for (_case, len, flexible, expected_prefix) in [
            ("non-flex null", None, false, &[0xFF, 0xFF, 0xFF, 0xFF][..]),
            ("non-flex some", Some(3), false, &[0, 0, 0, 3][..]),
            ("flex null", None, true, &[0x00][..]),
            ("flex some", Some(3), true, &[0x04][..]),
        ] {
            let mut buf = BytesMut::new();
            put_nullable_array_len(&mut buf, len, flexible);
            assert2::assert!(&buf[..] == expected_prefix);
            let payload_len = len.unwrap_or(0);
            buf.extend_from_slice(&vec![0; payload_len]);
            let mut cur = &buf[..];
            assert2::assert!(
                (
                    get_nullable_array_len(&mut cur, flexible).unwrap(),
                    cur.len()
                ) == (len, payload_len)
            );
        }
    }

    // --- prefix_len helpers -------------------------------------------------

    #[test]
    fn array_len_prefix_len_non_flex() {
        for (_case, len) in [("empty", 0), ("populated", 100)] {
            assert2::assert!(array_len_prefix_len(len, false) == 4);
        }
    }

    #[test]
    fn array_len_prefix_len_flex() {
        // len=0 → varint(1) = 1 byte; len=126 → varint(127) = 1 byte;
        // len=127 → varint(128) = 2 bytes.
        for (len, want) in [(0, 1), (126, 1), (127, 2)] {
            assert2::assert!(array_len_prefix_len(len, true) == want);
        }
    }

    #[test]
    fn nullable_prefix_len_non_flex_always_4() {
        for (_case, len) in [("null", None), ("some", Some(3))] {
            assert2::assert!(nullable_array_len_prefix_len(len, false) == 4);
        }
    }

    #[test]
    fn nullable_prefix_len_flex_null_is_1() {
        // null → varint(0) = 1 byte
        assert2::assert!(nullable_array_len_prefix_len(None, true) == 1);
    }

    // --- error cases --------------------------------------------------------

    #[test]
    fn non_nullable_rejects_null_cases() {
        for (_case, bytes, flexible) in [
            ("non-flex", &(-1i32).to_be_bytes()[..], false),
            ("flex", &[0x00][..], true),
        ] {
            let mut cur = bytes;
            assert2::assert!(matches!(
                get_array_len(&mut cur, flexible),
                Err(ProtocolError::InvalidValue(_))
            ));
        }
    }

    // --- pre-allocation DoS bound (length > remaining buffer) ----------------

    #[test]
    fn rejects_length_exceeding_remaining_cases() {
        for (_case, nullable, flexible) in [
            ("non-nullable non-flex", false, false),
            ("non-nullable flex", false, true),
            ("nullable non-flex", true, false),
            ("nullable flex", true, true),
        ] {
            let mut buf = BytesMut::new();
            if nullable {
                put_nullable_array_len(&mut buf, Some(2_000_000_000), flexible);
            } else {
                put_array_len(&mut buf, 2_000_000_000, flexible);
            }
            let mut cur = &buf[..];
            let result = if nullable {
                get_nullable_array_len(&mut cur, flexible).map(|value| value.unwrap_or(0))
            } else {
                get_array_len(&mut cur, flexible)
            };
            assert2::assert!(matches!(
                result,
                Err(ProtocolError::InvalidValue(
                    "array length exceeds remaining buffer"
                ))
            ));
        }
    }

    #[test]
    fn length_equal_to_remaining_is_accepted() {
        for (_case, flexible) in [("non-flex", false), ("flex", true)] {
            let mut buf = BytesMut::new();
            put_array_len(&mut buf, 5, flexible);
            buf.extend_from_slice(&[0u8; 5]);
            let mut cur = &buf[..];
            assert2::assert!(get_array_len(&mut cur, flexible).unwrap() == 5);
        }
    }
}
