//! Order-preserving encoders for key components.
//!
//! Unsigned big-endian fixed width is already order-preserving, and that is
//! all the local engine needs for table ids, index ids, and a monotonic
//! hidden rowid. Sortable encodings for arbitrary PRIMARY KEY column types
//! are deferred. The key layout reserves the slot, so a later addition is
//! additive.

use zerocopy::{
    FromBytes, IntoBytes,
    byteorder::big_endian::{U32, U64},
};

use crate::KvError;

pub fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(U32::new(v).as_bytes());
}

pub fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(U64::new(v).as_bytes());
}

/// Append a signed 64-bit integer in lexicographic numeric order.
pub fn put_i64(out: &mut Vec<u8>, v: i64) {
    put_u64(out, (v as u64) ^ (1_u64 << 63));
}

/// Append a signed 32-bit integer in lexicographic numeric order.
pub fn put_i32(out: &mut Vec<u8>, v: i32) {
    put_u32(out, (v as u32) ^ (1_u32 << 31));
}

/// Append bytes in lexicographic byte order, including embedded NULs.
///
/// A NUL byte is escaped as `00 ff`; `00 00` terminates the component.  Thus
/// component boundaries do not disturb the order of a prefix and its extension.
pub fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    for byte in bytes {
        out.push(*byte);
        if *byte == 0 {
            out.push(u8::MAX);
        }
    }
    out.extend_from_slice(&[0, 0]);
}

/// Decodes and consumes one big-endian `u32` key component.
///
/// # Errors
///
/// Returns [`KvError::CorruptRow`] when fewer than four bytes remain.
pub fn take_u32(cur: &mut &[u8]) -> Result<u32, KvError> {
    let (v, rest) = U32::read_from_prefix(cur)
        .map_err(|_| KvError::CorruptRow("truncated u32 key component".into()))?;
    *cur = rest;
    Ok(v.get())
}

/// Decodes and consumes one big-endian `u64` key component.
///
/// # Errors
///
/// Returns [`KvError::CorruptRow`] when fewer than eight bytes remain.
pub fn take_u64(cur: &mut &[u8]) -> Result<u64, KvError> {
    let (v, rest) = U64::read_from_prefix(cur)
        .map_err(|_| KvError::CorruptRow("truncated u64 key component".into()))?;
    *cur = rest;
    Ok(v.get())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn roundtrip_u32_u64() {
        let mut b = Vec::new();
        put_u32(&mut b, 0x0102_0304);
        put_u64(&mut b, 0x0102_0304_0506_0708);
        let mut cur = &b[..];
        assert_eq!(take_u32(&mut cur).expect("u32"), 0x0102_0304);
        assert_eq!(take_u64(&mut cur).expect("u64"), 0x0102_0304_0506_0708);
        assert!(cur.is_empty());
    }

    #[test]
    fn truncated_take_errors_not_panics() {
        let mut cur = &[0u8, 1][..];
        assert!(take_u32(&mut cur).is_err());
    }

    #[test]
    fn order_preservation_boundaries() {
        let enc = |v: u64| {
            let mut b = Vec::new();
            put_u64(&mut b, v);
            b
        };
        assert!(enc(0) < enc(1));
        assert!(enc(u64::MAX - 1) < enc(u64::MAX));
        assert!(enc(0x00FF_FFFF_FFFF_FFFF) < enc(0x0100_0000_0000_0000));

        let enc32 = |v: u32| {
            let mut b = Vec::new();
            put_u32(&mut b, v);
            b
        };
        assert!(enc32(0) < enc32(1));
        assert!(enc32(u32::MAX - 1) < enc32(u32::MAX));
        assert!(enc32(0x00FF_FFFF) < enc32(0x0100_0000));
    }

    #[test]
    fn put_emits_big_endian_bytes() {
        let mut b = Vec::new();
        put_u32(&mut b, 0x0102_0304);
        assert_eq!(b, vec![0x01, 0x02, 0x03, 0x04]);

        let mut b = Vec::new();
        put_u64(&mut b, 0x0102_0304_0506_0708);
        assert_eq!(b, vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    #[test]
    fn signed_i64_encoding_preserves_numeric_order() {
        let enc = |v| {
            let mut bytes = Vec::new();
            put_i64(&mut bytes, v);
            bytes
        };
        assert!(enc(i64::MIN) < enc(-1));
        assert!(enc(-1) < enc(0));
        assert!(enc(0) < enc(i64::MAX));
    }

    #[test]
    fn signed_i32_encoding_preserves_numeric_order() {
        let enc = |v| {
            let mut bytes = Vec::new();
            put_i32(&mut bytes, v);
            bytes
        };
        assert!(enc(i32::MIN) < enc(-1));
        assert!(enc(-1) < enc(0));
        assert!(enc(0) < enc(i32::MAX));
    }

    #[test]
    fn byte_encoding_preserves_lexicographic_order_and_component_boundaries() {
        let enc = |bytes: &[u8]| {
            let mut out = Vec::new();
            put_bytes(&mut out, bytes);
            out
        };
        for pair in [
            (&b""[..], &b"\0"[..]),
            (&b"\0"[..], &b"\0\0"[..]),
            (&b"a"[..], &b"a\0"[..]),
            (&b"a\0"[..], &b"b"[..]),
            (&b"\xff"[..], &b"\xff\0"[..]),
        ] {
            assert!(pair.0 < pair.1);
            assert!(enc(pair.0) < enc(pair.1));
        }
    }

    proptest! {
        #[test]
        fn u64_encoding_is_order_preserving(a: u64, b: u64) {
            let (mut ea, mut eb) = (Vec::new(), Vec::new());
            put_u64(&mut ea, a);
            put_u64(&mut eb, b);
            prop_assert_eq!(a.cmp(&b), ea.cmp(&eb));
        }

        #[test]
        fn u32_encoding_is_order_preserving(a: u32, b: u32) {
            let (mut ea, mut eb) = (Vec::new(), Vec::new());
            put_u32(&mut ea, a);
            put_u32(&mut eb, b);
            prop_assert_eq!(a.cmp(&b), ea.cmp(&eb));
        }
    }
}
