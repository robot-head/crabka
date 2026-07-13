use bytes::{Buf, BufMut, Bytes};

use crate::{
    ProtocolError,
    primitives::{
        fixed::{get_i16, get_i32, put_i16, put_i32},
        varint::{get_uvarint, put_uvarint, uvarint_len},
    },
};

// ---- STRING (non-flexible) ----
// Wire: INT16 length (>=0), then `length` bytes UTF-8. -1 = null.

/// # Panics
/// Panics if a value previously validated by the protocol type no longer satisfies its encoded-length or field-range invariant.
pub fn put_string<B: BufMut>(buf: &mut B, s: &str) {
    let len = i16::try_from(s.len()).expect("string length must fit in i16");
    put_i16(buf, len);
    buf.put_slice(s.as_bytes());
}

pub fn put_nullable_string<B: BufMut>(buf: &mut B, s: Option<&str>) {
    match s {
        None => put_i16(buf, -1),
        Some(s) => put_string(buf, s),
    }
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_string_owned<B: Buf>(buf: &mut B) -> Result<String, ProtocolError> {
    match get_nullable_string_owned(buf)? {
        Some(s) => Ok(s),
        None => Err(ProtocolError::InvalidValue("non-nullable STRING was null")),
    }
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
/// # Panics
/// Panics if a value previously validated by the protocol type no longer satisfies its encoded-length or field-range invariant.
pub fn get_nullable_string_owned<B: Buf>(buf: &mut B) -> Result<Option<String>, ProtocolError> {
    let len = get_i16(buf)?;
    if len < 0 {
        return Ok(None);
    }
    let n = usize::try_from(len).expect("non-negative string length must fit in usize");
    if buf.remaining() < n {
        return Err(ProtocolError::UnexpectedEof {
            needed: n - buf.remaining(),
        });
    }
    let mut v = vec![0u8; n];
    buf.copy_to_slice(&mut v);
    let s = String::from_utf8(v).map_err(|e| ProtocolError::InvalidUtf8(e.utf8_error()))?;
    Ok(Some(s))
}

#[must_use]
pub fn string_len(s: &str) -> usize {
    2 + s.len()
}
#[must_use]
pub fn nullable_string_len(s: Option<&str>) -> usize {
    2 + s.map_or(0, str::len)
}

// ---- COMPACT_STRING (flexible) ----
// Wire: UVARINT length+1 (0 = null), then `length` UTF-8 bytes.

/// # Panics
/// Panics if a value previously validated by the protocol type no longer satisfies its encoded-length or field-range invariant.
pub fn put_compact_string<B: BufMut>(buf: &mut B, s: &str) {
    let len = u32::try_from(s.len() + 1).expect("string length too large");
    put_uvarint(buf, len);
    buf.put_slice(s.as_bytes());
}

pub fn put_compact_nullable_string<B: BufMut>(buf: &mut B, s: Option<&str>) {
    match s {
        None => put_uvarint(buf, 0),
        Some(s) => put_compact_string(buf, s),
    }
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_compact_string_owned<B: Buf>(buf: &mut B) -> Result<String, ProtocolError> {
    match get_compact_nullable_string_owned(buf)? {
        Some(s) => Ok(s),
        None => Err(ProtocolError::InvalidValue(
            "non-nullable COMPACT_STRING was null",
        )),
    }
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_compact_nullable_string_owned<B: Buf>(
    buf: &mut B,
) -> Result<Option<String>, ProtocolError> {
    let raw = get_uvarint(buf)?;
    if raw == 0 {
        return Ok(None);
    }
    let n = (raw - 1) as usize;
    if buf.remaining() < n {
        return Err(ProtocolError::UnexpectedEof {
            needed: n - buf.remaining(),
        });
    }
    let mut v = vec![0u8; n];
    buf.copy_to_slice(&mut v);
    let s = String::from_utf8(v).map_err(|e| ProtocolError::InvalidUtf8(e.utf8_error()))?;
    Ok(Some(s))
}

#[must_use]
/// # Panics
/// Panics if a value previously validated by the protocol type no longer satisfies its encoded-length or field-range invariant.
pub fn compact_string_len(s: &str) -> usize {
    uvarint_len(u32::try_from(s.len() + 1).unwrap()) + s.len()
}
#[must_use]
pub fn compact_nullable_string_len(s: Option<&str>) -> usize {
    match s {
        None => uvarint_len(0),
        Some(s) => compact_string_len(s),
    }
}

// ---- BYTES / COMPACT_BYTES ----
// BYTES: INT32 length, `length` bytes. -1 = null.
// COMPACT_BYTES: UVARINT length+1 (0=null), `length` bytes.

/// # Panics
/// Panics if a value previously validated by the protocol type no longer satisfies its encoded-length or field-range invariant.
pub fn put_bytes<B: BufMut>(buf: &mut B, b: &[u8]) {
    let len = i32::try_from(b.len()).expect("bytes length must fit in i32");
    put_i32(buf, len);
    buf.put_slice(b);
}

pub fn put_nullable_bytes<B: BufMut>(buf: &mut B, b: Option<&[u8]>) {
    match b {
        None => put_i32(buf, -1),
        Some(b) => put_bytes(buf, b),
    }
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_bytes_owned<B: Buf>(buf: &mut B) -> Result<Bytes, ProtocolError> {
    match get_nullable_bytes_owned(buf)? {
        Some(b) => Ok(b),
        None => Err(ProtocolError::InvalidValue("non-nullable BYTES was null")),
    }
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
/// # Panics
/// Panics if a value previously validated by the protocol type no longer satisfies its encoded-length or field-range invariant.
pub fn get_nullable_bytes_owned<B: Buf>(buf: &mut B) -> Result<Option<Bytes>, ProtocolError> {
    let len = get_i32(buf)?;
    if len < 0 {
        return Ok(None);
    }
    let n = usize::try_from(len).expect("non-negative bytes length must fit in usize");
    if buf.remaining() < n {
        return Err(ProtocolError::UnexpectedEof {
            needed: n - buf.remaining(),
        });
    }
    let mut v = vec![0u8; n];
    buf.copy_to_slice(&mut v);
    Ok(Some(Bytes::from(v)))
}

#[must_use]
pub fn bytes_len(b: &[u8]) -> usize {
    4 + b.len()
}
#[must_use]
pub fn nullable_bytes_len(b: Option<&[u8]>) -> usize {
    4 + b.map_or(0, <[u8]>::len)
}

/// # Panics
/// Panics if a value previously validated by the protocol type no longer satisfies its encoded-length or field-range invariant.
pub fn put_compact_bytes<B: BufMut>(buf: &mut B, b: &[u8]) {
    let len = u32::try_from(b.len() + 1).expect("bytes length too large");
    put_uvarint(buf, len);
    buf.put_slice(b);
}

pub fn put_compact_nullable_bytes<B: BufMut>(buf: &mut B, b: Option<&[u8]>) {
    match b {
        None => put_uvarint(buf, 0),
        Some(b) => put_compact_bytes(buf, b),
    }
}

#[must_use]
/// # Panics
/// Panics if a value previously validated by the protocol type no longer satisfies its encoded-length or field-range invariant.
pub fn compact_bytes_len(b: &[u8]) -> usize {
    uvarint_len(u32::try_from(b.len() + 1).unwrap()) + b.len()
}

/// Like `compact_bytes_len` but takes the byte-count directly rather than a slice.
/// Useful when the content size is known without materialising the buffer.
#[must_use]
/// # Panics
/// Panics if a value previously validated by the protocol type no longer satisfies its encoded-length or field-range invariant.
pub fn compact_bytes_len_from_size(n: usize) -> usize {
    uvarint_len(u32::try_from(n + 1).unwrap()) + n
}
#[must_use]
pub fn compact_nullable_bytes_len(b: Option<&[u8]>) -> usize {
    match b {
        None => uvarint_len(0),
        Some(b) => compact_bytes_len(b),
    }
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_compact_bytes_owned<B: Buf>(buf: &mut B) -> Result<Bytes, ProtocolError> {
    match get_compact_nullable_bytes_owned(buf)? {
        Some(b) => Ok(b),
        None => Err(ProtocolError::InvalidValue(
            "non-nullable COMPACT_BYTES was null",
        )),
    }
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_compact_nullable_bytes_owned<B: Buf>(
    buf: &mut B,
) -> Result<Option<Bytes>, ProtocolError> {
    let raw = get_uvarint(buf)?;
    if raw == 0 {
        return Ok(None);
    }
    let n = (raw - 1) as usize;
    if buf.remaining() < n {
        return Err(ProtocolError::UnexpectedEof {
            needed: n - buf.remaining(),
        });
    }
    let mut v = vec![0u8; n];
    buf.copy_to_slice(&mut v);
    Ok(Some(Bytes::from(v)))
}

#[cfg(test)]
mod tests {

    use bytes::BytesMut;

    use super::*;

    #[test]
    fn string_roundtrip() {
        let mut buf = BytesMut::new();
        put_string(&mut buf, "kafka");
        // INT16(5) + bytes
        assert2::assert!(&buf[..] == &[0x00, 0x05, b'k', b'a', b'f', b'k', b'a']);
        let mut cur = &buf[..];
        assert2::assert!(get_string_owned(&mut cur).unwrap() == "kafka");
    }

    #[test]
    fn nullable_string_null() {
        let mut buf = BytesMut::new();
        put_nullable_string(&mut buf, None);
        assert2::assert!(&buf[..] == &[0xFF, 0xFF]);
        let mut cur = &buf[..];
        assert2::assert!(get_nullable_string_owned(&mut cur).unwrap() == None);
    }

    #[test]
    fn compact_string_roundtrip() {
        let mut buf = BytesMut::new();
        put_compact_string(&mut buf, "kafka");
        // UVARINT(6) + bytes
        assert2::assert!(&buf[..] == &[0x06, b'k', b'a', b'f', b'k', b'a']);
        let mut cur = &buf[..];
        assert2::assert!(get_compact_string_owned(&mut cur).unwrap() == "kafka");
    }

    #[test]
    fn compact_nullable_string_null() {
        let mut buf = BytesMut::new();
        put_compact_nullable_string(&mut buf, None);
        assert2::assert!(&buf[..] == &[0x00]);
        let mut cur = &buf[..];
        assert2::assert!(get_compact_nullable_string_owned(&mut cur).unwrap() == None);
    }

    #[test]
    fn empty_compact_string() {
        let mut buf = BytesMut::new();
        put_compact_string(&mut buf, "");
        assert2::assert!(&buf[..] == &[0x01]); // length = 1 means "0 bytes"
        let mut cur = &buf[..];
        assert2::assert!(get_compact_string_owned(&mut cur).unwrap() == "");
    }

    #[test]
    fn bytes_roundtrip() {
        let mut buf = BytesMut::new();
        put_bytes(&mut buf, &[1, 2, 3]);
        let mut cur = &buf[..];
        let out = get_bytes_owned(&mut cur).unwrap();
        assert2::assert!(out.as_ref() == &[1, 2, 3]);
    }

    #[test]
    fn invalid_utf8_is_rejected() {
        // INT16(2) + invalid UTF-8 byte sequence
        let bytes = [0x00, 0x02, 0xC3, 0x28];
        let mut cur = &bytes[..];
        assert2::assert!(matches!(
            get_string_owned(&mut cur),
            Err(ProtocolError::InvalidUtf8(_))
        ));
    }
}
