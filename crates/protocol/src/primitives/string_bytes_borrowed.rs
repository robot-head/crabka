use crate::{
    ProtocolError,
    primitives::{
        fixed::{get_i16, get_i32},
        varint::get_uvarint,
    },
};

/// Decode a `STRING` (non-flexible) borrowing from the input buffer.
/// Wire: INT16 length (≥0), then `length` UTF-8 bytes. Length −1 = null (error here).
pub fn get_string_borrowed<'de>(buf: &mut &'de [u8]) -> Result<&'de str, ProtocolError> {
    let len = get_i16(buf)?;
    if len < 0 {
        return Err(ProtocolError::InvalidValue("non-nullable STRING was null"));
    }
    #[allow(clippy::cast_sign_loss)]
    let n = len as usize;
    if buf.len() < n {
        return Err(ProtocolError::UnexpectedEof {
            needed: n - buf.len(),
        });
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    std::str::from_utf8(head).map_err(ProtocolError::InvalidUtf8)
}

/// Decode a nullable `STRING` (non-flexible) borrowing from the input buffer.
pub fn get_nullable_string_borrowed<'de>(
    buf: &mut &'de [u8],
) -> Result<Option<&'de str>, ProtocolError> {
    let len = get_i16(buf)?;
    if len < 0 {
        return Ok(None);
    }
    #[allow(clippy::cast_sign_loss)]
    let n = len as usize;
    if buf.len() < n {
        return Err(ProtocolError::UnexpectedEof {
            needed: n - buf.len(),
        });
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    Ok(Some(
        std::str::from_utf8(head).map_err(ProtocolError::InvalidUtf8)?,
    ))
}

/// Decode a `COMPACT_STRING` borrowing from the input buffer.
/// Requires a contiguous buffer (i.e. `&[u8]`).
pub fn get_compact_string_borrowed<'de>(buf: &mut &'de [u8]) -> Result<&'de str, ProtocolError> {
    let raw = get_uvarint(buf)?;
    if raw == 0 {
        return Err(ProtocolError::InvalidValue(
            "non-nullable COMPACT_STRING was null",
        ));
    }
    let n = (raw - 1) as usize;
    if buf.len() < n {
        return Err(ProtocolError::UnexpectedEof {
            needed: n - buf.len(),
        });
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    std::str::from_utf8(head).map_err(ProtocolError::InvalidUtf8)
}

pub fn get_compact_nullable_string_borrowed<'de>(
    buf: &mut &'de [u8],
) -> Result<Option<&'de str>, ProtocolError> {
    let raw = get_uvarint(buf)?;
    if raw == 0 {
        return Ok(None);
    }
    let n = (raw - 1) as usize;
    if buf.len() < n {
        return Err(ProtocolError::UnexpectedEof {
            needed: n - buf.len(),
        });
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    Ok(Some(
        std::str::from_utf8(head).map_err(ProtocolError::InvalidUtf8)?,
    ))
}

/// Decode `BYTES` (non-flexible) borrowing from the input buffer.
/// Wire: INT32 length, then `length` bytes. Length −1 = null (error here).
pub fn get_bytes_borrowed<'de>(buf: &mut &'de [u8]) -> Result<&'de [u8], ProtocolError> {
    let len = get_i32(buf)?;
    if len < 0 {
        return Err(ProtocolError::InvalidValue("non-nullable BYTES was null"));
    }
    #[allow(clippy::cast_sign_loss)]
    let n = len as usize;
    if buf.len() < n {
        return Err(ProtocolError::UnexpectedEof {
            needed: n - buf.len(),
        });
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    Ok(head)
}

/// Decode nullable `BYTES` (non-flexible) borrowing from the input buffer.
pub fn get_nullable_bytes_borrowed<'de>(
    buf: &mut &'de [u8],
) -> Result<Option<&'de [u8]>, ProtocolError> {
    let len = get_i32(buf)?;
    if len < 0 {
        return Ok(None);
    }
    #[allow(clippy::cast_sign_loss)]
    let n = len as usize;
    if buf.len() < n {
        return Err(ProtocolError::UnexpectedEof {
            needed: n - buf.len(),
        });
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    Ok(Some(head))
}

/// Decode `COMPACT_BYTES` (flexible) borrowing from the input buffer.
pub fn get_compact_bytes_borrowed<'de>(buf: &mut &'de [u8]) -> Result<&'de [u8], ProtocolError> {
    let raw = get_uvarint(buf)?;
    if raw == 0 {
        return Err(ProtocolError::InvalidValue(
            "non-nullable COMPACT_BYTES was null",
        ));
    }
    let n = (raw - 1) as usize;
    if buf.len() < n {
        return Err(ProtocolError::UnexpectedEof {
            needed: n - buf.len(),
        });
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    Ok(head)
}

/// Decode nullable `COMPACT_BYTES` (flexible) borrowing from the input buffer.
pub fn get_compact_nullable_bytes_borrowed<'de>(
    buf: &mut &'de [u8],
) -> Result<Option<&'de [u8]>, ProtocolError> {
    let raw = get_uvarint(buf)?;
    if raw == 0 {
        return Ok(None);
    }
    let n = (raw - 1) as usize;
    if buf.len() < n {
        return Err(ProtocolError::UnexpectedEof {
            needed: n - buf.len(),
        });
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    Ok(Some(head))
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn borrowed_decode_zero_copy() {
        let bytes = [0x06u8, b'k', b'a', b'f', b'k', b'a'];
        let mut cur: &[u8] = &bytes;
        let s = get_compact_string_borrowed(&mut cur).unwrap();
        assert2::assert!(s == "kafka");
        // Pointer identity: `s` points inside `bytes`.
        let bytes_ptr = bytes.as_ptr() as usize;
        let s_ptr = s.as_ptr() as usize;
        assert2::assert!(s_ptr >= bytes_ptr && s_ptr < bytes_ptr + bytes.len());
    }

    #[test]
    fn get_string_borrowed_roundtrip() {
        // INT16(5) + "kafka"
        let bytes = [0x00u8, 0x05, b'k', b'a', b'f', b'k', b'a'];
        let mut cur: &[u8] = &bytes;
        let s = get_string_borrowed(&mut cur).unwrap();
        check!(s == "kafka");
        check!(cur.is_empty());
        // zero-copy: pointer is inside bytes
        check!(s.as_ptr() as usize >= bytes.as_ptr() as usize);
    }

    #[test]
    fn get_string_borrowed_null_is_error() {
        // INT16(-1)
        let bytes = [0xFFu8, 0xFF];
        let mut cur: &[u8] = &bytes;
        assert2::assert!(matches!(
            get_string_borrowed(&mut cur),
            Err(ProtocolError::InvalidValue(_))
        ));
    }

    #[test]
    fn get_nullable_string_borrowed_null() {
        let bytes = [0xFFu8, 0xFF];
        let mut cur: &[u8] = &bytes;
        assert2::assert!(get_nullable_string_borrowed(&mut cur).unwrap() == None);
        assert2::assert!(cur.is_empty());
    }

    #[test]
    fn get_nullable_string_borrowed_some() {
        let bytes = [0x00u8, 0x03, b'f', b'o', b'o'];
        let mut cur: &[u8] = &bytes;
        assert2::assert!(get_nullable_string_borrowed(&mut cur).unwrap() == Some("foo"));
        assert2::assert!(cur.is_empty());
    }

    #[test]
    fn get_bytes_borrowed_roundtrip() {
        // INT32(3) + [1,2,3]
        let bytes = [0x00u8, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03];
        let mut cur: &[u8] = &bytes;
        let b = get_bytes_borrowed(&mut cur).unwrap();
        assert2::assert!(b == &[1u8, 2, 3]);
        assert2::assert!(cur.is_empty());
    }

    #[test]
    fn get_bytes_borrowed_null_is_error() {
        let bytes = [0xFFu8, 0xFF, 0xFF, 0xFF]; // -1 as INT32
        let mut cur: &[u8] = &bytes;
        assert2::assert!(matches!(
            get_bytes_borrowed(&mut cur),
            Err(ProtocolError::InvalidValue(_))
        ));
    }

    #[test]
    fn get_nullable_bytes_borrowed_null() {
        let bytes = [0xFFu8, 0xFF, 0xFF, 0xFF];
        let mut cur: &[u8] = &bytes;
        assert2::assert!(get_nullable_bytes_borrowed(&mut cur).unwrap() == None);
    }

    #[test]
    fn get_compact_bytes_borrowed_roundtrip() {
        // UVARINT(4) = length+1=4 → 3 bytes; [0xAA, 0xBB, 0xCC]
        let bytes = [0x04u8, 0xAA, 0xBB, 0xCC];
        let mut cur: &[u8] = &bytes;
        let b = get_compact_bytes_borrowed(&mut cur).unwrap();
        assert2::assert!(b == &[0xAAu8, 0xBB, 0xCC]);
        assert2::assert!(cur.is_empty());
    }

    #[test]
    fn get_compact_bytes_borrowed_null_is_error() {
        let bytes = [0x00u8]; // varint 0 = null
        let mut cur: &[u8] = &bytes;
        assert2::assert!(matches!(
            get_compact_bytes_borrowed(&mut cur),
            Err(ProtocolError::InvalidValue(_))
        ));
    }

    #[test]
    fn get_compact_nullable_bytes_borrowed_null() {
        let bytes = [0x00u8];
        let mut cur: &[u8] = &bytes;
        assert2::assert!(get_compact_nullable_bytes_borrowed(&mut cur).unwrap() == None);
    }

    #[test]
    fn get_compact_nullable_bytes_borrowed_some() {
        // UVARINT(3) = length+1=3 → 2 bytes; [0x01, 0x02]
        let bytes = [0x03u8, 0x01, 0x02];
        let mut cur: &[u8] = &bytes;
        assert2::assert!(
            get_compact_nullable_bytes_borrowed(&mut cur).unwrap() == Some(&[0x01u8, 0x02][..])
        );
        assert2::assert!(cur.is_empty());
    }
}
