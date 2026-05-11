use crate::primitives::varint::get_uvarint;
use crate::ProtocolError;

/// Decode a `COMPACT_STRING` borrowing from the input buffer.
/// Requires a contiguous buffer (i.e. `&[u8]`).
pub fn get_compact_string_borrowed<'de>(buf: &mut &'de [u8]) -> Result<&'de str, ProtocolError> {
    let raw = get_uvarint(buf)?;
    if raw == 0 {
        return Err(ProtocolError::InvalidValue("non-nullable COMPACT_STRING was null"));
    }
    let n = (raw - 1) as usize;
    if buf.len() < n {
        return Err(ProtocolError::UnexpectedEof { needed: n - buf.len() });
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    std::str::from_utf8(head).map_err(ProtocolError::InvalidUtf8)
}

pub fn get_compact_nullable_string_borrowed<'de>(buf: &mut &'de [u8]) -> Result<Option<&'de str>, ProtocolError> {
    let raw = get_uvarint(buf)?;
    if raw == 0 { return Ok(None); }
    let n = (raw - 1) as usize;
    if buf.len() < n {
        return Err(ProtocolError::UnexpectedEof { needed: n - buf.len() });
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    Ok(Some(std::str::from_utf8(head).map_err(ProtocolError::InvalidUtf8)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrowed_decode_zero_copy() {
        let bytes = [0x06u8, b'k', b'a', b'f', b'k', b'a'];
        let mut cur: &[u8] = &bytes;
        let s = get_compact_string_borrowed(&mut cur).unwrap();
        assert_eq!(s, "kafka");
        // Pointer identity: `s` points inside `bytes`.
        let bytes_ptr = bytes.as_ptr() as usize;
        let s_ptr = s.as_ptr() as usize;
        assert!(s_ptr >= bytes_ptr && s_ptr < bytes_ptr + bytes.len());
    }
}
