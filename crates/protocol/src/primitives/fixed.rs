use bytes::{Buf, BufMut};

use crate::ProtocolError;

#[inline]
fn need(buf: &impl Buf, n: usize) -> Result<(), ProtocolError> {
    if buf.remaining() < n {
        Err(ProtocolError::UnexpectedEof {
            needed: n - buf.remaining(),
        })
    } else {
        Ok(())
    }
}

pub fn put_i8<B: BufMut>(buf: &mut B, v: i8) {
    buf.put_i8(v);
}
/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_i8<B: Buf>(buf: &mut B) -> Result<i8, ProtocolError> {
    need(buf, 1)?;
    Ok(buf.get_i8())
}

pub fn put_i16<B: BufMut>(buf: &mut B, v: i16) {
    buf.put_i16(v);
}
/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_i16<B: Buf>(buf: &mut B) -> Result<i16, ProtocolError> {
    need(buf, 2)?;
    Ok(buf.get_i16())
}

pub fn put_u16<B: BufMut>(buf: &mut B, v: u16) {
    buf.put_u16(v);
}
/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_u16<B: Buf>(buf: &mut B) -> Result<u16, ProtocolError> {
    need(buf, 2)?;
    Ok(buf.get_u16())
}

pub fn put_i32<B: BufMut>(buf: &mut B, v: i32) {
    buf.put_i32(v);
}
/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_i32<B: Buf>(buf: &mut B) -> Result<i32, ProtocolError> {
    need(buf, 4)?;
    Ok(buf.get_i32())
}

pub fn put_i64<B: BufMut>(buf: &mut B, v: i64) {
    buf.put_i64(v);
}
/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_i64<B: Buf>(buf: &mut B) -> Result<i64, ProtocolError> {
    need(buf, 8)?;
    Ok(buf.get_i64())
}

pub fn put_u32<B: BufMut>(buf: &mut B, v: u32) {
    buf.put_u32(v);
}
/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_u32<B: Buf>(buf: &mut B) -> Result<u32, ProtocolError> {
    need(buf, 4)?;
    Ok(buf.get_u32())
}

pub fn put_bool<B: BufMut>(buf: &mut B, v: bool) {
    buf.put_u8(u8::from(v));
}
/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_bool<B: Buf>(buf: &mut B) -> Result<bool, ProtocolError> {
    need(buf, 1)?;
    match buf.get_u8() {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(ProtocolError::InvalidValue("boolean must be 0 or 1")),
    }
}

pub fn put_f64<B: BufMut>(buf: &mut B, v: f64) {
    buf.put_f64(v);
}
/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_f64<B: Buf>(buf: &mut B) -> Result<f64, ProtocolError> {
    need(buf, 8)?;
    Ok(buf.get_f64())
}

#[cfg(test)]
mod tests {

    use bytes::BytesMut;

    use super::*;

    macro_rules! roundtrip {
        ($name:ident, $put:ident, $get:ident, $values:expr) => {
            #[test]
            fn $name() {
                for v in $values {
                    let mut buf = BytesMut::new();
                    $put(&mut buf, v);
                    let mut cur = &buf[..];
                    assert2::assert!($get(&mut cur).unwrap() == v);
                    assert2::assert!(cur.is_empty());
                }
            }
        };
    }

    roundtrip!(i8_roundtrip, put_i8, get_i8, [i8::MIN, -1, 0, 1, i8::MAX]);
    roundtrip!(
        i16_roundtrip,
        put_i16,
        get_i16,
        [i16::MIN, -1, 0, 1, i16::MAX]
    );
    roundtrip!(
        i32_roundtrip,
        put_i32,
        get_i32,
        [i32::MIN, -1, 0, 1, i32::MAX]
    );
    roundtrip!(
        i64_roundtrip,
        put_i64,
        get_i64,
        [i64::MIN, -1, 0, 1, i64::MAX]
    );

    #[test]
    fn bool_roundtrip() {
        for v in [false, true] {
            let mut buf = BytesMut::new();
            put_bool(&mut buf, v);
            let mut cur = &buf[..];
            assert2::assert!(get_bool(&mut cur).unwrap() == v);
        }
    }

    #[test]
    fn bool_rejects_invalid() {
        let bytes = [2u8];
        let mut cur = &bytes[..];
        assert2::assert!(get_bool(&mut cur).is_err());
    }

    #[test]
    fn eof_is_reported() {
        let empty: &[u8] = &[];
        let mut cur = empty;
        assert2::assert!(matches!(
            get_i32(&mut cur),
            Err(ProtocolError::UnexpectedEof { needed: 4 })
        ));
    }

    #[test]
    fn big_endian_layout_i32() {
        let mut buf = BytesMut::new();
        put_i32(&mut buf, 0x0102_0304);
        assert2::assert!(&buf[..] == &[0x01, 0x02, 0x03, 0x04]);
    }
}
