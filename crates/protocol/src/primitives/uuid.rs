use bytes::{Buf, BufMut};

use crate::ProtocolError;

/// 16-byte Kafka UUID. Big-endian on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Uuid(pub [u8; 16]);

impl Uuid {
    pub const ZERO: Uuid = Uuid([0; 16]);
}

pub fn put_uuid<B: BufMut>(buf: &mut B, u: Uuid) {
    buf.put_slice(&u.0);
}

pub fn get_uuid<B: Buf>(buf: &mut B) -> Result<Uuid, ProtocolError> {
    if buf.remaining() < 16 {
        return Err(ProtocolError::UnexpectedEof {
            needed: 16 - buf.remaining(),
        });
    }
    let mut out = [0u8; 16];
    buf.copy_to_slice(&mut out);
    Ok(Uuid(out))
}

#[cfg(test)]
mod tests {

    use bytes::BytesMut;

    use super::*;

    #[test]
    fn uuid_roundtrip() {
        let u = Uuid([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
        let mut buf = BytesMut::new();
        put_uuid(&mut buf, u);
        assert2::assert!(buf.len() == 16);
        let mut cur = &buf[..];
        assert2::assert!(get_uuid(&mut cur).unwrap() == u);
    }
}
