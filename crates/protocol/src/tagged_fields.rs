//! KIP-482 flexible-version tagged fields.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::primitives::varint::{get_uvarint, put_uvarint, uvarint_len};
use crate::ProtocolError;

/// An unknown tagged field that was preserved verbatim during decode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownTaggedField {
    pub tag: u32,
    pub bytes: Bytes,
}

/// A collection of tagged fields that the schema does not declare. Generated
/// message types contain a `Vec<UnknownTaggedField>` (sorted by tag) so that
/// values can be round-tripped without information loss.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UnknownTaggedFields(pub Vec<UnknownTaggedField>);

impl UnknownTaggedFields {
    pub fn is_empty(&self) -> bool { self.0.is_empty() }
    pub fn len(&self) -> usize { self.0.len() }
}

/// Read the tagged-fields trailer at the current position of `buf`. `known`
/// is called for each entry whose tag is in the schema; it should consume the
/// field's payload (a `size`-byte slice) and return Ok if it recognised the
/// tag. Anything `known` returns `Ok(false)` for is captured into the unknown
/// vec instead.
pub fn read_tagged_fields<B, F>(buf: &mut B, mut known: F) -> Result<UnknownTaggedFields, ProtocolError>
where
    B: Buf,
    F: FnMut(u32, &mut &[u8]) -> Result<bool, ProtocolError>,
{
    let count = get_uvarint(buf)? as usize;
    let mut unknown = Vec::new();
    let mut last_tag: Option<u32> = None;
    for _ in 0..count {
        let tag = get_uvarint(buf)?;
        if let Some(prev) = last_tag {
            if tag <= prev {
                return Err(ProtocolError::InvalidValue("tagged fields not strictly ascending"));
            }
        }
        last_tag = Some(tag);
        let size = get_uvarint(buf)? as usize;
        if buf.remaining() < size {
            return Err(ProtocolError::UnexpectedEof { needed: size - buf.remaining() });
        }
        // Copy the payload so we can hand a slice to the closure or store it.
        let mut payload = vec![0u8; size];
        buf.copy_to_slice(&mut payload);
        let mut slice = &payload[..];
        if !known(tag, &mut slice)? {
            unknown.push(UnknownTaggedField { tag, bytes: Bytes::from(payload) });
        } else if !slice.is_empty() {
            return Err(ProtocolError::InvalidValue("tagged field decoder did not consume all bytes"));
        }
    }
    Ok(UnknownTaggedFields(unknown))
}

/// Helper used by generated code while emitting tagged fields. Call
/// `WriteTaggedFields::new()`, then `add` each known tag-and-payload-encoder,
/// then `write` to flush, merging with `unknown`.
pub struct WriteTaggedFields {
    entries: Vec<(u32, Bytes)>,
}

impl WriteTaggedFields {
    pub fn new() -> Self { Self { entries: Vec::new() } }

    pub fn add(&mut self, tag: u32, payload: Bytes) {
        self.entries.push((tag, payload));
    }

    pub fn write<B: BufMut>(mut self, buf: &mut B, unknown: &UnknownTaggedFields) {
        for u in &unknown.0 {
            self.entries.push((u.tag, u.bytes.clone()));
        }
        self.entries.sort_by_key(|(t, _)| *t);
        put_uvarint(buf, u32::try_from(self.entries.len()).expect("too many tagged fields"));
        for (tag, payload) in self.entries {
            put_uvarint(buf, tag);
            put_uvarint(buf, u32::try_from(payload.len()).expect("tagged field too large"));
            buf.put_slice(&payload);
        }
    }
}

/// Predicted length of the tagged-fields trailer.
pub fn tagged_fields_len(known: &[(u32, usize)], unknown: &UnknownTaggedFields) -> usize {
    let total = known.len() + unknown.0.len();
    let mut n = uvarint_len(u32::try_from(total).unwrap());
    for (tag, len) in known {
        n += uvarint_len(*tag) + uvarint_len(u32::try_from(*len).unwrap()) + *len;
    }
    for u in &unknown.0 {
        n += uvarint_len(u.tag) + uvarint_len(u32::try_from(u.bytes.len()).unwrap()) + u.bytes.len();
    }
    n
}

/// Encode a value into a freshly-allocated `Bytes` (used to materialize a
/// tagged-field payload before sizing the outer trailer).
pub fn encode_to_bytes<F>(predicted_len: usize, write: F) -> Bytes
where
    F: FnOnce(&mut BytesMut),
{
    let mut buf = BytesMut::with_capacity(predicted_len);
    write(&mut buf);
    debug_assert_eq!(buf.len(), predicted_len, "encoded_len lied");
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tagged_fields() {
        let buf = [0x00u8];
        let mut cur = &buf[..];
        let unknown = read_tagged_fields(&mut cur, |_, _| Ok(false)).unwrap();
        assert!(unknown.is_empty());
        assert!(cur.is_empty());
    }

    #[test]
    fn unknown_tagged_fields_preserved() {
        // count=1, tag=5, size=3, payload=[10,20,30]
        let buf = [0x01, 0x05, 0x03, 10, 20, 30];
        let mut cur = &buf[..];
        let unknown = read_tagged_fields(&mut cur, |_, _| Ok(false)).unwrap();
        assert_eq!(unknown.len(), 1);
        assert_eq!(unknown.0[0].tag, 5);
        assert_eq!(unknown.0[0].bytes.as_ref(), &[10, 20, 30]);
    }

    #[test]
    fn ascending_order_enforced() {
        // count=2, tag=5..., tag=3...  — invalid (descending)
        let buf = [0x02, 0x05, 0x01, 0x00, 0x03, 0x01, 0x00];
        let mut cur = &buf[..];
        assert!(read_tagged_fields(&mut cur, |_, _| Ok(false)).is_err());
    }

    #[test]
    fn write_merges_known_and_unknown_sorted() {
        let mut w = WriteTaggedFields::new();
        w.add(10, Bytes::from_static(&[0xAA]));
        let unknown = UnknownTaggedFields(vec![
            UnknownTaggedField { tag: 5, bytes: Bytes::from_static(&[0xBB]) },
        ]);
        let mut out = BytesMut::new();
        w.write(&mut out, &unknown);
        // Expect: count=2, tag=5,len=1,0xBB, tag=10,len=1,0xAA
        assert_eq!(&out[..], &[0x02, 0x05, 0x01, 0xBB, 0x0A, 0x01, 0xAA]);
    }
}
