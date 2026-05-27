//! `RecordsPayload`: the wire-field type for any Kafka message whose schema
//! declares a `records` field (`Fetch`, `Produce`, `FetchSnapshot`, `ShareFetch`).
//!
//! Kafka's records-field is "opaque bytes" at the protocol layer; the
//! contents may be a v2 `RecordBatch` (current) or a v0/v1 `MessageSet`
//! (legacy, used by old clients on down-conversion). The discriminator
//! is the **magic byte at offset 16** of the first batch, which is at
//! the same offset for both formats by coincidence of layout (v2:
//! `base_offset+batch_length+leader_epoch+magic`; legacy: `offset+
//! message_size+crc+magic`).
//!
//! Eagerly parsing the v2 form keeps the existing broker code paths
//! unchanged: where they used `RecordBatch::encoded_len` and similar,
//! they now call the equivalent method on `RecordsPayload`. Legacy
//! payloads are kept as raw [`Bytes`] and round-tripped verbatim — the
//! [`crabka-records-legacy`](../../records_legacy/index.html) crate
//! provides the codec when an old client actually appears on the wire.

use bytes::{Buf, BufMut, Bytes};

use crate::records::RecordsError;
use crate::records::borrowed::RecordBatch as RecordBatchBorrowed;
use crate::records::owned::RecordBatch;

/// Owned form of a records-field payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordsPayload {
    /// Parsed v2 batch.
    V2(RecordBatch),
    /// Opaque pre-v2 bytes (v0/v1 `MessageSet`). Decode with
    /// `crabka_records_legacy::decode_message_set`.
    Legacy(Bytes),
}

impl RecordsPayload {
    /// Construct from raw records-field bytes.
    ///
    /// Peeks the magic byte at offset 16; if it is `2`, eagerly parses
    /// the bytes as a v2 batch. Anything else is preserved as opaque
    /// legacy bytes.
    pub fn from_bytes(bytes: Bytes) -> Result<Self, RecordsError> {
        if looks_like_v2(&bytes) {
            let mut cur: &[u8] = &bytes;
            let rb = RecordBatch::decode(&mut cur)?;
            Ok(Self::V2(rb))
        } else {
            Ok(Self::Legacy(bytes))
        }
    }

    /// Wire size of the inner bytes (excluding the outer nullable-bytes
    /// length prefix). For a v2 batch, this is the byte count of
    /// `RecordBatch::encode`; for legacy it is the held byte length.
    #[must_use]
    pub fn payload_len(&self) -> usize {
        match self {
            Self::V2(rb) => rb.encoded_len(),
            Self::Legacy(b) => b.len(),
        }
    }

    /// Write the payload bytes into `buf`. Caller is responsible for the
    /// outer nullable-bytes framing.
    pub fn encode_to<B: BufMut>(&self, buf: &mut B) -> Result<(), RecordsError> {
        match self {
            Self::V2(rb) => rb.encode(buf),
            Self::Legacy(b) => {
                buf.put_slice(b);
                Ok(())
            }
        }
    }

    /// Borrow as a parsed v2 batch, if that's what this payload is.
    #[must_use]
    pub fn as_v2(&self) -> Option<&RecordBatch> {
        match self {
            Self::V2(rb) => Some(rb),
            Self::Legacy(_) => None,
        }
    }

    /// Borrow as raw legacy bytes, if that's what this payload is.
    #[must_use]
    pub fn as_legacy(&self) -> Option<&Bytes> {
        match self {
            Self::Legacy(b) => Some(b),
            Self::V2(_) => None,
        }
    }
}

impl From<RecordBatch> for RecordsPayload {
    fn from(rb: RecordBatch) -> Self {
        Self::V2(rb)
    }
}

impl Default for RecordsPayload {
    fn default() -> Self {
        Self::V2(RecordBatch::default())
    }
}

impl crate::Encode for RecordsPayload {
    fn encode<B: BufMut>(&self, buf: &mut B, _version: i16) -> Result<(), crate::ProtocolError> {
        self.encode_to(buf).map_err(Into::into)
    }

    fn encoded_len(&self, _version: i16) -> usize {
        self.payload_len()
    }
}

impl crate::Decode<'_> for RecordsPayload {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<Self, crate::ProtocolError> {
        // The caller (generated codec) has already sliced the buffer to
        // exactly the records-field bytes via `get_(nullable_)bytes_owned`,
        // so we consume everything.
        let bytes = buf.copy_to_bytes(buf.remaining());
        Self::from_bytes(bytes).map_err(Into::into)
    }
}

/// Borrowed form: zero-copy view into the input buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordsPayloadBorrowed<'a> {
    V2(RecordBatchBorrowed<'a>),
    Legacy(&'a [u8]),
}

impl<'a> RecordsPayloadBorrowed<'a> {
    pub fn from_slice(bytes: &'a [u8]) -> Result<Self, RecordsError> {
        if looks_like_v2(bytes) {
            let mut cur: &[u8] = bytes;
            let rb =
                <RecordBatchBorrowed<'a> as crate::DecodeBorrow<'a>>::decode_borrow(&mut cur, 0)
                    .map_err(|e| RecordsError::RecordParse(format!("borrowed v2 decode: {e}")))?;
            Ok(Self::V2(rb))
        } else {
            Ok(Self::Legacy(bytes))
        }
    }

    #[must_use]
    pub fn payload_len(&self) -> usize {
        match self {
            Self::V2(rb) => crate::Encode::encoded_len(rb, 0),
            Self::Legacy(b) => b.len(),
        }
    }

    pub fn encode_to<B: BufMut>(&self, buf: &mut B) -> Result<(), RecordsError> {
        match self {
            Self::V2(rb) => crate::Encode::encode(rb, buf, 0)
                .map_err(|e| RecordsError::RecordParse(format!("borrowed v2 encode: {e}"))),
            Self::Legacy(b) => {
                buf.put_slice(b);
                Ok(())
            }
        }
    }

    /// Convert to the owned flavor, performing any necessary buffer copies.
    pub fn to_owned(&self) -> Result<RecordsPayload, RecordsError> {
        match self {
            Self::V2(rb) => rb.to_owned().map(RecordsPayload::V2),
            Self::Legacy(b) => Ok(RecordsPayload::Legacy(Bytes::copy_from_slice(b))),
        }
    }
}

impl Default for RecordsPayloadBorrowed<'_> {
    fn default() -> Self {
        Self::V2(RecordBatchBorrowed::default())
    }
}

impl crate::Encode for RecordsPayloadBorrowed<'_> {
    fn encode<B: BufMut>(&self, buf: &mut B, _version: i16) -> Result<(), crate::ProtocolError> {
        self.encode_to(buf).map_err(Into::into)
    }

    fn encoded_len(&self, _version: i16) -> usize {
        self.payload_len()
    }
}

impl<'de> crate::DecodeBorrow<'de> for RecordsPayloadBorrowed<'de> {
    fn decode_borrow(buf: &mut &'de [u8], _version: i16) -> Result<Self, crate::ProtocolError> {
        // Caller has sliced the buffer to exactly the records-field bytes;
        // consume everything by swapping in an empty tail.
        let bytes = std::mem::take(buf);
        Self::from_slice(bytes).map_err(Into::into)
    }
}

/// True when `bytes` look like a v2 record batch (magic byte 2 at the
/// well-known offset). v0 and v1 legacy `MessageSets` carry magic 0 or 1
/// at the same offset, so this check distinguishes the two.
///
/// The threshold is `MAGIC_OFFSET + 1 = 17`, not the full `HEADER_LEN`:
/// a truncated v2 batch still needs to land in the V2 arm so the
/// downstream `RecordBatch::decode` can surface a precise error,
/// rather than be silently misclassified as legacy.
#[inline]
fn looks_like_v2(bytes: &[u8]) -> bool {
    // The magic byte sits at `base_offset(8) + batch_length(4) +
    // partition_leader_epoch(4) = 16` in v2. Legacy MessageSets place
    // the first message's magic byte at `offset(8) + message_size(4) +
    // crc(4) = 16` — same index, different meaning.
    const MAGIC_OFFSET: usize = 16;
    bytes.len() > MAGIC_OFFSET && bytes[MAGIC_OFFSET] == 2
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::{Record, RecordBatch};
    use bytes::BytesMut;

    fn sample_v2() -> RecordBatch {
        RecordBatch {
            base_offset: 42,
            records: vec![Record {
                key: Some(Bytes::from_static(b"k")),
                value: Some(Bytes::from_static(b"v")),
                ..Default::default()
            }],
            ..RecordBatch::default()
        }
    }

    #[test]
    fn from_bytes_dispatches_v2() {
        let rb = sample_v2();
        let mut buf = BytesMut::new();
        rb.encode(&mut buf).unwrap();
        let p = RecordsPayload::from_bytes(buf.freeze()).unwrap();
        match p {
            RecordsPayload::V2(parsed) => assert_eq!(parsed, rb),
            RecordsPayload::Legacy(_) => panic!("expected V2"),
        }
    }

    #[test]
    fn from_bytes_dispatches_legacy() {
        // Build a fake legacy MessageSet entry: offset(8) + size(4) + crc(4) +
        // magic(1)=1 + … . Just need byte 16 to be 1.
        let mut buf = vec![0u8; 17];
        buf[16] = 1;
        let p = RecordsPayload::from_bytes(Bytes::from(buf.clone())).unwrap();
        match p {
            RecordsPayload::Legacy(b) => assert_eq!(&b[..], &buf[..]),
            RecordsPayload::V2(_) => panic!("expected Legacy"),
        }
    }

    #[test]
    fn roundtrip_v2() {
        let p: RecordsPayload = sample_v2().into();
        let mut buf = BytesMut::new();
        p.encode_to(&mut buf).unwrap();
        let back = RecordsPayload::from_bytes(buf.freeze()).unwrap();
        assert_eq!(p, back);
        assert_eq!(p.payload_len(), back.payload_len());
    }

    #[test]
    fn encode_decode_via_traits() {
        let p: RecordsPayload = sample_v2().into();
        let mut buf = BytesMut::new();
        <RecordsPayload as crate::Encode>::encode(&p, &mut buf, 0).unwrap();
        let mut cur: &[u8] = &buf;
        let back = <RecordsPayload as crate::Decode>::decode(&mut cur, 0).unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn borrowed_dispatches() {
        let rb = sample_v2();
        let mut buf = BytesMut::new();
        rb.encode(&mut buf).unwrap();
        let frozen = buf.freeze();
        let p = RecordsPayloadBorrowed::from_slice(&frozen).unwrap();
        assert!(matches!(p, RecordsPayloadBorrowed::V2(_)));
        let owned = p.to_owned().unwrap();
        match owned {
            RecordsPayload::V2(parsed) => assert_eq!(parsed.base_offset, 42),
            RecordsPayload::Legacy(_) => panic!(),
        }
    }

    #[test]
    fn from_record_batch() {
        let rb = sample_v2();
        let p: RecordsPayload = rb.clone().into();
        assert_eq!(p.as_v2(), Some(&rb));
        assert!(p.as_legacy().is_none());
    }

    fn legacy_bytes() -> Bytes {
        let mut buf = vec![0u8; 24];
        buf[16] = 1;
        for (i, b) in (b'a'..=b'h').enumerate() {
            buf[17 + i % 7] = b;
        }
        Bytes::from(buf)
    }

    #[test]
    fn legacy_payload_len_and_encode_owned() {
        let bytes = legacy_bytes();
        let p = RecordsPayload::from_bytes(bytes.clone()).unwrap();
        assert_eq!(p.payload_len(), bytes.len());
        assert!(p.as_v2().is_none());
        assert_eq!(p.as_legacy(), Some(&bytes));

        let mut out = BytesMut::new();
        p.encode_to(&mut out).unwrap();
        assert_eq!(&out[..], &bytes[..]);
    }

    #[test]
    fn legacy_roundtrip_via_traits() {
        let bytes = legacy_bytes();
        let p = RecordsPayload::from_bytes(bytes.clone()).unwrap();
        let mut buf = BytesMut::new();
        <RecordsPayload as crate::Encode>::encode(&p, &mut buf, 0).unwrap();
        assert_eq!(
            <RecordsPayload as crate::Encode>::encoded_len(&p, 0),
            bytes.len()
        );
        let mut cur: &[u8] = &buf;
        let back = <RecordsPayload as crate::Decode>::decode(&mut cur, 0).unwrap();
        assert!(matches!(back, RecordsPayload::Legacy(_)));
        assert_eq!(back.as_legacy().unwrap(), &bytes);
    }

    #[test]
    fn owned_default_is_empty_v2() {
        let p = RecordsPayload::default();
        assert!(matches!(p, RecordsPayload::V2(_)));
    }

    #[test]
    fn looks_like_v2_rejects_short_buffer() {
        // Too short to peek the magic byte at offset 16; must fall through to Legacy.
        let short = Bytes::from_static(&[0u8; 10]);
        let p = RecordsPayload::from_bytes(short.clone()).unwrap();
        assert_eq!(p.as_legacy(), Some(&short));
    }

    #[test]
    fn borrowed_legacy_roundtrip() {
        let bytes = legacy_bytes();
        let p = RecordsPayloadBorrowed::from_slice(&bytes).unwrap();
        assert!(matches!(p, RecordsPayloadBorrowed::Legacy(_)));
        assert_eq!(p.payload_len(), bytes.len());

        let mut out = BytesMut::new();
        p.encode_to(&mut out).unwrap();
        assert_eq!(&out[..], &bytes[..]);

        let owned = p.to_owned().unwrap();
        match owned {
            RecordsPayload::Legacy(b) => assert_eq!(&b[..], &bytes[..]),
            RecordsPayload::V2(_) => panic!("expected Legacy"),
        }
    }

    #[test]
    fn borrowed_v2_payload_len_and_encode() {
        let rb = sample_v2();
        let mut buf = BytesMut::new();
        rb.encode(&mut buf).unwrap();
        let frozen = buf.freeze();
        let p = RecordsPayloadBorrowed::from_slice(&frozen).unwrap();
        assert_eq!(p.payload_len(), frozen.len());

        let mut out = BytesMut::new();
        p.encode_to(&mut out).unwrap();
        assert_eq!(&out[..], &frozen[..]);
    }

    #[test]
    fn borrowed_encode_decode_via_traits() {
        let rb = sample_v2();
        let mut buf = BytesMut::new();
        rb.encode(&mut buf).unwrap();
        let frozen = buf.freeze();

        let p = RecordsPayloadBorrowed::from_slice(&frozen).unwrap();
        let mut out = BytesMut::new();
        <RecordsPayloadBorrowed as crate::Encode>::encode(&p, &mut out, 0).unwrap();
        assert_eq!(
            <RecordsPayloadBorrowed as crate::Encode>::encoded_len(&p, 0),
            frozen.len()
        );

        let mut cur: &[u8] = &out;
        let back =
            <RecordsPayloadBorrowed as crate::DecodeBorrow>::decode_borrow(&mut cur, 0).unwrap();
        assert!(matches!(back, RecordsPayloadBorrowed::V2(_)));
    }

    #[test]
    fn borrowed_default_is_empty_v2() {
        let p = RecordsPayloadBorrowed::default();
        assert!(matches!(p, RecordsPayloadBorrowed::V2(_)));
    }
}
