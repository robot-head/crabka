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
    /// Zero or more parsed v2 batches (the records field is a *sequence*).
    V2(Vec<RecordBatch>),
    /// Verbatim, already-wire-format v2 bytes (one or more batches),
    /// forwarded without parsing. Produced by the fetch pass-through path.
    Raw(Bytes),
    /// Opaque pre-v2 bytes (v0/v1 `MessageSet`). Decode with
    /// `crabka_records_legacy::decode_message_set`.
    Legacy(Bytes),
    /// Zero-copy fetch (Increments D + E): the records run lives in segment
    /// `.log` files and is `sendfile(2)`d straight to a plaintext socket —
    /// never materialized in userspace. One [`FileRegion`] per contributing
    /// segment. `encode_to` falls back to `pread` + `put_slice` (used on TLS /
    /// non-sendfile platforms and for `encoded_len` agreement). Gated on the
    /// SENDFILE alias (Linux + Apple + FreeBSD/DragonFly) because it only exists
    /// to feed the `sendfile` drainer; on Windows the fallback `pread` path runs.
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    FileRegions(Vec<crate::records::FileRegion>),
}

impl RecordsPayload {
    /// Construct from raw records-field bytes. When the bytes look like v2,
    /// decode *every* batch in the field; otherwise keep as opaque legacy.
    pub fn from_bytes(bytes: Bytes) -> Result<Self, RecordsError> {
        if looks_like_v2(&bytes) {
            let mut cur: &[u8] = &bytes;
            let mut batches = Vec::new();
            while !cur.is_empty() {
                batches.push(RecordBatch::decode(&mut cur)?);
            }
            Ok(Self::V2(batches))
        } else {
            Ok(Self::Legacy(bytes))
        }
    }

    /// Wire size of the records-field bytes (no outer length prefix).
    #[must_use]
    pub fn payload_len(&self) -> usize {
        match self {
            Self::V2(batches) => batches.iter().map(RecordBatch::encoded_len).sum(),
            Self::Raw(b) | Self::Legacy(b) => b.len(),
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "ios",
                target_os = "tvos",
                target_os = "watchos",
                target_os = "freebsd",
                target_os = "dragonfly",
            ))]
            Self::FileRegions(regions) => regions.iter().map(|r| r.len).sum(),
        }
    }

    /// Write the payload bytes into `buf` (caller owns the outer framing).
    ///
    /// For `FileRegions` this is the **fallback** path (TLS / non-Linux /
    /// `encoded_len` agreement): it `pread`s each region out of the segment
    /// file and copies it into `buf`. The zero-copy `sendfile` path in the
    /// broker never calls this — it consumes the `FileRegion`s directly.
    pub fn encode_to<B: BufMut>(&self, buf: &mut B) -> Result<(), RecordsError> {
        match self {
            Self::V2(batches) => {
                for b in batches {
                    b.encode(buf)?;
                }
                Ok(())
            }
            Self::Raw(b) | Self::Legacy(b) => {
                buf.put_slice(b);
                Ok(())
            }
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "ios",
                target_os = "tvos",
                target_os = "watchos",
                target_os = "freebsd",
                target_os = "dragonfly",
            ))]
            Self::FileRegions(regions) => {
                use std::os::unix::fs::FileExt;
                let mut scratch = vec![0u8; 0];
                for region in regions {
                    scratch.resize(region.len, 0);
                    let mut filled = 0usize;
                    let mut offset = region.offset;
                    while filled < region.len {
                        match region.file.read_at(&mut scratch[filled..], offset) {
                            Ok(0) => {
                                return Err(RecordsError::RecordParse(
                                    "FileRegion read hit EOF before len bytes".into(),
                                ));
                            }
                            Ok(n) => {
                                filled += n;
                                offset += n as u64;
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                            Err(e) => {
                                return Err(RecordsError::RecordParse(format!(
                                    "FileRegion read error: {e}"
                                )));
                            }
                        }
                    }
                    buf.put_slice(&scratch);
                }
                Ok(())
            }
        }
    }

    /// Borrow the parsed v2 batches, if this is a parsed `V2` payload.
    /// Returns `None` for `Raw` (intentionally unparsed), `Legacy`, and
    /// `FileRegions` (deliberately never materialized).
    #[must_use]
    pub fn as_v2(&self) -> Option<&[RecordBatch]> {
        match self {
            Self::V2(batches) => Some(batches),
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "ios",
                target_os = "tvos",
                target_os = "watchos",
                target_os = "freebsd",
                target_os = "dragonfly",
            ))]
            Self::FileRegions(_) => None,
            Self::Raw(_) | Self::Legacy(_) => None,
        }
    }

    /// Borrow as raw legacy bytes, if that's what this payload is.
    #[must_use]
    pub fn as_legacy(&self) -> Option<&Bytes> {
        match self {
            Self::Legacy(b) => Some(b),
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "ios",
                target_os = "tvos",
                target_os = "watchos",
                target_os = "freebsd",
                target_os = "dragonfly",
            ))]
            Self::FileRegions(_) => None,
            Self::V2(_) | Self::Raw(_) => None,
        }
    }

    /// Decode a **response-side** records field, tolerating a truncated
    /// trailing batch. Kafka returns a partial final `RecordBatch` when a
    /// partition's fetch byte budget is hit mid-batch; the JVM consumer stops
    /// at the first incomplete batch and re-fetches it from the next offset.
    /// We mirror that: decode every complete batch, and on the first
    /// `HeaderTooShort` / `BodyTooShort` stop and drop the remainder. A
    /// *corrupt* complete batch (bad CRC/magic/content) still errors — leniency
    /// forgives truncation only. Strict [`from_bytes`](Self::from_bytes) is
    /// retained for Produce-request validation.
    ///
    /// Only `HeaderTooShort`/`BodyTooShort` are treated as truncation; a genuinely
    /// invalid `batch_length` (`RecordParse`) is corruption and still errors —
    /// legitimate Kafka truncation always preserves a valid `batch_length` prefix,
    /// so it can only manifest as the too-short variants.
    pub fn from_fetch_bytes(bytes: Bytes) -> Result<Self, RecordsError> {
        if !looks_like_v2(&bytes) {
            return Ok(Self::Legacy(bytes));
        }
        let mut cur: &[u8] = &bytes;
        let mut batches = Vec::new();
        while !cur.is_empty() {
            match RecordBatch::decode(&mut cur) {
                Ok(rb) => batches.push(rb),
                Err(RecordsError::HeaderTooShort { .. } | RecordsError::BodyTooShort { .. }) => {
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(Self::V2(batches))
    }

    /// `Decode`-shaped lenient entry point the generated codec calls for
    /// records fields in **response** messages. Consumes the whole sliced
    /// field buffer (the caller has already framed it) and parses leniently
    /// via [`from_fetch_bytes`](Self::from_fetch_bytes).
    pub fn decode_lenient<B: Buf>(
        buf: &mut B,
        _version: i16,
    ) -> Result<Self, crate::ProtocolError> {
        let bytes = buf.copy_to_bytes(buf.remaining());
        Self::from_fetch_bytes(bytes).map_err(Into::into)
    }
}

impl From<RecordBatch> for RecordsPayload {
    fn from(rb: RecordBatch) -> Self {
        Self::V2(vec![rb])
    }
}

impl From<Vec<RecordBatch>> for RecordsPayload {
    fn from(v: Vec<RecordBatch>) -> Self {
        Self::V2(v)
    }
}

impl Default for RecordsPayload {
    fn default() -> Self {
        Self::V2(Vec::new())
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
    V2(Vec<RecordBatchBorrowed<'a>>),
    Legacy(&'a [u8]),
}

impl<'a> RecordsPayloadBorrowed<'a> {
    pub fn from_slice(bytes: &'a [u8]) -> Result<Self, RecordsError> {
        if looks_like_v2(bytes) {
            let mut cur: &'a [u8] = bytes;
            let mut batches = Vec::new();
            while !cur.is_empty() {
                let rb = <RecordBatchBorrowed<'a> as crate::DecodeBorrow<'a>>::decode_borrow(
                    &mut cur, 0,
                )
                .map_err(|e| RecordsError::RecordParse(format!("borrowed v2 decode: {e}")))?;
                batches.push(rb);
            }
            Ok(Self::V2(batches))
        } else {
            Ok(Self::Legacy(bytes))
        }
    }

    #[must_use]
    pub fn payload_len(&self) -> usize {
        match self {
            Self::V2(batches) => batches
                .iter()
                .map(|rb| crate::Encode::encoded_len(rb, 0))
                .sum(),
            Self::Legacy(b) => b.len(),
        }
    }

    pub fn encode_to<B: BufMut>(&self, buf: &mut B) -> Result<(), RecordsError> {
        match self {
            Self::V2(batches) => {
                for rb in batches {
                    crate::Encode::encode(rb, buf, 0).map_err(|e| {
                        RecordsError::RecordParse(format!("borrowed v2 encode: {e}"))
                    })?;
                }
                Ok(())
            }
            Self::Legacy(b) => {
                buf.put_slice(b);
                Ok(())
            }
        }
    }

    /// Convert to the owned flavor, performing any necessary buffer copies.
    pub fn to_owned(&self) -> Result<RecordsPayload, RecordsError> {
        match self {
            Self::V2(batches) => {
                let mut owned = Vec::with_capacity(batches.len());
                for rb in batches {
                    owned.push(rb.to_owned()?);
                }
                Ok(RecordsPayload::V2(owned))
            }
            Self::Legacy(b) => Ok(RecordsPayload::Legacy(Bytes::copy_from_slice(b))),
        }
    }
}

impl Default for RecordsPayloadBorrowed<'_> {
    fn default() -> Self {
        Self::V2(Vec::new())
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
    use assert2::assert;
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
            RecordsPayload::V2(batches) => assert!(batches == vec![rb]),
            _ => panic!("expected V2"),
        }
    }

    #[test]
    fn from_bytes_parses_all_batches() {
        // Two v2 batches concatenated must both decode.
        let mut b0 = sample_v2();
        b0.base_offset = 0;
        let mut b1 = sample_v2();
        b1.base_offset = 1;
        let mut buf = BytesMut::new();
        b0.encode(&mut buf).unwrap();
        b1.encode(&mut buf).unwrap();
        let p = RecordsPayload::from_bytes(buf.freeze()).unwrap();
        let batches = p.as_v2().expect("v2");
        assert!(batches.len() == 2);
        assert!(batches[0].base_offset == 0);
        assert!(batches[1].base_offset == 1);
    }

    #[test]
    fn raw_passthrough_roundtrips() {
        let mut b = sample_v2();
        b.base_offset = 7;
        let mut wire = BytesMut::new();
        b.encode(&mut wire).unwrap();
        let wire = wire.freeze();
        let p = RecordsPayload::Raw(wire.clone());
        assert!(p.payload_len() == wire.len());
        let mut out = BytesMut::new();
        p.encode_to(&mut out).unwrap();
        assert!(&out[..] == &wire[..]); // verbatim
        assert!(p.as_v2().is_none()); // Raw is unparsed
    }

    #[test]
    fn from_bytes_dispatches_legacy() {
        // Build a fake legacy MessageSet entry: offset(8) + size(4) + crc(4) +
        // magic(1)=1 + … . Just need byte 16 to be 1.
        let mut buf = vec![0u8; 17];
        buf[16] = 1;
        let p = RecordsPayload::from_bytes(Bytes::from(buf.clone())).unwrap();
        match p {
            RecordsPayload::Legacy(b) => assert!(&b[..] == &buf[..]),
            _ => panic!("expected Legacy"),
        }
    }

    #[test]
    fn roundtrip_v2() {
        let p: RecordsPayload = sample_v2().into();
        let mut buf = BytesMut::new();
        p.encode_to(&mut buf).unwrap();
        let back = RecordsPayload::from_bytes(buf.freeze()).unwrap();
        assert!(p == back);
        assert!(p.payload_len() == back.payload_len());
    }

    #[test]
    fn encode_decode_via_traits() {
        let p: RecordsPayload = sample_v2().into();
        let mut buf = BytesMut::new();
        <RecordsPayload as crate::Encode>::encode(&p, &mut buf, 0).unwrap();
        let mut cur: &[u8] = &buf;
        let back = <RecordsPayload as crate::Decode>::decode(&mut cur, 0).unwrap();
        assert!(p == back);
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
            RecordsPayload::V2(batches) => assert!(batches[0].base_offset == 42),
            _ => panic!("expected V2"),
        }
    }

    #[test]
    fn from_record_batch() {
        let rb = sample_v2();
        let p: RecordsPayload = rb.clone().into();
        assert!(p.as_v2() == Some(&[rb][..]));
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
        assert!(p.payload_len() == bytes.len());
        assert!(p.as_v2().is_none());
        assert!(p.as_legacy() == Some(&bytes));

        let mut out = BytesMut::new();
        p.encode_to(&mut out).unwrap();
        assert!(&out[..] == &bytes[..]);
    }

    #[test]
    fn legacy_roundtrip_via_traits() {
        let bytes = legacy_bytes();
        let p = RecordsPayload::from_bytes(bytes.clone()).unwrap();
        let mut buf = BytesMut::new();
        <RecordsPayload as crate::Encode>::encode(&p, &mut buf, 0).unwrap();
        assert!(<RecordsPayload as crate::Encode>::encoded_len(&p, 0) == bytes.len());
        let mut cur: &[u8] = &buf;
        let back = <RecordsPayload as crate::Decode>::decode(&mut cur, 0).unwrap();
        assert!(matches!(back, RecordsPayload::Legacy(_)));
        assert!(back.as_legacy().unwrap() == &bytes);
    }

    #[test]
    fn owned_default_is_empty_v2() {
        let p = RecordsPayload::default();
        assert!(matches!(p, RecordsPayload::V2(ref v) if v.is_empty()));
    }

    #[test]
    fn looks_like_v2_rejects_short_buffer() {
        // Too short to peek the magic byte at offset 16; must fall through to Legacy.
        let short = Bytes::from_static(&[0u8; 10]);
        let p = RecordsPayload::from_bytes(short.clone()).unwrap();
        assert!(p.as_legacy() == Some(&short));
    }

    #[test]
    fn borrowed_legacy_roundtrip() {
        let bytes = legacy_bytes();
        let p = RecordsPayloadBorrowed::from_slice(&bytes).unwrap();
        assert!(matches!(p, RecordsPayloadBorrowed::Legacy(_)));
        assert!(p.payload_len() == bytes.len());

        let mut out = BytesMut::new();
        p.encode_to(&mut out).unwrap();
        assert!(&out[..] == &bytes[..]);

        let owned = p.to_owned().unwrap();
        match owned {
            RecordsPayload::Legacy(b) => assert!(&b[..] == &bytes[..]),
            _ => panic!("expected Legacy"),
        }
    }

    #[test]
    fn borrowed_v2_payload_len_and_encode() {
        let rb = sample_v2();
        let mut buf = BytesMut::new();
        rb.encode(&mut buf).unwrap();
        let frozen = buf.freeze();
        let p = RecordsPayloadBorrowed::from_slice(&frozen).unwrap();
        assert!(p.payload_len() == frozen.len());

        let mut out = BytesMut::new();
        p.encode_to(&mut out).unwrap();
        assert!(&out[..] == &frozen[..]);
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
        assert!(<RecordsPayloadBorrowed as crate::Encode>::encoded_len(&p, 0) == frozen.len());

        let mut cur: &[u8] = &out;
        let back =
            <RecordsPayloadBorrowed as crate::DecodeBorrow>::decode_borrow(&mut cur, 0).unwrap();
        assert!(matches!(back, RecordsPayloadBorrowed::V2(_)));
    }

    #[test]
    fn borrowed_default_is_empty_v2() {
        let p = RecordsPayloadBorrowed::default();
        assert!(matches!(p, RecordsPayloadBorrowed::V2(ref v) if v.is_empty()));
    }

    #[test]
    fn from_fetch_bytes_drops_incomplete_trailing_batch() {
        // Two complete batches followed by a truncated third (only a few
        // bytes of its header). Kafka sends this when the partition byte
        // budget cuts the final batch; the consumer must keep the two
        // complete batches and drop the fragment.
        let mut b0 = sample_v2();
        b0.base_offset = 0;
        let mut b1 = sample_v2();
        b1.base_offset = 1;
        let mut buf = BytesMut::new();
        b0.encode(&mut buf).unwrap();
        b1.encode(&mut buf).unwrap();
        buf.extend_from_slice(&[0u8; 7]); // partial trailing batch header
        let p = RecordsPayload::from_fetch_bytes(buf.freeze()).unwrap();
        let batches = p.as_v2().expect("v2");
        assert!(batches.len() == 2);
        assert!(batches[0].base_offset == 0);
        assert!(batches[1].base_offset == 1);
    }

    #[test]
    fn from_fetch_bytes_still_errors_on_corrupt_batch() {
        // A complete-looking batch whose CRC is wrong must still error, even
        // leniently — leniency only forgives truncation, not corruption.
        let rb = sample_v2();
        let mut buf = BytesMut::new();
        rb.encode(&mut buf).unwrap();
        let mut bytes = buf.to_vec();
        // Corrupt a body byte after the header (HEADER_LEN = 61) to break CRC.
        bytes[61] ^= 0xFF;
        let err = RecordsPayload::from_fetch_bytes(Bytes::from(bytes)).unwrap_err();
        assert!(matches!(err, RecordsError::CrcMismatch { .. }));
    }

    #[test]
    fn from_fetch_bytes_legacy_passes_through() {
        let bytes = legacy_bytes();
        let p = RecordsPayload::from_fetch_bytes(bytes.clone()).unwrap();
        assert!(p.as_legacy() == Some(&bytes));
    }

    #[test]
    fn from_fetch_bytes_empty_is_empty_v2() {
        // Empty bytes do not carry a magic byte, so looks_like_v2 returns false
        // and from_fetch_bytes yields an empty Legacy payload (no panic, no error).
        let p = RecordsPayload::from_fetch_bytes(Bytes::new()).unwrap();
        assert!(matches!(p, RecordsPayload::Legacy(ref b) if b.is_empty()));
    }
}
