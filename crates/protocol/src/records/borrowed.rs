//! Borrowed `RecordBatch<'a>`, `Record<'a>`, and `RecordHeader<'a>`.

use bytes::Bytes;
use zerocopy::FromBytes as _;

use crate::primitives::varint::{get_varint, get_varlong};
use crate::records::RecordsError;
use crate::records::crc::{crc32c, crc32c_append};
use crate::records::header::{Attributes, HEADER_LEN, RecordBatchHeader};

// batch_length field semantics: bytes after itself.
// Header tail = partition_leader_epoch(4) + magic(1) + crc(4) +
//   attributes(2) + last_offset_delta(4) + base_timestamp(8) +
//   max_timestamp(8) + producer_id(8) + producer_epoch(2) +
//   base_sequence(4) + records_count(4) = 49 bytes.
const HEADER_TAIL_LEN: i32 = 49;

pub struct RecordBatch<'a> {
    pub(crate) header: &'a RecordBatchHeader,
    pub(crate) body: RecordBody<'a>,
}

pub(crate) enum RecordBody<'a> {
    Borrowed(&'a [u8]),
    Owned(Bytes),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record<'a> {
    pub attributes: i8,
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: Vec<RecordHeader<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader<'a> {
    pub key: &'a str,
    pub value: Option<&'a [u8]>,
}

impl RecordBatch<'_> {
    #[must_use]
    pub fn header(&self) -> &RecordBatchHeader {
        self.header
    }

    #[must_use]
    pub fn attributes(&self) -> Attributes {
        Attributes(self.header.attributes.get())
    }
}

// ── Decode ────────────────────────────────────────────────────────────────────

impl<'de> crate::DecodeBorrow<'de> for RecordBatch<'de> {
    fn decode_borrow(buf: &mut &'de [u8], _version: i16) -> Result<Self, crate::ProtocolError> {
        decode_borrow_impl(buf).map_err(Into::into)
    }
}

fn decode_borrow_impl<'de>(buf: &mut &'de [u8]) -> Result<RecordBatch<'de>, RecordsError> {
    if buf.len() < HEADER_LEN {
        return Err(RecordsError::HeaderTooShort {
            needed: HEADER_LEN - buf.len(),
        });
    }
    // Split off the header slice — both slices remain tied to 'de.
    let (hdr_slice, rest) = buf.split_at(HEADER_LEN);
    let hdr: &'de RecordBatchHeader =
        RecordBatchHeader::ref_from_bytes(hdr_slice).map_err(|_| RecordsError::ZerocopyFailure)?;
    if hdr.magic != 2 {
        return Err(RecordsError::UnsupportedMagic { found: hdr.magic });
    }

    let body_len = i32::checked_sub(hdr.batch_length.get(), HEADER_TAIL_LEN)
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| RecordsError::RecordParse("negative or oversized batch_length".into()))?;

    if rest.len() < body_len {
        return Err(RecordsError::BodyTooShort {
            needed: body_len - rest.len(),
        });
    }
    let (raw_body, after) = rest.split_at(body_len);
    *buf = after;

    // CRC: hash header[21..HEADER_LEN] (attributes through records_count)
    // then append the raw_body bytes.
    let expected = hdr.crc.get();
    let mut computed = crc32c(&hdr_slice[21..HEADER_LEN]);
    computed = crc32c_append(computed, raw_body);
    if computed != expected {
        return Err(RecordsError::CrcMismatch { expected, computed });
    }

    let attributes = Attributes(hdr.attributes.get());
    let codec = attributes.compression();
    let body = if codec == crabka_compression::CompressionType::None {
        RecordBody::Borrowed(raw_body)
    } else {
        let decompressed = crabka_compression::decompress(codec, raw_body)?;
        RecordBody::Owned(decompressed)
    };

    Ok(RecordBatch { header: hdr, body })
}

// ── Iteration ─────────────────────────────────────────────────────────────────

impl RecordBatch<'_> {
    /// Iterate over records, parsing each lazily.
    ///
    /// The returned `Record<'b>` items borrow from `self` (lifetime `'b`),
    /// not from the original input buffer. For uncompressed batches the
    /// backing memory is the input buffer; for compressed batches it is the
    /// batch's internal decompressed `Bytes`.
    pub fn iter(&self) -> RecordIter<'_> {
        let body: &[u8] = match &self.body {
            RecordBody::Borrowed(s) => s,
            RecordBody::Owned(b) => b.as_ref(),
        };
        #[allow(clippy::cast_sign_loss)] // guarded by .max(0) above
        let count = self.header.records_count.get().max(0) as usize;
        RecordIter {
            remaining: body,
            count,
            index: 0,
        }
    }
}

impl<'a> IntoIterator for &'a RecordBatch<'_> {
    type Item = Result<Record<'a>, RecordsError>;
    type IntoIter = RecordIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct RecordIter<'a> {
    remaining: &'a [u8],
    count: usize,
    index: usize,
}

impl<'a> Iterator for RecordIter<'a> {
    type Item = Result<Record<'a>, RecordsError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.count {
            return None;
        }
        self.index += 1;
        Some(parse_one_record(&mut self.remaining))
    }
}

fn parse_one_record<'a>(buf: &mut &'a [u8]) -> Result<Record<'a>, RecordsError> {
    let body_len =
        get_varlong(buf).map_err(|e| RecordsError::RecordParse(format!("record length: {e}")))?;
    let body_len = usize::try_from(body_len).map_err(|_| {
        RecordsError::RecordParse(format!("record length negative or too large: {body_len}"))
    })?;
    if buf.len() < body_len {
        return Err(RecordsError::BodyTooShort {
            needed: body_len - buf.len(),
        });
    }
    let (body, rest) = buf.split_at(body_len);
    *buf = rest;
    let mut body_cur = body;
    let r = parse_body(&mut body_cur)?;
    if !body_cur.is_empty() {
        return Err(RecordsError::RecordParse(format!(
            "trailing bytes inside record (left={})",
            body_cur.len()
        )));
    }
    Ok(r)
}

fn parse_body<'a>(buf: &mut &'a [u8]) -> Result<Record<'a>, RecordsError> {
    if buf.is_empty() {
        return Err(RecordsError::RecordParse("record body empty".into()));
    }
    #[allow(clippy::cast_possible_wrap)] // intentional: Kafka attributes are i8 on the wire
    let attributes = buf[0] as i8;
    *buf = &buf[1..];
    let timestamp_delta =
        get_varlong(buf).map_err(|e| RecordsError::RecordParse(format!("timestamp_delta: {e}")))?;
    let offset_delta =
        get_varint(buf).map_err(|e| RecordsError::RecordParse(format!("offset_delta: {e}")))?;

    let key = read_nullable_slice(buf, "key")?;
    let value = read_nullable_slice(buf, "value")?;

    let header_count =
        get_varint(buf).map_err(|e| RecordsError::RecordParse(format!("header_count: {e}")))?;
    if header_count < 0 {
        return Err(RecordsError::RecordParse(format!(
            "negative header count {header_count}"
        )));
    }
    #[allow(clippy::cast_sign_loss)] // guarded by < 0 check above
    let mut headers = Vec::with_capacity(header_count as usize);
    for i in 0..header_count {
        let key_len = get_varint(buf)
            .map_err(|e| RecordsError::RecordParse(format!("header[{i}] key length: {e}")))?;
        if key_len < 0 {
            return Err(RecordsError::RecordParse(format!(
                "header[{i}] negative key length"
            )));
        }
        #[allow(clippy::cast_sign_loss)] // guarded by < 0 check above
        let n = key_len as usize;
        if buf.len() < n {
            return Err(RecordsError::BodyTooShort {
                needed: n - buf.len(),
            });
        }
        let (key_bytes, rest) = buf.split_at(n);
        *buf = rest;
        let key_str = std::str::from_utf8(key_bytes)
            .map_err(|e| RecordsError::RecordParse(format!("header[{i}] key utf-8: {e}")))?;

        let value = read_nullable_slice(buf, &format!("header[{i}] value"))?;
        headers.push(RecordHeader {
            key: key_str,
            value,
        });
    }

    Ok(Record {
        attributes,
        timestamp_delta,
        offset_delta,
        key,
        value,
        headers,
    })
}

fn read_nullable_slice<'a>(
    buf: &mut &'a [u8],
    label: &str,
) -> Result<Option<&'a [u8]>, RecordsError> {
    let len =
        get_varint(buf).map_err(|e| RecordsError::RecordParse(format!("{label} length: {e}")))?;
    if len < 0 {
        Ok(None)
    } else {
        #[allow(clippy::cast_sign_loss)] // guarded by < 0 check above
        let n = len as usize;
        if buf.len() < n {
            return Err(RecordsError::BodyTooShort {
                needed: n - buf.len(),
            });
        }
        let (head, rest) = buf.split_at(n);
        *buf = rest;
        Ok(Some(head))
    }
}

// ── to_owned bridge ───────────────────────────────────────────────────────────

impl RecordBatch<'_> {
    /// Materialise an owned `RecordBatch` by copying every byte slice into
    /// `Bytes` / `String`.
    pub fn to_owned(&self) -> Result<super::owned::RecordBatch, RecordsError> {
        let mut records = Vec::new();
        for r in self {
            let r = r?;
            records.push(super::owned::Record {
                attributes: r.attributes,
                timestamp_delta: r.timestamp_delta,
                offset_delta: r.offset_delta,
                key: r.key.map(Bytes::copy_from_slice),
                value: r.value.map(Bytes::copy_from_slice),
                headers: r
                    .headers
                    .into_iter()
                    .map(|h| super::owned::RecordHeader {
                        key: h.key.to_string(),
                        value: h.value.map(Bytes::copy_from_slice),
                    })
                    .collect(),
            });
        }
        Ok(super::owned::RecordBatch {
            base_offset: self.header.base_offset.get(),
            partition_leader_epoch: self.header.partition_leader_epoch.get(),
            attributes: self.attributes(),
            last_offset_delta: self.header.last_offset_delta.get(),
            base_timestamp: self.header.base_timestamp.get(),
            max_timestamp: self.header.max_timestamp.get(),
            producer_id: self.header.producer_id.get(),
            producer_epoch: self.header.producer_epoch.get(),
            base_sequence: self.header.base_sequence.get(),
            records,
        })
    }
}

// ── Debug / Clone / PartialEq / Eq ────────────────────────────────────────────
//
// RecordBatch<'a> holds a header reference and a body slice/bytes, so we can't
// #[derive] these. We provide hand-rolled impls that delegate to the owned type.

impl std::fmt::Debug for RecordBatch<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.to_owned() {
            Ok(o) => o.fmt(f),
            Err(e) => write!(f, "RecordBatch(<decode error: {e}>)"),
        }
    }
}

impl Clone for RecordBatch<'_> {
    /// Shallow clone: both `header` and `body` share the same underlying
    /// data as `self`.  For a `Borrowed` body this is a reference copy;
    /// for an `Owned` body, `Bytes::clone` is a cheap reference-count bump.
    fn clone(&self) -> Self {
        RecordBatch {
            header: self.header,
            body: match &self.body {
                RecordBody::Borrowed(s) => RecordBody::Borrowed(s),
                RecordBody::Owned(b) => RecordBody::Owned(b.clone()),
            },
        }
    }
}

impl PartialEq for RecordBatch<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self.to_owned(), other.to_owned()) {
            (Ok(a), Ok(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for RecordBatch<'_> {}

// ── Encode trait impl ─────────────────────────────────────────────────────────

impl crate::Encode for RecordBatch<'_> {
    fn encode<B: bytes::BufMut>(
        &self,
        buf: &mut B,
        version: i16,
    ) -> Result<(), crate::ProtocolError> {
        let owned = self.to_owned().map_err(crate::ProtocolError::from)?;
        crate::Encode::encode(&owned, buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        match self.to_owned() {
            Ok(o) => crate::Encode::encoded_len(&o, version),
            Err(_) => 0,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DecodeBorrow;
    use bytes::BytesMut;
    use crabka_compression::CompressionType;

    fn encode_owned_then_borrow(b: &super::super::owned::RecordBatch) -> Vec<u8> {
        let mut buf = BytesMut::new();
        b.encode(&mut buf).unwrap();
        buf.to_vec()
    }

    macro_rules! borrowed_roundtrip {
        ($name:ident, $codec:expr) => {
            #[test]
            fn $name() {
                let mut owned = super::super::owned::RecordBatch::default();
                owned.attributes = owned.attributes.with_compression($codec);
                owned.records.push(super::super::owned::Record {
                    key: Some(Bytes::from_static(b"key")),
                    value: Some(Bytes::from_static(b"value")),
                    ..Default::default()
                });

                let encoded = encode_owned_then_borrow(&owned);
                let mut cur: &[u8] = &encoded[..];
                let borrowed = RecordBatch::decode_borrow(&mut cur, 0).unwrap();
                assert!(cur.is_empty());
                assert_eq!(borrowed.attributes(), owned.attributes);

                let records: Vec<_> = borrowed.iter().collect::<Result<_, _>>().unwrap();
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].key, Some(b"key".as_slice()));
                assert_eq!(records[0].value, Some(b"value".as_slice()));

                let back_owned = borrowed.to_owned().unwrap();
                assert_eq!(back_owned, owned);
            }
        };
    }

    borrowed_roundtrip!(roundtrip_none, CompressionType::None);
    borrowed_roundtrip!(roundtrip_gzip, CompressionType::Gzip);
    borrowed_roundtrip!(roundtrip_snappy, CompressionType::Snappy);
    borrowed_roundtrip!(roundtrip_lz4, CompressionType::Lz4);
    borrowed_roundtrip!(roundtrip_zstd, CompressionType::Zstd);

    #[test]
    fn zero_copy_for_uncompressed() {
        // Pointer-identity: record key/value slices must point into the
        // input buffer for uncompressed batches.
        let mut owned = super::super::owned::RecordBatch::default();
        owned.records.push(super::super::owned::Record {
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        });
        let encoded = encode_owned_then_borrow(&owned);
        let encoded_start = encoded.as_ptr() as usize;
        let encoded_end = encoded_start + encoded.len();

        let mut cur: &[u8] = &encoded[..];
        let borrowed = RecordBatch::decode_borrow(&mut cur, 0).unwrap();
        let records: Vec<_> = borrowed.iter().collect::<Result<_, _>>().unwrap();

        let v_ptr = records[0].value.unwrap().as_ptr() as usize;
        assert!(
            v_ptr >= encoded_start && v_ptr < encoded_end,
            "value slice does not point into the input buffer: \
             input range [{encoded_start:#x}, {encoded_end:#x}), value ptr {v_ptr:#x}",
        );
    }

    #[test]
    fn borrowed_encode_via_trait_roundtrips() {
        use crate::Encode as _;
        let owned_in = super::super::owned::RecordBatch {
            records: vec![super::super::owned::Record {
                key: Some(Bytes::from_static(b"x")),
                value: Some(Bytes::from_static(b"y")),
                ..Default::default()
            }],
            ..Default::default()
        };
        let bytes_in = encode_owned_then_borrow(&owned_in);
        let mut cur: &[u8] = &bytes_in[..];
        let borrowed = RecordBatch::decode_borrow(&mut cur, 0).unwrap();

        let mut out = BytesMut::new();
        borrowed.encode(&mut out, 0).unwrap();
        assert_eq!(&out[..], &bytes_in[..]);
    }
}
