//! Owned `RecordBatch`, `Record`, and `RecordHeader` types.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use zerocopy::FromBytes as _;

use crate::{
    primitives::varint::{
        get_varint, get_varlong, put_varint, put_varlong, varint_len, varlong_len,
    },
    records::{
        RecordsError,
        crc::{crc32c, crc32c_append},
        header::{Attributes, HEADER_LEN},
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecordHeader {
    pub key: String,
    pub value: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Record {
    pub attributes: i8,
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub attributes: Attributes,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

impl Default for RecordBatch {
    fn default() -> Self {
        Self {
            base_offset: 0,
            partition_leader_epoch: 0,
            attributes: Attributes::default(),
            last_offset_delta: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1, // sentinel: non-idempotent
            producer_epoch: -1,
            base_sequence: -1,
            records: Vec::new(),
        }
    }
}

impl Record {
    /// Encode a single record (varlong length prefix + fields) into `buf`.
    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), RecordsError> {
        let body_len = self.body_len();
        put_varlong(
            buf,
            i64::try_from(body_len)
                .map_err(|_| RecordsError::RecordParse("record body length overflow".into()))?,
        );
        self.encode_body(buf)
    }

    /// Predicted total length of this record on the wire (length-prefix + body).
    pub fn encoded_len(&self) -> usize {
        let body = self.body_len();
        #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
        let body_i64 = body as i64;
        varlong_len(body_i64) + body
    }

    fn body_len(&self) -> usize {
        let mut n = 1; // attributes (i8)
        n += varlong_len(self.timestamp_delta);
        n += varint_len(self.offset_delta);
        n += match &self.key {
            None => varint_len(-1),
            Some(k) => varint_len(i32::try_from(k.len()).unwrap_or(i32::MAX)) + k.len(),
        };
        n += match &self.value {
            None => varint_len(-1),
            Some(v) => varint_len(i32::try_from(v.len()).unwrap_or(i32::MAX)) + v.len(),
        };
        n += varint_len(i32::try_from(self.headers.len()).unwrap_or(i32::MAX));
        for h in &self.headers {
            let key_bytes = h.key.as_bytes();
            n += varint_len(i32::try_from(key_bytes.len()).unwrap_or(i32::MAX)) + key_bytes.len();
            n += match &h.value {
                None => varint_len(-1),
                Some(v) => varint_len(i32::try_from(v.len()).unwrap_or(i32::MAX)) + v.len(),
            };
        }
        n
    }

    fn encode_body<B: BufMut>(&self, buf: &mut B) -> Result<(), RecordsError> {
        buf.put_i8(self.attributes);
        put_varlong(buf, self.timestamp_delta);
        put_varint(buf, self.offset_delta);
        match &self.key {
            None => put_varint(buf, -1),
            Some(k) => {
                put_varint(
                    buf,
                    i32::try_from(k.len()).map_err(|_| {
                        RecordsError::RecordParse("record key length overflow".into())
                    })?,
                );
                buf.put_slice(k);
            }
        }
        match &self.value {
            None => put_varint(buf, -1),
            Some(v) => {
                put_varint(
                    buf,
                    i32::try_from(v.len()).map_err(|_| {
                        RecordsError::RecordParse("record value length overflow".into())
                    })?,
                );
                buf.put_slice(v);
            }
        }
        put_varint(
            buf,
            i32::try_from(self.headers.len())
                .map_err(|_| RecordsError::RecordParse("record header count overflow".into()))?,
        );
        for h in &self.headers {
            let key_bytes = h.key.as_bytes();
            put_varint(
                buf,
                i32::try_from(key_bytes.len())
                    .map_err(|_| RecordsError::RecordParse("header key length overflow".into()))?,
            );
            buf.put_slice(key_bytes);
            match &h.value {
                None => put_varint(buf, -1),
                Some(v) => {
                    put_varint(
                        buf,
                        i32::try_from(v.len()).map_err(|_| {
                            RecordsError::RecordParse("header value length overflow".into())
                        })?,
                    );
                    buf.put_slice(v);
                }
            }
        }
        Ok(())
    }

    /// Decode a single record. `buf` must be positioned at the record's
    /// varlong length prefix.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, RecordsError> {
        let body_len = get_varlong(buf)
            .map_err(|e| RecordsError::RecordParse(format!("record length: {e}")))?;
        let body_len = usize::try_from(body_len).map_err(|_| {
            RecordsError::RecordParse(format!("record length negative or too large: {body_len}"))
        })?;
        if buf.remaining() < body_len {
            return Err(RecordsError::BodyTooShort {
                needed: body_len - buf.remaining(),
            });
        }
        // Restrict to body_len bytes so a malformed inner field doesn't run
        // past the record boundary.
        let mut body = buf.take(body_len);
        let r = Self::decode_body(&mut body)?;
        // Trailing bytes inside the record's claimed length — protocol corruption.
        if body.has_remaining() {
            return Err(RecordsError::RecordParse(format!(
                "trailing bytes inside record (left={})",
                body.remaining()
            )));
        }
        Ok(r)
    }

    fn decode_body<B: Buf>(buf: &mut B) -> Result<Self, RecordsError> {
        if buf.remaining() == 0 {
            return Err(RecordsError::RecordParse("record body empty".into()));
        }
        let attributes = buf.get_i8();
        let timestamp_delta = get_varlong(buf)
            .map_err(|e| RecordsError::RecordParse(format!("timestamp_delta: {e}")))?;
        let offset_delta =
            get_varint(buf).map_err(|e| RecordsError::RecordParse(format!("offset_delta: {e}")))?;

        let key = decode_nullable_bytes(buf, "key")?;
        let value = decode_nullable_bytes(buf, "value")?;

        let header_count =
            get_varint(buf).map_err(|e| RecordsError::RecordParse(format!("header_count: {e}")))?;
        if header_count < 0 {
            return Err(RecordsError::RecordParse(format!(
                "negative header count {header_count}"
            )));
        }
        #[allow(clippy::cast_sign_loss)] // checked < 0 above
        let header_count_usize = header_count as usize;
        // Bound pre-allocation: a record header is at least 1 byte on the wire,
        // so an honest `header_count` can never exceed the bytes left in the
        // record body. Clamp the capacity hint to reject huge declared counts
        // without affecting the loop (it stops at EOF anyway).
        let mut headers = Vec::with_capacity(header_count_usize.min(buf.remaining()));
        for i in 0..header_count {
            headers.push(
                decode_record_header(buf)
                    .map_err(|e| RecordsError::RecordParse(format!("header[{i}]: {e}")))?,
            );
        }

        Ok(Self {
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        })
    }
}

fn decode_nullable_bytes<B: Buf>(buf: &mut B, label: &str) -> Result<Option<Bytes>, RecordsError> {
    let len =
        get_varint(buf).map_err(|e| RecordsError::RecordParse(format!("{label} length: {e}")))?;
    if len < 0 {
        Ok(None)
    } else {
        #[allow(clippy::cast_sign_loss)] // checked < 0 above
        let n = len as usize;
        if buf.remaining() < n {
            return Err(RecordsError::BodyTooShort {
                needed: n - buf.remaining(),
            });
        }
        let mut v = vec![0u8; n];
        buf.copy_to_slice(&mut v);
        Ok(Some(Bytes::from(v)))
    }
}

fn decode_record_header<B: Buf>(buf: &mut B) -> Result<RecordHeader, String> {
    let key_len = get_varint(buf).map_err(|e| format!("key length: {e}"))?;
    if key_len < 0 {
        return Err(format!("non-nullable key has negative length {key_len}"));
    }
    #[allow(clippy::cast_sign_loss)] // checked < 0 above
    let n = key_len as usize;
    if buf.remaining() < n {
        return Err(format!("key truncated (need {} more)", n - buf.remaining()));
    }
    let mut kv = vec![0u8; n];
    buf.copy_to_slice(&mut kv);
    let key = String::from_utf8(kv).map_err(|e| format!("key utf-8: {e}"))?;

    let value_len = get_varint(buf).map_err(|e| format!("value length: {e}"))?;
    let value = if value_len < 0 {
        None
    } else {
        #[allow(clippy::cast_sign_loss)] // checked < 0 above
        let n = value_len as usize;
        if buf.remaining() < n {
            return Err(format!(
                "value truncated (need {} more)",
                n - buf.remaining()
            ));
        }
        let mut vv = vec![0u8; n];
        buf.copy_to_slice(&mut vv);
        Some(Bytes::from(vv))
    };

    Ok(RecordHeader { key, value })
}

#[cfg(test)]
mod record_tests {

    use bytes::BytesMut;

    use super::*;

    fn fixture_minimal_record() -> Record {
        Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key: None,
            value: None,
            headers: vec![],
        }
    }

    fn fixture_keyed_record() -> Record {
        Record {
            attributes: 0,
            timestamp_delta: 17,
            offset_delta: 2,
            key: Some(Bytes::from_static(b"the-key")),
            value: Some(Bytes::from_static(b"hello kafka")),
            headers: vec![
                RecordHeader {
                    key: "trace-id".to_string(),
                    value: Some(Bytes::from_static(b"abc")),
                },
                RecordHeader {
                    key: "null-val".to_string(),
                    value: None,
                },
            ],
        }
    }

    fn fixture_large_payload_record() -> Record {
        Record {
            attributes: 0,
            timestamp_delta: 1_000_000,
            offset_delta: 999,
            key: Some(Bytes::from(vec![b'k'; 128])),
            value: Some(Bytes::from(vec![b'v'; 4096])),
            headers: vec![],
        }
    }

    #[test]
    fn record_roundtrip_cases() {
        type TestCase1<'a> = (&'a str, fn() -> Record);
        let cases: [TestCase1<'_>; 3] = [
            ("minimal", fixture_minimal_record),
            ("keyed with headers", fixture_keyed_record),
            ("large payload", fixture_large_payload_record),
        ];
        for (_case, fixture) in cases {
            let record = fixture();
            let mut buf = BytesMut::new();
            record.encode(&mut buf).unwrap();
            assert2::assert!(buf.len() == record.encoded_len());

            let mut cur: &[u8] = &buf[..];
            let decoded = Record::decode(&mut cur).unwrap();
            assert2::assert!((decoded, cur.is_empty()) == (record, true));
        }
    }

    #[test]
    fn decode_rejects_negative_header_count() {
        let mut buf = BytesMut::new();
        // body: attributes(1) + timestamp_delta(1) + offset_delta(1) + key=-1(1)
        //       + value=-1(1) + headers=-1(1) = 6 bytes body
        put_varlong(&mut buf, 6); // body length 6 bytes
        buf.put_i8(0); // attributes
        put_varlong(&mut buf, 0); // timestamp_delta = 0  (1 byte)
        put_varint(&mut buf, 0); // offset_delta = 0     (1 byte)
        put_varint(&mut buf, -1); // key len               (1 byte)
        put_varint(&mut buf, -1); // value len             (1 byte)
        put_varint(&mut buf, -1); // negative header count (1 byte)

        let mut cur: &[u8] = &buf[..];
        match Record::decode(&mut cur) {
            Err(RecordsError::RecordParse(msg)) => {
                assert2::assert!(msg.contains("negative header count"));
            }
            other => panic!("expected RecordParse, got {other:?}"),
        }
    }

    #[test]
    fn decode_huge_header_count_does_not_overallocate() {
        // A record that declares ~1 billion headers but supplies none. The
        // capacity hint must be clamped to the (tiny) body remaining, and the
        // decode must fail cleanly on EOF rather than attempting a multi-GB
        // allocation.
        let mut inner = BytesMut::new();
        inner.put_i8(0); // attributes
        put_varlong(&mut inner, 0); // timestamp_delta
        put_varint(&mut inner, 0); // offset_delta
        put_varint(&mut inner, -1); // key len = null
        put_varint(&mut inner, -1); // value len = null
        put_varint(&mut inner, 1_000_000_000); // absurd header count

        let mut buf = BytesMut::new();
        put_varlong(&mut buf, i64::try_from(inner.len()).unwrap());
        buf.extend_from_slice(&inner);

        let mut cur: &[u8] = &buf[..];
        // Must return an Err (EOF reading the first header), not OOM.
        assert2::assert!(Record::decode(&mut cur).is_err());
    }
}

impl RecordBatch {
    /// KIP-534 delete horizon, if the delete-horizon attribute bit is set.
    /// `base_timestamp` is repurposed to carry it (no separate wire field).
    #[must_use]
    pub fn delete_horizon_ms(&self) -> Option<i64> {
        self.attributes
            .has_delete_horizon()
            .then_some(self.base_timestamp)
    }

    /// Stamp the delete horizon: set bit 6, move the horizon into
    /// `base_timestamp`, and rewrite every record's `timestamp_delta` so
    /// reconstructed absolute timestamps (`base_timestamp + delta`) are
    /// unchanged.
    #[must_use]
    pub fn with_delete_horizon(mut self, horizon_ms: i64) -> Self {
        let old_base = self.base_timestamp;
        for r in &mut self.records {
            // Reconstruct the original absolute timestamp, then re-base it onto
            // the new `base_timestamp`. Deltas are `i64`, so absolute timestamps
            // round-trip exactly across re-basing; `saturating_*` only guards
            // pathological inputs from panicking.
            let abs = old_base.saturating_add(r.timestamp_delta);
            r.timestamp_delta = abs.saturating_sub(horizon_ms);
        }
        self.base_timestamp = horizon_ms;
        self.attributes = self.attributes.with_delete_horizon(true);
        self
    }

    /// Decode a complete v2 record batch from `buf`. Reads from the start of
    /// the header.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, RecordsError> {
        // batch_length field semantics: bytes after itself.
        // Header tail = partition_leader_epoch(4) + magic(1) + crc(4) +
        //   attributes(2) + last_offset_delta(4) + base_timestamp(8) +
        //   max_timestamp(8) + producer_id(8) + producer_epoch(2) +
        //   base_sequence(4) + records_count(4) = 49 bytes.
        const HEADER_TAIL_LEN: i32 = 49;

        // Need the full header before doing anything.
        if buf.remaining() < HEADER_LEN {
            return Err(RecordsError::HeaderTooShort {
                needed: HEADER_LEN - buf.remaining(),
            });
        }
        // Copy out the header to a stack buffer so we can use zerocopy.
        let mut hdr_bytes = [0u8; HEADER_LEN];
        buf.copy_to_slice(&mut hdr_bytes);

        let hdr = crate::records::header::RecordBatchHeader::ref_from_bytes(&hdr_bytes[..])
            .map_err(|_| RecordsError::ZerocopyFailure)?;

        if hdr.magic != 2 {
            return Err(RecordsError::UnsupportedMagic { found: hdr.magic });
        }

        // body_len = batch_length - HEADER_TAIL_LEN
        let body_len = i32::checked_sub(hdr.batch_length.get(), HEADER_TAIL_LEN)
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| {
                RecordsError::RecordParse("negative or oversized batch_length".into())
            })?;

        if buf.remaining() < body_len {
            return Err(RecordsError::BodyTooShort {
                needed: body_len - buf.remaining(),
            });
        }

        // Read the (possibly compressed) body.
        let mut body = vec![0u8; body_len];
        buf.copy_to_slice(&mut body);

        // CRC is computed over: header bytes 21..HEADER_LEN (attributes through
        // records_count), then the body bytes.
        let expected_crc = hdr.crc.get();
        let mut computed = crc32c(&hdr_bytes[21..HEADER_LEN]);
        computed = crc32c_append(computed, &body);
        if computed != expected_crc {
            return Err(RecordsError::CrcMismatch {
                expected: expected_crc,
                computed,
            });
        }

        let attributes = Attributes(hdr.attributes.get());
        let codec = attributes.compression();

        // Decompress body if needed.
        let body_for_records: Bytes = if codec == crabka_compression::CompressionType::None {
            Bytes::from(body)
        } else {
            // Bound decompressed output: generous vs. legit ratios, but finite.
            // A small compressed batch must not be able to expand to gigabytes
            // and OOM the broker (decompression bomb).
            const DECOMPRESS_MIN_CAP: usize = 16 * 1024 * 1024; // 16 MiB floor (small inputs)
            const DECOMPRESS_MAX_RATIO: usize = 100; // ≤100x the compressed size
            const DECOMPRESS_ABSOLUTE_CEILING: usize = 1024 * 1024 * 1024; // 1 GiB hard ceiling
            let max_output = body
                .len()
                .saturating_mul(DECOMPRESS_MAX_RATIO)
                .clamp(DECOMPRESS_MIN_CAP, DECOMPRESS_ABSOLUTE_CEILING);
            crabka_compression::decompress(codec, &body, max_output)?
        };

        // Parse records.
        let count = hdr.records_count.get();
        if count < 0 {
            return Err(RecordsError::RecordParse(format!(
                "negative records_count {count}"
            )));
        }
        let mut body_cur: &[u8] = &body_for_records[..];
        // Bound pre-allocation: each record is at least 1 byte in the
        // decompressed body, so an honest `records_count` can never exceed the
        // body length. Clamp the capacity hint to reject huge declared counts.
        #[allow(clippy::cast_sign_loss)] // checked < 0 above
        let mut records = Vec::with_capacity((count as usize).min(body_for_records.len()));
        for i in 0..count {
            // Parse the record into borrowed slices, then materialise its
            // key / value / header-values as zero-copy `Bytes` views into
            // `body_for_records`. The whole batch shares that single backing
            // allocation instead of copying every field into its own `Bytes`,
            // which removes ~2 heap allocations + memcpys per record on the
            // common (header-free) path.
            let r = crate::records::borrowed::parse_one_record(&mut body_cur)
                .map_err(|e| RecordsError::RecordParse(format!("record[{i}]: {e}")))?;
            records.push(Record {
                attributes: r.attributes,
                timestamp_delta: r.timestamp_delta,
                offset_delta: r.offset_delta,
                key: r.key.map(|s| body_for_records.slice_ref(s)),
                value: r.value.map(|s| body_for_records.slice_ref(s)),
                headers: r
                    .headers
                    .into_iter()
                    .map(|h| RecordHeader {
                        key: h.key.to_string(),
                        value: h.value.map(|s| body_for_records.slice_ref(s)),
                    })
                    .collect(),
            });
        }
        if !body_cur.is_empty() {
            return Err(RecordsError::RecordParse(format!(
                "trailing bytes after records (left={})",
                body_cur.len()
            )));
        }

        Ok(Self {
            base_offset: hdr.base_offset.get(),
            partition_leader_epoch: hdr.partition_leader_epoch.get(),
            attributes,
            last_offset_delta: hdr.last_offset_delta.get(),
            base_timestamp: hdr.base_timestamp.get(),
            max_timestamp: hdr.max_timestamp.get(),
            producer_id: hdr.producer_id.get(),
            producer_epoch: hdr.producer_epoch.get(),
            base_sequence: hdr.base_sequence.get(),
            records,
        })
    }

    /// Encode this batch into `buf`.
    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), RecordsError> {
        const HEADER_TAIL_LEN: i32 = 49;

        // 1. Encode records into a temporary buffer.
        let mut raw_body =
            BytesMut::with_capacity(self.records.iter().map(Record::encoded_len).sum());
        for r in &self.records {
            r.encode(&mut raw_body)?;
        }
        let raw_body = raw_body.freeze();

        // 2. Compress if needed.
        let codec = self.attributes.compression();
        let body: Bytes = if codec == crabka_compression::CompressionType::None {
            raw_body
        } else {
            crabka_compression::compress(codec, &raw_body)?
        };

        // 3. batch_length = HEADER_TAIL_LEN + body_len
        let batch_length = HEADER_TAIL_LEN
            + i32::try_from(body.len())
                .map_err(|_| RecordsError::RecordParse("body length exceeds i32".into()))?;

        // 4. Build the CRC-covered header portion (attributes through records_count = 40 bytes).
        let mut covered = BytesMut::with_capacity(40);
        covered.put_i16(self.attributes.0);
        covered.put_i32(self.last_offset_delta);
        covered.put_i64(self.base_timestamp);
        covered.put_i64(self.max_timestamp);
        covered.put_i64(self.producer_id);
        covered.put_i16(self.producer_epoch);
        covered.put_i32(self.base_sequence);
        covered.put_i32(
            i32::try_from(self.records.len())
                .map_err(|_| RecordsError::RecordParse("records_count exceeds i32".into()))?,
        );
        let covered_head = covered.freeze();

        // 5. Compute CRC over covered_head then body.
        let mut crc = crc32c(&covered_head);
        crc = crc32c_append(crc, &body);

        // 6. Emit the full header then body.
        buf.put_i64(self.base_offset);
        buf.put_i32(batch_length);
        buf.put_i32(self.partition_leader_epoch);
        buf.put_i8(2); // magic v2
        buf.put_u32(crc);
        buf.put_slice(&covered_head);
        buf.put_slice(&body);
        Ok(())
    }

    /// Predicted total bytes that `encode` will write (uncompressed; for
    /// compressed batches the actual size will differ).
    pub fn encoded_len(&self) -> usize {
        let body: usize = self.records.iter().map(Record::encoded_len).sum();
        HEADER_LEN + body
    }
}

#[cfg(test)]
mod batch_tests {
    use assert2::check;
    use crabka_compression::CompressionType;

    use super::*;

    fn fixture_empty_batch() -> RecordBatch {
        RecordBatch::default()
    }

    fn fixture_single_record_batch() -> RecordBatch {
        RecordBatch {
            records: vec![Record {
                key: Some(Bytes::from_static(b"k1")),
                value: Some(Bytes::from_static(b"v1")),
                ..Default::default()
            }],
            ..RecordBatch::default()
        }
    }

    fn fixture_multi_record_batch() -> RecordBatch {
        RecordBatch {
            base_offset: 42,
            partition_leader_epoch: 5,
            last_offset_delta: 2,
            base_timestamp: 1_700_000_000,
            max_timestamp: 1_700_000_500,
            producer_id: 100,
            producer_epoch: 3,
            base_sequence: 7,
            records: vec![
                Record {
                    offset_delta: 0,
                    timestamp_delta: 0,
                    key: Some(Bytes::from_static(b"a")),
                    value: Some(Bytes::from_static(b"1")),
                    ..Default::default()
                },
                Record {
                    offset_delta: 1,
                    timestamp_delta: 100,
                    key: Some(Bytes::from_static(b"b")),
                    value: Some(Bytes::from_static(b"2")),
                    ..Default::default()
                },
                Record {
                    offset_delta: 2,
                    timestamp_delta: 500,
                    key: None,
                    value: Some(Bytes::from_static(b"3")),
                    headers: vec![RecordHeader {
                        key: "h".to_string(),
                        value: Some(Bytes::from_static(b"hv")),
                    }],
                    ..Default::default()
                },
            ],
            ..RecordBatch::default()
        }
    }

    #[test]
    fn uncompressed_roundtrip_cases() {
        type TestCase2 = (&'static str, fn() -> RecordBatch);
        let cases: [TestCase2; 3] = [
            ("empty", fixture_empty_batch),
            ("single", fixture_single_record_batch),
            ("multiple", fixture_multi_record_batch),
        ];
        for (_case, fixture) in cases {
            let mut batch = fixture();
            batch.attributes = batch.attributes.with_compression(CompressionType::None);
            let mut buf = BytesMut::new();
            batch.encode(&mut buf).unwrap();
            assert2::assert!(buf.len() == batch.encoded_len());
            let mut cur: &[u8] = &buf[..];
            let decoded = RecordBatch::decode(&mut cur).unwrap();
            assert2::assert!((decoded, cur.is_empty()) == (batch, true));
        }
    }

    #[test]
    fn rejects_pre_v2_magic() {
        let mut buf = BytesMut::new();
        buf.put_i64(0); // base_offset
        buf.put_i32(49); // batch_length
        buf.put_i32(0); // partition_leader_epoch
        buf.put_i8(1); // magic = 1 (v1, deprecated)
        buf.put_u32(0); // crc (irrelevant; we reject on magic first)
        for _ in 21..HEADER_LEN {
            buf.put_u8(0);
        }
        let mut cur: &[u8] = &buf[..];
        assert2::assert!(matches!(
            RecordBatch::decode(&mut cur),
            Err(RecordsError::UnsupportedMagic { found: 1 })
        ));
    }

    #[test]
    fn rejects_bad_crc() {
        let b = fixture_single_record_batch();
        let mut buf = BytesMut::new();
        b.encode(&mut buf).unwrap();
        // Corrupt the CRC bytes (offsets 17..21).
        buf[17] ^= 0xFF;
        let mut cur: &[u8] = &buf[..];
        assert2::assert!(matches!(
            RecordBatch::decode(&mut cur),
            Err(RecordsError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn compressed_roundtrip_cases() {
        for (_case, codec) in [
            ("gzip", CompressionType::Gzip),
            ("snappy", CompressionType::Snappy),
            ("lz4", CompressionType::Lz4),
            ("zstd", CompressionType::Zstd),
        ] {
            let mut batch = fixture_multi_record_batch();
            batch.attributes = batch.attributes.with_compression(codec);
            let mut buf = BytesMut::new();
            batch.encode(&mut buf).unwrap();
            let mut cur: &[u8] = &buf[..];
            let decoded = RecordBatch::decode(&mut cur).unwrap();
            assert2::assert!((decoded, cur.is_empty()) == (batch, true));
        }
    }

    #[test]
    fn with_delete_horizon_stamps_and_preserves_record_timestamps() {
        // base 1000, two records at deltas [0, 5] → absolutes [1000, 1005].
        let b = RecordBatch {
            base_timestamp: 1000,
            records: vec![
                Record {
                    timestamp_delta: 0,
                    ..Default::default()
                },
                Record {
                    timestamp_delta: 5,
                    ..Default::default()
                },
            ],
            ..RecordBatch::default()
        };

        let stamped = b.with_delete_horizon(9999);

        check!(stamped.attributes.has_delete_horizon());
        check!(stamped.base_timestamp == 9999);
        check!(stamped.delete_horizon_ms() == Some(9999));

        // Reconstructed absolutes (base + delta) must equal the ORIGINALS.
        let absolutes: Vec<i64> = stamped
            .records
            .iter()
            .map(|r| stamped.base_timestamp + r.timestamp_delta)
            .collect();
        assert2::assert!(absolutes == vec![1000, 1005]);
    }

    #[test]
    fn delete_horizon_round_trips_through_encode_decode() {
        // base 1000, two keyed records at deltas [0, 5] → absolutes [1000, 1005].
        let b = RecordBatch {
            base_timestamp: 1000,
            last_offset_delta: 1,
            records: vec![
                Record {
                    timestamp_delta: 0,
                    offset_delta: 0,
                    key: Some(Bytes::from_static(b"k1")),
                    value: Some(Bytes::from_static(b"v1")),
                    ..Default::default()
                },
                Record {
                    timestamp_delta: 5,
                    offset_delta: 1,
                    key: Some(Bytes::from_static(b"k2")),
                    value: Some(Bytes::from_static(b"v2")),
                    ..Default::default()
                },
            ],
            ..RecordBatch::default()
        }
        .with_delete_horizon(9999);

        let mut buf = BytesMut::new();
        b.encode(&mut buf).unwrap();

        let mut cur: &[u8] = &buf[..];
        let decoded = RecordBatch::decode(&mut cur).unwrap();
        assert2::assert!(cur.is_empty());

        assert2::assert!(decoded.delete_horizon_ms() == Some(9999));

        let absolutes: Vec<i64> = decoded
            .records
            .iter()
            .map(|r| decoded.base_timestamp + r.timestamp_delta)
            .collect();
        assert2::assert!(absolutes == vec![1000, 1005]);
    }

    #[test]
    fn decode_huge_records_count_does_not_overallocate() {
        // Encode an empty uncompressed batch, then overwrite records_count with
        // an absurd value (~1 billion) and fix up the CRC. The capacity hint
        // must be clamped to the (empty) decompressed body, and decode must
        // fail cleanly on EOF rather than attempting a multi-GB allocation.
        let mut b = fixture_empty_batch();
        b.attributes = b.attributes.with_compression(CompressionType::None);
        let mut buf = BytesMut::new();
        b.encode(&mut buf).unwrap();

        // records_count is the last 4 bytes of the fixed header.
        let rc_off = HEADER_LEN - 4;
        buf[rc_off..HEADER_LEN].copy_from_slice(&1_000_000_000i32.to_be_bytes());

        // Recompute CRC over header[21..HEADER_LEN] + body (body is empty here).
        let body = &buf[HEADER_LEN..];
        let mut computed = crc32c(&buf[21..HEADER_LEN]);
        computed = crc32c_append(computed, body);
        buf[17..21].copy_from_slice(&computed.to_be_bytes());

        let mut cur: &[u8] = &buf[..];
        // Must return an Err (EOF reading the first record), not OOM.
        assert2::assert!(RecordBatch::decode(&mut cur).is_err());
    }
}

impl crate::Encode for RecordBatch {
    fn encode<B: BufMut>(&self, buf: &mut B, _version: i16) -> Result<(), crate::ProtocolError> {
        RecordBatch::encode(self, buf).map_err(Into::into)
    }

    fn encoded_len(&self, _version: i16) -> usize {
        RecordBatch::encoded_len(self)
    }
}

impl crate::Decode<'_> for RecordBatch {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<Self, crate::ProtocolError> {
        RecordBatch::decode(buf).map_err(Into::into)
    }
}
