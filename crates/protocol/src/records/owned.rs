//! Owned `RecordBatch`, `Record`, and `RecordHeader` types.

use bytes::{Buf, BufMut, Bytes};

use crate::primitives::varint::{get_varint, get_varlong, put_varint, put_varlong, varint_len, varlong_len};
use crate::records::header::Attributes;
use crate::records::RecordsError;

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
            producer_id: -1,    // sentinel: non-idempotent
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
            i64::try_from(body_len).map_err(|_| {
                RecordsError::RecordParse("record body length overflow".into())
            })?,
        );
        self.encode_body(buf)
    }

    /// Predicted total length of this record on the wire (length-prefix + body).
    pub fn encoded_len(&self) -> usize {
        let body = self.body_len();
        varlong_len(body as i64) + body
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
            i32::try_from(self.headers.len()).map_err(|_| {
                RecordsError::RecordParse("record header count overflow".into())
            })?,
        );
        for h in &self.headers {
            let key_bytes = h.key.as_bytes();
            put_varint(
                buf,
                i32::try_from(key_bytes.len()).map_err(|_| {
                    RecordsError::RecordParse("header key length overflow".into())
                })?,
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
            RecordsError::RecordParse(format!(
                "record length negative or too large: {body_len}"
            ))
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
        let offset_delta = get_varint(buf)
            .map_err(|e| RecordsError::RecordParse(format!("offset_delta: {e}")))?;

        let key = decode_nullable_bytes(buf, "key")?;
        let value = decode_nullable_bytes(buf, "value")?;

        let header_count = get_varint(buf)
            .map_err(|e| RecordsError::RecordParse(format!("header_count: {e}")))?;
        if header_count < 0 {
            return Err(RecordsError::RecordParse(format!(
                "negative header count {header_count}"
            )));
        }
        let mut headers = Vec::with_capacity(header_count as usize);
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

fn decode_nullable_bytes<B: Buf>(
    buf: &mut B,
    label: &str,
) -> Result<Option<Bytes>, RecordsError> {
    let len = get_varint(buf)
        .map_err(|e| RecordsError::RecordParse(format!("{label} length: {e}")))?;
    if len < 0 {
        Ok(None)
    } else {
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
        return Err(format!(
            "non-nullable key has negative length {key_len}"
        ));
    }
    let n = key_len as usize;
    if buf.remaining() < n {
        return Err(format!(
            "key truncated (need {} more)",
            n - buf.remaining()
        ));
    }
    let mut kv = vec![0u8; n];
    buf.copy_to_slice(&mut kv);
    let key = String::from_utf8(kv).map_err(|e| format!("key utf-8: {e}"))?;

    let value_len = get_varint(buf).map_err(|e| format!("value length: {e}"))?;
    let value = if value_len < 0 {
        None
    } else {
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
    use super::*;
    use bytes::BytesMut;

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

    macro_rules! roundtrip {
        ($name:ident, $fixture:ident) => {
            #[test]
            fn $name() {
                let r = $fixture();
                let mut buf = BytesMut::new();
                r.encode(&mut buf).unwrap();
                assert_eq!(buf.len(), r.encoded_len(), "predicted len mismatch");

                let mut cur: &[u8] = &buf[..];
                let decoded = Record::decode(&mut cur).unwrap();
                assert_eq!(decoded, r);
                assert!(cur.is_empty(), "trailing bytes after decode");
            }
        };
    }

    roundtrip!(minimal, fixture_minimal_record);
    roundtrip!(keyed_with_headers, fixture_keyed_record);
    roundtrip!(large_payload, fixture_large_payload_record);

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
                assert!(msg.contains("negative header count"), "got: {msg}");
            }
            other => panic!("expected RecordParse, got {other:?}"),
        }
    }
}
