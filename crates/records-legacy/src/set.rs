//! v0/v1 `MessageSet`: a sequence of `(offset, size, message)` frames laid
//! out back-to-back, with no overall length prefix.
//!
//! ```text
//! [ offset:i64 | size:i32 | <Message bytes for `size` bytes> ]*
//! ```
//!
//! Compression in v0/v1 is encoded as a *wrapper* message whose value is
//! itself a compressed inner MessageSet. We handle that here: encoding
//! optionally wraps a flat MessageSet in a single compressed outer
//! message; decoding transparently unwraps a single layer.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crabka_compression::CompressionType;
use crabka_ids::Offset;

use crate::{
    error::LegacyRecordsError,
    message::{Magic, Message, attrs_with_compression, compression_from_attrs},
};

/// A single decoded MessageSet entry: the offset-tagged payload of one
/// logical record after compression unwrapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRecord {
    pub offset: Offset,
    /// Always `Some` when source magic is v1; `None` when v0.
    pub timestamp: Option<i64>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
}

/// Decode a flat (uncompressed) MessageSet from `buf`, expecting it to
/// consume exactly `set_size_bytes` bytes from the buffer. Compressed
/// wrapper messages encountered at top level are unwrapped recursively
/// once — nested compression (a compressed wrapper inside a compressed
/// wrapper) is rejected.
pub fn decode_message_set<B: Buf>(
    buf: &mut B,
    set_size_bytes: usize,
) -> Result<Vec<ParsedRecord>, LegacyRecordsError> {
    if buf.remaining() < set_size_bytes {
        return Err(LegacyRecordsError::Truncated {
            needed: set_size_bytes - buf.remaining(),
        });
    }
    let mut region = vec![0u8; set_size_bytes];
    buf.copy_to_slice(&mut region);
    let mut out = Vec::new();
    decode_into(&region, &mut out, /* allow_compression = */ true)?;
    Ok(out)
}

fn decode_into(
    bytes: &[u8],
    out: &mut Vec<ParsedRecord>,
    allow_compression: bool,
) -> Result<(), LegacyRecordsError> {
    let mut cur = bytes;
    while !cur.is_empty() {
        if cur.len() < 12 {
            return Err(LegacyRecordsError::Truncated {
                needed: 12 - cur.len(),
            });
        }
        let offset = cur.get_i64();
        let size = cur.get_i32();
        if size < 0 {
            return Err(LegacyRecordsError::NegativeLength {
                label: "message_size",
                len: size,
            });
        }
        let size = size as usize;
        if cur.remaining() < size {
            return Err(LegacyRecordsError::Truncated {
                needed: size - cur.remaining(),
            });
        }
        let msg = Message::decode_from(&mut cur, size)?;
        let codec = compression_from_attrs(msg.attributes)?;
        if codec == CompressionType::None {
            out.push(ParsedRecord {
                offset: Offset(offset),
                timestamp: msg.timestamp,
                key: msg.key,
                value: msg.value,
            });
        } else {
            if !allow_compression {
                return Err(LegacyRecordsError::NestedCompression);
            }
            let inner_compressed = msg.value.ok_or_else(|| {
                LegacyRecordsError::Malformed("compressed wrapper has null value".into())
            })?;
            // Bound decompressed output to guard against a decompression bomb
            // in a legacy compressed wrapper: ≤100x the compressed size, with a
            // 16 MiB floor and a 1 GiB ceiling.
            let max_output = inner_compressed
                .len()
                .saturating_mul(100)
                .clamp(16 * 1024 * 1024, 1024 * 1024 * 1024);
            let inner_bytes = crabka_compression::decompress(codec, &inner_compressed, max_output)?;

            // Parse the inner set (no nested compression allowed).
            let start_len = out.len();
            decode_into(&inner_bytes, out, false)?;

            // v1 wrapper-offset rewriting (KIP-32): inner offsets are
            // relative (0..count-1); absolute offset for inner[i] is
            // wrapper_offset - (count-1) + i. v0 wrappers always carry
            // absolute inner offsets, so leave them as-is.
            if matches!(msg.magic, Magic::V1) {
                let count = out.len() - start_len;
                if count > 0 {
                    let last_abs = offset;
                    let base_abs = last_abs - (count as i64 - 1);
                    for (i, rec) in out[start_len..].iter_mut().enumerate() {
                        rec.offset = Offset(base_abs + i as i64);
                    }
                }
            }
        }
    }
    Ok(())
}

/// Encode a flat MessageSet (one outer message per record) of magic
/// `magic` into `buf`. Useful when emitting an uncompressed batch.
pub fn encode_flat_message_set<B: BufMut, I: IntoIterator<Item = ParsedRecord>>(
    records: I,
    magic: Magic,
    buf: &mut B,
) {
    for r in records {
        let msg = Message {
            magic,
            attributes: 0,
            timestamp: match magic {
                Magic::V0 => None,
                Magic::V1 => Some(r.timestamp.unwrap_or(-1)),
            },
            key: r.key,
            value: r.value,
        };
        let msg_len = msg.encoded_len();
        buf.put_i64(r.offset.0);
        // Safe: legacy messages are well-bounded; capping at i32::MAX is
        // sufficient for any realistic batch.
        buf.put_i32(i32::try_from(msg_len).unwrap_or(i32::MAX));
        msg.encode_into(buf);
    }
}

/// Encode a MessageSet wrapped in a single compressed outer message.
/// The inner set is uncompressed and contains one message per record,
/// laid out per KIP-32 conventions (v1 inner offsets relative 0..N-1).
pub fn encode_compressed_message_set<B: BufMut>(
    records: &[ParsedRecord],
    magic: Magic,
    codec: CompressionType,
    buf: &mut B,
) -> Result<(), LegacyRecordsError> {
    debug_assert_ne!(
        codec,
        CompressionType::None,
        "use encode_flat_message_set for uncompressed"
    );
    if matches!(codec, CompressionType::Zstd) {
        return Err(LegacyRecordsError::Malformed(
            "zstd compression not representable in v0/v1".into(),
        ));
    }
    if records.is_empty() {
        // Nothing to wrap. Encode as a zero-message wrapper would be
        // ambiguous; emit nothing instead.
        return Ok(());
    }

    // Build inner uncompressed MessageSet.
    let mut inner = BytesMut::new();
    let count = records.len() as i64;
    for (i, r) in records.iter().enumerate() {
        let inner_offset = match magic {
            Magic::V0 => r.offset.0,
            // v1: relative 0..count-1
            Magic::V1 => i as i64,
        };
        let msg = Message {
            magic,
            attributes: 0,
            timestamp: match magic {
                Magic::V0 => None,
                Magic::V1 => Some(r.timestamp.unwrap_or(-1)),
            },
            key: r.key.clone(),
            value: r.value.clone(),
        };
        let msg_len = msg.encoded_len();
        inner.put_i64(inner_offset);
        inner.put_i32(i32::try_from(msg_len).unwrap_or(i32::MAX));
        msg.encode_into(&mut inner);
    }

    // Compress.
    let compressed = crabka_compression::compress(codec, &inner)?;

    // Wrapper message.
    let wrapper_attributes = attrs_with_compression(0, codec);
    let wrapper_timestamp = match magic {
        Magic::V0 => None,
        Magic::V1 => Some(
            records
                .iter()
                .filter_map(|r| r.timestamp)
                .max()
                .unwrap_or(-1),
        ),
    };
    let wrapper = Message {
        magic,
        attributes: wrapper_attributes,
        timestamp: wrapper_timestamp,
        key: None,
        value: Some(compressed),
    };
    let wrapper_len = wrapper.encoded_len();

    // Wrapper offset: v0 = 0 (per Kafka convention pre-KIP-32),
    // v1 = absolute offset of last inner record.
    let wrapper_offset = match magic {
        Magic::V0 => 0,
        Magic::V1 => records[records.len() - 1].offset.0,
    };
    buf.put_i64(wrapper_offset);
    buf.put_i32(i32::try_from(wrapper_len).unwrap_or(i32::MAX));
    wrapper.encode_into(buf);
    let _ = count;
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn sample_records_v1() -> Vec<ParsedRecord> {
        vec![
            ParsedRecord {
                offset: Offset(100),
                timestamp: Some(1_700_000_000),
                key: Some(Bytes::from_static(b"a")),
                value: Some(Bytes::from_static(b"1")),
            },
            ParsedRecord {
                offset: Offset(101),
                timestamp: Some(1_700_000_010),
                key: Some(Bytes::from_static(b"b")),
                value: Some(Bytes::from_static(b"2")),
            },
            ParsedRecord {
                offset: Offset(102),
                timestamp: Some(1_700_000_020),
                key: None,
                value: Some(Bytes::from_static(b"3")),
            },
        ]
    }

    fn sample_records_v0() -> Vec<ParsedRecord> {
        sample_records_v1()
            .into_iter()
            .map(|r| ParsedRecord {
                timestamp: None,
                ..r
            })
            .collect()
    }

    #[test]
    fn message_set_roundtrips() {
        for (name, magic, codec) in [
            ("flat v0", Magic::V0, None),
            ("flat v1", Magic::V1, None),
            ("gzip v0", Magic::V0, Some(CompressionType::Gzip)),
            ("gzip v1", Magic::V1, Some(CompressionType::Gzip)),
            ("snappy v1", Magic::V1, Some(CompressionType::Snappy)),
        ] {
            let records = match magic {
                Magic::V0 => sample_records_v0(),
                Magic::V1 => sample_records_v1(),
            };
            let mut buffer = BytesMut::new();
            if let Some(codec) = codec {
                encode_compressed_message_set(&records, magic, codec, &mut buffer).unwrap();
            } else {
                encode_flat_message_set(records.clone(), magic, &mut buffer);
            }

            let decoded = decode_message_set(&mut &buffer[..], buffer.len()).unwrap();
            assert_eq!(decoded, records, "case {name}");
        }
    }

    #[test]
    fn rejects_nested_compression() {
        // Construct a wrapper containing a wrapper.
        let inner_recs = sample_records_v1();
        let mut inner_outer = BytesMut::new();
        encode_compressed_message_set(
            &inner_recs,
            Magic::V1,
            CompressionType::Gzip,
            &mut inner_outer,
        )
        .unwrap();

        // Now wrap that bytestream as the value of another compressed wrapper.
        let outer_compressed =
            crabka_compression::compress(CompressionType::Gzip, &inner_outer).unwrap();
        let outer_msg = Message {
            magic: Magic::V1,
            attributes: attrs_with_compression(0, CompressionType::Gzip),
            timestamp: Some(0),
            key: None,
            value: Some(outer_compressed),
        };
        let mut wire = BytesMut::new();
        let outer_len = outer_msg.encoded_len();
        wire.put_i64(0);
        wire.put_i32(outer_len as i32);
        outer_msg.encode_into(&mut wire);

        let mut cur: &[u8] = &wire[..];
        let err = decode_message_set(&mut cur, wire.len()).unwrap_err();
        assert!(matches!(err, LegacyRecordsError::NestedCompression));
    }

    // --- mutation-coverage tests --------------------------------------------
    //
    // Round-trips above don't exercise the malformed/boundary paths or pin
    // exact framing. These do: precise `needed` counts, error-variant
    // boundaries, the decompression-cap floor, and the v1 inner-offset rewrite.
    //
    // `if count > 0` (the v1 rewrite guard) is an equivalent mutant under
    // `>= 0`: the rewrite loop is empty when count == 0, so both behave alike.

    #[test]
    fn decode_message_set_short_buffer_reports_needed() {
        // 4 bytes available, caller claims a 12-byte set: needed = 12 - 4 = 8.
        let data = [0u8; 4];
        let mut cur: &[u8] = &data;
        let err = decode_message_set(&mut cur, 12).unwrap_err();
        assert!(matches!(err, LegacyRecordsError::Truncated { needed: 8 }));
    }

    #[test]
    fn entry_header_truncated_reports_needed() {
        // 8 bytes where a 12-byte (offset+size) entry header is required:
        // needed = 12 - 8 = 4.
        let data = [0u8; 8];
        let mut cur: &[u8] = &data;
        let err = decode_message_set(&mut cur, 8).unwrap_err();
        assert!(matches!(err, LegacyRecordsError::Truncated { needed: 4 }));
    }

    #[test]
    fn entry_zero_message_size_is_malformed() {
        // offset(8) + size(0): clears the `< 12` and `size < 0` guards, then
        // Message::decode_from rejects the 0-byte frame as Malformed (< 6).
        // Distinguishes the `<` boundaries from `<=`/`==`.
        let mut data = BytesMut::new();
        data.put_i64(0);
        data.put_i32(0);
        let n = data.len();
        let mut cur: &[u8] = &data[..];
        let err = decode_message_set(&mut cur, n).unwrap_err();
        assert!(matches!(err, LegacyRecordsError::Malformed(_)));
    }

    #[test]
    fn entry_negative_message_size_rejected() {
        let mut data = BytesMut::new();
        data.put_i64(0);
        data.put_i32(-1);
        data.put_slice(&[0u8; 4]); // keep region >= 12 bytes
        let n = data.len();
        let mut cur: &[u8] = &data[..];
        let err = decode_message_set(&mut cur, n).unwrap_err();
        assert!(matches!(
            err,
            LegacyRecordsError::NegativeLength {
                label: "message_size",
                len: -1
            }
        ));
    }

    #[test]
    fn entry_message_body_truncated_reports_needed() {
        // Entry claims a 10-byte message but only 2 bytes follow:
        // needed = 10 - 2 = 8.
        let mut data = BytesMut::new();
        data.put_i64(0);
        data.put_i32(10);
        data.put_slice(&[0u8; 2]);
        let n = data.len();
        let mut cur: &[u8] = &data[..];
        let err = decode_message_set(&mut cur, n).unwrap_err();
        assert!(matches!(err, LegacyRecordsError::Truncated { needed: 8 }));
    }

    #[test]
    fn flat_v1_missing_timestamp_encodes_minus_one() {
        let recs = vec![ParsedRecord {
            offset: Offset(7),
            timestamp: None,
            key: None,
            value: Some(Bytes::from_static(b"v")),
        }];
        let mut buf = BytesMut::new();
        encode_flat_message_set(recs, Magic::V1, &mut buf);
        let mut cur: &[u8] = &buf[..];
        let decoded = decode_message_set(&mut cur, buf.len()).unwrap();
        assert_eq!(
            decoded,
            vec![ParsedRecord {
                offset: Offset(7),
                timestamp: Some(-1),
                key: None,
                value: Some(Bytes::from_static(b"v")),
            }]
        );
    }

    #[test]
    fn compressed_v1_missing_timestamps_default_to_minus_one() {
        // Records with no timestamps: inner messages encode ts = -1, and the
        // wrapper's own timestamp (max over records, none present) is -1.
        let recs = vec![ParsedRecord {
            offset: Offset(9),
            timestamp: None,
            key: None,
            value: Some(Bytes::from_static(b"v")),
        }];
        let mut buf = BytesMut::new();
        encode_compressed_message_set(&recs, Magic::V1, CompressionType::Gzip, &mut buf).unwrap();

        // Inspect the raw wrapper message's own timestamp before unwrapping.
        let mut cur: &[u8] = &buf[..];
        let _wrapper_offset = cur.get_i64();
        let wrapper_size = cur.get_i32() as usize;
        let wrapper = Message::decode_from(&mut cur, wrapper_size).unwrap();
        // The inner record's timestamp survives the unwrap as -1.
        let mut c2: &[u8] = &buf[..];
        let decoded = decode_message_set(&mut c2, buf.len()).unwrap();
        assert_eq!(
            (wrapper.timestamp, decoded[0].timestamp),
            (Some(-1), Some(-1))
        );
    }

    #[test]
    fn compressed_wrapper_allows_large_decompressed_output() {
        // The 16 MiB decompression-cap floor must let a ~2 MiB wrapper through
        // even though the compressed size is tiny. Shrinking the floor (a `*`
        // flip in `16 * 1024 * 1024`) would reject this round-trip as TooLarge.
        let big = vec![0x7Eu8; 2 * 1024 * 1024];
        let recs = vec![ParsedRecord {
            offset: Offset(0),
            timestamp: Some(5),
            key: None,
            value: Some(Bytes::from(big.clone())),
        }];
        let mut buf = BytesMut::new();
        encode_compressed_message_set(&recs, Magic::V1, CompressionType::Gzip, &mut buf).unwrap();
        let mut cur: &[u8] = &buf[..];
        let decoded = decode_message_set(&mut cur, buf.len()).unwrap();
        assert_eq!(decoded, recs);
    }

    #[test]
    fn v1_inner_offset_rewrite_uses_inner_count() {
        // A set with a flat record FOLLOWED by a compressed v1 wrapper: when the
        // wrapper is decoded, `out` already holds the flat record (start_len >
        // 0). The inner-offset rewrite must use the inner count (out.len() -
        // start_len), not out.len() + start_len.
        let mut buf = BytesMut::new();
        let flat = ParsedRecord {
            offset: Offset(50),
            timestamp: Some(1),
            key: None,
            value: Some(Bytes::from_static(b"flat")),
        };
        encode_flat_message_set(vec![flat.clone()], Magic::V1, &mut buf);
        let inner = vec![
            ParsedRecord {
                offset: Offset(100),
                timestamp: Some(2),
                key: None,
                value: Some(Bytes::from_static(b"x")),
            },
            ParsedRecord {
                offset: Offset(101),
                timestamp: Some(3),
                key: None,
                value: Some(Bytes::from_static(b"y")),
            },
        ];
        encode_compressed_message_set(&inner, Magic::V1, CompressionType::Gzip, &mut buf).unwrap();

        let mut cur: &[u8] = &buf[..];
        let decoded = decode_message_set(&mut cur, buf.len()).unwrap();
        let expected = std::iter::once(flat).chain(inner).collect::<Vec<_>>();
        assert_eq!(decoded, expected);
    }
}
