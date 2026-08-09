//! Confluent wire framing: `magic(0x00) | schema_id(4 BE) | [msg_index] | body`.
//!
//! Protobuf adds a message-index between the id and the body. The message-index
//! is a varint count and then that many varint indices. Confluent optimizes the
//! common top-level case `[0]` to a single `0x00` byte and omits the count.
//! This module writes and reads the same form.

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::SchemaSerdeError;

pub const MAGIC: u8 = 0x00;

/// Frame a non-Protobuf body: `0x00 | id(4 BE) | body`.
#[must_use]
pub fn encode(id: u32, body: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(5 + body.len());
    buf.put_u8(MAGIC);
    buf.put_u32(id); // big-endian
    buf.put_slice(body);
    buf.freeze()
}

/// Frame a Protobuf body with its message-index path.
#[must_use]
/// # Panics
/// Panics if a schema previously validated by the registry is missing a definition or dependency required during resolution.
pub fn encode_protobuf(id: u32, message_index: &[i32], body: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(8 + body.len());
    buf.put_u8(MAGIC);
    buf.put_u32(id);
    if message_index == [0] {
        buf.put_u8(0); // optimized single-byte form
    } else {
        let index_count = i64::try_from(message_index.len())
            .expect("Protobuf message-index count must fit in i64");
        put_varint(&mut buf, index_count);
        for &ix in message_index {
            put_varint(&mut buf, i64::from(ix));
        }
    }
    buf.put_slice(body);
    buf.freeze()
}

/// Split a non-Protobuf frame into `(id, body)`.
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub fn decode(bytes: &[u8]) -> Result<(u32, &[u8]), SchemaSerdeError> {
    strip_header(bytes)
}

/// Split a Protobuf frame into `(id, message_index, body)`.
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub fn decode_protobuf(bytes: &[u8]) -> Result<(u32, Vec<i32>, &[u8]), SchemaSerdeError> {
    let (id, after_id) = strip_header(bytes)?;
    let (len, mut rest) = read_varint(after_id)?;
    let indices = if len == 0 {
        vec![0] // optimized single-byte form
    } else {
        let index_count = usize::try_from(len).map_err(|_| {
            SchemaSerdeError::Wire(format!("negative Protobuf message-index count {len}"))
        })?;
        if index_count > rest.len() {
            return Err(SchemaSerdeError::Wire(format!(
                "Protobuf message-index count {index_count} exceeds remaining frame bytes {}",
                rest.len()
            )));
        }
        let mut v = Vec::with_capacity(index_count);
        for _ in 0..index_count {
            let (ix, r) = read_varint(rest)?;
            let ix = i32::try_from(ix).map_err(|_| {
                SchemaSerdeError::Wire(format!("Protobuf message index {ix} does not fit in i32"))
            })?;
            v.push(ix);
            rest = r;
        }
        v
    };
    Ok((id, indices, rest))
}

fn strip_header(bytes: &[u8]) -> Result<(u32, &[u8]), SchemaSerdeError> {
    if bytes.len() < 5 {
        return Err(SchemaSerdeError::Wire(format!(
            "frame too short: {} bytes",
            bytes.len()
        )));
    }
    if bytes[0] != MAGIC {
        return Err(SchemaSerdeError::Wire(format!(
            "bad magic byte 0x{:02x}",
            bytes[0]
        )));
    }
    let id = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    Ok((id, &bytes[5..]))
}

fn put_varint(buf: &mut BytesMut, value: i64) {
    let mut zig = ((value << 1) ^ (value >> 63)).cast_unsigned();
    loop {
        if zig < 0x80 {
            buf.put_u8(zig.to_le_bytes()[0]);
            break;
        }
        buf.put_u8((zig.to_le_bytes()[0] & 0x7f) | 0x80);
        zig >>= 7;
    }
}

fn read_varint(bytes: &[u8]) -> Result<(i64, &[u8]), SchemaSerdeError> {
    let mut result: u64 = 0;
    for (index, &byte) in bytes.iter().take(10).enumerate() {
        let payload = u64::from(byte & 0x7f);
        if index == 9 && payload > 1 {
            return Err(SchemaSerdeError::Wire("varint overflows 64 bits".into()));
        }
        result |= payload << (index * 7);
        if byte & 0x80 == 0 {
            if encoded_varint_len(result) != index + 1 {
                return Err(SchemaSerdeError::Wire("varint is overlong".into()));
            }
            let decoded = (result >> 1).cast_signed() ^ -(result & 1).cast_signed();
            return Ok((decoded, &bytes[index + 1..]));
        }
    }
    if bytes.len() >= 10 {
        return Err(SchemaSerdeError::Wire("varint exceeds 10 bytes".into()));
    }
    Err(SchemaSerdeError::Wire("truncated varint".into()))
}

fn encoded_varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn encode_prepends_magic_and_be_id() {
        let f = encode(1, b"xy");
        check!(f.as_ref() == [0x00, 0x00, 0x00, 0x00, 0x01, b'x', b'y']);
    }

    #[test]
    fn decode_round_trips() {
        let f = encode(258, b"body");
        let (id, body) = decode(&f).unwrap();
        check!((id, body) == (258, b"body".as_slice()));
    }

    #[test]
    fn decode_rejects_bad_magic_and_short() {
        for (name, frame) in [
            ("bad-magic", &[0x01, 0, 0, 0, 1][..]),
            ("short", &[0x00, 0, 0][..]),
        ] {
            check!(decode(frame).is_err(), "case {name}");
        }
    }

    #[test]
    fn protobuf_top_level_uses_single_zero_byte() {
        let f = encode_protobuf(7, &[0], b"pb");
        check!(f.as_ref() == [0x00, 0x00, 0x00, 0x00, 0x07, 0x00, b'p', b'b']);
        let (id, idx, body) = decode_protobuf(&f).unwrap();
        check!((id, idx, body) == (7, vec![0], b"pb".as_slice()));
    }

    #[test]
    fn protobuf_nested_index_round_trips() {
        let f = encode_protobuf(7, &[1, 0], b"pb");
        let (id, idx, body) = decode_protobuf(&f).unwrap();
        check!((id, idx, body) == (7, vec![1, 0], b"pb".as_slice()));
    }

    #[test]
    fn protobuf_rejects_negative_index_count() {
        let mut frame = BytesMut::from(&[MAGIC, 0, 0, 0, 7][..]);
        put_varint(&mut frame, -1);

        let error = decode_protobuf(&frame).unwrap_err();
        check!(
            error
                .to_string()
                .contains("negative Protobuf message-index count")
        );
    }

    #[test]
    fn protobuf_rejects_index_outside_i32() {
        let mut frame = BytesMut::from(&[MAGIC, 0, 0, 0, 7][..]);
        put_varint(&mut frame, 1);
        put_varint(&mut frame, i64::from(i32::MAX) + 1);

        let error = decode_protobuf(&frame).unwrap_err();
        check!(error.to_string().contains("does not fit in i32"));
    }

    #[test]
    fn strip_header_accepts_minimum_5_byte_frame() {
        // Exactly 5 bytes = magic + id with an empty body. The guard is
        // `len < 5`, so a 5-byte frame must decode (not be rejected as short).
        let (id, body) = decode(&[0x00, 0x00, 0x00, 0x01, 0x02]).unwrap();
        check!((id, body) == (0x0102, b"".as_slice()));
    }

    #[test]
    fn varint_zigzag_round_trips() {
        // Single- and multi-byte encodings plus zigzag sign handling, end to
        // end through put_varint/read_varint. Multi-byte values exercise the
        // continuation-bit and shift logic; negatives exercise the zigzag.
        let values: [i64; 17] = [
            0,
            1,
            -1,
            63,
            64,
            -64,
            127,
            128,
            -128,
            300,
            -300,
            8192,
            -8192,
            i64::from(i32::MAX),
            i64::from(i32::MIN),
            1 << 40,
            -(1 << 40),
        ];
        for v in values {
            let mut buf = BytesMut::new();
            put_varint(&mut buf, v);
            let (decoded, rest) = read_varint(&buf).expect("varint decodes");
            check!(decoded == v, "round-trip {v}");
            check!(rest.is_empty(), "no trailing bytes for {v}");
        }
    }

    #[test]
    fn protobuf_rejects_overflowing_and_noncanonical_message_indices() {
        let header = [MAGIC, 0, 0, 0, 7];
        let overflowing = [header.as_slice(), &[0x80; 9], &[0x02]].concat();
        let error = decode_protobuf(&overflowing).expect_err("overflowing varint is rejected");
        check!(error.to_string().contains("overflows 64 bits"));

        let overlong_zero = [header.as_slice(), &[0x80, 0x00]].concat();
        let error = decode_protobuf(&overlong_zero).expect_err("overlong zero is rejected");
        check!(error.to_string().contains("overlong"));
    }
}
