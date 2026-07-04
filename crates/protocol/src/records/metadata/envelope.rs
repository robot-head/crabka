//! The `KRaft` metadata record-value envelope (`MetadataRecordSerde` /
//! `ApiMessageAndVersion`): a record value is
//! `frameVersion (uvarint, 1) + apiKey (uvarint) + apiVersion (uvarint) + body`.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::primitives::varint::{get_uvarint, put_uvarint, uvarint_len};

/// Current `KRaft` metadata frame version. Kafka writes 1 (verified against
/// mirror.gcr.io/apache/kafka:4.0.0 `__cluster_metadata` record values — the leading byte is
/// `0x01`).
pub const FRAME_VERSION: u32 = 1;

/// Decoded envelope header (everything before the message body).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValueHeader {
    pub frame_version: u32,
    pub api_key: u32,
    pub api_version: u32,
}

/// Error decoding a metadata record value envelope.
#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    #[error("truncated metadata record value envelope")]
    Truncated,
}

/// Encode a record value: envelope header + the already-encoded `body` bytes.
#[must_use]
pub fn encode_value(api_key: u32, api_version: u32, body: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(
        uvarint_len(FRAME_VERSION) + uvarint_len(api_key) + uvarint_len(api_version) + body.len(),
    );
    put_uvarint(&mut out, FRAME_VERSION);
    put_uvarint(&mut out, api_key);
    put_uvarint(&mut out, api_version);
    out.put_slice(body);
    out.freeze()
}

/// Decode the envelope header, leaving `buf` positioned at the message body.
///
/// # Errors
/// Returns [`EnvelopeError::Truncated`] if any varint cannot be read.
pub fn decode_value_header<B: Buf>(buf: &mut B) -> Result<ValueHeader, EnvelopeError> {
    let frame_version = get_uvarint(buf).map_err(|_| EnvelopeError::Truncated)?;
    let api_key = get_uvarint(buf).map_err(|_| EnvelopeError::Truncated)?;
    let api_version = get_uvarint(buf).map_err(|_| EnvelopeError::Truncated)?;
    Ok(ValueHeader {
        frame_version,
        api_key,
        api_version,
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn frame_zero_apikey_apiversion_roundtrip() {
        // FeatureLevelRecord apiKey=12 apiVersion=0, body bytes b"\x01\x02".
        let body: &[u8] = &[0x01, 0x02];
        let value = encode_value(12, 0, body);
        // frameVersion(0)=1 byte, apiKey(12)=1 byte, apiVersion(0)=1 byte, +body.
        assert!(value.len() == 3 + body.len());
        let mut cur: &[u8] = &value;
        let hdr = decode_value_header(&mut cur).expect("decode header");
        let expected = ValueHeader {
            frame_version: 1,
            api_key: 12,
            api_version: 0,
        };
        assert!(hdr == expected);
        assert!(cur == body);
    }

    #[test]
    fn truncated_value_errors() {
        let mut cur: &[u8] = &[]; // no frameVersion byte
        assert!(decode_value_header(&mut cur).is_err());
    }
}
