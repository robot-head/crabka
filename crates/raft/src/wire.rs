//! Crabka-private controller RPCs over Kafka TCP framing.
//!
//! These bodies are NOT part of `crabka-protocol`'s codegen. They are
//! controller-only and Crabka-specific, with hand-written `encode_v0` and
//! `decode_v0` methods. The KIP-595 quorum RPCs (Fetch, Vote, Begin, End) ride
//! the generated codecs instead. See [`crate::kraft::transport::wire`]. The two
//! types here back the observer metadata-fetch and the follower-to-leader
//! submit-change forward.
//!
//! Api keys: `1003` `SubmitChange` (forward), `1004` `MetadataFetch` (observer).

use bytes::{Buf, BufMut, Bytes};
use crabka_protocol::ProtocolError;

const I32_LEN: usize = 4;
const SUBMIT_CHANGE_RESPONSE_FIXED_LEN: usize = 10;
const METADATA_FETCH_REQUEST_LEN: usize = 12;
const METADATA_FETCH_RESPONSE_FIXED_LEN: usize = 30;

/// Forwards a `Controller::submit_change` from a follower to the leader.
///
/// The body is the wincode-encoded `Vec<MetadataRecord>`. The response carries
/// a single `error_code`, where 0 means applied and any non-zero value means
/// not-leader or metadata-validation.
pub const API_KEY_SUBMIT_CHANGE: i16 = 1003;

/// Observer metadata fetch.
///
/// The body carries a `fetch_offset`, which is a `KraftLog` offset, and
/// `max_bytes`. The response carries committed `__cluster_metadata` entries
/// encoded as Kafka record batches, plus `log_start_offset`, `high_watermark`,
/// and a `leader_hint`.
pub const API_KEY_METADATA_FETCH: i16 = 1004;

fn require_remaining(buf: &[u8], required: usize) -> Result<(), ProtocolError> {
    match required.checked_sub(buf.remaining()) {
        Some(0) | None => Ok(()),
        Some(needed) => Err(ProtocolError::UnexpectedEof { needed }),
    }
}

fn put_i32_len_prefixed_bytes(
    out: &mut Vec<u8>,
    bytes: &Bytes,
    too_long: &'static str,
) -> Result<(), ProtocolError> {
    out.put_i32(i32::try_from(bytes.len()).map_err(|_| ProtocolError::InvalidValue(too_long))?);
    out.put_slice(bytes);
    Ok(())
}

fn get_i32_len_prefixed_bytes(
    buf: &mut &[u8],
    negative_len: &'static str,
) -> Result<Bytes, ProtocolError> {
    require_remaining(buf, I32_LEN)?;
    let len = buf.get_i32();
    let len = usize::try_from(len).map_err(|_| ProtocolError::InvalidValue(negative_len))?;
    require_remaining(buf, len)?;
    let bytes = Bytes::copy_from_slice(&buf[..len]);
    buf.advance(len);
    Ok(bytes)
}

/// Forward-to-leader payload.
///
/// The body is opaque wincode bytes that represent the `Vec<MetadataRecord>` to
/// apply. The controller layer owns the serde details, so the wire module stays
/// metadata-agnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaSubmitChangeRequest {
    pub records: Bytes,
}

impl CrabkaSubmitChangeRequest {
    /// # Errors
    /// Returns an error if the record payload is too large for the wire format.
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        put_i32_len_prefixed_bytes(out, &self.records, "records length exceeds i32::MAX")
    }

    /// # Errors
    /// Returns an error if the payload is truncated or has an invalid length.
    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        let records = get_i32_len_prefixed_bytes(buf, "negative records length")?;
        Ok(Self { records })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaSubmitChangeResponse {
    /// 0 means success. Any other value is an opaque transport-level error
    /// code: 1 is not leader, 2 is metadata validation, and 3 is other.
    pub error_code: i16,
    /// The leader id the responder believes is current, when the responder
    /// cannot apply the change itself. -1 means "unknown".
    pub leader_hint: i64,
    /// Wincode-encoded [`crate::SubmitChangeResult`] on success.
    pub result: Bytes,
}

impl CrabkaSubmitChangeResponse {
    /// Encodes this response with wire version zero.
    ///
    /// # Errors
    ///
    /// Returns an error when the response payload exceeds the protocol's
    /// signed 32-bit length field.
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i16(self.error_code);
        out.put_i64(self.leader_hint);
        put_i32_len_prefixed_bytes(out, &self.result, "result length exceeds i32::MAX")?;
        Ok(())
    }

    /// # Errors
    /// Returns an error if the response payload is truncated.
    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        require_remaining(buf, SUBMIT_CHANGE_RESPONSE_FIXED_LEN)?;
        Ok(Self {
            error_code: buf.get_i16(),
            leader_hint: buf.get_i64(),
            result: get_i32_len_prefixed_bytes(buf, "negative result length")?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaMetadataFetchRequest {
    /// Next `KraftLog` offset the observer wants.
    pub fetch_offset: i64,
    /// Soft cap on the encoded record-batch payload.
    pub max_bytes: i32,
}

impl CrabkaMetadataFetchRequest {
    pub fn encode_v0(&self, out: &mut Vec<u8>) {
        out.put_i64(self.fetch_offset);
        out.put_i32(self.max_bytes);
    }

    /// # Errors
    /// Returns an error if the request payload is truncated.
    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        require_remaining(buf, METADATA_FETCH_REQUEST_LEN)?;
        Ok(Self {
            fetch_offset: buf.get_i64(),
            max_bytes: buf.get_i32(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaMetadataFetchResponse {
    /// 0 means success. 1 means this node cannot serve, so read
    /// `leader_hint`.
    pub error_code: i16,
    /// Leader id the responder believes is current. -1 means unknown.
    pub leader_hint: i64,
    /// Lowest retained log offset on the responder.
    pub log_start_offset: i64,
    /// Highest committed and applied log offset on the responder.
    pub high_watermark: i64,
    /// Concatenated Kafka `RecordBatch`es, one for each committed log batch.
    pub records: Bytes,
}

impl CrabkaMetadataFetchResponse {
    /// # Errors
    /// Returns an error if the record payload is too large for the wire format.
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i16(self.error_code);
        out.put_i64(self.leader_hint);
        out.put_i64(self.log_start_offset);
        out.put_i64(self.high_watermark);
        put_i32_len_prefixed_bytes(out, &self.records, "records length exceeds i32::MAX")
    }

    /// # Errors
    /// Returns an error if the payload is truncated or has an invalid length.
    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        require_remaining(buf, METADATA_FETCH_RESPONSE_FIXED_LEN)?;
        let error_code = buf.get_i16();
        let leader_hint = buf.get_i64();
        let log_start_offset = buf.get_i64();
        let high_watermark = buf.get_i64();
        let records = get_i32_len_prefixed_bytes(buf, "negative records length")?;
        Ok(Self {
            error_code,
            leader_hint,
            log_start_offset,
            high_watermark,
            records,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn assert_unexpected_eof<T: std::fmt::Debug>(result: Result<T, ProtocolError>, want: usize) {
        match result {
            Err(ProtocolError::UnexpectedEof { needed }) => assert2::assert!(needed == want),
            other => panic!("expected UnexpectedEof {{ needed: {want} }}, got {other:?}"),
        }
    }

    fn assert_invalid_value<T: std::fmt::Debug>(result: Result<T, ProtocolError>) {
        match result {
            Err(ProtocolError::InvalidValue(_)) => {}
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn submit_change_round_trips() {
        let req = CrabkaSubmitChangeRequest {
            records: Bytes::from_static(b"\x01\x02\x03"),
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        assert2::assert!(CrabkaSubmitChangeRequest::decode_v0(&mut cur).unwrap() == req);

        let resp = CrabkaSubmitChangeResponse {
            error_code: 1,
            leader_hint: 3,
            result: Bytes::from_static(b"result"),
        };
        let mut out = Vec::new();
        resp.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        assert2::assert!(CrabkaSubmitChangeResponse::decode_v0(&mut cur).unwrap() == resp);
    }

    #[test]
    fn submit_change_request_decode_checks_prefix_and_payload_lengths() {
        let mut short_prefix: &[u8] = &[0, 0, 0];
        assert_unexpected_eof(CrabkaSubmitChangeRequest::decode_v0(&mut short_prefix), 1);

        let mut negative_len: &[u8] = &(-1_i32).to_be_bytes();
        assert_invalid_value(CrabkaSubmitChangeRequest::decode_v0(&mut negative_len));

        let mut exact_empty: &[u8] = &[0, 0, 0, 0];
        let decoded = CrabkaSubmitChangeRequest::decode_v0(&mut exact_empty).unwrap();
        assert2::assert!(decoded.records.is_empty());
        assert2::assert!(exact_empty.is_empty());

        let mut short_payload: &[u8] = &[0, 0, 0, 4, 0xaa];
        assert_unexpected_eof(CrabkaSubmitChangeRequest::decode_v0(&mut short_payload), 3);
    }

    #[test]
    fn submit_change_response_decode_checks_fixed_length() {
        let mut short: &[u8] = &[0, 1, 2];
        assert_unexpected_eof(CrabkaSubmitChangeResponse::decode_v0(&mut short), 7);
    }

    #[test]
    fn metadata_fetch_round_trips() {
        let req = CrabkaMetadataFetchRequest {
            fetch_offset: 42,
            max_bytes: 1_048_576,
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out);
        let mut cur: &[u8] = &out;
        assert2::assert!(CrabkaMetadataFetchRequest::decode_v0(&mut cur).unwrap() == req);

        let resp = CrabkaMetadataFetchResponse {
            error_code: 0,
            leader_hint: 3,
            log_start_offset: 1,
            high_watermark: 99,
            records: Bytes::from_static(b"\x01\x02\x03"),
        };
        let mut out = Vec::new();
        resp.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        assert2::assert!(CrabkaMetadataFetchResponse::decode_v0(&mut cur).unwrap() == resp);
    }

    #[test]
    fn metadata_fetch_request_decode_checks_fixed_length() {
        let mut short: &[u8] = &[0, 1, 2, 3, 4];
        assert_unexpected_eof(CrabkaMetadataFetchRequest::decode_v0(&mut short), 7);

        let mut exact = Vec::new();
        CrabkaMetadataFetchRequest {
            fetch_offset: 9,
            max_bytes: 512,
        }
        .encode_v0(&mut exact);
        let mut cur: &[u8] = &exact;
        let decoded = CrabkaMetadataFetchRequest::decode_v0(&mut cur).unwrap();
        assert2::assert!(
            decoded
                == CrabkaMetadataFetchRequest {
                    fetch_offset: 9,
                    max_bytes: 512,
                }
        );
        assert2::assert!(cur.is_empty());
    }

    #[test]
    fn metadata_fetch_response_decode_checks_fixed_and_payload_lengths() {
        let mut short_fixed: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7, 8];
        assert_unexpected_eof(CrabkaMetadataFetchResponse::decode_v0(&mut short_fixed), 21);

        let resp = CrabkaMetadataFetchResponse {
            error_code: 0,
            leader_hint: -1,
            log_start_offset: 4,
            high_watermark: 4,
            records: Bytes::new(),
        };
        let mut exact = Vec::new();
        resp.encode_v0(&mut exact).unwrap();
        let mut cur: &[u8] = &exact;
        assert2::assert!(CrabkaMetadataFetchResponse::decode_v0(&mut cur).unwrap() == resp);
        assert2::assert!(cur.is_empty());

        let mut short_payload = Vec::new();
        short_payload.extend_from_slice(&0_i16.to_be_bytes());
        short_payload.extend_from_slice(&1_i64.to_be_bytes());
        short_payload.extend_from_slice(&2_i64.to_be_bytes());
        short_payload.extend_from_slice(&3_i64.to_be_bytes());
        short_payload.extend_from_slice(&4_i32.to_be_bytes());
        short_payload.push(0xaa);
        let mut cur: &[u8] = &short_payload;
        assert_unexpected_eof(CrabkaMetadataFetchResponse::decode_v0(&mut cur), 3);
    }
}
