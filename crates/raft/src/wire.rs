//! Crabka-private controller RPCs over Kafka TCP framing.
//!
//! These bodies are NOT part of `crabka-protocol`'s codegen — they're
//! controller-only and Crabka-specific, with hand-written `encode_v0`/
//! `decode_v0` methods. The KIP-595 quorum RPCs (Fetch/Vote/Begin/End) ride the
//! generated codecs instead (see [`crate::kraft::transport::wire`]); the two
//! types here back the observer metadata-fetch and the follower→leader
//! submit-change forward.
//!
//! Api keys: `1003` `SubmitChange` (forward), `1004` `MetadataFetch` (observer).

use bytes::{Buf, BufMut, Bytes};

use crabka_protocol::ProtocolError;

/// Forward a `Controller::submit_change` from a follower to the leader. The body
/// is the wincode-encoded `Vec<MetadataRecord>`; the response carries a single
/// `error_code` (0 = applied, non-zero = not-leader / metadata-validation).
pub const API_KEY_SUBMIT_CHANGE: i16 = 1003;

/// Observer metadata fetch. The body carries a `fetch_offset` (`KraftLog`
/// offset) + `max_bytes`; the response carries committed `__cluster_metadata`
/// entries encoded as Kafka record batches, plus `log_start_offset` /
/// `high_watermark` and a `leader_hint`.
pub const API_KEY_METADATA_FETCH: i16 = 1004;

/// Forward-to-leader payload. Body is opaque wincode bytes representing the
/// `Vec<MetadataRecord>` to apply; the controller layer owns the serde details
/// so the wire module stays metadata-agnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaSubmitChangeRequest {
    pub records: Bytes,
}

impl CrabkaSubmitChangeRequest {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i32(
            i32::try_from(self.records.len())
                .map_err(|_| ProtocolError::InvalidValue("records length exceeds i32::MAX"))?,
        );
        out.put_slice(&self.records);
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        if buf.remaining() < 4 {
            return Err(ProtocolError::UnexpectedEof {
                needed: 4 - buf.remaining(),
            });
        }
        let len = buf.get_i32();
        let len = usize::try_from(len)
            .map_err(|_| ProtocolError::InvalidValue("negative records length"))?;
        if buf.remaining() < len {
            return Err(ProtocolError::UnexpectedEof {
                needed: len - buf.remaining(),
            });
        }
        let records = Bytes::copy_from_slice(&buf[..len]);
        buf.advance(len);
        Ok(Self { records })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaSubmitChangeResponse {
    /// 0 = success; otherwise an opaque transport-level error code (1 = not
    /// leader, 2 = metadata validation, 3 = other).
    pub error_code: i16,
    /// Hint: the leader id the responder believes is current, when it cannot
    /// apply the change itself. -1 means "unknown".
    pub leader_hint: i64,
}

impl CrabkaSubmitChangeResponse {
    pub fn encode_v0(&self, out: &mut Vec<u8>) {
        out.put_i16(self.error_code);
        out.put_i64(self.leader_hint);
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        const LEN: usize = 2 + 8;
        if buf.remaining() < LEN {
            return Err(ProtocolError::UnexpectedEof {
                needed: LEN - buf.remaining(),
            });
        }
        Ok(Self {
            error_code: buf.get_i16(),
            leader_hint: buf.get_i64(),
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

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        const LEN: usize = 8 + 4;
        if buf.remaining() < LEN {
            return Err(ProtocolError::UnexpectedEof {
                needed: LEN - buf.remaining(),
            });
        }
        Ok(Self {
            fetch_offset: buf.get_i64(),
            max_bytes: buf.get_i32(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaMetadataFetchResponse {
    /// 0 = success; 1 = this node cannot serve — consult `leader_hint`.
    pub error_code: i16,
    /// Leader id the responder believes is current; -1 = unknown.
    pub leader_hint: i64,
    /// Lowest retained log offset on the responder.
    pub log_start_offset: i64,
    /// Highest committed (applied) log offset on the responder.
    pub high_watermark: i64,
    /// Concatenated Kafka `RecordBatch`es (one per committed log batch).
    pub records: Bytes,
}

impl CrabkaMetadataFetchResponse {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i16(self.error_code);
        out.put_i64(self.leader_hint);
        out.put_i64(self.log_start_offset);
        out.put_i64(self.high_watermark);
        out.put_i32(
            i32::try_from(self.records.len())
                .map_err(|_| ProtocolError::InvalidValue("records length exceeds i32::MAX"))?,
        );
        out.put_slice(&self.records);
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        const FIXED: usize = 2 + 8 + 8 + 8 + 4;
        if buf.remaining() < FIXED {
            return Err(ProtocolError::UnexpectedEof {
                needed: FIXED - buf.remaining(),
            });
        }
        let error_code = buf.get_i16();
        let leader_hint = buf.get_i64();
        let log_start_offset = buf.get_i64();
        let high_watermark = buf.get_i64();
        let len = buf.get_i32();
        let len = usize::try_from(len)
            .map_err(|_| ProtocolError::InvalidValue("negative records length"))?;
        if buf.remaining() < len {
            return Err(ProtocolError::UnexpectedEof {
                needed: len - buf.remaining(),
            });
        }
        let records = Bytes::copy_from_slice(&buf[..len]);
        buf.advance(len);
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
    use assert2::assert;

    #[test]
    fn submit_change_round_trips() {
        let req = CrabkaSubmitChangeRequest {
            records: Bytes::from_static(b"\x01\x02\x03"),
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        assert!(CrabkaSubmitChangeRequest::decode_v0(&mut cur).unwrap() == req);

        let resp = CrabkaSubmitChangeResponse {
            error_code: 1,
            leader_hint: 3,
        };
        let mut out = Vec::new();
        resp.encode_v0(&mut out);
        let mut cur: &[u8] = &out;
        assert!(CrabkaSubmitChangeResponse::decode_v0(&mut cur).unwrap() == resp);
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
        assert!(CrabkaMetadataFetchRequest::decode_v0(&mut cur).unwrap() == req);

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
        assert!(CrabkaMetadataFetchResponse::decode_v0(&mut cur).unwrap() == resp);
    }
}
