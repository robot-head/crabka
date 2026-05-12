//! Crabka-private Raft RPCs over Kafka TCP framing.
//!
//! These bodies are NOT part of `crabka-protocol`'s codegen — they're
//! hand-written `encode_v0`/`decode_v0` methods living here because they're
//! controller-only and Crabka-specific.
//!
//! Api keys: 1000 `AppendEntries`, 1001 `Vote`, 1002 `InstallSnapshot` (stub).
//!
//! Explicit `encode_v0`/`decode_v0` methods are used rather than the
//! generic `Encode`/`Decode` traits from `crabka-protocol` because the
//! traits target schema-versioned codegen messages; these RPCs are fixed
//! at v0 and live entirely inside the Crabka controller.

use bytes::{Buf, BufMut, Bytes};

use crabka_protocol::ProtocolError;

use crate::types::NodeId;

pub const API_KEY_APPEND_ENTRIES: i16 = 1000;
pub const API_KEY_VOTE: i16 = 1001;
pub const API_KEY_INSTALL_SNAPSHOT: i16 = 1002;

/// Payload kind discriminator inside `AppendEntries.entries[].payload`.
#[repr(i8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadKind {
    Blank = 0,
    Normal = 1,
    Membership = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaLogEntry {
    pub log_index: i64,
    pub log_term: i64,
    pub payload_kind: i8,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaAppendEntriesRequest {
    pub node_id: i32,
    pub term: i64,
    pub leader_id: i32,
    pub prev_log_index: i64,
    pub prev_log_term: i64,
    pub leader_commit: i64,
    pub entries: Vec<CrabkaLogEntry>,
}

impl CrabkaAppendEntriesRequest {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i32(self.node_id);
        out.put_i64(self.term);
        out.put_i32(self.leader_id);
        out.put_i64(self.prev_log_index);
        out.put_i64(self.prev_log_term);
        out.put_i64(self.leader_commit);
        out.put_i32(
            i32::try_from(self.entries.len())
                .map_err(|_| ProtocolError::InvalidValue("entry count exceeds i32::MAX"))?,
        );
        for e in &self.entries {
            out.put_i64(e.log_index);
            out.put_i64(e.log_term);
            out.put_i8(e.payload_kind);
            out.put_i32(
                i32::try_from(e.payload.len())
                    .map_err(|_| ProtocolError::InvalidValue("payload length exceeds i32::MAX"))?,
            );
            out.put_slice(&e.payload);
        }
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        const HEADER_LEN: usize = 4 + 8 + 4 + 8 + 8 + 8 + 4;
        if buf.remaining() < HEADER_LEN {
            return Err(ProtocolError::UnexpectedEof {
                needed: HEADER_LEN - buf.remaining(),
            });
        }
        let node_id = buf.get_i32();
        let term = buf.get_i64();
        let leader_id = buf.get_i32();
        let prev_log_index = buf.get_i64();
        let prev_log_term = buf.get_i64();
        let leader_commit = buf.get_i64();
        let entry_count = buf.get_i32();
        let entry_count = usize::try_from(entry_count)
            .map_err(|_| ProtocolError::InvalidValue("negative entry count"))?;
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            const ENTRY_HEADER_LEN: usize = 8 + 8 + 1 + 4;
            if buf.remaining() < ENTRY_HEADER_LEN {
                return Err(ProtocolError::UnexpectedEof {
                    needed: ENTRY_HEADER_LEN - buf.remaining(),
                });
            }
            let log_index = buf.get_i64();
            let log_term = buf.get_i64();
            let payload_kind = buf.get_i8();
            let payload_len = buf.get_i32();
            let payload_len = usize::try_from(payload_len)
                .map_err(|_| ProtocolError::InvalidValue("negative payload length"))?;
            if buf.remaining() < payload_len {
                return Err(ProtocolError::UnexpectedEof {
                    needed: payload_len - buf.remaining(),
                });
            }
            let payload = Bytes::copy_from_slice(&buf[..payload_len]);
            buf.advance(payload_len);
            entries.push(CrabkaLogEntry {
                log_index,
                log_term,
                payload_kind,
                payload,
            });
        }
        Ok(Self {
            node_id,
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            leader_commit,
            entries,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaAppendEntriesResponse {
    pub success: bool,
    pub term: i64,
    pub last_log_index: i64,
}

impl CrabkaAppendEntriesResponse {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i8(i8::from(self.success));
        out.put_i64(self.term);
        out.put_i64(self.last_log_index);
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        const LEN: usize = 1 + 8 + 8;
        if buf.remaining() < LEN {
            return Err(ProtocolError::UnexpectedEof {
                needed: LEN - buf.remaining(),
            });
        }
        let success = buf.get_i8() != 0;
        let term = buf.get_i64();
        let last_log_index = buf.get_i64();
        Ok(Self {
            success,
            term,
            last_log_index,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaVoteRequest {
    pub term: i64,
    pub candidate_id: NodeId,
    pub last_log_index: i64,
    pub last_log_term: i64,
}

impl CrabkaVoteRequest {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i64(self.term);
        out.put_u64(self.candidate_id);
        out.put_i64(self.last_log_index);
        out.put_i64(self.last_log_term);
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        const LEN: usize = 8 + 8 + 8 + 8;
        if buf.remaining() < LEN {
            return Err(ProtocolError::UnexpectedEof {
                needed: LEN - buf.remaining(),
            });
        }
        Ok(Self {
            term: buf.get_i64(),
            candidate_id: buf.get_u64(),
            last_log_index: buf.get_i64(),
            last_log_term: buf.get_i64(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaVoteResponse {
    pub vote_granted: bool,
    pub term: i64,
}

impl CrabkaVoteResponse {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i8(i8::from(self.vote_granted));
        out.put_i64(self.term);
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        const LEN: usize = 1 + 8;
        if buf.remaining() < LEN {
            return Err(ProtocolError::UnexpectedEof {
                needed: LEN - buf.remaining(),
            });
        }
        Ok(Self {
            vote_granted: buf.get_i8() != 0,
            term: buf.get_i64(),
        })
    }
}

/// Stub for the deferred snapshot path. Encoded as a single byte `0`
/// so the wire stays well-defined.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CrabkaInstallSnapshotRequest;

impl CrabkaInstallSnapshotRequest {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_u8(0);
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        if buf.remaining() < 1 {
            return Err(ProtocolError::UnexpectedEof {
                needed: 1 - buf.remaining(),
            });
        }
        let _ = buf.get_u8();
        Ok(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaInstallSnapshotResponse {
    pub error_code: i16,
}

impl CrabkaInstallSnapshotResponse {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i16(self.error_code);
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        const LEN: usize = 2;
        if buf.remaining() < LEN {
            return Err(ProtocolError::UnexpectedEof {
                needed: LEN - buf.remaining(),
            });
        }
        Ok(Self {
            error_code: buf.get_i16(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_entries_round_trip() {
        let req = CrabkaAppendEntriesRequest {
            node_id: 1,
            term: 7,
            leader_id: 1,
            prev_log_index: 4,
            prev_log_term: 6,
            leader_commit: 3,
            entries: vec![CrabkaLogEntry {
                log_index: 5,
                log_term: 7,
                payload_kind: 1,
                payload: Bytes::from_static(b"hello"),
            }],
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        let decoded = CrabkaAppendEntriesRequest::decode_v0(&mut cur).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn vote_round_trip() {
        let req = CrabkaVoteRequest {
            term: 9,
            candidate_id: 2,
            last_log_index: 100,
            last_log_term: 9,
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        assert_eq!(CrabkaVoteRequest::decode_v0(&mut cur).unwrap(), req);
    }

    #[test]
    fn install_snapshot_stub_round_trip() {
        let req = CrabkaInstallSnapshotRequest;
        let mut out = Vec::new();
        req.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        assert_eq!(
            CrabkaInstallSnapshotRequest::decode_v0(&mut cur).unwrap(),
            req
        );
    }
}
