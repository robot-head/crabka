//! Crabka-private Raft RPCs over Kafka TCP framing.
//!
//! These bodies are NOT part of `crabka-protocol`'s codegen — they're
//! hand-written `encode_v0`/`decode_v0` methods living here because they're
//! controller-only and Crabka-specific.
//!
//! Api keys: 1000 `AppendEntries`, 1001 `Vote`, 1002 `InstallSnapshot`.
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
/// Forward a `Controller::submit_change` from a follower to the leader.
/// The body is the bincode-encoded `Vec<MetadataRecord>` and the response
/// carries a single `error_code` (0 = applied, non-zero = openraft /
/// metadata-validation failure).
pub const API_KEY_SUBMIT_CHANGE: i16 = 1003;

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
    /// Node id of the leader that originally authored this entry. Needed
    /// for openraft's `LogId` reconstruction on the receiver — the
    /// engine compares full `LogId` (term + `node_id` + index), and a
    /// mismatched `node_id` trips an internal debug-assert when the entry
    /// is already committed locally. Encoded as `i64` to leave room for
    /// the `NodeId = u64` range without sign issues.
    pub log_node_id: i64,
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
    /// Node id of the leader that originally authored the entry at
    /// `prev_log_index`. Required so the receiver can reconstruct the
    /// full `LogId` (term + `node_id` + index) and have it compare equal
    /// to its locally-stored copy. Sentinel `-1` mirrors the
    /// `prev_log_index = -1` "no previous entry" sentinel.
    pub prev_log_node_id: i64,
    pub leader_commit: i64,
    /// Term of the entry at `leader_commit`. Sentinel `-1` mirrors the
    /// `leader_commit = -1` "nothing committed yet" sentinel.
    pub leader_commit_term: i64,
    /// Node id of the leader that authored the `leader_commit` entry.
    pub leader_commit_node_id: i64,
    pub entries: Vec<CrabkaLogEntry>,
}

impl CrabkaAppendEntriesRequest {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i32(self.node_id);
        out.put_i64(self.term);
        out.put_i32(self.leader_id);
        out.put_i64(self.prev_log_index);
        out.put_i64(self.prev_log_term);
        out.put_i64(self.prev_log_node_id);
        out.put_i64(self.leader_commit);
        out.put_i64(self.leader_commit_term);
        out.put_i64(self.leader_commit_node_id);
        out.put_i32(
            i32::try_from(self.entries.len())
                .map_err(|_| ProtocolError::InvalidValue("entry count exceeds i32::MAX"))?,
        );
        for e in &self.entries {
            out.put_i64(e.log_index);
            out.put_i64(e.log_term);
            out.put_i64(e.log_node_id);
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
        const HEADER_LEN: usize = 4 + 8 + 4 + 8 + 8 + 8 + 8 + 8 + 8 + 4;
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
        let prev_log_node_id = buf.get_i64();
        let leader_commit = buf.get_i64();
        let leader_commit_term = buf.get_i64();
        let leader_commit_node_id = buf.get_i64();
        let entry_count = buf.get_i32();
        let entry_count = usize::try_from(entry_count)
            .map_err(|_| ProtocolError::InvalidValue("negative entry count"))?;
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            const ENTRY_HEADER_LEN: usize = 8 + 8 + 8 + 1 + 4;
            if buf.remaining() < ENTRY_HEADER_LEN {
                return Err(ProtocolError::UnexpectedEof {
                    needed: ENTRY_HEADER_LEN - buf.remaining(),
                });
            }
            let log_index = buf.get_i64();
            let log_term = buf.get_i64();
            let log_node_id = buf.get_i64();
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
                log_node_id,
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
            prev_log_node_id,
            leader_commit,
            leader_commit_term,
            leader_commit_node_id,
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
    /// Node id of the leader that authored the candidate's last log
    /// entry. Together with `last_log_index`/`last_log_term` lets the
    /// receiver reconstruct the candidate's `LogId` exactly — needed
    /// because openraft's vote/append paths cross-check full `LogId`s,
    /// not just (term, index).
    pub last_log_node_id: i64,
}

impl CrabkaVoteRequest {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i64(self.term);
        out.put_u64(self.candidate_id);
        out.put_i64(self.last_log_index);
        out.put_i64(self.last_log_term);
        out.put_i64(self.last_log_node_id);
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        const LEN: usize = 8 + 8 + 8 + 8 + 8;
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
            last_log_node_id: buf.get_i64(),
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

/// Observer metadata fetch (Component B). The body carries a
/// `fetch_offset` (openraft log index) + `max_bytes`; the response
/// carries committed `__cluster_metadata` entries encoded as Kafka
/// record batches, plus `log_start_offset` / `high_watermark` and a
/// `leader_hint` so the observer can retarget the quorum.
pub const API_KEY_METADATA_FETCH: i16 = 1004;

/// Forward-to-leader payload. Body is opaque bincode bytes representing
/// the `Vec<MetadataRecord>` to apply; the controller layer owns the
/// serde details so the wire module stays metadata-agnostic.
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
    /// 0 = success; otherwise an opaque transport-level error code (1 =
    /// not leader, 2 = metadata validation, 3 = openraft other).
    pub error_code: i16,
    /// Hint: the leader id the responder believes is current, when it
    /// itself cannot apply the change. -1 means "unknown".
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
    /// Next openraft log index the observer wants.
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
    /// 0 = success; 1 = this node cannot serve (not leader/not a voter) —
    /// consult `leader_hint`.
    pub error_code: i16,
    /// Leader id the responder believes is current; -1 = unknown.
    pub leader_hint: i64,
    /// Lowest retained log index on the responder.
    pub log_start_offset: i64,
    /// Highest committed (applied) log index on the responder.
    pub high_watermark: i64,
    /// Concatenated Kafka `RecordBatch`es (one per log entry).
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

/// Write an `i32`-length-prefixed byte field, mirroring the framing
/// `CrabkaSubmitChangeRequest` uses for its opaque `records` blob.
fn put_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ProtocolError> {
    out.put_i32(
        i32::try_from(bytes.len())
            .map_err(|_| ProtocolError::InvalidValue("length exceeds i32::MAX"))?,
    );
    out.put_slice(bytes);
    Ok(())
}

/// Read an `i32`-length-prefixed byte field written by [`put_len_prefixed`].
fn get_len_prefixed(buf: &mut &[u8]) -> Result<Bytes, ProtocolError> {
    if buf.remaining() < 4 {
        return Err(ProtocolError::UnexpectedEof {
            needed: 4 - buf.remaining(),
        });
    }
    let len = buf.get_i32();
    let len =
        usize::try_from(len).map_err(|_| ProtocolError::InvalidValue("negative length prefix"))?;
    if buf.remaining() < len {
        return Err(ProtocolError::UnexpectedEof {
            needed: len - buf.remaining(),
        });
    }
    let bytes = Bytes::copy_from_slice(&buf[..len]);
    buf.advance(len);
    Ok(bytes)
}

/// One chunk of an openraft `InstallSnapshot` RPC. `vote` and `meta` are
/// the bincode-encoded `Vote<NodeId>` / `SnapshotMeta<NodeId, Node>`;
/// `data` is the raw checkpoint-byte chunk starting at `offset`, with
/// `done` set on the final chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaInstallSnapshotRequest {
    pub vote: Bytes,
    pub meta: Bytes,
    pub offset: i64,
    pub data: Bytes,
    pub done: bool,
}

impl CrabkaInstallSnapshotRequest {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        put_len_prefixed(out, &self.vote)?;
        put_len_prefixed(out, &self.meta)?;
        out.put_i64(self.offset);
        put_len_prefixed(out, &self.data)?;
        out.put_u8(u8::from(self.done));
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        let vote = get_len_prefixed(buf)?;
        let meta = get_len_prefixed(buf)?;
        if buf.remaining() < 8 {
            return Err(ProtocolError::UnexpectedEof {
                needed: 8 - buf.remaining(),
            });
        }
        let offset = buf.get_i64();
        let data = get_len_prefixed(buf)?;
        if buf.remaining() < 1 {
            return Err(ProtocolError::UnexpectedEof { needed: 1 });
        }
        let done = buf.get_u8() != 0;
        Ok(Self {
            vote,
            meta,
            offset,
            data,
            done,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaInstallSnapshotResponse {
    /// bincode `Vote<NodeId>` the follower reports back so the leader can
    /// detect a higher term and step down.
    pub vote: Bytes,
}

impl CrabkaInstallSnapshotResponse {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        put_len_prefixed(out, &self.vote)?;
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        let vote = get_len_prefixed(buf)?;
        Ok(Self { vote })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn append_entries_round_trip() {
        let req = CrabkaAppendEntriesRequest {
            node_id: 1,
            term: 7,
            leader_id: 1,
            prev_log_index: 4,
            prev_log_term: 6,
            prev_log_node_id: 1,
            leader_commit: 3,
            leader_commit_term: 6,
            leader_commit_node_id: 1,
            entries: vec![CrabkaLogEntry {
                log_index: 5,
                log_term: 7,
                log_node_id: 1,
                payload_kind: 1,
                payload: Bytes::from_static(b"hello"),
            }],
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        let decoded = CrabkaAppendEntriesRequest::decode_v0(&mut cur).unwrap();
        assert!(decoded == req);
    }

    #[test]
    fn vote_round_trip() {
        let req = CrabkaVoteRequest {
            term: 9,
            candidate_id: 2,
            last_log_index: 100,
            last_log_term: 9,
            last_log_node_id: 2,
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        assert!(CrabkaVoteRequest::decode_v0(&mut cur).unwrap() == req);
    }

    #[test]
    fn install_snapshot_round_trip() {
        let req = CrabkaInstallSnapshotRequest {
            vote: Bytes::from_static(b"vote-bytes"),
            meta: Bytes::from_static(b"meta-bytes"),
            offset: 4096,
            data: Bytes::from_static(b"chunk-of-checkpoint"),
            done: true,
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        assert!(CrabkaInstallSnapshotRequest::decode_v0(&mut cur).unwrap() == req);

        let resp = CrabkaInstallSnapshotResponse {
            vote: Bytes::from_static(b"resp-vote"),
        };
        let mut out = Vec::new();
        resp.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        assert!(CrabkaInstallSnapshotResponse::decode_v0(&mut cur).unwrap() == resp);
    }

    #[test]
    fn metadata_fetch_request_round_trips() {
        let req = CrabkaMetadataFetchRequest {
            fetch_offset: 42,
            max_bytes: 1_048_576,
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out);
        let mut cur: &[u8] = &out;
        let got = CrabkaMetadataFetchRequest::decode_v0(&mut cur).unwrap();
        assert!(got == req);
    }

    #[test]
    fn metadata_fetch_response_round_trips() {
        let resp = CrabkaMetadataFetchResponse {
            error_code: 0,
            leader_hint: 3,
            log_start_offset: 1,
            high_watermark: 99,
            records: bytes::Bytes::from_static(b"\x01\x02\x03"),
        };
        let mut out = Vec::new();
        resp.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        let got = CrabkaMetadataFetchResponse::decode_v0(&mut cur).unwrap();
        assert!(got == resp);
    }
}
