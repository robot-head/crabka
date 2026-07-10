//! Xid-keyed tuple encoding for Crabka Gres MVCC.
//!
//! A rowid's versions live under `crabka_pgkv::key::row_key(table, rowid)` with
//! an ascending xid suffix, so versions sort chronologically. The value carries
//! the xmin/xmax header and the row payload.

use crabka_pgkv::KvError;
use crabka_pgtypes::Datum;
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned, byteorder::big_endian::U64,
};

use crate::xid::{FROZEN_XID, Xid};

// ── SP5 xid-keyed tuple format ────────────────────────────────────────────────

/// SP5 version key: the row key followed by the creating xid (big-endian,
/// ascending). A rowid's versions all share `crabka_pgkv::key::row_key(table, rowid)`.
#[must_use]
pub fn version_key_xid(table_id: u32, rowid: u64, xid: u64) -> Vec<u8> {
    let mut k = crabka_pgkv::key::row_key(table_id, rowid);
    k.extend_from_slice(U64::new(xid).as_bytes());
    k
}

/// Xid-keyed version key for a hash-sharded row. Versions sort within
/// `(table_id, bucket, rowid)` and bucket is the leading interval component.
#[must_use]
pub fn hash_version_key_xid(table_id: u32, bucket: u32, rowid: u64, xid: u64) -> Vec<u8> {
    let mut k = crabka_pgkv::key::hash_row_key(table_id, bucket, rowid);
    k.extend_from_slice(U64::new(xid).as_bytes());
    k
}

/// Timestamp-transaction version key: the row key followed by the creating
/// transaction's `start_ts` (big-endian, ascending).
#[must_use]
pub fn version_key_ts(table_id: u32, rowid: u64, start_ts: u64) -> Vec<u8> {
    let mut k = crabka_pgkv::key::row_key(table_id, rowid);
    k.extend_from_slice(U64::new(start_ts).as_bytes());
    k
}

/// Timestamp-transaction version key for a hash-sharded row.
#[must_use]
pub fn hash_version_key_ts(table_id: u32, bucket: u32, rowid: u64, start_ts: u64) -> Vec<u8> {
    let mut k = crabka_pgkv::key::hash_row_key(table_id, bucket, rowid);
    k.extend_from_slice(U64::new(start_ts).as_bytes());
    k
}

/// The row-key prefix of a version key (everything but the 8-byte xid suffix).
pub fn row_prefix_of(key: &[u8]) -> Result<&[u8], KvError> {
    if key.len() < 8 {
        return Err(KvError::CorruptRow("version key too short".into()));
    }
    Ok(&key[..key.len() - 8])
}

/// The creating xid encoded in a version key's 8-byte suffix.
pub fn xid_of_key(key: &[u8]) -> Result<u64, KvError> {
    let (_, xid) = U64::read_from_suffix(key)
        .map_err(|_| KvError::CorruptRow("version key too short".into()))?;
    Ok(xid.get())
}

/// Fixed 17-byte tuple header: tag + big-endian xmin/xmax. `#[repr(C)]` with
/// alignment-1 fields packs with no padding, matching the on-disk layout.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct TupleHeader {
    tag: u8,
    xmin: U64,
    xmax: U64,
}

const T_TUPLE: u8 = 1;
const T_TS_TUPLE: u8 = 2;
const TS_STATE_INTENT: u8 = 1;
const TS_STATE_COMMITTED: u8 = 2;
const TS_STATE_ABORTED: u8 = 3;
const TS_STATE_DELETED: u8 = 4;

/// Timestamp-transaction state carried by a sharded-table tuple version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TsVersionState {
    /// Durable prewrite intent. Readers must resolve it through the primary range
    /// before deciding visibility.
    Intent,
    /// Durable commit marker; visible when `commit_ts <= read_ts`.
    Committed { commit_ts: u64 },
    /// Durable abort marker; never visible.
    Aborted,
    /// Durable delete marker; hides older committed versions when `commit_ts <= read_ts`.
    Deleted { commit_ts: u64 },
}

impl TsVersionState {
    /// Return the state code persisted in the tuple header.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Intent => TS_STATE_INTENT,
            Self::Committed { .. } => TS_STATE_COMMITTED,
            Self::Aborted => TS_STATE_ABORTED,
            Self::Deleted { .. } => TS_STATE_DELETED,
        }
    }
}

/// Decoded timestamp-transaction tuple version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TsTupleVersion {
    /// Transaction start timestamp and version-key suffix.
    pub start_ts: u64,
    /// Intent/commit/abort state.
    pub state: TsVersionState,
    /// Decoded row payload.
    pub row: Vec<Datum>,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct TsTupleHeader {
    tag: u8,
    state: u8,
    start_ts: U64,
    commit_ts: U64,
}

/// Encode a tuple version: a 1-byte tag, the `xmin`/`xmax` header, then the row.
/// `xmax == INVALID_XID` (0) marks a live version. A delete keeps the row bytes
/// and sets `xmax` (Postgres retains the tuple until vacuum).
#[must_use]
pub fn encode_tuple(xmin: u64, xmax: u64, row: &[Datum]) -> Vec<u8> {
    let header = TupleHeader {
        tag: T_TUPLE,
        xmin: U64::new(xmin),
        xmax: U64::new(xmax),
    };
    let mut out = Vec::with_capacity(17 + row.len() * 8);
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(&crabka_pgkv::rowenc::encode_row(row));
    out
}

/// Encode a sharded-table timestamp version.
#[must_use]
pub fn encode_ts_tuple(start_ts: u64, state: TsVersionState, row: &[Datum]) -> Vec<u8> {
    let commit_ts = match state {
        TsVersionState::Committed { commit_ts } | TsVersionState::Deleted { commit_ts } => {
            commit_ts
        }
        TsVersionState::Intent | TsVersionState::Aborted => 0,
    };
    let header = TsTupleHeader {
        tag: T_TS_TUPLE,
        state: state.code(),
        start_ts: U64::new(start_ts),
        commit_ts: U64::new(commit_ts),
    };
    let mut out = Vec::with_capacity(18 + row.len() * 8);
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(&crabka_pgkv::rowenc::encode_row(row));
    out
}

/// Decode a tuple version into `(xmin, xmax, row)`.
pub fn decode_tuple(bytes: &[u8]) -> Result<(u64, u64, Vec<Datum>), KvError> {
    let (header, rest) = TupleHeader::ref_from_prefix(bytes)
        .map_err(|_| KvError::CorruptRow("bad tuple header".into()))?;
    if header.tag != T_TUPLE {
        return Err(KvError::CorruptRow("bad tuple header".into()));
    }
    let row = crabka_pgkv::rowenc::decode_row(rest)?;
    Ok((header.xmin.get(), header.xmax.get(), row))
}

/// Decode a timestamp-transaction tuple version.
pub fn decode_ts_tuple(bytes: &[u8]) -> Result<TsTupleVersion, KvError> {
    let (header, rest) = TsTupleHeader::ref_from_prefix(bytes)
        .map_err(|_| KvError::CorruptRow("bad timestamp tuple header".into()))?;
    if header.tag != T_TS_TUPLE {
        return Err(KvError::CorruptRow("bad timestamp tuple header".into()));
    }
    let state = match header.state {
        TS_STATE_INTENT if header.commit_ts.get() == 0 => TsVersionState::Intent,
        TS_STATE_COMMITTED if header.commit_ts.get() > header.start_ts.get() => {
            TsVersionState::Committed {
                commit_ts: header.commit_ts.get(),
            }
        }
        TS_STATE_ABORTED if header.commit_ts.get() == 0 => TsVersionState::Aborted,
        TS_STATE_DELETED if header.commit_ts.get() > header.start_ts.get() => {
            TsVersionState::Deleted {
                commit_ts: header.commit_ts.get(),
            }
        }
        _ => return Err(KvError::CorruptRow("invalid timestamp tuple state".into())),
    };
    let row = crabka_pgkv::rowenc::decode_row(rest)?;
    Ok(TsTupleVersion {
        start_ts: header.start_ts.get(),
        state,
        row,
    })
}

/// Rewrite only the tuple header's `xmin`, preserving `xmax` and row bytes.
///
/// This is the checkpoint/vacuum fast path: callers can freeze an old committed
/// tuple without decoding and re-encoding its row payload.
pub fn rewrite_tuple_xmin(bytes: &[u8], xmin: Xid) -> Result<Vec<u8>, KvError> {
    let (header, _) = TupleHeader::ref_from_prefix(bytes)
        .map_err(|_| KvError::CorruptRow("bad tuple header".into()))?;
    if header.tag != T_TUPLE {
        return Err(KvError::CorruptRow("bad tuple header".into()));
    }

    let mut rewritten = bytes.to_vec();
    rewritten[1..9].copy_from_slice(U64::new(xmin).as_bytes());
    Ok(rewritten)
}

/// Rewrite a tuple's `xmin` to [`FROZEN_XID`].
pub fn freeze_tuple_xmin(bytes: &[u8]) -> Result<Vec<u8>, KvError> {
    rewrite_tuple_xmin(bytes, FROZEN_XID)
}

#[cfg(test)]
mod tests {
    use crabka_pgtypes::Datum;

    use super::*;

    #[test]
    fn tuple_header_is_packed_17_bytes() {
        assert_eq!(core::mem::size_of::<TupleHeader>(), 17);
    }

    #[test]
    fn timestamp_tuple_header_is_packed_18_bytes() {
        assert_eq!(core::mem::size_of::<TsTupleHeader>(), 18);
    }

    #[test]
    fn tuple_header_layout_matches_manual_be() {
        use zerocopy::{IntoBytes, byteorder::big_endian::U64};
        let h = TupleHeader {
            tag: T_TUPLE,
            xmin: U64::new(5),
            xmax: U64::new(9),
        };
        let mut manual = vec![T_TUPLE];
        manual.extend_from_slice(&5u64.to_be_bytes());
        manual.extend_from_slice(&9u64.to_be_bytes());
        assert_eq!(h.as_bytes(), manual.as_slice());
    }

    #[test]
    fn row_prefix_of_strips_xid_suffix() {
        let k = version_key_xid(7, 42, 5);
        let expected = crabka_pgkv::key::row_key(7, 42);
        assert_eq!(row_prefix_of(&k).expect("valid key"), expected.as_slice());
    }

    #[test]
    fn row_prefix_of_rejects_too_short() {
        assert!(row_prefix_of(&[0u8; 4]).is_err());
    }

    #[test]
    fn row_prefix_of_at_exactly_the_suffix_length_is_an_empty_prefix() {
        // A key of exactly the 8-byte xid suffix has an EMPTY row prefix — it is
        // the boundary, not an error (only strictly-shorter keys are rejected).
        assert_eq!(row_prefix_of(&[0u8; 8]).expect("8 bytes is valid"), b"");
    }

    #[test]
    fn version_key_xid_is_rowid_prefix_plus_ascending_xid() {
        let prefix = crabka_pgkv::key::row_key(7, 42);
        let k = version_key_xid(7, 42, 100);
        assert!(k.starts_with(&prefix));
        assert_eq!(xid_of_key(&k).expect("xid"), 100);
        // ascending: a higher xid sorts after a lower one for the same row.
        assert!(version_key_xid(7, 42, 100) < version_key_xid(7, 42, 200));
        // row_prefix_of strips the 8-byte xid suffix back to the row key.
        assert_eq!(row_prefix_of(&k).expect("prefix"), prefix.as_slice());
    }

    #[test]
    fn tuple_roundtrips_header_and_row() {
        let row = vec![Datum::Int4(1), Datum::Text("a".into())];
        let bytes = encode_tuple(5, crate::xid::INVALID_XID, &row);
        assert_eq!(decode_tuple(&bytes).expect("decode"), (5, 0, row));
        // a deleted/superseded version keeps its row bytes and carries xmax.
        let bytes = encode_tuple(5, 9, &[Datum::Int4(1)]);
        assert_eq!(
            decode_tuple(&bytes).expect("decode"),
            (5, 9, vec![Datum::Int4(1)])
        );
    }

    #[test]
    fn timestamp_tuple_roundtrips_intent_commit_and_abort_states() {
        let row = vec![Datum::Int4(1), Datum::Text("a".into())];
        let intent = encode_ts_tuple(5, TsVersionState::Intent, &row);
        assert_eq!(
            decode_ts_tuple(&intent).expect("intent"),
            TsTupleVersion {
                start_ts: 5,
                state: TsVersionState::Intent,
                row: row.clone(),
            }
        );

        let committed = encode_ts_tuple(5, TsVersionState::Committed { commit_ts: 8 }, &row);
        assert_eq!(
            decode_ts_tuple(&committed).expect("committed"),
            TsTupleVersion {
                start_ts: 5,
                state: TsVersionState::Committed { commit_ts: 8 },
                row: row.clone(),
            }
        );

        let aborted = encode_ts_tuple(5, TsVersionState::Aborted, &row);
        assert_eq!(
            decode_ts_tuple(&aborted).expect("aborted"),
            TsTupleVersion {
                start_ts: 5,
                state: TsVersionState::Aborted,
                row,
            }
        );
    }

    #[test]
    fn timestamp_tuple_rejects_impossible_commit_order() {
        let encoded = encode_ts_tuple(5, TsVersionState::Committed { commit_ts: 8 }, &[]);
        let mut bad = encoded.clone();
        bad[10..18].copy_from_slice(&5_u64.to_be_bytes());

        assert!(decode_ts_tuple(&bad).is_err());
        assert!(decode_ts_tuple(&encoded).is_ok());
    }

    #[test]
    fn version_key_ts_sorts_by_start_timestamp() {
        assert!(version_key_ts(7, 42, 100) < version_key_ts(7, 42, 200));
        assert_eq!(
            xid_of_key(&version_key_ts(7, 42, 100)).expect("suffix"),
            100
        );
    }

    #[test]
    fn decode_tuple_rejects_corrupt() {
        assert!(decode_tuple(&[]).is_err());
        assert!(decode_tuple(&[99, 0, 0, 0, 0, 0, 0, 0, 0]).is_err()); // bad tag
        assert!(decode_tuple(&[1, 0, 0]).is_err()); // too short for header
    }

    #[test]
    fn freeze_tuple_xmin_rewrites_only_the_xmin_header() {
        let row = vec![Datum::Int4(11), Datum::Text("kept".into())];
        let original = encode_tuple(55, 99, &row);

        let frozen = freeze_tuple_xmin(&original).expect("freeze tuple");

        assert_eq!(
            decode_tuple(&frozen).expect("decode frozen"),
            (crate::xid::FROZEN_XID, 99, row)
        );
        assert_eq!(frozen[0], original[0]);
        assert_eq!(&frozen[9..], &original[9..]);
    }

    #[test]
    fn rewrite_tuple_xmin_rejects_corrupt_tuple_bytes() {
        assert!(freeze_tuple_xmin(&[]).is_err());

        let mut bad_tag = encode_tuple(55, 99, &[Datum::Int4(11)]);
        bad_tag[0] = 99;
        assert!(freeze_tuple_xmin(&bad_tag).is_err());
    }
}
