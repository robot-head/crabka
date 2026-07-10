//! Garbage-horizon rewrite rules for checkpoint snapshots.

use std::collections::BTreeMap;

use crabka_pgkv::{KvError, KvPair, key};
use crabka_pgmvcc::{
    FROZEN_XID, INVALID_XID,
    clog::{self, XidStatus},
    version,
};

use crate::error::SubstrateError;

/// Decision for one checkpoint snapshot key/value pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RewriteDecision {
    /// Keep the original key/value pair unchanged.
    Keep,
    /// Drop the pair from the checkpoint.
    Drop,
    /// Keep the key and replace the value bytes.
    Replace(Vec<u8>),
}

/// Timestamp values used when converting xid tuple history into ts tuple history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConversionTimestampBoundary {
    /// First read timestamp that may observe the converted table.
    pub read_floor_ts: u64,
    /// Synthetic commit timestamp assigned to frozen converted tuples.
    pub synthetic_commit_ts: u64,
}

/// Lookup surface for commit-status decisions needed by horizon rewriting.
pub trait ClogLookup {
    /// Return the recorded status for `xid`.
    ///
    /// # Errors
    ///
    /// Returns [`KvError`] when a stored clog value is malformed.
    fn xid_status(&self, xid: u64) -> Result<XidStatus, KvError>;
}

impl ClogLookup for BTreeMap<u64, XidStatus> {
    fn xid_status(&self, xid: u64) -> Result<XidStatus, KvError> {
        Ok(self.get(&xid).copied().unwrap_or(XidStatus::InProgress))
    }
}

/// Rewrite one key/value pair for a checkpoint garbage horizon.
///
/// # Errors
///
/// Returns [`SubstrateError`] if a row-version tuple or clog entry is malformed.
pub fn rewrite_for_checkpoint(
    key: &[u8],
    value: &[u8],
    horizon: u64,
    clog: &impl ClogLookup,
) -> Result<RewriteDecision, SubstrateError> {
    if let Some(xid) = key::clog_xid_of(key) {
        return Ok(rewrite_clog_entry(xid, horizon));
    }

    if !is_version_key(key) {
        return Ok(RewriteDecision::Keep);
    }

    rewrite_tuple_value(value, horizon, clog).map_err(SubstrateError::Kv)
}

/// Build a lookup table from snapshot clog keys.
///
/// # Errors
///
/// Returns [`SubstrateError`] if a clog entry is malformed.
pub fn collect_clog_statuses(pairs: &[KvPair]) -> Result<BTreeMap<u64, XidStatus>, SubstrateError> {
    let mut statuses = BTreeMap::new();
    for (key, value) in pairs {
        if let Some(xid) = key::clog_xid_of(key) {
            statuses.insert(xid, clog::decode(value)?);
        }
    }
    Ok(statuses)
}

/// Rewrite one table's xid-keyed tuple versions into timestamp tuple versions.
///
/// # Errors
///
/// Returns [`SubstrateError`] when a tuple/clog entry is malformed, when any
/// unresolved prepared marker exists in the checkpoint snapshot, or when the
/// supplied synthetic commit timestamp is not below the conversion read floor.
pub fn rewrite_snapshot_pairs_for_conversion(
    pairs: Vec<KvPair>,
    table_id: u32,
    boundary: ConversionTimestampBoundary,
) -> Result<Vec<KvPair>, SubstrateError> {
    if boundary.synthetic_commit_ts == 0 || boundary.synthetic_commit_ts >= boundary.read_floor_ts {
        return Err(SubstrateError::Checkpoint(
            "conversion commit timestamp must be non-zero and below the read floor".into(),
        ));
    }

    let clog = collect_clog_statuses(&pairs)?;
    reject_prepared_markers(&clog)?;

    let mut rewritten = Vec::with_capacity(pairs.len());
    for (key, value) in pairs {
        if let Some(pair) = rewrite_pair_for_conversion(&key, &value, table_id, boundary, &clog)? {
            rewritten.push(pair);
        }
    }
    rewritten.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(rewritten)
}

fn rewrite_clog_entry(xid: u64, horizon: u64) -> RewriteDecision {
    if xid < horizon {
        return RewriteDecision::Drop;
    }
    RewriteDecision::Keep
}

fn rewrite_tuple_value(
    value: &[u8],
    horizon: u64,
    clog: &impl ClogLookup,
) -> Result<RewriteDecision, KvError> {
    if let Ok(version) = version::decode_ts_tuple(value) {
        return Ok(rewrite_timestamp_tuple(&version, horizon));
    }

    let (xmin, xmax, _) = version::decode_tuple(value)?;
    if is_obsolete_deleted_version(xmax, horizon, clog)? {
        return Ok(RewriteDecision::Drop);
    }
    if xmin >= horizon || xmin == FROZEN_XID {
        return Ok(RewriteDecision::Keep);
    }

    match clog.xid_status(xmin)? {
        XidStatus::Committed => Ok(RewriteDecision::Replace(version::freeze_tuple_xmin(value)?)),
        XidStatus::Aborted | XidStatus::InProgress | XidStatus::Prepared(_) => {
            Ok(RewriteDecision::Drop)
        }
    }
}

fn reject_prepared_markers(clog: &BTreeMap<u64, XidStatus>) -> Result<(), SubstrateError> {
    if let Some((xid, XidStatus::Prepared(global))) = clog
        .iter()
        .find(|(_, status)| matches!(status, XidStatus::Prepared(_)))
    {
        return Err(SubstrateError::Checkpoint(format!(
            "conversion blocked by in-doubt prepared xid {xid} for global xid {global}",
        )));
    }
    Ok(())
}

fn rewrite_pair_for_conversion(
    key: &[u8],
    value: &[u8],
    table_id: u32,
    boundary: ConversionTimestampBoundary,
    clog: &impl ClogLookup,
) -> Result<Option<KvPair>, SubstrateError> {
    let Some((rowid, key_shape)) = conversion_row_key(key, table_id)? else {
        return Ok(Some((key.to_vec(), value.to_vec())));
    };

    let converted = convert_tuple_value(value, boundary, clog).map_err(SubstrateError::Kv)?;
    let Some(value) = converted else {
        return Ok(None);
    };
    let start_ts = boundary.synthetic_commit_ts - 1;
    let key = match key_shape {
        VersionKeyShape::Plain => version::version_key_ts(table_id, rowid, start_ts),
        VersionKeyShape::Hash { bucket } => {
            version::hash_version_key_ts(table_id, bucket, rowid, start_ts)
        }
    };
    Ok(Some((key, value)))
}

fn convert_tuple_value(
    value: &[u8],
    boundary: ConversionTimestampBoundary,
    clog: &impl ClogLookup,
) -> Result<Option<Vec<u8>>, KvError> {
    if version::decode_ts_tuple(value).is_ok() {
        return Ok(Some(value.to_vec()));
    }

    let (xmin, xmax, row) = version::decode_tuple(value)?;
    if xmin != FROZEN_XID && !matches!(clog.xid_status(xmin)?, XidStatus::Committed) {
        return Ok(None);
    }
    if xmax != INVALID_XID {
        match clog.xid_status(xmax)? {
            XidStatus::Committed => return Ok(None),
            XidStatus::Aborted => {}
            XidStatus::InProgress | XidStatus::Prepared(_) => {
                return Err(KvError::CorruptRow(
                    "conversion cannot rewrite tuple with unresolved xmax".into(),
                ));
            }
        }
    }

    Ok(Some(version::encode_ts_tuple(
        boundary.synthetic_commit_ts - 1,
        version::TsVersionState::Committed {
            commit_ts: boundary.synthetic_commit_ts,
        },
        &row,
    )))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VersionKeyShape {
    Plain,
    Hash { bucket: u32 },
}

fn conversion_row_key(
    bytes: &[u8],
    table_id: u32,
) -> Result<Option<(u64, VersionKeyShape)>, SubstrateError> {
    if !is_version_key(bytes) {
        return Ok(None);
    }
    let row_prefix = version::row_prefix_of(bytes).map_err(SubstrateError::Kv)?;
    if row_prefix.len() == key::row_key(table_id, 0).len() {
        let Some((actual_table, rowid)) = key::table_rowid_of(row_prefix) else {
            return Ok(None);
        };
        if actual_table != table_id {
            return Ok(None);
        }
        return Ok(Some((rowid, VersionKeyShape::Plain)));
    }
    if row_prefix.len() == key::hash_row_key(table_id, 0, 0).len() {
        let Some((actual_table, bucket, rowid)) = key::table_bucket_rowid_of(row_prefix) else {
            return Ok(None);
        };
        if actual_table != table_id {
            return Ok(None);
        }
        return Ok(Some((rowid, VersionKeyShape::Hash { bucket })));
    }

    Ok(None)
}

fn rewrite_timestamp_tuple(version: &version::TsTupleVersion, horizon: u64) -> RewriteDecision {
    if version.start_ts >= horizon {
        return RewriteDecision::Keep;
    }

    match version.state {
        version::TsVersionState::Aborted => RewriteDecision::Drop,
        version::TsVersionState::Deleted { commit_ts } if commit_ts < horizon => {
            RewriteDecision::Drop
        }
        version::TsVersionState::Intent | version::TsVersionState::Committed { .. } => {
            RewriteDecision::Keep
        }
        version::TsVersionState::Deleted { .. } => RewriteDecision::Keep,
    }
}

fn is_obsolete_deleted_version(
    xmax: u64,
    horizon: u64,
    clog: &impl ClogLookup,
) -> Result<bool, KvError> {
    if xmax == INVALID_XID || xmax >= horizon {
        return Ok(false);
    }
    Ok(matches!(clog.xid_status(xmax)?, XidStatus::Committed))
}

fn is_version_key(bytes: &[u8]) -> bool {
    let Ok(row_prefix) = version::row_prefix_of(bytes) else {
        return false;
    };
    key::table_rowid_of(row_prefix).is_some() && row_prefix.len() + 8 == bytes.len()
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgmvcc::{
        clog::XidStatus,
        version::{TsVersionState, version_key_ts, version_key_xid},
        visibility::{Snapshot, satisfies_mvcc},
    };
    use crabka_pgtypes::Datum;
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn drops_dead_versions_deleted_below_horizon() {
        let mut clog = BTreeMap::new();
        clog.insert(2, XidStatus::Committed);
        clog.insert(4, XidStatus::Committed);
        let tuple = version::encode_tuple(2, 4, &[Datum::Int4(9)]);

        let decision =
            rewrite_for_checkpoint(&version_key_xid(7, 42, 2), &tuple, 10, &clog).expect("rewrite");

        assert!(decision == RewriteDecision::Drop);
    }

    #[test]
    fn keeps_versions_deleted_at_or_above_horizon() {
        let mut clog = BTreeMap::new();
        clog.insert(2, XidStatus::Committed);
        clog.insert(10, XidStatus::Committed);
        let tuple = version::encode_tuple(2, 10, &[Datum::Int4(9)]);

        let decision =
            rewrite_for_checkpoint(&version_key_xid(7, 42, 2), &tuple, 10, &clog).expect("rewrite");

        assert!(matches!(decision, RewriteDecision::Replace(_)));
    }

    #[test]
    fn freezes_committed_old_xmin_without_touching_row_or_xmax() {
        let mut clog = BTreeMap::new();
        clog.insert(3, XidStatus::Committed);
        let row = vec![Datum::Int4(1), Datum::Text("a".into())];
        let tuple = version::encode_tuple(3, INVALID_XID, &row);

        let RewriteDecision::Replace(rewritten) =
            rewrite_for_checkpoint(&version_key_xid(7, 1, 3), &tuple, 9, &clog).expect("rewrite")
        else {
            panic!("expected tuple rewrite");
        };

        assert!(version::decode_tuple(&rewritten).expect("decode") == (FROZEN_XID, 0, row));
    }

    #[test]
    fn drops_aborted_old_xmin_versions() {
        let mut clog = BTreeMap::new();
        clog.insert(3, XidStatus::Aborted);
        let tuple = version::encode_tuple(3, INVALID_XID, &[Datum::Int4(1)]);

        let decision =
            rewrite_for_checkpoint(&version_key_xid(7, 1, 3), &tuple, 9, &clog).expect("rewrite");

        assert!(decision == RewriteDecision::Drop);
    }

    #[test]
    fn drops_clog_entries_below_horizon() {
        let key = key::clog_key(8);
        let decision = rewrite_for_checkpoint(&key, &[1], 9, &BTreeMap::new()).expect("rewrite");

        assert!(decision == RewriteDecision::Drop);
    }

    #[test]
    fn drops_resolved_aborted_timestamp_intents_below_horizon() {
        let tuple = version::encode_ts_tuple(4, TsVersionState::Aborted, &[Datum::Int4(1)]);

        let decision =
            rewrite_for_checkpoint(&version_key_ts(7, 1, 4), &tuple, 9, &BTreeMap::new())
                .expect("rewrite");

        assert!(decision == RewriteDecision::Drop);
    }

    #[test]
    fn keeps_pending_timestamp_intents_until_primary_resolution_is_known() {
        let tuple = version::encode_ts_tuple(4, TsVersionState::Intent, &[Datum::Int4(1)]);

        let decision =
            rewrite_for_checkpoint(&version_key_ts(7, 1, 4), &tuple, 9, &BTreeMap::new())
                .expect("rewrite");

        assert!(decision == RewriteDecision::Keep);
    }

    #[test]
    fn drops_timestamp_delete_markers_below_horizon() {
        let tuple = version::encode_ts_tuple(
            4,
            TsVersionState::Deleted { commit_ts: 8 },
            &[Datum::Int4(1)],
        );

        let decision =
            rewrite_for_checkpoint(&version_key_ts(7, 1, 4), &tuple, 9, &BTreeMap::new())
                .expect("rewrite");

        assert!(decision == RewriteDecision::Drop);
    }

    #[test]
    fn keeps_timestamp_delete_markers_at_or_above_horizon() {
        let tuple = version::encode_ts_tuple(
            4,
            TsVersionState::Deleted { commit_ts: 9 },
            &[Datum::Int4(1)],
        );

        let decision =
            rewrite_for_checkpoint(&version_key_ts(7, 1, 4), &tuple, 9, &BTreeMap::new())
                .expect("rewrite");

        assert!(decision == RewriteDecision::Keep);
    }

    #[test]
    fn conversion_rewrites_committed_xid_tuple_to_visible_timestamp_tuple() {
        let pairs = vec![
            (key::clog_key(3), vec![1]),
            (
                version_key_xid(7, 42, 3),
                version::encode_tuple(3, INVALID_XID, &[Datum::Int4(11)]),
            ),
        ];

        let rewritten = rewrite_snapshot_pairs_for_conversion(
            pairs,
            7,
            ConversionTimestampBoundary {
                read_floor_ts: 100,
                synthetic_commit_ts: 90,
            },
        )
        .expect("conversion rewrite");

        let (_, converted_value) = rewritten
            .iter()
            .find(|(key, _)| *key == version_key_ts(7, 42, 89))
            .expect("converted tuple");
        let converted = version::decode_ts_tuple(converted_value).expect("timestamp tuple");
        assert!(converted.start_ts == 89);
        assert!(converted.state == TsVersionState::Committed { commit_ts: 90 });
        assert!(converted.row == vec![Datum::Int4(11)]);
        assert!(crabka_pgmvcc::visibility::satisfies_ts(
            100,
            converted.state
        ));
    }

    #[test]
    fn conversion_rejects_in_doubt_prepared_marker() {
        let pairs = vec![
            (key::clog_key(3), prepared_status(50)),
            (
                version_key_xid(7, 42, 3),
                version::encode_tuple(3, INVALID_XID, &[Datum::Int4(11)]),
            ),
        ];

        let err = rewrite_snapshot_pairs_for_conversion(
            pairs,
            7,
            ConversionTimestampBoundary {
                read_floor_ts: 100,
                synthetic_commit_ts: 90,
            },
        )
        .expect_err("prepared marker rejects conversion");

        assert!(format!("{err}").contains("in-doubt prepared xid 3"));
    }

    #[test]
    fn conversion_preserves_unconverted_table_xid_tuple() {
        let old_key = version_key_xid(8, 42, 3);
        let old_value = version::encode_tuple(3, INVALID_XID, &[Datum::Int4(11)]);
        let pairs = vec![(old_key.clone(), old_value.clone())];

        let rewritten = rewrite_snapshot_pairs_for_conversion(
            pairs,
            7,
            ConversionTimestampBoundary {
                read_floor_ts: 100,
                synthetic_commit_ts: 90,
            },
        )
        .expect("conversion rewrite");

        assert!(rewritten == vec![(old_key, old_value)]);
    }

    proptest! {
        #[test]
        fn prop_timestamp_rewrite_preserves_visibility_at_or_above_horizon(
            start_ts in 2_u64..20,
            commit_delta in 1_u64..8,
            state_selector in 0_u8..4,
            horizon in 2_u64..20,
            read_delta in 0_u64..8,
        ) {
            let state = match state_selector {
                0 => TsVersionState::Intent,
                1 => TsVersionState::Aborted,
                2 => TsVersionState::Committed { commit_ts: start_ts + commit_delta },
                _ => TsVersionState::Deleted { commit_ts: start_ts + commit_delta },
            };
            let tuple = version::encode_ts_tuple(start_ts, state, &[Datum::Int4(1)]);
            let read_ts = horizon + read_delta;
            let original = crabka_pgmvcc::visibility::satisfies_ts(read_ts, state);

            let decision = rewrite_for_checkpoint(&version_key_ts(7, 1, start_ts), &tuple, horizon, &BTreeMap::new())
                .expect("rewrite");

            match decision {
                RewriteDecision::Keep => {
                    let kept = version::decode_ts_tuple(&tuple).expect("decode");
                    prop_assert_eq!(crabka_pgmvcc::visibility::satisfies_ts(read_ts, kept.state), original);
                }
                RewriteDecision::Replace(_) => prop_assert!(false, "timestamp tuples are not rewritten in place"),
                RewriteDecision::Drop => prop_assert!(!original),
            }
        }
    }

    proptest! {
        #[test]
        fn prop_rewrite_preserves_visibility_at_or_above_horizon(
            xmin in 2_u64..20,
            xmax in 0_u64..20,
            xmin_status in status_strategy(),
            xmax_status in status_strategy(),
            horizon in 2_u64..20,
            snapshot_xmax_delta in 0_u64..8,
        ) {
            let xmax = if xmax == xmin { INVALID_XID } else { xmax };
            let mut clog = BTreeMap::new();
            clog.insert(xmin, xmin_status);
            if xmax != INVALID_XID {
                clog.insert(xmax, xmax_status);
            }
            let tuple = version::encode_tuple(xmin, xmax, &[Datum::Int4(1)]);
            let snapshot = Snapshot {
                xmin: horizon,
                xmax: horizon.saturating_add(snapshot_xmax_delta).saturating_add(1),
                xip: Vec::new(),
            };
            let original = satisfies_mvcc(xmin, xmax, &snapshot, None, |xid| {
                Ok(clog.get(&xid).copied().unwrap_or(XidStatus::InProgress))
            }).expect("original visibility");

            let decision = rewrite_for_checkpoint(&version_key_xid(7, 1, xmin), &tuple, horizon, &clog)
                .expect("rewrite");
            match decision {
                RewriteDecision::Keep => {
                    let kept = satisfies_mvcc(xmin, xmax, &snapshot, None, |xid| {
                        Ok(clog.get(&xid).copied().unwrap_or(XidStatus::InProgress))
                    }).expect("kept visibility");
                    prop_assert_eq!(kept, original);
                }
                RewriteDecision::Replace(rewritten) => {
                    let (new_xmin, new_xmax, _) = version::decode_tuple(&rewritten).expect("decode");
                    let rewritten_visible = satisfies_mvcc(new_xmin, new_xmax, &snapshot, None, |xid| {
                        if xid < horizon {
                            return Ok(XidStatus::InProgress);
                        }
                        Ok(clog.get(&xid).copied().unwrap_or(XidStatus::InProgress))
                    }).expect("rewritten visibility");
                    prop_assert_eq!(rewritten_visible, original);
                }
                RewriteDecision::Drop => {
                    prop_assert!(!original);
                }
            }
        }
    }

    fn status_strategy() -> impl Strategy<Value = XidStatus> {
        prop_oneof![
            Just(XidStatus::Committed),
            Just(XidStatus::Aborted),
            Just(XidStatus::InProgress),
        ]
    }

    fn prepared_status(global_xid: u64) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(9);
        bytes.push(3);
        bytes.extend_from_slice(&global_xid.to_be_bytes());
        bytes
    }
}
