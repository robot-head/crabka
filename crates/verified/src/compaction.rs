//! KIP-534 log-compaction decision core, extracted from `crabka-log` so
//! Creusot can verify it. The host crate re-exports these; the stateright
//! model in `crabka-log/src/compact_model.rs` drives these exact functions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Per-record facts the retain decision needs.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct RecordMeta {
    /// Whether the record has a key.
    pub has_key: bool,
    /// Whether the record has a non-null value.
    pub has_value: bool,
}

/// Per-batch facts the retain decision needs.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct BatchMeta {
    /// Whether the batch is a transactional control batch.
    pub is_control: bool,
    /// Producer id for transactional batches; negative for non-transactional.
    pub producer_id: i64,
    /// The batch's existing delete horizon (`base_timestamp` when bit 6 is
    /// set), `None` if the batch has never been stamped.
    pub existing_horizon: Option<i64>,
}

/// Whether a producer's transactional DATA still survives compaction.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TxnDataState {
    /// `producer_id < 0`: not a transactional producer.
    NotTransactional,
    /// At least one of this producer's data records survives compaction.
    DataSurvives,
    /// All of this producer's data records have been compacted away.
    DataFullyGone,
}

/// What to do with a record during the rewrite pass.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum RetainDecision {
    /// Keep the record as-is.
    Keep,
    /// Keep the record but stamp its batch with this delete horizon
    /// (`base_timestamp = horizon`, bit 6 set).
    SetHorizon(i64),
    /// Drop the record.
    Delete,
}

/// Compute the delete horizon timestamp: `now + delete.retention.ms`. The
/// tombstone/marker is retained until wall-clock reaches this value.
#[cfg(creusot)]
#[logic]
pub fn compute_horizon_model(now_ms: i64, delete_retention_ms: i64) -> Int {
    pearlite! {
        if now_ms@ + delete_retention_ms@ > 9223372036854775807 {
            9223372036854775807
        } else if now_ms@ + delete_retention_ms@ < -9223372036854775807 - 1 {
            -9223372036854775807 - 1
        } else {
            now_ms@ + delete_retention_ms@
        }
    }
}

#[ensures(result@ == compute_horizon_model(now_ms, delete_retention_ms))]
#[ensures(result@ == if now_ms@ + delete_retention_ms@ > 9223372036854775807 {
    9223372036854775807
} else if now_ms@ + delete_retention_ms@ < -9223372036854775807 - 1 {
    -9223372036854775807 - 1
} else {
    now_ms@ + delete_retention_ms@
})]
#[must_use]
pub const fn compute_horizon(now_ms: i64, delete_retention_ms: i64) -> i64 {
    now_ms.saturating_add(delete_retention_ms)
}

/// The single per-record KIP-534 retain decision.
///
/// Control batches (txn commit/abort markers) are retained as long as their
/// transaction's data survives; once the data is fully compacted away the
/// marker ages out via the delete horizon. Data records dedup newest-wins;
/// tombstones (null value) age out via the delete horizon once they are the
/// newest entry for their key.
#[ensures(batch.is_control && (txn == TxnDataState::DataSurvives || txn == TxnDataState::NotTransactional)
    ==> result == RetainDecision::Keep)]
#[ensures(batch.is_control && txn == TxnDataState::DataFullyGone && batch.existing_horizon == None
    ==> exists<h: i64> result == RetainDecision::SetHorizon(h)
        && h@ == compute_horizon_model(now_ms, delete_retention_ms))]
#[ensures(forall<h: i64> batch.is_control && txn == TxnDataState::DataFullyGone
        && batch.existing_horizon == Some(h)
    ==> result == (if now_ms@ >= h@ { RetainDecision::Delete } else { RetainDecision::Keep }))]
#[ensures(!batch.is_control && !rec.has_key ==> result == RetainDecision::Delete)]
#[ensures(!batch.is_control && rec.has_key && !is_newest_for_key ==> result == RetainDecision::Delete)]
#[ensures(!batch.is_control && rec.has_key && is_newest_for_key && rec.has_value
    ==> result == RetainDecision::Keep)]
#[ensures(!batch.is_control && rec.has_key && is_newest_for_key && !rec.has_value
        && batch.existing_horizon == None
    ==> exists<h: i64> result == RetainDecision::SetHorizon(h)
        && h@ == compute_horizon_model(now_ms, delete_retention_ms))]
#[ensures(forall<h: i64> !batch.is_control && rec.has_key && is_newest_for_key && !rec.has_value
        && batch.existing_horizon == Some(h)
    ==> result == (if now_ms@ >= h@ { RetainDecision::Delete } else { RetainDecision::Keep }))]
#[must_use]
pub const fn retain_decision(
    rec: RecordMeta,
    batch: BatchMeta,
    is_newest_for_key: bool,
    txn: TxnDataState,
    now_ms: i64,
    delete_retention_ms: i64,
) -> RetainDecision {
    if batch.is_control {
        return match txn {
            TxnDataState::DataSurvives | TxnDataState::NotTransactional => RetainDecision::Keep,
            TxnDataState::DataFullyGone => match batch.existing_horizon {
                Some(h) if now_ms >= h => RetainDecision::Delete,
                Some(_) => RetainDecision::Keep,
                None => RetainDecision::SetHorizon(compute_horizon(now_ms, delete_retention_ms)),
            },
        };
    }
    if !rec.has_key {
        return RetainDecision::Delete;
    }
    if !is_newest_for_key {
        return RetainDecision::Delete;
    }
    if rec.has_value {
        return RetainDecision::Keep;
    }
    // Newest-for-key tombstone: age out via the delete horizon.
    match batch.existing_horizon {
        Some(h) if now_ms >= h => RetainDecision::Delete,
        Some(_) => RetainDecision::Keep,
        None => RetainDecision::SetHorizon(compute_horizon(now_ms, delete_retention_ms)),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    const fn record(has_key: bool, has_value: bool) -> RecordMeta {
        RecordMeta { has_key, has_value }
    }

    const fn batch(is_control: bool, existing_horizon: Option<i64>) -> BatchMeta {
        BatchMeta {
            is_control,
            producer_id: -1,
            existing_horizon,
        }
    }

    #[test]
    fn compute_horizon_saturates_at_i64_bounds() {
        assert!(compute_horizon(100, 50) == 150);
        assert!(compute_horizon(i64::MAX - 1, 50) == i64::MAX);
        assert!(compute_horizon(i64::MIN + 1, -50) == i64::MIN);
    }

    #[test]
    fn retain_decision_distinguishes_expired_and_live_horizons() {
        let tombstone = record(true, false);
        let live_value = record(true, true);

        assert!(
            retain_decision(
                tombstone,
                batch(false, Some(10)),
                true,
                TxnDataState::NotTransactional,
                10,
                50
            ) == RetainDecision::Delete
        );
        assert!(
            retain_decision(
                tombstone,
                batch(false, Some(10)),
                true,
                TxnDataState::NotTransactional,
                9,
                50
            ) == RetainDecision::Keep
        );
        assert!(
            retain_decision(
                live_value,
                batch(false, None),
                true,
                TxnDataState::NotTransactional,
                100,
                50
            ) == RetainDecision::Keep
        );
        assert!(
            retain_decision(
                record(false, true),
                batch(false, None),
                true,
                TxnDataState::NotTransactional,
                100,
                50
            ) == RetainDecision::Delete
        );
    }

    #[test]
    fn retain_decision_stamps_new_tombstone_and_expired_control_marker() {
        assert!(
            retain_decision(
                record(true, false),
                batch(false, None),
                true,
                TxnDataState::NotTransactional,
                100,
                50
            ) == RetainDecision::SetHorizon(150)
        );
        assert!(
            retain_decision(
                record(true, false),
                batch(true, Some(10)),
                false,
                TxnDataState::DataFullyGone,
                10,
                50
            ) == RetainDecision::Delete
        );
        assert!(
            retain_decision(
                record(true, false),
                batch(true, Some(10)),
                false,
                TxnDataState::DataFullyGone,
                9,
                50
            ) == RetainDecision::Keep
        );
        assert!(
            retain_decision(
                record(true, false),
                batch(true, Some(10)),
                false,
                TxnDataState::DataSurvives,
                10,
                50
            ) == RetainDecision::Keep
        );
    }
}
