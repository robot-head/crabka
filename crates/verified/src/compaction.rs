//! KIP-534 log-compaction decision core, extracted from `crabka-log` so
//! Creusot can verify it. The host crate re-exports these; the stateright
//! model in `crabka-log/src/compact_model.rs` drives these exact functions.

/// Per-record facts the retain decision needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordMeta {
    /// Whether the record has a key.
    pub has_key: bool,
    /// Whether the record has a non-null value.
    pub has_value: bool,
}

/// Per-batch facts the retain decision needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnDataState {
    /// `producer_id < 0`: not a transactional producer.
    NotTransactional,
    /// At least one of this producer's data records survives compaction.
    DataSurvives,
    /// All of this producer's data records have been compacted away.
    DataFullyGone,
}

/// What to do with a record during the rewrite pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
