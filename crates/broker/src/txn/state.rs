#![allow(dead_code)] // consumed by TxnCoordinator.

use std::collections::HashSet;

use crabka_ids::PartitionIndex;

/// Tx state machine, mirroring Apache Kafka's classic transaction
/// states (KIP-98) extended for KIP-1319 v2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    Empty,
    Ongoing,
    PrepareCommit,
    PrepareAbort,
    CompleteCommit,
    CompleteAbort,
    Dead,
}

impl TxnState {
    /// Can transition from `self` to `other`?
    pub fn can_transition_to(self, other: TxnState) -> bool {
        use TxnState::{
            CompleteAbort, CompleteCommit, Dead, Empty, Ongoing, PrepareAbort, PrepareCommit,
        };
        matches!(
            (self, other),
            // re-init: empty → empty, or after a completed txn
            (Empty | CompleteCommit | CompleteAbort, Empty)
            // add partitions: first or subsequent, or starting a new txn after
            // a completed one (CompleteCommit/CompleteAbort → Ongoing is the
            // implicit "re-use without init" path that Apache Kafka supports)
            | (Empty | Ongoing | CompleteCommit | CompleteAbort, Ongoing)
            // begin end-of-txn
            | (Ongoing, PrepareCommit | PrepareAbort)
            // finalise
            | (PrepareCommit, CompleteCommit)
            | (PrepareAbort, CompleteAbort)
            // expire / delete
            | (CompleteCommit | CompleteAbort, Dead)
        )
    }

    /// Kafka `TransactionState` byte id (TransactionLogValue.TransactionStatus,
    /// int8). Matches org.apache.kafka.coordinator.transaction.TransactionState
    /// exactly: Empty=0, Ongoing=1, PrepareCommit=2, PrepareAbort=3,
    /// CompleteCommit=4, CompleteAbort=5, Dead=6. (Kafka also has
    /// `PrepareEpochFence`=7, a transient fencing state Crabka does not model.)
    #[must_use]
    pub fn to_kafka_status(self) -> i8 {
        match self {
            TxnState::Empty => 0,
            TxnState::Ongoing => 1,
            TxnState::PrepareCommit => 2,
            TxnState::PrepareAbort => 3,
            TxnState::CompleteCommit => 4,
            TxnState::CompleteAbort => 5,
            TxnState::Dead => 6,
        }
    }

    /// Inverse of [`Self::to_kafka_status`]. Returns `None` for an id Crabka
    /// does not model (e.g. 7 = `PrepareEpochFence`) or an out-of-range id.
    #[must_use]
    pub fn from_kafka_status(id: i8) -> Option<TxnState> {
        Some(match id {
            0 => TxnState::Empty,
            1 => TxnState::Ongoing,
            2 => TxnState::PrepareCommit,
            3 => TxnState::PrepareAbort,
            4 => TxnState::CompleteCommit,
            5 => TxnState::CompleteAbort,
            6 => TxnState::Dead,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: PartitionIndex,
}

#[derive(Debug, Clone)]
pub struct TxnEntry {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub state: TxnState,
    pub txn_timeout_ms: i32,
    pub partitions: HashSet<TopicPartition>,
    /// KIP-890 epoch bookkeeping (Kafka's `TransactionLogValue.PreviousProducerId`
    /// / `NextProducerId` tagged fields). `-1` means "none", matching Kafka's
    /// tagged-field default.
    pub prev_producer_id: i64,
    pub next_producer_id: i64,
    pub last_update_ms: i64,
    pub start_ms: i64,
}

impl TxnEntry {
    /// Fresh entry for a tid that's never been seen.
    pub fn new_empty(
        transactional_id: String,
        producer_id: i64,
        producer_epoch: i16,
        txn_timeout_ms: i32,
        now_ms: i64,
    ) -> Self {
        Self {
            transactional_id,
            producer_id,
            producer_epoch,
            state: TxnState::Empty,
            txn_timeout_ms,
            partitions: HashSet::new(),
            prev_producer_id: -1,
            next_producer_id: -1,
            last_update_ms: now_ms,
            start_ms: now_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn empty_to_ongoing_allowed() {
        assert!(TxnState::Empty.can_transition_to(TxnState::Ongoing));
    }

    #[test]
    fn empty_to_prepare_commit_disallowed() {
        assert!(!TxnState::Empty.can_transition_to(TxnState::PrepareCommit));
    }

    #[test]
    fn ongoing_to_complete_commit_disallowed_without_prepare() {
        assert!(!TxnState::Ongoing.can_transition_to(TxnState::CompleteCommit));
    }

    #[test]
    fn complete_commit_to_empty_for_reuse() {
        assert!(TxnState::CompleteCommit.can_transition_to(TxnState::Empty));
    }

    #[test]
    fn kafka_status_round_trips_all_states() {
        for s in [
            TxnState::Empty,
            TxnState::Ongoing,
            TxnState::PrepareCommit,
            TxnState::PrepareAbort,
            TxnState::CompleteCommit,
            TxnState::CompleteAbort,
            TxnState::Dead,
        ] {
            assert!(TxnState::from_kafka_status(s.to_kafka_status()) == Some(s));
        }
    }

    #[test]
    fn kafka_status_values_match_kafka() {
        // Empirically pinned: cp-kafka 4.0 Ongoing record had TransactionStatus=1.
        for (state, want) in [
            (TxnState::Ongoing, 1),
            (TxnState::Empty, 0),
            (TxnState::Dead, 6),
        ] {
            assert!(state.to_kafka_status() == want, "{state:?}");
        }
        // PrepareEpochFence (7) and out-of-range are not modeled.
        for status in [7, -1] {
            assert!(TxnState::from_kafka_status(status).is_none(), "{status}");
        }
    }
}
