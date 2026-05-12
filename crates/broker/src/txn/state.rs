#![allow(dead_code)] // consumed by TxnCoordinator in Task 9+.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

/// Tx state machine, mirroring Apache Kafka's classic transaction
/// states (KIP-98) extended for KIP-1319 v2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
            // add partitions: first or subsequent
            | (Empty | Ongoing, Ongoing)
            // begin end-of-txn
            | (Ongoing, PrepareCommit | PrepareAbort)
            // finalise
            | (PrepareCommit, CompleteCommit)
            | (PrepareAbort, CompleteAbort)
            // expire / delete
            | (CompleteCommit | CompleteAbort, Dead)
        )
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnEntry {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub state: TxnState,
    pub txn_timeout_ms: i32,
    pub partitions: HashSet<TopicPartition>,
    pub offset_commit_groups: HashSet<String>,
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
            offset_commit_groups: HashSet::new(),
            last_update_ms: now_ms,
            start_ms: now_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_wincode::SerdeCompat;
    use wincode::{Deserialize as _, Serialize as _};

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
    fn entry_serde_round_trip() {
        let mut e = TxnEntry::new_empty("my-tid".into(), 1000, 0, 60_000, 1000);
        e.partitions.insert(TopicPartition {
            topic: "t".into(),
            partition: 0,
        });
        e.state = TxnState::Ongoing;

        let bytes = <SerdeCompat<TxnEntry>>::serialize(&e).unwrap();
        let decoded: TxnEntry = <SerdeCompat<TxnEntry>>::deserialize(&bytes).unwrap();

        assert_eq!(decoded.transactional_id, "my-tid");
        assert_eq!(decoded.producer_id, 1000);
        assert_eq!(decoded.state, TxnState::Ongoing);
        assert_eq!(decoded.partitions.len(), 1);
    }
}
