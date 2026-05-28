//! KIP-966 offset-aware unclean recovery: pure selection helpers + the
//! controller-side Unclean Recovery Manager (URM) task. The URM polls
//! surviving replicas for their log-end-offset and last-written leader
//! epoch (`GetReplicaLogInfo`, api_key 70) and elects the most complete
//! log. See docs/superpowers/specs/2026-05-28-crabka-unclean-recovery-kip966-design.md.

#![allow(dead_code)]

use crabka_raft::NodeId;

/// One replica's reported log state, gathered from a `GetReplicaLogInfo`
/// response. Decoupled from the generated wire type so the selection
/// logic is unit-testable without building protocol structs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplicaLogInfo {
    pub broker_id: NodeId,
    pub last_written_leader_epoch: i32,
    pub log_end_offset: i64,
    pub current_leader_epoch: i32,
}

/// Pick the replica with the most complete log: highest
/// `last_written_leader_epoch`, then highest `log_end_offset`, then
/// lowest `broker_id` for determinism. Returns `None` for an empty input.
pub(crate) fn select_best_replica(responses: &[ReplicaLogInfo]) -> Option<NodeId> {
    responses
        .iter()
        .max_by(|a, b| {
            a.last_written_leader_epoch
                .cmp(&b.last_written_leader_epoch)
                .then(a.log_end_offset.cmp(&b.log_end_offset))
                .then(b.broker_id.cmp(&a.broker_id)) // lower broker_id wins ties
        })
        .map(|r| r.broker_id)
}

/// True if any responder reports a `current_leader_epoch` strictly
/// greater than the controller's known `leader_epoch` for the partition,
/// meaning a newer leader already exists and this recovery is stale.
pub(crate) fn has_newer_leader(responses: &[ReplicaLogInfo], known_leader_epoch: i32) -> bool {
    responses
        .iter()
        .any(|r| r.current_leader_epoch > known_leader_epoch)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ri(broker_id: NodeId, epoch: i32, leo: i64) -> ReplicaLogInfo {
        ReplicaLogInfo {
            broker_id,
            last_written_leader_epoch: epoch,
            log_end_offset: leo,
            current_leader_epoch: epoch,
        }
    }

    #[test]
    fn picks_highest_epoch_then_offset() {
        // Broker 3 has a higher epoch even though broker 2 has a longer log.
        let r = [ri(2, 4, 100), ri(3, 5, 10)];
        assert_eq!(select_best_replica(&r), Some(3));
    }

    #[test]
    fn ties_on_epoch_break_by_offset() {
        let r = [ri(2, 5, 90), ri(3, 5, 120)];
        assert_eq!(select_best_replica(&r), Some(3));
    }

    #[test]
    fn ties_on_epoch_and_offset_break_by_lowest_broker_id() {
        let r = [ri(3, 5, 100), ri(1, 5, 100), ri(2, 5, 100)];
        assert_eq!(select_best_replica(&r), Some(1));
    }

    #[test]
    fn empty_input_returns_none() {
        assert_eq!(select_best_replica(&[]), None);
    }

    #[test]
    fn newer_leader_detected() {
        let r = [ReplicaLogInfo {
            broker_id: 2,
            last_written_leader_epoch: 5,
            log_end_offset: 10,
            current_leader_epoch: 7,
        }];
        assert!(has_newer_leader(&r, 6));
        assert!(!has_newer_leader(&r, 7));
    }
}
