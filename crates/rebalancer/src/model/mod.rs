//! In-memory data model for the rebalancer.
//!
//! `ClusterState` is the snapshot fed into the optimizer. `Movement`
//! is a single proposed change (replica-set update, leader change, or
//! both). Validity helpers reject malformed movements before they
//! reach the optimizer's accumulator.

pub mod proposal;
pub mod store;

pub use proposal::{Movement, Proposal, ProposalStatus, ProposalSummary};
pub use store::{ProposalStore, StoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterState {
    pub cluster_id: Option<String>,
    pub snapshot_at_ms: i64,
    pub brokers: Vec<BrokerView>,
    pub partitions: Vec<PartitionView>,
    pub in_flight_reassignments: Vec<InFlightReassignment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerView {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionView {
    pub topic: String,
    pub partition: i32,
    pub replicas: Vec<i32>,
    pub leader: i32,
    pub isr: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InFlightReassignment {
    pub topic: String,
    pub partition: i32,
    pub adding: Vec<i32>,
    pub removing: Vec<i32>,
}

/// Why a proposed movement was rejected. Returned by
/// [`validate_movement`]. The optimizer logs at debug + drops the
/// movement.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MovementError {
    #[error("replication factor changed: old={old} new={new}")]
    ReplicationFactorChanged { old: usize, new: usize },
    #[error("new_leader {leader} not in new_replicas {replicas:?}")]
    LeaderNotInReplicas { leader: i32, replicas: Vec<i32> },
    #[error("new_replicas has duplicates: {replicas:?}")]
    DuplicateReplicas { replicas: Vec<i32> },
    #[error("new_replicas contains unknown broker id {id}")]
    UnknownBroker { id: i32 },
    #[error("target partition not found: {topic}-{partition}")]
    UnknownPartition { topic: String, partition: i32 },
}

/// Inspect `movement` against `state`'s broker + partition tables.
/// Returns `Ok(())` for movements the optimizer should accept,
/// `Err(MovementError)` for ones it should drop.
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub fn validate_movement(state: &ClusterState, mv: &Movement) -> Result<(), MovementError> {
    if mv.old_replicas.len() != mv.new_replicas.len() {
        return Err(MovementError::ReplicationFactorChanged {
            old: mv.old_replicas.len(),
            new: mv.new_replicas.len(),
        });
    }
    if !mv.new_replicas.contains(&mv.new_leader) {
        return Err(MovementError::LeaderNotInReplicas {
            leader: mv.new_leader,
            replicas: mv.new_replicas.clone(),
        });
    }
    let mut seen = std::collections::HashSet::new();
    for r in &mv.new_replicas {
        if !seen.insert(*r) {
            return Err(MovementError::DuplicateReplicas {
                replicas: mv.new_replicas.clone(),
            });
        }
    }
    let known: std::collections::HashSet<i32> = state.brokers.iter().map(|b| b.id).collect();
    for r in &mv.new_replicas {
        if !known.contains(r) {
            return Err(MovementError::UnknownBroker { id: *r });
        }
    }
    let part_known = state
        .partitions
        .iter()
        .any(|p| p.topic == mv.topic && p.partition == mv.partition);
    if !part_known {
        return Err(MovementError::UnknownPartition {
            topic: mv.topic.clone(),
            partition: mv.partition,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    fn fixture_state() -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: vec![
                BrokerView {
                    id: 1,
                    host: "h1".into(),
                    port: 9092,
                    rack: None,
                },
                BrokerView {
                    id: 2,
                    host: "h2".into(),
                    port: 9092,
                    rack: None,
                },
                BrokerView {
                    id: 3,
                    host: "h3".into(),
                    port: 9092,
                    rack: None,
                },
            ],
            partitions: vec![PartitionView {
                topic: "foo".into(),
                partition: 0,
                replicas: vec![1, 2],
                leader: 1,
                isr: vec![1, 2],
            }],
            in_flight_reassignments: vec![],
        }
    }

    #[test]
    fn validate_valid_movement_ok() {
        let mv = Movement {
            topic: "foo".into(),
            partition: 0,
            old_replicas: vec![1, 2],
            new_replicas: vec![1, 3],
            old_leader: 1,
            new_leader: 1,
        };
        assert2::assert!(validate_movement(&fixture_state(), &mv).is_ok());
    }

    #[test]
    fn validate_rejects_rf_change() {
        let mv = Movement {
            topic: "foo".into(),
            partition: 0,
            old_replicas: vec![1, 2],
            new_replicas: vec![1, 2, 3],
            old_leader: 1,
            new_leader: 1,
        };
        assert2::assert!(matches!(
            validate_movement(&fixture_state(), &mv),
            Err(MovementError::ReplicationFactorChanged { .. })
        ));
    }

    #[test]
    fn validate_rejects_leader_not_in_replicas() {
        let mv = Movement {
            topic: "foo".into(),
            partition: 0,
            old_replicas: vec![1, 2],
            new_replicas: vec![1, 3],
            old_leader: 1,
            new_leader: 2,
        };
        assert2::assert!(matches!(
            validate_movement(&fixture_state(), &mv),
            Err(MovementError::LeaderNotInReplicas { .. })
        ));
    }

    #[test]
    fn validate_rejects_duplicate_replicas() {
        let mv = Movement {
            topic: "foo".into(),
            partition: 0,
            old_replicas: vec![1, 2],
            new_replicas: vec![1, 1],
            old_leader: 1,
            new_leader: 1,
        };
        assert2::assert!(matches!(
            validate_movement(&fixture_state(), &mv),
            Err(MovementError::DuplicateReplicas { .. })
        ));
    }

    #[test]
    fn validate_rejects_unknown_broker() {
        let mv = Movement {
            topic: "foo".into(),
            partition: 0,
            old_replicas: vec![1, 2],
            new_replicas: vec![1, 99],
            old_leader: 1,
            new_leader: 1,
        };
        assert2::assert!(matches!(
            validate_movement(&fixture_state(), &mv),
            Err(MovementError::UnknownBroker { id: 99 })
        ));
    }

    #[test]
    fn validate_rejects_unknown_partition() {
        let mv = Movement {
            topic: "ghost".into(),
            partition: 0,
            old_replicas: vec![1, 2],
            new_replicas: vec![1, 3],
            old_leader: 1,
            new_leader: 1,
        };
        assert2::assert!(matches!(
            validate_movement(&fixture_state(), &mv),
            Err(MovementError::UnknownPartition { .. })
        ));
    }
}
