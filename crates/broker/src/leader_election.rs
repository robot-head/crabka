//! Controller-side leader-election scan. Called by the liveness ticker
//! when a broker transitions `alive → dead`. Scans every partition where
//! the dead broker is leader, picks the first alive ISR replica as new
//! leader, bumps `leader_epoch`, and emits the new `PartitionRecord`s
//! through openraft.

#![allow(dead_code)]

use std::sync::Arc;

use crabka_metadata::{MetadataRecord, PartitionRecord};
use crabka_raft::{ControllerHandle, NodeId};
use tracing::warn;

use crate::error::BrokerError;
use crate::heartbeat::controller_state::ControllerLivenessState;

/// Called when the liveness ticker observes `AliveToDead(dead)`. Scans
/// every partition where `dead` is leader OR in the ISR; proposes
/// updated `PartitionRecord`s.
///
/// No-op unless `controller` is currently the openraft leader (only
/// the leader can `submit_change`).
pub(crate) async fn on_broker_dead(
    controller: &Arc<ControllerHandle>,
    node_id: NodeId,
    dead: NodeId,
    liveness: &Arc<ControllerLivenessState>,
) -> Result<(), BrokerError> {
    let is_controller_leader = controller
        .watch_leader()
        .borrow()
        .is_some_and(|n| n == node_id);
    if !is_controller_leader {
        return Ok(());
    }

    let image = controller.current_image();
    let mut changes: Vec<MetadataRecord> = Vec::new();
    for topic in image.topics() {
        for pr in image.partitions_of(&topic.name) {
            if !pr.replicas.contains(&dead) && !pr.isr.contains(&dead) {
                continue;
            }
            // Compute the new ISR after dropping the dead broker AND any
            // other replicas that aren't alive.
            let mut alive_isr: Vec<NodeId> = Vec::with_capacity(pr.isr.len());
            for n in &pr.isr {
                if *n != dead && liveness.is_alive(*n).await {
                    alive_isr.push(*n);
                }
            }
            let needs_election = pr.leader == dead;
            if needs_election {
                let Some(&new_leader) = alive_isr.first() else {
                    warn!(
                        topic = %pr.topic, partition = pr.partition,
                        "no live ISR replica; partition unavailable"
                    );
                    continue;
                };
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader: new_leader,
                    replicas: pr.replicas.clone(),
                    isr: alive_isr,
                    leader_epoch: pr.leader_epoch + 1,
                }));
            } else if alive_isr.len() < pr.isr.len() {
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader: pr.leader,
                    replicas: pr.replicas.clone(),
                    isr: alive_isr,
                    leader_epoch: pr.leader_epoch,
                }));
            }
        }
    }
    if !changes.is_empty() {
        controller
            .submit_change(changes)
            .await
            .map_err(|e| BrokerError::Replication(format!("submit_change: {e}")))?;
    }
    Ok(())
}

/// Called when the liveness ticker observes `DeadToAlive(alive)`. For
/// slice-10b this is a no-op — ISR expand happens organically via
/// `isr_maintenance` once the rejoined broker's replicator catches up.
/// The hook is here for future enhancements (e.g. auto-rebalance).
#[allow(clippy::unused_async)]
pub(crate) async fn on_broker_alive(
    _controller: &Arc<ControllerHandle>,
    _node_id: NodeId,
    _alive: NodeId,
    _liveness: &Arc<ControllerLivenessState>,
) -> Result<(), BrokerError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    // Integration tests in tests/leader_election.rs cover the real-multi-
    // broker scenarios. Unit-testing on_broker_dead in isolation requires
    // mocking ControllerHandle + the metadata image, which is heavy.
}
