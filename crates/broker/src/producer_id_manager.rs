//! Cluster-wide producer-ID block allocation.
//!
//! Brokers reserve non-overlapping blocks through the metadata controller and
//! then serve IDs from their local block without a controller round trip. The
//! committed `ProducerIdsRecord` stores the first ID in the next unassigned
//! block, so broker restarts and controller failover cannot reuse IDs.

use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

use crabka_log::ProducerId;
use crabka_metadata::{MetadataRecord, NodeId, ProducerIdsRecord};
use tokio::sync::Mutex;

use crate::metadata_source::MetadataSource;

/// Kafka's controller-assigned producer-ID block size.
pub(crate) const PRODUCER_ID_BLOCK_SIZE: i64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProducerIdBlock {
    pub(crate) first: i64,
    pub(crate) len: i32,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProducerIdAllocationError {
    #[error("broker {0} is not registered")]
    BrokerNotRegistered(NodeId),
    #[error("broker {broker_id} epoch is stale: requested {requested}, registered {registered}")]
    StaleBrokerEpoch {
        broker_id: NodeId,
        requested: i64,
        registered: i64,
    },
    #[error("the signed 64-bit producer ID space is exhausted")]
    Exhausted,
    #[error("controller failed to allocate a producer ID block: {0}")]
    Controller(String),
}

impl From<ProducerIdAllocationError> for crate::error::BrokerError {
    fn from(error: ProducerIdAllocationError) -> Self {
        Self::Txn(error.to_string())
    }
}

/// Atomically reserve one durable block for a registered broker.
///
/// Concurrent brokers can observe the same candidate start. The controller
/// serializes their metadata records; its monotonic validation accepts one and
/// rejects the stale candidate. The loser observes the new image and retries.
pub(crate) async fn allocate_block(
    controller: &Arc<dyn MetadataSource>,
    broker_id: NodeId,
    broker_epoch: i64,
) -> Result<ProducerIdBlock, ProducerIdAllocationError> {
    loop {
        let image = controller.current_image();
        let Some(registered_epoch) = image.broker_epoch(broker_id) else {
            return Err(ProducerIdAllocationError::BrokerNotRegistered(broker_id));
        };
        if registered_epoch != broker_epoch {
            return Err(ProducerIdAllocationError::StaleBrokerEpoch {
                broker_id,
                requested: broker_epoch,
                registered: registered_epoch,
            });
        }

        let first = image.next_producer_id();
        let next = first
            .checked_add(PRODUCER_ID_BLOCK_SIZE)
            .ok_or(ProducerIdAllocationError::Exhausted)?;
        let record = MetadataRecord::V1ProducerIds(ProducerIdsRecord {
            broker_id,
            broker_epoch,
            next_producer_id: next,
        });
        match controller.submit_change(vec![record]).await {
            Ok(_) => {
                return Ok(ProducerIdBlock {
                    first,
                    len: i32::try_from(PRODUCER_ID_BLOCK_SIZE)
                        .expect("producer ID block size fits i32"),
                });
            }
            Err(error) => {
                // A competing allocation committed first. Retry from the new
                // durable boundary; return all other controller failures.
                if controller.current_image().next_producer_id() > first {
                    continue;
                }
                return Err(ProducerIdAllocationError::Controller(error.to_string()));
            }
        }
    }
}

pub struct ProducerIdManager {
    controller: Option<Arc<dyn MetadataSource>>,
    node_id: NodeId,
    next: AtomicI64,
    end_exclusive: AtomicI64,
    refill: Mutex<()>,
}

impl std::fmt::Debug for ProducerIdManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerIdManager")
            .field("node_id", &self.node_id)
            .field("next", &self.next.load(Ordering::Relaxed))
            .field("end_exclusive", &self.end_exclusive.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl ProducerIdManager {
    #[must_use]
    pub(crate) fn clustered(node_id: NodeId, controller: Arc<dyn MetadataSource>) -> Self {
        Self {
            controller: Some(controller),
            node_id,
            next: AtomicI64::new(0),
            end_exclusive: AtomicI64::new(0),
            refill: Mutex::new(()),
        }
    }

    /// Allocate a fresh `(producer_id, producer_epoch=0)`.
    pub(crate) async fn allocate(&self) -> Result<(ProducerId, i16), ProducerIdAllocationError> {
        loop {
            if let Some(id) = self.claim() {
                return Ok((ProducerId(id), 0));
            }

            let _refill = self.refill.lock().await;
            if let Some(id) = self.claim() {
                return Ok((ProducerId(id), 0));
            }
            let controller = self.controller.as_ref().ok_or_else(|| {
                ProducerIdAllocationError::Controller(
                    "test allocator exhausted its local ID space".into(),
                )
            })?;
            let image = controller.current_image();
            let broker_epoch = image
                .broker_epoch(self.node_id)
                .ok_or(ProducerIdAllocationError::BrokerNotRegistered(self.node_id))?;
            let block = allocate_block(controller, self.node_id, broker_epoch).await?;
            let end = block
                .first
                .checked_add(i64::from(block.len))
                .ok_or(ProducerIdAllocationError::Exhausted)?;
            self.next.store(block.first, Ordering::Release);
            self.end_exclusive.store(end, Ordering::Release);
        }
    }

    fn claim(&self) -> Option<i64> {
        loop {
            let next = self.next.load(Ordering::Acquire);
            if next >= self.end_exclusive.load(Ordering::Acquire) {
                return None;
            }
            if self
                .next
                .compare_exchange_weak(next, next + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(next);
            }
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            controller: None,
            node_id: NodeId(0),
            next: AtomicI64::new(0),
            end_exclusive: AtomicI64::new(i64::MAX),
            refill: Mutex::new(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[tokio::test]
    async fn local_test_allocator_returns_monotonic_ids() {
        let manager = ProducerIdManager::new();
        for want in 0..3 {
            assert!(manager.allocate().await.unwrap() == (ProducerId(want), 0));
        }
    }

    #[test]
    fn claim_stops_at_the_exclusive_block_end() {
        let manager = ProducerIdManager {
            controller: None,
            node_id: NodeId(0),
            next: AtomicI64::new(10),
            end_exclusive: AtomicI64::new(12),
            refill: Mutex::new(()),
        };
        assert!(manager.claim() == Some(10));
        assert!(manager.claim() == Some(11));
        assert!(manager.claim() == None);
    }
}
