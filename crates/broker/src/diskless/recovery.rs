//! Crash-restart recovery helpers for diskless WAL partitions.

use std::sync::Arc;

use crabka_ids::{Offset, PartitionIndex};
use crabka_log::{Log, LogConfig};

use crate::{error::BrokerError, producer_state::ProducerState};

/// Returns the log-open config for a partition. It forces tail validation for
/// a diskless log.
#[must_use]
pub(crate) fn open_config(base: &LogConfig, diskless: bool) -> LogConfig {
    let mut config = base.clone();
    if diskless {
        config.validate_on_open = true;
    }
    config
}

/// Applies diskless crash-restart recovery to a log that is already open.
///
/// `KRaft` is the offset authority for a diskless partition, so
/// `committed_next_offset` re-anchors the append-at offset assignment after a
/// crash in the `[KRaft commit, fsync)` window. The recovered WAL tail then
/// rebuilds the idempotent producer sequence map.
pub(crate) async fn recover_open_log(
    topic: &str,
    partition: PartitionIndex,
    log: &mut Log,
    producer_state: &Arc<ProducerState>,
    committed_next_offset: Option<i64>,
) -> Result<(), BrokerError> {
    if let Some(next_offset) = committed_next_offset {
        log.reconcile_next_offset(Offset(next_offset));
    }
    rebuild_producer_state(topic, partition, log, producer_state).await
}

/// Rebuilds the idempotent-producer dedup state from the raw batches of a
/// recovered log.
pub(crate) async fn rebuild_producer_state(
    topic: &str,
    partition: PartitionIndex,
    log: &Log,
    producer_state: &Arc<ProducerState>,
) -> Result<(), BrokerError> {
    producer_state
        .rebuild_from_log(topic, partition, log)
        .await
        .map_err(BrokerError::from)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;
    use crabka_protocol::records::{Attributes, Record, RecordBatch};
    use tempfile::tempdir;

    use super::*;
    use crate::producer_state::Decision;

    fn idempotent_batch(base_offset: i64, base_sequence: i32, count: i32) -> RecordBatch {
        RecordBatch {
            base_offset,
            partition_leader_epoch: 0,
            attributes: Attributes::default(),
            last_offset_delta: count - 1,
            base_timestamp: 0,
            max_timestamp: 7,
            producer_id: 42,
            producer_epoch: 3,
            base_sequence,
            records: (0..count)
                .map(|offset_delta| Record {
                    attributes: 0,
                    offset_delta,
                    timestamp_delta: 0,
                    key: None,
                    value: Some(Bytes::from_static(b"v")),
                    headers: vec![],
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn producer_dedup_rebuilt_from_recovered_wal() {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            log.append(&mut idempotent_batch(0, 0, 2)).unwrap();
        }
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let producer_state = Arc::new(ProducerState::new());

        rebuild_producer_state("orders", PartitionIndex(0), &log, &producer_state)
            .await
            .unwrap();

        assert!(
            producer_state
                .check("orders", PartitionIndex(0), 42, 3, 0, 2)
                .await
                == Decision::Duplicate { base_offset: 0 }
        );
        assert!(
            producer_state
                .check("orders", PartitionIndex(0), 42, 3, 2, 1)
                .await
                == Decision::Append
        );
    }

    #[tokio::test]
    async fn producer_rebuild_ignores_transaction_control_markers() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&mut idempotent_batch(0, 0, 2)).unwrap();
        let mut marker = crate::txn::marker::build_marker_batch(
            crabka_log::ProducerId(42),
            3,
            Offset(2),
            crate::txn::marker::MarkerType::Commit,
            0,
        );
        marker.base_sequence = 0;
        log.append(&mut marker).unwrap();
        let producer_state = Arc::new(ProducerState::new());

        rebuild_producer_state("orders", PartitionIndex(0), &log, &producer_state)
            .await
            .unwrap();

        assert!(
            producer_state
                .check("orders", PartitionIndex(0), 42, 3, 0, 2)
                .await
                == Decision::Duplicate { base_offset: 0 }
        );
        assert!(
            producer_state
                .check("orders", PartitionIndex(0), 42, 3, 2, 1)
                .await
                == Decision::Append
        );
    }

    #[tokio::test]
    async fn producer_rebuild_ignores_invalid_producer_identity_and_sequence() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();

        let mut producer_zero = idempotent_batch(0, 0, 1);
        producer_zero.producer_id = 0;
        log.append(&mut producer_zero).unwrap();

        let mut invalid_id = idempotent_batch(1, 5, 1);
        invalid_id.producer_id = -1;
        log.append(&mut invalid_id).unwrap();

        let mut invalid_sequence = idempotent_batch(2, -1, 1);
        invalid_sequence.producer_id = 43;
        log.append(&mut invalid_sequence).unwrap();

        let producer_state = Arc::new(ProducerState::new());
        rebuild_producer_state("orders", PartitionIndex(0), &log, &producer_state)
            .await
            .unwrap();

        assert!(
            producer_state
                .check("orders", PartitionIndex(0), 0, 3, 0, 1)
                .await
                == Decision::Duplicate { base_offset: 0 }
        );
        assert!(
            producer_state
                .check("orders", PartitionIndex(0), -1, 3, 5, 1)
                .await
                == Decision::Append
        );
        assert!(
            producer_state
                .check("orders", PartitionIndex(0), 43, 3, -1, 1)
                .await
                == Decision::Append
        );
    }
}
