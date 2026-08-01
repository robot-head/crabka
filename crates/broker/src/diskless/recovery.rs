//! Crash-restart recovery helpers for diskless WAL partitions.

use std::sync::Arc;

use crabka_ids::{Offset, PartitionIndex};
use crabka_log::{Log, LogConfig};
use crabka_protocol::records::RecordBatch;
use crabka_units::{ByteSize, convert::ByteSizeExt as _};

use crate::{error::BrokerError, producer_state::ProducerState};

/// Return the log-open config for a partition, forcing tail validation for diskless logs.
#[must_use]
pub(crate) fn open_config(base: &LogConfig, diskless: bool) -> LogConfig {
    let mut config = base.clone();
    if diskless {
        config.validate_on_open = true;
    }
    config
}

/// Apply diskless crash-restart recovery to an already-open log.
///
/// `KRaft` is the offset authority for diskless partitions, so `committed_next_offset`
/// re-anchors append-at offset assignment after a `[KRaft commit, fsync)` crash.
/// The recovered WAL tail then rebuilds the idempotent producer sequence map.
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

/// Rebuild idempotent-producer dedup state from a recovered log's raw batches.
pub(crate) async fn rebuild_producer_state(
    topic: &str,
    partition: PartitionIndex,
    log: &Log,
    producer_state: &Arc<ProducerState>,
) -> Result<(), BrokerError> {
    // The rebuild replays every batch in the log, so the read is uncapped.
    let raw = log
        .read_raw(
            log.log_start_offset(),
            log.log_end_offset(),
            ByteSize::from_bytes(u64::MAX),
        )
        .map_err(BrokerError::from)?;
    let mut cur: &[u8] = &raw.bytes;
    while !cur.is_empty() {
        let before = cur.len();
        let batch = RecordBatch::decode(&mut cur).map_err(|error| {
            BrokerError::Txn(format!("diskless producer-state rebuild: {error}"))
        })?;
        if cur.len() == before {
            return Err(BrokerError::Txn(
                "diskless producer-state rebuild made no progress".into(),
            ));
        }
        if batch.producer_id < 0 {
            continue;
        }
        producer_state
            .commit(
                topic,
                partition,
                (batch.producer_id, batch.producer_epoch),
                (batch.base_sequence, batch.last_offset_delta),
                (batch.base_offset, batch.max_timestamp),
            )
            .await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;
    use crabka_protocol::records::{Attributes, Record};
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
}
