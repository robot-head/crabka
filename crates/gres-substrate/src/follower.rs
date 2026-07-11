//! Read-only range-0 recovery and committed-WAL application.

use std::sync::Arc;

use crabka_gres_ranges::{Range0EndSampler, Range0Frame, Range0Tail};
use crabka_pgkv::{Kv, RestoreKv};

use crate::{
    ReplayItem, WalFrame,
    checkpoint::{CheckpointStore, restore_latest},
    error::SubstrateError,
    recovery::{CommittedWalReader, LiveRecoveryConfig},
};

/// Samples the committed end of a WAL topic after a barrier call begins.
#[async_trait::async_trait]
pub trait CommittedEndSampler: Send + Sync {
    /// Return the current committed end offset, or `-1` for an empty topic.
    async fn committed_end_after_call_begins(&self) -> Result<i64, SubstrateError>;
}

/// Adapter that makes a committed-end sampler usable by range-0 read barriers.
pub struct BrokerRange0EndSampler(pub Arc<dyn CommittedEndSampler>);

/// Broker-backed committed-end sampler for a live range-zero follower.
#[derive(Clone)]
pub struct LiveCommittedEndSampler {
    config: LiveRecoveryConfig,
}

impl LiveCommittedEndSampler {
    #[must_use]
    pub const fn new(config: LiveRecoveryConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl CommittedEndSampler for LiveCommittedEndSampler {
    async fn committed_end_after_call_begins(&self) -> Result<i64, SubstrateError> {
        crate::recovery::live_committed_end(&self.config).await
    }
}

#[async_trait::async_trait]
impl Range0EndSampler for BrokerRange0EndSampler {
    async fn sample_end_after_call_begins(&self) -> Result<i64, crabka_gres_ranges::BarrierError> {
        self.0
            .committed_end_after_call_begins()
            .await
            .map_err(|error| crabka_gres_ranges::BarrierError::Sample(error.to_string()))
    }
}

/// A range-0 replica that can only restore and apply committed WAL records.
#[derive(Clone)]
pub struct ReadOnlyRange0Follower {
    tail: Range0Tail,
}

impl ReadOnlyRange0Follower {
    /// Restore a checkpoint when supplied, then replay all committed records after it.
    ///
    /// This API deliberately accepts a reader rather than a producer: follower construction
    /// cannot initialize transactions, fence a writer, or append a recovery barrier.
    pub async fn bootstrap(
        config: &LiveRecoveryConfig,
        store: Arc<dyn RestoreKv>,
        reader: &dyn CommittedWalReader,
        checkpoints: Option<&dyn CheckpointStore>,
    ) -> Result<Self, SubstrateError> {
        let log_start = reader.log_start_offset().await?;
        let restored = match checkpoints {
            Some(checkpoints) => {
                restore_latest(
                    checkpoints,
                    &config.checkpoint_namespace(),
                    store.as_ref(),
                    0,
                    log_start,
                )
                .await?
            }
            None => None,
        };
        if restored.is_none() && log_start.is_some_and(|offset| offset > 0) {
            return Err(SubstrateError::Checkpoint(format!(
                "no valid checkpoint covers retained WAL starting at {}",
                log_start.expect("checked above")
            )));
        }
        let applied_offset = restored.map_or(-1, |checkpoint| checkpoint.covered_offset);
        let kv: Arc<dyn Kv> = store;
        let follower = Self {
            tail: Range0Tail::from_checkpoint(kv, applied_offset),
        };
        let start = applied_offset.checked_add(1).ok_or_else(|| {
            SubstrateError::Unavailable("range-0 follower replay offset overflowed".into())
        })?;
        for item in &reader.committed_from(start).await? {
            follower.apply_committed(item)?;
        }
        Ok(follower)
    }

    /// Apply one record obtained from a `READ_COMMITTED` continuous tail.
    pub fn apply_committed(&self, item: &ReplayItem) -> Result<(), SubstrateError> {
        let frame = WalFrame::decode(&item.bytes)?;
        self.tail
            .apply_committed(&Range0Frame::new(item.offset, frame.ops))
            .map_err(|error| {
                SubstrateError::Unavailable(format!("range-0 follower apply: {error}"))
            })
    }

    /// Return the local tail used to build a range-0 barrier.
    #[must_use]
    pub fn tail(&self) -> Range0Tail {
        self.tail.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_gres_ranges::{RangeId, TenantName};
    use crabka_pgkv::{Kv, MemKv, WriteOp};

    use super::*;
    use crate::{
        InMemoryWalLog, TransactionalWalWriter, WriterGeneration,
        checkpoint::{
            CheckpointSnapshot, DEFAULT_PART_MAX_BYTES, InMemoryCheckpointStore, write_checkpoint,
        },
    };

    #[tokio::test]
    async fn bootstrap_restores_checkpoint_and_applies_only_committed_post_checkpoint_records() {
        let log = InMemoryWalLog::shared();
        let checkpoint_frame = WalFrame {
            journal_seq: 0,
            ops: vec![WriteOp::Put {
                key: b"catalog/checkpoint".to_vec(),
                value: b"covered".to_vec(),
            }],
        };
        log.commit_group(crate::GroupCommitRequest {
            generation: WriterGeneration(0),
            frames: vec![checkpoint_frame],
        })
        .await
        .expect("commit checkpoint-covered frame");

        let checkpoints = InMemoryCheckpointStore::shared();
        let checkpoint_store = MemKv::default();
        checkpoint_store
            .put(b"catalog/checkpoint".to_vec(), b"covered".to_vec())
            .expect("seed checkpoint state");
        write_checkpoint(
            checkpoints.as_ref(),
            "replica/r0",
            &checkpoint_store,
            CheckpointSnapshot {
                covered_offset: 0,
                journal_seq: 1,
                producer_epoch: 0,
                wal_generation: 0,
                garbage_horizon_xid: 0,
            },
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("write checkpoint");

        let committed = WalFrame {
            journal_seq: 1,
            ops: vec![WriteOp::Put {
                key: b"catalog/committed-after-checkpoint".to_vec(),
                value: b"visible".to_vec(),
            }],
        };
        log.commit_group(crate::GroupCommitRequest {
            generation: WriterGeneration(0),
            frames: vec![committed],
        })
        .await
        .expect("commit post-checkpoint frame");
        let uncommitted = WalFrame {
            journal_seq: 2,
            ops: vec![WriteOp::Put {
                key: b"catalog/aborted".to_vec(),
                value: b"hidden".to_vec(),
            }],
        };
        log.append_unacked(WriterGeneration(0), &[uncommitted])
            .await
            .expect("append aborted frame");
        let store: Arc<dyn RestoreKv> = Arc::new(MemKv::default());
        let config = LiveRecoveryConfig::new(
            "unused",
            TenantName::parse("replica").expect("tenant"),
            RangeId::COORDINATOR,
            None,
        );

        let follower = ReadOnlyRange0Follower::bootstrap(
            &config,
            store.clone(),
            log.as_ref(),
            Some(checkpoints.as_ref()),
        )
        .await
        .expect("bootstrap follower");

        let kv: Arc<dyn Kv> = store;
        assert!(kv.get(b"catalog/checkpoint").expect("checkpoint") == Some(b"covered".to_vec()));
        assert!(
            kv.get(b"catalog/committed-after-checkpoint")
                .expect("committed")
                == Some(b"visible".to_vec())
        );
        assert!(kv.get(b"catalog/aborted").expect("aborted").is_none());
        assert!(follower.tail().applied_offset() == 1);
        assert!(log.current_generation().await == WriterGeneration(0));
    }
}
