//! In-process WAL quorum engine.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicI64, Ordering},
};

use bytes::Bytes;
use crabka_ids::{LeaderEpoch, Offset, ProducerId};
use crabka_kraft_core::{NodeId, QuorumState, QuorumStateMachine};
use crabka_log::{Log, VerbatimBatch};
use crabka_protocol::records::RecordBatch;
use crabka_units::{ByteSize, convert::ByteSizeExt as _, millis};

use crate::error::BrokerError;

/// Election timeout of the in-process WAL quorum's state machine. The replicas
/// are local, so the window only has to cover a stalled replica task.
const ELECTION_TIMEOUT: crabka_units::Time = millis(1_000);

/// A single durable member of a WAL quorum.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct WalReplica {
    pub(super) id: NodeId,
    log: Arc<Mutex<Log>>,
    alive: AtomicBool,
}

impl WalReplica {
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn new(id: NodeId, log: Arc<Mutex<Log>>) -> Self {
        Self {
            id,
            log,
            alive: AtomicBool::new(true),
        }
    }

    #[must_use]
    pub(crate) fn id(&self) -> NodeId {
        self.id
    }
}

/// Drives the durable quorum frontier of a WAL shard.
#[derive(Debug)]
pub(crate) struct WalShardEngine {
    #[allow(dead_code)]
    core: Mutex<QuorumStateMachine>,
    replicas: Vec<WalReplica>,
    durable_watermark: AtomicI64,
}

impl WalShardEngine {
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn new(me: NodeId, state: QuorumState, replicas: Vec<WalReplica>) -> Self {
        Self {
            core: Mutex::new(QuorumStateMachine::new(me, state, ELECTION_TIMEOUT)),
            replicas,
            durable_watermark: AtomicI64::new(0),
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn for_logs(logs: std::collections::BTreeMap<NodeId, Arc<Mutex<Log>>>) -> Self {
        let voters = voter_set(logs.keys().copied());
        let state = QuorumState::bootstrap(uuid::Uuid::nil(), voters);
        let replicas = logs
            .into_iter()
            .map(|(id, log)| WalReplica::new(id, log))
            .collect();
        Self::new(NodeId(1), state, replicas)
    }

    #[must_use]
    pub(crate) fn durable_watermark(&self) -> Offset {
        Offset(self.durable_watermark.load(Ordering::Acquire))
    }

    #[cfg(test)]
    pub(crate) fn set_replica_alive(&self, id: NodeId, alive: bool) {
        if let Some(replica) = self.replicas.iter().find(|replica| replica.id == id) {
            replica.alive.store(alive, Ordering::Release);
        }
    }

    pub(crate) async fn replicate_and_sync(
        &self,
        source: &Arc<Mutex<Log>>,
        target: Offset,
    ) -> Result<Offset, BrokerError> {
        let start = self.durable_watermark();
        if target <= start {
            return Ok(start);
        }
        let batches = read_batches(source, start, target)?;
        let mut synced = 0usize;
        for replica in &self.replicas {
            if !replica.alive.load(Ordering::Acquire) {
                continue;
            }
            if sync_replica(replica.log.clone(), &batches).await.is_ok() {
                synced += 1;
            }
        }
        let required = self.replicas.len() / 2 + 1;
        if synced < required {
            return Err(BrokerError::Replication(format!(
                "wal quorum has {synced} synced replicas, needs {required}"
            )));
        }
        self.durable_watermark.store(target.0, Ordering::Release);
        Ok(target)
    }

    pub(crate) fn serve_fetch(
        &self,
        fetch_offset: Offset,
        max_size: ByteSize,
    ) -> Result<(Offset, Bytes), BrokerError> {
        let hwm = self.durable_watermark();
        if fetch_offset < Offset(0) || fetch_offset >= hwm || max_size == ByteSize::ZERO {
            return Ok((hwm, Bytes::new()));
        }
        let replica = self
            .replicas
            .iter()
            .find(|replica| replica.alive.load(Ordering::Acquire))
            .ok_or_else(|| {
                BrokerError::Replication("wal quorum has no live fetch replica".into())
            })?;
        let raw = replica
            .log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .read_raw(fetch_offset, hwm, max_size)?;
        Ok((hwm, raw.bytes))
    }
}

#[derive(Debug, Clone)]
struct BatchBytes {
    base_offset: Offset,
    last_offset: Offset,
    verbatim: VerbatimBatch,
}

fn read_batches(
    source: &Arc<Mutex<Log>>,
    start: Offset,
    target: Offset,
) -> Result<Vec<BatchBytes>, BrokerError> {
    let raw = source
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        // Replication must carry every batch in `start..target`, so the read
        // is uncapped.
        .read_raw(start, target, ByteSize::from_bytes(u64::MAX))?;
    split_batches(&raw.bytes)
}

fn split_batches(bytes: &Bytes) -> Result<Vec<BatchBytes>, BrokerError> {
    let mut out = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let mut cur = bytes.slice(offset..);
        let batch = RecordBatch::decode(&mut cur)
            .map_err(|err| BrokerError::Replication(format!("decode WAL batch: {err}")))?;
        let len = batch.encoded_len();
        if len == 0 || offset + len > bytes.len() {
            return Err(BrokerError::Replication("invalid WAL batch length".into()));
        }
        let base_offset = Offset(batch.base_offset);
        let last_offset = Offset(batch.base_offset + i64::from(batch.last_offset_delta));
        out.push(BatchBytes {
            base_offset,
            last_offset,
            verbatim: VerbatimBatch {
                bytes: bytes.slice(offset..offset + len),
                last_offset_delta: batch.last_offset_delta,
                max_timestamp: batch.max_timestamp,
                leader_epoch: LeaderEpoch(batch.partition_leader_epoch),
                producer_id: ProducerId(batch.producer_id),
                is_transactional: batch.attributes.is_transactional(),
            },
        });
        offset += len;
    }
    Ok(out)
}

async fn sync_replica(log: Arc<Mutex<Log>>, batches: &[BatchBytes]) -> Result<(), BrokerError> {
    if tokio::runtime::Handle::current().runtime_flavor()
        == tokio::runtime::RuntimeFlavor::MultiThread
    {
        tokio::task::block_in_place(|| sync_replica_blocking(&log, batches))
    } else {
        let batches = batches.to_vec();
        tokio::task::spawn_blocking(move || sync_replica_blocking(&log, &batches))
            .await
            .map_err(|e| BrokerError::Replication(format!("wal replica task panicked: {e}")))?
    }
}

fn sync_replica_blocking(log: &Mutex<Log>, batches: &[BatchBytes]) -> Result<(), BrokerError> {
    let mut log = log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for batch in batches {
        let end = log.log_end_offset();
        if end <= batch.base_offset {
            log.append_verbatim_at(&batch.verbatim, batch.base_offset)?;
        } else if end < batch.last_offset + 1 {
            return Err(BrokerError::Replication(format!(
                "wal replica overlaps batch {}..{} at leo {end}",
                batch.base_offset.0,
                batch.last_offset.0 + 1
            )));
        }
    }
    log.sync()?;
    Ok(())
}

pub(super) fn voter_set(ids: impl IntoIterator<Item = NodeId>) -> crabka_voters::VoterSet {
    crabka_voters::VoterSet::from_voters(ids.into_iter().map(|id| crabka_voters::Voter {
        id,
        directory_id: uuid::Uuid::nil(),
        endpoints: Vec::new(),
        kraft_version: crabka_voters::KRaftVersionRange::default(),
    }))
}
