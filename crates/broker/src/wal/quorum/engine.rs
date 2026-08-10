//! In-process WAL quorum engine.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicI64, Ordering},
};

use bytes::Bytes;
use crabka_ids::{LeaderEpoch, Offset, ProducerId};
use crabka_kraft_core::{LogView as _, NodeId, QuorumState, QuorumStateMachine};
use crabka_log::{Log, VerbatimBatch};
use crabka_protocol::records::RecordBatch;
use crabka_units::{ByteSize, convert::ByteSizeExt as _, millis};

use crate::{error::BrokerError, wal::quorum::log_view::ShardLog};

/// Election timeout of the in-process WAL quorum's state machine. The replicas
/// are local, so the window only has to cover a stalled replica task.
const ELECTION_TIMEOUT: crabka_units::Time = millis(1_000);

/// A single durable member of a WAL quorum.
#[derive(Debug)]
pub(crate) struct WalReplica {
    pub(super) id: NodeId,
    log: ShardLog,
    alive: AtomicBool,
}

impl WalReplica {
    #[must_use]
    pub(crate) fn new(id: NodeId, log: Arc<Mutex<Log>>) -> Self {
        Self {
            id,
            log: ShardLog::new(log),
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
    core: Mutex<QuorumStateMachine>,
    replicas: Vec<WalReplica>,
    durable_watermark: AtomicI64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum OpenMode {
    BootstrapFrom(NodeId),
    Recover,
}

impl WalShardEngine {
    pub(crate) fn new(
        me: NodeId,
        state: QuorumState,
        replicas: Vec<WalReplica>,
        mode: OpenMode,
    ) -> Result<Self, BrokerError> {
        if replicas.is_empty() {
            return Err(BrokerError::Replication(
                "wal quorum must contain at least one replica".into(),
            ));
        }
        if state.voters.len() != replicas.len()
            || replicas
                .iter()
                .any(|replica| !state.voters.contains(replica.id))
        {
            return Err(BrokerError::Replication(
                "wal quorum replicas do not match the persisted voter set".into(),
            ));
        }

        let core = QuorumStateMachine::new(me, state, ELECTION_TIMEOUT);
        let durable_watermark = match mode {
            OpenMode::BootstrapFrom(source) => bootstrap_durable_prefix(&replicas, source)?,
            OpenMode::Recover => recover_durable_prefix(&replicas, core.quorum_state().majority())?,
        };
        Ok(Self {
            core: Mutex::new(core),
            replicas,
            durable_watermark: AtomicI64::new(durable_watermark.0),
        })
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
        Self::new(NodeId(1), state, replicas, OpenMode::Recover).expect("test WAL quorum recovers")
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

    #[cfg(test)]
    pub(crate) fn replica_end_offsets(&self) -> Vec<Offset> {
        self.replicas.iter().map(replica_end_offset).collect()
    }

    pub(crate) async fn replicate_and_sync(
        &self,
        source: &Arc<Mutex<Log>>,
        target: Offset,
    ) -> Result<Offset, BrokerError> {
        let committed = self.durable_watermark();
        if target <= committed {
            return Ok(committed);
        }
        let source = ShardLog::new(source.clone());
        let source_end = Offset(source.end_offset());
        if target > source_end {
            return Err(BrokerError::Replication(format!(
                "wal source ends at {}, before requested durable offset {}",
                source_end.0, target.0
            )));
        }

        let mut synced = 0usize;
        for replica in &self.replicas {
            if !replica.alive.load(Ordering::Acquire) {
                continue;
            }
            let replica_end = replica_end_offset(replica);
            let Ok(batches) = read_batches_exact(&source, replica_end.min(target), target) else {
                continue;
            };
            if sync_replica(replica.log.clone(), &batches).await.is_ok() {
                synced += 1;
            }
        }
        let required = self
            .core
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .quorum_state()
            .majority();
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
        let raw = replica.log.lock().read_raw(fetch_offset, hwm, max_size)?;
        Ok((hwm, raw.bytes))
    }
}

fn recover_durable_prefix(replicas: &[WalReplica], majority: usize) -> Result<Offset, BrokerError> {
    let ends = replicas.iter().map(replica_end_offset).collect::<Vec<_>>();
    let (donor_index, donor_end) = ends
        .iter()
        .enumerate()
        .max_by_key(|(_, offset)| offset.0)
        .map(|(index, offset)| (index, *offset))
        .ok_or_else(|| BrokerError::Replication("wal quorum has no recovery donor".into()))?;
    let follower_ends = ends
        .iter()
        .enumerate()
        .filter_map(|(index, offset)| (index != donor_index).then_some(offset.0))
        .collect::<Vec<_>>();
    let durable = Offset(crabka_verified::recompute_high_watermark(
        donor_end.0,
        &follower_ends,
        majority,
        -1,
        0,
    ));

    normalize_durable_prefix(replicas, &ends, donor_index, durable)?;
    Ok(durable)
}

fn bootstrap_durable_prefix(
    replicas: &[WalReplica],
    source: NodeId,
) -> Result<Offset, BrokerError> {
    let ends = replicas.iter().map(replica_end_offset).collect::<Vec<_>>();
    let source_index = replicas
        .iter()
        .position(|replica| replica.id == source)
        .ok_or_else(|| {
            BrokerError::Replication(format!(
                "wal quorum bootstrap source {} is not a voter",
                source.0
            ))
        })?;
    let durable = ends[source_index];
    normalize_durable_prefix(replicas, &ends, source_index, durable)?;
    Ok(durable)
}

fn normalize_durable_prefix(
    replicas: &[WalReplica],
    ends: &[Offset],
    donor_index: usize,
    durable: Offset,
) -> Result<(), BrokerError> {
    for replica in replicas {
        let mut log = replica.log.lock();
        log.truncate_to(durable)?;
    }
    for (replica, end) in replicas.iter().zip(ends) {
        let batches = read_batches_exact(&replicas[donor_index].log, (*end).min(durable), durable)?;
        sync_replica_blocking(&replica.log, &batches)?;
    }
    Ok(())
}

fn replica_end_offset(replica: &WalReplica) -> Offset {
    Offset(replica.log.end_offset())
}

#[derive(Debug, Clone)]
struct BatchBytes {
    base_offset: Offset,
    last_offset: Offset,
    verbatim: VerbatimBatch,
}

fn read_batches(
    source: &ShardLog,
    start: Offset,
    target: Offset,
) -> Result<Vec<BatchBytes>, BrokerError> {
    let raw = source
        .lock()
        // Replication must carry every batch in `start..target`, so the read
        // is uncapped.
        .read_raw(start, target, ByteSize::from_bytes(u64::MAX))?;
    split_batches(&raw.bytes)
}

fn read_batches_exact(
    source: &ShardLog,
    start: Offset,
    target: Offset,
) -> Result<Vec<BatchBytes>, BrokerError> {
    if start == target {
        return Ok(Vec::new());
    }
    let batches = read_batches(source, start, target)?;
    let first = batches.first().map(|batch| batch.base_offset);
    let end = batches
        .last()
        .and_then(|batch| batch.last_offset.0.checked_add(1))
        .map(Offset);
    if (first, end) != (Some(start), Some(target)) {
        return Err(BrokerError::Replication(format!(
            "wal source does not contain the complete range {}..{}",
            start.0, target.0
        )));
    }
    Ok(batches)
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

async fn sync_replica(log: ShardLog, batches: &[BatchBytes]) -> Result<(), BrokerError> {
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

fn sync_replica_blocking(log: &ShardLog, batches: &[BatchBytes]) -> Result<(), BrokerError> {
    let mut log = log.lock();
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
