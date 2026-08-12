//! In-process WAL quorum engine.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
};

use bytes::Bytes;
use crabka_ids::{LeaderEpoch, Offset, ProducerId};
use crabka_kraft_core::{LogView as _, NodeId};
use crabka_log::{Log, VerbatimBatch};
use crabka_protocol::records::RecordBatch;
use crabka_units::{ByteSize, convert::ByteSizeExt as _};
use tokio::sync::Notify;

use crate::{error::BrokerError, wal::quorum::log_view::ShardLog};

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
    replicas: Vec<WalReplica>,
    expected_voters: usize,
    durable_watermark: AtomicI64,
    local_durable: AtomicI64,
    distributed_required: AtomicBool,
    distributed: Mutex<Option<DistributedQuorum>>,
    durable_advanced: Notify,
}

#[derive(Debug)]
struct DistributedQuorum {
    me: NodeId,
    voters: Vec<NodeId>,
    durable_offsets: HashMap<NodeId, Offset>,
}

pub(super) fn strict_majority(voter_count: usize) -> usize {
    voter_count.div_euclid(2).saturating_add(1)
}

/// One response from the leader-side WAL fetch path.
#[derive(Debug)]
pub(crate) struct WalFetchData {
    pub(crate) high_watermark: Offset,
    pub(crate) log_end_offset: Offset,
    pub(crate) log_start_offset: Offset,
    pub(crate) records: Bytes,
    pub(crate) offset_out_of_range: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum OpenMode {
    BootstrapFrom(NodeId),
    Recover,
    Distributed,
}

impl WalShardEngine {
    pub(crate) fn new(replicas: Vec<WalReplica>, mode: OpenMode) -> Result<Self, BrokerError> {
        if replicas.is_empty() {
            return Err(BrokerError::Replication(
                "wal quorum must contain at least one replica".into(),
            ));
        }
        let expected_voters = replicas.len();
        let durable_watermark = match mode {
            OpenMode::BootstrapFrom(source) => bootstrap_durable_prefix(&replicas, source)?,
            OpenMode::Recover => {
                recover_durable_prefix(&replicas, strict_majority(expected_voters))?
            }
            OpenMode::Distributed => replicas[0].log.lock().log_start_offset(),
        };
        Ok(Self {
            replicas,
            expected_voters,
            durable_watermark: AtomicI64::new(durable_watermark.0),
            local_durable: AtomicI64::new(durable_watermark.0),
            distributed_required: AtomicBool::new(false),
            distributed: Mutex::new(None),
            durable_advanced: Notify::new(),
        })
    }

    pub(crate) fn new_distributed(
        source: Arc<Mutex<Log>>,
        expected_voters: usize,
    ) -> Result<Self, BrokerError> {
        if expected_voters == 0 {
            return Err(BrokerError::Replication(
                "diskless WAL voter count must be positive".into(),
            ));
        }
        if expected_voters.is_multiple_of(2) {
            return Err(BrokerError::Replication(
                "diskless WAL voter count must be odd".into(),
            ));
        }
        let local_durable = {
            let mut log = source
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            log.sync()?;
            log.log_end_offset()
        };
        let mut engine = Self::new(
            vec![WalReplica::new(NodeId(0), source)],
            OpenMode::Distributed,
        )?;
        engine.expected_voters = expected_voters;
        engine
            .local_durable
            .store(local_durable.0, Ordering::Release);
        Ok(engine)
    }

    /// Switch this shard from the compatibility-only local replica harness to
    /// the metadata-selected broker voter set. Production registries call this
    /// before the shard can acknowledge another append.
    pub(crate) fn configure_distributed(&self, me: NodeId, voters: &[NodeId]) {
        self.distributed_required.store(true, Ordering::Release);
        let mut distributed = self
            .distributed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if voters.first() != Some(&me) || voters.len() != self.expected_voters {
            *distributed = None;
            drop(distributed);
            self.durable_advanced.notify_waiters();
            return;
        }
        if distributed
            .as_ref()
            .is_some_and(|current| current.voters == voters)
        {
            return;
        }
        let previous = distributed
            .take()
            .map_or_else(HashMap::new, |current| current.durable_offsets);
        let durable_offsets = voters
            .iter()
            .filter_map(|voter| previous.get(voter).copied().map(|offset| (*voter, offset)))
            .collect();
        *distributed = Some(DistributedQuorum {
            me,
            voters: voters.to_vec(),
            durable_offsets,
        });
        if let Some(source) = self.replicas.first() {
            let (log_start, log_end) = {
                let log = source.log.lock();
                (log.log_start_offset(), log.log_end_offset())
            };
            drop(distributed);
            let local_durable = Offset(
                self.local_durable
                    .load(Ordering::Acquire)
                    .clamp(log_start.0, log_end.0),
            );
            self.record_durable_offset(me, local_durable, log_start, log_end);
        }
    }

    /// Record the offset a remote voter requested after its preceding fsync.
    /// Returns `true` when the quorum-durable watermark advanced.
    pub(crate) fn record_follower_ack(&self, from: NodeId, offset: Offset) -> bool {
        let is_remote_voter = self
            .distributed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|quorum| quorum.voters.get(1..))
            .is_some_and(|followers| followers.contains(&from));
        if !is_remote_voter {
            return false;
        }
        let Some(source) = self.replicas.first() else {
            return false;
        };
        let (log_start, log_end) = {
            let log = source.log.lock();
            (log.log_start_offset(), log.log_end_offset())
        };
        if !(log_start..=log_end).contains(&offset) {
            return false;
        }
        self.record_durable_offset(from, offset, log_start, log_end)
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn for_logs(logs: std::collections::BTreeMap<NodeId, Arc<Mutex<Log>>>) -> Self {
        let replicas = logs
            .into_iter()
            .map(|(id, log)| WalReplica::new(id, log))
            .collect();
        Self::new(replicas, OpenMode::Recover).expect("test WAL quorum recovers")
    }

    #[must_use]
    pub(crate) fn durable_watermark(&self) -> Offset {
        Offset(self.durable_watermark.load(Ordering::Acquire))
    }

    /// Mark an adopted checkpointed follower prefix as locally durable.
    /// Promotion has already fsynced and exact-validated this range against
    /// the canonical log. A quorum watermark still advances only after enough
    /// configured voters report the same frontier.
    pub(crate) fn adopt_local_durable_prefix(
        &self,
        durable: Offset,
        log_start: Offset,
        log_end: Offset,
    ) {
        if !(log_start..=log_end).contains(&durable) {
            return;
        }
        self.local_durable.fetch_max(durable.0, Ordering::AcqRel);
        let me = self
            .distributed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|quorum| quorum.me);
        if let Some(me) = me {
            self.record_durable_offset(me, durable, log_start, log_end);
        }
    }

    pub(crate) async fn wait_for_durable_advance(&self, after: Offset) -> Offset {
        loop {
            let advanced = self.durable_advanced.notified();
            let current = self.durable_watermark();
            if current.cmp(&after).is_gt() {
                return current;
            }
            advanced.await;
        }
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

    #[cfg(test)]
    pub(crate) fn replica_start_offsets(&self) -> Vec<Offset> {
        self.replicas
            .iter()
            .map(|replica| replica.log.lock().log_start_offset())
            .collect()
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

        if self.distributed_required.load(Ordering::Acquire) {
            let configured = self
                .distributed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some();
            if !configured {
                return Err(BrokerError::Replication(
                    "diskless WAL broker placement is not available".into(),
                ));
            }
            sync_replica(source.clone(), &[]).await?;
            self.local_durable.fetch_max(target.0, Ordering::AcqRel);
            let me = self
                .distributed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .map(|quorum| quorum.me)
                .ok_or_else(|| {
                    BrokerError::Replication("diskless WAL broker placement disappeared".into())
                })?;
            let log_start = source.lock().log_start_offset();
            self.record_durable_offset(me, target, log_start, source_end);
            loop {
                if self.durable_watermark() >= target {
                    return Ok(self.durable_watermark());
                }
                let advanced = self.durable_advanced.notified();
                if self.durable_watermark() >= target {
                    return Ok(self.durable_watermark());
                }
                if self
                    .distributed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_none()
                {
                    return Err(BrokerError::Replication(
                        "diskless WAL broker placement disappeared".into(),
                    ));
                }
                advanced.await;
            }
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
        let required = strict_majority(self.expected_voters);
        if synced < required {
            return Err(BrokerError::Replication(format!(
                "wal quorum has {synced} synced replicas, needs {required}"
            )));
        }
        self.durable_watermark.store(target.0, Ordering::Release);
        Ok(target)
    }

    fn record_durable_offset(
        &self,
        from: NodeId,
        offset: Offset,
        log_start: Offset,
        leader_end: Offset,
    ) -> bool {
        let mut distributed = self
            .distributed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(quorum) = distributed.as_mut() else {
            return false;
        };
        if !quorum.voters.contains(&from) {
            return false;
        }
        let previous = quorum
            .durable_offsets
            .get(&from)
            .copied()
            .unwrap_or(log_start);
        if offset.cmp(&previous).is_gt() {
            quorum.durable_offsets.insert(from, offset);
        }
        let follower_ends = quorum
            .voters
            .iter()
            .filter(|voter| **voter != quorum.me)
            .map(|voter| {
                quorum
                    .durable_offsets
                    .get(voter)
                    .copied()
                    .unwrap_or(log_start)
                    .0
            })
            .collect::<Vec<_>>();
        let current = self.durable_watermark();
        let durable = Offset(crabka_verified::recompute_high_watermark(
            leader_end.0,
            &follower_ends,
            strict_majority(quorum.voters.len()),
            current.0,
            log_start.0,
        ));
        if durable <= current {
            return false;
        }
        self.durable_watermark.store(durable.0, Ordering::Release);
        drop(distributed);
        self.durable_advanced.notify_waiters();
        true
    }

    pub(crate) fn serve_fetch(
        &self,
        fetch_offset: Offset,
        max_size: ByteSize,
    ) -> Result<WalFetchData, BrokerError> {
        let replica = self
            .replicas
            .iter()
            .find(|replica| replica.alive.load(Ordering::Acquire))
            .ok_or_else(|| {
                BrokerError::Replication("wal quorum has no live fetch replica".into())
            })?;
        let log = replica.log.lock();
        let log_start_offset = log.log_start_offset();
        let log_end_offset = log.log_end_offset();
        let offset_out_of_range = fetch_offset < log_start_offset || fetch_offset > log_end_offset;
        let records = if offset_out_of_range
            || fetch_offset == log_end_offset
            || max_size == ByteSize::ZERO
        {
            Bytes::new()
        } else {
            // A WAL follower must receive the leader's uncommitted tail and
            // fsync it before that follower can acknowledge the range. Limiting
            // this read to the current high watermark creates a deadlock: no
            // follower can fetch the bytes needed to advance the watermark.
            log.read_raw(fetch_offset, log_end_offset, max_size)?.bytes
        };
        Ok(WalFetchData {
            high_watermark: self.durable_watermark(),
            log_end_offset,
            log_start_offset,
            records,
            offset_out_of_range,
        })
    }

    pub(crate) async fn trim_to_offset(
        &self,
        source: &Arc<Mutex<Log>>,
        new_start: Offset,
    ) -> Result<Offset, BrokerError> {
        let source_replica = self
            .replicas
            .first()
            .ok_or_else(|| BrokerError::Replication("wal quorum has no source replica".into()))?;
        if !source_replica.log.shares_log(source) {
            return Err(BrokerError::Replication(
                "wal quorum source is not its first replica".into(),
            ));
        }
        if self.distributed_required.load(Ordering::Acquire) {
            if self
                .distributed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none()
            {
                return Err(BrokerError::Replication(
                    "diskless WAL broker placement is not available".into(),
                ));
            }
            return trim_log(ShardLog::new(source.clone()), new_start).await;
        }
        // Trim replica copies before the partition source. If one copy fails,
        // the source remains available and a later flusher tick can retry.
        for replica in &self.replicas[1..] {
            trim_log(replica.log.clone(), new_start).await?;
        }
        trim_log(ShardLog::new(source.clone()), new_start).await
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
pub(super) struct BatchBytes {
    pub(super) base_offset: Offset,
    pub(super) last_offset: Offset,
    pub(super) verbatim: VerbatimBatch,
}

fn read_batches(
    source: &ShardLog,
    start: Offset,
    target: Offset,
) -> Result<Vec<BatchBytes>, BrokerError> {
    let raw = source.lock().read_raw(
        start,
        target,
        // Replication must carry every batch in `start..target`, so the read
        // is uncapped.
        ByteSize::from_bytes(u64::MAX),
    )?;
    split_batches(&raw.bytes)
}

pub(super) fn read_batches_exact(
    source: &ShardLog,
    start: Offset,
    target: Offset,
) -> Result<Vec<BatchBytes>, BrokerError> {
    exact_batches(read_batches(source, start, target)?, start, target)
}

pub(super) fn read_log_batches_exact(
    source: &Log,
    start: Offset,
    target: Offset,
) -> Result<Vec<BatchBytes>, BrokerError> {
    let raw = source.read_raw(start, target, ByteSize::from_bytes(u64::MAX))?;
    exact_batches(split_batches(&raw.bytes)?, start, target)
}

fn exact_batches(
    batches: Vec<BatchBytes>,
    start: Offset,
    target: Offset,
) -> Result<Vec<BatchBytes>, BrokerError> {
    if start == target {
        return Ok(Vec::new());
    }
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

pub(super) fn split_batches(bytes: &Bytes) -> Result<Vec<BatchBytes>, BrokerError> {
    let mut out = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let mut cur = bytes.slice(offset..);
        let remaining = cur.len();
        let batch = RecordBatch::decode(&mut cur)
            .map_err(|err| BrokerError::Replication(format!("decode WAL batch: {err}")))?;
        let consumed = remaining
            .checked_sub(cur.len())
            .and_then(std::num::NonZeroUsize::new)
            .ok_or_else(|| BrokerError::Replication("invalid WAL batch length".into()))?;
        let end = offset
            .checked_add(consumed.get())
            .filter(|end| bytes.get(offset..*end).is_some())
            .ok_or_else(|| BrokerError::Replication("invalid WAL batch length".into()))?;
        let base_offset = Offset(batch.base_offset);
        let delta = i64::from(batch.last_offset_delta);
        let last_offset = Offset(
            batch
                .base_offset
                .checked_add(delta)
                .filter(|_| delta >= 0)
                .ok_or_else(|| BrokerError::Replication("invalid WAL batch offset range".into()))?,
        );
        out.push(BatchBytes {
            base_offset,
            last_offset,
            verbatim: VerbatimBatch {
                bytes: bytes.slice(offset..end),
                last_offset_delta: batch.last_offset_delta,
                max_timestamp: batch.max_timestamp,
                leader_epoch: LeaderEpoch(batch.partition_leader_epoch),
                producer_id: ProducerId(batch.producer_id),
                producer_epoch: batch.producer_epoch,
                base_sequence: batch.base_sequence,
                is_transactional: batch.attributes.is_transactional(),
            },
        });
        offset = end;
    }
    Ok(out)
}

pub(super) async fn sync_replica(log: ShardLog, batches: &[BatchBytes]) -> Result<(), BrokerError> {
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

async fn trim_log(log: ShardLog, new_start: Offset) -> Result<Offset, BrokerError> {
    if tokio::runtime::Handle::current().runtime_flavor()
        == tokio::runtime::RuntimeFlavor::MultiThread
    {
        tokio::task::block_in_place(|| log.lock().trim_to_offset(new_start).map_err(Into::into))
    } else {
        tokio::task::spawn_blocking(move || {
            log.lock().trim_to_offset(new_start).map_err(Into::into)
        })
        .await
        .map_err(|error| {
            crate::partition_writer::storage_failure_error("wal trim task panicked", error)
        })?
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
