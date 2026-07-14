//! In-process raw-KV split runtime for exercising the durable split seams.
//!
//! This is deliberately a test runtime, not tenant SQL serving. It uses real
//! checkpoint objects, WAL frames, filtered restore, and the range recovery
//! prologue over [`MemKv`].

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use crabka_gres_ranges::{
    CheckpointManifest, InDoubtMarker, RangeId, RangeKey, SplitError, SplitHooks, SplitState,
    SplitStateStore, TableId,
    prologue::{
        InDoubtSettlement, ProducedBarrier, PrologueError, Range0RecoveryHooks,
        RangeRecoverySubstrate, RecoverRange, ReplaySummary, ServingGate, ServingRange,
        SettleOutcome, recover_range,
    },
};
use crabka_pgkv::{Kv, MemKv, SnapshotKv, WriteOp, key};

use crate::{
    CheckpointConfig, CheckpointFilter, CheckpointService, CheckpointSnapshotSource,
    CheckpointStats, CheckpointTrigger, CommittedWalReader, InMemoryWalLog, RecoveryBarrier,
    RecoveryFencer, RestoreTail, TransactionalWalWriter, WalFrame, WriterGeneration, apply_frame,
    checkpoint::{CheckpointStore, InMemoryCheckpointStore},
    restore_latest_filtered_and_replay_tail,
};

/// Durable in-memory state storage solely for the raw-KV split test runtime.
#[derive(Debug, Default)]
pub struct InMemorySplitStateStore {
    states: Mutex<BTreeMap<String, SplitState>>,
}

#[async_trait]
impl SplitStateStore for InMemorySplitStateStore {
    async fn load_split_state(&self, operation_id: &str) -> Result<Option<SplitState>, SplitError> {
        Ok(self
            .states
            .lock()
            .map_err(|_| SplitError::Store("in-memory split state lock poisoned".into()))?
            .get(operation_id)
            .cloned())
    }

    async fn save_split_state(&self, state: &SplitState) -> Result<(), SplitError> {
        self.states
            .lock()
            .map_err(|_| SplitError::Store("in-memory split state lock poisoned".into()))?
            .insert(state.operation_id.clone(), state.clone());
        Ok(())
    }
}

/// Concrete raw-KV runtime and [`SplitHooks`] adapter.
///
/// All accepted data keys must be `pgkv` table-row keys. This restriction is
/// intentional: a range interval cannot safely classify other key classes.
pub struct RawKvSplitRuntime {
    tenant: String,
    checkpoints: Arc<InMemoryCheckpointStore>,
    ranges: Mutex<BTreeMap<RangeId, Arc<RawKvRange>>>,
    committed_map: Mutex<Option<crabka_gres_ranges::RangeMap>>,
}

struct RawKvRange {
    kv: std::sync::RwLock<Arc<MemKv>>,
    wal: Arc<InMemoryWalLog>,
    snapshot: CheckpointSnapshotSource,
    write_gate: tokio::sync::Mutex<()>,
    accepting_writes: std::sync::atomic::AtomicBool,
    serving: std::sync::atomic::AtomicBool,
    pause_barrier: tokio::sync::Mutex<Option<i64>>,
    recovered_serving_state: tokio::sync::Mutex<Option<ServingRange>>,
}

impl RawKvSplitRuntime {
    /// Create an empty raw-KV runtime isolated under `tenant` checkpoint objects.
    #[must_use]
    pub fn new(tenant: impl Into<String>) -> Self {
        Self {
            tenant: tenant.into(),
            checkpoints: InMemoryCheckpointStore::shared(),
            ranges: Mutex::new(BTreeMap::new()),
            committed_map: Mutex::new(None),
        }
    }

    /// Add an initially serving range backed by a fresh `MemKv` and WAL.
    ///
    /// # Errors
    ///
    /// Returns an error when the range already exists or checkpoint wiring fails.
    pub fn add_serving_range(&self, range: RangeId) -> Result<Arc<MemKv>, SplitError> {
        let raw_range = Self::new_range();
        raw_range
            .accepting_writes
            .store(true, std::sync::atomic::Ordering::SeqCst);
        raw_range
            .serving
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let kv = raw_range.kv()?;
        let mut ranges = self.ranges()?;
        if ranges.insert(range, Arc::new(raw_range)).is_some() {
            return Err(SplitError::Hook(format!("range r{range} already exists")));
        }
        Ok(kv)
    }

    /// Write one supported row key through the range WAL and apply it locally.
    ///
    /// # Errors
    ///
    /// Returns an error for parked/paused ranges or keys that cannot be routed
    /// by a raw range interval.
    pub async fn write_row(
        &self,
        range: RangeId,
        key_bytes: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), SplitError> {
        let range_key = parse_row_range_key(&key_bytes)?;
        self.ensure_committed_range_owns_key(range, range_key)?;
        let raw_range = self.range(range)?;
        let _gate = raw_range.write_gate.lock().await;
        if !raw_range
            .accepting_writes
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(SplitError::Hook(format!(
                "range r{range} write gate is closed"
            )));
        }
        let sequence = raw_range.snapshot.snapshot().journal_seq;
        let frame = WalFrame {
            journal_seq: sequence,
            ops: vec![WriteOp::Put {
                key: key_bytes,
                value,
            }],
        };
        let ack = raw_range
            .wal
            .commit_group(crate::GroupCommitRequest {
                generation: raw_range.wal.current_generation().await,
                frames: vec![frame.clone()],
            })
            .await
            .map_err(split_hook_error)?;
        apply_frame(raw_range.kv()?.as_ref(), &frame.ops)
            .map_err(|error| SplitError::Hook(error.to_string()))?;
        raw_range.snapshot.record_commit(ack.frames[0]);
        Ok(())
    }

    /// Return a range's local KV object for raw assertions.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn kv(&self, range: RangeId) -> Result<Arc<MemKv>, SplitError> {
        self.range(range)?.kv()
    }

    /// Return whether a range has completed prologue and remains serving.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn is_serving(&self, range: RangeId) -> Result<bool, SplitError> {
        Ok(self
            .range(range)?
            .serving
            .load(std::sync::atomic::Ordering::SeqCst))
    }

    /// Return whether the supplied checkpoint manifest object is durable.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn has_checkpoint_manifest(&self, key: &str) -> Result<bool, SplitError> {
        Ok(self.checkpoints.get(key).await.is_ok_and(|_| true))
    }

    /// Force a checkpoint using the range's current post-recovery writer state.
    ///
    /// # Errors
    ///
    /// Returns an error when the range is unavailable or checkpointing fails.
    pub async fn force_checkpoint(
        &self,
        range: RangeId,
    ) -> Result<crate::CheckpointRun, SplitError> {
        let raw_range = self.range(range)?;
        let _gate = raw_range.write_gate.lock().await;
        let checkpoint = self.checkpoint_service(range, raw_range.kv()?, raw_range.wal.clone())?;
        checkpoint
            .checkpoint(raw_range.snapshot.snapshot(), CheckpointTrigger::Manual)
            .await
            .map_err(split_hook_error)
    }

    fn new_range() -> RawKvRange {
        let kv = Arc::new(MemKv::default());
        let wal = InMemoryWalLog::shared();
        RawKvRange {
            kv: std::sync::RwLock::new(kv),
            wal,
            snapshot: CheckpointSnapshotSource::new(-1, 0, WriterGeneration(0)),
            write_gate: tokio::sync::Mutex::new(()),
            accepting_writes: std::sync::atomic::AtomicBool::new(false),
            serving: std::sync::atomic::AtomicBool::new(false),
            pause_barrier: tokio::sync::Mutex::new(None),
            recovered_serving_state: tokio::sync::Mutex::new(None),
        }
    }

    fn range(&self, range: RangeId) -> Result<Arc<RawKvRange>, SplitError> {
        self.ranges()?
            .get(&range)
            .cloned()
            .ok_or_else(|| SplitError::Hook(format!("raw-KV range r{range} is not registered")))
    }

    fn ranges(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, BTreeMap<RangeId, Arc<RawKvRange>>>, SplitError> {
        self.ranges
            .lock()
            .map_err(|_| SplitError::Hook("raw-KV range registry lock poisoned".into()))
    }

    fn checkpoint_tenant(&self, range: RangeId) -> String {
        format!("{}-r{range}", self.tenant)
    }

    fn checkpoint_service(
        &self,
        range: RangeId,
        kv: Arc<MemKv>,
        wal: Arc<InMemoryWalLog>,
    ) -> Result<CheckpointService<InMemoryWalLog>, SplitError> {
        CheckpointService::new(
            CheckpointConfig::new(
                self.checkpoint_tenant(range),
                format!("raw-r{range}"),
                1,
                0,
                1024,
                2,
            )
            .map_err(split_hook_error)?,
            kv as Arc<dyn SnapshotKv>,
            self.checkpoints.clone() as Arc<dyn CheckpointStore>,
            wal,
            Arc::new(CheckpointStats::default()),
        )
        .map_err(split_hook_error)
    }

    fn ensure_committed_range_owns_key(
        &self,
        range: RangeId,
        key: RangeKey,
    ) -> Result<(), SplitError> {
        let committed_map = self
            .committed_map
            .lock()
            .map_err(|_| SplitError::Hook("raw-KV map lock poisoned".into()))?;
        let Some(map) = committed_map.as_ref() else {
            return Ok(());
        };
        let route = map
            .range_for_key(key.table_id, key.rowid)
            .map_err(|error| {
                SplitError::Hook(format!("raw-KV key is not committed-map routable: {error}"))
            })?;
        if route.range_id != range {
            return Err(SplitError::Hook(format!(
                "raw-KV key belongs to committed range r{}, not r{range}",
                route.range_id
            )));
        }
        Ok(())
    }
}

impl RawKvRange {
    fn kv(&self) -> Result<Arc<MemKv>, SplitError> {
        self.kv
            .read()
            .map_err(|_| SplitError::Hook("raw-KV range store lock poisoned".into()))
            .map(|kv| kv.clone())
    }

    fn replace_kv(&self, kv: Arc<MemKv>) -> Result<(), SplitError> {
        *self
            .kv
            .write()
            .map_err(|_| SplitError::Hook("raw-KV range store lock poisoned".into()))? = kv;
        Ok(())
    }
}

#[async_trait]
impl SplitHooks for RawKvSplitRuntime {
    async fn pause_conversion_writes(&self, state: &SplitState) -> Result<(), SplitError> {
        self.pause_writes_at_covered_offset(
            state,
            state
                .checkpoint
                .as_ref()
                .ok_or(SplitError::MissingCheckpoint)?,
        )
        .await
    }

    async fn force_predecessor_checkpoint(
        &self,
        state: &SplitState,
    ) -> Result<CheckpointManifest, SplitError> {
        let run = self.force_checkpoint(state.predecessor).await?;
        Ok(CheckpointManifest {
            range_id: state.predecessor,
            covered_offset: run.manifest.covered_offset,
            manifest_key: run.metadata.manifest_key,
        })
    }

    async fn force_right_predecessor_checkpoint(
        &self,
        _state: &SplitState,
    ) -> Result<CheckpointManifest, SplitError> {
        Err(SplitError::Hook(
            "raw-KV split runtime supports split only, not merge".into(),
        ))
    }

    async fn pause_writes_at_covered_offset(
        &self,
        state: &SplitState,
        checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError> {
        let raw_range = self.range(state.predecessor)?;
        let _gate = raw_range.write_gate.lock().await;
        if raw_range.snapshot.snapshot().covered_offset < checkpoint.covered_offset {
            return Err(SplitError::Hook(
                "predecessor WAL is behind the requested checkpoint boundary".into(),
            ));
        }
        raw_range
            .accepting_writes
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let barrier = raw_range
            .wal
            .fence_with_barrier()
            .await
            .map_err(split_hook_error)?;
        *raw_range.pause_barrier.lock().await = Some(barrier.offset);
        Ok(())
    }

    async fn commit_map_version(&self, state: &SplitState) -> Result<(), SplitError> {
        let mut committed_map = self
            .committed_map
            .lock()
            .map_err(|_| SplitError::Hook("raw-KV map lock poisoned".into()))?;
        *committed_map = Some(state.target_map.clone());
        if self.ranges()?.contains_key(&state.successor) {
            return Ok(());
        }
        let successor = Arc::new(Self::new_range());
        self.ranges()?.insert(state.successor, successor);
        Ok(())
    }

    async fn start_successor_restore(
        &self,
        state: &SplitState,
        checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError> {
        let successor = self.range(state.successor)?;
        let predecessor = self.range(state.predecessor)?;
        self.checkpoints
            .get(&checkpoint.manifest_key)
            .await
            .map_err(split_hook_error)?;
        let filter = CheckpointFilter::new(state.successor_after.start, state.successor_after.end)
            .map_err(split_hook_error)?
            .with_physical_to_logical(raw_identity_table_mapping(predecessor.kv()?.as_ref())?);
        let tail =
            RestoreTail {
                current_generation: predecessor.snapshot.snapshot().wal_generation,
                log_start: predecessor
                    .wal
                    .log_start_offset()
                    .await
                    .map_err(split_hook_error)?,
                committed_frames: predecessor
                    .wal
                    .committed_from_start()
                    .await
                    .map_err(split_hook_error)?,
                barrier_offset: predecessor.pause_barrier.lock().await.ok_or_else(|| {
                    SplitError::Hook("predecessor pause barrier is missing".into())
                })?,
            };
        let staged_kv = Arc::new(MemKv::default());
        restore_latest_filtered_and_replay_tail(
            self.checkpoints.as_ref(),
            &self.checkpoint_tenant(state.predecessor),
            staged_kv.as_ref(),
            tail,
            filter,
        )
        .await
        .map_err(split_hook_error)?;
        successor.replace_kv(staged_kv)?;
        Ok(())
    }

    async fn start_merge_successor_restore(
        &self,
        _state: &SplitState,
        _left: &CheckpointManifest,
        _right: &CheckpointManifest,
    ) -> Result<(), SplitError> {
        Err(SplitError::Hook(
            "raw-KV split runtime supports split only, not merge".into(),
        ))
    }

    async fn successor_fence_prologue(&self, state: &SplitState) -> Result<(), SplitError> {
        let successor = self.range(state.successor)?;
        let _write_gate = successor.write_gate.lock().await;
        if successor.recovered_serving_state.lock().await.is_some() {
            return Ok(());
        }
        let successor_kv = successor.kv()?;
        let adapter = RawPrologue {
            range: state.successor,
            wal: successor.wal.clone(),
            barrier: Mutex::new(None),
        };
        let serving_gate = RawServingGate {
            raw_range: successor.clone(),
        };
        recover_range(RecoverRange {
            range: state.successor,
            store: successor_kv.as_ref(),
            substrate: &adapter,
            range0_hooks: &NoRange0Hooks,
            settlement: &NoInDoubtSettlement,
            serving_gate: &serving_gate,
        })
        .await
        .map_err(|error| SplitError::Hook(error.to_string()))?;
        Ok(())
    }

    async fn inherit_in_doubt_markers(
        &self,
        state: &SplitState,
    ) -> Result<Vec<InDoubtMarker>, SplitError> {
        self.open_recovered_successor(state.successor).await?;
        Ok(Vec::new())
    }

    async fn park_predecessor(&self, state: &SplitState) -> Result<(), SplitError> {
        let predecessor = self.range(state.predecessor)?;
        predecessor
            .accepting_writes
            .store(false, std::sync::atomic::Ordering::SeqCst);
        predecessor
            .serving
            .store(false, std::sync::atomic::Ordering::SeqCst);
        predecessor
            .wal
            .fence_with_barrier()
            .await
            .map_err(split_hook_error)?;
        Ok(())
    }

    async fn park_right_predecessor(&self, _state: &SplitState) -> Result<(), SplitError> {
        Err(SplitError::Hook(
            "raw-KV split runtime supports split only, not merge".into(),
        ))
    }

    async fn unpause_serving(&self, _state: &SplitState) -> Result<(), SplitError> {
        Ok(())
    }
}

impl RawKvSplitRuntime {
    async fn open_recovered_successor(&self, range: RangeId) -> Result<(), SplitError> {
        let raw_range = self.range(range)?;
        let _write_gate = raw_range.write_gate.lock().await;
        if raw_range.serving.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }
        let recovery = raw_range
            .recovered_serving_state
            .lock()
            .await
            .take()
            .ok_or_else(|| {
                SplitError::Hook("successor recovery result is missing before serving opens".into())
            })?;
        let generation = u64::try_from(recovery.epoch)
            .map(WriterGeneration)
            .map_err(|_| SplitError::Hook("prologue epoch is negative".into()))?;
        raw_range.snapshot.record_recovery(
            generation,
            recovery.barrier_offset,
            recovery.next_journal_seq,
        );
        raw_range
            .accepting_writes
            .store(true, std::sync::atomic::Ordering::SeqCst);
        raw_range
            .serving
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

fn split_hook_error(error: impl std::fmt::Display) -> SplitError {
    SplitError::Hook(error.to_string())
}

fn parse_row_range_key(key_bytes: &[u8]) -> Result<RangeKey, SplitError> {
    if let Some((table_id, bucket, rowid)) = key::table_bucket_rowid_of(key_bytes) {
        return Ok(RangeKey::hash(
            TableId::new(u64::from(table_id)),
            bucket,
            rowid,
        ));
    }
    let (table_id, rowid) = key::table_rowid_of(key_bytes).ok_or_else(|| {
        SplitError::Hook("raw-KV split runtime rejects unsupported non-row key".into())
    })?;
    Ok(RangeKey::new(TableId::new(u64::from(table_id)), rowid))
}

fn raw_identity_table_mapping(
    kv: &dyn SnapshotKv,
) -> Result<BTreeMap<TableId, TableId>, SplitError> {
    let mut snapshot = kv.snapshot().map_err(split_hook_error)?;
    let mut mapping = BTreeMap::new();
    while let Some((key_bytes, _)) = snapshot.next().map_err(split_hook_error)? {
        let table_id = key::table_bucket_rowid_of(&key_bytes)
            .map(|(table_id, _, _)| table_id)
            .or_else(|| key::table_rowid_of(&key_bytes).map(|(table_id, _)| table_id));
        if let Some(table_id) = table_id {
            let table_id = TableId::new(u64::from(table_id));
            mapping.insert(table_id, table_id);
        }
    }
    Ok(mapping)
}

struct RawPrologue {
    range: RangeId,
    wal: Arc<InMemoryWalLog>,
    barrier: Mutex<Option<RecoveryBarrier>>,
}

#[async_trait]
impl RangeRecoverySubstrate for RawPrologue {
    async fn fence_epoch(&self, range: RangeId) -> Result<i16, PrologueError> {
        if range != self.range {
            return Err(PrologueError::Substrate("prologue range mismatch".into()));
        }
        let barrier = self
            .wal
            .fence_with_barrier()
            .await
            .map_err(|error| PrologueError::Substrate(error.to_string()))?;
        let epoch = i16::try_from(barrier.generation.0)
            .map_err(|_| PrologueError::Substrate("raw-KV WAL epoch exceeds i16".into()))?;
        *self.barrier.lock().map_err(|_| {
            PrologueError::Substrate("raw-KV prologue barrier lock poisoned".into())
        })? = Some(barrier);
        Ok(epoch)
    }

    async fn produce_barrier(
        &self,
        range: RangeId,
        epoch: i16,
    ) -> Result<ProducedBarrier, PrologueError> {
        if range != self.range {
            return Err(PrologueError::Substrate("prologue range mismatch".into()));
        }
        let barrier = self
            .barrier
            .lock()
            .map_err(|_| PrologueError::Substrate("raw-KV prologue barrier lock poisoned".into()))?
            .ok_or_else(|| {
                PrologueError::Substrate("prologue barrier is missing after fencing".into())
            })?;
        let barrier_epoch = i16::try_from(barrier.generation.0)
            .map_err(|_| PrologueError::Substrate("raw-KV WAL epoch exceeds i16".into()))?;
        if barrier_epoch != epoch {
            return Err(PrologueError::Substrate(
                "prologue barrier epoch differs from fenced epoch".into(),
            ));
        }
        Ok(ProducedBarrier {
            range,
            epoch,
            offset: barrier.offset,
        })
    }

    async fn replay_to_barrier(
        &self,
        store: &dyn Kv,
        barrier: ProducedBarrier,
    ) -> Result<ReplaySummary, PrologueError> {
        let frames = self
            .wal
            .committed_from_start()
            .await
            .map_err(|error| PrologueError::Substrate(error.to_string()))?;
        let replay = crate::replay_committed_frames(store, frames, barrier.offset)
            .map_err(|error| PrologueError::Substrate(error.to_string()))?;
        Ok(ReplaySummary {
            next_journal_seq: replay.next_journal_seq,
        })
    }
}

struct NoRange0Hooks;
#[async_trait]
impl Range0RecoveryHooks for NoRange0Hooks {
    async fn reseed_counters(&self, _store: &dyn Kv) -> Result<(), PrologueError> {
        Ok(())
    }
    async fn reseed_gtm(&self, _store: &dyn Kv) -> Result<(), PrologueError> {
        Ok(())
    }
}
struct NoInDoubtSettlement;
#[async_trait]
impl InDoubtSettlement for NoInDoubtSettlement {
    async fn reacquire_in_doubt_locks(&self, _range: RangeId) -> Result<(), PrologueError> {
        Ok(())
    }
    async fn settle_once(&self, _range: RangeId) -> Result<SettleOutcome, PrologueError> {
        Ok(SettleOutcome::Complete)
    }
}
struct RawServingGate {
    raw_range: Arc<RawKvRange>,
}
#[async_trait]
impl ServingGate for RawServingGate {
    async fn mark_served(
        &self,
        range: RangeId,
        epoch: i16,
        barrier_offset: i64,
        next_journal_seq: u64,
    ) -> Result<(), PrologueError> {
        let serving = ServingRange {
            range,
            epoch,
            barrier_offset,
            next_journal_seq,
        };
        *self.raw_range.recovered_serving_state.lock().await = Some(serving);
        Ok(())
    }
}
