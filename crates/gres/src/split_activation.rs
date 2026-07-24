//! Crash-safe completion of live split activation before SQL readiness.

use std::{collections::BTreeMap, sync::Arc};

use crabka_gres_ranges::{
    RangeId, RangeKey, RangeMap, TableId,
    control::{
        RangeZeroTopologyActivationStore, TopologyActivationPhase, TopologyActivationReceipt,
        TopologyActivationReceiptStore,
    },
};
use crabka_pgexec::SqlEngine;

fn registry_boundary(key: RangeKey) -> crabka_gres_control::RangeBoundary {
    if key.bucket == 0 {
        crabka_gres_control::RangeBoundary::new(key.table_id.as_u64(), key.rowid)
    } else {
        crabka_gres_control::RangeBoundary::hash(key.table_id.as_u64(), key.bucket, key.rowid)
    }
}

fn registry_boundary_matches(
    boundary: Option<crabka_gres_control::RangeBoundary>,
    key: Option<RangeKey>,
) -> bool {
    match (boundary, key) {
        (None, None) => true,
        (Some(boundary), Some(key)) => {
            boundary.table_id == key.table_id.as_u64()
                && boundary.bucket.unwrap_or(0) == key.bucket
                && boundary.rowid == key.rowid
        }
        _ => false,
    }
}

use super::{
    LiveMultiRangeTransfer, LiveMultirangeEngines, LiveRangeEngine, LiveRangeResources,
    PrepareTopologyFault, SubstrateRuntimeConfig, TopologyActivationFault, committed_tail_sha256,
    open_live_range_substrate_engine, open_substrate_range_cache, range_pause_lock_error,
};

static RECOVERY_CACHE_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub(super) struct ActivationDiscovery {
    pub(super) receipt: TopologyActivationReceipt,
    pub(super) recovery_map: RangeMap,
    pub(super) recovery_generations: BTreeMap<RangeId, u64>,
}

impl ActivationDiscovery {
    pub(super) fn promote_authoritative_target_recovery(&mut self) -> std::io::Result<()> {
        if self.receipt.split.predecessor == RangeId::COORDINATOR {
            return Ok(());
        }
        for spec in self.receipt.split.target_map.ranges() {
            if spec.range_id == self.receipt.split.predecessor
                || (!self
                    .receipt
                    .split
                    .current_map
                    .ranges()
                    .iter()
                    .any(|current| {
                        current.range_id == spec.range_id
                            && current.range_id != self.receipt.split.predecessor
                    })
                    && !self.receipt.targets.contains_key(&spec.range_id))
            {
                return Err(std::io::Error::other(format!(
                    "authoritative activation target r{} lacks a durable descriptor",
                    spec.range_id.as_u32()
                )));
            }
        }
        self.recovery_generations
            .remove(&self.receipt.split.predecessor);
        for target in self.receipt.targets.values() {
            self.recovery_generations
                .insert(target.range_id, target.wal_generation);
        }
        self.recovery_map = self.receipt.split.target_map.clone();
        Ok(())
    }

    pub(super) fn timestamp_primary_aliases(&self) -> BTreeMap<RangeId, RangeId> {
        if self.receipt.split.predecessor == RangeId::COORDINATOR || self.receipt.targets.len() != 1
        {
            return BTreeMap::new();
        }
        let Some(target) = self.receipt.targets.values().next() else {
            return BTreeMap::new();
        };
        if target.interval.start != self.receipt.split.predecessor_before.start
            || target.interval.end != self.receipt.split.predecessor_before.end
        {
            return BTreeMap::new();
        }
        BTreeMap::from([(self.receipt.split.predecessor, target.range_id)])
    }

    pub(super) fn provisional_tenant_record(
        &self,
        current: &crabka_gres_control::TenantRecord,
        source_record_version: u64,
    ) -> std::io::Result<crabka_gres_control::TenantRecord> {
        let target_record_version = source_record_version
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("activation tenant version overflow"))?;
        if self.receipt.split.target_map.epoch() <= self.receipt.split.current_map.epoch() {
            return Err(std::io::Error::other(
                "activation target map epoch must advance current map epoch",
            ));
        }
        let layout_matches = |map: &RangeMap| {
            current.ranges.len() == map.ranges().len()
                && map.ranges().iter().all(|spec| {
                    current.ranges.iter().any(|entry| {
                        entry.range_id == spec.range_id.as_u32()
                            && registry_boundary_matches(entry.end_key, spec.end)
                    })
                })
        };
        if layout_matches(&self.receipt.split.target_map) {
            if current.record_version < target_record_version {
                return Err(std::io::Error::other(
                    "activation target tenant version predates sealed cutover",
                ));
            }
            let exact_targets = self.receipt.targets.values().all(|target| {
                current.ranges.iter().any(|entry| {
                    entry.range_id == target.range_id.as_u32()
                        && entry.endpoint == target.endpoint
                        && entry.wal_generation == target.wal_generation
                })
            });
            if exact_targets {
                return Ok(current.clone());
            }
            return Err(std::io::Error::other(
                "activation target tenant differs from sealed target descriptors",
            ));
        }
        if !layout_matches(&self.receipt.split.current_map) {
            return Err(std::io::Error::other(
                "activation receipt current map conflicts with tenant registry",
            ));
        }
        if current.record_version != source_record_version {
            return Err(std::io::Error::other(
                "activation current tenant version differs from sealed source version",
            ));
        }
        let predecessor = current
            .ranges
            .iter()
            .find(|entry| entry.range_id == self.receipt.split.predecessor.as_u32())
            .ok_or_else(|| std::io::Error::other("activation predecessor is absent from tenant"))?;
        let mut target_layout = Vec::with_capacity(self.receipt.split.target_map.ranges().len());
        for spec in self.receipt.split.target_map.ranges() {
            let mut entry = if let Some(target) = self.receipt.targets.get(&spec.range_id) {
                let mut entry = predecessor.clone();
                entry.range_id = target.range_id.as_u32();
                entry.endpoint.clone_from(&target.endpoint);
                entry.wal_generation = target.wal_generation;
                entry.retirement = None;
                entry
            } else {
                current
                    .ranges
                    .iter()
                    .find(|entry| entry.range_id == spec.range_id.as_u32())
                    .cloned()
                    .ok_or_else(|| {
                        std::io::Error::other(format!(
                            "activation target retains unknown range r{}",
                            spec.range_id.as_u32()
                        ))
                    })?
            };
            entry.end_key = spec.end.map(registry_boundary);
            target_layout.push(entry);
        }
        let mut target = current.clone();
        target.record_version = target_record_version;
        target.ranges = target_layout;
        Ok(target)
    }
}

#[derive(Clone)]
pub(super) struct PendingLiveTopology {
    pub(super) operation_id: String,
    pub(super) source_checkpoint: crabka_gres_ranges::CheckpointManifest,
    pub(super) barrier_offset: i64,
    pub(super) tail_sha256: String,
    pub(super) predecessor: RangeId,
    pub(super) left_id: RangeId,
    pub(super) left_replay_journal_seq: u64,
    pub(super) left: LiveRangeResources,
    pub(super) right: Option<(RangeId, u64, LiveRangeResources)>,
}

impl PendingLiveTopology {
    pub(super) fn abort_staged_checkpoint_workers(&self) {
        for resources in std::iter::once(&self.left)
            .chain(self.right.as_ref().map(|(_, _, resources)| resources))
        {
            if let Some(checkpoint) = &resources.checkpoint {
                checkpoint.handle.abort();
            }
        }
    }
}

pub(super) struct PreparedLiveTopology {
    pub(super) predecessor: RangeId,
    pub(super) ranges: BTreeMap<RangeId, LiveRangeResources>,
    pub(super) engines: BTreeMap<RangeId, SqlEngine>,
    pub(super) service: crabka_gres_ranges::HostedRangeService,
    pub(super) tso_rpc: Option<Arc<dyn crabka_gres_ranges::TsoRpc>>,
}

/// Runtime-owned successor kept outside the serving snapshot until publication.
pub(super) struct StagedLiveRangeSuccessor {
    pub(super) operation_id: String,
    pub(super) source_checkpoint: crabka_gres_ranges::CheckpointManifest,
    pub(super) barrier_offset: i64,
    pub(super) tail_sha256: String,
    pub(super) replay_journal_seq: u64,
    pub(super) engine: SqlEngine,
    pub(super) resources: LiveRangeResources,
}

impl LiveMultiRangeTransfer {
    pub(super) fn activation_fault(
        &self,
        fault: TopologyActivationFault,
        range_id: RangeId,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        if self
            .activation_fault
            .compare_exchange(
                fault as u8,
                TopologyActivationFault::None as u8,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_err()
        {
            return Ok(());
        }
        if std::env::var_os("CRABKA_GRES_ACTIVATION_HARD_CRASH").is_some() {
            std::process::abort();
        }
        Err(crabka_gres_ranges::RangeTransferError::Runtime {
            range_id,
            reason: format!("injected topology activation crash: {fault:?}"),
        })
    }

    fn take_prepare_fault(&self, fault: PrepareTopologyFault) -> bool {
        self.prepare_fault
            .compare_exchange(
                fault as u8,
                PrepareTopologyFault::None as u8,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
    }

    fn injected_prepare_failure(
        &self,
        fault: PrepareTopologyFault,
        range_id: RangeId,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        if !self.take_prepare_fault(fault) {
            return Ok(());
        }
        if let Some(pending) = self.pending.lock().expect("pending topology lock").take() {
            pending.abort_staged_checkpoint_workers();
        }
        Err(crabka_gres_ranges::RangeTransferError::Runtime {
            range_id,
            reason: format!("injected topology preparation fault: {fault:?}"),
        })
    }

    pub(super) fn publish_topology(
        &self,
        serving_engines: &BTreeMap<RangeId, SqlEngine>,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        self.injected_prepare_failure(PrepareTopologyFault::LockAcquisition, RangeId::COORDINATOR)?;
        let pending = self
            .pending
            .lock()
            .map_err(|_| range_pause_lock_error(RangeId::COORDINATOR))?
            .clone()
            .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: RangeId::COORDINATOR,
                reason: "claimed successor topology is missing".into(),
            })?;
        let mut ranges = self
            .ranges
            .read()
            .map_err(|_| range_pause_lock_error(pending.predecessor))?
            .clone();
        ranges.remove(&pending.predecessor);
        ranges.insert(pending.left_id, pending.left);
        if let Some((right_id, _, right)) = pending.right {
            ranges.insert(right_id, right);
        }
        let engines = serving_engines
            .iter()
            .map(|(id, engine)| (*id, engine.clone_handle()))
            .collect::<BTreeMap<_, _>>();
        let coordinator_resources = ranges.get(&RangeId::COORDINATOR).cloned();
        let mut tso_rpc = self
            .tso_rpc
            .read()
            .map_err(|_| range_pause_lock_error(RangeId::COORDINATOR))?
            .clone();
        if let Some(horizon) = coordinator_resources.and_then(|resources| resources.tso_horizon) {
            self.injected_prepare_failure(PrepareTopologyFault::HorizonLoad, pending.predecessor)?;
            let persisted_max_ts = horizon.load_max_ts().map_err(|error| {
                crabka_gres_ranges::RangeTransferError::Runtime {
                    range_id: RangeId::COORDINATOR,
                    reason: format!("load replacement TSO horizon: {error}"),
                }
            })?;
            self.injected_prepare_failure(
                PrepareTopologyFault::TsoConstruction,
                pending.predecessor,
            )?;
            tso_rpc = Some(
                super::mode_tso_rpc_from_horizon(
                    &horizon,
                    persisted_max_ts,
                    self.config.timestamp_source_mode,
                    self.config.hlc_wall_offset_ms,
                )
                .map_err(|error| {
                    crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id: RangeId::COORDINATOR,
                        reason: format!("open replacement TSO RPC: {error}"),
                    }
                })?,
            );
        }
        self.injected_prepare_failure(PrepareTopologyFault::ServiceAssembly, pending.predecessor)?;
        let mut service = crabka_gres_ranges::HostedRangeService::new(
            engines
                .iter()
                .map(|(id, engine)| (*id, engine.clone_handle()))
                .collect(),
        )
        .with_timestamp_primary_aliases(self.timestamp_primary_aliases.clone());
        let current_service = self.range_service.load();
        service = service.with_ddl_gate(current_service.ddl_gate_dispatcher());
        if let Some(barrier) = current_service.catalog_follower_dispatcher() {
            service = service.with_catalog_follower(barrier);
        }
        if let Some(inspector) = current_service.durable_inspector_dispatcher() {
            service = service.with_durable_inspector(inspector);
        }
        if let Some((registry, client)) = current_service.timestamp_primary_remote_dispatcher() {
            service = service.with_timestamp_primary_remote(registry, client);
        }
        if let Some(tso) = tso_rpc.clone() {
            service = service.with_tso(tso);
        }
        *self
            .prepared
            .lock()
            .map_err(|_| range_pause_lock_error(pending.predecessor))? =
            Some(PreparedLiveTopology {
                predecessor: pending.predecessor,
                ranges,
                engines,
                service,
                tso_rpc,
            });
        Ok(())
    }

    pub(super) fn commit_prepared_topology(&self) {
        let prepared = self
            .prepared
            .lock()
            .expect("prepared topology lock")
            .take()
            .expect("topology prepared before commit");
        let operation_id = self
            .pending
            .lock()
            .expect("pending topology lock")
            .take()
            .map(|pending| pending.operation_id);
        let retired = self
            .ranges
            .read()
            .expect("live ranges lock")
            .get(&prepared.predecessor)
            .cloned();
        *self.ranges.write().expect("live ranges lock") = prepared.ranges;
        *self.engines.write().expect("live engines lock") = prepared.engines;
        *self.tso_rpc.write().expect("live tso lock") = prepared.tso_rpc;
        if let Some(retired) = retired {
            self.retired
                .lock()
                .expect("retired ranges lock")
                .insert(prepared.predecessor, retired);
        }
        self.range_service.replace(prepared.service);
        *self
            .committed_activation
            .lock()
            .expect("committed activation lock") = operation_id;
    }
}

/// Commit the one-way activation decision through the still-canonical predecessor r0.
/// This is the final fallible instruction before canonical producer construction.
pub(super) async fn persist_must_activate(
    transfer: &LiveMultiRangeTransfer,
) -> Result<(), crabka_gres_ranges::RangeTransferError> {
    let pending = transfer
        .pending
        .lock()
        .map_err(|_| range_pause_lock_error(RangeId::COORDINATOR))?
        .clone()
        .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Runtime {
            range_id: RangeId::COORDINATOR,
            reason: "mark activation without pending topology".into(),
        })?;
    let engine = transfer
        .engines
        .read()
        .map_err(|_| range_pause_lock_error(RangeId::COORDINATOR))?
        .get(&RangeId::COORDINATOR)
        .map(SqlEngine::clone_handle)
        .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Unavailable {
            range_id: RangeId::COORDINATOR,
            reason: "predecessor range zero unavailable for must-activate receipt".into(),
        })?;
    let source_resources = transfer
        .ranges
        .read()
        .map_err(|_| range_pause_lock_error(RangeId::COORDINATOR))?
        .get(&RangeId::COORDINATOR)
        .cloned()
        .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Unavailable {
            range_id: RangeId::COORDINATOR,
            reason: "predecessor range zero resources unavailable for must-activate receipt".into(),
        })?;
    let tenant = source_resources.recovery_config.tenant.to_string();
    let store = RangeZeroTopologyActivationStore::new(tenant.clone(), engine);
    let mut receipt = store
        .load(&pending.operation_id)
        .await
        .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
            range_id: RangeId::COORDINATOR,
            reason: format!("load must-activate receipt: {reason}"),
        })?
        .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Runtime {
            range_id: RangeId::COORDINATOR,
            reason: "activation receipt missing before must-activate".into(),
        })?;
    if receipt.phase == TopologyActivationPhase::MustActivate {
        transfer.note_activation_irreversible(&pending.operation_id);
        return Ok(());
    }
    if receipt.phase != TopologyActivationPhase::SourceCheckpoint {
        return Err(crabka_gres_ranges::RangeTransferError::Boundary {
            range_id: pending.predecessor,
            reason: "must-activate requires the durable source-checkpoint phase".into(),
        });
    }
    let expected_receipt = receipt.clone();
    receipt.revision = receipt.revision.checked_add(1).ok_or_else(|| {
        crabka_gres_ranges::RangeTransferError::Runtime {
            range_id: pending.predecessor,
            reason: "activation receipt revision overflow".into(),
        }
    })?;
    receipt.phase = TopologyActivationPhase::MustActivate;
    receipt.source_checkpoint = Some(pending.source_checkpoint.clone());
    receipt.barrier_offset = Some(pending.barrier_offset);
    receipt.tail_sha256 = Some(pending.tail_sha256.clone());
    receipt
        .targets
        .get_mut(&pending.left_id)
        .expect("validated left target")
        .replay_journal_seq = Some(pending.left_replay_journal_seq);
    if let Some((right_id, replay_journal_seq, _)) = &pending.right {
        receipt
            .targets
            .get_mut(right_id)
            .expect("validated right target")
            .replay_journal_seq = Some(*replay_journal_seq);
    }
    validate_receipt_shape(&receipt).map_err(|error| {
        crabka_gres_ranges::RangeTransferError::Boundary {
            range_id: pending.predecessor,
            reason: error.to_string(),
        }
    })?;
    transfer.activation_fault(
        TopologyActivationFault::BeforeMustActivate,
        pending.predecessor,
    )?;
    let expected = serde_json::to_vec(&expected_receipt).map_err(|reason| {
        crabka_gres_ranges::RangeTransferError::Runtime {
            range_id: pending.predecessor,
            reason: format!("encode prior must-activate receipt: {reason}"),
        }
    })?;
    let value = serde_json::to_vec(&receipt).map_err(|reason| {
        crabka_gres_ranges::RangeTransferError::Runtime {
            range_id: pending.predecessor,
            reason: format!("encode must-activate receipt: {reason}"),
        }
    })?;
    let committed = if pending.predecessor == RangeId::COORDINATOR {
        let authorization = {
            let pause = source_resources
                .pause
                .lock()
                .map_err(|_| range_pause_lock_error(pending.predecessor))?;
            let super::RangePauseState::Paused(paused) = &*pause else {
                return Err(crabka_gres_ranges::RangeTransferError::Runtime {
                    range_id: pending.predecessor,
                    reason: "must-activate append requires the held predecessor pause".into(),
                });
            };
            if paused.barrier_offset != pending.barrier_offset {
                return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                    range_id: pending.predecessor,
                    reason: "must-activate pause barrier differs from staged evidence".into(),
                });
            }
            paused.activation_authorization()
        };
        source_resources
            .activation_committer
            .commit_activation_receipt_cas(
                &authorization,
                pending.barrier_offset,
                &tenant,
                &pending.operation_id,
                expected,
                value,
            )
            .await
            .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: pending.predecessor,
                reason: format!("persist must-activate receipt: {reason}"),
            })?
    } else {
        store
            .compare_and_swap(
                &pending.operation_id,
                Some(expected_receipt.revision),
                receipt,
            )
            .await
            .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: pending.predecessor,
                reason: format!("persist must-activate receipt through range zero: {reason}"),
            })?
    };
    if !committed {
        return Err(crabka_gres_ranges::RangeTransferError::Runtime {
            range_id: pending.predecessor,
            reason: "must-activate receipt CAS raced".into(),
        });
    }
    transfer.note_activation_irreversible(&pending.operation_id);
    transfer.activation_fault(
        TopologyActivationFault::AfterMustActivate,
        pending.predecessor,
    )?;
    Ok(())
}

pub(super) async fn copy_must_activate_before_bind(
    transfer: &LiveMultiRangeTransfer,
    pending: &PendingLiveTopology,
    canonical: Arc<crabka_gres_substrate::ProducerWalWriter>,
) -> Result<(), crabka_gres_ranges::RangeTransferError> {
    let receipt_engine = transfer
        .prepared
        .lock()
        .map_err(|_| range_pause_lock_error(RangeId::COORDINATOR))?
        .as_ref()
        .and_then(|prepared| prepared.engines.get(&RangeId::COORDINATOR))
        .map(SqlEngine::clone_handle)
        .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Runtime {
            range_id: RangeId::COORDINATOR,
            reason: "prepared range zero missing before canonical bind".into(),
        })?;
    let tenant = pending.left.recovery_config.tenant.to_string();
    let store = RangeZeroTopologyActivationStore::new(tenant.clone(), receipt_engine);
    let mut receipt = store
        .load(&pending.operation_id)
        .await
        .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
            range_id: RangeId::COORDINATOR,
            reason: format!("load replacement activation anchor: {reason}"),
        })?
        .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Runtime {
            range_id: RangeId::COORDINATOR,
            reason: "replacement activation anchor is missing".into(),
        })?;
    if receipt.phase == TopologyActivationPhase::MustActivate {
        return Ok(());
    }
    let expected_receipt = receipt.clone();
    receipt.revision = receipt.revision.checked_add(1).ok_or_else(|| {
        crabka_gres_ranges::RangeTransferError::Runtime {
            range_id: RangeId::COORDINATOR,
            reason: "replacement activation revision overflow".into(),
        }
    })?;
    receipt.phase = TopologyActivationPhase::MustActivate;
    receipt.source_checkpoint = Some(pending.source_checkpoint.clone());
    receipt.barrier_offset = Some(pending.barrier_offset);
    receipt.tail_sha256 = Some(pending.tail_sha256.clone());
    receipt
        .targets
        .get_mut(&pending.left_id)
        .expect("left target")
        .replay_journal_seq = Some(pending.left_replay_journal_seq);
    if let Some((right_id, replay_journal_seq, _)) = &pending.right {
        receipt
            .targets
            .get_mut(right_id)
            .expect("right target")
            .replay_journal_seq = Some(*replay_journal_seq);
    }
    let expected = serde_json::to_vec(&expected_receipt).map_err(|error| {
        crabka_gres_ranges::RangeTransferError::Runtime {
            range_id: RangeId::COORDINATOR,
            reason: format!("encode prior activation anchor: {error}"),
        }
    })?;
    let value = serde_json::to_vec(&receipt).map_err(|error| {
        crabka_gres_ranges::RangeTransferError::Runtime {
            range_id: RangeId::COORDINATOR,
            reason: format!("encode activation anchor: {error}"),
        }
    })?;
    if !pending
        .left
        .activation_committer
        .commit_activation_anchor_before_bind(
            canonical,
            &tenant,
            &pending.operation_id,
            expected,
            value,
        )
        .await
        .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
            range_id: RangeId::COORDINATOR,
            reason: format!("commit replacement activation anchor: {error}"),
        })?
    {
        return Err(crabka_gres_ranges::RangeTransferError::Runtime {
            range_id: RangeId::COORDINATOR,
            reason: "replacement activation anchor CAS raced".into(),
        });
    }
    Ok(())
}

/// Execute the post-MustActivate state machine. `lib.rs` deliberately delegates the complete
/// protocol here so producer ownership and receipt transitions have one implementation home.
pub(super) async fn activate_serving_topology(
    transfer: &LiveMultiRangeTransfer,
) -> Result<(), crabka_gres_ranges::RangeTransferError> {
    let pending = transfer
        .pending
        .lock()
        .map_err(|_| range_pause_lock_error(RangeId::COORDINATOR))?
        .clone()
        .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Runtime {
            range_id: RangeId::COORDINATOR,
            reason: "activate without pending topology".into(),
        })?;
    let mut targets = vec![(pending.left_id, pending.left.clone())];
    if let Some((right_id, _, right)) = &pending.right {
        targets.push((*right_id, right.clone()));
    }
    for (index, (range_id, resources)) in targets.into_iter().enumerate() {
        if !resources.writer.is_activated() {
            transfer.activation_fault(TopologyActivationFault::BeforeProducerInit, range_id)?;
            let recovered = crabka_gres_substrate::recover_live_for_range_with_restore(
                resources.recovery_config.clone(),
                resources.store.as_ref(),
            )
            .await
            .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id,
                reason: format!("activate canonical successor writer: {error}"),
            })?;
            if recovered.generation != resources.generation {
                return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                    range_id,
                    reason: format!(
                        "activated generation {} differs from staged generation {}",
                        recovered.generation.0, resources.generation.0
                    ),
                });
            }
            transfer.activation_fault(TopologyActivationFault::AfterProducerInit, range_id)?;
            let canonical = Arc::new(crabka_gres_substrate::ProducerWalWriter::new(
                recovered.producer,
                resources.recovery_config.wal_topic(),
            ));
            if range_id == RangeId::COORDINATOR {
                copy_must_activate_before_bind(transfer, &pending, Arc::clone(&canonical)).await?;
            }
            transfer.activation_fault(TopologyActivationFault::BeforeDeferredBind, range_id)?;
            resources.writer.activate(canonical).map_err(|error| {
                crabka_gres_ranges::RangeTransferError::Runtime {
                    range_id,
                    reason: format!("bind canonical successor writer: {error}"),
                }
            })?;
            transfer.activation_fault(TopologyActivationFault::AfterDeferredBind, range_id)?;
        }

        let store = prepared_receipt_store(transfer, &pending, range_id)?;
        let mut receipt = load_transfer_receipt(&store, &pending.operation_id, range_id).await?;
        if receipt.barrier_offset != Some(pending.barrier_offset)
            || receipt.tail_sha256.as_deref() != Some(pending.tail_sha256.as_str())
        {
            return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                range_id,
                reason: "staged activation boundary differs from durable receipt".into(),
            });
        }
        if !receipt.targets[&range_id].writer_activated {
            let expected = receipt.revision;
            receipt.revision = receipt.revision.checked_add(1).ok_or_else(|| {
                crabka_gres_ranges::RangeTransferError::Runtime {
                    range_id,
                    reason: "activation receipt revision overflow".into(),
                }
            })?;
            receipt.phase = TopologyActivationPhase::WriterActivated;
            receipt
                .targets
                .get_mut(&range_id)
                .expect("target")
                .writer_activated = true;
            cas_transfer_receipt(
                &store,
                &pending.operation_id,
                expected,
                receipt,
                range_id,
                "writer activation",
            )
            .await?;
            transfer.activation_fault(
                if index == 0 {
                    TopologyActivationFault::FirstWriterActivated
                } else {
                    TopologyActivationFault::SecondWriterActivated
                },
                range_id,
            )?;
        }

        let mut receipt = load_transfer_receipt(&store, &pending.operation_id, range_id).await?;
        if receipt.targets[&range_id].bootstrap_checkpoint.is_none() {
            let checkpoint = resources.checkpoint.as_ref().ok_or_else(|| {
                crabka_gres_ranges::RangeTransferError::Unavailable {
                    range_id,
                    reason: "activated successor checkpoint runtime missing".into(),
                }
            })?;
            let run = checkpoint
                .handle
                .checkpoint_from_source(
                    Arc::clone(&resources.snapshot_source),
                    crabka_gres_substrate::CheckpointTrigger::Manual,
                )
                .await
                .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
                    range_id,
                    reason: format!("write activated successor checkpoint: {error}"),
                })?;
            let expected = receipt.revision;
            receipt.revision = receipt.revision.checked_add(1).ok_or_else(|| {
                crabka_gres_ranges::RangeTransferError::Runtime {
                    range_id,
                    reason: "activation receipt revision overflow".into(),
                }
            })?;
            receipt
                .targets
                .get_mut(&range_id)
                .expect("target")
                .bootstrap_checkpoint = Some(crabka_gres_ranges::CheckpointManifest {
                range_id,
                covered_offset: run.metadata.covered_offset,
                manifest_key: run.metadata.manifest_key,
            });
            cas_transfer_receipt(
                &store,
                &pending.operation_id,
                expected,
                receipt,
                range_id,
                "successor checkpoint",
            )
            .await?;
            transfer.activation_fault(
                if index == 0 {
                    TopologyActivationFault::FirstCheckpointDurable
                } else {
                    TopologyActivationFault::SecondCheckpointDurable
                },
                range_id,
            )?;
        }
    }

    let store = prepared_receipt_store(transfer, &pending, pending.predecessor)?;
    let mut receipt =
        load_transfer_receipt(&store, &pending.operation_id, pending.predecessor).await?;
    if receipt.phase != TopologyActivationPhase::CheckpointDurable {
        if !receipt
            .targets
            .values()
            .all(|target| target.writer_activated && target.bootstrap_checkpoint.is_some())
        {
            return Err(crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: pending.predecessor,
                reason: "successor checkpoint phase advanced before every target".into(),
            });
        }
        let expected = receipt.revision;
        receipt.revision = receipt.revision.checked_add(1).ok_or_else(|| {
            crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: pending.predecessor,
                reason: "activation receipt revision overflow".into(),
            }
        })?;
        receipt.phase = TopologyActivationPhase::CheckpointDurable;
        cas_transfer_receipt(
            &store,
            &pending.operation_id,
            expected,
            receipt,
            pending.predecessor,
            "durable checkpoint phase",
        )
        .await?;
        transfer.activation_fault(
            TopologyActivationFault::CheckpointDurable,
            pending.predecessor,
        )?;
    }
    Ok(())
}

fn prepared_receipt_store(
    transfer: &LiveMultiRangeTransfer,
    pending: &PendingLiveTopology,
    range_id: RangeId,
) -> Result<RangeZeroTopologyActivationStore, crabka_gres_ranges::RangeTransferError> {
    let engine = transfer
        .prepared
        .lock()
        .map_err(|_| range_pause_lock_error(range_id))?
        .as_ref()
        .and_then(|prepared| prepared.engines.get(&RangeId::COORDINATOR))
        .map(SqlEngine::clone_handle)
        .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Runtime {
            range_id,
            reason: "prepared range-zero engine missing during activation".into(),
        })?;
    Ok(RangeZeroTopologyActivationStore::new(
        pending.left.recovery_config.tenant.to_string(),
        engine,
    ))
}

async fn load_transfer_receipt(
    store: &RangeZeroTopologyActivationStore,
    operation_id: &str,
    range_id: RangeId,
) -> Result<TopologyActivationReceipt, crabka_gres_ranges::RangeTransferError> {
    store
        .load(operation_id)
        .await
        .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
            range_id,
            reason: format!("load activation receipt: {reason}"),
        })?
        .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Runtime {
            range_id,
            reason: "activation receipt is missing".into(),
        })
}

async fn cas_transfer_receipt(
    store: &RangeZeroTopologyActivationStore,
    operation_id: &str,
    expected: u64,
    receipt: TopologyActivationReceipt,
    range_id: RangeId,
    transition: &str,
) -> Result<(), crabka_gres_ranges::RangeTransferError> {
    if !store
        .compare_and_swap(operation_id, Some(expected), receipt)
        .await
        .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
            range_id,
            reason: format!("persist {transition}: {reason}"),
        })?
    {
        return Err(crabka_gres_ranges::RangeTransferError::Runtime {
            range_id,
            reason: format!("{transition} receipt CAS raced"),
        });
    }
    Ok(())
}

/// Read range zero without constructing a producer so startup can select the already
/// activated writer generation instead of attempting to resurrect a fenced predecessor.
pub(super) async fn discover_activation_receipt(
    config: &SubstrateRuntimeConfig,
    checkpoint_store: Option<&dyn crabka_gres_substrate::checkpoint::CheckpointStore>,
) -> std::io::Result<Option<ActivationDiscovery>> {
    let tenant = crabka_gres_ranges::TenantName::parse(config.tenant.clone()).map_err(|error| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("tenant: {error}"))
    })?;
    let recovery = config.live_recovery_config(tenant, RangeId::COORDINATOR);
    crabka_gres_substrate::ensure_live_wal_topic(&recovery)
        .await
        .map_err(|error| std::io::Error::other(format!("activation discovery topic: {error}")))?;
    if crabka_gres_substrate::live_committed_end(&recovery)
        .await
        .map_err(|error| std::io::Error::other(format!("activation discovery end: {error}")))?
        < 0
    {
        return Ok(None);
    }
    let mut receipts = BTreeMap::new();
    for receipt in read_only_receipts(&recovery, checkpoint_store).await? {
        validate_receipt_shape(&receipt)?;
        if receipts
            .insert(receipt.operation_id.clone(), receipt)
            .is_some()
        {
            return Err(std::io::Error::other(
                "duplicate activation operation id in range-zero state",
            ));
        }
    }
    let mut generation = 0;
    let mut recovery_map = receipts
        .values()
        .min_by_key(|receipt| receipt.split.current_map.epoch())
        .map(|receipt| receipt.split.current_map.clone());
    let mut recovery_generations = recovery_map
        .as_ref()
        .map(|map| {
            map.ranges()
                .iter()
                .map(|range| (range.range_id, 0))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let mut visited = std::collections::BTreeSet::from([generation]);
    while let Some(next_generation) = next_replacement_generation(generation, receipts.values())? {
        let edge_operation = receipts
            .values()
            .find(|receipt| {
                rolls_forward(receipt.phase)
                    && receipt
                        .targets
                        .get(&RangeId::COORDINATOR)
                        .is_some_and(|target| target.wal_generation == next_generation)
            })
            .map(|receipt| receipt.operation_id.clone())
            .expect("replacement edge selected from receipt set");
        if next_generation <= generation || !visited.insert(next_generation) {
            return Err(std::io::Error::other(
                "activation receipt graph contains a generation cycle or regression",
            ));
        }
        let candidate_recovery = recovery.clone().with_wal_generation(next_generation);
        let candidate_end = crabka_gres_substrate::live_committed_end(&candidate_recovery)
            .await
            .map_err(|error| {
                std::io::Error::other(format!(
                    "read replacement range-zero generation {next_generation}: {error}"
                ))
            })?;
        if candidate_end < 0 {
            // MustActivate deliberately precedes construction of this generation's producer.
            // An empty target WAL is therefore the terminal pre-init crash state: return the
            // predecessor receipt so recovery can construct and activate the successor.
            break;
        }
        let checkpoint_receipts =
            read_checkpoint_receipts(&candidate_recovery, checkpoint_store).await?;
        let mut histories = BTreeMap::<String, Vec<TopologyActivationReceipt>>::new();
        for candidate in receipt_values_from_wal(&candidate_recovery, candidate_end).await? {
            histories
                .entry(candidate.operation_id.clone())
                .or_default()
                .push(candidate);
        }
        if histories.is_empty() && checkpoint_receipts.is_empty() {
            // Canonical producer initialization may create the target topic before the
            // predecessor MustActivate value has been copied into it. The predecessor marker
            // remains sufficient roll-forward authority for this exact crash window.
            break;
        }
        for operation_id in checkpoint_receipts.keys() {
            histories.entry(operation_id.clone()).or_default();
        }
        for (operation_id, mut history) in histories {
            history = canonicalize_receipt_history(&operation_id, history)?;
            let mut chain = Vec::with_capacity(history.len() + 1);
            let mut compacted_edge = None;
            if let Some(prefix) = receipts.get(&operation_id) {
                chain.push(prefix.clone());
                if let Some(checkpoint) = checkpoint_receipts.get(&operation_id)
                    && checkpoint.revision > prefix.revision
                {
                    validate_compacted_receipt_extension(checkpoint, prefix)?;
                    compacted_edge = Some((prefix.revision, checkpoint.revision));
                    chain.push(checkpoint.clone());
                }
            } else if let Some(checkpoint) = checkpoint_receipts.get(&operation_id) {
                validate_receipt_shape(checkpoint)?;
                chain.push(checkpoint.clone());
            } else {
                if history.first().is_none_or(|receipt| {
                    receipt.revision != 0 || receipt.phase != TopologyActivationPhase::Prepared
                }) {
                    return Err(std::io::Error::other(format!(
                        "activation operation {operation_id} does not begin with revision-zero Prepared"
                    )));
                }
                chain.push(history.remove(0));
            }
            for candidate in history {
                let anchor = chain.last().expect("activation history has an anchor");
                if candidate.revision < anchor.revision {
                    continue;
                }
                if candidate.revision == anchor.revision {
                    if candidate != *anchor {
                        return Err(std::io::Error::other(format!(
                            "activation operation {operation_id} conflicts at revision {}",
                            candidate.revision
                        )));
                    }
                    continue;
                }
                chain.push(candidate);
            }
            for receipt in &chain {
                validate_receipt_shape(receipt)?;
            }
            for pair in chain.windows(2) {
                if compacted_edge == Some((pair[0].revision, pair[1].revision)) {
                    validate_compacted_receipt_extension(&pair[1], &pair[0])?;
                } else {
                    validate_receipt_history(pair)?;
                    validate_receipt_extension(&pair[1], &pair[0])?;
                }
            }
            let terminal = chain
                .pop()
                .expect("validated activation history is non-empty");
            receipts.insert(operation_id, terminal);
        }
        if let Some(completed) = receipts.get(&edge_operation)
            && matches!(
                completed.phase,
                TopologyActivationPhase::CheckpointDurable
                    | TopologyActivationPhase::TopologyCommitted
            )
        {
            recovery_generations.remove(&completed.split.predecessor);
            for (range_id, target) in &completed.targets {
                recovery_generations.insert(*range_id, target.wal_generation);
            }
            recovery_map = Some(completed.split.target_map.clone());
        }
        generation = next_generation;
    }
    let receipts = receipts.into_values().collect::<Vec<_>>();
    let latest = receipts
        .into_iter()
        .filter(|receipt| {
            matches!(
                receipt.phase,
                TopologyActivationPhase::MustActivate
                    | TopologyActivationPhase::WriterActivated
                    | TopologyActivationPhase::CheckpointDurable
                    | TopologyActivationPhase::TopologyCommitted
            )
        })
        .max_by_key(|receipt| receipt.split.target_map.epoch());
    Ok(latest.map(|receipt| ActivationDiscovery {
        recovery_map: recovery_map.unwrap_or_else(|| receipt.split.current_map.clone()),
        receipt,
        recovery_generations,
    }))
}

async fn read_checkpoint_receipts(
    recovery: &crabka_gres_substrate::LiveRecoveryConfig,
    checkpoint_store: Option<&dyn crabka_gres_substrate::checkpoint::CheckpointStore>,
) -> std::io::Result<BTreeMap<String, TopologyActivationReceipt>> {
    let Some(checkpoint_store) = checkpoint_store else {
        return Ok(BTreeMap::new());
    };
    let kv = Arc::new(crabka_pgkv::MemKv::default());
    let restored = crabka_gres_substrate::checkpoint::restore_latest(
        checkpoint_store,
        &format!("{}/r{}", recovery.tenant, RangeId::COORDINATOR.as_u32()),
        kv.as_ref(),
        recovery.wal_generation,
        None,
    )
    .await
    .map_err(|error| std::io::Error::other(format!("restore activation checkpoint: {error}")))?;
    if restored.is_none() {
        return Ok(BTreeMap::new());
    }
    let engine = SqlEngine::with_kv(kv as Arc<dyn crabka_pgkv::Kv>).map_err(|error| {
        std::io::Error::other(format!("activation checkpoint engine: {error:?}"))
    })?;
    let mut receipts = BTreeMap::new();
    for receipt in RangeZeroTopologyActivationStore::new(recovery.tenant.to_string(), engine)
        .list()
        .await
        .map_err(|error| std::io::Error::other(format!("list checkpoint receipts: {error}")))?
    {
        validate_receipt_shape(&receipt)?;
        if receipts
            .insert(receipt.operation_id.clone(), receipt)
            .is_some()
        {
            return Err(std::io::Error::other(
                "checkpoint contains duplicate activation operation ids",
            ));
        }
    }
    Ok(receipts)
}

async fn receipt_values_from_wal(
    recovery: &crabka_gres_substrate::LiveRecoveryConfig,
    end: i64,
) -> std::io::Result<Vec<TopologyActivationReceipt>> {
    let prefix = crabka_pgkv::key::topology_activation_receipt_prefix(&recovery.tenant.to_string());
    let items = crabka_gres_substrate::read_live_retained_committed(recovery, end)
        .await
        .map_err(|error| std::io::Error::other(format!("read replacement receipt WAL: {error}")))?;
    let mut receipts = Vec::new();
    for item in items {
        let frame = crabka_gres_substrate::WalFrame::decode(&item.bytes).map_err(|error| {
            std::io::Error::other(format!("decode replacement receipt WAL: {error}"))
        })?;
        for operation in frame.ops {
            let keyed_value = match operation {
                crabka_pgkv::WriteOp::Put {
                    key: candidate,
                    value,
                }
                | crabka_pgkv::WriteOp::ConditionalPut {
                    key: candidate,
                    value,
                    ..
                } if candidate.starts_with(&prefix) => Some((candidate, value)),
                _ => None,
            };
            if let Some((key, value)) = keyed_value {
                let receipt: TopologyActivationReceipt =
                    serde_json::from_slice(&value).map_err(|error| {
                        std::io::Error::other(format!(
                            "decode replacement activation receipt: {error}"
                        ))
                    })?;
                validate_receipt_wal_identity(&recovery.tenant.to_string(), &key, &receipt)?;
                receipts.push(receipt);
            }
        }
    }
    Ok(receipts)
}

fn validate_receipt_wal_identity(
    recovery_tenant: &str,
    key: &[u8],
    receipt: &TopologyActivationReceipt,
) -> std::io::Result<()> {
    let expected_key =
        crabka_pgkv::key::topology_activation_receipt_key(recovery_tenant, &receipt.operation_id);
    if key == expected_key && receipt.tenant == recovery_tenant {
        Ok(())
    } else {
        Err(std::io::Error::other(
            "activation receipt WAL key and payload identity differ",
        ))
    }
}

fn same_target_intent(
    candidate: &TopologyActivationReceipt,
    prefix: &TopologyActivationReceipt,
) -> bool {
    candidate.targets.len() == prefix.targets.len()
        && candidate.targets.iter().all(|(range_id, target)| {
            prefix.targets.get(range_id).is_some_and(|original| {
                target.range_id == original.range_id
                    && target.wal_generation == original.wal_generation
                    && target.endpoint == original.endpoint
                    && target.interval == original.interval
            })
        })
}

fn validate_receipt_extension(
    candidate: &TopologyActivationReceipt,
    prefix: &TopologyActivationReceipt,
) -> std::io::Result<()> {
    let immutable_matches = candidate.operation_id == prefix.operation_id
        && candidate.tenant == prefix.tenant
        && candidate.split == prefix.split
        && same_target_intent(candidate, prefix)
        && candidate.revision >= prefix.revision;
    let boundary_is_monotone = prefix
        .source_checkpoint
        .as_ref()
        .is_none_or(|value| candidate.source_checkpoint.as_ref() == Some(value))
        && prefix
            .barrier_offset
            .is_none_or(|value| candidate.barrier_offset == Some(value))
        && prefix
            .tail_sha256
            .as_ref()
            .is_none_or(|value| candidate.tail_sha256.as_ref() == Some(value));
    let targets_are_monotone = prefix.targets.iter().all(|(range_id, prior)| {
        candidate.targets.get(range_id).is_some_and(|next| {
            (!prior.writer_activated || next.writer_activated)
                && prior
                    .replay_journal_seq
                    .is_none_or(|value| next.replay_journal_seq == Some(value))
                && prior
                    .bootstrap_checkpoint
                    .as_ref()
                    .is_none_or(|value| next.bootstrap_checkpoint.as_ref() == Some(value))
        })
    });
    if immutable_matches && boundary_is_monotone && targets_are_monotone {
        validate_receipt_history(&[prefix.clone(), candidate.clone()])
    } else {
        Err(std::io::Error::other(
            "replacement range-zero receipt does not monotonically extend its prepared intent",
        ))
    }
}

fn validate_compacted_receipt_extension(
    candidate: &TopologyActivationReceipt,
    prefix: &TopologyActivationReceipt,
) -> std::io::Result<()> {
    validate_receipt_shape(prefix)?;
    validate_receipt_shape(candidate)?;
    let immutable = candidate.operation_id == prefix.operation_id
        && candidate.tenant == prefix.tenant
        && candidate.split == prefix.split
        && same_target_intent(candidate, prefix);
    let boundary = prefix
        .source_checkpoint
        .as_ref()
        .is_none_or(|value| candidate.source_checkpoint.as_ref() == Some(value))
        && prefix
            .barrier_offset
            .is_none_or(|value| candidate.barrier_offset == Some(value))
        && prefix
            .tail_sha256
            .as_ref()
            .is_none_or(|value| candidate.tail_sha256.as_ref() == Some(value));
    let targets = prefix.targets.iter().all(|(range_id, prior)| {
        candidate.targets.get(range_id).is_some_and(|next| {
            (!prior.writer_activated || next.writer_activated)
                && prior
                    .replay_journal_seq
                    .is_none_or(|value| next.replay_journal_seq == Some(value))
                && prior
                    .bootstrap_checkpoint
                    .as_ref()
                    .is_none_or(|value| next.bootstrap_checkpoint.as_ref() == Some(value))
        })
    });
    let revision_delta = candidate.revision.checked_sub(prefix.revision);
    let phase_delta = phase_rank(prefix.phase)
        .zip(phase_rank(candidate.phase))
        .and_then(|(prior, next)| next.checked_sub(prior))
        .map(u64::from);
    if immutable
        && boundary
        && targets
        && revision_delta
            .zip(phase_delta)
            .is_some_and(|(revisions, phases)| revisions > 0 && revisions >= phases)
    {
        Ok(())
    } else {
        Err(std::io::Error::other(
            "checkpointed activation receipt is not a monotone extension",
        ))
    }
}

fn phase_rank(phase: TopologyActivationPhase) -> Option<u8> {
    match phase {
        TopologyActivationPhase::Prepared => Some(0),
        TopologyActivationPhase::SourceCheckpoint => Some(1),
        TopologyActivationPhase::MustActivate => Some(2),
        TopologyActivationPhase::WriterActivated => Some(3),
        TopologyActivationPhase::CheckpointDurable => Some(4),
        TopologyActivationPhase::TopologyCommitted => Some(5),
        TopologyActivationPhase::Aborted => None,
    }
}

fn rolls_forward(phase: TopologyActivationPhase) -> bool {
    matches!(
        phase,
        TopologyActivationPhase::MustActivate
            | TopologyActivationPhase::WriterActivated
            | TopologyActivationPhase::CheckpointDurable
            | TopologyActivationPhase::TopologyCommitted
    )
}

fn next_replacement_generation<'a>(
    current_generation: u64,
    receipts: impl IntoIterator<Item = &'a TopologyActivationReceipt>,
) -> std::io::Result<Option<u64>> {
    let mut edge = None;
    for receipt in receipts {
        validate_receipt_shape(receipt)?;
        if !rolls_forward(receipt.phase) {
            continue;
        }
        if receipt.split.predecessor != RangeId::COORDINATOR {
            continue;
        }
        let target = receipt.targets.get(&RangeId::COORDINATOR).ok_or_else(|| {
            std::io::Error::other("activation receipt has no replacement range zero")
        })?;
        if target.wal_generation <= current_generation {
            if receipt.split.predecessor_generation == current_generation {
                return Err(std::io::Error::other(
                    "activation receipt replacement generation does not increase",
                ));
            }
            continue;
        }
        if edge.is_some() {
            return Err(std::io::Error::other(
                "activation receipt graph forks from one range-zero generation",
            ));
        }
        edge = Some(target.wal_generation);
    }
    Ok(edge)
}

fn canonicalize_receipt_history(
    operation_id: &str,
    receipts: impl IntoIterator<Item = TopologyActivationReceipt>,
) -> std::io::Result<Vec<TopologyActivationReceipt>> {
    let mut canonical = BTreeMap::new();
    for receipt in receipts {
        if receipt.operation_id != operation_id {
            return Err(std::io::Error::other(
                "activation history contains another operation id",
            ));
        }
        if let Some(prior) = canonical.insert(receipt.revision, receipt.clone())
            && prior != receipt
        {
            return Err(std::io::Error::other(format!(
                "activation operation {operation_id} has divergent values at revision {}",
                receipt.revision
            )));
        }
    }
    Ok(canonical.into_values().collect())
}

fn validate_receipt_shape(receipt: &TopologyActivationReceipt) -> std::io::Result<()> {
    let source = receipt.source_checkpoint.is_some();
    let boundary = receipt.barrier_offset.is_some() && receipt.tail_sha256.is_some();
    let no_partial_boundary = receipt.barrier_offset.is_some() == receipt.tail_sha256.is_some();
    let no_seeds = receipt
        .targets
        .values()
        .all(|target| target.replay_journal_seq.is_none());
    let all_seeds = receipt
        .targets
        .values()
        .all(|target| target.replay_journal_seq.is_some());
    let any_writer = receipt
        .targets
        .values()
        .any(|target| target.writer_activated);
    let all_writers = receipt
        .targets
        .values()
        .all(|target| target.writer_activated);
    let any_checkpoint = receipt
        .targets
        .values()
        .any(|target| target.bootstrap_checkpoint.is_some());
    let all_checkpoints = receipt
        .targets
        .values()
        .all(|target| target.bootstrap_checkpoint.is_some());
    let expected_target_ids = std::iter::once(receipt.split.left.range_id)
        .chain(receipt.split.right.as_ref().map(|right| right.range_id))
        .collect::<std::collections::BTreeSet<_>>();
    let target_identity = receipt
        .targets
        .keys()
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        == expected_target_ids
        && receipt.targets.iter().all(|(range_id, target)| {
            let descriptor = if *range_id == receipt.split.left.range_id {
                Some(&receipt.split.left)
            } else {
                receipt
                    .split
                    .right
                    .as_ref()
                    .filter(|right| right.range_id == *range_id)
            };
            *range_id == target.range_id
                && descriptor.is_some_and(|descriptor| {
                    target.endpoint == descriptor.endpoint
                        && target.wal_generation == descriptor.wal_generation
                        && target.interval == descriptor.interval
                })
                && target
                    .bootstrap_checkpoint
                    .as_ref()
                    .is_none_or(|checkpoint| checkpoint.range_id == *range_id)
                && (target.bootstrap_checkpoint.is_none() || target.writer_activated)
        });
    let source_identity = receipt.source_checkpoint.as_ref().is_none_or(|checkpoint| {
        checkpoint.range_id == receipt.split.predecessor
            && receipt
                .barrier_offset
                .is_none_or(|barrier| barrier > checkpoint.covered_offset)
    });
    let receipt_identity = receipt.operation_id == receipt.split.operation_id
        && receipt.tenant == receipt.split.current_map.tenant().to_string()
        && receipt.tenant == receipt.split.target_map.tenant().to_string();
    let valid = no_partial_boundary
        && receipt_identity
        && target_identity
        && source_identity
        && match receipt.phase {
            TopologyActivationPhase::Prepared => {
                !source && !boundary && no_seeds && !any_writer && !any_checkpoint
            }
            TopologyActivationPhase::SourceCheckpoint => {
                source && !boundary && no_seeds && !any_writer && !any_checkpoint
            }
            TopologyActivationPhase::MustActivate => {
                source && boundary && all_seeds && !any_writer && !any_checkpoint
            }
            TopologyActivationPhase::WriterActivated => {
                source && boundary && all_seeds && any_writer
            }
            TopologyActivationPhase::CheckpointDurable
            | TopologyActivationPhase::TopologyCommitted => {
                source && boundary && all_seeds && all_writers && all_checkpoints
            }
            TopologyActivationPhase::Aborted => !any_writer && !any_checkpoint && !boundary,
        };
    if valid {
        Ok(())
    } else {
        Err(std::io::Error::other(
            "activation receipt phase is inconsistent with its durable fields",
        ))
    }
}

fn validate_receipt_history(receipts: &[TopologyActivationReceipt]) -> std::io::Result<()> {
    for receipt in receipts {
        validate_receipt_shape(receipt)?;
    }
    for pair in receipts.windows(2) {
        let prior = &pair[0];
        let next = &pair[1];
        if next.revision
            != prior
                .revision
                .checked_add(1)
                .ok_or_else(|| std::io::Error::other("activation receipt revision overflow"))?
        {
            return Err(std::io::Error::other(format!(
                "activation receipt revision is not contiguous: {} {:?} -> {} {:?}",
                prior.revision, prior.phase, next.revision, next.phase
            )));
        }
        let valid_phase = if next.phase == TopologyActivationPhase::Aborted {
            matches!(
                prior.phase,
                TopologyActivationPhase::Prepared | TopologyActivationPhase::SourceCheckpoint
            )
        } else if prior.phase == next.phase {
            !matches!(
                prior.phase,
                TopologyActivationPhase::Prepared
                    | TopologyActivationPhase::SourceCheckpoint
                    | TopologyActivationPhase::MustActivate
                    | TopologyActivationPhase::CheckpointDurable
                    | TopologyActivationPhase::TopologyCommitted
                    | TopologyActivationPhase::Aborted
            )
        } else {
            phase_rank(prior.phase)
                .zip(phase_rank(next.phase))
                .is_some_and(|(prior, next)| next == prior + 1)
        };
        if !valid_phase {
            return Err(std::io::Error::other(
                "activation receipt phase transition is not monotone",
            ));
        }
    }
    Ok(())
}

async fn read_only_receipts(
    recovery: &crabka_gres_substrate::LiveRecoveryConfig,
    checkpoint_store: Option<&dyn crabka_gres_substrate::checkpoint::CheckpointStore>,
) -> std::io::Result<Vec<TopologyActivationReceipt>> {
    let follower_kv = Arc::new(crabka_pgkv::MemKv::default());
    crabka_gres_substrate::bootstrap_live_range0_follower(
        recovery,
        follower_kv.clone(),
        checkpoint_store,
    )
    .await
    .map_err(|error| std::io::Error::other(format!("activation discovery: {error}")))?;
    let engine = SqlEngine::with_kv(follower_kv as Arc<dyn crabka_pgkv::Kv>).map_err(|error| {
        std::io::Error::other(format!("activation discovery engine: {error:?}"))
    })?;
    RangeZeroTopologyActivationStore::new(recovery.tenant.to_string(), engine)
        .list()
        .await
        .map_err(|error| std::io::Error::other(format!("discover activation receipts: {error}")))
}

/// Resolve every durable activation receipt before a listener can advertise readiness.
///
/// Before writer activation, aborting is safe.  Once either successor writer has been
/// activated, the only safe direction is forward: reconstruct both successors from the
/// recorded source checkpoint and bounded tail, recover their canonical WALs, checkpoint
/// them, and publish the target map entirely in the not-yet-visible startup graph.
pub(super) async fn reconcile_before_readiness(
    config: &SubstrateRuntimeConfig,
    engines: &mut LiveMultirangeEngines,
    checkpoint_store: Option<Arc<dyn crabka_gres_substrate::checkpoint::CheckpointStore>>,
    discovered: Option<ActivationDiscovery>,
) -> std::io::Result<(Option<RangeMap>, bool)> {
    if discovered.is_none() && !engines.engines.contains_key(&RangeId::COORDINATOR) {
        return Ok((None, false));
    }
    let range0 = engines.engines.get(&RangeId::COORDINATOR).ok_or_else(|| {
        std::io::Error::other("range zero missing during activation reconciliation")
    })?;
    if let Some(discovered) = discovered.as_ref()
        && discovered.receipt.split.predecessor != RangeId::COORDINATOR
        && discovered.recovery_map == discovered.receipt.split.target_map
        && !matches!(
            discovered.receipt.phase,
            TopologyActivationPhase::CheckpointDurable | TopologyActivationPhase::TopologyCommitted
        )
    {
        if !topology_is_recovered(engines, &discovered.receipt) {
            return Err(std::io::Error::other(
                "authoritative target journal names an unrecovered activation target",
            ));
        }
        return Ok((Some(discovered.recovery_map.clone()), false));
    }
    let tenant = range0.resources.recovery_config.tenant.to_string();
    let source_store =
        RangeZeroTopologyActivationStore::new(tenant.clone(), range0.engine.clone_handle());
    let control_recovery_operations = range0
        .engine
        .range_control_receipts(&tenant)
        .map_err(|error| std::io::Error::other(format!("list range-control receipts: {error:?}")))?
        .into_iter()
        .map(|bytes| {
            serde_json::from_slice::<crabka_gres_ranges::control::RangeControlReceipt>(&bytes)
                .map(|receipt| receipt.request.operation_id)
                .map_err(|error| {
                    std::io::Error::other(format!("decode range-control receipt: {error}"))
                })
        })
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    let mut receipts = source_store
        .list()
        .await
        .map_err(|error| std::io::Error::other(format!("list activation receipts: {error}")))?;
    if let Some(discovered) = discovered.map(|discovery| discovery.receipt) {
        if let Some(existing) = receipts
            .iter_mut()
            .find(|receipt| receipt.operation_id == discovered.operation_id)
        {
            if *existing != discovered {
                validate_compacted_receipt_extension(&discovered, existing)?;
                *existing = discovered;
            }
        } else {
            receipts.push(discovered);
        }
    }
    receipts.sort_by_key(|receipt| receipt.split.target_map.epoch());
    let defer_timestamp_recovery = receipts.iter().any(|receipt| {
        should_defer_timestamp_recovery(
            receipt.phase,
            control_recovery_operations.contains(&receipt.operation_id),
        )
    });

    let mut activation = None;
    for receipt in receipts {
        match receipt.phase {
            TopologyActivationPhase::Aborted => {}
            TopologyActivationPhase::SourceCheckpoint
                if control_recovery_operations.contains(&receipt.operation_id) => {}
            TopologyActivationPhase::Prepared | TopologyActivationPhase::SourceCheckpoint => {
                abort_pre_activation(&source_store, receipt).await?;
            }
            TopologyActivationPhase::MustActivate
            | TopologyActivationPhase::WriterActivated
            | TopologyActivationPhase::CheckpointDurable
            | TopologyActivationPhase::TopologyCommitted => {
                activation = Some(receipt);
            }
        }
    }
    let Some(receipt) = activation else {
        return Ok((None, defer_timestamp_recovery));
    };
    if topology_is_recovered(engines, &receipt) {
        return Ok((Some(receipt.split.target_map), false));
    }
    complete_post_activation(config, engines, checkpoint_store, receipt)
        .await
        .map(|map| (Some(map), false))
}

const fn should_defer_timestamp_recovery(
    phase: TopologyActivationPhase,
    has_active_control_pause: bool,
) -> bool {
    matches!(phase, TopologyActivationPhase::SourceCheckpoint) && has_active_control_pause
}

fn topology_is_recovered(
    engines: &LiveMultirangeEngines,
    receipt: &TopologyActivationReceipt,
) -> bool {
    receipt.targets.iter().all(|(range_id, target)| {
        engines
            .engines
            .get(range_id)
            .is_some_and(|engine| engine.resources.generation.0 == target.wal_generation)
    }) && (receipt.targets.contains_key(&receipt.split.predecessor)
        || !engines.engines.contains_key(&receipt.split.predecessor))
}

async fn abort_pre_activation(
    store: &RangeZeroTopologyActivationStore,
    mut receipt: TopologyActivationReceipt,
) -> std::io::Result<()> {
    let operation_id = receipt.operation_id.clone();
    let expected = receipt.revision;
    receipt.revision = receipt.revision.saturating_add(1);
    receipt.phase = TopologyActivationPhase::Aborted;
    if !store
        .compare_and_swap(&operation_id, Some(expected), receipt)
        .await
        .map_err(|error| std::io::Error::other(format!("abort prepared activation: {error}")))?
    {
        return Err(std::io::Error::other(
            "prepared activation receipt changed during startup",
        ));
    }
    Ok(())
}

async fn complete_post_activation(
    config: &SubstrateRuntimeConfig,
    engines: &mut LiveMultirangeEngines,
    checkpoint_store: Option<Arc<dyn crabka_gres_substrate::checkpoint::CheckpointStore>>,
    receipt: TopologyActivationReceipt,
) -> std::io::Result<RangeMap> {
    let checkpoint = receipt.source_checkpoint.clone().ok_or_else(|| {
        std::io::Error::other("activated topology receipt is missing its source checkpoint")
    })?;
    let barrier_offset = receipt.barrier_offset.ok_or_else(|| {
        std::io::Error::other("activated topology receipt is missing its barrier offset")
    })?;
    let expected_tail_sha = receipt.tail_sha256.as_deref().ok_or_else(|| {
        std::io::Error::other("activated topology receipt is missing its bounded-tail digest")
    })?;
    if checkpoint.range_id != receipt.split.predecessor
        || barrier_offset <= checkpoint.covered_offset
    {
        return Err(std::io::Error::other(
            "activated topology receipt has an invalid checkpoint boundary",
        ));
    }
    let predecessor = engines
        .engines
        .get(&receipt.split.predecessor)
        .ok_or_else(|| std::io::Error::other("activation predecessor is not recovered"))?;
    let source_checkpoint_runtime =
        predecessor.resources.checkpoint.as_ref().ok_or_else(|| {
            std::io::Error::other("activation predecessor has no checkpoint store")
        })?;
    let tail = crabka_gres_substrate::read_live_committed_tail(
        &predecessor.resources.recovery_config,
        checkpoint.covered_offset,
        barrier_offset,
    )
    .await
    .map_err(|error| std::io::Error::other(format!("read activation tail: {error}")))?
    .into_iter()
    .map(|record| crabka_gres_ranges::CommittedTailRecord {
        offset: record.offset,
        bytes: record.bytes,
    })
    .collect::<Vec<_>>();
    if committed_tail_sha256(&tail) != expected_tail_sha {
        return Err(std::io::Error::other(
            "activation bounded-tail digest differs from its durable receipt",
        ));
    }
    let physical_to_logical = physical_to_logical(engines, &receipt)?;

    let mut successors = BTreeMap::new();
    for target in receipt.targets.values() {
        let cache_nonce = RECOVERY_CACHE_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let cache = config.cache_dir.as_ref().map(|base| {
            base.join(format!(
                "activation-recovery-{}-r{}-g{}-p{}-n{}",
                receipt.operation_id,
                target.range_id.as_u32(),
                target.wal_generation,
                std::process::id(),
                cache_nonce,
            ))
        });
        let target_store = open_substrate_range_cache(cache.as_deref(), target.range_id)?;
        let mut recovery = config
            .live_recovery_config(
                predecessor.resources.recovery_config.tenant.clone(),
                target.range_id,
            )
            .with_wal_generation(target.wal_generation)
            .with_optional_advertised_endpoint(config.advertised_endpoint.clone());
        let recovery_checkpoints = if target.bootstrap_checkpoint.is_some() {
            checkpoint_store.clone()
        } else {
            let filter = crabka_gres_substrate::CheckpointFilter::new(
                target.interval.start,
                target.interval.end,
            )
            .map_err(|error| std::io::Error::other(format!("successor interval: {error}")))?
            .with_physical_to_logical(physical_to_logical.clone())
            .with_structural_ownership(target.range_id == receipt.split.left.range_id)
            .with_target_range(target.range_id);
            let restored = crabka_gres_substrate::restore_filtered_from_manifest_and_replay_tail(
                source_checkpoint_runtime.store.as_ref(),
                &checkpoint.manifest_key,
                &source_checkpoint_runtime.tenant,
                checkpoint.covered_offset,
                target_store.as_ref(),
                crabka_gres_substrate::RestoreTail {
                    current_generation: receipt.split.predecessor_generation,
                    log_start: None,
                    committed_frames: tail
                        .iter()
                        .map(|record| crabka_gres_substrate::ReplayItem {
                            offset: record.offset,
                            bytes: record.bytes.clone(),
                        })
                        .collect(),
                    barrier_offset,
                },
                filter,
            )
            .await
            .map_err(|error| {
                std::io::Error::other(format!(
                    "reconstruct successor r{}: {error}",
                    target.range_id.as_u32()
                ))
            })?;
            let replay_journal_seq = target.replay_journal_seq.ok_or_else(|| {
                std::io::Error::other(format!(
                    "successor r{} receipt is missing its replay journal seed",
                    target.range_id.as_u32()
                ))
            })?;
            if replay_journal_seq != restored.replay.next_journal_seq {
                return Err(std::io::Error::other(format!(
                    "successor r{} replay seed {} differs from reconstructed {}",
                    target.range_id.as_u32(),
                    replay_journal_seq,
                    restored.replay.next_journal_seq
                )));
            }
            recovery = recovery.with_replay_seed(0, replay_journal_seq);
            None
        };
        let recovered =
            open_live_range_substrate_engine(config, recovery, target_store, recovery_checkpoints)
                .await?;
        if recovered.resources.generation.0 != target.wal_generation {
            return Err(std::io::Error::other(format!(
                "successor r{} recovered generation {} instead of {}",
                target.range_id.as_u32(),
                recovered.resources.generation.0,
                target.wal_generation
            )));
        }
        successors.insert(target.range_id, recovered);
    }

    let receipt_range0 = successors
        .get(&RangeId::COORDINATOR)
        .map(|recovered| recovered.engine.clone_handle())
        .or_else(|| {
            engines
                .engines
                .get(&RangeId::COORDINATOR)
                .map(|engine| engine.engine.clone_handle())
        })
        .ok_or_else(|| {
            std::io::Error::other("recovered topology has no retained or replacement range zero")
        })?;
    let store = RangeZeroTopologyActivationStore::new(receipt.tenant.clone(), receipt_range0);
    ensure_must_activate_receipt(&store, &receipt).await?;
    complete_target_receipts(&store, &receipt.operation_id, &successors).await?;

    engines.engines.remove(&receipt.split.predecessor);
    engines.engines.extend(successors);
    engines.range0_tso_horizon = engines
        .engines
        .get(&RangeId::COORDINATOR)
        .and_then(|engine| engine.tso_horizon.clone());
    mark_topology_committed(&store, &receipt.operation_id).await?;
    Ok(receipt.split.target_map)
}

async fn ensure_must_activate_receipt(
    store: &RangeZeroTopologyActivationStore,
    predecessor_receipt: &TopologyActivationReceipt,
) -> std::io::Result<()> {
    let current = load_receipt(store, &predecessor_receipt.operation_id).await?;
    if current.revision >= predecessor_receipt.revision {
        validate_receipt_shape(&current)?;
        return Ok(());
    }
    if current.phase != TopologyActivationPhase::SourceCheckpoint
        || predecessor_receipt.phase != TopologyActivationPhase::MustActivate
    {
        return Err(std::io::Error::other(
            "replacement range zero is missing the MustActivate predecessor transition",
        ));
    }
    validate_receipt_extension(predecessor_receipt, &current)?;
    if !store
        .compare_and_swap(
            &predecessor_receipt.operation_id,
            Some(current.revision),
            predecessor_receipt.clone(),
        )
        .await
        .map_err(|error| {
            std::io::Error::other(format!(
                "copy MustActivate receipt to replacement r0: {error}"
            ))
        })?
    {
        return Err(std::io::Error::other(
            "replacement MustActivate receipt CAS raced",
        ));
    }
    Ok(())
}

async fn complete_target_receipts(
    store: &RangeZeroTopologyActivationStore,
    operation_id: &str,
    successors: &BTreeMap<RangeId, LiveRangeEngine>,
) -> std::io::Result<()> {
    for (range_id, successor) in successors {
        let mut receipt = load_receipt(store, operation_id).await?;
        let target = receipt.targets.get(range_id).ok_or_else(|| {
            std::io::Error::other("recovered successor is absent from activation receipt")
        })?;
        if !target.writer_activated {
            let expected = receipt.revision;
            receipt.revision = receipt.revision.saturating_add(1);
            receipt.phase = TopologyActivationPhase::WriterActivated;
            receipt
                .targets
                .get_mut(range_id)
                .expect("target existence checked")
                .writer_activated = true;
            cas_receipt(store, operation_id, expected, receipt, "writer activation").await?;
        }

        let mut receipt = load_receipt(store, operation_id).await?;
        if receipt.targets[range_id].bootstrap_checkpoint.is_none() {
            let checkpoint = successor.resources.checkpoint.as_ref().ok_or_else(|| {
                std::io::Error::other("recovered successor has no checkpoint runtime")
            })?;
            let run = checkpoint
                .handle
                .checkpoint_from_source(
                    Arc::clone(&successor.resources.snapshot_source),
                    crabka_gres_substrate::CheckpointTrigger::Manual,
                )
                .await
                .map_err(|error| {
                    std::io::Error::other(format!("checkpoint recovered successor: {error}"))
                })?;
            let expected = receipt.revision;
            receipt.revision = receipt.revision.saturating_add(1);
            receipt
                .targets
                .get_mut(range_id)
                .expect("target existence checked")
                .bootstrap_checkpoint = Some(crabka_gres_ranges::CheckpointManifest {
                range_id: *range_id,
                covered_offset: run.metadata.covered_offset,
                manifest_key: run.metadata.manifest_key,
            });
            cas_receipt(
                store,
                operation_id,
                expected,
                receipt,
                "bootstrap checkpoint",
            )
            .await?;
        }
    }
    let mut receipt = load_receipt(store, operation_id).await?;
    if !receipt
        .targets
        .values()
        .all(|target| target.bootstrap_checkpoint.is_some())
    {
        return Err(std::io::Error::other(
            "activation recovery did not durably checkpoint every successor",
        ));
    }
    if !matches!(
        receipt.phase,
        TopologyActivationPhase::CheckpointDurable | TopologyActivationPhase::TopologyCommitted
    ) {
        let expected = receipt.revision;
        receipt.revision = receipt.revision.saturating_add(1);
        receipt.phase = TopologyActivationPhase::CheckpointDurable;
        cas_receipt(
            store,
            operation_id,
            expected,
            receipt,
            "durable checkpoint phase",
        )
        .await?;
    }
    Ok(())
}

async fn mark_topology_committed(
    store: &RangeZeroTopologyActivationStore,
    operation_id: &str,
) -> std::io::Result<()> {
    let mut receipt = load_receipt(store, operation_id).await?;
    if receipt.phase == TopologyActivationPhase::TopologyCommitted {
        return Ok(());
    }
    if receipt.phase != TopologyActivationPhase::CheckpointDurable {
        return Err(std::io::Error::other(
            "topology commit attempted before durable successor checkpoints",
        ));
    }
    let expected = receipt.revision;
    receipt.revision = receipt.revision.saturating_add(1);
    receipt.phase = TopologyActivationPhase::TopologyCommitted;
    cas_receipt(store, operation_id, expected, receipt, "topology commit").await
}

async fn load_receipt(
    store: &RangeZeroTopologyActivationStore,
    operation_id: &str,
) -> std::io::Result<TopologyActivationReceipt> {
    store
        .load(operation_id)
        .await
        .map_err(|error| std::io::Error::other(format!("load activation receipt: {error}")))?
        .ok_or_else(|| std::io::Error::other("activation receipt disappeared during recovery"))
}

async fn cas_receipt(
    store: &RangeZeroTopologyActivationStore,
    operation_id: &str,
    expected: u64,
    receipt: TopologyActivationReceipt,
    transition: &str,
) -> std::io::Result<()> {
    if !store
        .compare_and_swap(operation_id, Some(expected), receipt)
        .await
        .map_err(|error| std::io::Error::other(format!("persist {transition} receipt: {error}")))?
    {
        return Err(std::io::Error::other(format!(
            "{transition} receipt changed during startup"
        )));
    }
    Ok(())
}

fn physical_to_logical(
    engines: &LiveMultirangeEngines,
    receipt: &TopologyActivationReceipt,
) -> std::io::Result<BTreeMap<TableId, TableId>> {
    let coordinator = engines.engines.get(&RangeId::COORDINATOR).ok_or_else(|| {
        std::io::Error::other("range zero missing while reconstructing activation mapping")
    })?;
    crabka_gres_ranges::transfer::predecessor_table_mapping(
        &receipt.split.current_map,
        receipt.split.predecessor,
        crabka_pgcatalog::list_tables(coordinator.engine.catalog_kv())
            .map_err(|error| std::io::Error::other(format!("list activation tables: {error:?}")))?
            .into_iter()
            .map(|table| {
                (
                    TableId::new(u64::from(table.id)),
                    routing_table_id(&table.name.name),
                )
            }),
    )
    .map_err(|error| std::io::Error::other(format!("activation table mapping: {error}")))
}

/// Routing id for a relation, read from the trailing digits of its name.
///
/// The split contract keys on the *unqualified* name: a schema qualifier
/// carries no digits, so `s.t42` and `public.t42` route identically. That is the
/// same collision the convention already accepts between any two names ending in
/// `42`.
fn routing_table_id(table: &str) -> TableId {
    let digits = table
        .chars()
        .rev()
        .take_while(char::is_ascii_digit)
        .collect::<Vec<_>>();
    if digits.is_empty() {
        return TableId::ZERO;
    }
    digits
        .into_iter()
        .rev()
        .collect::<String>()
        .parse::<u64>()
        .ok()
        .map_or(TableId::ZERO, TableId::new)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crabka_gres_ranges::{
        MapEpoch, MoveRangeCommand, RangeId, RangeKey, RangeMap, RangeSpec, SplitCommand,
        SplitState, SuccessorDescriptor, TableId, TenantName,
        control::{ActivationTargetProgress, TopologyActivationPhase, TopologyActivationReceipt},
    };

    use super::{
        ActivationDiscovery, canonicalize_receipt_history, next_replacement_generation,
        registry_boundary, registry_boundary_matches, routing_table_id,
        should_defer_timestamp_recovery, validate_receipt_extension, validate_receipt_history,
        validate_receipt_wal_identity,
    };

    #[test]
    fn routing_id_matches_split_catalog_contract() {
        assert_eq!(routing_table_id("accounts42"), TableId::new(42));
        assert_eq!(routing_table_id("accounts"), TableId::ZERO);
    }

    #[test]
    fn activation_registry_boundary_preserves_hash_bucket() {
        assert_eq!(
            registry_boundary(RangeKey::hash(TableId::new(50), 8, 0)),
            crabka_gres_control::RangeBoundary::hash(50, 8, 0)
        );
        assert_eq!(
            registry_boundary(RangeKey::new(TableId::new(51), 16)),
            crabka_gres_control::RangeBoundary::new(51, 16)
        );
        assert!(registry_boundary_matches(
            Some(crabka_gres_control::RangeBoundary::hash(50, 0, 0)),
            Some(RangeKey::hash(TableId::new(50), 0, 0))
        ));
    }

    #[test]
    fn timestamp_recovery_is_deferred_only_for_reversible_source_pause() {
        assert!(should_defer_timestamp_recovery(
            TopologyActivationPhase::SourceCheckpoint,
            true,
        ));
        assert!(!should_defer_timestamp_recovery(
            TopologyActivationPhase::SourceCheckpoint,
            false,
        ));
        assert!(!should_defer_timestamp_recovery(
            TopologyActivationPhase::MustActivate,
            true,
        ));
        assert!(!should_defer_timestamp_recovery(
            TopologyActivationPhase::TopologyCommitted,
            true,
        ));
    }

    #[test]
    fn forged_activation_receipt_extensions_fail_closed() {
        let mut prefix = receipt();
        prefix.phase = TopologyActivationPhase::SourceCheckpoint;
        prefix.source_checkpoint = Some(crabka_gres_ranges::CheckpointManifest {
            range_id: prefix.split.predecessor,
            covered_offset: 3,
            manifest_key: "checkpoint".into(),
        });
        let mut valid = prefix.clone();
        valid.revision += 1;
        valid.phase = TopologyActivationPhase::MustActivate;
        valid.barrier_offset = Some(7);
        valid.tail_sha256 = Some("tail".into());
        for target in valid.targets.values_mut() {
            target.replay_journal_seq = Some(11);
        }
        validate_receipt_extension(&valid, &prefix).expect("monotone extension");

        let mut forgeries = Vec::new();
        let mut forged = valid.clone();
        forged.tenant = "other".into();
        forgeries.push(forged);
        let mut forged = valid.clone();
        forged.operation_id = "other-op".into();
        forgeries.push(forged);
        let mut forged = valid.clone();
        forged.split.operation_id = "other-split".into();
        forgeries.push(forged);
        let mut forged = valid.clone();
        forged
            .targets
            .get_mut(&RangeId::COORDINATOR)
            .unwrap()
            .wal_generation += 1;
        forgeries.push(forged);
        let mut forged = valid.clone();
        forged.revision = 0;
        forgeries.push(forged);
        let mut boundary_prefix = valid.clone();
        boundary_prefix.revision += 1;
        let mut forged = boundary_prefix.clone();
        forged.revision += 1;
        forged.tail_sha256 = Some("forged".into());
        assert!(validate_receipt_extension(&forged, &boundary_prefix).is_err());

        for forged in forgeries {
            assert!(validate_receipt_extension(&forged, &prefix).is_err());
        }
    }

    #[test]
    fn receipt_history_rejects_phase_regression_jump_and_inconsistent_fields() {
        let mut prepared = receipt();
        validate_receipt_history(&[prepared.clone()]).expect("prepared receipt");

        let mut source = prepared.clone();
        source.revision += 1;
        source.phase = TopologyActivationPhase::SourceCheckpoint;
        source.source_checkpoint = Some(crabka_gres_ranges::CheckpointManifest {
            range_id: source.split.predecessor,
            covered_offset: 3,
            manifest_key: "checkpoint".into(),
        });
        validate_receipt_history(&[prepared.clone(), source.clone()])
            .expect("source-checkpoint transition");

        let mut must_activate = source.clone();
        must_activate.revision += 1;
        must_activate.phase = TopologyActivationPhase::MustActivate;
        must_activate.barrier_offset = Some(7);
        must_activate.tail_sha256 = Some("tail".into());
        for target in must_activate.targets.values_mut() {
            target.replay_journal_seq = Some(11);
        }
        validate_receipt_history(&[prepared.clone(), source.clone(), must_activate.clone()])
            .expect("must-activate transition");

        let mut regression = must_activate.clone();
        regression.revision += 1;
        regression.phase = TopologyActivationPhase::SourceCheckpoint;
        assert!(validate_receipt_history(&[must_activate.clone(), regression]).is_err());

        let mut jump = prepared.clone();
        jump.revision += 1;
        jump.phase = TopologyActivationPhase::WriterActivated;
        jump.source_checkpoint = source.source_checkpoint.clone();
        jump.barrier_offset = Some(7);
        jump.tail_sha256 = Some("tail".into());
        for target in jump.targets.values_mut() {
            target.replay_journal_seq = Some(11);
            target.writer_activated = true;
        }
        assert!(validate_receipt_history(&[prepared.clone(), jump]).is_err());

        prepared.barrier_offset = Some(7);
        assert!(validate_receipt_history(&[prepared]).is_err());

        let mut inconsistent = must_activate;
        inconsistent.tail_sha256 = None;
        assert!(validate_receipt_history(&[source, inconsistent]).is_err());

        let mut partial_seed = receipt();
        partial_seed
            .targets
            .get_mut(&RangeId::COORDINATOR)
            .unwrap()
            .replay_journal_seq = Some(1);
        assert!(validate_receipt_history(&[partial_seed]).is_err());

        let mut checkpoint_before_writer = receipt();
        checkpoint_before_writer.phase = TopologyActivationPhase::WriterActivated;
        checkpoint_before_writer.source_checkpoint = Some(crabka_gres_ranges::CheckpointManifest {
            range_id: checkpoint_before_writer.split.predecessor,
            covered_offset: 3,
            manifest_key: "source".into(),
        });
        checkpoint_before_writer.barrier_offset = Some(7);
        checkpoint_before_writer.tail_sha256 = Some("tail".into());
        for target in checkpoint_before_writer.targets.values_mut() {
            target.replay_journal_seq = Some(1);
        }
        checkpoint_before_writer
            .targets
            .get_mut(&RangeId::COORDINATOR)
            .unwrap()
            .writer_activated = true;
        checkpoint_before_writer
            .targets
            .get_mut(&RangeId::new(2))
            .unwrap()
            .bootstrap_checkpoint = Some(crabka_gres_ranges::CheckpointManifest {
            range_id: RangeId::new(2),
            covered_offset: 1,
            manifest_key: "impossible".into(),
        });
        assert!(validate_receipt_history(&[checkpoint_before_writer]).is_err());

        let mut mismatched_identity = receipt();
        mismatched_identity
            .targets
            .get_mut(&RangeId::new(2))
            .unwrap()
            .range_id = RangeId::new(9);
        assert!(validate_receipt_history(&[mismatched_identity]).is_err());

        let mut mismatched_operation = receipt();
        mismatched_operation.operation_id = "payload-op".into();
        assert!(validate_receipt_history(&[mismatched_operation]).is_err());
    }

    #[test]
    fn replacement_generation_selection_supports_distinct_operation_chain_and_rejects_forks() {
        let mut first = receipt();
        first.phase = TopologyActivationPhase::MustActivate;
        first.source_checkpoint = Some(crabka_gres_ranges::CheckpointManifest {
            range_id: first.split.predecessor,
            covered_offset: 3,
            manifest_key: "g0".into(),
        });
        first.barrier_offset = Some(7);
        first.tail_sha256 = Some("g0-tail".into());
        for target in first.targets.values_mut() {
            target.replay_journal_seq = Some(11);
        }
        assert_eq!(next_replacement_generation(0, [&first]).unwrap(), Some(1));

        let mut second = first.clone();
        second.operation_id = "activation-op-2".into();
        second.split.operation_id = second.operation_id.clone();
        second.split.predecessor_generation = 1;
        second.source_checkpoint.as_mut().unwrap().manifest_key = "g1".into();
        second
            .targets
            .get_mut(&RangeId::COORDINATOR)
            .unwrap()
            .wal_generation = 2;
        second.split.left.wal_generation = 2;
        assert_eq!(
            next_replacement_generation(1, [&first, &second]).unwrap(),
            Some(2)
        );

        let mut fork = second.clone();
        fork.operation_id = "activation-op-fork".into();
        fork.split.operation_id = fork.operation_id.clone();
        fork.targets
            .get_mut(&RangeId::COORDINATOR)
            .unwrap()
            .wal_generation = 3;
        fork.split.left.wal_generation = 3;
        assert!(next_replacement_generation(1, [&first, &second, &fork]).is_err());
    }

    #[test]
    fn non_range_zero_mutations_do_not_create_range_zero_generation_edges() {
        let move_receipt = move_receipt();
        let split_receipt = non_range_zero_split_receipt();

        assert_eq!(
            next_replacement_generation(0, [&move_receipt, &split_receipt]).unwrap(),
            None
        );
    }

    #[test]
    fn non_range_zero_mutation_with_missing_target_fails_closed() {
        let mut receipt = non_range_zero_split_receipt();
        receipt.targets.remove(&RangeId::new(3));

        assert!(next_replacement_generation(0, [&receipt]).is_err());
    }

    #[test]
    fn timestamp_primary_alias_requires_one_exact_interval_replacement() {
        let move_receipt = move_receipt();
        let move_discovery = ActivationDiscovery {
            recovery_map: move_receipt.split.current_map.clone(),
            recovery_generations: BTreeMap::new(),
            receipt: move_receipt,
        };
        assert_eq!(
            move_discovery.timestamp_primary_aliases(),
            BTreeMap::from([(RangeId::new(1), RangeId::new(2))])
        );

        let split_receipt = non_range_zero_split_receipt();
        let split_discovery = ActivationDiscovery {
            recovery_map: split_receipt.split.current_map.clone(),
            recovery_generations: BTreeMap::new(),
            receipt: split_receipt,
        };
        assert!(split_discovery.timestamp_primary_aliases().is_empty());
    }

    #[test]
    fn target_phase_journal_promotes_non_range_zero_move_recovery() {
        let mut move_authority = move_receipt();
        move_authority.phase = TopologyActivationPhase::CheckpointDurable;
        let mut discovery = ActivationDiscovery {
            recovery_map: move_authority.split.current_map.clone(),
            recovery_generations: BTreeMap::from([
                (RangeId::COORDINATOR, 0),
                (
                    move_authority.split.predecessor,
                    move_authority.split.predecessor_generation,
                ),
            ]),
            receipt: move_authority,
        };
        discovery
            .promote_authoritative_target_recovery()
            .expect("target-phase journal is additional roll-forward authority");
        assert_eq!(discovery.recovery_map, discovery.receipt.split.target_map);
        assert!(
            !discovery
                .recovery_generations
                .contains_key(&discovery.receipt.split.predecessor)
        );

        let mut missing = non_range_zero_split_receipt();
        missing.phase = TopologyActivationPhase::TopologyCommitted;
        missing.targets.remove(&RangeId::new(3));
        let mut discovery = ActivationDiscovery {
            recovery_map: missing.split.current_map.clone(),
            recovery_generations: BTreeMap::new(),
            receipt: missing,
        };
        assert!(discovery.promote_authoritative_target_recovery().is_err());

        let range_zero = receipt();
        let mut discovery = ActivationDiscovery {
            recovery_map: range_zero.split.current_map.clone(),
            recovery_generations: BTreeMap::from([(RangeId::COORDINATOR, 0)]),
            receipt: range_zero,
        };
        discovery
            .promote_authoritative_target_recovery()
            .expect("range-zero recovery remains graph-owned");
        assert_eq!(discovery.recovery_map, discovery.receipt.split.current_map);
    }

    #[test]
    fn provisional_registry_overlay_replaces_only_sealed_non_range_zero_targets() {
        for receipt in [move_receipt(), non_range_zero_split_receipt()] {
            let current = tenant_record_for_receipt(&receipt);
            let expected_ids = receipt
                .split
                .target_map
                .ranges()
                .iter()
                .map(|range| range.range_id.as_u32())
                .collect::<Vec<_>>();
            let discovery = ActivationDiscovery {
                recovery_map: receipt.split.current_map.clone(),
                recovery_generations: BTreeMap::new(),
                receipt,
            };
            let source_record_version = current.record_version;

            let provisional = discovery
                .provisional_tenant_record(&current, source_record_version)
                .expect("exact overlay");

            assert_eq!(
                provisional
                    .ranges
                    .iter()
                    .map(|range| range.range_id)
                    .collect::<Vec<_>>(),
                expected_ids
            );
            assert_eq!(provisional.record_version, source_record_version + 1);
            for target in discovery.receipt.targets.values() {
                let overlaid = provisional
                    .ranges
                    .iter()
                    .find(|range| range.range_id == target.range_id.as_u32())
                    .expect("target range");
                assert_eq!(overlaid.endpoint, target.endpoint);
                assert_eq!(overlaid.wal_generation, target.wal_generation);
            }
            assert_eq!(
                discovery
                    .provisional_tenant_record(&provisional, source_record_version)
                    .expect("already-target layout remains exact"),
                provisional
            );
        }
    }

    #[test]
    fn provisional_registry_overlay_rejects_conflicting_current_layout() {
        let receipt = move_receipt();
        let mut current = tenant_record_for_receipt(&receipt);
        current.ranges[1].range_id = 9;
        let discovery = ActivationDiscovery {
            recovery_map: receipt.split.current_map.clone(),
            recovery_generations: BTreeMap::new(),
            receipt,
        };

        assert!(
            discovery
                .provisional_tenant_record(&current, current.record_version)
                .is_err()
        );
    }

    #[test]
    fn provisional_registry_overlay_rejects_unsealed_record_versions() {
        let receipt = move_receipt();
        let current = tenant_record_for_receipt(&receipt);
        let source_record_version = current.record_version;
        let discovery = ActivationDiscovery {
            recovery_map: receipt.split.current_map.clone(),
            recovery_generations: BTreeMap::new(),
            receipt,
        };

        let mut wrong_current = current.clone();
        wrong_current.record_version += 1;
        assert!(
            discovery
                .provisional_tenant_record(&wrong_current, source_record_version)
                .is_err()
        );

        let mut stale_target = discovery
            .provisional_tenant_record(&current, source_record_version)
            .expect("target");
        stale_target.record_version = source_record_version;
        assert!(
            discovery
                .provisional_tenant_record(&stale_target, source_record_version)
                .is_err()
        );
    }

    #[test]
    fn receipt_history_canonicalization_accepts_identical_duplicates_only() {
        let prepared = receipt();
        let mut source = prepared.clone();
        source.revision += 1;
        source.phase = TopologyActivationPhase::SourceCheckpoint;
        source.source_checkpoint = Some(crabka_gres_ranges::CheckpointManifest {
            range_id: source.split.predecessor,
            covered_offset: 3,
            manifest_key: "checkpoint".into(),
        });
        let canonical = canonicalize_receipt_history(
            &prepared.operation_id,
            [prepared.clone(), prepared.clone(), source.clone()],
        )
        .expect("identical duplicate canonicalizes");
        assert_eq!(canonical, [prepared.clone(), source.clone()]);

        let mut divergent = prepared.clone();
        divergent.tenant = "forged".into();
        assert!(
            canonicalize_receipt_history(
                &prepared.operation_id,
                [prepared.clone(), divergent, source.clone()],
            )
            .is_err()
        );

        let mut missing = source;
        missing.revision += 1;
        let operation_id = prepared.operation_id.clone();
        let missing = canonicalize_receipt_history(&operation_id, [prepared, missing])
            .expect("canonical values retain the gap");
        assert!(validate_receipt_history(&missing).is_err());
    }

    #[test]
    fn wal_receipt_key_must_match_payload_operation_and_tenant() {
        let receipt = receipt();
        let exact = crabka_pgkv::key::topology_activation_receipt_key(
            &receipt.tenant,
            &receipt.operation_id,
        );
        validate_receipt_wal_identity(&receipt.tenant, &exact, &receipt).expect("exact identity");
        let wrong_operation =
            crabka_pgkv::key::topology_activation_receipt_key(&receipt.tenant, "other-op");
        assert!(
            validate_receipt_wal_identity(&receipt.tenant, &wrong_operation, &receipt).is_err()
        );
        assert!(validate_receipt_wal_identity("other-tenant", &exact, &receipt).is_err());
    }

    fn receipt() -> TopologyActivationReceipt {
        let tenant = TenantName::parse("activation-test").expect("tenant");
        let current_map = RangeMap::new(
            tenant,
            MapEpoch::ZERO,
            vec![RangeSpec::for_interval(
                RangeId::COORDINATOR,
                RangeKey::MIN,
                None,
            )],
        )
        .expect("map");
        let split_at = RangeKey::table_start(TableId::new(1));
        let left = SuccessorDescriptor {
            range_id: RangeId::COORDINATOR,
            endpoint: "local".into(),
            wal_generation: 1,
            interval: RangeSpec::for_interval(RangeId::COORDINATOR, RangeKey::MIN, Some(split_at)),
        };
        let right = SuccessorDescriptor {
            range_id: RangeId::new(2),
            endpoint: "local".into(),
            wal_generation: 1,
            interval: RangeSpec::for_interval(RangeId::new(2), split_at, None),
        };
        let split = SplitState::for_split(
            "activation-op",
            SplitCommand {
                current_map,
                predecessor: RangeId::COORDINATOR,
                predecessor_generation: 0,
                left: left.clone(),
                right: right.clone(),
            },
        )
        .expect("split");
        let targets = [left, right]
            .into_iter()
            .map(|target| {
                (
                    target.range_id,
                    ActivationTargetProgress {
                        range_id: target.range_id,
                        wal_generation: target.wal_generation,
                        endpoint: target.endpoint,
                        interval: target.interval,
                        replay_journal_seq: None,
                        writer_activated: false,
                        bootstrap_checkpoint: None,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        TopologyActivationReceipt {
            tenant: "activation-test".into(),
            operation_id: "activation-op".into(),
            revision: 1,
            phase: TopologyActivationPhase::Prepared,
            split,
            source_checkpoint: None,
            barrier_offset: None,
            tail_sha256: None,
            targets,
        }
    }

    fn move_receipt() -> TopologyActivationReceipt {
        let tenant = TenantName::parse("activation-move-test").expect("tenant");
        let boundary = RangeKey::table_start(TableId::new(50));
        let current_map = RangeMap::new(
            tenant,
            MapEpoch::ZERO,
            vec![
                RangeSpec::for_interval(RangeId::COORDINATOR, RangeKey::MIN, Some(boundary)),
                RangeSpec::for_interval(RangeId::new(1), boundary, None),
            ],
        )
        .expect("map");
        let replacement = SuccessorDescriptor {
            range_id: RangeId::new(2),
            endpoint: "local".into(),
            wal_generation: 1,
            interval: RangeSpec::for_interval(RangeId::new(2), boundary, None),
        };
        let split = SplitState::for_move(
            "activation-move-op",
            MoveRangeCommand {
                current_map,
                range_id: RangeId::new(1),
                predecessor_generation: 0,
                replacement: replacement.clone(),
            },
        )
        .expect("move");
        let targets = BTreeMap::from([(
            replacement.range_id,
            ActivationTargetProgress {
                range_id: replacement.range_id,
                wal_generation: replacement.wal_generation,
                endpoint: replacement.endpoint,
                interval: replacement.interval,
                replay_journal_seq: Some(11),
                writer_activated: false,
                bootstrap_checkpoint: None,
            },
        )]);
        TopologyActivationReceipt {
            tenant: "activation-move-test".into(),
            operation_id: "activation-move-op".into(),
            revision: 3,
            phase: TopologyActivationPhase::MustActivate,
            source_checkpoint: Some(crabka_gres_ranges::CheckpointManifest {
                range_id: RangeId::new(1),
                covered_offset: 3,
                manifest_key: "move-g0".into(),
            }),
            barrier_offset: Some(7),
            tail_sha256: Some("move-tail".into()),
            split,
            targets,
        }
    }

    fn non_range_zero_split_receipt() -> TopologyActivationReceipt {
        let tenant = TenantName::parse("activation-split-test").expect("tenant");
        let source_start = RangeKey::table_start(TableId::new(50));
        let split_at = RangeKey::table_start(TableId::new(75));
        let current_map = RangeMap::new(
            tenant,
            MapEpoch::ZERO,
            vec![
                RangeSpec::for_interval(RangeId::COORDINATOR, RangeKey::MIN, Some(source_start)),
                RangeSpec::for_interval(RangeId::new(1), source_start, None),
            ],
        )
        .expect("map");
        let left = SuccessorDescriptor {
            range_id: RangeId::new(2),
            endpoint: "left".into(),
            wal_generation: 1,
            interval: RangeSpec::for_interval(RangeId::new(2), source_start, Some(split_at)),
        };
        let right = SuccessorDescriptor {
            range_id: RangeId::new(3),
            endpoint: "right".into(),
            wal_generation: 1,
            interval: RangeSpec::for_interval(RangeId::new(3), split_at, None),
        };
        let split = SplitState::for_split(
            "activation-split-op",
            SplitCommand {
                current_map,
                predecessor: RangeId::new(1),
                predecessor_generation: 0,
                left: left.clone(),
                right: right.clone(),
            },
        )
        .expect("split");
        let targets = [left, right]
            .into_iter()
            .map(|target| {
                (
                    target.range_id,
                    ActivationTargetProgress {
                        range_id: target.range_id,
                        wal_generation: target.wal_generation,
                        endpoint: target.endpoint,
                        interval: target.interval,
                        replay_journal_seq: Some(11),
                        writer_activated: false,
                        bootstrap_checkpoint: None,
                    },
                )
            })
            .collect();
        TopologyActivationReceipt {
            tenant: "activation-split-test".into(),
            operation_id: "activation-split-op".into(),
            revision: 3,
            phase: TopologyActivationPhase::MustActivate,
            source_checkpoint: Some(crabka_gres_ranges::CheckpointManifest {
                range_id: RangeId::new(1),
                covered_offset: 3,
                manifest_key: "split-g0".into(),
            }),
            barrier_offset: Some(7),
            tail_sha256: Some("split-tail".into()),
            split,
            targets,
        }
    }

    fn tenant_record_for_receipt(
        receipt: &TopologyActivationReceipt,
    ) -> crabka_gres_control::TenantRecord {
        let ranges = receipt
            .split
            .current_map
            .ranges()
            .iter()
            .map(|spec| crabka_gres_control::RangeLayoutEntry {
                range_id: spec.range_id.as_u32(),
                end_key: spec.end.map(|end| {
                    crabka_gres_control::RangeBoundary::new(end.table_id.as_u64(), end.rowid)
                }),
                endpoint: format!("source-r{}", spec.range_id.as_u32()),
                wal_generation: 0,
                lifecycle: crabka_gres_control::RangeLifecycle::default(),
                retirement: None,
            })
            .collect();
        crabka_gres_control::TenantRecord::new(
            u64::from(receipt.split.current_map.epoch()).max(1),
            crabka_gres_control::TenantId::try_from(receipt.tenant.as_str()).expect("id"),
            crabka_gres_control::TenantName::try_from(receipt.tenant.as_str()).expect("name"),
            crabka_gres_control::TenantState::Active,
            crabka_gres_control::SqlUser::try_from("alice").expect("user"),
            "SCRAM-SHA-256$4096:salt$stored:server".into(),
            3,
        )
        .expect("record")
        .with_range_layout(ranges)
        .expect("layout")
    }
}
