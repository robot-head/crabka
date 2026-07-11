//! In-process multi-range tenant assembly for G-7a.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
    path::PathBuf,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use arc_swap::ArcSwap;
use crabka_pgcatalog::ShardingStrategy;
use crabka_pgexec::{
    ExecError, PredicateOp, PredicatePushdown, SqlEngine, foreign::ForeignScanner,
};
use crabka_pgtypes::Datum;
use crabka_pgwire::{
    engine::{
        BoundParam, CloseTarget, Engine, ExecuteOutcome, PortalDescription, PreparedDescription,
        QueryResult, Session, TxStatus,
    },
    error::{PgError, sqlstate},
};
use tokio::sync::Mutex;

use crate::{
    CheckpointManifest, HashShardSpec, MapEpoch, RangeId, RangeKey, RangeMap, RangeScanSegment,
    RangeSpec, RangeTransferCapability, RangeTransferError, RouteIntent,
    RowInterval as MapRowInterval, SplitCommand, SplitError, SplitHooks, SplitState,
    SplitStateStore, TableId, TableTransferRequest, TenantName,
    barrier::{Range0Barrier, Range0EndSampler},
    coordinator::{LocalCoordinator, LocalCoordinatorError, TransactionDecision},
    forward::{
        ForwardError, RegistryRangeScanner, RegistryRemoteForward, RegistryTsoRpc, RemoteForward,
        RemoteRangeSession,
    },
    registry::RangeRegistry,
    run_split,
    transport::FramedTcpClient,
    tso::{
        BatchedTsoClient, EpochHeartbeat, GrantLease, MemoryTsoHorizon, TsoError,
        TsoHorizonCommitter, TsoOracle, TsoRpc,
    },
};

/// Configuration for an in-process tenant composed from local range engines.
#[derive(Debug, Clone)]
pub struct MultiRangeTenantConfig {
    /// Tenant name used by topic/id constructors and diagnostics.
    pub tenant: TenantName,
    /// Validated range map used by the gateway router.
    pub range_map: RangeMap,
    /// Optional parent directory for one durable store per range.
    pub data_dir: Option<PathBuf>,
    /// Optional set of range engines hosted by this gateway process.
    ///
    /// The set may exclude range 0 only when a read-only range-0 replica is supplied.
    pub hosted_ranges: Option<Vec<RangeId>>,
    /// Read-only local replica used by an rN-only compute for catalog and global-decision reads.
    pub range0_replica: Option<ReadOnlyRange0Replica>,
    /// Optional control-plane endpoint map used to reach ranges not hosted here.
    pub range_registry: Option<RangeRegistry>,
    /// TLS-only client used for every remote range call.
    pub range_client: Option<FramedTcpClient>,
    #[doc(hidden)]
    pub commit_fault_for_testing: Option<GatewayCommitFault>,
    #[doc(hidden)]
    pub empty_table_split_test_hook: Option<EmptyTableSplitTestHook>,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayCommitFault {
    BeforeDecisionAfterPrepare,
    BeforeReleaseAfterCommitDecision,
    AfterCommitDecisionWithoutRecoveryMetadata,
    AfterTimestampPrewriteBeforeDecision,
    AfterTimestampCommitDecision,
}

/// Test synchronization point after an empty-table split's initial validation.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct EmptyTableSplitTestHook {
    initial_validation_complete: Arc<tokio::sync::Notify>,
    allow_final_validation: Arc<tokio::sync::Notify>,
}

impl Default for EmptyTableSplitTestHook {
    fn default() -> Self {
        Self {
            initial_validation_complete: Arc::new(tokio::sync::Notify::new()),
            allow_final_validation: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

impl EmptyTableSplitTestHook {
    /// Create a hook that blocks the split until released by the test.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Wait until the split has completed its initial validation.
    pub async fn initial_validation_complete(&self) {
        self.initial_validation_complete.notified().await;
    }

    /// Allow the split to acquire its table write gate and revalidate.
    pub fn allow_final_validation(&self) {
        self.allow_final_validation.notify_one();
    }

    async fn wait_after_initial_validation(&self) {
        self.initial_validation_complete.notify_one();
        self.allow_final_validation.notified().await;
    }
}

impl MultiRangeTenantConfig {
    /// Build a config from comma-separated table-start boundaries.
    pub fn from_boundaries(tenant: TenantName, boundaries: &str) -> Result<Self, TenantError> {
        let range_map = range_map_from_boundaries(tenant.clone(), boundaries)?;
        Ok(Self {
            tenant,
            range_map,
            data_dir: None,
            hosted_ranges: None,
            range0_replica: None,
            range_registry: None,
            range_client: None,
            commit_fault_for_testing: None,
            empty_table_split_test_hook: None,
        })
    }

    /// Return a config that opens each range under `data_dir/r<id>`.
    #[must_use]
    pub fn with_data_dir(mut self, data_dir: PathBuf) -> Self {
        self.data_dir = Some(data_dir);
        self
    }

    /// Return a config that opens only the requested local ranges.
    ///
    /// An rN-only gateway requires [`Self::with_read_only_range0_replica`].
    pub fn with_hosted_ranges(mut self, hosted_ranges: Vec<RangeId>) -> Result<Self, TenantError> {
        let hosted_ranges = normalize_hosted_ranges(&self.range_map, hosted_ranges)?;
        self.hosted_ranges = Some(hosted_ranges);
        Ok(self)
    }

    /// Supply the read-only range-0 replica required by an rN-only gateway.
    #[must_use]
    pub fn with_read_only_range0_replica(mut self, replica: ReadOnlyRange0Replica) -> Self {
        self.range0_replica = Some(replica);
        self
    }

    /// Route non-hosted ranges through this registry snapshot.
    #[must_use]
    pub fn with_range_registry(mut self, registry: RangeRegistry) -> Self {
        self.range_registry = Some(registry);
        self
    }

    /// Require this authenticated TLS client for remote range forwarding.
    #[must_use]
    pub fn with_range_client(mut self, client: FramedTcpClient) -> Self {
        self.range_client = Some(client);
        self
    }

    #[doc(hidden)]
    #[must_use]
    pub fn with_commit_fault_for_testing(mut self, fault: GatewayCommitFault) -> Self {
        self.commit_fault_for_testing = Some(fault);
        self
    }

    #[doc(hidden)]
    #[must_use]
    pub fn with_empty_table_split_test_hook(mut self, hook: EmptyTableSplitTestHook) -> Self {
        self.empty_table_split_test_hook = Some(hook);
        self
    }
}

/// Errors while parsing or starting a multi-range tenant.
#[derive(Debug, thiserror::Error)]
pub enum TenantError {
    /// Boundary list is empty.
    #[error("--ranges must contain at least one boundary")]
    EmptyBoundaries,
    /// Boundary token is malformed.
    #[error("invalid --ranges boundary {token:?}: {source}")]
    InvalidBoundary {
        token: String,
        source: std::num::ParseIntError,
    },
    /// Boundary values must be sorted and unique.
    #[error("--ranges boundaries must be strictly increasing and start at 0 or 0:0")]
    InvalidBoundaryOrder,
    /// The resulting range map is invalid.
    #[error(transparent)]
    InvalidRangeMap(#[from] crate::MapValidationError),
    /// A range engine failed to open.
    #[error("range r{range_id} engine: {error:?}")]
    Engine { range_id: RangeId, error: ExecError },
    /// A hosted range was not present in the layout.
    #[error("--host-ranges contains r{0}, which is absent from --ranges")]
    HostedRangeMissing(RangeId),
    /// An rN-only gateway lacks the local read-only range-0 replica required for safe reads.
    #[error("gateway hosting no r0 requires a supplied read-only range-0 replica")]
    MissingRangeZeroReplica,
    /// The range-0 timestamp oracle failed to start.
    #[error("range r0 timestamp oracle: {0}")]
    TimestampOracle(TsoError),
    /// A durable timestamp transaction could not be settled before serving.
    #[error("timestamp transaction recovery: {0}")]
    TimestampRecovery(String),
    /// A registry can route calls remotely only through mTLS.
    #[error("remote range routing requires a TLS client identity and trust configuration")]
    MissingRangeTls,
}

/// Read-only range-0 state injected into an rN-only compute.
#[derive(Clone)]
pub struct ReadOnlyRange0Replica {
    catalog_kv: Arc<dyn crabka_pgkv::Kv>,
    barrier: Arc<Range0Barrier>,
}

impl std::fmt::Debug for ReadOnlyRange0Replica {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ReadOnlyRange0Replica(..)")
    }
}

impl ReadOnlyRange0Replica {
    /// Bind a follower tail's catalog store and barrier into one replica.
    ///
    /// The catalog KV is derived from `tail`, so the barrier can only certify
    /// the exact store to which the follower applies committed frames.
    #[must_use]
    pub fn new(tail: crate::Range0Tail, sampler: Arc<dyn Range0EndSampler>) -> Self {
        Self {
            catalog_kv: tail.store_handle(),
            barrier: Arc::new(Range0Barrier::new(tail, sampler)),
        }
    }
}

/// Fail-clear rejection for the deliberately narrow local SQL split bridge.
#[derive(Debug, thiserror::Error)]
pub enum LocalSqlSplitError {
    #[error(transparent)]
    Orchestration(#[from] SplitError),
    #[error("local SQL splits require a table-boundary split point")]
    NonTableBoundary,
    #[error("split target table {0} is not catalog-visible")]
    MissingTable(TableId),
    #[error("split target table {0} is not owned by the predecessor")]
    WrongPredecessor(TableId),
    #[error("split successor r{0} is already hosted")]
    SuccessorAlreadyHosted(RangeId),
    #[error("split operation id was retried with different table or successor")]
    RetryMismatch,
    #[error("local SQL split bridge does not support remote ranges")]
    RemoteRange,
    #[error("local SQL split bridge requires no explicit transaction")]
    ExplicitTransaction,
    #[error("split target table {0} is not an ordinary unsharded table")]
    UnsupportedTableKind(TableId),
    #[error("split target table {0} has secondary indexes")]
    IndexedTable(TableId),
    #[error(
        "split target table {0} contains physical rows; local SQL splitting requires a durable range-owned SQL snapshot transfer"
    )]
    NonEmptyTable(TableId),
    #[error("split target table {0} has allocated row IDs")]
    AllocatedRowIds(TableId),
    #[error("successor r{range_id} engine: {error:?}")]
    SuccessorEngine { range_id: RangeId, error: ExecError },
    #[error(transparent)]
    Catalog(#[from] crabka_pgcatalog::CatalogError),
    #[error(transparent)]
    Storage(#[from] crabka_pgkv::KvError),
    #[error(transparent)]
    Transfer(#[from] RangeTransferError),
    #[error(
        "split successor interval contains catalog-visible table {0} besides the transferred table"
    )]
    SuccessorIntervalHasOtherTable(TableId),
}

/// Opaque handles that keep all in-process range engines alive.
#[derive(Clone)]
pub struct MultiRangeTenantHandles {
    inner: Arc<TenantInner>,
}

impl MultiRangeTenantHandles {
    /// Return the validated map used by this tenant.
    #[must_use]
    pub fn range_map(&self) -> RangeMap {
        self.inner.serving.load().range_map.clone()
    }

    /// Snapshot the gateway route log used by behavior tests.
    pub async fn route_log(&self) -> Vec<RouteRecord> {
        self.inner.route_log.lock().await.clone()
    }

    /// Return the local coordinator handle used by cross-range transactions.
    #[must_use]
    pub fn coordinator(&self) -> LocalCoordinator {
        self.inner.coordinator.clone()
    }
}

/// Gateway engine exposed to pgwire and tests.
#[derive(Clone)]
pub struct MultiRangeTenant {
    inner: Arc<TenantInner>,
}

impl MultiRangeTenant {
    /// Start N local range engines and return the gateway plus lifetime handles.
    pub fn start(
        config: MultiRangeTenantConfig,
    ) -> Result<(Self, MultiRangeTenantHandles), TenantError> {
        Self::start_with_engine_factory(config, open_range_engine)
    }

    /// Start a tenant from explicitly injected per-range engines.
    ///
    /// The factory is called once for every hosted range. Production substrate callers should use
    /// this seam to pass engines recovered from each range's WAL instead of falling back to local
    /// in-process stores.
    pub fn start_with_engine_factory(
        config: MultiRangeTenantConfig,
        open_engine: impl FnMut(Option<&PathBuf>, RangeId) -> Result<SqlEngine, ExecError>,
    ) -> Result<(Self, MultiRangeTenantHandles), TenantError> {
        Self::start_with_engine_factory_and_timestamp_oracle(config, open_engine, None)
    }

    /// Start a tenant with an explicitly supplied timestamp oracle.
    pub fn start_with_engine_factory_and_timestamp_oracle(
        mut config: MultiRangeTenantConfig,
        mut open_engine: impl FnMut(Option<&PathBuf>, RangeId) -> Result<SqlEngine, ExecError>,
        timestamp_oracle: Option<Arc<dyn crabka_pgexec::TimestampOracle>>,
    ) -> Result<(Self, MultiRangeTenantHandles), TenantError> {
        let hosts_range0 = Self::validate_range0_assembly(&mut config)?;
        let mut engines = BTreeMap::new();
        let hosted_ranges = config.hosted_ranges.as_ref();
        for spec in config.range_map.ranges() {
            if hosted_ranges.is_some_and(|ranges| !ranges.contains(&spec.range_id)) {
                continue;
            }
            let engine = open_engine(config.data_dir.as_ref(), spec.range_id).map_err(|error| {
                TenantError::Engine {
                    range_id: spec.range_id,
                    error,
                }
            })?;
            engines.insert(spec.range_id, engine);
        }

        if let Some(replica) = &config.range0_replica {
            install_replica_catalog(&mut engines, replica);
        } else {
            install_range0_catalog(&mut engines);
        }

        if let Some(coordinator) = engines.get_mut(&RangeId::COORDINATOR) {
            coordinator
                .init_gtm_coordinator()
                .map_err(|error| TenantError::Engine {
                    range_id: RangeId::COORDINATOR,
                    error,
                })?;
            let coordinator = coordinator.clone_handle();
            for (range_id, engine) in &mut engines {
                if range_id.is_coordinator() {
                    continue;
                }
                coordinator.share_gtm_to(engine);
            }
        }

        match timestamp_oracle {
            Some(timestamp_oracle) => install_timestamp_oracle(&mut engines, &timestamp_oracle),
            None if hosts_range0 => install_memory_timestamp_oracle(&mut engines)?,
            None => {
                if let (Some(registry), Some(client)) =
                    (config.range_registry.clone(), config.range_client.clone())
                {
                    let rpc = Arc::new(RegistryTsoRpc::new(registry, client));
                    let timestamp_oracle: Arc<dyn crabka_pgexec::TimestampOracle> =
                        Arc::new(PgexecTsoOracle {
                            client: BatchedTsoClient::new(rpc),
                        });
                    install_timestamp_oracle(&mut engines, &timestamp_oracle);
                } else {
                    install_unavailable_timestamp_oracle(&mut engines);
                }
            }
        }

        recover_durable_timestamp_transactions(&engines)?;
        let scanner_engines = engines
            .iter()
            .map(|(range_id, engine)| (*range_id, engine.clone_handle()))
            .collect();
        let range_scanner: Arc<dyn crabka_pgexec::RangeScanner> =
            match config.range_registry.clone() {
                Some(registry) => Arc::new(RegistryRangeScanner::new(
                    registry,
                    config
                        .range_client
                        .clone()
                        .ok_or(TenantError::MissingRangeTls)?,
                    scanner_engines,
                )),
                None => Arc::new(InProcessRangeScanner {
                    engines: scanner_engines,
                    range_map: config.range_map.clone(),
                }),
            };
        for engine in engines.values_mut() {
            engine.set_range_scanner(range_scanner.clone());
        }
        let range_client = range_client_for_registry(&config)?;
        let remote_forward = match (config.range_registry, range_client) {
            (Some(registry), Some(client)) => {
                Some(Arc::new(RegistryRemoteForward::new(registry, client))
                    as Arc<dyn RemoteForward>)
            }
            (None, None | Some(_)) => None,
            (Some(_), None) => return Err(TenantError::MissingRangeTls),
        };
        let inner = Arc::new(TenantInner {
            tenant: config.tenant,
            serving: ArcSwap::from_pointee(ServingSnapshot::ready(config.range_map, engines)),
            remote_forward,
            coordinator: LocalCoordinator::default(),
            route_log: Mutex::new(Vec::new()),
            data_dir: config.data_dir,
            split_states: Mutex::new(BTreeMap::new()),
            split_lock: Mutex::new(()),
            schema_gate: Arc::new(Mutex::new(())),
            table_write_gates: StdMutex::new(BTreeMap::new()),
            active_explicit_transactions: AtomicUsize::new(0),
            empty_table_split_test_hook: config.empty_table_split_test_hook,
            commit_fault_for_testing: config
                .commit_fault_for_testing
                .map(|fault| Arc::new(StdMutex::new(Some(fault)))),
        });
        let gateway = Self {
            inner: Arc::clone(&inner),
        };
        let handles = MultiRangeTenantHandles { inner };
        Ok((gateway, handles))
    }

    fn validate_range0_assembly(config: &mut MultiRangeTenantConfig) -> Result<bool, TenantError> {
        config.hosted_ranges = config
            .hosted_ranges
            .take()
            .map(|ranges| normalize_hosted_ranges(&config.range_map, ranges))
            .transpose()?;
        let hosts_range0 = config
            .hosted_ranges
            .as_ref()
            .is_none_or(|ranges| ranges.contains(&RangeId::COORDINATOR));
        if !hosts_range0 && config.range0_replica.is_none() {
            return Err(TenantError::MissingRangeZeroReplica);
        }
        Ok(hosts_range0)
    }

    /// Snapshot handles for the ranges hosted by this compute process.
    #[must_use]
    pub fn hosted_range_engines(&self) -> BTreeMap<RangeId, SqlEngine> {
        self.inner
            .serving
            .load()
            .engines
            .iter()
            .map(|(range_id, engine)| (*range_id, engine.clone_handle()))
            .collect()
    }

    /// Install one foreign scanner on every currently served range engine.
    ///
    /// The serving snapshot is replaced atomically so new gateway sessions
    /// cannot observe a mixture of scanner-enabled and scanner-less engines.
    pub fn set_foreign_scanner(&self, scanner: &Arc<dyn ForeignScanner>) {
        let serving = self.inner.serving.load_full();
        let mut engines = serving
            .engines
            .iter()
            .map(|(range_id, engine)| (*range_id, engine.clone_handle()))
            .collect::<BTreeMap<_, _>>();
        for engine in engines.values_mut() {
            engine.set_foreign_scanner(Arc::clone(scanner));
        }
        self.inner
            .serving
            .store(Arc::new(ServingSnapshot::ready_with_keepalives(
                serving.range_map.clone(),
                engines,
                serving.keepalives.clone(),
            )));
    }

    /// Split off one physically empty ordinary table into a locally hosted successor.
    ///
    /// This first bridge intentionally performs no physical data migration. It accepts only a
    /// table boundary whose moved table has no rows and whose row-id allocator has not advanced.
    pub async fn split_empty_table_successor(
        &self,
        operation_id: impl Into<String>,
        successor: RangeId,
        table_id: TableId,
    ) -> Result<SplitState, LocalSqlSplitError> {
        let _split_guard = self.inner.split_lock.lock().await;
        let operation_id = operation_id.into();
        if let Some(state) = self
            .inner
            .split_states
            .lock()
            .await
            .get(&operation_id)
            .cloned()
        {
            if state.successor != successor
                || state.successor_after.start != RangeKey::table_start(table_id)
            {
                return Err(LocalSqlSplitError::RetryMismatch);
            }
            let serving = self.inner.serving.load_full();
            let _table_write_gate = if serving.range_map == state.target_map {
                None
            } else {
                let table_write_gate = self.inner.table_write_gate(table_id);
                let table_write_gate = table_write_gate.lock_owned().await;
                let serving = self.inner.serving.load_full();
                self.validate_empty_table_split(&serving, successor, table_id)?;
                Some(table_write_gate)
            };
            let bridge = LocalSqlSplitBridge { tenant: self };
            return run_split(
                operation_id,
                SplitCommand {
                    current_map: state.current_map,
                    predecessor: state.predecessor,
                    successor: state.successor,
                    split_at: state.successor_after.start,
                },
                &bridge,
                &bridge,
            )
            .await
            .map_err(Into::into);
        }
        let serving = self.inner.serving.load_full();
        self.validate_empty_table_split(&serving, successor, table_id)?;
        if let Some(hook) = &self.inner.empty_table_split_test_hook {
            hook.wait_after_initial_validation().await;
        }
        let table_write_gate = self.inner.table_write_gate(table_id);
        let _table_write_gate = table_write_gate.lock_owned().await;
        let serving = self.inner.serving.load_full();
        self.validate_empty_table_split(&serving, successor, table_id)?;
        let predecessor = serving
            .range_map
            .range_for_key(table_id, 0)
            .map_err(SplitError::from)?
            .range_id;
        let command = SplitCommand {
            current_map: serving.range_map.clone(),
            predecessor,
            successor,
            split_at: RangeKey::table_start(table_id),
        };
        let bridge = LocalSqlSplitBridge { tenant: self };
        run_split(operation_id, command, &bridge, &bridge)
            .await
            .map_err(Into::into)
    }

    /// Physically transfer and publish one populated ordinary table into a local successor.
    ///
    /// This is intentionally limited to a target interval containing exactly one
    /// catalog-visible table.  The table gate and source WAL pause are held until
    /// the complete successor engine is atomically published with the new map.
    pub async fn split_populated_table_successor(
        &self,
        operation_id: impl Into<String>,
        successor: RangeId,
        table_id: TableId,
        transfer: &dyn RangeTransferCapability,
    ) -> Result<SplitState, LocalSqlSplitError> {
        let _split_guard = self.inner.split_lock.lock().await;
        let _schema_gate = self.inner.schema_gate.clone().lock_owned().await;
        let operation_id = operation_id.into();
        let table_write_gate = self.inner.table_write_gate(table_id);
        let _table_write_gate = table_write_gate.lock_owned().await;
        let serving = self.inner.serving.load_full();
        let transfer_table = self.validate_populated_table_split(&serving, successor, table_id)?;
        let state = SplitState::for_split(
            operation_id,
            SplitCommand {
                current_map: serving.range_map.clone(),
                predecessor: transfer_table.predecessor,
                successor,
                split_at: RangeKey::table_start(table_id),
            },
        )?;
        Self::validate_successor_interval(&serving, &state, table_id)?;

        let checkpoint = transfer
            .force_checkpoint(transfer_table.predecessor)
            .await?;
        let barrier = transfer.pause_at_checkpoint(&checkpoint).await?;
        let pause = TransferPauseGuard::new(transfer, barrier);
        self.stage_and_publish_successor(
            transfer,
            &serving,
            &state,
            TableTransferRequest {
                target_range: state.successor,
                routing_table_id: table_id,
                physical_table_id: transfer_table.physical_table_id,
            },
            &checkpoint,
            barrier,
        )
        .await?;
        pause.resume().await?;
        self.inner
            .split_states
            .lock()
            .await
            .insert(state.operation_id.clone(), state.clone());
        Ok(state)
    }

    async fn stage_and_publish_successor(
        &self,
        transfer: &dyn RangeTransferCapability,
        serving: &ServingSnapshot,
        state: &SplitState,
        request: TableTransferRequest,
        checkpoint: &CheckpointManifest,
        barrier: crate::RangeTransferBarrier,
    ) -> Result<(), LocalSqlSplitError> {
        let tail = transfer
            .read_committed_tail(state.predecessor, checkpoint.covered_offset, barrier)
            .await?;
        let staged = transfer
            .stage_empty_successor(request, checkpoint, &tail, barrier)
            .await?;
        let claimed = transfer.claim_staged_successor(&staged, barrier).await?;
        self.publish_claimed_successor(serving, state, claimed)
    }

    fn publish_claimed_successor(
        &self,
        serving: &ServingSnapshot,
        state: &SplitState,
        claimed: crate::ClaimedStagedSuccessor,
    ) -> Result<(), LocalSqlSplitError> {
        let coordinator = serving
            .engines
            .get(&RangeId::COORDINATOR)
            .ok_or(LocalSqlSplitError::RemoteRange)?;
        let mut successor = claimed.engine;
        successor.set_catalog_kv(coordinator.kv_handle());
        coordinator.share_gtm_to(&mut successor);
        successor.set_timestamp_oracle(coordinator.timestamp_oracle_handle());
        if let Some(scanner) = coordinator.foreign_scanner_handle() {
            successor.set_foreign_scanner(scanner);
        }

        let mut engines = serving
            .engines
            .iter()
            .map(|(range_id, engine)| (*range_id, engine.clone_handle()))
            .collect::<BTreeMap<_, _>>();
        engines.insert(state.successor, successor);
        let scanner: Arc<dyn crabka_pgexec::RangeScanner> = Arc::new(InProcessRangeScanner {
            engines: engines
                .iter()
                .map(|(range_id, engine)| (*range_id, engine.clone_handle()))
                .collect(),
            range_map: state.target_map.clone(),
        });
        for engine in engines.values_mut() {
            engine.set_range_scanner(Arc::clone(&scanner));
        }
        let mut keepalives = serving.keepalives.clone();
        keepalives.insert(state.successor, claimed.keepalive);
        self.inner
            .serving
            .store(Arc::new(ServingSnapshot::ready_with_keepalives(
                state.target_map.clone(),
                engines,
                keepalives,
            )));
        Ok(())
    }

    fn validate_populated_table_split(
        &self,
        serving: &ServingSnapshot,
        successor: RangeId,
        table_id: TableId,
    ) -> Result<PopulatedTransferTable, LocalSqlSplitError> {
        if self.inner.remote_forward.is_some() {
            return Err(LocalSqlSplitError::RemoteRange);
        }
        if self
            .inner
            .active_explicit_transactions
            .load(Ordering::Acquire)
            != 0
        {
            return Err(LocalSqlSplitError::ExplicitTransaction);
        }
        if serving.engines.contains_key(&successor) {
            return Err(LocalSqlSplitError::SuccessorAlreadyHosted(successor));
        }
        let predecessor = serving
            .range_map
            .range_for_key(table_id, 0)
            .map_err(SplitError::from)?
            .range_id;
        if predecessor == RangeId::COORDINATOR || !serving.engines.contains_key(&predecessor) {
            return Err(LocalSqlSplitError::RemoteRange);
        }
        let coordinator = serving
            .engines
            .get(&RangeId::COORDINATOR)
            .ok_or(LocalSqlSplitError::RemoteRange)?;
        let table = crabka_pgcatalog::list_tables(coordinator.catalog_kv())?
            .into_iter()
            .find(|table| routing_table_id(&table.name) == table_id)
            .ok_or(LocalSqlSplitError::MissingTable(table_id))?;
        if table.sharded || table.sharding.is_some() || table.foreign.is_some() {
            return Err(LocalSqlSplitError::UnsupportedTableKind(table_id));
        }
        if !crabka_pgcatalog::list_table_indexes(coordinator.catalog_kv(), &table.name)?.is_empty()
        {
            return Err(LocalSqlSplitError::IndexedTable(table_id));
        }
        Ok(PopulatedTransferTable {
            predecessor,
            physical_table_id: table.id,
        })
    }

    fn validate_successor_interval(
        serving: &ServingSnapshot,
        state: &SplitState,
        table_id: TableId,
    ) -> Result<(), LocalSqlSplitError> {
        let coordinator = serving
            .engines
            .get(&RangeId::COORDINATOR)
            .ok_or(LocalSqlSplitError::RemoteRange)?;
        for table in crabka_pgcatalog::list_tables(coordinator.catalog_kv())? {
            let catalog_table_id = routing_table_id(&table.name);
            if state
                .target_map
                .route_table(catalog_table_id)
                .map_err(SplitError::from)?
                .range_id
                == state.successor
                && catalog_table_id != table_id
            {
                return Err(LocalSqlSplitError::SuccessorIntervalHasOtherTable(
                    catalog_table_id,
                ));
            }
        }
        Ok(())
    }

    fn validate_empty_table_split(
        &self,
        serving: &ServingSnapshot,
        successor: RangeId,
        table_id: TableId,
    ) -> Result<(), LocalSqlSplitError> {
        if self.inner.remote_forward.is_some() {
            return Err(LocalSqlSplitError::RemoteRange);
        }
        if self
            .inner
            .active_explicit_transactions
            .load(Ordering::Acquire)
            != 0
        {
            return Err(LocalSqlSplitError::ExplicitTransaction);
        }
        if serving.engines.contains_key(&successor) {
            return Err(LocalSqlSplitError::SuccessorAlreadyHosted(successor));
        }
        let predecessor = serving
            .range_map
            .range_for_key(table_id, 0)
            .map_err(SplitError::from)?
            .range_id;
        let Some(predecessor_engine) = serving.engines.get(&predecessor) else {
            return Err(LocalSqlSplitError::RemoteRange);
        };
        let Some(coordinator) = serving.engines.get(&RangeId::COORDINATOR) else {
            return Err(LocalSqlSplitError::RemoteRange);
        };
        let table = crabka_pgcatalog::list_tables(coordinator.catalog_kv())?
            .into_iter()
            .find(|table| routing_table_id(&table.name) == table_id)
            .ok_or(LocalSqlSplitError::MissingTable(table_id))?;
        if table.sharded || table.sharding.is_some() || table.foreign.is_some() {
            return Err(LocalSqlSplitError::UnsupportedTableKind(table_id));
        }
        if !crabka_pgcatalog::list_table_indexes(coordinator.catalog_kv(), &table.name)?.is_empty()
        {
            return Err(LocalSqlSplitError::IndexedTable(table_id));
        }
        if !predecessor_engine
            .kv_handle()
            .scan_prefix(&crabka_pgkv::key::table_prefix(table.id))?
            .is_empty()
        {
            return Err(LocalSqlSplitError::NonEmptyTable(table_id));
        }
        let sequence = predecessor_engine
            .kv_handle()
            .get(&crabka_pgkv::key::seq_key(table.id))?
            .ok_or(LocalSqlSplitError::AllocatedRowIds(table_id))?;
        if sequence != 1_u64.to_be_bytes() {
            return Err(LocalSqlSplitError::AllocatedRowIds(table_id));
        }
        Ok(())
    }
}

/// Settle every descriptor recovered from range 0 before this tenant can serve.
/// A pending descriptor is abort-won under range 0's descriptor lock; a durable
/// commit is physically replayed with the operations stored in that descriptor.
fn recover_durable_timestamp_transactions(
    engines: &BTreeMap<RangeId, SqlEngine>,
) -> Result<(), TenantError> {
    let Some(coordinator) = engines.get(&RangeId::COORDINATOR) else {
        return Ok(());
    };
    let descriptors = coordinator
        .timestamp_transaction_descriptors()
        .map_err(|error| TenantError::TimestampRecovery(format!("{error:?}")))?;
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| TenantError::TimestampRecovery(error.to_string()))?
                    .block_on(async {
                        for descriptor in descriptors {
                            let decision = coordinator
                                .recover_timestamp_transaction(descriptor.start_ts)
                                .await
                                .map_err(|error| TenantError::TimestampRecovery(format!("{error:?}")))?;
                            for range_id in descriptor.participants {
                                let range_id = RangeId::new(range_id);
                                let participant = engines.get(&range_id).ok_or_else(|| {
                                    TenantError::TimestampRecovery(format!(
                                        "cannot recover timestamp transaction {}: participant r{range_id} is not hosted",
                                        descriptor.start_ts.get()
                                    ))
                                })?;
                                let identity = crabka_pgexec::TimestampTxnIdentity {
                                    start_ts: descriptor.start_ts,
                                    global_xid: descriptor.global_xid,
                                    primary_range: RangeId::COORDINATOR.as_u32(),
                                };
                                let operations = descriptor
                                    .operations
                                    .iter()
                                    .copied()
                                    .filter(|operation| operation.range_id == range_id.as_u32())
                                    .collect::<Vec<_>>();
                                let resolve_result = if decision == crabka_pgexec::PrimaryTxnDecision::Aborted {
                                    participant
                                        .abort_timestamp_transaction_intents(descriptor.start_ts)
                                        .await
                                } else {
                                    participant
                                        .resolve_timestamp_transaction_operations(
                                            range_id.as_u32(),
                                            identity,
                                            decision,
                                            &operations,
                                        )
                                        .await
                                };
                                resolve_result.map_err(|error| {
                                        TenantError::TimestampRecovery(format!("{error:?}"))
                                    })?;
                            }
                        }
                        Ok(())
                    })
            })
            .join()
            .map_err(|_| TenantError::TimestampRecovery("recovery worker panicked".into()))?
    })
}

struct InProcessTsoRpc<C, H> {
    oracle: Arc<TsoOracle<C, H>>,
}

#[async_trait::async_trait]
impl<C, H> TsoRpc for InProcessTsoRpc<C, H>
where
    C: TsoHorizonCommitter + 'static,
    H: EpochHeartbeat + 'static,
{
    async fn grant(&self, count: NonZeroU64) -> Result<GrantLease, TsoError> {
        self.oracle.grant(count).await
    }
}

struct PgexecTsoOracle<R> {
    client: BatchedTsoClient<R>,
}

struct SharedTsoRpc(Arc<dyn TsoRpc>);

#[async_trait::async_trait]
impl TsoRpc for SharedTsoRpc {
    async fn grant(&self, count: NonZeroU64) -> Result<GrantLease, TsoError> {
        self.0.grant(count).await
    }
}

#[async_trait::async_trait]
impl<R> crabka_pgexec::TimestampOracle for PgexecTsoOracle<R>
where
    R: TsoRpc,
{
    async fn allocate_read_timestamp(
        &self,
    ) -> Result<crabka_pgexec::timestamp_txn::ReadTimestamp, crabka_pgexec::TimestampOracleError>
    {
        let timestamp = self.grant_one().await?;
        crabka_pgexec::timestamp_txn::ReadTimestamp::new(timestamp).map_err(Into::into)
    }

    async fn allocate_transaction_id(
        &self,
    ) -> Result<crabka_pgexec::TimestampTransactionId, crabka_pgexec::TimestampOracleError> {
        let timestamp = self.grant_one().await?;
        crabka_pgexec::TimestampTransactionId::new(timestamp).map_err(Into::into)
    }

    async fn allocate_commit_after(
        &self,
        start_ts: crabka_pgexec::TimestampTransactionId,
    ) -> Result<crabka_pgexec::CommitTimestamp, crabka_pgexec::TimestampOracleError> {
        let timestamp = self.grant_one().await?;
        crabka_pgexec::CommitTimestamp::after_start(start_ts, timestamp).map_err(Into::into)
    }
}

impl<R> PgexecTsoOracle<R>
where
    R: TsoRpc,
{
    async fn grant_one(&self) -> Result<u64, crabka_pgexec::TimestampOracleError> {
        let count = NonZeroU64::new(1).expect("one is non-zero");
        self.client
            .grant(count)
            .await
            .map(|lease| lease.first_ts.get())
            .map_err(|error| crabka_pgexec::TimestampOracleError::Unavailable(error.to_string()))
    }
}

fn install_range0_catalog(engines: &mut BTreeMap<RangeId, SqlEngine>) {
    let Some(catalog_kv) = engines.get(&RangeId::COORDINATOR).map(SqlEngine::kv_handle) else {
        return;
    };
    for (range_id, engine) in engines.iter_mut() {
        if range_id.is_coordinator() {
            continue;
        }
        engine.set_catalog_kv(Arc::clone(&catalog_kv));
    }
}

fn install_replica_catalog(
    engines: &mut BTreeMap<RangeId, SqlEngine>,
    replica: &ReadOnlyRange0Replica,
) {
    for engine in engines.values_mut() {
        engine.set_catalog_kv(Arc::clone(&replica.catalog_kv));
        engine.set_range0_barrier(replica.barrier.clone());
    }
}

/// Build a pgexec timestamp oracle from a recovered range-0 TSO horizon.
pub fn pgexec_timestamp_oracle_from_horizon<C, H>(
    committer: C,
    heartbeat: H,
    epoch: i16,
    persisted_max_ts: u64,
) -> Result<Arc<dyn crabka_pgexec::TimestampOracle>, TsoError>
where
    C: TsoHorizonCommitter + 'static,
    H: EpochHeartbeat + 'static,
{
    let stride = NonZeroU64::new(1024).expect("stride is non-zero");
    let oracle = Arc::new(TsoOracle::recover(
        committer,
        heartbeat,
        epoch,
        stride,
        persisted_max_ts,
    )?);
    let rpc: Arc<dyn TsoRpc> = Arc::new(InProcessTsoRpc { oracle });
    Ok(pgexec_timestamp_oracle_from_rpc(rpc))
}

/// Adapt a range-0 TSO RPC client into pgexec's timestamp oracle seam.
#[must_use]
pub fn pgexec_timestamp_oracle_from_rpc(
    rpc: Arc<dyn TsoRpc>,
) -> Arc<dyn crabka_pgexec::TimestampOracle> {
    Arc::new(PgexecTsoOracle {
        client: BatchedTsoClient::new(Arc::new(SharedTsoRpc(rpc))),
    })
}

/// Build the in-process RPC endpoint backed by a recovered durable horizon.
pub fn tso_rpc_from_horizon<C, H>(
    committer: C,
    heartbeat: H,
    epoch: i16,
    persisted_max_ts: u64,
) -> Result<Arc<dyn TsoRpc>, TsoError>
where
    C: TsoHorizonCommitter + 'static,
    H: EpochHeartbeat + 'static,
{
    let stride = NonZeroU64::new(1024).expect("stride is non-zero");
    let oracle = Arc::new(TsoOracle::recover(
        committer,
        heartbeat,
        epoch,
        stride,
        persisted_max_ts,
    )?);
    Ok(Arc::new(InProcessTsoRpc { oracle }))
}

fn install_memory_timestamp_oracle(
    engines: &mut BTreeMap<RangeId, SqlEngine>,
) -> Result<(), TenantError> {
    let Some(coordinator) = engines.get(&RangeId::COORDINATOR) else {
        return Ok(());
    };

    let horizon = MemoryTsoHorizon::new(coordinator.kv_handle(), 1);
    let persisted_max_ts = horizon
        .load_max_ts()
        .map_err(TenantError::TimestampOracle)?;
    let timestamp_oracle =
        pgexec_timestamp_oracle_from_horizon(horizon.clone(), horizon, 1, persisted_max_ts)
            .map_err(TenantError::TimestampOracle)?;

    install_timestamp_oracle(engines, &timestamp_oracle);
    Ok(())
}

fn install_timestamp_oracle(
    engines: &mut BTreeMap<RangeId, SqlEngine>,
    timestamp_oracle: &Arc<dyn crabka_pgexec::TimestampOracle>,
) {
    for engine in engines.values_mut() {
        engine.set_timestamp_oracle(Arc::clone(timestamp_oracle));
    }
}

struct UnavailableRange0TimestampOracle;

#[async_trait::async_trait]
impl crabka_pgexec::TimestampOracle for UnavailableRange0TimestampOracle {
    async fn allocate_read_timestamp(
        &self,
    ) -> Result<crabka_pgexec::timestamp_txn::ReadTimestamp, crabka_pgexec::TimestampOracleError>
    {
        Err(timestamp_oracle_unavailable())
    }

    async fn allocate_transaction_id(
        &self,
    ) -> Result<crabka_pgexec::TimestampTransactionId, crabka_pgexec::TimestampOracleError> {
        Err(timestamp_oracle_unavailable())
    }

    async fn allocate_commit_after(
        &self,
        _start_ts: crabka_pgexec::TimestampTransactionId,
    ) -> Result<crabka_pgexec::CommitTimestamp, crabka_pgexec::TimestampOracleError> {
        Err(timestamp_oracle_unavailable())
    }
}

fn timestamp_oracle_unavailable() -> crabka_pgexec::TimestampOracleError {
    crabka_pgexec::TimestampOracleError::Unavailable(
        "range-0 timestamp oracle is unavailable on an rN-only compute".into(),
    )
}

fn install_unavailable_timestamp_oracle(engines: &mut BTreeMap<RangeId, SqlEngine>) {
    let timestamp_oracle: Arc<dyn crabka_pgexec::TimestampOracle> =
        Arc::new(UnavailableRange0TimestampOracle);
    install_timestamp_oracle(engines, &timestamp_oracle);
}

struct InProcessRangeScanner {
    engines: BTreeMap<RangeId, SqlEngine>,
    range_map: RangeMap,
}

impl crabka_pgexec::RangeScanner for InProcessRangeScanner {
    fn scan(
        &self,
        request: crabka_pgexec::ScanRequest<'_>,
    ) -> Result<Vec<crabka_pgexec::ScannedRow>, ExecError> {
        if !request.table.sharded {
            return crabka_pgexec::RangeScanner::scan(&crabka_pgexec::LocalRangeScanner, request);
        }
        if request.read_ts.is_none() {
            return Err(ExecError::Unsupported(
                "sharded scatter scans require a finite statement read timestamp".into(),
            ));
        }
        let table_id = routing_table_id(&request.table.name);
        let hash_segments = hash_scan_segments(&self.range_map, request.table, &request)?;

        let segments = match hash_segments {
            Some(segments) => segments,
            None => self
                .range_map
                .scan_segments(table_id, map_interval_from_exec(request.interval))
                .map_err(|error| ExecError::Unsupported(error.to_string()))?,
        };

        let mut rows = Vec::new();
        for segment in segments {
            let Some(engine) = self.engines.get(&segment.range_id) else {
                return Err(ExecError::Unsupported(format!(
                    "range r{} is required for sharded scan but is not hosted",
                    segment.range_id
                )));
            };
            let local_rows = engine.scan_local_visible(
                request.table,
                request.global_snapshot,
                request.snapshot,
                request.own_xid,
                request.read_ts,
                local_scan_interval(request.interval, segment.interval),
            )?;
            rows.extend(crabka_pgexec::scanner::apply_executable_scan_pushdown(
                local_rows,
                &request.predicate,
                &request.projection,
                request.partial_aggregate.as_ref(),
                request.top_k.as_ref(),
            )?);
        }
        if let Some(spec) = request.partial_aggregate.as_ref() {
            rows = crabka_pgexec::scanner::merge_partial_aggregate_rows(rows, spec)?;
        } else if let Some(spec) = request.top_k.as_ref() {
            crabka_pgexec::scanner::apply_top_k_pushdown(&mut rows, spec)?;
        } else {
            rows.sort_by_key(|row| (row.rowid, row.xmin));
        }
        Ok(rows)
    }
}

fn hash_scan_segments(
    range_map: &RangeMap,
    table: &crabka_pgcatalog::Table,
    request: &crabka_pgexec::ScanRequest<'_>,
) -> Result<Option<Vec<RangeScanSegment>>, ExecError> {
    let Some(ShardingStrategy::Hash(hash)) = table.sharding.as_ref() else {
        return Ok(None);
    };
    let Some(hash_value) = hash_equality_value(table, hash, &request.predicate) else {
        return Ok(None);
    };

    let table_id = routing_table_id(&table.name);
    let spec = HashShardSpec::new(
        table_id,
        hash.columns.clone(),
        hash.buckets,
        hash.co_location_group.clone(),
    )
    .map_err(|error| ExecError::Unsupported(error.to_string()))?;
    let bucket = spec.bucket_for_value(hash_value);
    range_map
        .scan_hash_bucket_segments(table_id, bucket, map_interval_from_exec(request.interval))
        .map(Some)
        .map_err(|error| ExecError::Unsupported(error.to_string()))
}

fn hash_equality_value(
    table: &crabka_pgcatalog::Table,
    hash: &crabka_pgcatalog::HashSharding,
    predicate: &PredicatePushdown,
) -> Option<Vec<u8>> {
    let PredicatePushdown::Conjunctive(predicates) = predicate else {
        return None;
    };

    let mut bytes = Vec::new();
    for column in &hash.columns {
        let column_index = table.column_index(column)?;
        let value = predicates
            .iter()
            .find(|predicate| predicate.column == column_index && predicate.op == PredicateOp::Eq)
            .map(|predicate| &predicate.value)?;
        bytes.extend(datum_hash_bytes(value)?);
    }
    Some(bytes)
}

fn map_interval_from_exec(interval: crabka_pgexec::RowInterval) -> MapRowInterval {
    MapRowInterval {
        start: interval.start,
        end: interval.end,
    }
}

fn exec_interval_from_map(interval: MapRowInterval) -> crabka_pgexec::RowInterval {
    crabka_pgexec::RowInterval {
        start: interval.start,
        end: interval.end,
    }
}

fn local_scan_interval(
    requested: crabka_pgexec::RowInterval,
    segment: MapRowInterval,
) -> crabka_pgexec::RowInterval {
    if requested == crabka_pgexec::RowInterval::ALL {
        return crabka_pgexec::RowInterval::ALL;
    }
    exec_interval_from_map(segment)
}

fn open_range_engine(
    data_dir: Option<&PathBuf>,
    range_id: RangeId,
) -> Result<SqlEngine, ExecError> {
    let Some(parent) = data_dir else {
        return Ok(SqlEngine::new());
    };
    SqlEngine::open(parent.join(format!("r{}", range_id.as_u32())))
}

fn range_client_for_registry(
    config: &MultiRangeTenantConfig,
) -> Result<Option<FramedTcpClient>, TenantError> {
    config
        .range_registry
        .as_ref()
        .map(|_| {
            config
                .range_client
                .clone()
                .ok_or(TenantError::MissingRangeTls)
        })
        .transpose()
}

impl Engine for MultiRangeTenant {
    type Session = GatewaySession;

    fn connect(&self) -> Self::Session {
        let serving = self.inner.serving.load_full();
        let sessions = serving
            .engines
            .iter()
            .map(|(range_id, engine)| (*range_id, engine.connect()))
            .collect();
        GatewaySession {
            inner: Arc::clone(&self.inner),
            sessions,
            remote_sessions: BTreeMap::new(),
            serving_epoch: serving.range_map.epoch(),
            explicit_transaction: false,
            transaction: GatewayTransaction::Idle,
            status: TxStatus::Idle,
            prepared: BTreeMap::new(),
            portals: BTreeMap::new(),
        }
    }
}

struct TenantInner {
    tenant: TenantName,
    serving: ArcSwap<ServingSnapshot>,
    remote_forward: Option<Arc<dyn RemoteForward>>,
    coordinator: LocalCoordinator,
    route_log: Mutex<Vec<RouteRecord>>,
    data_dir: Option<PathBuf>,
    split_states: Mutex<BTreeMap<String, SplitState>>,
    split_lock: Mutex<()>,
    schema_gate: Arc<Mutex<()>>,
    table_write_gates: StdMutex<BTreeMap<TableId, Arc<Mutex<()>>>>,
    active_explicit_transactions: AtomicUsize,
    empty_table_split_test_hook: Option<EmptyTableSplitTestHook>,
    commit_fault_for_testing: Option<Arc<StdMutex<Option<GatewayCommitFault>>>>,
}

struct PopulatedTransferTable {
    predecessor: RangeId,
    physical_table_id: u32,
}

/// Owns a successful source pause until the transfer either resumes it or is dropped.
struct TransferPauseGuard<'a> {
    transfer: &'a dyn RangeTransferCapability,
    barrier: Option<crate::RangeTransferBarrier>,
}

impl<'a> TransferPauseGuard<'a> {
    fn new(
        transfer: &'a dyn RangeTransferCapability,
        barrier: crate::RangeTransferBarrier,
    ) -> Self {
        Self {
            transfer,
            barrier: Some(barrier),
        }
    }

    async fn resume(mut self) -> Result<(), RangeTransferError> {
        let barrier = self
            .barrier
            .expect("transfer pause guard must hold a barrier");
        self.transfer.resume(barrier).await?;
        self.barrier = None;
        Ok(())
    }
}

impl Drop for TransferPauseGuard<'_> {
    fn drop(&mut self) {
        if let Some(barrier) = self.barrier {
            self.transfer.resume_after_drop(barrier);
        }
    }
}

impl TenantInner {
    fn table_write_gate(&self, table_id: TableId) -> Arc<Mutex<()>> {
        let mut table_write_gates = self
            .table_write_gates
            .lock()
            .expect("table write gate lock must not be poisoned");
        table_write_gates
            .entry(table_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

/// Atomically published serving state. A published map never names an engine that is not ready.
struct ServingSnapshot {
    range_map: RangeMap,
    engines: BTreeMap<RangeId, SqlEngine>,
    ready: BTreeSet<RangeId>,
    keepalives: BTreeMap<RangeId, Arc<dyn std::any::Any + Send + Sync>>,
}

impl ServingSnapshot {
    fn ready(range_map: RangeMap, engines: BTreeMap<RangeId, SqlEngine>) -> Self {
        Self::ready_with_keepalives(range_map, engines, BTreeMap::new())
    }

    fn ready_with_keepalives(
        range_map: RangeMap,
        engines: BTreeMap<RangeId, SqlEngine>,
        keepalives: BTreeMap<RangeId, Arc<dyn std::any::Any + Send + Sync>>,
    ) -> Self {
        let ready = engines.keys().copied().collect();
        Self {
            range_map,
            engines,
            ready,
            keepalives,
        }
    }

    fn engine(&self, range_id: RangeId) -> Option<&SqlEngine> {
        self.ready
            .contains(&range_id)
            .then(|| self.engines.get(&range_id))
            .flatten()
    }
}

struct LocalSqlSplitBridge<'a> {
    tenant: &'a MultiRangeTenant,
}

#[async_trait::async_trait]
impl SplitStateStore for LocalSqlSplitBridge<'_> {
    async fn load_split_state(&self, operation_id: &str) -> Result<Option<SplitState>, SplitError> {
        Ok(self
            .tenant
            .inner
            .split_states
            .lock()
            .await
            .get(operation_id)
            .cloned())
    }

    async fn save_split_state(&self, state: &SplitState) -> Result<(), SplitError> {
        self.tenant
            .inner
            .split_states
            .lock()
            .await
            .insert(state.operation_id.clone(), state.clone());
        Ok(())
    }
}

#[async_trait::async_trait]
impl SplitHooks for LocalSqlSplitBridge<'_> {
    async fn pause_conversion_writes(&self, _state: &SplitState) -> Result<(), SplitError> {
        Err(SplitError::Hook(
            "local SQL split does not convert tables".to_owned(),
        ))
    }

    async fn force_predecessor_checkpoint(
        &self,
        state: &SplitState,
    ) -> Result<CheckpointManifest, SplitError> {
        Ok(CheckpointManifest {
            range_id: state.predecessor,
            covered_offset: 0,
            manifest_key: "local-empty-table-no-data-migration".to_owned(),
        })
    }

    async fn force_right_predecessor_checkpoint(
        &self,
        _state: &SplitState,
    ) -> Result<CheckpointManifest, SplitError> {
        Err(SplitError::Hook(
            "local SQL split does not merge ranges".to_owned(),
        ))
    }

    async fn pause_writes_at_covered_offset(
        &self,
        _state: &SplitState,
        _checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError> {
        Ok(())
    }

    async fn commit_map_version(&self, _state: &SplitState) -> Result<(), SplitError> {
        // The durable orchestration step precedes publication. The gateway keeps serving the old
        // snapshot until the successor has been constructed and prologued below.
        Ok(())
    }

    async fn start_successor_restore(
        &self,
        _state: &SplitState,
        _checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError> {
        // The restrictions prove there is no physical data to restore.
        Ok(())
    }

    async fn start_merge_successor_restore(
        &self,
        _state: &SplitState,
        _left_checkpoint: &CheckpointManifest,
        _right_checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError> {
        Err(SplitError::Hook(
            "local SQL split does not merge ranges".to_owned(),
        ))
    }

    async fn successor_fence_prologue(&self, state: &SplitState) -> Result<(), SplitError> {
        let serving = self.tenant.inner.serving.load_full();
        if serving.range_map == state.target_map {
            return Ok(());
        }
        let successor = open_range_engine(self.tenant.inner.data_dir.as_ref(), state.successor)
            .map_err(|error| {
                SplitError::Hook(format!("successor r{} engine: {error:?}", state.successor))
            })?;
        let coordinator = serving.engines.get(&RangeId::COORDINATOR).ok_or_else(|| {
            SplitError::Hook("local SQL split requires hosted range r0".to_owned())
        })?;
        let mut successor = successor;
        successor.set_catalog_kv(coordinator.kv_handle());
        coordinator.share_gtm_to(&mut successor);
        successor.set_timestamp_oracle(coordinator.timestamp_oracle_handle());
        successor.set_range_scanner(coordinator.range_scanner_handle());
        if let Some(scanner) = coordinator.foreign_scanner_handle() {
            successor.set_foreign_scanner(scanner);
        }

        let mut engines = serving
            .engines
            .iter()
            .map(|(range_id, engine)| (*range_id, engine.clone_handle()))
            .collect::<BTreeMap<_, _>>();
        engines.insert(state.successor, successor);
        // The sole serving publication is after the successor is fully initialized. A reader sees
        // either the old map/engine set or this complete new set, never a mixed pair.
        self.tenant
            .inner
            .serving
            .store(Arc::new(ServingSnapshot::ready(
                state.target_map.clone(),
                engines,
            )));
        Ok(())
    }

    async fn inherit_in_doubt_markers(
        &self,
        _state: &SplitState,
    ) -> Result<Vec<crate::InDoubtMarker>, SplitError> {
        Ok(Vec::new())
    }

    async fn park_predecessor(&self, _state: &SplitState) -> Result<(), SplitError> {
        // Map publication already removes the moved table from predecessor routing.
        Ok(())
    }

    async fn park_right_predecessor(&self, _state: &SplitState) -> Result<(), SplitError> {
        Err(SplitError::Hook(
            "local SQL split does not merge ranges".to_owned(),
        ))
    }

    async fn unpause_serving(&self, _state: &SplitState) -> Result<(), SplitError> {
        Ok(())
    }
}

/// One statement routed by the gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteRecord {
    /// SQL verb category.
    pub kind: StatementKind,
    /// Target range selected by the router.
    pub range_id: RangeId,
    /// Table id parsed at the boundary, if the statement named a table.
    pub table_id: Option<TableId>,
}

/// Statement category understood by the in-process router.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementKind {
    /// Transaction start.
    Begin,
    /// Transaction commit.
    Commit,
    /// Transaction rollback.
    Rollback,
    /// DDL routed through range 0.
    Ddl,
    /// DML routed through the owning data range.
    Dml,
    /// Read routed like DML when it names one table.
    Query,
    /// Statement without a table route.
    Local,
}

pub struct GatewaySession {
    inner: Arc<TenantInner>,
    sessions: BTreeMap<RangeId, crabka_pgexec::SqlSession>,
    remote_sessions: BTreeMap<RangeId, RemoteRangeSession>,
    serving_epoch: MapEpoch,
    explicit_transaction: bool,
    transaction: GatewayTransaction,
    status: TxStatus,
    prepared: BTreeMap<String, GatewayPrepared>,
    portals: BTreeMap<String, GatewayPortal>,
}

#[derive(Debug, Clone)]
struct GatewayPrepared {
    sql: String,
    route: StatementRoute,
    description: PreparedDescription,
}

#[derive(Debug, Clone)]
struct GatewayPortal {
    sql: String,
    route: StatementRoute,
    description: PortalDescription,
    gateway_execution: Option<ExecuteOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum GatewayTransaction {
    Idle,
    Open {
        touched: Vec<RangeId>,
        escalated: bool,
    },
    Failed {
        touched: Vec<RangeId>,
        escalated: bool,
        recovery: Option<GlobalCommitRecovery>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StatementRoute {
    kind: StatementKind,
    range_id: RangeId,
    table_id: Option<TableId>,
    scatter_ranges: Option<Vec<RangeId>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PreparedParticipantRelease {
    range_id: RangeId,
    global_xid: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GlobalCommitRecovery {
    global_xid: u64,
    prepared: Vec<PreparedParticipantRelease>,
    decision: Option<TransactionDecision>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GlobalCommitError {
    error: PgError,
    recovery: Option<GlobalCommitRecovery>,
}

impl From<PgError> for GlobalCommitError {
    fn from(error: PgError) -> Self {
        Self {
            error,
            recovery: None,
        }
    }
}

impl Session for GatewaySession {
    async fn simple_query(&mut self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        let mut all_results = Vec::new();
        for statement in split_statements(sql) {
            let results = self.execute_one(statement).await?;
            all_results.extend(results);
        }
        if all_results.is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        Ok(all_results)
    }

    async fn simple_query_into<S: crabka_pgwire::engine::ResultSink>(
        &mut self,
        sql: &str,
        page_rows: usize,
        sink: &mut S,
    ) -> Result<(), PgError> {
        if page_rows == 0 {
            return Err(PgError::protocol(
                "result page size must be greater than zero",
            ));
        }
        let statements = split_statements(sql).collect::<Vec<_>>();
        if statements.is_empty() {
            return sink
                .send(crabka_pgwire::engine::ResultPage::Empty { result_index: 0 })
                .await;
        }
        let mut result_index = 0usize;
        for statement in statements {
            self.current_serving()?;
            self.reject_statement_in_failed_transaction(statement)?;
            let route = self.route_statement(statement)?;
            let remote_streamable = matches!(self.transaction, GatewayTransaction::Idle)
                && route.kind == StatementKind::Query
                && route.scatter_ranges.is_none()
                && !self.sessions.contains_key(&route.range_id);
            if remote_streamable {
                self.inner.route_log.lock().await.push(RouteRecord {
                    kind: route.kind,
                    range_id: route.range_id,
                    table_id: route.table_id,
                });
                let forward = self.inner.remote_forward.clone().ok_or_else(|| {
                    PgError::error(
                        sqlstate::FEATURE_NOT_SUPPORTED,
                        format!(
                            "range r{} is not hosted by tenant {}",
                            route.range_id, self.inner.tenant
                        ),
                    )
                })?;
                let mut offset = OffsetResultSink {
                    inner: sink,
                    offset: result_index,
                    completed: 0,
                };
                forward
                    .forward_query_into(route.range_id, statement.to_owned(), &mut offset)
                    .await
                    .map_err(ForwardError::into_pg)?;
                result_index = result_index.saturating_add(offset.completed);
                continue;
            }

            for result in self.execute_one(statement).await? {
                send_gateway_result(sink, result_index, page_rows, result).await?;
                result_index = result_index.saturating_add(1);
            }
        }
        Ok(())
    }

    async fn parse(
        &mut self,
        name: &str,
        sql: &str,
        parameter_types: &[u32],
    ) -> Result<PreparedDescription, PgError> {
        let result = self.parse_inner(name, sql, parameter_types).await;
        self.finish_statement(result)
    }

    async fn bind(
        &mut self,
        portal: &str,
        statement: &str,
        params: &[BoundParam],
        result_formats: &[i16],
    ) -> Result<PortalDescription, PgError> {
        let result = self
            .bind_inner(portal, statement, params, result_formats)
            .await;
        self.finish_statement(result)
    }

    async fn describe_statement(&mut self, name: &str) -> Result<PreparedDescription, PgError> {
        self.current_serving()?;
        self.prepared
            .get(name)
            .map(|prepared| prepared.description.clone())
            .ok_or_else(|| missing_statement(name))
    }

    async fn describe_portal(&mut self, name: &str) -> Result<PortalDescription, PgError> {
        self.current_serving()?;
        self.portals
            .get(name)
            .map(|portal| portal.description.clone())
            .ok_or_else(|| missing_portal(name))
    }

    async fn execute(&mut self, portal: &str, max_rows: u32) -> Result<ExecuteOutcome, PgError> {
        let result = self.execute_portal_inner(portal, max_rows).await;
        self.finish_statement(result)
    }

    async fn close(&mut self, target: CloseTarget<'_>) -> Result<(), PgError> {
        match target {
            CloseTarget::Statement(name) => {
                if let Some(prepared) = self.prepared.remove(name) {
                    self.close_on_range(prepared.route.range_id, CloseTarget::Statement(name))
                        .await?;
                }
            }
            CloseTarget::Portal(name) => {
                if let Some(portal) = self.portals.remove(name) {
                    self.close_on_range(portal.route.range_id, CloseTarget::Portal(name))
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn sync(&mut self) -> Result<(), PgError> {
        for session in self.sessions.values_mut() {
            session.sync().await?;
        }
        for session in self.remote_sessions.values_mut() {
            self.status = session.sync().await?;
        }
        self.portals.clear();
        Ok(())
    }

    fn tx_status(&self) -> TxStatus {
        self.status
    }
}

struct OffsetResultSink<'a, S> {
    inner: &'a mut S,
    offset: usize,
    completed: usize,
}

#[async_trait::async_trait]
impl<S: crabka_pgwire::engine::ResultSink> crabka_pgwire::engine::ResultSink
    for OffsetResultSink<'_, S>
{
    async fn send(&mut self, mut page: crabka_pgwire::engine::ResultPage) -> Result<(), PgError> {
        let (index, terminal) = match &mut page {
            crabka_pgwire::engine::ResultPage::Rows {
                result_index, tag, ..
            } => (result_index, tag.is_some()),
            crabka_pgwire::engine::ResultPage::Command { result_index, .. }
            | crabka_pgwire::engine::ResultPage::Empty { result_index } => (result_index, true),
        };
        *index = index
            .checked_add(self.offset)
            .ok_or_else(|| PgError::error("54000", "remote result index exceeds capacity"))?;
        if terminal {
            self.completed = self.completed.saturating_add(1);
        }
        self.inner.send(page).await
    }
}

async fn send_gateway_result<S: crabka_pgwire::engine::ResultSink>(
    sink: &mut S,
    result_index: usize,
    page_rows: usize,
    result: QueryResult,
) -> Result<(), PgError> {
    use crabka_pgwire::engine::ResultPage;
    match result {
        QueryResult::Rows { fields, rows, tag } => {
            if rows.is_empty() {
                return sink
                    .send(ResultPage::Rows {
                        result_index,
                        fields: Some(fields),
                        rows,
                        tag: Some(tag),
                    })
                    .await;
            }
            let chunks = rows.len().div_ceil(page_rows);
            let mut fields = Some(fields);
            for (index, rows) in rows.chunks(page_rows).enumerate() {
                sink.send(ResultPage::Rows {
                    result_index,
                    fields: fields.take(),
                    rows: rows.to_vec(),
                    tag: (index + 1 == chunks).then(|| tag.clone()),
                })
                .await?;
            }
            Ok(())
        }
        QueryResult::Command { tag } => sink.send(ResultPage::Command { result_index, tag }).await,
        QueryResult::Empty => sink.send(ResultPage::Empty { result_index }).await,
    }
}

impl GatewaySession {
    async fn ensure_remote_session(&mut self, range_id: RangeId) -> Result<(), PgError> {
        if self.remote_sessions.contains_key(&range_id) {
            return Ok(());
        }
        let forward = self.inner.remote_forward.clone().ok_or_else(|| {
            PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                format!(
                    "range r{range_id} is not hosted by tenant {}",
                    self.inner.tenant
                ),
            )
        })?;
        let session = forward
            .open_session(range_id)
            .await
            .map_err(ForwardError::into_pg)?;
        self.remote_sessions.insert(range_id, session);
        Ok(())
    }

    async fn close_on_range(
        &mut self,
        range_id: RangeId,
        target: CloseTarget<'_>,
    ) -> Result<(), PgError> {
        if let Some(session) = self.sessions.get_mut(&range_id) {
            session.close(target).await
        } else {
            self.ensure_remote_session(range_id).await?;
            self.remote_sessions
                .get_mut(&range_id)
                .expect("remote session inserted")
                .close(target)
                .await
        }
    }

    async fn simple_on_range(
        &mut self,
        range_id: RangeId,
        sql: &str,
    ) -> Result<Vec<QueryResult>, PgError> {
        if let Some(session) = self.sessions.get_mut(&range_id) {
            session.simple_query(sql).await
        } else {
            self.ensure_remote_session(range_id).await?;
            self.remote_sessions
                .get_mut(&range_id)
                .expect("remote session inserted")
                .simple_query(sql.to_owned())
                .await
        }
    }

    async fn prepare_on_range(
        &mut self,
        range_id: RangeId,
        global_xid: u64,
    ) -> Result<u64, PgError> {
        if let Some(session) = self.sessions.get_mut(&range_id) {
            session
                .prepare_global_participant(global_xid)
                .await
                .map_err(ExecError::into_pg)
        } else {
            self.ensure_remote_session(range_id).await?;
            self.remote_sessions
                .get_mut(&range_id)
                .expect("remote session inserted")
                .prepare_global(global_xid)
                .await
        }
    }

    async fn release_on_range(
        &mut self,
        range_id: RangeId,
        global_xid: u64,
        commit: bool,
    ) -> Result<(), PgError> {
        if let Some(session) = self.sessions.get_mut(&range_id) {
            if commit {
                session.release_global_participant_commit(global_xid).await
            } else {
                session.release_global_participant_abort(global_xid).await
            }
            .map_err(ExecError::into_pg)
        } else {
            self.ensure_remote_session(range_id).await?;
            let session = self
                .remote_sessions
                .get_mut(&range_id)
                .expect("remote session inserted");
            session
                .record_global_decision(
                    global_xid,
                    if commit {
                        crabka_pgmvcc::clog::XidStatus::Committed
                    } else {
                        crabka_pgmvcc::clog::XidStatus::Aborted
                    },
                )
                .await?;
            session.release_global(global_xid, commit).await
        }
    }

    async fn parse_inner(
        &mut self,
        name: &str,
        sql: &str,
        parameter_types: &[u32],
    ) -> Result<PreparedDescription, PgError> {
        self.current_serving()?;
        self.reject_statement_in_failed_transaction(sql)?;
        if !name.is_empty() && self.prepared.contains_key(name) {
            return Err(PgError::error(
                sqlstate::DUPLICATE_PREPARED_STATEMENT,
                format!("prepared statement \"{name}\" already exists"),
            ));
        }
        if name.is_empty()
            && let Some(old) = self.prepared.remove(name)
        {
            let old_range = old.route.range_id;
            self.close_on_range(old_range, CloseTarget::Statement(name))
                .await?;
        }
        let route = self.route_statement(sql)?;
        if route.scatter_ranges.is_some() {
            return Err(PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                "extended statements must target one range",
            ));
        }
        if matches!(
            route.kind,
            StatementKind::Begin | StatementKind::Commit | StatementKind::Rollback
        ) {
            if !parameter_types.is_empty() {
                return Err(PgError::error("42P02", "there is no parameter $1"));
            }
            let description = PreparedDescription {
                parameter_types: Vec::new(),
                fields: Vec::new(),
            };
            self.prepared.insert(
                name.to_owned(),
                GatewayPrepared {
                    sql: sql.to_owned(),
                    route,
                    description: description.clone(),
                },
            );
            return Ok(description);
        }
        let description = if let Some(session) = self.sessions.get_mut(&route.range_id) {
            session.parse(name, sql, parameter_types).await?
        } else {
            self.ensure_remote_session(route.range_id).await?;
            self.remote_sessions
                .get_mut(&route.range_id)
                .expect("remote session inserted")
                .parse(name.to_owned(), sql.to_owned(), parameter_types.to_vec())
                .await?
        };
        self.prepared.insert(
            name.to_owned(),
            GatewayPrepared {
                sql: sql.to_owned(),
                route,
                description: description.clone(),
            },
        );
        Ok(description)
    }

    async fn bind_inner(
        &mut self,
        portal: &str,
        statement: &str,
        params: &[BoundParam],
        result_formats: &[i16],
    ) -> Result<PortalDescription, PgError> {
        self.current_serving()?;
        if portal.is_empty()
            && let Some(old) = self.portals.remove(portal)
        {
            let old_range = old.route.range_id;
            self.close_on_range(old_range, CloseTarget::Portal(portal))
                .await?;
        }
        let prepared = self
            .prepared
            .get(statement)
            .cloned()
            .ok_or_else(|| missing_statement(statement))?;
        if !portal.is_empty() && self.portals.contains_key(portal) {
            return Err(PgError::error(
                sqlstate::DUPLICATE_CURSOR,
                format!("cursor \"{portal}\" already exists"),
            ));
        }
        if matches!(
            prepared.route.kind,
            StatementKind::Begin | StatementKind::Commit | StatementKind::Rollback
        ) {
            if !params.is_empty() {
                return Err(PgError::protocol(format!(
                    "bind message supplies {} parameters, but prepared statement requires 0",
                    params.len()
                )));
            }
            let description = PortalDescription { fields: Vec::new() };
            self.portals.insert(
                portal.to_owned(),
                GatewayPortal {
                    sql: prepared.sql,
                    route: prepared.route,
                    description: description.clone(),
                    gateway_execution: None,
                },
            );
            return Ok(description);
        }
        let description = if let Some(session) = self.sessions.get_mut(&prepared.route.range_id) {
            session
                .bind(portal, statement, params, result_formats)
                .await?
        } else {
            self.ensure_remote_session(prepared.route.range_id).await?;
            self.remote_sessions
                .get_mut(&prepared.route.range_id)
                .expect("remote session inserted")
                .bind(
                    portal.to_owned(),
                    statement.to_owned(),
                    params,
                    result_formats.to_vec(),
                )
                .await?
        };
        self.portals.insert(
            portal.to_owned(),
            GatewayPortal {
                sql: prepared.sql,
                route: prepared.route,
                description: description.clone(),
                gateway_execution: None,
            },
        );
        Ok(description)
    }

    async fn execute_portal_inner(
        &mut self,
        portal: &str,
        max_rows: u32,
    ) -> Result<ExecuteOutcome, PgError> {
        self.current_serving()?;
        let portal_state = self
            .portals
            .get(portal)
            .cloned()
            .ok_or_else(|| missing_portal(portal))?;
        match portal_state.route.kind {
            StatementKind::Begin
            | StatementKind::Commit
            | StatementKind::Rollback
            | StatementKind::Ddl => {
                if let Some(outcome) = portal_state.gateway_execution {
                    return Ok(outcome);
                }
                let mut results = self.execute_one(&portal_state.sql).await?;
                let result = results.pop().unwrap_or(QueryResult::Empty);
                let outcome = query_result_to_outcome(result);
                self.portals
                    .get_mut(portal)
                    .expect("portal exists throughout gateway execution")
                    .gateway_execution = Some(outcome.clone());
                return Ok(outcome);
            }
            StatementKind::Dml | StatementKind::Query | StatementKind::Local => {}
        }
        self.inner.route_log.lock().await.push(RouteRecord {
            kind: portal_state.route.kind,
            range_id: portal_state.route.range_id,
            table_id: portal_state.route.table_id,
        });
        let _table_write_gate = self.acquire_routed_dml_gate(&portal_state.route).await?;
        if portal_state.route.kind == StatementKind::Dml {
            self.touch_write_range(portal_state.route.range_id).await?;
        }
        if let Some(session) = self.sessions.get_mut(&portal_state.route.range_id) {
            session.execute(portal, max_rows).await
        } else {
            self.ensure_remote_session(portal_state.route.range_id)
                .await?;
            self.remote_sessions
                .get_mut(&portal_state.route.range_id)
                .expect("remote session inserted")
                .execute(portal.to_owned(), max_rows)
                .await
        }
    }

    fn current_serving(&self) -> Result<Arc<ServingSnapshot>, PgError> {
        let serving = self.inner.serving.load_full();
        if serving.range_map.epoch() != self.serving_epoch {
            return Err(PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                "range map changed; reconnect before issuing another statement",
            ));
        }
        Ok(serving)
    }

    async fn execute_one(&mut self, statement: &str) -> Result<Vec<QueryResult>, PgError> {
        let result = self.execute_one_inner(statement).await;
        self.finish_statement(result)
    }

    async fn execute_one_inner(&mut self, statement: &str) -> Result<Vec<QueryResult>, PgError> {
        self.current_serving()?;
        self.reject_statement_in_failed_transaction(statement)?;
        let route = self.route_statement(statement)?;
        self.inner.route_log.lock().await.push(RouteRecord {
            kind: route.kind,
            range_id: route.range_id,
            table_id: route.table_id,
        });
        let _table_write_gate = self.acquire_routed_dml_gate(&route).await?;

        if let Some(ranges) = route.scatter_ranges.clone() {
            return self
                .execute_timestamp_scatter(statement, ranges)
                .await
                .map(|result| vec![result]);
        }

        match route.kind {
            StatementKind::Begin => self.begin_transaction(),
            StatementKind::Commit => self.commit_transaction().await,
            StatementKind::Rollback => self.rollback_transaction().await,
            StatementKind::Ddl => self.execute_ddl(statement).await,
            StatementKind::Dml | StatementKind::Query | StatementKind::Local => {
                self.execute_routed_statement(statement, route.kind, route.range_id)
                    .await
            }
        }
    }

    fn begin_transaction(&mut self) -> Result<Vec<QueryResult>, PgError> {
        if !matches!(self.transaction, GatewayTransaction::Idle) {
            return Err(PgError::error("25001", "transaction already in progress"));
        }
        self.transaction = GatewayTransaction::Open {
            touched: Vec::new(),
            escalated: false,
        };
        self.explicit_transaction = true;
        self.inner
            .active_explicit_transactions
            .fetch_add(1, Ordering::AcqRel);
        self.status = TxStatus::InTransaction;
        Ok(vec![QueryResult::Command {
            tag: "BEGIN".to_string(),
        }])
    }

    async fn commit_transaction(&mut self) -> Result<Vec<QueryResult>, PgError> {
        let GatewayTransaction::Open { touched, escalated } = &self.transaction else {
            self.status = TxStatus::Idle;
            return Ok(vec![QueryResult::Command {
                tag: "COMMIT".to_string(),
            }]);
        };
        let touched = touched.clone();
        let escalated = *escalated;

        if touched.is_empty() {
            self.transaction = GatewayTransaction::Idle;
            self.status = TxStatus::Idle;
            self.release_explicit_transaction();
            return Ok(vec![QueryResult::Command {
                tag: "COMMIT".to_string(),
            }]);
        }

        if escalated {
            if let Err(error) = self.commit_global_transaction(touched.clone()).await {
                self.transaction = GatewayTransaction::Failed {
                    touched,
                    escalated,
                    recovery: error.recovery,
                };
                self.status = TxStatus::Failed;
                return Err(error.error);
            }
        } else {
            let [range_id] = touched.as_slice() else {
                self.transaction = GatewayTransaction::Failed {
                    touched,
                    escalated,
                    recovery: None,
                };
                self.status = TxStatus::Failed;
                return Err(PgError::error(
                    "XX000",
                    "single-range transaction reached commit with multiple participants",
                ));
            };
            if let Err(error) = self.simple_on_range(*range_id, "COMMIT").await {
                self.transaction = GatewayTransaction::Failed {
                    touched,
                    escalated,
                    recovery: None,
                };
                self.status = TxStatus::Failed;
                return Err(error);
            }
        }

        self.transaction = GatewayTransaction::Idle;
        self.release_explicit_transaction();
        self.status = TxStatus::Idle;
        Ok(vec![QueryResult::Command {
            tag: "COMMIT".to_string(),
        }])
    }

    async fn rollback_transaction(&mut self) -> Result<Vec<QueryResult>, PgError> {
        let previous = std::mem::replace(&mut self.transaction, GatewayTransaction::Idle);
        let Some((touched, escalated, recovery)) = transaction_participants(previous) else {
            self.status = TxStatus::Idle;
            return Ok(rollback_command_response());
        };
        if touched.is_empty() {
            self.status = TxStatus::Idle;
            self.release_explicit_transaction();
            return Ok(rollback_command_response());
        }

        if let Some(recovery) = recovery {
            if let Err(error) = self
                .cleanup_global_commit_recovery(&recovery, &touched)
                .await
            {
                self.transaction = GatewayTransaction::Failed {
                    touched,
                    escalated,
                    recovery: error.recovery,
                };
                self.status = TxStatus::Failed;
                return Err(error.error);
            }
            self.status = TxStatus::Idle;
            self.release_explicit_transaction();
            return Ok(rollback_command_response());
        }
        self.inner
            .coordinator
            .abort(touched.clone(), escalated)
            .await;
        for (index, range_id) in touched.iter().copied().enumerate() {
            if let Err(error) = self.simple_on_range(range_id, "ROLLBACK").await {
                self.transaction = GatewayTransaction::Failed {
                    touched: touched[index..].to_vec(),
                    escalated,
                    recovery: None,
                };
                self.status = TxStatus::Failed;
                return Err(error);
            }
        }
        self.status = TxStatus::Idle;
        self.release_explicit_transaction();
        Ok(rollback_command_response())
    }

    async fn commit_global_transaction(
        &mut self,
        touched: Vec<RangeId>,
    ) -> Result<(), GlobalCommitError> {
        let global_xid = self.begin_global_transaction(&touched).await?;
        let mut prepared = Vec::with_capacity(touched.len());

        for range_id in touched.iter().copied() {
            match self.prepare_on_range(range_id, global_xid).await {
                Ok(effective_global_xid) => {
                    self.inner
                        .coordinator
                        .prepare(global_xid, range_id)
                        .await
                        .map_err(|error| coordinator_error_to_pg(&error))?;
                    prepared.push(PreparedParticipantRelease {
                        range_id,
                        global_xid: effective_global_xid,
                    });
                    if effective_global_xid != global_xid {
                        self.abort_global_transaction(global_xid, &prepared, &touched)
                            .await
                            .map_err(|error| GlobalCommitError {
                                error,
                                recovery: Some(GlobalCommitRecovery {
                                    global_xid,
                                    prepared: prepared.clone(),
                                    decision: None,
                                }),
                            })?;
                        return Err(PgError::error(
                            sqlstate::FEATURE_NOT_SUPPORTED,
                            "global participant adopted an existing in-doubt transaction",
                        )
                        .into());
                    }
                }
                Err(error) => {
                    self.abort_global_transaction(global_xid, &prepared, &touched)
                        .await
                        .map_err(|abort_error| GlobalCommitError {
                            error: abort_error,
                            recovery: Some(GlobalCommitRecovery {
                                global_xid,
                                prepared: prepared.clone(),
                                decision: None,
                            }),
                        })?;
                    return Err(error.into());
                }
            }
        }

        if self.take_commit_fault_for_testing(GatewayCommitFault::BeforeDecisionAfterPrepare) {
            return Err(GlobalCommitError {
                error: PgError::error("XX000", "injected commit failure before global decision"),
                recovery: Some(GlobalCommitRecovery {
                    global_xid,
                    prepared,
                    decision: None,
                }),
            });
        }

        let decision = self
            .record_global_decision(global_xid, TransactionDecision::Commit)
            .await
            .map_err(|error| GlobalCommitError {
                error,
                recovery: Some(GlobalCommitRecovery {
                    global_xid,
                    prepared: prepared.clone(),
                    decision: None,
                }),
            })?;
        if decision != TransactionDecision::Commit {
            self.release_prepared_participants_recoverably(
                global_xid,
                &prepared,
                TransactionDecision::Abort,
            )
            .await?;
            return Err(PgError::error("40001", "local 2PC transaction aborted").into());
        }

        if let Some((message, decision)) = self.take_post_decision_commit_fault_for_testing() {
            return Err(GlobalCommitError {
                error: PgError::error("XX000", message),
                recovery: Some(GlobalCommitRecovery {
                    global_xid,
                    prepared,
                    decision,
                }),
            });
        }

        self.release_prepared_participants_recoverably(
            global_xid,
            &prepared,
            TransactionDecision::Commit,
        )
        .await
    }

    fn take_post_decision_commit_fault_for_testing(
        &self,
    ) -> Option<(&'static str, Option<TransactionDecision>)> {
        if self.take_commit_fault_for_testing(GatewayCommitFault::BeforeReleaseAfterCommitDecision)
        {
            return Some((
                "injected commit failure after global decision",
                Some(TransactionDecision::Commit),
            ));
        }
        if self.take_commit_fault_for_testing(
            GatewayCommitFault::AfterCommitDecisionWithoutRecoveryMetadata,
        ) {
            return Some((
                "injected commit failure after global decision without recovery metadata",
                None,
            ));
        }
        None
    }

    fn take_commit_fault_for_testing(&self, expected: GatewayCommitFault) -> bool {
        let Some(fault) = &self.inner.commit_fault_for_testing else {
            return false;
        };
        let mut fault = fault.lock().expect("commit fault mutex poisoned");
        if *fault != Some(expected) {
            return false;
        }
        fault.take();
        true
    }

    async fn begin_global_transaction(&mut self, participants: &[RangeId]) -> Result<u64, PgError> {
        let serving = self.current_serving()?;
        let global_xid = if let Some(coordinator) = serving.engine(RangeId::COORDINATOR) {
            coordinator
                .begin_global_durable()
                .await
                .map_err(ExecError::into_pg)?
        } else {
            self.ensure_remote_session(RangeId::COORDINATOR).await?;
            self.remote_sessions
                .get_mut(&RangeId::COORDINATOR)
                .expect("remote range 0 session inserted")
                .begin_global()
                .await?
        };
        self.inner
            .coordinator
            .begin_existing_xid(global_xid, participants.to_vec())
            .await
            .map_err(|error| coordinator_error_to_pg(&error))?;
        Ok(global_xid)
    }

    async fn cleanup_global_commit_recovery(
        &mut self,
        recovery: &GlobalCommitRecovery,
        touched: &[RangeId],
    ) -> Result<(), GlobalCommitError> {
        let Some(decision) = recovery.decision else {
            self.abort_global_transaction(recovery.global_xid, &recovery.prepared, touched)
                .await
                .map_err(|error| GlobalCommitError {
                    error,
                    recovery: Some(recovery.clone()),
                })?;
            return Ok(());
        };
        self.release_prepared_participants_recoverably(
            recovery.global_xid,
            &recovery.prepared,
            decision,
        )
        .await?;
        if decision == TransactionDecision::Commit {
            return Ok(());
        }
        for range_id in touched.iter().copied() {
            if recovery
                .prepared
                .iter()
                .any(|participant| participant.range_id == range_id)
            {
                continue;
            }
            self.simple_on_range(range_id, "ROLLBACK")
                .await
                .map_err(GlobalCommitError::from)?;
        }
        Ok(())
    }

    async fn release_prepared_participants_recoverably(
        &mut self,
        global_xid: u64,
        prepared: &[PreparedParticipantRelease],
        decision: TransactionDecision,
    ) -> Result<(), GlobalCommitError> {
        for (index, participant) in prepared.iter().copied().enumerate() {
            let result = self
                .release_on_range(
                    participant.range_id,
                    participant.global_xid,
                    decision == TransactionDecision::Commit,
                )
                .await;
            if let Err(error) = result {
                return Err(GlobalCommitError {
                    error,
                    recovery: Some(GlobalCommitRecovery {
                        global_xid,
                        prepared: prepared[index..].to_vec(),
                        decision: Some(decision),
                    }),
                });
            }
        }
        Ok(())
    }

    async fn abort_global_transaction(
        &mut self,
        global_xid: u64,
        prepared: &[PreparedParticipantRelease],
        touched: &[RangeId],
    ) -> Result<(), PgError> {
        let effective_decision = self
            .record_global_decision(global_xid, TransactionDecision::Abort)
            .await?;
        for participant_global_xid in prepared.iter().map(|participant| participant.global_xid) {
            if participant_global_xid == global_xid {
                continue;
            }
            self.record_range0_global_decision(participant_global_xid, effective_decision)
                .await?;
        }
        self.release_prepared_participants(prepared, effective_decision)
            .await?;
        if effective_decision == TransactionDecision::Commit {
            return Ok(());
        }
        for range_id in touched.iter().copied() {
            if prepared
                .iter()
                .any(|participant| participant.range_id == range_id)
            {
                continue;
            }
            self.simple_on_range(range_id, "ROLLBACK").await?;
        }
        Ok(())
    }

    async fn record_global_decision(
        &mut self,
        global_xid: u64,
        decision: TransactionDecision,
    ) -> Result<TransactionDecision, PgError> {
        if let Some(existing_decision) = self.existing_coordinator_decision(global_xid).await {
            return Ok(existing_decision);
        }

        let status = status_for_transaction_decision(decision);
        let effective_status = self.record_range0_global_status(global_xid, status).await?;
        let effective_decision = transaction_decision_from_status(effective_status)?;
        let coordinator_decision = self
            .inner
            .coordinator
            .decide_prepared(global_xid, effective_decision)
            .await
            .or_else(|error| match error {
                LocalCoordinatorError::DecisionAlreadyFinal { existing, .. } => Ok(existing),
                error => Err(error),
            })
            .map_err(|error| coordinator_error_to_pg(&error))?;
        Ok(coordinator_decision)
    }

    async fn existing_coordinator_decision(&self, global_xid: u64) -> Option<TransactionDecision> {
        self.inner
            .coordinator
            .records()
            .await
            .into_iter()
            .find(|record| record.xid == global_xid)
            .and_then(|record| record.decision)
    }

    async fn record_range0_global_decision(
        &mut self,
        global_xid: u64,
        decision: TransactionDecision,
    ) -> Result<(), PgError> {
        let status = status_for_transaction_decision(decision);
        self.record_range0_global_status(global_xid, status).await?;
        Ok(())
    }

    async fn record_range0_global_status(
        &mut self,
        global_xid: u64,
        status: crabka_pgmvcc::clog::XidStatus,
    ) -> Result<crabka_pgmvcc::clog::XidStatus, PgError> {
        let serving = self.current_serving()?;
        if let Some(coordinator) = serving.engine(RangeId::COORDINATOR) {
            coordinator
                .commit_global_decision(global_xid, status)
                .await
                .map_err(ExecError::into_pg)
        } else {
            self.ensure_remote_session(RangeId::COORDINATOR).await?;
            self.remote_sessions
                .get_mut(&RangeId::COORDINATOR)
                .expect("remote range 0 session inserted")
                .record_global_decision(global_xid, status)
                .await
        }
    }

    async fn release_prepared_participants(
        &mut self,
        prepared: &[PreparedParticipantRelease],
        decision: TransactionDecision,
    ) -> Result<(), PgError> {
        for participant in prepared.iter().copied() {
            self.release_on_range(
                participant.range_id,
                participant.global_xid,
                decision == TransactionDecision::Commit,
            )
            .await?;
        }
        Ok(())
    }

    async fn execute_ddl(&mut self, statement: &str) -> Result<Vec<QueryResult>, PgError> {
        let _schema_gate = self.inner.schema_gate.clone().lock_owned().await;
        self.session_for(RangeId::COORDINATOR)?
            .simple_query(statement)
            .await
    }

    async fn acquire_routed_dml_gate(
        &self,
        route: &StatementRoute,
    ) -> Result<Option<tokio::sync::OwnedMutexGuard<()>>, PgError> {
        if route.kind != StatementKind::Dml {
            return Ok(None);
        }
        let Some(table_id) = route.table_id else {
            return Ok(None);
        };

        let table_write_gate = self.inner.table_write_gate(table_id);
        let table_write_gate = table_write_gate.lock_owned().await;
        self.current_serving()?;
        Ok(Some(table_write_gate))
    }

    async fn execute_routed_statement(
        &mut self,
        statement: &str,
        kind: StatementKind,
        range_id: RangeId,
    ) -> Result<Vec<QueryResult>, PgError> {
        if !self.sessions.contains_key(&range_id) {
            return self
                .execute_remote_statement(statement, kind, range_id)
                .await;
        }
        if kind == StatementKind::Dml {
            self.touch_write_range(range_id).await?;
        }
        let result = self.session_for(range_id)?.simple_query(statement).await;
        if result.is_err() && matches!(self.transaction, GatewayTransaction::Open { .. }) {
            self.fail_transaction_preserving_participants();
            self.status = TxStatus::Failed;
        }
        result
    }

    async fn execute_remote_statement(
        &mut self,
        statement: &str,
        kind: StatementKind,
        range_id: RangeId,
    ) -> Result<Vec<QueryResult>, PgError> {
        if kind != StatementKind::Dml && kind != StatementKind::Query {
            return Err(PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                format!(
                    "range r{range_id} is not hosted by tenant {}",
                    self.inner.tenant
                ),
            ));
        }
        if kind == StatementKind::Dml {
            self.touch_write_range(range_id).await?;
        }
        self.ensure_remote_session(range_id).await?;
        self.remote_sessions
            .get_mut(&range_id)
            .expect("remote session inserted")
            .simple_query(statement.to_owned())
            .await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the timestamp scatter 2PC protocol must keep its durable ordering visible"
    )]
    async fn execute_timestamp_scatter(
        &mut self,
        statement: &str,
        ranges: Vec<RangeId>,
    ) -> Result<QueryResult, PgError> {
        if !matches!(self.transaction, GatewayTransaction::Idle) {
            return Err(PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                "multi-range sharded timestamp DML is only supported in autocommit",
            ));
        }
        if ranges
            .iter()
            .any(|range_id| !self.sessions.contains_key(range_id))
        {
            return Err(PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                "remote multi-range transactions are unsupported until a stateful participant RPC is designed",
            ));
        }
        ensure_timestamp_scatter_is_supported(statement)?;
        let serving = self.current_serving()?;
        let coordinator = serving.engine(RangeId::COORDINATOR).ok_or_else(|| {
            PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                "range r0 is not hosted by this tenant",
            )
        })?;
        let plan = coordinator
            .plan_timestamp_write_sql(statement)
            .map_err(ExecError::into_pg)?;
        if plan.writes.is_empty() {
            return Ok(plan.result);
        }
        if plan
            .writes
            .iter()
            .any(|write| !write.global_index_intents.is_empty())
        {
            return Err(PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                "multi-range timestamp scatter does not support global-index maintenance",
            ));
        }
        let mut writes_by_range = BTreeMap::<RangeId, Vec<crabka_pgexec::TimestampWrite>>::new();
        let table = self
            .table_for_timestamp_writes(&plan.writes)
            .map_err(ExecError::into_pg)?;
        let write_routes = timestamp_insert_write_routes(
            &self.current_serving()?.range_map,
            &table,
            statement,
            plan.writes.len(),
        )?;
        for ((mut write, range_id), rowid) in plan
            .writes
            .into_iter()
            .zip(write_routes)
            .zip(timestamp_insert_physical_rowids(statement, &table)?)
        {
            if let Some(rowid) = rowid {
                write.rowid = rowid;
            }
            writes_by_range.entry(range_id).or_default().push(write);
        }
        if writes_by_range
            .keys()
            .any(|range_id| !ranges.contains(range_id))
        {
            return Err(PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                "timestamp scatter plan does not match routed ranges",
            ));
        }
        let (start_ts, identity) = self
            .begin_timestamp_scatter(coordinator, writes_by_range.keys().copied())
            .await?;
        let mut participants = Vec::with_capacity(writes_by_range.len());
        for (range_id, writes) in &writes_by_range {
            let participant = self.timestamp_participant(*range_id)?;
            if let Err(error) = participant.prewrite_with_primary(identity, writes).await {
                self.abort_timestamp_scatter(coordinator, identity, &participants)
                    .await
                    .map_err(ExecError::into_pg)?;
                return Err(error.into_pg());
            }
            // Track durable prewrite before the range-0 acknowledgement. An ack
            // write failure must still abort and resolve this participant.
            participants.push((*range_id, participant.clone(), writes.clone()));
            let operations = writes
                .iter()
                .map(|write| crabka_pgexec::TimestampTxnOperation {
                    range_id: range_id.as_u32(),
                    table_id: write.table_id,
                    rowid: write.rowid,
                    delete: write.delete,
                })
                .collect::<Vec<_>>();
            if let Err(error) = coordinator
                .acknowledge_timestamp_participant_operations(
                    start_ts,
                    range_id.as_u32(),
                    &operations,
                )
                .await
            {
                // The primary descriptor was begun durably; choose abort and
                // resolve every participant known to have prewritten, including
                // this one whose acknowledgement did not reach range 0.
                self.abort_timestamp_scatter(coordinator, identity, &participants)
                    .await
                    .map_err(ExecError::into_pg)?;
                return Err(error.into_pg());
            }
        }
        if self
            .take_commit_fault_for_testing(GatewayCommitFault::AfterTimestampPrewriteBeforeDecision)
        {
            return Err(PgError::error(
                "XX000",
                "injected crash after timestamp prewrites before durable decision",
            ));
        }
        let commit_ts = coordinator
            .allocate_commit_timestamp_after(start_ts)
            .await
            .map_err(ExecError::into_pg)?;
        let decision = coordinator
            .decide_timestamp_transaction(
                start_ts,
                crabka_pgexec::PrimaryTxnDecision::Committed(commit_ts),
            )
            .await
            .map_err(ExecError::into_pg)?;
        let crabka_pgexec::PrimaryTxnDecision::Committed(commit_ts) = decision else {
            self.abort_timestamp_scatter(coordinator, identity, &participants)
                .await
                .map_err(ExecError::into_pg)?;
            return Err(PgError::error("40001", "timestamp transaction aborted"));
        };
        if self.take_commit_fault_for_testing(GatewayCommitFault::AfterTimestampCommitDecision) {
            return Err(PgError::error(
                "XX000",
                "injected crash after durable timestamp commit decision",
            ));
        }
        let mut commit_ops = Some(plan.commit_ops);
        for (_range_id, participant, writes) in &participants {
            let extra_ops = commit_ops.take().unwrap_or_default();
            if let Err(error) = participant
                .resolve_with_primary(
                    identity,
                    crabka_pgexec::TimestampTxnDecision::Committed(commit_ts),
                    writes,
                )
                .await
            {
                return Err(error.into_pg());
            }
            if !extra_ops.is_empty() {
                coordinator
                    .commit_timestamp_statement_ops(extra_ops)
                    .await
                    .map_err(ExecError::into_pg)?;
            }
        }
        Ok(plan.result)
    }

    async fn begin_timestamp_scatter(
        &self,
        coordinator: &SqlEngine,
        participant_ranges: impl Iterator<Item = RangeId>,
    ) -> Result<
        (
            crabka_pgexec::TimestampTransactionId,
            crabka_pgexec::TimestampTxnIdentity,
        ),
        PgError,
    > {
        let start_ts = coordinator
            .allocate_timestamp_transaction_id()
            .await
            .map_err(ExecError::into_pg)?;
        // Timestamp transactions use the descriptor as their only primary record;
        // allocating a GTM status would create a second, timestamp-less decision.
        let global_xid = start_ts.get();
        let descriptor = crabka_pgexec::TimestampTxnDescriptor::begun(
            start_ts,
            global_xid,
            participant_ranges.map(RangeId::as_u32).collect(),
        );
        coordinator
            .begin_timestamp_transaction(&descriptor)
            .await
            .map_err(ExecError::into_pg)?;
        Ok((
            start_ts,
            crabka_pgexec::TimestampTxnIdentity {
                start_ts,
                global_xid,
                primary_range: RangeId::COORDINATOR.as_u32(),
            },
        ))
    }

    fn timestamp_participant(
        &self,
        range_id: RangeId,
    ) -> Result<crabka_pgexec::TimestampTxnParticipant, PgError> {
        self.current_serving()?
            .engine(range_id)
            .map(|engine| engine.timestamp_txn_participant(range_id.as_u32()))
            .ok_or_else(|| {
                PgError::error(
                    sqlstate::FEATURE_NOT_SUPPORTED,
                    format!(
                        "range r{range_id} is not hosted by tenant {}",
                        self.inner.tenant
                    ),
                )
            })
    }

    async fn abort_timestamp_scatter(
        &self,
        coordinator: &SqlEngine,
        identity: crabka_pgexec::TimestampTxnIdentity,
        participants: &[(
            RangeId,
            crabka_pgexec::TimestampTxnParticipant,
            Vec<crabka_pgexec::TimestampWrite>,
        )],
    ) -> Result<(), ExecError> {
        let decision = coordinator
            .decide_timestamp_transaction(
                identity.start_ts,
                crabka_pgexec::PrimaryTxnDecision::Aborted,
            )
            .await?;
        let effective_decision = match decision {
            crabka_pgexec::PrimaryTxnDecision::Aborted => {
                crabka_pgexec::TimestampTxnDecision::Aborted
            }
            crabka_pgexec::PrimaryTxnDecision::Committed(commit_ts) => {
                crabka_pgexec::TimestampTxnDecision::Committed(commit_ts)
            }
            crabka_pgexec::PrimaryTxnDecision::Pending => {
                return Err(ExecError::Unsupported(
                    "timestamp transaction primary remained pending during abort cleanup".into(),
                ));
            }
        };
        for (_range_id, participant, writes) in participants {
            participant
                .resolve_with_primary(identity, effective_decision, writes)
                .await?;
        }
        Ok(())
    }

    fn table_for_timestamp_writes(
        &self,
        writes: &[crabka_pgexec::TimestampWrite],
    ) -> Result<crabka_pgcatalog::Table, ExecError> {
        let Some(first) = writes.first() else {
            return Err(ExecError::Unsupported(
                "timestamp scatter has no writes".into(),
            ));
        };
        let serving = self.current_serving().map_err(|error| {
            ExecError::Unsupported(format!(
                "current serving snapshot is unavailable: {error:?}"
            ))
        })?;
        let catalog = serving
            .engine(RangeId::COORDINATOR)
            .ok_or_else(|| ExecError::Unsupported("range r0 is not hosted".into()))?;
        crabka_pgcatalog::list_tables(catalog.catalog_kv())?
            .into_iter()
            .find(|table| table.id == first.table_id)
            .ok_or_else(|| ExecError::Unsupported("timestamp scatter table is missing".into()))
    }

    fn fail_transaction_preserving_participants(&mut self) {
        let GatewayTransaction::Open { touched, escalated } =
            std::mem::replace(&mut self.transaction, GatewayTransaction::Idle)
        else {
            return;
        };
        self.transaction = GatewayTransaction::Failed {
            touched,
            escalated,
            recovery: None,
        };
    }

    fn finish_statement<T>(&mut self, result: Result<T, PgError>) -> Result<T, PgError> {
        if result.is_err() && matches!(self.transaction, GatewayTransaction::Open { .. }) {
            self.fail_transaction_preserving_participants();
            self.status = TxStatus::Failed;
        }
        result
    }

    async fn touch_write_range(&mut self, range_id: RangeId) -> Result<(), PgError> {
        let GatewayTransaction::Open { touched, .. } = &self.transaction else {
            return Ok(());
        };
        if touched.contains(&range_id) {
            return Ok(());
        }
        let escalates_transaction = !touched.is_empty();
        if let Some(session) = self.sessions.get_mut(&range_id) {
            session.simple_query("BEGIN").await?;
        } else {
            self.ensure_remote_session(range_id).await?;
            self.remote_sessions
                .get_mut(&range_id)
                .expect("remote session inserted")
                .simple_query("BEGIN".into())
                .await?;
        }

        let GatewayTransaction::Open { touched, escalated } = &mut self.transaction else {
            return Ok(());
        };
        if escalates_transaction {
            *escalated = true;
        }
        touched.push(range_id);
        Ok(())
    }

    fn reject_statement_in_failed_transaction(&self, statement: &str) -> Result<(), PgError> {
        if !matches!(self.transaction, GatewayTransaction::Failed { .. }) {
            return Ok(());
        }
        if statement_is_abort_cleanup(statement) {
            return Ok(());
        }
        Err(failed_transaction_error())
    }

    fn session_for(
        &mut self,
        range_id: RangeId,
    ) -> Result<&mut crabka_pgexec::SqlSession, PgError> {
        self.current_serving()?;
        self.sessions.get_mut(&range_id).ok_or_else(|| {
            PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                format!(
                    "range r{range_id} is not hosted by tenant {}",
                    self.inner.tenant
                ),
            )
        })
    }

    fn route_statement(&self, sql: &str) -> Result<StatementRoute, PgError> {
        let serving = self.current_serving()?;
        let Some(catalog) = serving.engine(RangeId::COORDINATOR) else {
            return Err(PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                "range r0 is not hosted by this tenant",
            ));
        };
        route_statement(&serving.range_map, catalog, sql)
    }

    fn release_explicit_transaction(&mut self) {
        if !self.explicit_transaction {
            return;
        }
        self.explicit_transaction = false;
        self.inner
            .active_explicit_transactions
            .fetch_sub(1, Ordering::AcqRel);
    }
}

impl Drop for GatewaySession {
    fn drop(&mut self) {
        self.release_explicit_transaction();
    }
}

/// Map each INSERT tuple to its physical owner from the same literal shard keys
/// used by gateway routing. Timestamp writes receive sequence rowids, so routing
/// them by those generated ids would silently disagree with the SQL shard key.
fn timestamp_insert_write_routes(
    range_map: &RangeMap,
    table: &crabka_pgcatalog::Table,
    statement: &str,
    write_count: usize,
) -> Result<Vec<RangeId>, PgError> {
    let lower = statement.trim_start().to_ascii_lowercase();
    let table_id = routing_table_id(&table.name);
    let routes = if let Some(ShardingStrategy::Hash(hash)) = table.sharding.as_ref() {
        let spec = HashShardSpec::new(
            table_id,
            hash.columns.clone(),
            hash.buckets,
            hash.co_location_group.clone(),
        )
        .map_err(|error| map_error_to_pg(&error))?;
        inserted_hash_values(&lower, table, hash)?
            .into_iter()
            .map(|value| {
                range_map
                    .route_hash_equality(&spec, value)
                    .map(|route| route.range_id)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| map_error_to_pg(&error))?
    } else {
        inserted_rowid_keys(&lower)?
            .into_iter()
            .map(|rowid| {
                range_map
                    .range_for_key(table_id, rowid)
                    .map(|route| route.range_id)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| map_error_to_pg(&error))?
    };
    if routes.len() != write_count {
        return Err(PgError::error(
            sqlstate::FEATURE_NOT_SUPPORTED,
            "timestamp scatter tuple routing did not match planned writes",
        ));
    }
    Ok(routes)
}

/// Row-range scans are partitioned by the durable row id. For literal `id`
/// INSERTs, preserve that key in the scatter writes; hash ownership remains
/// independent of sequence rowids and therefore leaves them unchanged.
fn timestamp_insert_physical_rowids(
    statement: &str,
    table: &crabka_pgcatalog::Table,
) -> Result<Vec<Option<u64>>, PgError> {
    if table.sharding.is_some() {
        let count = values_tuples(&statement.trim_start().to_ascii_lowercase()).count();
        return Ok(vec![None; count]);
    }
    Ok(
        inserted_rowid_keys(&statement.trim_start().to_ascii_lowercase())?
            .into_iter()
            .map(Some)
            .collect(),
    )
}

fn ensure_timestamp_scatter_is_supported(statement: &str) -> Result<(), PgError> {
    let statements = crabka_pgparser::parse(statement).map_err(|error| {
        PgError::error(
            sqlstate::FEATURE_NOT_SUPPORTED,
            format!("cannot parse timestamp scatter statement: {error}"),
        )
    })?;
    if matches!(
        statements.as_slice(),
        [crabka_pgparser::ast::Statement::Insert { .. }]
    ) {
        return Ok(());
    }
    Err(PgError::error(
        sqlstate::FEATURE_NOT_SUPPORTED,
        "multi-range timestamp scatter currently supports INSERT only; UPDATE and DELETE remain unsupported",
    ))
}

fn failed_transaction_error() -> PgError {
    PgError::error(
        sqlstate::IN_FAILED_SQL_TRANSACTION,
        "current transaction is aborted, commands ignored until end of transaction block",
    )
}

fn missing_statement(name: &str) -> PgError {
    PgError::error(
        sqlstate::INVALID_SQL_STATEMENT_NAME,
        format!("prepared statement \"{name}\" does not exist"),
    )
}

fn missing_portal(name: &str) -> PgError {
    PgError::error(
        sqlstate::INVALID_CURSOR_NAME,
        format!("portal \"{name}\" does not exist"),
    )
}

fn query_result_to_outcome(result: QueryResult) -> ExecuteOutcome {
    match result {
        QueryResult::Rows { rows, tag, .. } => ExecuteOutcome::Rows {
            rows: rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|cell| cell.map(|cell| cell.text))
                        .collect()
                })
                .collect(),
            completion: Some(tag),
        },
        QueryResult::Command { tag } => ExecuteOutcome::CommandComplete { tag },
        QueryResult::Empty => ExecuteOutcome::EmptyQuery,
    }
}

fn rollback_command_response() -> Vec<QueryResult> {
    vec![QueryResult::Command {
        tag: "ROLLBACK".to_string(),
    }]
}

fn status_for_transaction_decision(
    decision: TransactionDecision,
) -> crabka_pgmvcc::clog::XidStatus {
    match decision {
        TransactionDecision::Commit => crabka_pgmvcc::clog::XidStatus::Committed,
        TransactionDecision::Abort => crabka_pgmvcc::clog::XidStatus::Aborted,
    }
}

fn transaction_decision_from_status(
    status: crabka_pgmvcc::clog::XidStatus,
) -> Result<TransactionDecision, PgError> {
    match status {
        crabka_pgmvcc::clog::XidStatus::Committed => Ok(TransactionDecision::Commit),
        crabka_pgmvcc::clog::XidStatus::Aborted => Ok(TransactionDecision::Abort),
        crabka_pgmvcc::clog::XidStatus::InProgress
        | crabka_pgmvcc::clog::XidStatus::Prepared(_) => Err(PgError::error(
            "XX000",
            "global transaction decision did not become terminal",
        )),
    }
}

fn coordinator_error_to_pg(error: &LocalCoordinatorError) -> PgError {
    match error {
        LocalCoordinatorError::UnknownTransaction { xid } => PgError::error(
            "XX000",
            format!("unknown local coordinator transaction {xid}"),
        ),
        LocalCoordinatorError::UnknownParticipant { xid, range_id } => PgError::error(
            "XX000",
            format!(
                "range r{range_id} is not a participant in local coordinator transaction {xid}"
            ),
        ),
        LocalCoordinatorError::DecisionAlreadyFinal { xid, existing } => PgError::error(
            "XX000",
            format!("local coordinator transaction {xid} already decided {existing:?}"),
        ),
        LocalCoordinatorError::CommitBeforePrepared { xid } => PgError::error(
            "XX000",
            format!(
                "local coordinator transaction {xid} cannot commit before every participant prepared"
            ),
        ),
        LocalCoordinatorError::ExistingTransactionConflict { xid } => PgError::error(
            "XX000",
            format!(
                "local coordinator transaction {xid} already begun with different participants"
            ),
        ),
        LocalCoordinatorError::ExistingTransactionAdvanced { xid, phase } => PgError::error(
            "XX000",
            format!("local coordinator transaction {xid} is already {phase:?}"),
        ),
    }
}

fn statement_is_abort_cleanup(sql: &str) -> bool {
    statement_matches_transaction_control(sql, &["rollback", "abort"])
}

fn statement_is_commit(sql: &str) -> bool {
    statement_matches_transaction_control(sql, &["commit"])
}

fn statement_matches_transaction_control(sql: &str, commands: &[&str]) -> bool {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return false;
    }
    let statement = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    let mut tokens = statement.split_ascii_whitespace();
    let Some(command) = tokens.next() else {
        return false;
    };
    if !commands
        .iter()
        .any(|allowed| command.eq_ignore_ascii_case(allowed))
    {
        return false;
    }

    let modifier_is_valid = match tokens.next() {
        None => true,
        Some(modifier) => {
            modifier.eq_ignore_ascii_case("work") || modifier.eq_ignore_ascii_case("transaction")
        }
    };
    modifier_is_valid && tokens.next().is_none()
}

fn transaction_participants(
    transaction: GatewayTransaction,
) -> Option<(Vec<RangeId>, bool, Option<GlobalCommitRecovery>)> {
    match transaction {
        GatewayTransaction::Idle => None,
        GatewayTransaction::Open { touched, escalated } => Some((touched, escalated, None)),
        GatewayTransaction::Failed {
            touched,
            escalated,
            recovery,
        } => Some((touched, escalated, recovery)),
    }
}

fn range_map_from_boundaries(
    tenant: TenantName,
    boundaries: &str,
) -> Result<RangeMap, TenantError> {
    let parsed = parse_boundaries(boundaries)?;
    let mut specs = Vec::with_capacity(parsed.len());
    for (index, boundary) in parsed.iter().copied().enumerate() {
        let range_id = RangeId::new(u32::try_from(index).expect("range index fits u32"));
        let end = parsed.get(index + 1).copied();
        specs.push(RangeSpec::for_interval(range_id, boundary, end));
    }
    Ok(RangeMap::new(tenant, MapEpoch::ZERO, specs)?)
}

fn parse_boundaries(boundaries: &str) -> Result<Vec<RangeKey>, TenantError> {
    let values = boundaries
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(parse_boundary)
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() {
        return Err(TenantError::EmptyBoundaries);
    }
    if values[0] != RangeKey::MIN || values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(TenantError::InvalidBoundaryOrder);
    }
    Ok(values)
}

fn parse_boundary(token: &str) -> Result<RangeKey, TenantError> {
    let parts = token.split(':').collect::<Vec<_>>();
    match parts.as_slice() {
        [table] => {
            parse_boundary_u64(table, token).map(|table| RangeKey::table_start(TableId::new(table)))
        }
        [table, rowid] => Ok(RangeKey::new(
            TableId::new(parse_boundary_u64(table, token)?),
            parse_boundary_u64(rowid, token)?,
        )),
        [table, bucket, rowid] => Ok(RangeKey::hash(
            TableId::new(parse_boundary_u64(table, token)?),
            parse_boundary_u32(bucket, token)?,
            parse_boundary_u64(rowid, token)?,
        )),
        _ => parse_boundary_u64("", token).map(|_| RangeKey::MIN),
    }
}

fn parse_boundary_u64(value: &str, token: &str) -> Result<u64, TenantError> {
    value
        .parse::<u64>()
        .map_err(|source| TenantError::InvalidBoundary {
            token: token.to_string(),
            source,
        })
}

fn parse_boundary_u32(value: &str, token: &str) -> Result<u32, TenantError> {
    value
        .parse::<u32>()
        .map_err(|source| TenantError::InvalidBoundary {
            token: token.to_string(),
            source,
        })
}

fn normalize_hosted_ranges(
    range_map: &RangeMap,
    mut hosted_ranges: Vec<RangeId>,
) -> Result<Vec<RangeId>, TenantError> {
    hosted_ranges.sort_unstable();
    hosted_ranges.dedup();
    for range_id in &hosted_ranges {
        if !range_map
            .ranges()
            .iter()
            .any(|range| range.range_id == *range_id)
        {
            return Err(TenantError::HostedRangeMissing(*range_id));
        }
    }
    Ok(hosted_ranges)
}

fn split_statements(sql: &str) -> impl Iterator<Item = &str> {
    sql.split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
}

fn route_statement(
    range_map: &RangeMap,
    catalog: &crabka_pgexec::SqlEngine,
    sql: &str,
) -> Result<StatementRoute, PgError> {
    let normalized = sql.trim_start().to_ascii_lowercase();
    if normalized.is_empty() {
        return Ok(StatementRoute {
            kind: StatementKind::Local,
            range_id: RangeId::COORDINATOR,
            table_id: None,
            scatter_ranges: None,
        });
    }
    if starts_with_any(&normalized, &["begin", "start transaction"]) {
        return Ok(StatementRoute {
            kind: StatementKind::Begin,
            range_id: RangeId::COORDINATOR,
            table_id: None,
            scatter_ranges: None,
        });
    }
    if statement_is_commit(sql) {
        return Ok(StatementRoute {
            kind: StatementKind::Commit,
            range_id: RangeId::COORDINATOR,
            table_id: None,
            scatter_ranges: None,
        });
    }
    if statement_is_abort_cleanup(sql) {
        return Ok(StatementRoute {
            kind: StatementKind::Rollback,
            range_id: RangeId::COORDINATOR,
            table_id: None,
            scatter_ranges: None,
        });
    }
    if starts_with_any(&normalized, &["create", "alter", "drop", "truncate"]) {
        return Ok(StatementRoute {
            kind: StatementKind::Ddl,
            range_id: RouteIntent::DataDefinition
                .route(range_map)
                .map_err(|error| map_error_to_pg(&error))?
                .range_id,
            table_id: None,
            scatter_ranges: None,
        });
    }

    let kind = if starts_with_any(&normalized, &["insert", "update", "delete"]) {
        StatementKind::Dml
    } else if normalized.starts_with("select") {
        StatementKind::Query
    } else {
        StatementKind::Local
    };
    if kind == StatementKind::Local {
        return Ok(StatementRoute {
            kind,
            range_id: RangeId::COORDINATOR,
            table_id: None,
            scatter_ranges: None,
        });
    }

    let table_refs = table_refs_in_statement(&normalized);
    reject_unsupported_cross_range_statement(range_map, catalog, &table_refs)?;
    let table_ref = table_refs.first();
    let table_id = table_ref.map_or(TableId::ZERO, |table_ref| table_ref.table_id);
    let route = route_sql_statement(range_map, catalog, &normalized, kind, table_ref)?;
    Ok(StatementRoute {
        kind,
        range_id: route.primary_range(),
        table_id: Some(table_id),
        scatter_ranges: route.scatter_ranges,
    })
}

struct RouteTarget {
    range_id: RangeId,
    scatter_ranges: Option<Vec<RangeId>>,
}

impl RouteTarget {
    fn single(range_id: RangeId) -> Self {
        Self {
            range_id,
            scatter_ranges: None,
        }
    }

    fn primary_range(&self) -> RangeId {
        self.range_id
    }

    fn scatter(mut ranges: Vec<RangeId>) -> Self {
        ranges.sort_unstable();
        ranges.dedup();
        let range_id = ranges[0];
        Self {
            range_id,
            scatter_ranges: Some(ranges),
        }
    }
}

fn route_sql_statement(
    range_map: &RangeMap,
    catalog: &crabka_pgexec::SqlEngine,
    sql: &str,
    kind: StatementKind,
    table_ref: Option<&TableRef>,
) -> Result<RouteTarget, PgError> {
    let Some(table_ref) = table_ref else {
        return Ok(RouteTarget::single(RangeId::COORDINATOR));
    };

    if !catalog_table_is_sharded(catalog, &table_ref.name)? {
        return route_table(range_map, table_ref.table_id).map(RouteTarget::single);
    }

    if let Some(route) = route_hash_sharded_statement(range_map, catalog, sql, kind, table_ref)? {
        return Ok(route);
    }

    let rowids = statement_rowid_keys(sql, kind)?;
    if rowids.is_empty() {
        if kind == StatementKind::Dml && (sql.starts_with("update") || sql.starts_with("delete")) {
            return Err(PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                "sharded timestamp UPDATE/DELETE requires a statically known shard key",
            ));
        }
        return route_table(range_map, table_ref.table_id).map(RouteTarget::single);
    }

    let ranges = rowids
        .iter()
        .map(|rowid| {
            range_map
                .range_for_key(table_ref.table_id, *rowid)
                .map(|route| route.range_id)
                .map_err(|error| map_error_to_pg(&error))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let Some(first_range) = ranges.first().copied() else {
        return route_table(range_map, table_ref.table_id).map(RouteTarget::single);
    };
    if ranges.iter().all(|range_id| *range_id == first_range) {
        return Ok(RouteTarget::single(first_range));
    }
    if kind == StatementKind::Dml && sql.starts_with("insert") {
        return Ok(RouteTarget::scatter(ranges));
    }

    Err(PgError::error(
        sqlstate::FEATURE_NOT_SUPPORTED,
        "sharded statement spans multiple row ranges",
    ))
}

fn route_hash_sharded_statement(
    range_map: &RangeMap,
    catalog: &crabka_pgexec::SqlEngine,
    sql: &str,
    kind: StatementKind,
    table_ref: &TableRef,
) -> Result<Option<RouteTarget>, PgError> {
    let table = catalog_table(catalog, &table_ref.name)?;
    let Some(ShardingStrategy::Hash(hash)) = table.sharding.as_ref() else {
        return Ok(None);
    };
    let spec = HashShardSpec::new(
        table_ref.table_id,
        hash.columns.clone(),
        hash.buckets,
        hash.co_location_group.clone(),
    )
    .map_err(|error| map_error_to_pg(&error))?;
    let hash_values = sql_hash_values(sql, kind, &table, hash)?;
    if hash_values.is_empty() {
        return Ok(None);
    }
    let ranges = hash_values
        .into_iter()
        .map(|hash_value| {
            range_map
                .route_hash_equality(&spec, hash_value)
                .map(|route| route.range_id)
                .map_err(|error| map_error_to_pg(&error))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let Some(first_range) = ranges.first().copied() else {
        return Ok(None);
    };
    if ranges.iter().all(|range_id| *range_id == first_range) {
        return Ok(Some(RouteTarget::single(first_range)));
    }
    if kind == StatementKind::Dml && sql.starts_with("insert") {
        return Ok(Some(RouteTarget::scatter(ranges)));
    }
    Err(PgError::error(
        sqlstate::FEATURE_NOT_SUPPORTED,
        "hash-sharded statement spans multiple ranges",
    ))
}

fn route_table(range_map: &RangeMap, table_id: TableId) -> Result<RangeId, PgError> {
    RouteIntent::DataManipulation { table_id }
        .route(range_map)
        .map(|route| route.range_id)
        .map_err(|error| map_error_to_pg(&error))
}

fn catalog_table_is_sharded(
    catalog: &crabka_pgexec::SqlEngine,
    table_name: &str,
) -> Result<bool, PgError> {
    match catalog.table_uses_global_visibility(table_name) {
        Ok(uses_global_visibility) => Ok(uses_global_visibility),
        Err(ExecError::Catalog(crabka_pgcatalog::CatalogError::UndefinedTable(_))) => Ok(false),
        Err(error) => Err(error.into_pg()),
    }
}

fn catalog_table(
    catalog: &crabka_pgexec::SqlEngine,
    table_name: &str,
) -> Result<crabka_pgcatalog::Table, PgError> {
    catalog
        .catalog_table(table_name)
        .map_err(ExecError::into_pg)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableRef {
    name: String,
    table_id: TableId,
}

fn reject_unsupported_cross_range_statement(
    range_map: &RangeMap,
    catalog: &crabka_pgexec::SqlEngine,
    table_refs: &[TableRef],
) -> Result<(), PgError> {
    if table_refs.len() <= 1 {
        return Ok(());
    }
    let ranges = table_refs
        .iter()
        .map(|table_ref| {
            RouteIntent::DataManipulation {
                table_id: table_ref.table_id,
            }
            .route(range_map)
            .map(|route| route.range_id)
            .map_err(|error| map_error_to_pg(&error))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if ranges.windows(2).all(|pair| pair[0] == pair[1]) {
        return Ok(());
    }
    let all_global_visibility = table_refs.iter().try_fold(true, |all_global, table_ref| {
        let uses_global = match catalog.table_uses_global_visibility(&table_ref.name) {
            Ok(uses_global) => uses_global,
            Err(ExecError::Catalog(crabka_pgcatalog::CatalogError::UndefinedTable(_))) => false,
            Err(error) => return Err(error.into_pg()),
        };
        Ok(all_global && uses_global)
    })?;
    if all_global_visibility {
        return Ok(());
    }
    Err(PgError::error(
        sqlstate::FEATURE_NOT_SUPPORTED,
        "cross-range single statements require all referenced tables to use global visibility",
    ))
}

fn starts_with_any(value: &str, prefixes: &[&str]) -> bool {
    prefixes.iter().any(|prefix| value.starts_with(prefix))
}

fn table_refs_in_statement(sql: &str) -> Vec<TableRef> {
    let tokens = sql
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    let mut refs = Vec::new();
    for window in tokens.windows(2) {
        let [keyword, table] = window else {
            continue;
        };
        if !matches!(*keyword, "from" | "into" | "update" | "join") {
            continue;
        }
        if trailing_table_id(table).is_some()
            && !refs
                .iter()
                .any(|table_ref: &TableRef| table_ref.name == *table)
        {
            refs.push(TableRef {
                name: (*table).to_string(),
                table_id: routing_table_id(table),
            });
        }
    }
    refs
}

fn routing_table_id(table: &str) -> TableId {
    trailing_table_id(table).unwrap_or(TableId::ZERO)
}

fn trailing_table_id(table: &str) -> Option<TableId> {
    let digits = table
        .chars()
        .rev()
        .take_while(char::is_ascii_digit)
        .collect::<Vec<_>>();
    if digits.is_empty() {
        return Some(TableId::ZERO);
    }
    let value = digits.into_iter().rev().collect::<String>();
    value.parse::<u64>().ok().map(TableId::new)
}

fn statement_rowid_keys(sql: &str, kind: StatementKind) -> Result<Vec<u64>, PgError> {
    if kind == StatementKind::Dml && sql.starts_with("insert") {
        return inserted_rowid_keys(sql);
    }
    if identifier_equals_parameter(sql, "id") {
        return Err(parameterized_shard_key_error());
    }
    Ok(identifier_equals_integer(sql, "id").into_iter().collect())
}

fn sql_hash_values(
    sql: &str,
    kind: StatementKind,
    table: &crabka_pgcatalog::Table,
    hash: &crabka_pgcatalog::HashSharding,
) -> Result<Vec<Vec<u8>>, PgError> {
    if kind == StatementKind::Dml && sql.starts_with("insert") {
        return inserted_hash_values(sql, table, hash);
    }
    if hash
        .columns
        .iter()
        .any(|column| identifier_equals_parameter(sql, column))
    {
        return Err(parameterized_shard_key_error());
    }
    Ok(sql_hash_equality_value(sql, table, hash)
        .into_iter()
        .collect())
}

fn sql_hash_equality_value(
    sql: &str,
    table: &crabka_pgcatalog::Table,
    hash: &crabka_pgcatalog::HashSharding,
) -> Option<Vec<u8>> {
    let mut bytes = Vec::new();
    for column in &hash.columns {
        let column_index = table.column_index(column)?;
        let column_type = table.columns.get(column_index)?.ty;
        let value = sql_hash_column_value(sql, column, column_type, column_index)?;
        bytes.extend(value);
    }
    Some(bytes)
}

fn inserted_hash_values(
    sql: &str,
    table: &crabka_pgcatalog::Table,
    hash: &crabka_pgcatalog::HashSharding,
) -> Result<Vec<Vec<u8>>, PgError> {
    let Some(values_index) = sql.find("values") else {
        return Err(unknown_shard_key_error());
    };
    let hash_columns = hash
        .columns
        .iter()
        .map(|column| {
            let default_column_index = table
                .column_index(column)
                .ok_or_else(unknown_shard_key_error)?;
            let column_type = table
                .columns
                .get(default_column_index)
                .map(|column| column.ty)
                .ok_or_else(unknown_shard_key_error)?;
            let tuple_column_index = insert_tuple_column_index(
                &sql[..values_index],
                column,
                default_column_index,
                unknown_shard_key_error,
            )?;
            Ok((tuple_column_index, column_type))
        })
        .collect::<Result<Vec<_>, PgError>>()?;
    let hash_values = values_tuples(&sql[values_index + "values".len()..])
        .map(|tuple| {
            let mut bytes = Vec::new();
            for (column_index, column_type) in &hash_columns {
                let field =
                    tuple_field(tuple, *column_index).ok_or_else(unknown_shard_key_error)?;
                if is_parameter_marker(field) {
                    return Err(parameterized_shard_key_error());
                }
                let value =
                    literal_hash_bytes(field, *column_type).ok_or_else(unknown_shard_key_error)?;
                bytes.extend(value);
            }
            Ok(bytes)
        })
        .collect::<Result<Vec<_>, PgError>>()?;
    if hash_values.is_empty() {
        return Err(unknown_shard_key_error());
    }
    Ok(hash_values)
}

fn sql_hash_column_value(
    sql: &str,
    column: &str,
    column_type: crabka_pgtypes::ColumnType,
    default_column_index: usize,
) -> Option<Vec<u8>> {
    if let Some(value) = inserted_hash_column_value(sql, column, column_type, default_column_index)
    {
        return Some(value);
    }
    match column_type {
        crabka_pgtypes::ColumnType::Int4 => identifier_equals_integer(sql, column)
            .and_then(|value| i32::try_from(value).ok())
            .map(|value| value.to_be_bytes().to_vec()),
        crabka_pgtypes::ColumnType::Int8 => identifier_equals_integer(sql, column)
            .and_then(|value| i64::try_from(value).ok())
            .map(|value| value.to_be_bytes().to_vec()),
        crabka_pgtypes::ColumnType::Text => {
            identifier_equals_quoted_text(sql, column).map(String::into_bytes)
        }
        _ => None,
    }
}

fn inserted_hash_column_value(
    sql: &str,
    column: &str,
    column_type: crabka_pgtypes::ColumnType,
    default_column_index: usize,
) -> Option<Vec<u8>> {
    let values_index = sql.find("values")?;
    let column_index = match insert_column_position(&sql[..values_index], column) {
        InsertColumnPosition::ImplicitTableOrder => default_column_index,
        InsertColumnPosition::ExplicitTupleIndex(index) => index,
        InsertColumnPosition::ExplicitColumnMissing => return None,
    };
    let field = values_tuples(&sql[values_index + "values".len()..])
        .next()
        .and_then(|tuple| tuple_field(tuple, column_index))?;
    literal_hash_bytes(field, column_type)
}

fn literal_hash_bytes(value: &str, column_type: crabka_pgtypes::ColumnType) -> Option<Vec<u8>> {
    match column_type {
        crabka_pgtypes::ColumnType::Int4 => parse_integer_literal(value)
            .and_then(|value| i32::try_from(value).ok())
            .map(|value| value.to_be_bytes().to_vec()),
        crabka_pgtypes::ColumnType::Int8 => parse_integer_literal(value)
            .and_then(|value| i64::try_from(value).ok())
            .map(|value| value.to_be_bytes().to_vec()),
        crabka_pgtypes::ColumnType::Text => parse_quoted_text(value).map(String::into_bytes),
        _ => None,
    }
}

fn identifier_equals_integer(sql: &str, identifier: &str) -> Option<u64> {
    let mut rest = sql;
    while let Some(index) = rest.find(identifier) {
        let before = rest[..index].chars().next_back();
        let after_index = index + identifier.len();
        let after = rest[after_index..].chars().next();
        if is_identifier_boundary(before) && is_identifier_boundary(after) {
            let after_identifier = rest[after_index..].trim_start();
            if let Some(after_equals) = after_identifier.strip_prefix('=') {
                return parse_leading_u64(after_equals.trim_start());
            }
        }
        rest = &rest[after_index..];
    }
    None
}

fn identifier_equals_quoted_text(sql: &str, identifier: &str) -> Option<String> {
    let mut rest = sql;
    while let Some(index) = rest.find(identifier) {
        let before = rest[..index].chars().next_back();
        let after_index = index + identifier.len();
        let after = rest[after_index..].chars().next();
        if is_identifier_boundary(before) && is_identifier_boundary(after) {
            let after_identifier = rest[after_index..].trim_start();
            if let Some(after_equals) = after_identifier.strip_prefix('=') {
                return parse_quoted_text(after_equals.trim_start());
            }
        }
        rest = &rest[after_index..];
    }
    None
}

fn identifier_equals_parameter(sql: &str, identifier: &str) -> bool {
    let mut rest = sql;
    while let Some(index) = rest.find(identifier) {
        let before = rest[..index].chars().next_back();
        let after_index = index + identifier.len();
        let after = rest[after_index..].chars().next();
        if is_identifier_boundary(before) && is_identifier_boundary(after) {
            let after_identifier = rest[after_index..].trim_start();
            if let Some(after_equals) = after_identifier.strip_prefix('=') {
                return is_parameter_marker(after_equals.trim_start());
            }
        }
        rest = &rest[after_index..];
    }
    false
}

fn is_parameter_marker(value: &str) -> bool {
    let Some(value) = value.trim_start().strip_prefix('$') else {
        return false;
    };
    value
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_digit())
}

fn parse_quoted_text(value: &str) -> Option<String> {
    let value = value.strip_prefix('\'')?;
    let end = value.find('\'')?;
    Some(value[..end].to_string())
}

fn inserted_rowid_keys(sql: &str) -> Result<Vec<u64>, PgError> {
    let Some(values_index) = sql.find("values") else {
        return Err(row_shard_key_error());
    };
    let id_column_index =
        insert_tuple_column_index(&sql[..values_index], "id", 0, row_shard_key_error)?;
    let rowids = values_tuples(&sql[values_index + "values".len()..])
        .map(|tuple| {
            let field = tuple_field(tuple, id_column_index).ok_or_else(row_shard_key_error)?;
            if is_parameter_marker(field) {
                return Err(parameterized_shard_key_error());
            }
            parse_integer_literal(field).ok_or_else(row_shard_key_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if rowids.is_empty() {
        return Err(row_shard_key_error());
    }
    Ok(rowids)
}

fn insert_tuple_column_index(
    prefix: &str,
    column_name: &str,
    default_column_index: usize,
    missing_column_error: fn() -> PgError,
) -> Result<usize, PgError> {
    match insert_column_position(prefix, column_name) {
        InsertColumnPosition::ImplicitTableOrder => Ok(default_column_index),
        InsertColumnPosition::ExplicitTupleIndex(index) => Ok(index),
        InsertColumnPosition::ExplicitColumnMissing => Err(missing_column_error()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InsertColumnPosition {
    ImplicitTableOrder,
    ExplicitTupleIndex(usize),
    ExplicitColumnMissing,
}

fn insert_column_position(prefix: &str, column_name: &str) -> InsertColumnPosition {
    let Some(open) = prefix.find('(') else {
        return InsertColumnPosition::ImplicitTableOrder;
    };
    let Some(close) = prefix[open + 1..].find(')') else {
        return InsertColumnPosition::ImplicitTableOrder;
    };
    prefix[open + 1..open + 1 + close]
        .split(',')
        .map(str::trim)
        .position(|column| column == column_name)
        .map_or(
            InsertColumnPosition::ExplicitColumnMissing,
            InsertColumnPosition::ExplicitTupleIndex,
        )
}

fn values_tuples(values_sql: &str) -> impl Iterator<Item = &str> {
    values_sql
        .split('(')
        .skip(1)
        .filter_map(|suffix| suffix.split_once(')').map(|(tuple, _)| tuple))
}

fn tuple_field(tuple: &str, index: usize) -> Option<&str> {
    tuple.split(',').map(str::trim).nth(index)
}

fn parse_integer_literal(value: &str) -> Option<u64> {
    let value = value.strip_prefix('+').unwrap_or(value);
    parse_leading_u64(value).filter(|_| {
        value
            .chars()
            .all(|character| character.is_ascii_digit() || character.is_ascii_whitespace())
    })
}

fn parse_leading_u64(value: &str) -> Option<u64> {
    let digits = value
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
}

fn datum_hash_bytes(value: &Datum) -> Option<Vec<u8>> {
    match value {
        Datum::Bool(value) => Some(vec![u8::from(*value)]),
        Datum::Int4(value) => Some(value.to_be_bytes().to_vec()),
        Datum::Int8(value) => Some(value.to_be_bytes().to_vec()),
        Datum::Text(value) => Some(value.as_bytes().to_vec()),
        Datum::Bytea(value) => Some(value.clone()),
        Datum::Null
        | Datum::Float8(_)
        | Datum::Numeric(_)
        | Datum::Date(_)
        | Datum::Time(_)
        | Datum::Timestamp(_)
        | Datum::Timestamptz(_)
        | Datum::Interval(_) => None,
    }
}

fn is_identifier_boundary(character: Option<char>) -> bool {
    character.is_none_or(|character| !character.is_ascii_alphanumeric() && character != '_')
}

fn map_error_to_pg(error: &crate::MapValidationError) -> PgError {
    PgError::error(sqlstate::FEATURE_NOT_SUPPORTED, error.to_string())
}

fn parameterized_shard_key_error() -> PgError {
    PgError::error(
        sqlstate::FEATURE_NOT_SUPPORTED,
        "parameterized shard keys cannot be routed by the gateway",
    )
}

fn unknown_shard_key_error() -> PgError {
    PgError::error(
        sqlstate::FEATURE_NOT_SUPPORTED,
        "hash-sharded statements require statically known shard keys",
    )
}

fn row_shard_key_error() -> PgError {
    PgError::error(
        sqlstate::FEATURE_NOT_SUPPORTED,
        "row-sharded INSERT requires statically known id shard keys",
    )
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgkv::{Kv, MemKv};

    use super::*;

    fn tenant() -> TenantName {
        TenantName::parse("tenant_a").expect("tenant")
    }

    struct EmptyRange0End;

    #[async_trait::async_trait]
    impl crate::barrier::Range0EndSampler for EmptyRange0End {
        async fn sample_end_after_call_begins(&self) -> Result<i64, crate::barrier::BarrierError> {
            Ok(-1)
        }
    }

    #[test]
    fn rn_only_assembly_requires_and_uses_read_only_range0_replica() {
        let range0_kv = Arc::new(MemKv::default());
        let replica = ReadOnlyRange0Replica::new(
            crate::range0_tail::Range0Tail::new(range0_kv),
            Arc::new(EmptyRange0End),
        );
        let config = MultiRangeTenantConfig::from_boundaries(tenant(), "0,100")
            .expect("config")
            .with_hosted_ranges(vec![RangeId::new(1)])
            .expect("r1 host")
            .with_read_only_range0_replica(replica);

        let (gateway, _) = MultiRangeTenant::start_with_engine_factory(config, |_dir, range_id| {
            assert!(range_id == RangeId::new(1));
            Ok(SqlEngine::new())
        })
        .expect("rN-only assembly");

        let engines = gateway.hosted_range_engines();
        assert!(engines.contains_key(&RangeId::new(1)));
        assert!(!engines.contains_key(&RangeId::COORDINATOR));
        assert!(!engines[&RangeId::new(1)].has_gtm());
    }

    #[tokio::test]
    async fn rn_only_replica_binds_barrier_to_follower_catalog_and_rejects_timestamp_dml() {
        let follower_kv: Arc<dyn Kv> = Arc::new(MemKv::default());
        let unrelated_kv: Arc<dyn Kv> = Arc::new(MemKv::default());
        let follower_catalog = SqlEngine::with_kv(Arc::clone(&follower_kv)).expect("catalog");
        let unrelated_catalog =
            SqlEngine::with_kv(Arc::clone(&unrelated_kv)).expect("unrelated catalog");
        follower_catalog
            .connect()
            .simple_query("CREATE TABLE follower_table (id int4) SHARDED")
            .await
            .expect("follower catalog table");
        unrelated_catalog
            .connect()
            .simple_query("CREATE TABLE unrelated_table (id int4) SHARDED")
            .await
            .expect("unrelated catalog table");

        let replica = ReadOnlyRange0Replica::new(
            crate::range0_tail::Range0Tail::new(Arc::clone(&follower_kv)),
            Arc::new(EmptyRange0End),
        );
        let config = MultiRangeTenantConfig::from_boundaries(tenant(), "0:0,0:1")
            .expect("config")
            .with_hosted_ranges(vec![RangeId::new(1)])
            .expect("r1 host")
            .with_read_only_range0_replica(replica);
        let (_gateway, handles) =
            MultiRangeTenant::start_with_engine_factory(config, |_dir, _id| Ok(SqlEngine::new()))
                .expect("rN-only assembly");

        let range1 = &handles.inner.serving.load().engines[&RangeId::new(1)];
        assert!(range1.catalog_table("follower_table").is_ok());
        assert!(range1.catalog_table("unrelated_table").is_err());

        let error = range1
            .connect()
            .simple_query("INSERT INTO follower_table VALUES (1)")
            .await
            .expect_err("rN-only timestamp DML must fail closed");
        assert_eq!(error.code, sqlstate::FEATURE_NOT_SUPPORTED);
        assert!(
            error
                .message
                .contains("range-0 timestamp oracle is unavailable"),
            "unexpected rN-only DML error: {error:?}"
        );
    }

    #[test]
    fn boundaries_parse_into_half_open_ranges() {
        let config = MultiRangeTenantConfig::from_boundaries(tenant(), "0,100,200").expect("cfg");

        assert_eq!(config.range_map.ranges().len(), 3);
        assert_eq!(
            config
                .range_map
                .route_table(TableId::new(150))
                .expect("route")
                .range_id,
            RangeId::new(1)
        );
    }

    #[test]
    fn boundaries_parse_rowid_split_points() {
        let config =
            MultiRangeTenantConfig::from_boundaries(tenant(), "0:0,100:25,100:50").expect("cfg");

        assert_eq!(
            config
                .range_map
                .range_for_key(TableId::new(100), 24)
                .expect("route")
                .range_id,
            RangeId::COORDINATOR
        );
        assert_eq!(
            config
                .range_map
                .range_for_key(TableId::new(100), 25)
                .expect("route")
                .range_id,
            RangeId::new(1)
        );
    }

    #[test]
    fn hosted_ranges_allow_remote_range_zero_until_replica_is_supplied() {
        let config = MultiRangeTenantConfig::from_boundaries(tenant(), "0,100,200")
            .expect("cfg")
            .with_hosted_ranges(vec![RangeId::new(2)])
            .expect("rN-only configuration is parsed");

        assert!(config.hosted_ranges == Some(vec![RangeId::new(2)]));
    }

    #[test]
    fn hosted_ranges_reject_missing_ids() {
        let error = MultiRangeTenantConfig::from_boundaries(tenant(), "0,100")
            .expect("cfg")
            .with_hosted_ranges(vec![RangeId::COORDINATOR, RangeId::new(2)])
            .expect_err("missing range rejected");

        assert!(
            matches!(error, TenantError::HostedRangeMissing(range) if range == RangeId::new(2))
        );
    }

    #[test]
    fn start_rejects_config_that_omits_local_range_zero() {
        let mut config =
            MultiRangeTenantConfig::from_boundaries(tenant(), "0,100,200").expect("cfg");
        config.hosted_ranges = Some(vec![RangeId::new(2)]);

        let Err(error) =
            MultiRangeTenant::start_with_engine_factory(config, |_data_dir, _range_id| {
                panic!("range engine must not open when range zero is remote")
            })
        else {
            panic!("remote range zero must be rejected at startup");
        };

        assert!(matches!(error, TenantError::MissingRangeZeroReplica));
    }

    #[test]
    fn local_range_zero_assembles_requested_ranges() {
        let config = MultiRangeTenantConfig::from_boundaries(tenant(), "0,100,200")
            .expect("cfg")
            .with_hosted_ranges(vec![RangeId::COORDINATOR, RangeId::new(2)])
            .expect("hosted ranges");

        assert_eq!(
            config.hosted_ranges,
            Some(vec![RangeId::COORDINATOR, RangeId::new(2)])
        );

        let mut opened = Vec::new();
        let (_gateway, handles) =
            MultiRangeTenant::start_with_engine_factory(config, |_data_dir, range_id| {
                opened.push(range_id);
                Ok(SqlEngine::new())
            })
            .expect("tenant starts with local range zero");

        assert_eq!(opened, vec![RangeId::COORDINATOR, RangeId::new(2)]);
        assert_eq!(handles.inner.serving.load().engines.len(), 2);
        assert!(
            handles
                .inner
                .serving
                .load()
                .engines
                .contains_key(&RangeId::COORDINATOR)
        );
        assert!(
            handles
                .inner
                .serving
                .load()
                .engines
                .contains_key(&RangeId::new(2))
        );
    }

    #[test]
    fn start_with_engine_factory_is_explicit_per_range_injection_seam() {
        let config = MultiRangeTenantConfig::from_boundaries(tenant(), "0,100,200")
            .expect("cfg")
            .with_hosted_ranges(vec![RangeId::COORDINATOR, RangeId::new(2)])
            .expect("hosted ranges");
        let mut opened = Vec::new();

        let (_gateway, handles) =
            MultiRangeTenant::start_with_engine_factory(config, |_data_dir, range_id| {
                opened.push(range_id);
                Ok(SqlEngine::new())
            })
            .expect("tenant starts with injected engines");

        assert_eq!(opened, vec![RangeId::COORDINATOR, RangeId::new(2)]);
        assert_eq!(handles.inner.serving.load().engines.len(), 2);
        assert!(
            handles
                .inner
                .serving
                .load()
                .engines
                .contains_key(&RangeId::COORDINATOR)
        );
        assert!(
            handles
                .inner
                .serving
                .load()
                .engines
                .contains_key(&RangeId::new(2))
        );
    }

    #[test]
    fn durable_timestamp_recovery_aborts_intents_and_removes_all_sidecars_idempotently() {
        let coordinator_kv: Arc<dyn Kv> = Arc::new(crabka_pgkv::MemKv::new());
        let participant_kv: Arc<dyn Kv> = Arc::new(crabka_pgkv::MemKv::new());
        let coordinator = SqlEngine::with_kv(Arc::clone(&coordinator_kv)).expect("coordinator");
        let mut participant = SqlEngine::with_kv(Arc::clone(&participant_kv)).expect("participant");
        participant.set_catalog_kv(Arc::clone(&coordinator_kv));

        let start_ts = crabka_pgexec::TimestampTransactionId::new(10).expect("start timestamp");
        let identity = crabka_pgexec::TimestampTxnIdentity {
            start_ts,
            global_xid: 9,
            primary_range: RangeId::COORDINATOR.as_u32(),
        };
        let write = crabka_pgexec::TimestampWrite {
            table_id: 10,
            rowid: 11,
            row: vec![Datum::Int4(12)],
            delete: false,
            global_index_intents: vec![crabka_pgexec::timestamp_txn::GlobalIndexIntent {
                index_id: 1,
                indexed_values: vec![Datum::Int4(12)],
                base_table_id: 10,
                base_rowid: 11,
                unique: false,
                delete: false,
            }],
        };

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            coordinator
                .begin_timestamp_transaction(&crabka_pgexec::TimestampTxnDescriptor::begun(
                    start_ts,
                    identity.global_xid,
                    vec![1],
                ))
                .await
                .expect("descriptor");
            participant
                .timestamp_txn_participant(1)
                .prewrite_with_primary(identity, std::slice::from_ref(&write))
                .await
                .expect("prewrite");
        });
        drop(runtime);
        assert!(
            !participant_kv
                .scan_prefix(b"\0\0\0\0index/ts_intent/")
                .expect("scan prewritten global-index intents")
                .is_empty()
        );

        let mut engines = BTreeMap::new();
        engines.insert(RangeId::COORDINATOR, coordinator);
        engines.insert(RangeId::new(1), participant);

        recover_durable_timestamp_transactions(&engines).expect("first recovery");
        assert!(
            participant_kv
                .scan_prefix(b"\0\0\0\0index/ts_intent/")
                .expect("scan global-index intents after first recovery")
                .is_empty()
        );
        recover_durable_timestamp_transactions(&engines).expect("repeat recovery");

        let tuple_key =
            crabka_pgmvcc::version::version_key_ts(write.table_id, write.rowid, start_ts.get());
        let tuple = participant_kv
            .get(&tuple_key)
            .expect("read settled tuple")
            .expect("settled tuple");
        let tuple = crabka_pgmvcc::version::decode_ts_tuple(&tuple).expect("decode tuple");
        assert!(tuple.state == crabka_pgmvcc::version::TsVersionState::Aborted);
        assert!(
            participant_kv
                .scan_prefix(b"\0\0\0\0meta/ts_prewrite/")
                .expect("scan reservations")
                .is_empty()
        );
        assert!(
            participant_kv
                .scan_prefix(b"\0\0\0\0meta/ts_intent/")
                .expect("scan identities")
                .is_empty()
        );
        assert!(
            participant_kv
                .scan_prefix(b"\0\0\0\0index/ts_intent/")
                .expect("scan global-index intents after repeat recovery")
                .is_empty()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hosted_ranges_share_one_timestamp_oracle_for_sharded_writes() {
        let config = MultiRangeTenantConfig::from_boundaries(tenant(), "0:0,0:50").expect("cfg");
        let (gateway, handles) = MultiRangeTenant::start(config).expect("tenant starts");
        let mut session = gateway.connect();

        session
            .simple_query("CREATE TABLE t (id int4, v text) SHARDED")
            .await
            .expect("create sharded table");
        session
            .simple_query("INSERT INTO t (id, v) VALUES (10, 'range0')")
            .await
            .expect("insert range 0");
        session
            .simple_query("INSERT INTO t (id, v) VALUES (60, 'range1')")
            .await
            .expect("insert range 1");
        session
            .simple_query("INSERT INTO t (id, v) VALUES (11, 'range0-b')")
            .await
            .expect("insert range 0 again");

        let serving = handles.inner.serving.load();
        let table = serving
            .engines
            .get(&RangeId::COORDINATOR)
            .expect("range 0")
            .catalog_table("t")
            .expect("table");
        let range0_versions = committed_timestamp_versions(
            serving.engines.get(&RangeId::COORDINATOR).expect("range 0"),
            table.id,
        );
        let range1_versions = committed_timestamp_versions(
            serving.engines.get(&RangeId::new(1)).expect("range 1"),
            table.id,
        );

        assert!(range0_versions.len() == 2);
        assert!(range1_versions.len() == 1);
        let mut all_versions = range0_versions
            .into_iter()
            .chain(range1_versions)
            .collect::<Vec<_>>();
        all_versions.sort_unstable_by_key(|version| version.commit_ts);

        assert!(
            all_versions
                == vec![
                    TimestampVersion::new(1, 2),
                    TimestampVersion::new(3, 4),
                    TimestampVersion::new(5, 6)
                ]
        );
    }

    #[tokio::test]
    async fn fenced_range_zero_timestamp_oracle_fails_clear() {
        let kv = Arc::new(crabka_pgkv::MemKv::new());
        let horizon = MemoryTsoHorizon::new(kv, 1);
        let oracle = Arc::new(
            TsoOracle::recover(
                horizon.clone(),
                horizon.clone(),
                1,
                NonZeroU64::new(8).expect("stride"),
                0,
            )
            .expect("oracle"),
        );
        horizon.set_live_epoch(2).await;
        let rpc = Arc::new(InProcessTsoRpc { oracle });
        let timestamp_oracle = PgexecTsoOracle {
            client: BatchedTsoClient::new(rpc),
        };

        let error = crabka_pgexec::TimestampOracle::allocate_transaction_id(&timestamp_oracle)
            .await
            .expect_err("fenced oracle rejects grants");

        assert!(error.to_string().contains("fenced"));
    }

    #[tokio::test]
    async fn statement_errors_abort_open_gateway_transactions_until_rollback() {
        let config = MultiRangeTenantConfig::from_boundaries(tenant(), "0:0,0:50").expect("cfg");
        let (gateway, _) = MultiRangeTenant::start(config).expect("tenant starts");
        let mut session = gateway.connect();
        session
            .simple_query("CREATE TABLE t (id int4)")
            .await
            .expect("create local table");
        session
            .simple_query("CREATE TABLE s (id int4) SHARDED")
            .await
            .expect("create sharded table");

        for (statement, expected_code) in [
            ("SELECT * FROM missing", "42P01"),
            ("CREATE TABLE t (id int4)", "42P07"),
            ("INSERT INTO s VALUES (10), (60)", "0A000"),
            ("SELECT missing_column FROM t", "42703"),
        ] {
            session
                .simple_query("BEGIN")
                .await
                .expect("begin transaction");
            let error = session
                .simple_query(statement)
                .await
                .expect_err("statement fails");
            assert_eq!(error.code, expected_code);
            assert_eq!(session.tx_status(), TxStatus::Failed);

            let error = session
                .simple_query("SELECT 1")
                .await
                .expect_err("failed transaction rejects later statements");
            assert_eq!(error.code, sqlstate::IN_FAILED_SQL_TRANSACTION);
            session
                .simple_query("ROLLBACK")
                .await
                .expect("rollback clears failed transaction");
            assert_eq!(session.tx_status(), TxStatus::Idle);
        }
    }

    #[tokio::test]
    async fn unhosted_remote_query_never_falls_back_to_range_zero() {
        let config = MultiRangeTenantConfig::from_boundaries(tenant(), "0:0,0:50")
            .expect("cfg")
            .with_hosted_ranges(vec![RangeId::COORDINATOR])
            .expect("hosted ranges");
        let (gateway, _) = MultiRangeTenant::start(config).expect("tenant starts");
        let mut session = gateway.connect();
        session
            .simple_query("CREATE TABLE s (id int4) SHARDED")
            .await
            .expect("create sharded table");

        let error = session
            .simple_query("SELECT * FROM s WHERE id = 60")
            .await
            .expect_err("remote query cannot use range zero");

        assert_eq!(error.code, sqlstate::FEATURE_NOT_SUPPORTED);
        assert!(error.message.contains("range r1 is not hosted"));
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TimestampVersion {
        start_ts: u64,
        commit_ts: u64,
    }

    impl TimestampVersion {
        const fn new(start_ts: u64, commit_ts: u64) -> Self {
            Self {
                start_ts,
                commit_ts,
            }
        }
    }

    fn committed_timestamp_versions(engine: &SqlEngine, table_id: u32) -> Vec<TimestampVersion> {
        engine
            .kv_handle()
            .scan_prefix(&crabka_pgkv::key::table_prefix(table_id))
            .expect("scan table")
            .into_iter()
            .filter_map(|(_key, value)| {
                let version = crabka_pgmvcc::version::decode_ts_tuple(&value).ok()?;
                let crabka_pgmvcc::version::TsVersionState::Committed { commit_ts } = version.state
                else {
                    return None;
                };
                Some(TimestampVersion::new(version.start_ts, commit_ts))
            })
            .collect()
    }
}
