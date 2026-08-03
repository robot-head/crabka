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
        BoundParam, CloseTarget, Engine, ExecuteOutcome, Notification, PortalDescription,
        PreparedDescription, QueryResult, Session, TxStatus,
    },
    error::{PgError, sqlstate},
};
use crabka_units::convert::TimeExt as _;
use tokio::sync::{Mutex, RwLock};

use crate::{
    CheckpointManifest, HashShardSpec, MapEpoch, RangeId, RangeKey, RangeMap, RangeScanSegment,
    RangeSpec, RangeTransferCapability, RangeTransferError, RouteIntent,
    RowInterval as MapRowInterval, SplitCommand, SplitError, SplitHooks, SplitState,
    SplitStateStore, TableId, TenantName, ValidatedSplitTransferPlan,
    barrier::{BarrierError, Range0Barrier, Range0EndSampler},
    coordinator::{LocalCoordinator, LocalCoordinatorError, TransactionDecision},
    forward::{
        ForwardError, RegistryRangeScanner, RegistryRemoteForward, RegistryTsoRpc, RemoteForward,
        RemoteRangeSession, canonicalize_timestamp_operations,
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
    /// Whether tenant assembly should settle durable timestamp transactions immediately.
    /// Activation recovery disables this until successor publication owns recovery.
    pub recover_timestamps_on_start: bool,
    /// Which timestamp-ordering source this tenant installs on its engines.
    pub timestamp_source_mode: TimestampSourceMode,
    /// This node's identity for cross-gateway `NOTIFY`, and the switch that
    /// turns it on.
    ///
    /// `Some` opts the node in: the coordinator engine stamps every committed
    /// notification with this string and appends it to the range-0 log, and a
    /// node without range 0 re-injects the records its follower tail reads,
    /// skipping the ones carrying its own identity. It must therefore differ
    /// between the nodes of one tenant — the live runtime passes its
    /// authenticated range-listen endpoint.
    ///
    /// `None` keeps notifications in-process. That is the required setting for
    /// a tenant whose engines commit straight to their KV rather than through
    /// the substrate WAL: only the WAL apply paths drop notify records, so
    /// replicating without one would write a notification into the catalog
    /// store, where checkpoints would keep it forever.
    pub node_identity: Option<String>,
    /// Signed wall-clock skew in milliseconds applied to this node's HLC reads.
    ///
    /// Fault-injection knob for load and chaos testing, not a production
    /// setting: when [`TimestampSourceMode::Hlc`] is installed, the system
    /// clock is wrapped in a [`crabka_pgexec::SkewedWallClock`] so this node
    /// mints stamps as if its wall clock ran ahead of (positive) or behind
    /// (negative) its peers. Node-local by design — unlike the mode, it need
    /// not match across nodes — and ignored under
    /// [`TimestampSourceMode::LogicalTso`].
    pub hlc_wall_offset_ms: i64,
    /// Distributed range limits and pacing.
    pub runtime_policy: crate::RangeRuntimePolicy,
    #[doc(hidden)]
    pub commit_fault_for_testing: Option<GatewayCommitFault>,
    #[doc(hidden)]
    pub empty_table_split_test_hook: Option<EmptyTableSplitTestHook>,
}

/// The timestamp-ordering source a tenant installs, chosen at provision time.
///
/// Mode is explicit tenant configuration, not inferred from topology: a
/// distributed topology running [`LogicalTso`](TimestampSourceMode::LogicalTso)
/// is a legitimate single-zone-HA configuration, and promotion to
/// [`Hlc`](TimestampSourceMode::Hlc) is an administrative act. The existing
/// topology-based selection (in-process oracle vs. registry-forwarded vs.
/// unavailable) still decides *how* the chosen mode is wired for this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampSourceMode {
    /// Centralized range-0 logical oracle — the solo default. Timestamps flow
    /// through one authority with its successor grace period, stride batching,
    /// and epoch fencing.
    #[default]
    LogicalTso,
    /// Node-local Hybrid Logical Clock. Each node mints stamps from its own
    /// clock with no RPC. A single `HlcTimestampSource` fanned to every engine
    /// is correct on its own (one authority, so no cross-node skew); multi-node
    /// stamping and uncertainty-window read-restart are documented follow-up.
    Hlc {
        /// Maximum tolerated clock offset in milliseconds; sizes the read
        /// uncertainty window.
        max_offset_ms: u64,
    },
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
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
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
            recover_timestamps_on_start: true,
            node_identity: None,
            timestamp_source_mode: TimestampSourceMode::default(),
            hlc_wall_offset_ms: 0,
            runtime_policy: crate::RangeRuntimePolicy::default(),
            commit_fault_for_testing: None,
            empty_table_split_test_hook: None,
        })
    }

    /// Bind the initial serving map to the authoritative registry record version.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn with_map_epoch(mut self, epoch: MapEpoch) -> Result<Self, TenantError> {
        self.range_map =
            RangeMap::new(self.tenant.clone(), epoch, self.range_map.ranges().to_vec())?;
        Ok(self)
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
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
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

    /// Defer timestamp recovery while durable split activation is being resumed.
    #[must_use]
    pub fn defer_timestamp_recovery(mut self) -> Self {
        self.recover_timestamps_on_start = false;
        self
    }

    /// Replicate this node's notifications through the range-0 log, stamped
    /// with `identity`.
    ///
    /// Must differ between the nodes of one tenant: it is how a node recognises
    /// the records it published itself. Only for a tenant whose ranges commit
    /// through the substrate WAL — see [`Self::node_identity`].
    #[must_use]
    pub fn with_node_identity(mut self, identity: impl Into<String>) -> Self {
        self.node_identity = Some(identity.into());
        self
    }

    /// Select the timestamp-ordering source this tenant installs.
    #[must_use]
    pub fn with_timestamp_source_mode(mut self, mode: TimestampSourceMode) -> Self {
        self.timestamp_source_mode = mode;
        self
    }

    /// Skew this node's HLC wall-clock reads by a signed millisecond offset.
    ///
    /// Fault-injection knob for load and chaos testing, not production
    /// configuration; ignored unless [`TimestampSourceMode::Hlc`] is selected.
    #[must_use]
    pub fn with_hlc_wall_offset_ms(mut self, hlc_wall_offset_ms: i64) -> Self {
        self.hlc_wall_offset_ms = hlc_wall_offset_ms;
        self
    }

    /// Apply distributed range runtime policy.
    #[must_use]
    pub fn with_runtime_policy(mut self, policy: crate::RangeRuntimePolicy) -> Self {
        self.runtime_policy = policy;
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
    TimestampSource(TsoError),
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
    tail: crate::Range0Tail,
    refresh_poke: Option<Arc<tokio::sync::Notify>>,
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
        Self::new_with_policy(tail, sampler, &crate::RangeRuntimePolicy::default())
    }

    /// Bind a follower replica using explicit range runtime policy.
    #[must_use]
    pub fn new_with_policy(
        tail: crate::Range0Tail,
        sampler: Arc<dyn Range0EndSampler>,
        policy: &crate::RangeRuntimePolicy,
    ) -> Self {
        Self {
            catalog_kv: tail.store_handle(),
            barrier: Arc::new(Range0Barrier::with_timeout(
                tail.clone(),
                sampler,
                policy.range0_wait_timeout,
            )),
            tail,
            refresh_poke: None,
        }
    }

    /// Barrier handle bound to this replica's follower tail.
    #[must_use]
    pub fn barrier(&self) -> Arc<Range0Barrier> {
        Arc::clone(&self.barrier)
    }

    /// The follower tail this replica reads, for consumers of records the merge
    /// rules deliberately never store — cross-gateway `NOTIFY` being the one.
    #[must_use]
    pub fn tail(&self) -> &crate::Range0Tail {
        &self.tail
    }

    /// Rebuild with a poke that wakes the follower poll loop before sampling.
    #[must_use]
    pub fn with_catalog_refresh_poke(self, poke: Arc<tokio::sync::Notify>) -> Self {
        Self {
            catalog_kv: self.catalog_kv,
            barrier: Arc::new(
                self.barrier
                    .as_ref()
                    .clone()
                    .with_refresh_poke(Arc::clone(&poke)),
            ),
            tail: self.tail,
            refresh_poke: Some(poke),
        }
    }

    /// Wake the follower's poll loop without waiting for it.
    ///
    /// A `NOTIFY` this node forwarded to the coordinator is already durable in
    /// the range-0 log by the time the forwarded statement returns, but this
    /// node only sees it on the follower's next poll — up to its full poll
    /// period away. Poking costs nothing, never blocks the notifying client,
    /// and turns that wait into one broker round trip for the listeners on
    /// *this* node. Listeners elsewhere still learn on their own node's poll.
    fn poke_catalog_refresh(&self) {
        if let Some(poke) = &self.refresh_poke {
            // `notify_one` leaves a permit when the loop is mid-iteration, so
            // the wake cannot be lost between two polls.
            poke.notify_one();
        }
    }

    /// Wait until this replica covers every catalog write committed before this call.
    ///
    /// # Errors
    ///
    /// Returns [`BarrierError`] when the committed end cannot be sampled or the
    /// follower tail does not apply it within the barrier timeout.
    pub async fn wait_for_latest_catalog(&self) -> Result<(), BarrierError> {
        self.barrier.wait_for_fresh_end().await
    }
}

/// Re-injects the notifications carried by the range-0 log into this node's bus.
///
/// Installed on the follower tail of a node that does not host range 0. Notify
/// records never reach a KV — every apply site drops them — so this hook is the
/// only place they are visible, and it runs inline on the apply path: it does
/// exactly one non-blocking best-effort fan-out and returns.
struct NotifyTailObserver {
    bus: Arc<crabka_pgexec::notify::NotifyBus>,
    origin: String,
}

impl crate::range0_tail::Range0FrameObserver for NotifyTailObserver {
    fn observe(&self, ops: &[crabka_pgkv::WriteOp]) {
        let notifications = remote_notifications(ops, &self.origin);
        if notifications.is_empty() {
            return;
        }
        self.bus.deliver_remote(&notifications);
    }
}

/// The notifications a frame carries for delivery on a node identified by
/// `origin`.
///
/// Non-notify ops and this node's own records are skipped, and so is any record
/// that fails to decode: these bytes came off a log another node wrote, so a
/// corrupt or future-versioned record must cost one warning, not the catalog
/// apply path it runs on.
fn remote_notifications(ops: &[crabka_pgkv::WriteOp], origin: &str) -> Vec<Notification> {
    let mut notifications = Vec::new();
    for op in ops {
        if !crabka_pgkv::is_notify_op(op) {
            continue;
        }
        let (crabka_pgkv::WriteOp::Put { value, .. }
        | crabka_pgkv::WriteOp::ConditionalPut { value, .. }) = op
        else {
            continue;
        };
        match crabka_pgkv::NotifyRecord::decode(value) {
            // A node that observes its own record has already delivered it
            // locally; the leader applies its own frames without this tail, so
            // this is a safety net rather than the load-bearing check.
            Ok(record) if record.origin == origin => {}
            Ok(record) => notifications.push(Notification {
                process_id: record.process_id,
                channel: record.channel,
                payload: record.payload,
            }),
            Err(error) => {
                tracing::warn!(%error, "skipping undecodable range-0 notify record");
            }
        }
    }
    notifications
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
    /// Build a transfer plan from the currently served coordinator catalog.
    ///
    /// The returned physical-to-logical table mapping is authoritative for
    /// control-plane restores; callers must not synthesize it from routing IDs.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn validated_control_transfer_plan(
        &self,
        state: SplitState,
    ) -> Result<ValidatedSplitTransferPlan, LocalSqlSplitError> {
        let serving = self.inner.serving.load_full();
        Self::validated_transfer_plan(&serving, state)
    }

    #[must_use]
    pub fn control_range_map(&self) -> RangeMap {
        self.inner.serving.load().range_map.clone()
    }

    #[must_use]
    pub fn control_range_is_hosted(&self, range_id: RangeId) -> bool {
        self.inner.serving.load().engines.contains_key(&range_id)
    }

    /// Snapshot pending timestamp descriptors as split-transfer in-doubt markers.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn control_in_doubt_markers(
        &self,
        interval_start: RangeKey,
        interval_end: Option<RangeKey>,
    ) -> Result<Vec<crate::InDoubtMarker>, SplitError> {
        let serving = self.inner.serving.load_full();
        let owners = serving
            .range_map
            .ranges()
            .iter()
            .filter(|range| {
                interval_end.is_none_or(|end| range.start < end)
                    && range.end.is_none_or(|end| interval_start < end)
            })
            .map(|range| range.range_id)
            .collect::<Vec<_>>();
        let [owner] = owners.as_slice() else {
            return Err(SplitError::Hook(format!(
                "marker interval {interval_start:?}..{interval_end:?} must resolve to exactly one range, found {owners:?}"
            )));
        };
        let engine = serving
            .engines
            .get(owner)
            .ok_or_else(|| SplitError::Hook(format!("marker owner r{owner} is not hosted")))?;
        in_doubt_markers_for_engine(engine, interval_start, interval_end)
    }
}

/// Snapshot pending timestamp descriptors from one non-serving staged engine.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn in_doubt_markers_for_engine(
    engine: &SqlEngine,
    interval_start: RangeKey,
    interval_end: Option<RangeKey>,
) -> Result<Vec<crate::InDoubtMarker>, SplitError> {
    let routing_by_physical = crabka_pgcatalog::list_tables(engine.catalog_kv())
        .map_err(|error| SplitError::Hook(format!("list marker tables: {error}")))?
        .into_iter()
        .map(|table| (table.id, routing_table_id(&table.name)))
        .collect::<BTreeMap<_, _>>();
    let mut markers = Vec::new();
    for descriptor in engine
        .timestamp_transaction_descriptors()
        .map_err(|error| SplitError::Hook(format!("read timestamp descriptors: {error:?}")))?
    {
        if descriptor.decision != crabka_pgexec::PrimaryTxnDecision::Pending {
            continue;
        }
        for operation in descriptor.operations {
            let Some(table_id) = routing_by_physical.get(&operation.table_id).copied() else {
                continue;
            };
            let key = match operation.bucket {
                Some(bucket) => RangeKey::hash(table_id, bucket, operation.rowid),
                None => RangeKey::new(table_id, operation.rowid),
            };
            if key >= interval_start && interval_end.is_none_or(|end| key < end) {
                markers.push(crate::InDoubtMarker {
                    transaction_id: descriptor.start_ts.get(),
                    key,
                    hash_bucket: operation.bucket,
                });
            }
        }
    }
    markers.sort_unstable_by_key(|marker| (marker.transaction_id, marker.key));
    markers.dedup();
    Ok(markers)
}

impl MultiRangeTenant {
    /// Settle durable ordinary 2PC participants before publishing SQL readiness.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn recover_ordinary_globals_before_serving(&self) -> Result<(), ExecError> {
        let serving = self.inner.serving.load_full();
        let Some(coordinator) = serving.engine(RangeId::COORDINATOR) else {
            return Ok(());
        };
        let Some(forward) = &self.inner.remote_forward else {
            return Ok(());
        };
        for global_xid in coordinator.prepared_globals()? {
            let status = coordinator
                .commit_global_decision(global_xid, crabka_pgmvcc::clog::XidStatus::Aborted)
                .await?;
            let commit = status == crabka_pgmvcc::clog::XidStatus::Committed;
            for range_id in serving
                .range_map
                .ranges()
                .iter()
                .map(|range| range.range_id)
            {
                if serving.engine(range_id).is_none() {
                    forward
                        .recover_global(range_id, global_xid, commit)
                        .await
                        .map_err(|error| ExecError::Unsupported(error.to_string()))?;
                }
            }
        }
        Ok(())
    }
    /// Start N local range engines and return the gateway plus lifetime handles.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
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
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn start_with_engine_factory(
        config: MultiRangeTenantConfig,
        open_engine: impl FnMut(Option<&PathBuf>, RangeId) -> Result<SqlEngine, ExecError>,
    ) -> Result<(Self, MultiRangeTenantHandles), TenantError> {
        Self::start_with_engine_factory_and_timestamp_oracle(config, open_engine, None)
    }

    /// Start a tenant with an explicitly supplied timestamp oracle.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn start_with_engine_factory_and_timestamp_oracle(
        mut config: MultiRangeTenantConfig,
        mut open_engine: impl FnMut(Option<&PathBuf>, RangeId) -> Result<SqlEngine, ExecError>,
        timestamp_oracle: Option<Arc<dyn crabka_pgexec::TimestampSource>>,
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
            // Explicitly supplied oracles are already mode-appropriate: the
            // live boot path builds a logical or wall-anchored HLC grant
            // oracle from the recovered range-0 horizon before assembly.
            Some(timestamp_oracle) => install_timestamp_oracle(&mut engines, &timestamp_oracle),
            None if hosts_range0 => match config.timestamp_source_mode {
                TimestampSourceMode::LogicalTso => install_memory_timestamp_oracle(&mut engines)?,
                TimestampSourceMode::Hlc { max_offset_ms } => {
                    install_hlc_timestamp_source(
                        &mut engines,
                        max_offset_ms,
                        config.hlc_wall_offset_ms,
                    )?;
                }
            },
            None => {
                if let (Some(registry), Some(client)) =
                    (config.range_registry.clone(), config.range_client.clone())
                {
                    let rpc = Arc::new(RegistryTsoRpc::new(registry, client));
                    let timestamp_oracle: Arc<dyn crabka_pgexec::TimestampSource> =
                        Arc::new(PgexecTsoOracle {
                            client: BatchedTsoClient::new(rpc),
                            observed_floor: std::sync::atomic::AtomicU64::new(0),
                        });
                    install_timestamp_oracle(&mut engines, &timestamp_oracle);
                } else {
                    install_unavailable_timestamp_oracle(&mut engines);
                }
            }
        }

        if config.recover_timestamps_on_start {
            recover_durable_timestamp_transactions(
                &engines,
                &config.range_map,
                config
                    .range_registry
                    .as_ref()
                    .zip(config.range_client.as_ref()),
            )?;
        }
        let scanner_engines = engines
            .iter()
            .map(|(range_id, engine)| (*range_id, engine.clone_handle()))
            .collect();
        let range_scanner: Arc<dyn crabka_pgexec::RangeScanner> =
            match config.range_registry.clone() {
                Some(registry) => Arc::new(RegistryRangeScanner::new_with_policy(
                    registry,
                    config
                        .range_client
                        .clone()
                        .ok_or(TenantError::MissingRangeTls)?,
                    scanner_engines,
                    config.runtime_policy.join,
                )),
                None => Arc::new(InProcessRangeScanner {
                    engines: scanner_engines,
                    range_map: config.range_map.clone(),
                }),
            };
        for engine in engines.values_mut() {
            engine.set_range_scanner(range_scanner.clone());
        }
        let timestamp_primary_remote = config
            .range_registry
            .clone()
            .zip(config.range_client.clone());
        let range_client = range_client_for_registry(&config)?;
        let remote_forward = match (config.range_registry, range_client) {
            (Some(registry), Some(client)) => {
                Some(Arc::new(RegistryRemoteForward::new(registry, client))
                    as Arc<dyn RemoteForward>)
            }
            (None, None | Some(_)) => None,
            (Some(_), None) => return Err(TenantError::MissingRangeTls),
        };
        let notify_origin = config.node_identity.clone();
        let notify_bus = install_node_notify_bus(
            &engines,
            config.range0_replica.as_ref(),
            notify_origin.clone(),
        );
        let inner = Arc::new(TenantInner {
            tenant: config.tenant,
            notify_bus,
            notify_origin,
            serving: ArcSwap::from_pointee(ServingSnapshot::ready(config.range_map, engines)),
            remote_forward,
            range0_replica: config.range0_replica,
            timestamp_primary_remote,
            coordinator: LocalCoordinator::default(),
            route_log: Mutex::new(Vec::new()),
            data_dir: config.data_dir,
            split_states: Mutex::new(BTreeMap::new()),
            split_lock: Mutex::new(()),
            schema_gate: Arc::new(Mutex::new(())),
            table_write_gates: StdMutex::new(BTreeMap::new()),
            topology_mutation_gate: Arc::new(RwLock::new(())),
            active_explicit_transactions: AtomicUsize::new(0),
            empty_table_split_test_hook: config.empty_table_split_test_hook,
            commit_fault_for_testing: config
                .commit_fault_for_testing
                .map(|fault| Arc::new(StdMutex::new(Some(fault)))),
            runtime_policy: config.runtime_policy,
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

    /// This node's identity on the cross-gateway notification log: the origin
    /// stamped on every record it publishes, and the one it ignores when
    /// reading records back off the log. `None` when its notifications stay
    /// in-process.
    #[must_use]
    pub fn node_notify_origin(&self) -> Option<&str> {
        self.inner.notify_origin.as_deref()
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

    #[must_use]
    pub fn timestamp_primary_remote(&self) -> Option<(RangeRegistry, FramedTcpClient)> {
        self.inner.timestamp_primary_remote.clone()
    }

    /// Process-wide DDL/schema serialization gate, shared with the range RPC service.
    ///
    /// The node binary must hand this same `Arc` to its
    /// [`HostedRangeService`](crate::forward::HostedRangeService) so local DDL,
    /// forwarded DDL, and split activation serialize behind one mutex.
    #[must_use]
    pub fn schema_gate(&self) -> Arc<Mutex<()>> {
        Arc::clone(&self.inner.schema_gate)
    }

    /// The read-only range-0 replica on an rN-only node.
    #[must_use]
    pub fn range0_replica(&self) -> Option<ReadOnlyRange0Replica> {
        self.inner.range0_replica.clone()
    }

    /// Runtime policy used by this tenant.
    #[must_use]
    pub fn runtime_policy(&self) -> crate::RangeRuntimePolicy {
        self.inner.runtime_policy
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
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn split_empty_successors(
        &self,
        operation_id: impl Into<String>,
        command: SplitCommand,
    ) -> Result<SplitState, LocalSqlSplitError> {
        let _split_guard = self.inner.split_lock.lock().await;
        let _topology_fence = self.acquire_control_topology_fence().await;
        let operation_id = operation_id.into();
        let table_id = command.right.interval.start.table_id;
        if let Some(state) = self
            .inner
            .split_states
            .lock()
            .await
            .get(&operation_id)
            .cloned()
        {
            if state.current_map != command.current_map
                || state.predecessor != command.predecessor
                || state.predecessor_generation != command.predecessor_generation
                || state.left != command.left
                || state.right.as_ref() != Some(&command.right)
            {
                return Err(LocalSqlSplitError::RetryMismatch);
            }
            let serving = self.inner.serving.load_full();
            let _table_write_gate = if serving.range_map == state.target_map {
                None
            } else {
                let table_write_gate = self.acquire_table_publication_fence(table_id).await;
                let serving = self.inner.serving.load_full();
                self.validate_empty_table_split(&serving, command.right.range_id, table_id)?;
                Some(table_write_gate)
            };
            let bridge = LocalSqlSplitBridge { tenant: self };
            return run_split(operation_id, command, &bridge, &bridge)
                .await
                .map_err(Into::into);
        }
        let serving = self.inner.serving.load_full();
        self.validate_empty_table_split(&serving, command.right.range_id, table_id)?;
        if let Some(hook) = &self.inner.empty_table_split_test_hook {
            hook.wait_after_initial_validation().await;
        }
        let _table_write_gate = self.acquire_table_publication_fence(table_id).await;
        let serving = self.inner.serving.load_full();
        self.validate_empty_table_split(&serving, command.right.range_id, table_id)?;
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
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn split_successors(
        &self,
        operation_id: impl Into<String>,
        command: SplitCommand,
        transfer: &dyn RangeTransferCapability,
    ) -> Result<SplitState, LocalSqlSplitError> {
        let _split_guard = self.inner.split_lock.lock().await;
        let _schema_gate = self.inner.schema_gate.clone().lock_owned().await;
        let _topology_fence = self.acquire_control_topology_fence().await;
        let operation_id = operation_id.into();
        let table_id = command.right.interval.start.table_id;
        let _table_write_gate = self.acquire_table_publication_fence(table_id).await;
        let serving = self.inner.serving.load_full();
        let transfer_table =
            self.validate_populated_table_split(&serving, command.right.range_id, table_id)?;
        if command.current_map != serving.range_map
            || command.predecessor != transfer_table.predecessor
        {
            return Err(LocalSqlSplitError::RetryMismatch);
        }
        let state = SplitState::for_split(operation_id, command)?;
        Self::validate_successor_interval(&serving, &state, table_id)?;
        let plan = Self::validated_transfer_plan(&serving, state.clone())?;
        transfer.validate_successors(&plan)?;
        transfer.record_topology_activation_intent(&state).await?;
        let checkpoint = transfer
            .force_checkpoint(&state.operation_id, transfer_table.predecessor)
            .await?;
        if let Err(error) = transfer
            .record_topology_activation_checkpoint(&state.operation_id, &checkpoint)
            .await
        {
            if let Err(cleanup_error) = transfer
                .release_checkpoint_pin(&state.operation_id, transfer_table.predecessor)
                .await
            {
                tracing::error!(%cleanup_error, "release checkpoint pin after receipt failure");
            }
            return Err(error.into());
        }
        let barrier = transfer.pause_at_checkpoint(&checkpoint).await?;
        let pause = TransferPauseGuard::new(transfer, state.operation_id.clone(), barrier);
        self.stage_and_publish_successors(transfer, &serving, &state, &plan, &checkpoint, barrier)
            .await?;
        pause.resume().await?;
        self.inner
            .split_states
            .lock()
            .await
            .insert(state.operation_id.clone(), state.clone());
        Ok(state)
    }

    async fn stage_and_publish_successors(
        &self,
        transfer: &dyn RangeTransferCapability,
        serving: &ServingSnapshot,
        state: &SplitState,
        plan: &ValidatedSplitTransferPlan,
        checkpoint: &CheckpointManifest,
        barrier: crate::RangeTransferBarrier,
    ) -> Result<(), LocalSqlSplitError> {
        let tail = transfer
            .read_committed_tail(state.predecessor, checkpoint.covered_offset, barrier)
            .await?;
        let staged = transfer
            .stage_successors(plan, checkpoint, &tail, barrier)
            .await?;
        let claimed = transfer.claim_successors(&staged, barrier).await?;
        self.publish_claimed_successors(serving, state, claimed, Some(transfer))
            .await
    }

    async fn publish_claimed_successors(
        &self,
        serving: &ServingSnapshot,
        state: &SplitState,
        claimed: crate::ClaimedStagedSuccessors,
        transfer: Option<&dyn RangeTransferCapability>,
    ) -> Result<(), LocalSqlSplitError> {
        let coordinator = serving
            .engines
            .get(&RangeId::COORDINATOR)
            .ok_or(LocalSqlSplitError::RemoteRange)?;
        let mut left = claimed.left;
        let mut right = claimed.right;
        if left.range_id != state.left.range_id
            || left.endpoint != state.left.endpoint
            || left.wal_generation != state.left.wal_generation
        {
            return Err(LocalSqlSplitError::RetryMismatch);
        }
        match (right.as_ref(), state.right.as_ref()) {
            (Some(right), Some(descriptor))
                if right.range_id == descriptor.range_id
                    && right.endpoint == descriptor.endpoint
                    && right.wal_generation == descriptor.wal_generation => {}
            (None, None) => {}
            _ => return Err(LocalSqlSplitError::RetryMismatch),
        }
        if state.left.range_id.is_coordinator() {
            left.engine.set_catalog_kv(left.engine.kv_handle());
            left.engine.init_gtm_coordinator().map_err(|error| {
                LocalSqlSplitError::SuccessorEngine {
                    range_id: state.left.range_id,
                    error,
                }
            })?;
            if let Some(scanner) = coordinator.foreign_scanner_handle() {
                left.engine.set_foreign_scanner(scanner);
            }
            if let Some(right) = &mut right {
                configure_successor_engine(&left.engine, &mut right.engine);
            }
        } else {
            configure_successor_engine(coordinator, &mut left.engine);
            if let Some(right) = &mut right {
                configure_successor_engine(coordinator, &mut right.engine);
            }
        }

        let mut engines = serving
            .engines
            .iter()
            .map(|(range_id, engine)| (*range_id, engine.clone_handle()))
            .collect::<BTreeMap<_, _>>();
        engines.remove(&state.predecessor);
        engines.insert(state.left.range_id, left.engine.clone_handle());
        if let Some(right) = right.as_ref() {
            engines.insert(right.range_id, right.engine.clone_handle());
        }
        if state.left.range_id.is_coordinator() {
            let replacement = engines
                .get(&RangeId::COORDINATOR)
                .expect("replacement r0 inserted")
                .clone_handle();
            for (range_id, engine) in &mut engines {
                if !range_id.is_coordinator() {
                    configure_successor_engine(&replacement, engine);
                }
            }
        }
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
        keepalives.remove(&state.predecessor);
        keepalives.insert(state.left.range_id, left.keepalive);
        if let Some(right) = right {
            keepalives.insert(right.range_id, right.keepalive);
        }
        if let Some(transfer) = transfer {
            transfer.publish_serving_topology(&engines)?;
            transfer.begin_serving_topology_publication();
            transfer.mark_topology_must_activate().await?;
            transfer.activate_serving_topology().await?;
            self.inner
                .serving
                .store(Arc::new(ServingSnapshot::publishing_with_keepalives(
                    state.target_map.clone(),
                    engines
                        .iter()
                        .map(|(id, engine)| (*id, engine.clone_handle()))
                        .collect(),
                    keepalives.clone(),
                )));
            recover_durable_timestamp_transactions(
                &engines,
                &state.target_map,
                self.inner
                    .timestamp_primary_remote
                    .as_ref()
                    .map(|(registry, client)| (registry, client)),
            )
            .map_err(|error| {
                LocalSqlSplitError::Orchestration(SplitError::Hook(format!(
                    "settle timestamp transactions before topology readiness: {error}"
                )))
            })?;
            transfer.commit_serving_topology();
            transfer.finish_topology_activation().await?;
        }
        self.inner
            .serving
            .store(Arc::new(ServingSnapshot::ready_with_keepalives(
                state.target_map.clone(),
                engines,
                keepalives,
            )));
        if let Some(transfer) = transfer {
            transfer.finish_serving_topology_publication();
        }
        Ok(())
    }

    /// Publish a fully restored, fenced control-plane successor as one atomic serving snapshot.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn publish_control_successors(
        &self,
        command: SplitCommand,
        claimed: crate::ClaimedStagedSuccessors,
    ) -> Result<(), LocalSqlSplitError> {
        let serving = self.inner.serving.load_full();
        if command.current_map != serving.range_map {
            return Err(LocalSqlSplitError::RetryMismatch);
        }
        let next_epoch = u64::from(serving.range_map.epoch())
            .checked_add(1)
            .map(MapEpoch::new)
            .ok_or(SplitError::MapEpochOverflow)?;
        let state = SplitState::for_split("control-publication", command)?;
        if state.target_map.epoch() != next_epoch {
            return Err(LocalSqlSplitError::Orchestration(SplitError::Hook(
                "control successor target map is not the next epoch".into(),
            )));
        }
        self.publish_claimed_successors(&serving, &state, claimed, None)
            .await
    }

    /// Publish control-plane successors through the same irreversible activation protocol used
    /// by locally initiated splits.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn publish_control_successors_with_transfer(
        &self,
        operation_id: String,
        command: SplitCommand,
        claimed: crate::ClaimedStagedSuccessors,
        transfer: &dyn RangeTransferCapability,
    ) -> Result<(), LocalSqlSplitError> {
        let serving = self.inner.serving.load_full();
        if command.current_map != serving.range_map {
            return Err(LocalSqlSplitError::RetryMismatch);
        }
        let state = SplitState::for_split(operation_id, command)?;
        self.publish_claimed_successors(&serving, &state, claimed, Some(transfer))
            .await
    }

    /// Publish an already authorized one-or-two-successor mutation through the irreversible
    /// activation protocol. The sealed state is the sole topology authority.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn publish_control_mutation_with_transfer(
        &self,
        state: SplitState,
        claimed: crate::ClaimedStagedSuccessors,
        transfer: &dyn RangeTransferCapability,
    ) -> Result<(), LocalSqlSplitError> {
        let _table_write_gate = self
            .acquire_table_publication_fence(state.predecessor_before.start.table_id)
            .await;
        let serving = self.inner.serving.load_full();
        if state.current_map != serving.range_map {
            return Err(LocalSqlSplitError::RetryMismatch);
        }
        self.publish_claimed_successors(&serving, &state, claimed, Some(transfer))
            .await
    }

    /// Fence new timestamp transactions while a control-plane topology mutation is paused.
    pub async fn acquire_control_topology_fence(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        self.inner
            .topology_mutation_gate
            .clone()
            .write_owned()
            .await
    }

    async fn acquire_table_publication_fence(
        &self,
        table_id: TableId,
    ) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        debug_assert!(
            self.inner.topology_mutation_gate.try_read().is_err(),
            "table publication fence requires topology publication fence first"
        );
        self.inner.table_write_gate(table_id).write_owned().await
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
        if !serving.engines.contains_key(&predecessor) {
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
        if table.foreign.is_some() {
            return Err(LocalSqlSplitError::UnsupportedTableKind(table_id));
        }
        if !crabka_pgcatalog::list_table_indexes(coordinator.catalog_kv(), &table.name)?.is_empty()
        {
            return Err(LocalSqlSplitError::IndexedTable(table_id));
        }
        Ok(PopulatedTransferTable { predecessor })
    }

    fn validated_transfer_plan(
        serving: &ServingSnapshot,
        state: SplitState,
    ) -> Result<ValidatedSplitTransferPlan, LocalSqlSplitError> {
        if serving.range_map != state.current_map {
            return Err(LocalSqlSplitError::RetryMismatch);
        }
        let coordinator = serving
            .engines
            .get(&RangeId::COORDINATOR)
            .ok_or(LocalSqlSplitError::RemoteRange)?;
        let source = serving
            .engines
            .get(&state.predecessor)
            .ok_or(LocalSqlSplitError::RemoteRange)?;
        let mapping = crate::transfer::predecessor_table_mapping(
            &serving.range_map,
            state.predecessor,
            crabka_pgcatalog::list_tables(coordinator.catalog_kv())?
                .into_iter()
                .map(|table| {
                    (
                        TableId::new(u64::from(table.id)),
                        routing_table_id(&table.name),
                    )
                }),
        )?;
        for (bytes, _) in source.kv_handle().scan_range(&[], &[u8::MAX])? {
            let physical = match crabka_pgkv::key::classify_key(&bytes) {
                crabka_pgkv::key::KeyClass::PrimaryRow { table_id, .. }
                | crabka_pgkv::key::KeyClass::PrimaryVersion { table_id, .. }
                | crabka_pgkv::key::KeyClass::HashPrimaryRow { table_id, .. }
                | crabka_pgkv::key::KeyClass::HashPrimaryVersion { table_id, .. }
                | crabka_pgkv::key::KeyClass::Sequence { table_id } => {
                    Some(TableId::new(u64::from(table_id)))
                }
                _ => None,
            };
            if let Some(physical) = physical
                && !mapping.contains_key(&physical)
            {
                return Err(LocalSqlSplitError::Transfer(RangeTransferError::Boundary {
                    range_id: state.predecessor,
                    reason: format!("unmapped physical table {physical} in predecessor storage"),
                }));
            }
        }
        Ok(ValidatedSplitTransferPlan::new(state, mapping))
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
        if table.foreign.is_some() {
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

/// Settle every primary descriptor recovered from hosted ranges before serving.
/// A pending descriptor is abort-won under its primary-range descriptor lock; a durable
/// commit is physically replayed with the operations stored in that descriptor.
fn hosted_participant_requires_recovery(
    descriptor: &crabka_pgexec::TimestampTxnDescriptor,
    identity: crabka_pgexec::TimestampTxnIdentity,
    participant: RangeId,
    outstanding: &BTreeSet<crabka_pgexec::DurableTimestampIntentIdentity>,
) -> bool {
    descriptor.decision == crabka_pgexec::PrimaryTxnDecision::Pending
        || outstanding.contains(&crabka_pgexec::DurableTimestampIntentIdentity {
            identity,
            participant_range: participant.as_u32(),
        })
}

fn route_timestamp_descriptor_to_active_map(
    mut descriptor: crabka_pgexec::TimestampTxnDescriptor,
    range_map: &RangeMap,
) -> Result<crabka_pgexec::TimestampTxnDescriptor, crate::MapValidationError> {
    let active_ranges = range_map
        .ranges()
        .iter()
        .map(|range| range.range_id.as_u32())
        .collect::<BTreeSet<_>>();
    let original_prepared = descriptor.prepared.iter().copied().collect::<BTreeSet<_>>();
    let mut participants = descriptor
        .participants
        .iter()
        .copied()
        .filter(|range| active_ranges.contains(range))
        .collect::<BTreeSet<_>>();
    let mut prepared = descriptor
        .prepared
        .iter()
        .copied()
        .filter(|range| active_ranges.contains(range))
        .collect::<BTreeSet<_>>();
    for operation in &mut descriptor.operations {
        let source_range = operation.range_id;
        if !active_ranges.contains(&source_range) {
            let route = operation.bucket.map_or_else(
                || {
                    range_map
                        .range_for_key(TableId::new(u64::from(operation.table_id)), operation.rowid)
                },
                |bucket| {
                    range_map.range_for_hash_bucket(
                        TableId::new(u64::from(operation.table_id)),
                        bucket,
                        operation.rowid,
                    )
                },
            )?;
            operation.range_id = route.range_id.as_u32();
        }
        participants.insert(operation.range_id);
        if original_prepared.contains(&source_range) {
            prepared.insert(operation.range_id);
        }
    }
    descriptor.participants = participants.into_iter().collect();
    descriptor.prepared = prepared.into_iter().collect();
    descriptor.operations.sort_unstable();
    Ok(descriptor)
}

fn recover_durable_timestamp_transactions(
    engines: &BTreeMap<RangeId, SqlEngine>,
    range_map: &RangeMap,
    remote: Option<(&RangeRegistry, &FramedTcpClient)>,
) -> Result<(), TenantError> {
    let mut descriptors = Vec::new();
    for (primary_range, engine) in engines {
        for descriptor in engine
            .timestamp_transaction_descriptors()
            .map_err(|error| TenantError::TimestampRecovery(format!("{error:?}")))?
        {
            let descriptor = route_timestamp_descriptor_to_active_map(descriptor, range_map)
                .map_err(|error| TenantError::TimestampRecovery(error.to_string()))?;
            descriptors.push((*primary_range, descriptor));
        }
    }
    let mut outstanding = BTreeSet::new();
    for engine in engines.values() {
        outstanding.extend(
            engine
                .durable_timestamp_intent_identities()
                .map_err(|error| TenantError::TimestampRecovery(format!("{error:?}")))?,
        );
    }
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| TenantError::TimestampRecovery(error.to_string()))?
                    .block_on(async {
                        for (primary_range, descriptor) in descriptors {
                            let identity = crabka_pgexec::TimestampTxnIdentity {
                                start_ts: descriptor.start_ts,
                                global_xid: descriptor.global_xid,
                                primary_range: primary_range.as_u32(),
                            };
                            let mut needs_recovery = false;
                            for &range_id in &descriptor.participants {
                                let range_id = RangeId::new(range_id);
                                let Some(participant) = engines.get(&range_id) else {
                                    needs_recovery = true;
                                    break;
                                };
                                let operations = descriptor
                                    .operations
                                    .iter()
                                    .copied()
                                    .filter(|operation| operation.range_id == range_id.as_u32())
                                    .collect::<Vec<_>>();
                                if hosted_participant_requires_recovery(
                                    &descriptor,
                                    identity,
                                    range_id,
                                    &outstanding,
                                ) || !participant
                                    .timestamp_transaction_operations_are_resolved(
                                        range_id.as_u32(),
                                        identity,
                                        descriptor.decision,
                                        &operations,
                                    )
                                    .map_err(|error| {
                                        TenantError::TimestampRecovery(format!("{error:?}"))
                                    })?
                                {
                                    needs_recovery = true;
                                    break;
                                }
                            }
                            if !needs_recovery {
                                continue;
                            }
                            let primary = engines.get(&primary_range).expect("hosted primary");
                            let decision = primary
                                .recover_timestamp_transaction(descriptor.start_ts)
                                .await
                                .map_err(|error| {
                                    TenantError::TimestampRecovery(format!("{error:?}"))
                                })?;
                            for &range_id in &descriptor.participants {
                                let range_id = RangeId::new(range_id);
                                let operations = descriptor
                                    .operations
                                    .iter()
                                    .copied()
                                    .filter(|operation| operation.range_id == range_id.as_u32())
                                    .collect::<Vec<_>>();
                                if let Some(participant) = engines.get(&range_id)
                                    && !hosted_participant_requires_recovery(
                                        &descriptor,
                                        identity,
                                        range_id,
                                        &outstanding,
                                    )
                                    && participant
                                        .timestamp_transaction_operations_are_resolved(
                                            range_id.as_u32(),
                                            identity,
                                            descriptor.decision,
                                            &operations,
                                        )
                                        .map_err(|error| {
                                            TenantError::TimestampRecovery(format!("{error:?}"))
                                        })?
                                {
                                    continue;
                                }
                                let resolve_result = if let Some(participant) =
                                    engines.get(&range_id)
                                {
                                    if decision == crabka_pgexec::PrimaryTxnDecision::Aborted {
                                        participant
                                            .abort_timestamp_transaction_intents(
                                                descriptor.start_ts,
                                            )
                                            .await
                                    } else {
                                        match participant
                                            .resolve_timestamp_transaction_operations(
                                                range_id.as_u32(),
                                                identity,
                                                decision,
                                                &operations,
                                            )
                                            .await
                                        {
                                            Ok(()) => {
                                                participant
                                                    .recover_timestamp_scan_terminals(&operations)
                                                    .await
                                            }
                                            Err(error) => Err(error),
                                        }
                                    }
                                } else {
                                    recover_remote_timestamp_participant(
                                        remote,
                                        range_id,
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
                        recover_orphan_timestamp_participants(engines, remote).await?;
                        Ok(())
                    })
            })
            .join()
            .map_err(|_| TenantError::TimestampRecovery("recovery worker panicked".into()))?
    })
}

async fn recover_orphan_timestamp_participants(
    engines: &BTreeMap<RangeId, SqlEngine>,
    remote: Option<(&RangeRegistry, &FramedTcpClient)>,
) -> Result<(), TenantError> {
    let recovery_error =
        |error: crabka_pgexec::ExecError| TenantError::TimestampRecovery(format!("{error:?}"));
    let mut orphan_participants =
        BTreeMap::<crabka_pgexec::TimestampTxnIdentity, Vec<RangeId>>::new();
    for engine in engines.values() {
        for durable in engine
            .durable_timestamp_intent_identities()
            .map_err(recovery_error)?
        {
            orphan_participants
                .entry(durable.identity)
                .or_default()
                .push(RangeId::new(durable.participant_range));
        }
    }
    for (identity, mut participants) in orphan_participants {
        participants.sort_unstable();
        participants.dedup();
        let primary_range = RangeId::new(identity.primary_range);
        let remote_primary_decision;
        let (primary, primary_decision) = if let Some(primary) = engines.get(&primary_range) {
            let decision = primary
                .primary_timestamp_decision(identity.start_ts)
                .map_err(recovery_error)?;
            (Some(primary), decision)
        } else {
            remote_primary_decision = recover_remote_timestamp_primary(remote, identity)
                .await
                .map_err(recovery_error)?;
            (None, remote_primary_decision)
        };
        if primary_decision != crabka_pgexec::PrimaryTxnDecision::Pending {
            if primary.is_none() {
                for range_id in participants {
                    let participant = engines.get(&range_id).ok_or_else(|| {
                        TenantError::TimestampRecovery(format!(
                            "terminal participant r{range_id} is not hosted"
                        ))
                    })?;
                    if primary_decision == crabka_pgexec::PrimaryTxnDecision::Aborted {
                        participant
                            .abort_timestamp_transaction_intents(identity.start_ts)
                            .await
                            .map_err(recovery_error)?;
                    } else {
                        return Err(TenantError::TimestampRecovery(
                            "committed orphan lacks acknowledged primary operations".into(),
                        ));
                    }
                }
                continue;
            }
            let primary = primary.expect("checked hosted primary");
            let descriptor = primary
                .timestamp_transaction_descriptors()
                .map_err(recovery_error)?
                .into_iter()
                .find(|descriptor| descriptor.start_ts == identity.start_ts)
                .ok_or_else(|| {
                    TenantError::TimestampRecovery("terminal primary descriptor disappeared".into())
                })?;
            for range_id in participants {
                let participant = engines.get(&range_id).ok_or_else(|| {
                    TenantError::TimestampRecovery(format!(
                        "terminal participant r{range_id} is not hosted"
                    ))
                })?;
                if primary_decision == crabka_pgexec::PrimaryTxnDecision::Aborted {
                    participant
                        .abort_timestamp_transaction_intents(identity.start_ts)
                        .await
                        .map_err(recovery_error)?;
                    continue;
                }
                let operations = descriptor
                    .operations
                    .iter()
                    .copied()
                    .filter(|operation| operation.range_id == range_id.as_u32())
                    .collect::<Vec<_>>();
                participant
                    .resolve_timestamp_transaction_operations(
                        range_id.as_u32(),
                        identity,
                        primary_decision,
                        &operations,
                    )
                    .await
                    .map_err(recovery_error)?;
            }
            continue;
        }
        let primary = primary.expect("pending decision requires hosted primary");
        let descriptor = crabka_pgexec::TimestampTxnDescriptor::begun(
            identity.start_ts,
            identity.global_xid,
            participants.iter().map(|range| range.as_u32()).collect(),
        );
        primary
            .begin_timestamp_transaction(&descriptor)
            .await
            .map_err(recovery_error)?;
        primary
            .decide_timestamp_transaction(
                identity.start_ts,
                crabka_pgexec::PrimaryTxnDecision::Aborted,
            )
            .await
            .map_err(recovery_error)?;
        for range_id in participants {
            engines
                .get(&range_id)
                .ok_or_else(|| {
                    TenantError::TimestampRecovery(format!(
                        "orphan participant r{range_id} is not hosted"
                    ))
                })?
                .abort_timestamp_transaction_intents(identity.start_ts)
                .await
                .map_err(recovery_error)?;
        }
    }
    Ok(())
}

async fn recover_remote_timestamp_primary(
    remote: Option<(&RangeRegistry, &FramedTcpClient)>,
    identity: crabka_pgexec::TimestampTxnIdentity,
) -> Result<crabka_pgexec::PrimaryTxnDecision, ExecError> {
    let (registry, client) = remote.ok_or_else(|| {
        ExecError::Unsupported(format!(
            "timestamp primary r{} is unreachable",
            identity.primary_range
        ))
    })?;
    let primary_range = RangeId::new(identity.primary_range);
    let endpoint = registry
        .resolve(primary_range)
        .await
        .map_err(|error| ExecError::Unsupported(error.to_string()))?;
    let request = crate::transport::RangeRequest::TimestampPrimaryRecover(
        crate::transport::TimestampPrimaryRecoverReq {
            primary_range,
            identity: crate::transport::WireTimestampIdentity {
                start_ts: identity.start_ts.get(),
                global_xid: identity.global_xid,
                primary_range: identity.primary_range,
            },
        },
    );
    match client
        .call(&endpoint.endpoint, &request)
        .await
        .map_err(|error| ExecError::Unsupported(error.to_string()))?
    {
        crate::transport::RangeResponse::TimestampPrimaryDecision { decision, .. } => {
            match decision {
                crate::transport::WireTimestampDecision::Aborted => {
                    Ok(crabka_pgexec::PrimaryTxnDecision::Aborted)
                }
                crate::transport::WireTimestampDecision::Committed { commit_ts } => {
                    Ok(crabka_pgexec::PrimaryTxnDecision::Committed(
                        crabka_pgexec::CommitTimestamp::new(commit_ts)
                            .map_err(|error| ExecError::Unsupported(error.to_string()))?,
                    ))
                }
            }
        }
        crate::transport::RangeResponse::SqlError { message, .. }
        | crate::transport::RangeResponse::Error { message, .. } => {
            Err(ExecError::Unsupported(message))
        }
        _ => Err(ExecError::Unsupported(
            "unexpected timestamp primary recovery response".into(),
        )),
    }
}

async fn recover_remote_timestamp_participant(
    remote: Option<(&RangeRegistry, &FramedTcpClient)>,
    range_id: RangeId,
    identity: crabka_pgexec::TimestampTxnIdentity,
    decision: crabka_pgexec::PrimaryTxnDecision,
    operations: &[crabka_pgexec::TimestampTxnOperation],
) -> Result<(), ExecError> {
    let (registry, client) = remote.ok_or_else(|| {
        ExecError::Unsupported(format!(
            "cannot recover timestamp transaction {}: participant r{range_id} is not hosted",
            identity.start_ts.get()
        ))
    })?;
    let endpoint = registry
        .resolve(range_id)
        .await
        .map_err(|error| ExecError::Unsupported(error.to_string()))?;
    let decision = match decision {
        crabka_pgexec::PrimaryTxnDecision::Aborted => {
            crate::transport::WireTimestampDecision::Aborted
        }
        crabka_pgexec::PrimaryTxnDecision::Committed(ts) => {
            crate::transport::WireTimestampDecision::Committed {
                commit_ts: ts.get(),
            }
        }
        crabka_pgexec::PrimaryTxnDecision::Pending => {
            return Err(ExecError::Unsupported(
                "timestamp recovery primary remained pending".into(),
            ));
        }
    };
    let request =
        crate::transport::RangeRequest::TimestampRecover(crate::transport::TimestampRecoverReq {
            range_id,
            identity: crate::transport::WireTimestampIdentity {
                start_ts: identity.start_ts.get(),
                global_xid: identity.global_xid,
                primary_range: identity.primary_range,
            },
            decision,
            operations: operations
                .iter()
                .map(|op| crate::transport::WireTimestampOperation {
                    range_id: op.range_id,
                    table_id: op.table_id,
                    bucket: op.bucket,
                    rowid: op.rowid,
                    delete: op.delete,
                })
                .collect(),
        });
    match client
        .call(&endpoint.endpoint, &request)
        .await
        .map_err(|error| ExecError::Unsupported(error.to_string()))?
    {
        crate::transport::RangeResponse::TimestampParticipantDone => Ok(()),
        crate::transport::RangeResponse::SqlError { message, .. }
        | crate::transport::RangeResponse::Error { message, .. } => {
            Err(ExecError::Unsupported(message))
        }
        _ => Err(ExecError::Unsupported(
            "unexpected timestamp recovery response".into(),
        )),
    }
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
    /// Highest range-local timestamp observed via the Lamport receive seam.
    /// Single-shard bypass commits advance per-range local sequences without
    /// consulting the global oracle, so a grant may lag behind stamps a range
    /// has already spent; every allocation skips to strictly above this floor
    /// so cross-range leases never collide with locally minted rowids.
    observed_floor: std::sync::atomic::AtomicU64,
}

struct SharedTsoRpc(Arc<dyn TsoRpc>);

#[async_trait::async_trait]
impl TsoRpc for SharedTsoRpc {
    async fn grant(&self, count: NonZeroU64) -> Result<GrantLease, TsoError> {
        self.0.grant(count).await
    }
}

#[async_trait::async_trait]
impl<R> crabka_pgexec::TimestampSource for PgexecTsoOracle<R>
where
    R: TsoRpc,
{
    async fn allocate_read_timestamp(
        &self,
    ) -> Result<crabka_pgexec::timestamp_txn::ReadTimestamp, crabka_pgexec::TimestampSourceError>
    {
        let timestamp = self.grant_one().await?;
        crabka_pgexec::timestamp_txn::ReadTimestamp::new(timestamp).map_err(Into::into)
    }

    async fn allocate_transaction_id(
        &self,
    ) -> Result<crabka_pgexec::TimestampTransactionId, crabka_pgexec::TimestampSourceError> {
        let timestamp = self.grant_one().await?;
        crabka_pgexec::TimestampTransactionId::new(timestamp).map_err(Into::into)
    }

    async fn allocate_write_lease(
        &self,
        hidden_rowid_count: usize,
    ) -> Result<
        crabka_pgexec::timestamp_txn::TimestampWriteLease,
        crabka_pgexec::TimestampSourceError,
    > {
        let count = u64::try_from(hidden_rowid_count)
            .ok()
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| {
                crabka_pgexec::TimestampSourceError::Unavailable(
                    "timestamp write lease is too large".into(),
                )
            })?;
        let first = self.grant_above_floor(count).await?;
        let start_ts = crabka_pgexec::TimestampTransactionId::new(first)?;
        let hidden_rowids = (1..=hidden_rowid_count)
            .map(|offset| {
                first
                    .checked_add(u64::try_from(offset).expect("offset fits u64"))
                    .ok_or_else(|| {
                        crabka_pgexec::TimestampSourceError::Unavailable(
                            "timestamp write lease overflow".into(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(crabka_pgexec::timestamp_txn::TimestampWriteLease {
            start_ts,
            hidden_rowids,
        })
    }

    async fn allocate_commit_after(
        &self,
        start_ts: crabka_pgexec::TimestampTransactionId,
    ) -> Result<crabka_pgexec::CommitTimestamp, crabka_pgexec::TimestampSourceError> {
        let timestamp = self.grant_one().await?;
        crabka_pgexec::CommitTimestamp::after_start(start_ts, timestamp).map_err(Into::into)
    }

    fn observe(&self, observed_ts: u64) {
        self.observed_floor
            .fetch_max(observed_ts, std::sync::atomic::Ordering::AcqRel);
    }
}

impl<R> PgexecTsoOracle<R>
where
    R: TsoRpc,
{
    async fn grant_one(&self) -> Result<u64, crabka_pgexec::TimestampSourceError> {
        self.grant_above_floor(1).await
    }

    /// Grant `count` contiguous timestamps whose first stamp strictly exceeds
    /// the observed range-local floor, burning below-floor stamps as needed.
    /// Grants are contiguous and strictly monotone across calls, so each
    /// deficit re-grant closes the gap and the loop only repeats when a
    /// concurrent observation raises the floor mid-flight.
    ///
    /// The floor is a best-effort high-water mark, not a reservation: a
    /// bypass allocation racing this grant can still mint overlapping stamps,
    /// and that collision is caught by the prewrite reservation checks as a
    /// retryable serialization conflict. Serializing against every range's
    /// local allocator would reintroduce exactly the global coordination the
    /// single-shard bypass exists to avoid.
    async fn grant_above_floor(
        &self,
        count: u64,
    ) -> Result<u64, crabka_pgexec::TimestampSourceError> {
        let mut request = count;
        loop {
            let requested = NonZeroU64::new(request).ok_or_else(|| {
                crabka_pgexec::TimestampSourceError::Unavailable("timestamp grant is empty".into())
            })?;
            let lease = self.client.grant(requested).await.map_err(|error| {
                crabka_pgexec::TimestampSourceError::Unavailable(error.to_string())
            })?;
            let floor = self
                .observed_floor
                .load(std::sync::atomic::Ordering::Acquire);
            // Use the top `count` stamps of the contiguous grant.
            let first = lease
                .first_ts
                .get()
                .checked_add(request - count)
                .ok_or_else(|| {
                    crabka_pgexec::TimestampSourceError::Unavailable(
                        "timestamp grant overflow".into(),
                    )
                })?;
            if first > floor {
                return Ok(first);
            }
            let deficit = floor - first + 1;
            request = count.checked_add(deficit).ok_or_else(|| {
                crabka_pgexec::TimestampSourceError::Unavailable(
                    "timestamp floor exhausts the grant domain".into(),
                )
            })?;
        }
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

/// Give this node the one `LISTEN`/`NOTIFY` bus every connection to it shares,
/// and — when the node has an identity to publish under — connect that bus to
/// the range-0 log in whichever direction this node can reach it.
///
/// A node hosting range 0 must use that engine's own bus, not a bus beside it:
/// the coordinator engine publishes there, so a second bus would leave every
/// statement executed on this node invisible to its own listeners. It is also
/// the node that owns the notification log, so it is the one that stamps and
/// appends records — hence [`SqlEngine::set_notify_origin`] on that engine and
/// no observer, since the leader applies its own frames without the tail.
///
/// A node without range 0 is fed from the other direction: its follower tail
/// observes the records the coordinator appended and re-injects them.
///
/// Without an identity the bus stays in-process, which is what a tenant whose
/// engines commit straight to their KV needs: nothing stamps, nothing appends,
/// and no notification can reach a store.
fn install_node_notify_bus(
    engines: &BTreeMap<RangeId, SqlEngine>,
    replica: Option<&ReadOnlyRange0Replica>,
    identity: Option<String>,
) -> Arc<crabka_pgexec::notify::NotifyBus> {
    if let Some(coordinator) = engines.get(&RangeId::COORDINATOR) {
        if let Some(identity) = identity {
            coordinator.set_notify_origin(identity);
        }
        return Arc::clone(coordinator.notify_bus());
    }
    let bus = Arc::new(crabka_pgexec::notify::NotifyBus::new());
    if let (Some(replica), Some(identity)) = (replica, identity) {
        replica
            .tail()
            .set_frame_observer(Arc::new(NotifyTailObserver {
                bus: Arc::clone(&bus),
                origin: identity,
            }));
    }
    bus
}

/// Build a pgexec timestamp oracle from a recovered range-0 TSO horizon.
/// # Panics
///
/// Panics if an internal invariant is violated.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn pgexec_timestamp_oracle_from_horizon<C, H>(
    committer: C,
    heartbeat: H,
    epoch: i16,
    persisted_max_ts: u64,
) -> Result<Arc<dyn crabka_pgexec::TimestampSource>, TsoError>
where
    C: TsoHorizonCommitter + 'static,
    H: EpochHeartbeat + 'static,
{
    pgexec_timestamp_oracle_from_horizon_with_policy(
        committer,
        heartbeat,
        epoch,
        persisted_max_ts,
        &crate::RangeRuntimePolicy::default(),
    )
}

/// Build a pgexec timestamp oracle using explicit range runtime policy.
/// # Errors
/// Returns an error when the durable horizon is invalid.
pub fn pgexec_timestamp_oracle_from_horizon_with_policy<C, H>(
    committer: C,
    heartbeat: H,
    epoch: i16,
    persisted_max_ts: u64,
    policy: &crate::RangeRuntimePolicy,
) -> Result<Arc<dyn crabka_pgexec::TimestampSource>, TsoError>
where
    C: TsoHorizonCommitter + 'static,
    H: EpochHeartbeat + 'static,
{
    let stride = NonZeroU64::new(policy.logical_base_persist_stride.get())
        .ok_or(TsoError::TimestampOverflow)?;
    let oracle = Arc::new(TsoOracle::recover_with_policy(
        committer,
        heartbeat,
        epoch,
        stride,
        persisted_max_ts,
        policy,
    )?);
    let rpc: Arc<dyn TsoRpc> =
        Arc::new(BatchedTsoClient::new(Arc::new(InProcessTsoRpc { oracle })));
    Ok(pgexec_timestamp_oracle_from_rpc(rpc))
}

/// Adapt a range-0 TSO RPC client into pgexec's timestamp oracle seam.
#[must_use]
pub fn pgexec_timestamp_oracle_from_rpc(
    rpc: Arc<dyn TsoRpc>,
) -> Arc<dyn crabka_pgexec::TimestampSource> {
    Arc::new(PgexecTsoOracle {
        client: BatchedTsoClient::new(Arc::new(SharedTsoRpc(rpc))),
        observed_floor: std::sync::atomic::AtomicU64::new(0),
    })
}

/// Build the in-process RPC endpoint backed by a recovered durable horizon.
/// # Panics
///
/// Panics if an internal invariant is violated.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
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
    tso_rpc_from_horizon_with_policy(
        committer,
        heartbeat,
        epoch,
        persisted_max_ts,
        &crate::RangeRuntimePolicy::default(),
    )
}

/// Build an in-process TSO endpoint using explicit runtime policy.
/// # Errors
/// Returns an error when the durable horizon is invalid.
pub fn tso_rpc_from_horizon_with_policy<C, H>(
    committer: C,
    heartbeat: H,
    epoch: i16,
    persisted_max_ts: u64,
    policy: &crate::RangeRuntimePolicy,
) -> Result<Arc<dyn TsoRpc>, TsoError>
where
    C: TsoHorizonCommitter + 'static,
    H: EpochHeartbeat + 'static,
{
    let stride = NonZeroU64::new(policy.logical_base_persist_stride.get())
        .ok_or(TsoError::TimestampOverflow)?;
    let oracle = Arc::new(TsoOracle::recover_with_policy(
        committer,
        heartbeat,
        epoch,
        stride,
        persisted_max_ts,
        policy,
    )?);
    Ok(Arc::new(BatchedTsoClient::new(Arc::new(InProcessTsoRpc {
        oracle,
    }))))
}

/// Build the in-process RPC endpoint for a wall-anchored HLC grant oracle
/// recovered from a durable horizon.
///
/// This is the `Hlc`-mode counterpart of [`tso_rpc_from_horizon`]: range 0
/// stays the single timestamp authority, but grants are packed HLC stamps
/// anchored to `wall` instead of dense logical integers. The oracle seeds its
/// clock from `persisted_max_ts`, so every grant strictly dominates everything
/// any predecessor granted even when `wall` reads behind the predecessor's
/// wall clock, and it persists the configured horizon headroom through the
/// same epoch-gated committer the logical oracle uses.
/// # Panics
///
/// Panics if an internal invariant is violated.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn hlc_tso_rpc_from_horizon<C, H>(
    committer: C,
    heartbeat: H,
    epoch: i16,
    persisted_max_ts: u64,
    wall: Arc<dyn crabka_pgexec::WallClock>,
) -> Result<Arc<dyn TsoRpc>, TsoError>
where
    C: TsoHorizonCommitter + 'static,
    H: EpochHeartbeat + 'static,
{
    hlc_tso_rpc_from_horizon_with_policy(
        committer,
        heartbeat,
        epoch,
        persisted_max_ts,
        wall,
        &crate::RangeRuntimePolicy::default(),
    )
}

/// Build an HLC TSO endpoint using explicit runtime policy.
/// # Errors
/// Returns an error when the durable horizon is invalid.
pub fn hlc_tso_rpc_from_horizon_with_policy<C, H>(
    committer: C,
    heartbeat: H,
    epoch: i16,
    persisted_max_ts: u64,
    wall: Arc<dyn crabka_pgexec::WallClock>,
    policy: &crate::RangeRuntimePolicy,
) -> Result<Arc<dyn TsoRpc>, TsoError>
where
    C: TsoHorizonCommitter + 'static,
    H: EpochHeartbeat + 'static,
{
    let headroom_ms =
        u64::try_from(policy.hlc_horizon_headroom.millis_i64_trunc()).unwrap_or(u64::MAX);
    let stride = NonZeroU64::new(crabka_pgexec::hlc::pack(headroom_ms, 0))
        .ok_or(TsoError::TimestampOverflow)?;
    let oracle = Arc::new(TsoOracle::recover_hlc_with_policy(
        committer,
        heartbeat,
        epoch,
        stride,
        persisted_max_ts,
        wall,
        policy,
    )?);
    Ok(Arc::new(BatchedTsoClient::new(Arc::new(InProcessTsoRpc {
        oracle,
    }))))
}

/// The wall clock an HLC component on this node should read.
///
/// A zero `wall_offset_ms` is the plain system clock; a nonzero offset wraps
/// it in the fault-injection [`crabka_pgexec::SkewedWallClock`] used by load
/// and chaos tests to emulate cross-node wall-clock skew.
#[must_use]
pub fn hlc_wall_clock(wall_offset_ms: i64) -> Arc<dyn crabka_pgexec::WallClock> {
    if wall_offset_ms == 0 {
        Arc::new(crabka_pgexec::SystemWallClock)
    } else {
        Arc::new(crabka_pgexec::SkewedWallClock::new(
            Arc::new(crabka_pgexec::SystemWallClock),
            wall_offset_ms,
        ))
    }
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
        .map_err(TenantError::TimestampSource)?;
    let timestamp_oracle =
        pgexec_timestamp_oracle_from_horizon(horizon.clone(), horizon, 1, persisted_max_ts)
            .map_err(TenantError::TimestampSource)?;

    install_timestamp_oracle(engines, &timestamp_oracle);
    Ok(())
}

/// Install a node-local Hybrid Logical Clock source, seeded from the durable
/// `LogicalTso` horizon so its first stamp dominates every solo-mode stamp.
///
/// This mirrors [`install_memory_timestamp_oracle`]: it fans one source to every
/// hosted engine. A single `HlcTimestampSource` is the sole timestamp authority
/// here, so it is correct on its own — multi-node stamping and the
/// uncertainty-window read-restart are the documented follow-up.
///
/// A nonzero `wall_offset_ms` wraps the system clock in a fault-injection
/// [`crabka_pgexec::SkewedWallClock`], for load and chaos tests emulating
/// cross-node wall-clock skew.
fn install_hlc_timestamp_source(
    engines: &mut BTreeMap<RangeId, SqlEngine>,
    max_offset_ms: u64,
    wall_offset_ms: i64,
) -> Result<(), TenantError> {
    let Some(coordinator) = engines.get(&RangeId::COORDINATOR) else {
        return Ok(());
    };
    // The persisted LogicalTso horizon is a packed stamp with physical zero;
    // seeding folds it in so every distributed stamp strictly dominates it.
    let horizon = MemoryTsoHorizon::new(coordinator.kv_handle(), 1);
    let persisted_max_ts = horizon
        .load_max_ts()
        .map_err(TenantError::TimestampSource)?;
    let wall = hlc_wall_clock(wall_offset_ms);
    let timestamp_source: Arc<dyn crabka_pgexec::TimestampSource> =
        Arc::new(crabka_pgexec::HlcTimestampSource::seeded_from_horizon(
            persisted_max_ts,
            wall,
            max_offset_ms,
        ));
    install_timestamp_oracle(engines, &timestamp_source);
    Ok(())
}

fn install_timestamp_oracle(
    engines: &mut BTreeMap<RangeId, SqlEngine>,
    timestamp_oracle: &Arc<dyn crabka_pgexec::TimestampSource>,
) {
    for engine in engines.values_mut() {
        engine.set_timestamp_oracle(Arc::clone(timestamp_oracle));
    }
}

struct UnavailableRange0TimestampSource;

#[async_trait::async_trait]
impl crabka_pgexec::TimestampSource for UnavailableRange0TimestampSource {
    async fn allocate_read_timestamp(
        &self,
    ) -> Result<crabka_pgexec::timestamp_txn::ReadTimestamp, crabka_pgexec::TimestampSourceError>
    {
        Err(timestamp_oracle_unavailable())
    }

    async fn allocate_transaction_id(
        &self,
    ) -> Result<crabka_pgexec::TimestampTransactionId, crabka_pgexec::TimestampSourceError> {
        Err(timestamp_oracle_unavailable())
    }

    async fn allocate_commit_after(
        &self,
        _start_ts: crabka_pgexec::TimestampTransactionId,
    ) -> Result<crabka_pgexec::CommitTimestamp, crabka_pgexec::TimestampSourceError> {
        Err(timestamp_oracle_unavailable())
    }
}

fn timestamp_oracle_unavailable() -> crabka_pgexec::TimestampSourceError {
    crabka_pgexec::TimestampSourceError::Unavailable(
        "range-0 timestamp oracle is unavailable on an rN-only compute".into(),
    )
}

fn install_unavailable_timestamp_oracle(engines: &mut BTreeMap<RangeId, SqlEngine>) {
    let timestamp_oracle: Arc<dyn crabka_pgexec::TimestampSource> =
        Arc::new(UnavailableRange0TimestampSource);
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
            let local_rows = engine.scan_local_visible_with_timestamp_owner(
                request.table,
                request.global_snapshot,
                request.snapshot,
                request.own_xid,
                crabka_pgexec::TimestampScanOwner {
                    read_ts: request.read_ts,
                    own_start_ts: request.own_start_ts,
                },
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
    let table_id = routing_table_id(&table.name);
    let Some(hash_value) = hash_equality_value(table, hash, &request.predicate) else {
        // Full scan: a hash table partitions across ranges by bucket, so the
        // rowid-sliced `scan_segments` decomposition would hand each range a
        // rowid interval that does not correspond to the rows it stores
        // (per-range sequences reuse rowid values). Cover every bucket via
        // the bucket-aware segmenter and scan each owning range exactly once.
        let mut range_ids = BTreeSet::new();
        for bucket in 0..hash.buckets {
            for segment in range_map
                .scan_hash_bucket_segments(
                    table_id,
                    bucket,
                    map_interval_from_exec(request.interval),
                )
                .map_err(|error| ExecError::Unsupported(error.to_string()))?
            {
                range_ids.insert(segment.range_id);
            }
        }
        return Ok(Some(
            range_ids
                .into_iter()
                .map(|range_id| RangeScanSegment {
                    range_id,
                    table_id,
                    interval: map_interval_from_exec(request.interval),
                })
                .collect(),
        ));
    };

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

impl MultiRangeTenant {
    /// Build a gateway session over the engines this node currently serves.
    ///
    /// When `notify_pid` is set the session also joins **this node's**
    /// notification bus under that backend pid — the coordinator engine's own
    /// bus where range 0 is hosted, a standalone bus fed by the range-0 tail
    /// where it is not. The gateway keeps the receiver — the wire loop takes it
    /// through [`Session::take_notifications`] and pushes `NotificationResponse`
    /// from there — and hands the registration handle to one hosted range's
    /// [`crabka_pgexec::SqlSession`], the seat this connection's `LISTEN` and
    /// `UNLISTEN` run on.
    ///
    /// A session that cannot register — no backend pid, or no hosted engine to
    /// seat the registration on — records why in [`GatewayNotify`] and refuses
    /// the statements rather than accepting them into a queue nothing on this
    /// connection drains.
    fn open_session(&self, notify_pid: Option<i32>) -> GatewaySession {
        let serving = self.inner.serving.load_full();
        let mut sessions: BTreeMap<RangeId, crabka_pgexec::SqlSession> = serving
            .engines
            .iter()
            .map(|(range_id, engine)| (*range_id, engine.connect()))
            .collect();
        let (notify, notifications) = match (notify_pid, notify_seat(&sessions)) {
            (None, _) => (GatewayNotify::NoBackendPid, None),
            (Some(_), None) => (GatewayNotify::NoLocalSeat, None),
            (Some(pid), Some(seat)) => {
                let (handle, receiver) =
                    crabka_pgexec::notify::NotifyBus::register(&self.inner.notify_bus, pid);
                // Replaces (and unregisters) the registration the seat engine
                // made for its own session, so this connection has exactly one
                // seat on the bus and the gateway holds its queue.
                sessions
                    .get_mut(&seat)
                    .expect("notify seat names a hosted session")
                    .adopt_notify(handle);
                (GatewayNotify::Registered { seat }, Some(receiver))
            }
        };
        GatewaySession {
            inner: Arc::clone(&self.inner),
            sessions,
            remote_sessions: BTreeMap::new(),
            timestamp_topology_guard: None,
            serving_epoch: serving.range_map.epoch(),
            explicit_transaction: false,
            transaction: GatewayTransaction::Idle,
            status: TxStatus::Idle,
            prepared: BTreeMap::new(),
            portals: BTreeMap::new(),
            next_internal_statement: 0,
            notify,
            notify_pid,
            notifications,
        }
    }
}

/// The hosted range whose session carries a connection's bus registration.
///
/// Range 0 when this node hosts it — the seat every notification statement
/// already ran on — and otherwise the lowest hosted range, because `LISTEN` and
/// `UNLISTEN` only ever touch the registration handle the seat holds, never the
/// seat's own data. A node hosting nothing has no seat, and no statement at all
/// (its router has no catalog engine either).
fn notify_seat(sessions: &BTreeMap<RangeId, crabka_pgexec::SqlSession>) -> Option<RangeId> {
    sessions
        .contains_key(&RangeId::COORDINATOR)
        .then_some(RangeId::COORDINATOR)
        .or_else(|| sessions.keys().next().copied())
}

/// Whether this gateway session holds a seat on this node's notification bus,
/// and if not, why — the two shapes carry different refusals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GatewayNotify {
    /// Registered on the node-local bus, with the hosted range whose session
    /// holds the registration: `LISTEN` and `UNLISTEN` run there rather than
    /// routing to range 0, since a subscription is only meaningful to the wire
    /// loop attached to this connection.
    Registered { seat: RangeId },
    /// Opened through [`Engine::connect`] without a backend pid, so there is no
    /// wire loop to hand a notification queue to. Only in-process callers reach
    /// this: the wire layer always opens sessions with a pid.
    NoBackendPid,
    /// This node hosts no range engine, so there is no session to seat the
    /// registration on.
    NoLocalSeat,
}

impl GatewayNotify {
    /// The hosted range this connection's subscriptions live on.
    fn seat(self) -> Option<RangeId> {
        match self {
            Self::Registered { seat } => Some(seat),
            Self::NoBackendPid | Self::NoLocalSeat => None,
        }
    }

    /// The gateway's verdict on a `LISTEN`/`NOTIFY`/`UNLISTEN` statement.
    fn ensure_supported(self, tenant: &str) -> Result<(), PgError> {
        let reason = match self {
            Self::Registered { .. } => return Ok(()),
            Self::NoBackendPid => "this session was opened without a backend process id".to_owned(),
            Self::NoLocalSeat => {
                format!("tenant {tenant} hosts no range engine on this gateway")
            }
        };
        Err(PgError::error(
            sqlstate::FEATURE_NOT_SUPPORTED,
            format!("LISTEN/NOTIFY is unavailable on this connection: {reason}"),
        ))
    }
}

impl Engine for MultiRangeTenant {
    type Session = GatewaySession;

    fn connect(&self) -> Self::Session {
        self.open_session(None)
    }

    fn connect_with_pid(&self, pid: i32) -> Self::Session {
        self.open_session(Some(pid))
    }
}

struct TenantInner {
    tenant: TenantName,
    /// The one notification bus of this node: shared with the coordinator
    /// engine where range 0 is hosted, fed by the range-0 tail where it is not.
    notify_bus: Arc<crabka_pgexec::notify::NotifyBus>,
    /// This node's identity on the notification log, or `None` when its
    /// notifications stay in-process.
    notify_origin: Option<String>,
    serving: ArcSwap<ServingSnapshot>,
    remote_forward: Option<Arc<dyn RemoteForward>>,
    range0_replica: Option<ReadOnlyRange0Replica>,
    timestamp_primary_remote: Option<(RangeRegistry, FramedTcpClient)>,
    coordinator: LocalCoordinator,
    route_log: Mutex<Vec<RouteRecord>>,
    data_dir: Option<PathBuf>,
    split_states: Mutex<BTreeMap<String, SplitState>>,
    split_lock: Mutex<()>,
    schema_gate: Arc<Mutex<()>>,
    table_write_gates: StdMutex<BTreeMap<TableId, Arc<RwLock<()>>>>,
    topology_mutation_gate: Arc<RwLock<()>>,
    active_explicit_transactions: AtomicUsize,
    empty_table_split_test_hook: Option<EmptyTableSplitTestHook>,
    commit_fault_for_testing: Option<Arc<StdMutex<Option<GatewayCommitFault>>>>,
    runtime_policy: crate::RangeRuntimePolicy,
}

struct PopulatedTransferTable {
    predecessor: RangeId,
}

/// Owns a successful source pause until the transfer either resumes it or is dropped.
struct TransferPauseGuard<'a> {
    transfer: &'a dyn RangeTransferCapability,
    operation_id: String,
    barrier: Option<crate::RangeTransferBarrier>,
}

impl<'a> TransferPauseGuard<'a> {
    fn new(
        transfer: &'a dyn RangeTransferCapability,
        operation_id: String,
        barrier: crate::RangeTransferBarrier,
    ) -> Self {
        Self {
            transfer,
            operation_id,
            barrier: Some(barrier),
        }
    }

    async fn resume(mut self) -> Result<(), RangeTransferError> {
        let barrier = self
            .barrier
            .expect("transfer pause guard must hold a barrier");
        self.transfer.resume(&self.operation_id, barrier).await?;
        self.barrier = None;
        if let Err(error) = self
            .transfer
            .release_checkpoint_pin(&self.operation_id, barrier.range_id)
            .await
        {
            tracing::error!(%error, "release checkpoint pin after successful transfer resume");
        }
        Ok(())
    }
}

impl Drop for TransferPauseGuard<'_> {
    fn drop(&mut self) {
        if let Some(barrier) = self.barrier {
            self.transfer.resume_after_drop(&self.operation_id, barrier);
        }
    }
}

impl TenantInner {
    fn table_write_gate(&self, table_id: TableId) -> Arc<RwLock<()>> {
        let mut table_write_gates = self
            .table_write_gates
            .lock()
            .expect("table write gate lock must not be poisoned");
        table_write_gates
            .entry(table_id)
            .or_insert_with(|| Arc::new(RwLock::new(())))
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

    fn publishing_with_keepalives(
        range_map: RangeMap,
        engines: BTreeMap<RangeId, SqlEngine>,
        keepalives: BTreeMap<RangeId, Arc<dyn std::any::Any + Send + Sync>>,
    ) -> Self {
        Self {
            range_map,
            engines,
            ready: BTreeSet::new(),
            keepalives,
        }
    }

    fn engine(&self, range_id: RangeId) -> Option<&SqlEngine> {
        self.ready
            .contains(&range_id)
            .then(|| self.engines.get(&range_id))
            .flatten()
    }

    /// The engine that stands in for range 0 on this node: r0's own engine when
    /// hosted, otherwise any hosted data-range engine. Every hosted engine shares
    /// the certified range-0 (follower) catalog KV and the installed timestamp
    /// oracle, so catalog classification, timestamp-write planning, and TSO
    /// allocation behave identically on any seat.
    fn planner_engine(&self) -> Option<&SqlEngine> {
        self.engine(RangeId::COORDINATOR)
            .or_else(|| self.engines.values().next())
    }
}

fn planner_engine(serving: &ServingSnapshot) -> Result<&SqlEngine, PgError> {
    serving.planner_engine().ok_or_else(|| {
        PgError::error(
            sqlstate::FEATURE_NOT_SUPPORTED,
            "tenant has no hosted engine with a range-0 catalog view",
        )
    })
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
            manifest_key: format!(
                "local-split-migration/v1/empty/r{}/epoch{}",
                state.predecessor,
                u64::from(state.current_map.epoch())
            ),
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
        let coordinator = serving.engines.get(&RangeId::COORDINATOR).ok_or_else(|| {
            SplitError::Hook("local SQL split requires hosted range r0".to_owned())
        })?;

        let mut engines = serving
            .engines
            .iter()
            .map(|(range_id, engine)| (*range_id, engine.clone_handle()))
            .collect::<BTreeMap<_, _>>();
        if !state.predecessor.is_coordinator() {
            engines.remove(&state.predecessor);
        }
        for descriptor in std::iter::once(&state.left).chain(state.right.iter()) {
            if descriptor.range_id.is_coordinator() {
                continue;
            }
            let mut successor =
                open_range_engine(self.tenant.inner.data_dir.as_ref(), descriptor.range_id)
                    .map_err(|error| {
                        SplitError::Hook(format!(
                            "successor r{} engine: {error:?}",
                            descriptor.range_id
                        ))
                    })?;
            configure_successor_engine(coordinator, &mut successor);
            engines.insert(descriptor.range_id, successor);
        }
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
    /// DDL executed on the range-0 catalog owner, forwarded when r0 is remote.
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
    timestamp_topology_guard: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    serving_epoch: MapEpoch,
    explicit_transaction: bool,
    transaction: GatewayTransaction,
    status: TxStatus,
    prepared: BTreeMap<String, GatewayPrepared>,
    portals: BTreeMap<String, GatewayPortal>,
    next_internal_statement: u64,
    /// This session's standing on this node's notification bus.
    notify: GatewayNotify,
    /// This connection's backend pid, forwarded on `SessionOpen` so a `NOTIFY`
    /// executed on the range owner is stamped with the originating pid.
    notify_pid: Option<i32>,
    /// The receiving end of this connection's registration on that bus, handed to
    /// the wire loop once by [`Session::take_notifications`]. `None` unless
    /// [`GatewayNotify::Registered`], and `None` again once taken.
    notifications: Option<tokio::sync::mpsc::Receiver<Notification>>,
}

#[derive(Debug, Clone)]
struct GatewayPrepared {
    sql: String,
    route: Option<StatementRoute>,
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
    Timestamp {
        identity: crabka_pgexec::TimestampTxnIdentity,
        participants: BTreeMap<RangeId, Vec<crabka_pgexec::TimestampWrite>>,
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

struct RoutedDmlFences {
    _autocommit_topology_guard: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    _table_write_guard: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
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
    fn take_notifications(&mut self) -> Option<tokio::sync::mpsc::Receiver<Notification>> {
        self.notifications.take()
    }

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
        let result = match self.execute_portal_inner(portal, max_rows).await {
            // A range-local session answers a COPY portal with a copy-in
            // handshake, but the gateway has no portal-scoped copy completion
            // path yet — fail clearly instead of half-entering copy mode.
            Ok(ExecuteOutcome::CopyIn { .. }) => Err(PgError::error(
                crabka_pgwire::error::sqlstate::FEATURE_NOT_SUPPORTED,
                "COPY FROM STDIN is not supported through the multi-range gateway",
            )),
            other => other,
        };
        self.finish_statement(result)
    }

    async fn close(&mut self, target: CloseTarget<'_>) -> Result<(), PgError> {
        match target {
            CloseTarget::Statement(name) => {
                if let Some(prepared) = self.prepared.remove(name)
                    && let Some(route) = prepared.route
                {
                    self.close_on_range(route.range_id, CloseTarget::Statement(name))
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
    async fn acquire_timestamp_topology_guard(&mut self) {
        if self.timestamp_topology_guard.is_none() {
            self.timestamp_topology_guard =
                Some(self.inner.topology_mutation_gate.clone().read_owned().await);
        }
    }

    fn release_timestamp_topology_guard(&mut self) {
        self.timestamp_topology_guard = None;
    }

    fn complete_timestamp_abort(&mut self) {
        self.transaction = GatewayTransaction::Failed {
            touched: Vec::new(),
            escalated: false,
            recovery: None,
        };
        self.release_timestamp_topology_guard();
        self.status = TxStatus::Failed;
    }

    fn complete_timestamp_commit(&mut self) {
        self.transaction = GatewayTransaction::Idle;
        self.release_timestamp_topology_guard();
        self.status = TxStatus::Idle;
        self.release_explicit_transaction();
    }

    fn complete_timestamp_rollback(&mut self) {
        self.transaction = GatewayTransaction::Idle;
        self.release_timestamp_topology_guard();
        self.status = TxStatus::Idle;
        self.release_explicit_transaction();
    }

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
            .open_session(range_id, self.notify_pid)
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
            let cap = self.cross_range_statement_cap();
            self.ensure_remote_session(range_id).await?;
            self.remote_sessions
                .get_mut(&range_id)
                .expect("remote session inserted")
                .simple_query(sql.to_owned(), cap)
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
        // A release validates the durable decision against the participant's
        // LOCAL range-0 replica, which follows range 0's log at a polling
        // cadence — so a release arriving right behind its own decision can
        // transiently read the decision as still in doubt (55000) until the
        // replica applies it. Retrying here turns that lag into a short wait
        // instead of failing a transaction whose decision is already durable;
        // any other error (and lag beyond the deadline) surfaces unchanged
        // and leaves the commit-recovery path to resolve it.
        let retries = self.inner.runtime_policy.decision_release_lag_retries.get();
        let backoff = self
            .inner
            .runtime_policy
            .decision_release_retry_backoff
            .to_std();
        let mut attempt = 0;
        loop {
            let result = self
                .release_on_range_once(range_id, global_xid, commit)
                .await;
            match result {
                Err(error) if error.code == "55000" && attempt < retries => {
                    attempt += 1;
                    tokio::time::sleep(backoff).await;
                }
                result => return result,
            }
        }
    }

    async fn release_on_range_once(
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
            && let Some(old_route) = old.route
        {
            self.close_on_range(old_route.range_id, CloseTarget::Statement(name))
                .await?;
        }
        let route = match self.route_statement(sql) {
            Ok(route) => Some(route),
            Err(error) if error.message == parameterized_shard_key_error().message => None,
            Err(error) => return Err(error),
        };
        if route.is_none() {
            let serving = self.current_serving()?;
            let catalog = planner_engine(&serving)?;
            let mut inference = catalog.connect();
            let description = inference.parse("", sql, parameter_types).await?;
            self.prepared.insert(
                name.to_owned(),
                GatewayPrepared {
                    sql: sql.to_owned(),
                    route: None,
                    description: description.clone(),
                },
            );
            return Ok(description);
        }
        let route = route.expect("concrete route checked above");
        if route.scatter_ranges.is_some() && route.kind != StatementKind::Dml {
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
                    route: Some(route),
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
                route: Some(route),
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
        let bound_sql = if params.is_empty() {
            prepared.sql.clone()
        } else {
            routing_sql_with_bound_params(
                &prepared.sql,
                &prepared.description.parameter_types,
                params,
            )?
        };
        let route = if let Some(route) = prepared.route.clone() {
            route
        } else {
            self.route_statement(&bound_sql)?
        };
        if matches!(
            route.kind,
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
                    route,
                    description: description.clone(),
                    gateway_execution: None,
                },
            );
            return Ok(description);
        }
        let gateway_timestamp_dml = route.kind == StatementKind::Dml
            && self.statement_targets_sharded_table(&bound_sql)?
            && (route.scatter_ranges.is_some()
                || matches!(self.transaction, GatewayTransaction::Timestamp { .. })
                || matches!(self.transaction, GatewayTransaction::Open { ref touched, .. } if touched.is_empty()));
        if gateway_timestamp_dml {
            let description = PortalDescription { fields: Vec::new() };
            self.portals.insert(
                portal.to_owned(),
                GatewayPortal {
                    sql: bound_sql,
                    route,
                    description: description.clone(),
                    gateway_execution: None,
                },
            );
            return Ok(description);
        }
        let deferred = prepared.route.is_none();
        let owner_statement = if deferred {
            let id = self.next_internal_statement;
            self.next_internal_statement = self.next_internal_statement.wrapping_add(1);
            format!("__crabka_gateway_{id}")
        } else {
            statement.to_owned()
        };
        let description = if let Some(session) = self.sessions.get_mut(&route.range_id) {
            if deferred {
                session
                    .parse(
                        &owner_statement,
                        &prepared.sql,
                        &prepared.description.parameter_types,
                    )
                    .await?;
            }
            let bind_result = session
                .bind(portal, &owner_statement, params, result_formats)
                .await;
            if deferred {
                let close_result = session
                    .close(CloseTarget::Statement(&owner_statement))
                    .await;
                if close_result.is_err() && bind_result.is_ok() {
                    let _ = session.close(CloseTarget::Portal(portal)).await;
                }
                if let Err(error) = bind_result {
                    return Err(error);
                }
                close_result?;
            }
            bind_result?
        } else {
            self.ensure_remote_session(route.range_id).await?;
            let session = self
                .remote_sessions
                .get_mut(&route.range_id)
                .expect("remote session inserted");
            if deferred {
                session
                    .parse(
                        owner_statement.clone(),
                        prepared.sql.clone(),
                        prepared.description.parameter_types.clone(),
                    )
                    .await?;
            }
            let bind_result = session
                .bind(
                    portal.to_owned(),
                    owner_statement.clone(),
                    params,
                    result_formats.to_vec(),
                )
                .await;
            if deferred {
                let close_result = session
                    .close(CloseTarget::Statement(&owner_statement))
                    .await;
                if close_result.is_err() && bind_result.is_ok() {
                    let _ = session.close(CloseTarget::Portal(portal)).await;
                }
                if let Err(error) = bind_result {
                    return Err(error);
                }
                close_result?;
            }
            bind_result?
        };
        self.portals.insert(
            portal.to_owned(),
            GatewayPortal {
                sql: prepared.sql,
                route,
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
        if portal_state.route.kind == StatementKind::Query {
            self.fold_hosted_read_floors()?;
        }
        if portal_state.route.kind == StatementKind::Dml
            && self.statement_targets_sharded_table(&portal_state.sql)?
            && (portal_state.route.scatter_ranges.is_some()
                || matches!(self.transaction, GatewayTransaction::Timestamp { .. })
                || matches!(self.transaction, GatewayTransaction::Open { ref touched, .. } if touched.is_empty()))
        {
            if let Some(outcome) = portal_state.gateway_execution {
                return Ok(outcome);
            }
            let mut results = self.execute_one(&portal_state.sql).await?;
            let outcome = query_result_to_outcome(results.pop().unwrap_or(QueryResult::Empty));
            self.portals
                .get_mut(portal)
                .expect("portal exists throughout gateway execution")
                .gateway_execution = Some(outcome.clone());
            return Ok(outcome);
        }
        self.inner.route_log.lock().await.push(RouteRecord {
            kind: portal_state.route.kind,
            range_id: portal_state.route.range_id,
            table_id: portal_state.route.table_id,
        });
        let _dml_fences = self
            .acquire_routed_dml_fences(&portal_state.route, false)
            .await?;
        let own_start_ts = match &self.transaction {
            GatewayTransaction::Timestamp { identity, .. }
                if portal_state.route.kind == StatementKind::Query =>
            {
                Some(identity.start_ts)
            }
            _ => None,
        };
        if portal_state.route.kind == StatementKind::Dml {
            self.touch_write_range(portal_state.route.range_id).await?;
        }
        if let Some(session) = self.sessions.get_mut(&portal_state.route.range_id) {
            session.set_timestamp_own_start_ts(own_start_ts);
            session.execute(portal, max_rows).await
        } else {
            let cap = self.cross_range_statement_cap();
            self.ensure_remote_session(portal_state.route.range_id)
                .await?;
            let remote = self
                .remote_sessions
                .get_mut(&portal_state.route.range_id)
                .expect("remote session inserted");
            remote.set_timestamp_own_start_ts(own_start_ts).await?;
            remote.execute(portal.to_owned(), max_rows, cap).await
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

    /// Fold the highest hosted local timestamp floor into every hosted
    /// engine's source before a statement read timestamp is allocated:
    /// single-shard bypass commits advance per-range sequences without the
    /// global oracle, and a read timestamp minted below an already
    /// acknowledged commit would serve a stale (non-linearizable) snapshot.
    /// The fold targets every engine because the read stamp is drawn from
    /// whichever engine seats the session — routed, forwarded, or portal.
    fn fold_hosted_read_floors(&self) -> Result<(), PgError> {
        let serving = self.current_serving()?;
        let mut floor = 0;
        for engine in serving.engines.values() {
            floor = floor.max(engine.local_timestamp_floor().map_err(ExecError::into_pg)?);
        }
        for engine in serving.engines.values() {
            engine.observe_timestamp_source(floor);
        }
        Ok(())
    }

    async fn execute_one(&mut self, statement: &str) -> Result<Vec<QueryResult>, PgError> {
        let result = self.execute_one_inner(statement).await;
        if result.is_err() && matches!(self.transaction, GatewayTransaction::Timestamp { .. }) {
            self.abort_failed_timestamp_transaction().await?;
        }
        self.finish_statement(result)
    }

    async fn abort_failed_timestamp_transaction(&mut self) -> Result<(), PgError> {
        let GatewayTransaction::Timestamp {
            identity,
            participants,
            ..
        } = &self.transaction
        else {
            return Ok(());
        };
        let identity = *identity;
        let participants = participants.clone();
        // Nothing durable exists until the first prewrite lands, so a scatter
        // abort has nothing to resolve and would only replace the statement's
        // failure with its own error.
        if !participants.is_empty() {
            self.abort_timestamp_scatter(identity, &participants.into_iter().collect::<Vec<_>>())
                .await
                .map_err(ExecError::into_pg)?;
        }
        self.complete_timestamp_abort();
        Ok(())
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
        let timestamp_write = route.scatter_ranges.is_some()
            || (route.kind == StatementKind::Dml
                && (matches!(
                    self.transaction,
                    GatewayTransaction::Idle | GatewayTransaction::Timestamp { .. }
                ) || matches!(self.transaction, GatewayTransaction::Open { ref touched, .. } if touched.is_empty()))
                && self.statement_targets_sharded_table(statement)?);
        let _dml_fences = self
            .acquire_routed_dml_fences(&route, timestamp_write)
            .await?;

        if route.kind == StatementKind::Query {
            self.fold_hosted_read_floors()?;
        }
        if let Some(ranges) = route.scatter_ranges.clone() {
            return self
                .execute_timestamp_scatter(statement, ranges)
                .await
                .map(|result| vec![result]);
        }
        if timestamp_write {
            return self
                .execute_timestamp_scatter(statement, vec![route.range_id])
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
        if matches!(self.transaction, GatewayTransaction::Timestamp { .. }) {
            return self.commit_explicit_timestamp_transaction().await;
        }
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
        if let GatewayTransaction::Timestamp {
            identity,
            participants,
            ..
        } = &self.transaction
        {
            let identity = *identity;
            let participants = participants.clone();
            if !participants.is_empty() {
                self.abort_timestamp_scatter(
                    identity,
                    &participants.into_iter().collect::<Vec<_>>(),
                )
                .await
                .map_err(ExecError::into_pg)?;
            }
            self.complete_timestamp_rollback();
            return Ok(rollback_command_response());
        }
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
        // DDL always executes on the range-0 catalog owner: locally when this
        // node hosts r0, otherwise forwarded to the owner over the range RPC.
        let results = if self.sessions.contains_key(&RangeId::COORDINATOR) {
            self.session_for(RangeId::COORDINATOR)?
                .simple_query(statement)
                .await?
        } else if let Some(forward) = &self.inner.remote_forward {
            forward
                .forward_ddl(statement.to_owned())
                .await
                .map_err(ForwardError::into_pg)?
        } else {
            return Err(PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                format!(
                    "range r{} is not hosted by tenant {} and no remote forwarder is configured",
                    RangeId::COORDINATOR,
                    self.inner.tenant
                ),
            ));
        };
        self.barrier_ddl_visibility().await?;
        Ok(results)
    }

    /// Confirm the committed catalog change is visible cluster-wide.
    ///
    /// Barrier contract: once DDL returns, a statement on any node observes the
    /// change — the local replica (if any) and every follower node's replica
    /// have applied it. A single-process topology has neither, so this is a
    /// no-op there. Failures here are reported as `58000` because the DDL is
    /// already committed on range 0; only its visibility is unconfirmed.
    async fn barrier_ddl_visibility(&self) -> Result<(), PgError> {
        if let Some(replica) = &self.inner.range0_replica {
            // The barrier's own timeout only bounds tail catch-up; the broker
            // end sample inside it can stall (for example on an admin
            // connection hang), so the whole local wait shares the follower
            // RPC reply budget to keep committed DDL from blocking forever.
            let reply_budget = self.inner.runtime_policy.range0_barrier_reply_budget;
            let wait =
                tokio::time::timeout(reply_budget.to_std(), replica.wait_for_latest_catalog());
            wait.await
                .map_err(|_| BarrierError::CatchUpTimeout(reply_budget))
                .and_then(|outcome| outcome)
                .map_err(|error| {
                    PgError::error(
                        "58000",
                        format!(
                            "ddl committed on range 0 but local catalog visibility was not confirmed: {error}"
                        ),
                    )
                })?;
        }
        if let Some(forward) = &self.inner.remote_forward {
            forward.barrier_catalog_followers().await.map_err(|error| {
                PgError::error(
                    "58000",
                    format!(
                        "ddl committed on range 0 but cluster-wide visibility was not confirmed: {error}"
                    ),
                )
            })?;
        }
        Ok(())
    }

    async fn acquire_routed_dml_fences(
        &mut self,
        route: &StatementRoute,
        timestamp_write: bool,
    ) -> Result<RoutedDmlFences, PgError> {
        let autocommit_topology_guard = if timestamp_write
            && matches!(self.transaction, GatewayTransaction::Idle)
        {
            Some(self.inner.topology_mutation_gate.clone().read_owned().await)
        } else {
            if timestamp_write
                && (matches!(self.transaction, GatewayTransaction::Timestamp { .. })
                    || matches!(self.transaction, GatewayTransaction::Open { ref touched, .. } if touched.is_empty()))
            {
                self.acquire_timestamp_topology_guard().await;
            }
            None
        };
        if route.kind != StatementKind::Dml {
            return Ok(RoutedDmlFences {
                _autocommit_topology_guard: autocommit_topology_guard,
                _table_write_guard: None,
            });
        }
        let Some(table_id) = route.table_id else {
            return Ok(RoutedDmlFences {
                _autocommit_topology_guard: autocommit_topology_guard,
                _table_write_guard: None,
            });
        };

        let table_write_gate = self.inner.table_write_gate(table_id);
        let table_write_gate = table_write_gate.read_owned().await;
        self.current_serving()?;
        Ok(RoutedDmlFences {
            _autocommit_topology_guard: autocommit_topology_guard,
            _table_write_guard: Some(table_write_gate),
        })
    }

    fn statement_targets_sharded_table(&self, statement: &str) -> Result<bool, PgError> {
        let normalized = statement.trim_start().to_ascii_lowercase();
        let table_refs = table_refs_in_statement(&normalized);
        let Some(table_ref) = table_refs.first() else {
            return Ok(false);
        };
        let serving = self.current_serving()?;
        let catalog = planner_engine(&serving)?;
        catalog_table_is_sharded(catalog, &table_ref.name)
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
        let own_start_ts = match &self.transaction {
            GatewayTransaction::Timestamp { identity, .. } if kind == StatementKind::Query => {
                Some(identity.start_ts)
            }
            _ => None,
        };
        self.session_for(range_id)?
            .set_timestamp_own_start_ts(own_start_ts);
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
        // `NOTIFY` is forwarded even though it is neither DML nor a query: the
        // coordinator owns the notification log, so running it there is exactly
        // what carries the notification to every node, this one included.
        let publishes_notification = statement_is_notify_family(statement);
        if kind != StatementKind::Dml && kind != StatementKind::Query && !publishes_notification {
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
        let own_start_ts = match &self.transaction {
            GatewayTransaction::Timestamp { identity, .. } if kind == StatementKind::Query => {
                Some(identity.start_ts)
            }
            _ => None,
        };
        let cap = self.cross_range_statement_cap();
        let remote = self
            .remote_sessions
            .get_mut(&range_id)
            .expect("remote session inserted");
        remote.set_timestamp_own_start_ts(own_start_ts).await?;
        let result = remote.simple_query(statement.to_owned(), cap).await;
        if result.is_ok() && publishes_notification {
            // The record is durable in the range-0 log by now; wake this node's
            // follower so its own listeners do not wait out a poll period.
            if let Some(replica) = &self.inner.range0_replica {
                replica.poke_catalog_refresh();
            }
        }
        result
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
        let explicit_timestamp = matches!(self.transaction, GatewayTransaction::Open { ref touched, .. } if touched.is_empty())
            || matches!(self.transaction, GatewayTransaction::Timestamp { .. });
        if !matches!(self.transaction, GatewayTransaction::Idle) && !explicit_timestamp {
            return Err(PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                "cannot mix ordinary and timestamp writes in one explicit transaction",
            ));
        }
        for range_id in &ranges {
            if !self.sessions.contains_key(range_id) {
                self.ensure_remote_session(*range_id).await?;
            }
        }
        // The INSERT-only check must stay ahead of planning: UPDATE/DELETE
        // planning scans the planning engine's own KV, which is only correct on
        // the range that owns the rows — not on an arbitrary planner seat.
        ensure_timestamp_scatter_is_supported(statement)?;
        let serving = self.current_serving()?;
        let planner = planner_engine(&serving)?;
        let autocommit = matches!(self.transaction, GatewayTransaction::Idle);
        // Single-shard bypass: an autocommit statement routed to exactly one
        // hosted range commits against that range's own local sequence instead
        // of the global timestamp source. `ranges` is the routed range set, so a
        // one-element set is the single-shard classification; the target engine
        // must be hosted here to reach its local sequence.
        let bypass_engine = if autocommit && ranges.len() == 1 {
            serving.engine(ranges[0])
        } else {
            None
        };
        let mut plan = planner
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
        let write_lease = {
            let lease = if let Some(engine) = bypass_engine {
                engine
                    .allocate_local_timestamp_write_lease(plan.writes.len())
                    .map_err(ExecError::into_pg)?
            } else {
                // Fold every routed hosted range's local timestamp floor into
                // the global source first: single-shard bypass commits advance
                // per-range sequences without the global oracle, so an unfolded
                // grant could mint hidden rowids that collide with rows those
                // ranges already committed locally. Only hosted engines can be
                // sampled — LogicalTso is the solo (co-hosted) mode, and
                // distributed deployments run the HLC source whose receive
                // rule folds remote stamps instead. A floor that is stale by
                // the time the lease lands (a concurrent bypass allocation)
                // fails closed through the prewrite reservation checks as a
                // retryable serialization conflict.
                for range_id in &ranges {
                    if let Some(engine) = serving.engine(*range_id) {
                        planner.observe_timestamp_source(
                            engine.local_timestamp_floor().map_err(ExecError::into_pg)?,
                        );
                    }
                }
                planner
                    .allocate_timestamp_write_lease(plan.writes.len())
                    .await
                    .map_err(ExecError::into_pg)?
            };
            plan = planner
                .plan_timestamp_write_sql_with_rowids(statement, &lease.hidden_rowids)
                .map_err(ExecError::into_pg)?;
            lease
        };
        let write_routes = timestamp_insert_write_routes(
            &self.current_serving()?.range_map,
            &table,
            statement,
            &plan.writes,
        )?;
        for (write, range_id) in plan.writes.into_iter().zip(write_routes) {
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
        if autocommit {
            let primary_range =
                timestamp_primary_range(&writes_by_range).expect("one primary range");
            let start_ts = write_lease.start_ts;
            let identity = crabka_pgexec::TimestampTxnIdentity {
                start_ts,
                global_xid: start_ts.get(),
                primary_range: primary_range.as_u32(),
            };
            let participants = writes_by_range.keys().copied().collect::<Vec<_>>();
            for (range_id, writes) in &writes_by_range {
                if *range_id == primary_range {
                    self.timestamp_prewrite_as_primary(*range_id, identity, &participants, writes)
                        .await?;
                } else {
                    self.timestamp_prewrite_as_secondary(*range_id, identity, writes)
                        .await?;
                    self.acknowledge_primary_operations(primary_range, identity, *range_id, writes)
                        .await?;
                }
            }
            if self.take_commit_fault_for_testing(
                GatewayCommitFault::AfterTimestampPrewriteBeforeDecision,
            ) {
                return Err(PgError::error(
                    "XX000",
                    "injected crash after timestamp prewrites before durable decision",
                ));
            }
            let commit_ts = if let Some(engine) = bypass_engine {
                engine
                    .allocate_local_commit_timestamp_after(start_ts)
                    .map_err(ExecError::into_pg)?
            } else {
                planner
                    .allocate_commit_timestamp_after(start_ts)
                    .await
                    .map_err(ExecError::into_pg)?
            };
            self.timestamp_resolve(
                primary_range,
                identity,
                crabka_pgexec::TimestampTxnDecision::Committed(commit_ts),
                writes_by_range.get(&primary_range).expect("primary writes"),
            )
            .await?;
            let committed_table_ids = writes_by_range
                .values()
                .flatten()
                .map(|write| write.table_id)
                .collect::<BTreeSet<_>>();
            tracing::info!(
                primary_range = primary_range.as_u32(),
                start_ts = start_ts.get(),
                table_ids = ?committed_table_ids,
                "timestamp_primary_committed"
            );
            for (range_id, writes) in &writes_by_range {
                if *range_id != primary_range {
                    self.timestamp_resolve(
                        *range_id,
                        identity,
                        crabka_pgexec::TimestampTxnDecision::Committed(commit_ts),
                        writes,
                    )
                    .await?;
                }
            }
            if self.take_commit_fault_for_testing(GatewayCommitFault::AfterTimestampCommitDecision)
            {
                return Err(PgError::error(
                    "XX000",
                    "injected crash after durable timestamp commit decision",
                ));
            }
            return Ok(plan.result);
        }
        let existing = matches!(self.transaction, GatewayTransaction::Timestamp { .. });
        let identity = if let GatewayTransaction::Timestamp { identity, .. } = &self.transaction {
            *identity
        } else {
            self.acquire_timestamp_topology_guard().await;
            self.current_serving()?;
            let primary_range = timestamp_primary_range(&writes_by_range).expect("primary range");
            let identity = crabka_pgexec::TimestampTxnIdentity {
                start_ts: write_lease.start_ts,
                global_xid: write_lease.start_ts.get(),
                primary_range: primary_range.as_u32(),
            };
            self.transaction = GatewayTransaction::Timestamp {
                identity,
                participants: BTreeMap::new(),
            };
            identity
        };
        let primary_range = RangeId::new(identity.primary_range);
        let mut statement_participants = Vec::with_capacity(writes_by_range.len());
        for (range_id, writes) in &writes_by_range {
            if existing && *range_id != primary_range {
                self.add_primary_participant(primary_range, identity, *range_id)
                    .await?;
            }
            let prewrite = if *range_id == primary_range {
                if existing {
                    self.timestamp_prewrite_on_primary(*range_id, identity, writes)
                        .await
                } else {
                    let participants = writes_by_range.keys().copied().collect::<Vec<_>>();
                    self.timestamp_prewrite_as_primary(*range_id, identity, &participants, writes)
                        .await
                }
            } else {
                self.timestamp_prewrite_as_secondary(*range_id, identity, writes)
                    .await
            };
            if let Err(error) = prewrite {
                // Nothing durable exists until the first prewrite lands, so a
                // scatter abort has nothing to resolve and would only replace
                // the prewrite failure with its own error.
                let participants = self.timestamp_participants_with(&statement_participants);
                if !participants.is_empty() {
                    self.abort_timestamp_scatter(identity, &participants)
                        .await
                        .map_err(ExecError::into_pg)?;
                }
                return Err(error);
            }
            statement_participants.push((*range_id, writes.clone()));
            if *range_id != primary_range {
                if !existing {
                    self.add_primary_participant(primary_range, identity, *range_id)
                        .await?;
                }
                self.acknowledge_primary_operations(primary_range, identity, *range_id, writes)
                    .await?;
            }
        }
        if !autocommit {
            let GatewayTransaction::Timestamp { participants, .. } = &mut self.transaction else {
                unreachable!()
            };
            for (range_id, writes) in statement_participants {
                participants.entry(range_id).or_default().extend(writes);
            }
            return Ok(plan.result);
        }
        unreachable!("autocommit timestamp writes return through the primary-range fast path")
    }

    fn timestamp_participants_with(
        &self,
        current: &[(RangeId, Vec<crabka_pgexec::TimestampWrite>)],
    ) -> Vec<(RangeId, Vec<crabka_pgexec::TimestampWrite>)> {
        let mut all = match &self.transaction {
            GatewayTransaction::Timestamp { participants, .. } => participants.clone(),
            _ => BTreeMap::new(),
        };
        for (range_id, writes) in current {
            all.entry(*range_id).or_default().extend(writes.clone());
        }
        all.into_iter().collect()
    }

    async fn commit_explicit_timestamp_transaction(&mut self) -> Result<Vec<QueryResult>, PgError> {
        let GatewayTransaction::Timestamp {
            identity,
            participants,
        } = &self.transaction
        else {
            unreachable!()
        };
        let identity = *identity;
        let participants = participants.clone();
        let serving = self.current_serving()?;
        let planner = planner_engine(&serving)?;
        if self
            .take_commit_fault_for_testing(GatewayCommitFault::AfterTimestampPrewriteBeforeDecision)
        {
            return Err(PgError::error(
                "XX000",
                "injected crash after timestamp prewrites before durable decision",
            ));
        }
        let commit_ts = planner
            .allocate_commit_timestamp_after(identity.start_ts)
            .await
            .map_err(ExecError::into_pg)?;
        let primary_range = RangeId::new(identity.primary_range);
        let primary_writes = participants.get(&primary_range).ok_or_else(|| {
            PgError::error("XX000", "timestamp primary has no participant writes")
        })?;
        self.timestamp_resolve(
            primary_range,
            identity,
            crabka_pgexec::TimestampTxnDecision::Committed(commit_ts),
            primary_writes,
        )
        .await?;
        if self.take_commit_fault_for_testing(GatewayCommitFault::AfterTimestampCommitDecision) {
            return Err(PgError::error(
                "XX000",
                "injected crash after durable timestamp commit decision",
            ));
        }
        for (range_id, writes) in &participants {
            if *range_id == primary_range {
                continue;
            }
            self.timestamp_resolve(
                *range_id,
                identity,
                crabka_pgexec::TimestampTxnDecision::Committed(commit_ts),
                writes,
            )
            .await?;
        }
        self.complete_timestamp_commit();
        Ok(vec![QueryResult::Command {
            tag: "COMMIT".into(),
        }])
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

    async fn timestamp_prewrite_as_primary(
        &self,
        range_id: RangeId,
        identity: crabka_pgexec::TimestampTxnIdentity,
        participants: &[RangeId],
        writes: &[crabka_pgexec::TimestampWrite],
    ) -> Result<(), PgError> {
        let participants = participants
            .iter()
            .map(|range| range.as_u32())
            .collect::<Vec<_>>();
        if let Ok(participant) = self.timestamp_participant(range_id) {
            return participant
                .prewrite_as_primary(identity, &participants, writes)
                .await
                .map_err(ExecError::into_pg);
        }
        self.remote_sessions
            .get(&range_id)
            .ok_or_else(|| {
                PgError::error("08003", "remote timestamp participant session is missing")
            })?
            .timestamp_prewrite_as_primary(identity, &participants, writes)
            .await
    }

    async fn timestamp_prewrite_as_secondary(
        &self,
        range_id: RangeId,
        identity: crabka_pgexec::TimestampTxnIdentity,
        writes: &[crabka_pgexec::TimestampWrite],
    ) -> Result<(), PgError> {
        if let Ok(participant) = self.timestamp_participant(range_id) {
            return participant
                .prewrite_as_secondary(identity, writes)
                .await
                .map_err(ExecError::into_pg);
        }
        self.remote_sessions
            .get(&range_id)
            .ok_or_else(|| {
                PgError::error("08003", "remote timestamp participant session is missing")
            })?
            .timestamp_prewrite_as_secondary(identity, writes)
            .await
    }

    async fn timestamp_prewrite_on_primary(
        &self,
        range_id: RangeId,
        identity: crabka_pgexec::TimestampTxnIdentity,
        writes: &[crabka_pgexec::TimestampWrite],
    ) -> Result<(), PgError> {
        if let Ok(participant) = self.timestamp_participant(range_id) {
            return participant
                .prewrite_on_primary(identity, writes)
                .await
                .map_err(ExecError::into_pg);
        }
        self.remote_sessions
            .get(&range_id)
            .ok_or_else(|| PgError::error("08003", "remote timestamp primary session is missing"))?
            .timestamp_prewrite_on_primary(identity, writes)
            .await
    }

    async fn add_primary_participant(
        &self,
        primary_range: RangeId,
        identity: crabka_pgexec::TimestampTxnIdentity,
        participant_range: RangeId,
    ) -> Result<(), PgError> {
        let serving = self.current_serving()?;
        if let Some(engine) = serving.engine(primary_range) {
            engine
                .validate_timestamp_primary_identity(identity)
                .map_err(ExecError::into_pg)?;
            return engine
                .add_timestamp_transaction_participant(
                    identity.start_ts,
                    participant_range.as_u32(),
                )
                .await
                .map(|_| ())
                .map_err(ExecError::into_pg);
        }
        self.remote_sessions
            .get(&primary_range)
            .ok_or_else(|| PgError::error("08003", "remote timestamp primary session is missing"))?
            .timestamp_primary_add_participant(identity, participant_range)
            .await
    }

    async fn acknowledge_primary_operations(
        &self,
        primary_range: RangeId,
        identity: crabka_pgexec::TimestampTxnIdentity,
        participant_range: RangeId,
        writes: &[crabka_pgexec::TimestampWrite],
    ) -> Result<(), PgError> {
        let operations = writes
            .iter()
            .map(|write| crabka_pgexec::TimestampTxnOperation {
                range_id: participant_range.as_u32(),
                table_id: write.table_id,
                bucket: write.bucket,
                rowid: write.rowid,
                delete: write.delete,
            })
            .collect::<Vec<_>>();
        let serving = self.current_serving()?;
        if let Some(engine) = serving.engine(primary_range) {
            engine
                .validate_timestamp_primary_identity(identity)
                .map_err(ExecError::into_pg)?;
            return engine
                .acknowledge_timestamp_participant_operations(
                    identity.start_ts,
                    participant_range.as_u32(),
                    &operations,
                )
                .await
                .map(|_| ())
                .map_err(ExecError::into_pg);
        }
        self.remote_sessions
            .get(&primary_range)
            .ok_or_else(|| PgError::error("08003", "remote timestamp primary session is missing"))?
            .timestamp_primary_ack(identity, participant_range, writes)
            .await
    }

    async fn timestamp_resolve(
        &self,
        range_id: RangeId,
        identity: crabka_pgexec::TimestampTxnIdentity,
        decision: crabka_pgexec::TimestampTxnDecision,
        writes: &[crabka_pgexec::TimestampWrite],
    ) -> Result<(), PgError> {
        if let Ok(participant) = self.timestamp_participant(range_id) {
            return if identity.primary_range == range_id.as_u32() {
                participant
                    .resolve_as_primary(identity, decision, writes)
                    .await
            } else {
                let primary_range = RangeId::new(identity.primary_range);
                let (actual_decision, primary_operations) = if let Some(primary) =
                    self.current_serving()?.engine(primary_range)
                {
                    let descriptor = primary
                        .validate_timestamp_primary_identity(identity)
                        .map_err(ExecError::into_pg)?;
                    (descriptor.decision, descriptor.operations)
                } else {
                    self.remote_sessions
                        .get(&primary_range)
                        .ok_or_else(|| {
                            PgError::error("08003", "remote timestamp primary session is missing")
                        })?
                        .timestamp_primary_inspect(identity)
                        .await?
                };
                let asserted_decision = match decision {
                    crabka_pgexec::TimestampTxnDecision::Aborted => {
                        crabka_pgexec::PrimaryTxnDecision::Aborted
                    }
                    crabka_pgexec::TimestampTxnDecision::Committed(commit_ts)
                    | crabka_pgexec::TimestampTxnDecision::Deleted(commit_ts) => {
                        crabka_pgexec::PrimaryTxnDecision::Committed(commit_ts)
                    }
                    crabka_pgexec::TimestampTxnDecision::Pending => {
                        return Err(PgError::error(
                            "40001",
                            "timestamp primary has no terminal decision",
                        ));
                    }
                };
                let asserted_operations = writes
                    .iter()
                    .map(|write| crabka_pgexec::TimestampTxnOperation {
                        range_id: range_id.as_u32(),
                        table_id: write.table_id,
                        bucket: write.bucket,
                        rowid: write.rowid,
                        delete: write.delete,
                    })
                    .collect::<Vec<_>>();
                let asserted_operations = canonicalize_timestamp_operations(asserted_operations)?;
                let actual_operations = primary_operations
                    .into_iter()
                    .filter(|operation| operation.range_id == range_id.as_u32())
                    .collect::<Vec<_>>();
                let actual_operations = canonicalize_timestamp_operations(actual_operations)?;
                if actual_decision == crabka_pgexec::PrimaryTxnDecision::Pending
                    || actual_decision != asserted_decision
                    || actual_operations != asserted_operations
                {
                    return Err(PgError::error(
                        "40001",
                        "timestamp secondary assertion differs from primary descriptor",
                    ));
                }
                participant
                    .resolve_as_secondary(identity, decision, writes)
                    .await
            }
            .map_err(ExecError::into_pg);
        }
        self.remote_sessions
            .get(&range_id)
            .ok_or_else(|| {
                PgError::error("08003", "remote timestamp participant session is missing")
            })?
            .timestamp_resolve(identity, decision, writes)
            .await
    }

    async fn abort_timestamp_scatter(
        &self,
        identity: crabka_pgexec::TimestampTxnIdentity,
        participants: &[(RangeId, Vec<crabka_pgexec::TimestampWrite>)],
    ) -> Result<(), ExecError> {
        let primary_range = RangeId::new(identity.primary_range);
        let primary_writes = participants
            .iter()
            .find(|(range_id, _)| *range_id == primary_range)
            .map(|(_, writes)| writes)
            .ok_or_else(|| {
                ExecError::Unsupported("timestamp primary participant is missing".into())
            })?;
        self.timestamp_resolve(
            primary_range,
            identity,
            crabka_pgexec::TimestampTxnDecision::Aborted,
            primary_writes,
        )
        .await
        .map_err(|error| ExecError::Unsupported(error.message))?;
        for (range_id, writes) in participants {
            if *range_id == primary_range {
                continue;
            }
            self.timestamp_resolve(
                *range_id,
                identity,
                crabka_pgexec::TimestampTxnDecision::Aborted,
                writes,
            )
            .await
            .map_err(|error| ExecError::Unsupported(error.message))?;
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
        let catalog = serving.planner_engine().ok_or_else(|| {
            ExecError::Unsupported("tenant has no hosted engine with a range-0 catalog view".into())
        })?;
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
                .simple_query("BEGIN".into(), None)
                .await?;
        }

        let GatewayTransaction::Open { touched, escalated } = &mut self.transaction else {
            return Ok(());
        };
        if escalates_transaction {
            *escalated = true;
        }
        touched.push(range_id);
        if *escalated {
            // The transaction now spans ranges, so a deadlock cycle it joins
            // can span engines and no single engine's wait-for graph will see
            // it. Bound every enlisted local session's lock waits so such a
            // cycle aborts (40P01) instead of hanging; remote participants are
            // hosted sessions, which are born with this cap.
            let enlisted = touched.clone();
            for enlisted_range in enlisted {
                if let Some(session) = self.sessions.get_mut(&enlisted_range) {
                    session.set_lock_wait_cap(Some(
                        self.inner.runtime_policy.cross_range_lock_wait_cap.to_std(),
                    ));
                }
            }
        }
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
        let catalog = planner_engine(&serving)?;
        let mut route = route_statement(&serving.range_map, catalog, sql)?;
        // Every notification statement acts on the registration this session
        // adopted onto its seat. Without that registration the statement would
        // appear to succeed while nothing on this connection could ever be
        // delivered, so refuse it instead.
        if statement_is_notify_family(sql) {
            self.notify.ensure_supported(self.inner.tenant.as_str())?;
        }
        // `NOTIFY` keeps routing to the coordinator, which owns the log every
        // node reads: publishing there is what makes it cross-gateway. A
        // subscription is the opposite — only this node's wire loop can deliver
        // to it — so `LISTEN`/`UNLISTEN` run on the local seat instead of being
        // forwarded to a bus on another node that could never reach back.
        if route.kind == StatementKind::Local
            && statement_is_subscription_change(sql)
            && let Some(seat) = self.notify.seat()
        {
            route.range_id = seat;
        }
        Ok(route)
    }

    /// The lock-wait cap this session's next remote statement should carry:
    /// bounded only while the transaction spans more than one range — the only
    /// state in which a wait can be an edge of a cross-engine deadlock cycle.
    /// Single-range and autocommit forwarding keep `None`, preserving exact
    /// engine-local blocking on the remote host.
    fn cross_range_statement_cap(&self) -> Option<crabka_units::Time> {
        matches!(
            self.transaction,
            GatewayTransaction::Open {
                escalated: true,
                ..
            }
        )
        .then_some(self.inner.runtime_policy.cross_range_lock_wait_cap)
    }

    fn release_explicit_transaction(&mut self) {
        if !self.explicit_transaction {
            return;
        }
        self.explicit_transaction = false;
        // Escalation bounded the touched local sessions' lock waits for the
        // cross-range transaction's lifetime; restore exact engine-local
        // blocking for whatever runs on them next.
        for session in self.sessions.values_mut() {
            session.set_lock_wait_cap(None);
        }
        self.inner
            .active_explicit_transactions
            .fetch_sub(1, Ordering::AcqRel);
    }
}

impl Drop for GatewaySession {
    fn drop(&mut self) {
        if let GatewayTransaction::Timestamp {
            identity,
            participants,
            ..
        } = &self.transaction
        {
            let identity = *identity;
            let participants = participants.clone();
            let serving = self.inner.serving.load_full();
            let remote_sessions = self.remote_sessions.clone();
            let topology_guard = self.timestamp_topology_guard.take();
            dispatch_dropped_timestamp_cleanup(
                serving,
                remote_sessions,
                identity,
                participants,
                topology_guard,
            );
        }
        self.release_explicit_transaction();
    }
}

fn dispatch_dropped_timestamp_cleanup(
    serving: Arc<ServingSnapshot>,
    remote_sessions: BTreeMap<RangeId, RemoteRangeSession>,
    identity: crabka_pgexec::TimestampTxnIdentity,
    participants: BTreeMap<RangeId, Vec<crabka_pgexec::TimestampWrite>>,
    topology_guard: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
) {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move {
            let _topology_guard = topology_guard;
            if let Err(error) = cleanup_dropped_timestamp_session(
                serving,
                remote_sessions,
                identity,
                participants,
            )
            .await
            {
                tracing::warn!(%error, start_ts = identity.start_ts.get(), "dropped timestamp session cleanup failed; descriptor recovery remains authoritative");
            }
        });
        return;
    }
    let spawn = std::thread::Builder::new()
        .name("crabka-timestamp-drop-cleanup".into())
        .spawn(move || {
            let _topology_guard = topology_guard;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            match runtime {
                Ok(runtime) => {
                    if let Err(error) = runtime.block_on(cleanup_dropped_timestamp_session(
                        serving,
                        remote_sessions,
                        identity,
                        participants,
                    )) {
                        tracing::warn!(%error, start_ts = identity.start_ts.get(), "fallback timestamp session cleanup failed; descriptor recovery remains authoritative");
                    }
                }
                Err(error) => tracing::warn!(%error, start_ts = identity.start_ts.get(), "failed to build timestamp cleanup runtime; descriptor recovery remains authoritative"),
            }
        });
    match spawn {
        Ok(thread) => {
            if thread.join().is_err() {
                tracing::warn!(
                    start_ts = identity.start_ts.get(),
                    "timestamp cleanup executor panicked; descriptor recovery remains authoritative"
                );
            }
        }
        Err(error) => {
            tracing::warn!(%error, start_ts = identity.start_ts.get(), "failed to spawn timestamp cleanup executor; descriptor recovery remains authoritative");
        }
    }
}

async fn cleanup_dropped_timestamp_session(
    serving: Arc<ServingSnapshot>,
    remote_sessions: BTreeMap<RangeId, RemoteRangeSession>,
    identity: crabka_pgexec::TimestampTxnIdentity,
    participants: BTreeMap<RangeId, Vec<crabka_pgexec::TimestampWrite>>,
) -> Result<(), PgError> {
    let primary_range = RangeId::new(identity.primary_range);
    let primary_decision = if let Some(engine) = serving.engine(primary_range) {
        engine
            .primary_timestamp_decision(identity.start_ts)
            .map_err(ExecError::into_pg)?
    } else {
        remote_sessions
            .get(&primary_range)
            .ok_or_else(|| PgError::error("08003", "remote timestamp primary session is missing"))?
            .timestamp_primary_decision(identity.start_ts)
            .await?
    };
    let decision = match primary_decision {
        crabka_pgexec::PrimaryTxnDecision::Pending => {
            let primary_writes = participants.get(&primary_range).ok_or_else(|| {
                PgError::error("XX000", "timestamp primary participant is missing")
            })?;
            if let Some(engine) = serving.engine(primary_range) {
                engine
                    .timestamp_txn_participant(primary_range.as_u32())
                    .resolve_as_primary(
                        identity,
                        crabka_pgexec::TimestampTxnDecision::Aborted,
                        primary_writes,
                    )
                    .await
                    .map_err(ExecError::into_pg)?;
            } else {
                remote_sessions
                    .get(&primary_range)
                    .ok_or_else(|| {
                        PgError::error("08003", "remote timestamp primary session is missing")
                    })?
                    .timestamp_resolve(
                        identity,
                        crabka_pgexec::TimestampTxnDecision::Aborted,
                        primary_writes,
                    )
                    .await?;
            }
            crabka_pgexec::TimestampTxnDecision::Aborted
        }
        crabka_pgexec::PrimaryTxnDecision::Aborted => crabka_pgexec::TimestampTxnDecision::Aborted,
        crabka_pgexec::PrimaryTxnDecision::Committed(commit_ts) => {
            crabka_pgexec::TimestampTxnDecision::Committed(commit_ts)
        }
    };
    for (range_id, writes) in participants {
        if range_id == primary_range {
            continue;
        }
        if let Some(engine) = serving.engine(range_id) {
            engine
                .timestamp_txn_participant(range_id.as_u32())
                .resolve_as_secondary(identity, decision, &writes)
                .await
                .map_err(ExecError::into_pg)?;
        } else {
            remote_sessions
                .get(&range_id)
                .ok_or_else(|| PgError::error("08003", "remote timestamp session is missing"))?
                .timestamp_resolve(identity, decision, &writes)
                .await?;
        }
    }
    Ok(())
}

/// Map each INSERT tuple to its physical owner. Hash-sharded tables use their
/// declared literal shard keys; row-sharded tables use their leased hidden IDs.
fn timestamp_insert_write_routes(
    range_map: &RangeMap,
    table: &crabka_pgcatalog::Table,
    statement: &str,
    writes: &[crabka_pgexec::TimestampWrite],
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
        writes
            .iter()
            .map(|write| {
                range_map
                    .range_for_key(table_id, write.rowid)
                    .map(|route| route.range_id)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| map_error_to_pg(&error))?
    };
    if routes.len() != writes.len() {
        return Err(PgError::error(
            sqlstate::FEATURE_NOT_SUPPORTED,
            "timestamp scatter tuple routing did not match planned writes",
        ));
    }
    Ok(routes)
}

fn ensure_timestamp_scatter_is_supported(statement: &str) -> Result<(), PgError> {
    let statements = crabka_pgparser::parse(statement).map_err(|error| {
        PgError::error(
            sqlstate::FEATURE_NOT_SUPPORTED,
            format!("cannot parse timestamp scatter statement: {error}"),
        )
    })?;
    // `ON CONFLICT` needs the arbiter's unique-key probe, which is engine-local:
    // scattering the statement would let each range decide independently. The
    // primary refusal lives in the executor's timestamp write path; this keeps an
    // upsert from reaching the scatter protocol as if it were a plain insert.
    if matches!(
        statements.as_slice(),
        [crabka_pgparser::ast::Statement::Insert {
            on_conflict: None,
            ..
        }]
    ) {
        return Ok(());
    }
    Err(PgError::error(
        sqlstate::FEATURE_NOT_SUPPORTED,
        "multi-range timestamp scatter currently supports plain INSERT only; INSERT ... ON CONFLICT, UPDATE, and DELETE remain unsupported",
    ))
}

/// Whether `sql` acts on this connection's notification registration:
/// `LISTEN`, `NOTIFY`, `UNLISTEN`, or a `pg_notify()` call anywhere in a query.
///
/// `pg_notify` publishes exactly as `NOTIFY` does, so a statement calling it is
/// as much a notification statement as one that leads with the keyword —
/// matching only the leading word would let `SELECT pg_notify('c', 'p')` slip
/// past the registration check and be accepted by a connection that can never
/// be delivered to.
fn statement_is_notify_family(sql: &str) -> bool {
    leading_keyword_is(sql, &["listen", "notify", "unlisten"]) || calls_pg_notify(sql)
}

/// Whether `sql` changes this connection's subscriptions, as opposed to
/// publishing. There is no function form of either, so the leading keyword
/// decides.
fn statement_is_subscription_change(sql: &str) -> bool {
    leading_keyword_is(sql, &["listen", "unlisten"])
}

fn leading_keyword_is(sql: &str, keywords: &[&str]) -> bool {
    let command = sql
        .trim_start()
        .split(|character: char| character.is_ascii_whitespace() || character == ';')
        .next()
        .unwrap_or_default();
    keywords
        .iter()
        .any(|keyword| command.eq_ignore_ascii_case(keyword))
}

/// Whether `sql` contains a call to `pg_notify`: the identifier on a token
/// boundary, followed by its argument list.
///
/// Deliberately lexical rather than parsed, and allocation-free — every
/// forwarded statement passes through here ahead of planning, and the cost of a
/// false positive is a refusal on a connection that could not have been
/// delivered to anyway.
fn calls_pg_notify(sql: &str) -> bool {
    const NAME: &[u8] = b"pg_notify";
    let bytes = sql.as_bytes();
    bytes.windows(NAME.len()).enumerate().any(|(at, window)| {
        window.eq_ignore_ascii_case(NAME)
                // At index 0 the wrapping subtraction indexes past the end, so
                // `get` reports what is true there: nothing precedes the name.
                && !bytes
                    .get(at.wrapping_sub(1))
                    .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
                && sql[at + NAME.len()..].trim_start().starts_with('(')
    })
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
        GatewayTransaction::Idle | GatewayTransaction::Timestamp { .. } => None,
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

fn timestamp_primary_range(
    writes_by_range: &BTreeMap<RangeId, Vec<crabka_pgexec::TimestampWrite>>,
) -> Option<RangeId> {
    writes_by_range.keys().next().copied()
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

    if kind == StatementKind::Dml && sql.starts_with("insert") {
        return Ok(RouteTarget::scatter(
            range_map
                .ranges()
                .iter()
                .map(|range| range.range_id)
                .collect(),
        ));
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
    let table_name = crabka_pgcatalog::RelationName::public(table_name);
    match catalog.table_uses_global_visibility(&table_name) {
        Ok(uses_global_visibility) => Ok(uses_global_visibility),
        Err(ExecError::Catalog(crabka_pgcatalog::CatalogError::UndefinedTable(_))) => Ok(false),
        Err(error) => Err(error.into_pg()),
    }
}

fn catalog_table(
    catalog: &crabka_pgexec::SqlEngine,
    table_name: &str,
) -> Result<crabka_pgcatalog::Table, PgError> {
    let table_name = crabka_pgcatalog::RelationName::public(table_name);
    catalog
        .catalog_table(&table_name)
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
        let table_name = crabka_pgcatalog::RelationName::public(&table_ref.name);
        let uses_global = match catalog.table_uses_global_visibility(&table_name) {
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

fn routing_table_id<T: std::fmt::Display + ?Sized>(table: &T) -> TableId {
    let table = table.to_string();
    trailing_table_id(&table).unwrap_or(TableId::ZERO)
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
        | Datum::Int2(_)
        | Datum::Float4(_)
        | Datum::Float8(_)
        | Datum::Point(_)
        | Datum::Path(_)
        | Datum::Numeric(_)
        | Datum::Date(_)
        | Datum::Time(_)
        | Datum::Timetz(_)
        | Datum::Timestamp(_)
        | Datum::Timestamptz(_)
        | Datum::Interval(_)
        | Datum::Jsonb(_)
        | Datum::JsonPath(_)
        | Datum::Array(_)
        | Datum::OidVector(_)
        | Datum::Record(_)
        | Datum::Enum(_)
        | Datum::Regclass(_)
        | Datum::TsVector(_)
        | Datum::TsQuery(_)
        | Datum::Range(_)
        | Datum::Multirange(_) => None,
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

fn routing_sql_with_bound_params(
    sql: &str,
    parameter_types: &[u32],
    params: &[BoundParam],
) -> Result<String, PgError> {
    if params.len() != parameter_types.len() {
        return Err(PgError::protocol(format!(
            "bind message supplies {} parameters, but prepared statement requires {}",
            params.len(),
            parameter_types.len()
        )));
    }
    let literals = params
        .iter()
        .zip(parameter_types)
        .map(|(param, oid)| routing_param_literal(param, *oid))
        .collect::<Result<Vec<_>, _>>()?;
    let bytes = sql.as_bytes();
    let mut result = String::with_capacity(sql.len());
    let mut index = 0usize;
    let mut single_quoted = false;
    let mut double_quoted = false;
    while index < bytes.len() {
        if !single_quoted && !double_quoted && bytes[index..].starts_with(b"--") {
            let end = bytes[index..]
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(bytes.len(), |offset| index + offset);
            result.push_str(&sql[index..end]);
            index = end;
            continue;
        }
        if !single_quoted && !double_quoted && bytes[index..].starts_with(b"/*") {
            let end = sql[index + 2..]
                .find("*/")
                .map_or(bytes.len(), |offset| index + 2 + offset + 2);
            result.push_str(&sql[index..end]);
            index = end;
            continue;
        }
        if !single_quoted
            && !double_quoted
            && bytes[index] == b'$'
            && let Some(tag_end) = sql[index + 1..].find('$')
        {
            let delimiter_end = index + 1 + tag_end;
            let tag = &sql[index + 1..delimiter_end];
            if tag.is_empty()
                || tag
                    .chars()
                    .next()
                    .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
            {
                let delimiter = &sql[index..=delimiter_end];
                let body_start = delimiter_end + 1;
                let end = sql[body_start..]
                    .find(delimiter)
                    .map_or(bytes.len(), |offset| body_start + offset + delimiter.len());
                result.push_str(&sql[index..end]);
                index = end;
                continue;
            }
        }
        match bytes[index] {
            b'\'' if !double_quoted => {
                result.push('\'');
                index += 1;
                if single_quoted && index < bytes.len() && bytes[index] == b'\'' {
                    result.push('\'');
                    index += 1;
                } else {
                    single_quoted = !single_quoted;
                }
            }
            b'"' if !single_quoted => {
                double_quoted = !double_quoted;
                result.push('"');
                index += 1;
            }
            b'\\' if single_quoted && index + 1 < bytes.len() => {
                let character = sql[index..]
                    .chars()
                    .next()
                    .expect("backslash is within SQL string");
                result.push(character);
                index += character.len_utf8();
                let escaped = sql[index..]
                    .chars()
                    .next()
                    .expect("escaped character exists");
                result.push(escaped);
                index += escaped.len_utf8();
            }
            b'$' if !single_quoted && !double_quoted => {
                let start = index + 1;
                let mut end = start;
                while end < bytes.len() && bytes[end].is_ascii_digit() {
                    end += 1;
                }
                if end == start {
                    result.push('$');
                    index += 1;
                    continue;
                }
                let number = sql[start..end]
                    .parse::<usize>()
                    .map_err(|_| PgError::error("42P02", "invalid parameter reference"))?;
                let literal = number
                    .checked_sub(1)
                    .and_then(|parameter| literals.get(parameter))
                    .ok_or_else(|| {
                        PgError::error("42P02", format!("there is no parameter ${number}"))
                    })?;
                result.push_str(literal);
                index = end;
            }
            _ => {
                let character = sql[index..]
                    .chars()
                    .next()
                    .expect("index is within SQL string");
                result.push(character);
                index += character.len_utf8();
            }
        }
    }
    Ok(result)
}

fn routing_param_literal(param: &BoundParam, inferred_oid: u32) -> Result<String, PgError> {
    let oid = param.type_oid.unwrap_or(inferred_oid);
    if oid != inferred_oid {
        return Err(PgError::protocol(format!(
            "bound parameter type {oid} does not match inferred type {inferred_oid}"
        )));
    }
    let Some(value) = param.value.as_deref() else {
        return Ok("null".to_owned());
    };
    match (oid, param.format) {
        (16, 0) => match std::str::from_utf8(value).unwrap_or_default() {
            "true" | "t" | "1" => Ok("true".into()),
            "false" | "f" | "0" => Ok("false".into()),
            _ => Err(PgError::error("22P02", "invalid boolean parameter")),
        },
        (16, 1) if value.len() == 1 => match value[0] {
            0 => Ok("false".into()),
            1 => Ok("true".into()),
            _ => Err(PgError::error("22P02", "invalid boolean parameter")),
        },
        (21, 0) => std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse::<i16>().ok())
            .map(|value| value.to_string())
            .ok_or_else(|| PgError::error("22P02", "invalid smallint parameter")),
        (21, 1) if value.len() == 2 => {
            Ok(i16::from_be_bytes(value.try_into().expect("length checked")).to_string())
        }
        (23 | 20, 0) => std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .map(|value| value.to_string())
            .ok_or_else(|| PgError::error("22P02", "invalid integer parameter")),
        (23, 1) if value.len() == 4 => {
            Ok(i32::from_be_bytes(value.try_into().expect("length checked")).to_string())
        }
        (20, 1) if value.len() == 8 => {
            Ok(i64::from_be_bytes(value.try_into().expect("length checked")).to_string())
        }
        (700, 1) if value.len() == 4 => Ok(f32::from_bits(u32::from_be_bytes(
            value.try_into().expect("length checked"),
        ))
        .to_string()),
        (701, 1) if value.len() == 8 => Ok(f64::from_bits(u64::from_be_bytes(
            value.try_into().expect("length checked"),
        ))
        .to_string()),
        (17, 1) => Ok(format!("'\\x{}'", hex_bytes(value))),
        (25 | 1043 | 1042, 0 | 1) => {
            let value = std::str::from_utf8(value)
                .map_err(|_| PgError::error("22021", "invalid UTF-8 text parameter"))?;
            Ok(format!("'{}'", value.replace('\'', "''")))
        }
        (17, 0) => {
            let value = std::str::from_utf8(value)
                .map_err(|_| PgError::error("22021", "invalid UTF-8 bytea parameter"))?;
            Ok(format!("'{}'", value.replace('\'', "''")))
        }
        (700, 0) => typed_numeric_literal(value, "real"),
        (701, 0) => typed_numeric_literal(value, "double precision"),
        (1700, 0) => typed_numeric_literal(value, "numeric"),
        (1082, 0) => quoted_typed_literal(value, "date"),
        (1083, 0) => quoted_typed_literal(value, "time"),
        (1114, 0) => quoted_typed_literal(value, "timestamp"),
        (1184, 0) => quoted_typed_literal(value, "timestamptz"),
        (1186, 0) => quoted_typed_literal(value, "interval"),
        (2950, 0) => quoted_typed_literal(value, "uuid"),
        (2950, 1) if value.len() == 16 => Ok(format_binary_uuid(value)),
        (2950, 1) => Err(PgError::error(
            "22P03",
            "invalid binary UUID parameter length",
        )),
        (_, 0 | 1) => Err(PgError::error(
            sqlstate::FEATURE_NOT_SUPPORTED,
            format!("parameter type {oid} cannot be used as a shard key"),
        )),
        (_, format) => Err(PgError::protocol(format!(
            "unsupported parameter format code {format}"
        ))),
    }
}

fn quoted_typed_literal(value: &[u8], ty: &str) -> Result<String, PgError> {
    let value = std::str::from_utf8(value)
        .map_err(|_| PgError::error("22021", "invalid UTF-8 parameter"))?;
    Ok(format!("'{}'::{ty}", value.replace('\'', "''")))
}

fn typed_numeric_literal(value: &[u8], ty: &str) -> Result<String, PgError> {
    let value = std::str::from_utf8(value)
        .map_err(|_| PgError::error("22021", "invalid UTF-8 numeric parameter"))?;
    if value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'+' | b'-' | b'.' | b'e' | b'E'))
    {
        Ok(format!("{value}::{ty}"))
    } else {
        Err(PgError::error("22P02", "invalid numeric parameter"))
    }
}

fn hex_bytes(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

fn format_binary_uuid(value: &[u8]) -> String {
    let hex = hex_bytes(value);
    format!(
        "'{}-{}-{}-{}-{}'::uuid",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
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

fn configure_successor_engine(coordinator: &SqlEngine, successor: &mut SqlEngine) {
    successor.set_catalog_kv(coordinator.kv_handle());
    coordinator.share_gtm_to(successor);
    successor.set_timestamp_oracle(coordinator.timestamp_oracle_handle());
    if let Some(scanner) = coordinator.foreign_scanner_handle() {
        successor.set_foreign_scanner(scanner);
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgexec::{TimestampSource, WallClock as _};
    use crabka_pgkv::{Kv, MemKv};

    use super::*;

    fn text_rows(results: &[QueryResult]) -> Vec<Vec<Option<String>>> {
        let [QueryResult::Rows { rows, .. }] = results else {
            panic!("expected one row result: {results:?}")
        };
        rows.iter()
            .map(|row| {
                row.iter()
                    .map(|cell| {
                        cell.as_ref()
                            .map(|cell| String::from_utf8(cell.text.to_vec()).expect("UTF-8 cell"))
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn timestamp_recovery_rehomes_predecessor_operations_by_active_hash_ownership() {
        let split_at = RangeKey::hash(TableId::new(52), 2, 0);
        let right_at = RangeKey::hash(TableId::new(52), 8, 0);
        let range_map = RangeMap::new(
            tenant(),
            MapEpoch::new(9),
            vec![
                RangeSpec::for_interval(RangeId::new(0), RangeKey::MIN, Some(split_at)),
                RangeSpec::for_interval(RangeId::new(2), split_at, Some(right_at)),
                RangeSpec::for_interval(RangeId::new(3), right_at, None),
            ],
        )
        .expect("post-split map");
        let start_ts = crabka_pgexec::TimestampTransactionId::new(10).expect("start timestamp");
        let mut descriptor = crabka_pgexec::TimestampTxnDescriptor::begun(start_ts, 7, vec![0, 1]);
        descriptor.prepared = vec![0, 1];
        descriptor.operations = vec![
            crabka_pgexec::TimestampTxnOperation {
                range_id: 0,
                table_id: 52,
                bucket: Some(1),
                rowid: 1,
                delete: false,
            },
            crabka_pgexec::TimestampTxnOperation {
                range_id: 1,
                table_id: 52,
                bucket: Some(2),
                rowid: 2,
                delete: false,
            },
            crabka_pgexec::TimestampTxnOperation {
                range_id: 1,
                table_id: 52,
                bucket: Some(8),
                rowid: 3,
                delete: true,
            },
        ];

        let routed = route_timestamp_descriptor_to_active_map(descriptor, &range_map)
            .expect("route recovered descriptor");

        assert_eq!(routed.participants, vec![0, 2, 3]);
        assert_eq!(routed.prepared, vec![0, 2, 3]);
        assert_eq!(
            routed
                .operations
                .iter()
                .map(|operation| operation.range_id)
                .collect::<Vec<_>>(),
            vec![0, 2, 3]
        );
    }

    #[test]
    fn timestamp_recovery_preserves_an_active_participant_despite_physical_table_identity() {
        let range_map = MultiRangeTenantConfig::from_boundaries(tenant(), "0:0,0:50")
            .expect("active map")
            .range_map;
        let start_ts = crabka_pgexec::TimestampTransactionId::new(10).expect("start timestamp");
        let mut descriptor = crabka_pgexec::TimestampTxnDescriptor::begun(start_ts, 7, vec![0]);
        descriptor.prepared = vec![0];
        descriptor.operations = vec![crabka_pgexec::TimestampTxnOperation {
            range_id: 0,
            table_id: 1,
            bucket: None,
            rowid: 60,
            delete: false,
        }];

        let routed = route_timestamp_descriptor_to_active_map(descriptor, &range_map)
            .expect("preserve active participant");

        assert_eq!(routed.participants, vec![0]);
        assert_eq!(routed.prepared, vec![0]);
        assert_eq!(routed.operations[0].range_id, 0);
    }

    #[test]
    fn settled_terminal_descriptor_is_not_replayed_on_a_hosted_participant() {
        let start_ts = crabka_pgexec::TimestampTransactionId::new(10).expect("start timestamp");
        let commit_ts =
            crabka_pgexec::CommitTimestamp::after_start(start_ts, 20).expect("commit timestamp");
        let mut descriptor = crabka_pgexec::TimestampTxnDescriptor::begun(start_ts, 7, vec![1]);
        descriptor
            .decide(crabka_pgexec::PrimaryTxnDecision::Committed(commit_ts))
            .expect("terminal descriptor");
        let identity = crabka_pgexec::TimestampTxnIdentity {
            start_ts,
            global_xid: 7,
            primary_range: 0,
        };
        let outstanding = BTreeSet::from([crabka_pgexec::DurableTimestampIntentIdentity {
            identity,
            participant_range: 1,
        }]);

        assert!(!hosted_participant_requires_recovery(
            &descriptor,
            identity,
            RangeId::new(1),
            &BTreeSet::new(),
        ));
        assert!(hosted_participant_requires_recovery(
            &descriptor,
            identity,
            RangeId::new(1),
            &outstanding,
        ));

        let pending = crabka_pgexec::TimestampTxnDescriptor::begun(start_ts, 7, vec![1]);
        assert!(hosted_participant_requires_recovery(
            &pending,
            identity,
            RangeId::new(1),
            &BTreeSet::new(),
        ));
    }

    #[test]
    fn timestamp_primary_is_the_first_write_range_not_range_zero() {
        let writes = BTreeMap::from([
            (
                RangeId::new(3),
                vec![crabka_pgexec::TimestampWrite {
                    table_id: 50,
                    bucket: None,
                    rowid: 1,
                    row: vec![Datum::Int4(1)],
                    delete: false,
                    global_index_intents: Vec::new(),
                }],
            ),
            (
                RangeId::new(4),
                vec![crabka_pgexec::TimestampWrite {
                    table_id: 50,
                    bucket: None,
                    rowid: 2,
                    row: vec![Datum::Int4(2)],
                    delete: false,
                    global_index_intents: Vec::new(),
                }],
            ),
        ]);

        assert_eq!(timestamp_primary_range(&writes), Some(RangeId::new(3)));
    }

    #[tokio::test]
    async fn table_fence_allows_overlapping_dml_and_blocks_exclusive_split() {
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse("table_fence_overlap").expect("tenant"),
            "0,100",
        )
        .expect("config");
        let (_gateway, handles) = MultiRangeTenant::start(config).expect("start");
        let gate = handles.inner.table_write_gate(TableId::new(50));
        let first_dml = Arc::clone(&gate).read_owned().await;
        let second_dml = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            Arc::clone(&gate).read_owned(),
        )
        .await
        .expect("DML overlaps");
        let exclusive = tokio::spawn(Arc::clone(&gate).write_owned());
        tokio::task::yield_now().await;
        assert!(!exclusive.is_finished());
        drop(first_dml);
        assert!(!exclusive.is_finished());
        drop(second_dml);
        exclusive.await.expect("exclusive fence");
    }

    async fn timestamp_session_with_blocked_publication() -> (
        GatewaySession,
        tokio::task::JoinHandle<tokio::sync::OwnedRwLockWriteGuard<()>>,
    ) {
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse("timestamp_topology_fence").expect("tenant"),
            "0,100",
        )
        .expect("config");
        let (gateway, handles) = MultiRangeTenant::start(config).expect("start");
        let mut session = gateway.connect();
        session.acquire_timestamp_topology_guard().await;
        let publication = tokio::spawn(handles.inner.topology_mutation_gate.clone().write_owned());
        tokio::task::yield_now().await;
        assert!(!publication.is_finished());
        (session, publication)
    }

    fn timestamp_dml_route(table_id: TableId) -> StatementRoute {
        StatementRoute {
            kind: StatementKind::Dml,
            range_id: RangeId::COORDINATOR,
            table_id: Some(table_id),
            scatter_ranges: Some(vec![RangeId::COORDINATOR]),
        }
    }

    #[tokio::test]
    async fn timestamp_dml_waiting_on_topology_never_holds_table_fence() {
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse("timestamp-lock-order-interleave").expect("tenant"),
            "0,100",
        )
        .expect("config");
        let (gateway, handles) = MultiRangeTenant::start(config).expect("start");
        let table_id = TableId::new(50);
        let table_gate = handles.inner.table_write_gate(table_id);
        let topology_publication = handles
            .inner
            .topology_mutation_gate
            .clone()
            .write_owned()
            .await;
        let mut session = gateway.connect();
        let incoming_dml = tokio::spawn(async move {
            session
                .acquire_routed_dml_fences(&timestamp_dml_route(table_id), true)
                .await
                .expect("timestamp DML fences")
        });
        tokio::task::yield_now().await;
        assert!(!incoming_dml.is_finished());

        let table_publication =
            tokio::time::timeout(std::time::Duration::from_secs(1), table_gate.write_owned())
                .await
                .expect("topology-first DML cannot block publication's table fence");
        drop(table_publication);
        drop(topology_publication);
        incoming_dml.await.expect("incoming DML task");
    }

    #[tokio::test]
    async fn timestamp_dml_fence_helper_holds_topology_then_table() {
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse("timestamp-lock-order-truth").expect("tenant"),
            "0,100",
        )
        .expect("config");
        let (gateway, handles) = MultiRangeTenant::start(config).expect("start");
        let table_id = TableId::new(50);
        let table_gate = handles.inner.table_write_gate(table_id);
        let mut session = gateway.connect();
        let fences = session
            .acquire_routed_dml_fences(&timestamp_dml_route(table_id), true)
            .await
            .expect("timestamp DML fences");

        let topology_publication =
            tokio::spawn(handles.inner.topology_mutation_gate.clone().write_owned());
        let table_publication = tokio::spawn(table_gate.write_owned());
        tokio::task::yield_now().await;
        assert!(!topology_publication.is_finished());
        assert!(!table_publication.is_finished());

        drop(fences);
        topology_publication.await.expect("topology publication");
        table_publication.await.expect("table publication");
    }

    #[tokio::test]
    async fn timestamp_commit_releases_topology_publication_fence() {
        let (mut session, publication) = timestamp_session_with_blocked_publication().await;
        session.transaction = GatewayTransaction::Timestamp {
            identity: crabka_pgexec::TimestampTxnIdentity {
                start_ts: crabka_pgexec::TimestampTransactionId::new(10).expect("timestamp"),
                global_xid: 10,
                primary_range: RangeId::COORDINATOR.as_u32(),
            },
            participants: BTreeMap::new(),
        };

        session.complete_timestamp_commit();

        assert_eq!(session.transaction, GatewayTransaction::Idle);
        assert_eq!(session.status, TxStatus::Idle);
        publication
            .await
            .expect("commit releases publication fence");
    }

    #[tokio::test]
    async fn timestamp_abort_releases_topology_publication_fence() {
        let (mut session, publication) = timestamp_session_with_blocked_publication().await;
        session.transaction = GatewayTransaction::Timestamp {
            identity: crabka_pgexec::TimestampTxnIdentity {
                start_ts: crabka_pgexec::TimestampTransactionId::new(11).expect("timestamp"),
                global_xid: 11,
                primary_range: RangeId::COORDINATOR.as_u32(),
            },
            participants: BTreeMap::new(),
        };

        session.complete_timestamp_abort();

        assert!(matches!(
            session.transaction,
            GatewayTransaction::Failed { .. }
        ));
        assert_eq!(session.status, TxStatus::Failed);
        publication.await.expect("abort releases publication fence");
    }

    #[tokio::test]
    async fn timestamp_rollback_releases_topology_and_explicit_transaction_fences() {
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse("timestamp-rollback-fences").expect("tenant"),
            "0,100",
        )
        .expect("config");
        let (gateway, handles) = MultiRangeTenant::start(config).expect("start");
        let mut session = gateway.connect();
        session
            .simple_query("CREATE TABLE s (id int4) SHARDED")
            .await
            .expect("create sharded table");
        session.simple_query("BEGIN").await.expect("begin");
        session
            .simple_query("INSERT INTO s VALUES (1)")
            .await
            .expect("timestamp write");
        assert!(matches!(
            session.transaction,
            GatewayTransaction::Timestamp { .. }
        ));
        assert!(session.timestamp_topology_guard.is_some());
        assert!(session.explicit_transaction);
        let publication = tokio::spawn(handles.inner.topology_mutation_gate.clone().write_owned());
        tokio::task::yield_now().await;
        assert!(!publication.is_finished());

        session.simple_query("ROLLBACK").await.expect("rollback");

        assert_eq!(session.transaction, GatewayTransaction::Idle);
        assert_eq!(session.status, TxStatus::Idle);
        assert!(session.timestamp_topology_guard.is_none());
        assert!(!session.explicit_transaction);
        publication
            .await
            .expect("rollback releases publication fence");
    }

    async fn session_with_unabortable_timestamp(gateway: &MultiRangeTenant) -> GatewaySession {
        let mut session = gateway.connect();
        session.acquire_timestamp_topology_guard().await;
        // A prewritten secondary without its primary participant cannot be
        // scatter-aborted; an EMPTY participant set is trivially abortable
        // (nothing durable exists yet), so the unabortable fixture must carry
        // a participant while the primary is absent.
        session.transaction = GatewayTransaction::Timestamp {
            identity: crabka_pgexec::TimestampTxnIdentity {
                start_ts: crabka_pgexec::TimestampTransactionId::new(12).expect("timestamp"),
                global_xid: 12,
                primary_range: RangeId::COORDINATOR.as_u32(),
            },
            participants: BTreeMap::from([(RangeId::new(1), Vec::new())]),
        };
        session
    }

    #[tokio::test]
    async fn timestamp_rollback_error_preserves_cleanup_capable_state() {
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse("timestamp-rollback-error").expect("tenant"),
            "0,100",
        )
        .expect("config");
        let (gateway, _) = MultiRangeTenant::start(config).expect("start");
        let mut session = session_with_unabortable_timestamp(&gateway).await;

        session
            .rollback_transaction()
            .await
            .expect_err("missing primary participant abort fails");

        assert!(matches!(
            session.transaction,
            GatewayTransaction::Timestamp { .. }
        ));
        assert!(session.timestamp_topology_guard.is_some());
    }

    #[tokio::test]
    async fn failed_timestamp_rollback_drop_releases_fence_after_cleanup_attempt() {
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse("timestamp-rollback-drop").expect("tenant"),
            "0,100",
        )
        .expect("config");
        let (gateway, handles) = MultiRangeTenant::start(config).expect("start");
        let mut session = session_with_unabortable_timestamp(&gateway).await;
        let publication = tokio::spawn(handles.inner.topology_mutation_gate.clone().write_owned());
        tokio::task::yield_now().await;
        assert!(!publication.is_finished());
        session
            .rollback_transaction()
            .await
            .expect_err("missing primary participant abort fails");

        drop(session);

        tokio::time::timeout(std::time::Duration::from_secs(5), publication)
            .await
            .expect("drop cleanup releases topology fence")
            .expect("publication task");
    }

    #[tokio::test]
    async fn dropped_timestamp_session_holds_fence_through_async_cleanup() {
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse("timestamp_drop_topology_fence").expect("tenant"),
            "0,100",
        )
        .expect("config");
        let (gateway, handles) = MultiRangeTenant::start(config).expect("start");
        let mut session = gateway.connect();
        session.acquire_timestamp_topology_guard().await;
        session.transaction = GatewayTransaction::Timestamp {
            identity: crabka_pgexec::TimestampTxnIdentity {
                start_ts: crabka_pgexec::TimestampTransactionId::new(10).expect("timestamp"),
                global_xid: 10,
                primary_range: RangeId::COORDINATOR.as_u32(),
            },
            participants: BTreeMap::new(),
        };
        let publication = tokio::spawn(handles.inner.topology_mutation_gate.clone().write_owned());
        tokio::task::yield_now().await;
        assert!(!publication.is_finished());
        drop(session);
        tokio::time::timeout(std::time::Duration::from_secs(5), publication)
            .await
            .expect("cleanup releases publication fence")
            .expect("publication task");
    }

    fn tenant() -> TenantName {
        TenantName::parse("tenant_a").expect("tenant")
    }

    #[tokio::test]
    async fn control_markers_reads_exact_r1_owner() {
        let config =
            MultiRangeTenantConfig::from_boundaries(tenant(), "0:0,50:10").expect("config");
        let (gateway, _) = MultiRangeTenant::start(config).expect("tenant");
        let engines = gateway.hosted_range_engines();
        let r1 = &engines[&RangeId::new(1)];
        r1.connect()
            .simple_query("CREATE TABLE marker52 (id int4) SHARDED")
            .await
            .expect("marker table");
        let table = r1
            .catalog_table(&crabka_pgcatalog::RelationName::public("marker52"))
            .expect("marker catalog");
        let start_ts = crabka_pgexec::TimestampTransactionId::new(700).expect("timestamp");
        let identity = crabka_pgexec::TimestampTxnIdentity {
            start_ts,
            global_xid: 701,
            primary_range: 1,
        };
        r1.timestamp_txn_participant(1)
            .prewrite_as_primary(
                identity,
                &[1],
                &[crabka_pgexec::TimestampWrite {
                    table_id: table.id,
                    bucket: None,
                    rowid: 1,
                    row: vec![Datum::Int4(1)],
                    delete: false,
                    global_index_intents: Vec::new(),
                }],
            )
            .await
            .expect("pending r1 prewrite");

        assert_eq!(
            gateway
                .control_in_doubt_markers(RangeKey::new(TableId::new(50), 10), None)
                .expect("r1 marker scan"),
            vec![crate::InDoubtMarker {
                transaction_id: 700,
                key: RangeKey::new(TableId::new(52), 1),
                hash_bucket: None,
            }]
        );
    }

    #[tokio::test]
    async fn hlc_mode_serves_monotonic_timestamps_through_the_allocation_path() {
        let config = MultiRangeTenantConfig::from_boundaries(tenant(), "0:0,50:10")
            .expect("config")
            .with_timestamp_source_mode(TimestampSourceMode::Hlc { max_offset_ms: 250 });
        let (gateway, _handles) = MultiRangeTenant::start(config).expect("tenant");
        let engines = gateway.hosted_range_engines();
        let source = engines[&RangeId::COORDINATOR].timestamp_oracle_handle();

        // Selecting Hlc installs a real, working single-source clock: its
        // uncertainty window is the configured offset in the packed domain.
        assert!(source.uncertainty_window() == crabka_pgexec::hlc::pack(250, 0));
        assert!(source.uncertainty_window() > 0);

        // Every stamp minted through the normal allocation path is monotone.
        let mut previous = 0;
        for _ in 0..8 {
            let read = source.allocate_read_timestamp().await.expect("read").get();
            assert!(read > previous);
            previous = read;

            let start = source
                .allocate_transaction_id()
                .await
                .expect("start timestamp");
            assert!(start.get() > previous);
            previous = start.get();

            let commit = source
                .allocate_commit_after(start)
                .await
                .expect("commit timestamp");
            assert!(commit.get() > start.get());
            assert!(commit.get() > previous);
            previous = commit.get();
        }
    }

    #[tokio::test]
    async fn hlc_wall_offset_skews_minted_timestamps_by_the_configured_amount() {
        // One hour dwarfs any wall-clock movement during the test, so the
        // configured skew is unambiguous in the minted physical component.
        const SKEW_MAGNITUDE_MS: i64 = 3_600_000;
        for skew_ms in [SKEW_MAGNITUDE_MS, -SKEW_MAGNITUDE_MS] {
            let config = MultiRangeTenantConfig::from_boundaries(tenant(), "0:0,50:10")
                .expect("config")
                .with_timestamp_source_mode(TimestampSourceMode::Hlc { max_offset_ms: 250 })
                .with_hlc_wall_offset_ms(skew_ms);
            let before_ms = crabka_pgexec::SystemWallClock.now_ms();
            let (gateway, _handles) = MultiRangeTenant::start(config).expect("tenant");
            let engines = gateway.hosted_range_engines();
            let source = engines[&RangeId::COORDINATOR].timestamp_oracle_handle();

            let minted = source.allocate_read_timestamp().await.expect("read").get();
            let after_ms = crabka_pgexec::SystemWallClock.now_ms();

            // The fresh clock mints straight off the (skewed) wall reading, so
            // the physical component sits exactly in the skewed wall interval
            // spanning the allocation.
            let physical_ms = crabka_pgexec::hlc::unpack(minted).physical_ms;
            assert!(
                physical_ms >= before_ms.saturating_add_signed(skew_ms),
                "skew {skew_ms}"
            );
            assert!(
                physical_ms <= after_ms.saturating_add_signed(skew_ms),
                "skew {skew_ms}"
            );
        }
    }

    #[test]
    fn logical_tso_mode_is_the_default() {
        let config =
            MultiRangeTenantConfig::from_boundaries(tenant(), "0:0,50:10").expect("config");
        assert!(config.timestamp_source_mode == TimestampSourceMode::LogicalTso);
        assert!(config.hlc_wall_offset_ms == 0);
    }

    #[test]
    fn control_markers_accepts_exact_r0_owner() {
        let config =
            MultiRangeTenantConfig::from_boundaries(tenant(), "0:0,50:10").expect("config");
        let (gateway, _) = MultiRangeTenant::start(config).expect("tenant");
        assert!(
            gateway
                .control_in_doubt_markers(
                    RangeKey::new(TableId::new(0), 0),
                    Some(RangeKey::new(TableId::new(50), 10)),
                )
                .expect("r0 marker scan")
                .is_empty()
        );
    }

    #[test]
    fn control_markers_rejects_mixed_owner_interval() {
        let config =
            MultiRangeTenantConfig::from_boundaries(tenant(), "0:0,50:10").expect("config");
        let (gateway, _) = MultiRangeTenant::start(config).expect("tenant");
        let error = gateway
            .control_in_doubt_markers(
                RangeKey::new(TableId::new(50), 9),
                Some(RangeKey::new(TableId::new(50), 11)),
            )
            .expect_err("mixed marker interval");
        assert!(error.to_string().contains("exactly one range"));
    }

    #[test]
    fn control_markers_rejects_unhosted_owner() {
        let config = MultiRangeTenantConfig::from_boundaries(tenant(), "0:0,50:10")
            .expect("config")
            .with_hosted_ranges(vec![RangeId::COORDINATOR])
            .expect("r0-only host");
        let (gateway, _) = MultiRangeTenant::start(config).expect("tenant");
        let error = gateway
            .control_in_doubt_markers(RangeKey::new(TableId::new(50), 10), None)
            .expect_err("unhosted marker owner");
        assert!(error.to_string().contains("r1 is not hosted"));
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
        let (gateway, handles) =
            MultiRangeTenant::start_with_engine_factory(config, |_dir, _id| Ok(SqlEngine::new()))
                .expect("rN-only assembly");

        let range1 = &handles.inner.serving.load().engines[&RangeId::new(1)];
        assert!(
            range1
                .catalog_table(&crabka_pgcatalog::RelationName::public("follower_table"))
                .is_ok()
        );
        assert!(
            range1
                .catalog_table(&crabka_pgcatalog::RelationName::public("unrelated_table"))
                .is_err()
        );

        let session = gateway.connect();
        assert!(
            session
                .route_statement("SELECT id FROM follower_table")
                .is_ok(),
            "rN-only routing must use the follower-backed catalog"
        );

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

    /// Counting delegate around [`LocalTimestampSource`]: read grants pass
    /// through untouched so visibility checks stay exact, while write leases and
    /// commit grants are tallied to prove when the global oracle is bypassed.
    ///
    /// [`LocalTimestampSource`]: crabka_pgexec::timestamp_txn::LocalTimestampSource
    #[derive(Default)]
    struct CountingTimestampSource {
        inner: crabka_pgexec::timestamp_txn::LocalTimestampSource,
        write_leases: AtomicUsize,
        commit_grants: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl TimestampSource for CountingTimestampSource {
        async fn allocate_read_timestamp(
            &self,
        ) -> Result<crabka_pgexec::timestamp_txn::ReadTimestamp, crabka_pgexec::TimestampSourceError>
        {
            self.inner.allocate_read_timestamp().await
        }

        async fn allocate_transaction_id(
            &self,
        ) -> Result<crabka_pgexec::TimestampTransactionId, crabka_pgexec::TimestampSourceError>
        {
            self.write_leases.fetch_add(1, Ordering::SeqCst);
            self.inner.allocate_transaction_id().await
        }

        async fn allocate_write_lease(
            &self,
            hidden_rowid_count: usize,
        ) -> Result<
            crabka_pgexec::timestamp_txn::TimestampWriteLease,
            crabka_pgexec::TimestampSourceError,
        > {
            self.write_leases.fetch_add(1, Ordering::SeqCst);
            self.inner.allocate_write_lease(hidden_rowid_count).await
        }

        async fn allocate_commit_after(
            &self,
            start_ts: crabka_pgexec::TimestampTransactionId,
        ) -> Result<crabka_pgexec::CommitTimestamp, crabka_pgexec::TimestampSourceError> {
            self.commit_grants.fetch_add(1, Ordering::SeqCst);
            self.inner.allocate_commit_after(start_ts).await
        }

        async fn allocate_read_timestamp_after(
            &self,
            durable_horizon: u64,
        ) -> Result<crabka_pgexec::timestamp_txn::ReadTimestamp, crabka_pgexec::TimestampSourceError>
        {
            self.inner
                .allocate_read_timestamp_after(durable_horizon)
                .await
        }

        async fn allocate_transaction_id_after(
            &self,
            durable_horizon: u64,
        ) -> Result<crabka_pgexec::TimestampTransactionId, crabka_pgexec::TimestampSourceError>
        {
            self.write_leases.fetch_add(1, Ordering::SeqCst);
            self.inner
                .allocate_transaction_id_after(durable_horizon)
                .await
        }

        async fn allocate_commit_after_durable(
            &self,
            start_ts: crabka_pgexec::TimestampTransactionId,
            durable_horizon: u64,
        ) -> Result<crabka_pgexec::CommitTimestamp, crabka_pgexec::TimestampSourceError> {
            self.commit_grants.fetch_add(1, Ordering::SeqCst);
            self.inner
                .allocate_commit_after_durable(start_ts, durable_horizon)
                .await
        }

        fn observe(&self, observed_ts: u64) {
            self.inner.observe(observed_ts);
        }

        fn uncertainty_window(&self) -> u64 {
            self.inner.uncertainty_window()
        }
    }

    /// An rN-only gateway hosting `[r1]` whose follower catalog carries the
    /// hash-sharded table `t7`, with a counting global timestamp source
    /// installed. Boundaries `0,7` put every `t7` hash bucket in r1, so
    /// statically routable inserts land entirely on the hosted data range.
    async fn rn_only_timestamp_gateway(
        tenant_name: &str,
    ) -> (MultiRangeTenant, Arc<CountingTimestampSource>) {
        let follower_kv: Arc<dyn Kv> = Arc::new(MemKv::default());
        let seed_catalog =
            SqlEngine::with_kv(Arc::clone(&follower_kv)).expect("seed catalog engine");
        seed_catalog
            .connect()
            .simple_query("CREATE TABLE t7 (id int4, note int4) SHARDED BY HASH (id) BUCKETS 16")
            .await
            .expect("seed sharded table");
        let replica = ReadOnlyRange0Replica::new(
            crate::range0_tail::Range0Tail::new(Arc::clone(&follower_kv)),
            Arc::new(EmptyRange0End),
        );
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse(tenant_name).expect("tenant"),
            "0,7",
        )
        .expect("config")
        .with_hosted_ranges(vec![RangeId::new(1)])
        .expect("r1 host")
        .with_read_only_range0_replica(replica);
        let oracle = Arc::new(CountingTimestampSource::default());
        let (gateway, _) = MultiRangeTenant::start_with_engine_factory_and_timestamp_oracle(
            config,
            |_dir, _range_id| Ok(SqlEngine::new()),
            Some(Arc::clone(&oracle) as Arc<dyn TimestampSource>),
        )
        .expect("rN-only assembly");
        (gateway, oracle)
    }

    #[tokio::test]
    async fn planner_seat_prefers_r0_and_falls_back_to_hosted_data_range() {
        // An r0-hosting node classifies sharded DML through range 0 itself.
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse("planner_seat_r0").expect("tenant"),
            "0,7",
        )
        .expect("config");
        let (gateway, _) = MultiRangeTenant::start(config).expect("tenant");
        let mut session = gateway.connect();
        session
            .simple_query("CREATE TABLE t7 (id int4, note int4) SHARDED BY HASH (id) BUCKETS 16")
            .await
            .expect("create sharded table");
        assert!(
            session
                .statement_targets_sharded_table("INSERT INTO t7 VALUES (1, 10)")
                .expect("r0-seat classification")
        );

        // An rN-only node classifies and routes the same DML through a hosted
        // data-range seat instead of demanding a local r0.
        let (gateway, _oracle) = rn_only_timestamp_gateway("planner_seat_rn_only").await;
        let session = gateway.connect();
        assert!(
            session
                .statement_targets_sharded_table("INSERT INTO t7 VALUES (1, 10)")
                .expect("fallback-seat classification")
        );
        assert!(
            session
                .route_statement("INSERT INTO t7 VALUES (1, 10)")
                .is_ok()
        );

        // With no hosted engine at all there is no seat left: fail closed.
        let replica = ReadOnlyRange0Replica::new(
            crate::range0_tail::Range0Tail::new(Arc::new(MemKv::default())),
            Arc::new(EmptyRange0End),
        );
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse("planner_seat_zero_engines").expect("tenant"),
            "0,7",
        )
        .expect("config")
        .with_hosted_ranges(Vec::new())
        .expect("no hosted ranges")
        .with_read_only_range0_replica(replica);
        let (gateway, _) = MultiRangeTenant::start_with_engine_factory(config, |_dir, range_id| {
            panic!("no engine must open for r{range_id}")
        })
        .expect("zero-engine assembly");
        let session = gateway.connect();
        let error = session
            .statement_targets_sharded_table("INSERT INTO t7 VALUES (1, 10)")
            .expect_err("zero-engine classification must fail closed");
        assert!(
            error
                .message
                .contains("tenant has no hosted engine with a range-0 catalog view"),
            "unexpected zero-engine error: {error:?}"
        );
    }

    #[tokio::test]
    async fn rn_only_gateway_commits_explicit_timestamp_transaction() {
        let (gateway, _oracle) = rn_only_timestamp_gateway("rn_only_commit").await;
        let mut session = gateway.connect();

        session.simple_query("BEGIN").await.expect("begin");
        session
            .simple_query("INSERT INTO t7 VALUES (1, 10)")
            .await
            .expect("first timestamp write");
        session
            .simple_query("INSERT INTO t7 VALUES (2, 20)")
            .await
            .expect("second timestamp write");
        session
            .simple_query("COMMIT")
            .await
            .expect("commit through the planner seat");

        let rows = session
            .simple_query("SELECT id, note FROM t7")
            .await
            .expect("read committed rows");
        let mut rows = text_rows(&rows);
        rows.sort();
        assert!(
            rows == vec![
                vec![Some("1".to_string()), Some("10".to_string())],
                vec![Some("2".to_string()), Some("20".to_string())],
            ]
        );
    }

    #[tokio::test]
    async fn rn_only_gateway_rolls_back_and_recovers_failed_timestamp_transactions() {
        let (gateway, _oracle) = rn_only_timestamp_gateway("rn_only_rollback").await;
        let mut session = gateway.connect();

        session.simple_query("BEGIN").await.expect("begin");
        session
            .simple_query("INSERT INTO t7 VALUES (1, 10)")
            .await
            .expect("timestamp write");
        session
            .simple_query("ROLLBACK")
            .await
            .expect("rollback through the planner seat");
        let rows = session
            .simple_query("SELECT id, note FROM t7")
            .await
            .expect("read after rollback");
        assert!(text_rows(&rows).is_empty());

        session.simple_query("BEGIN").await.expect("begin again");
        session
            .simple_query("INSERT INTO t7 VALUES (2, 20)")
            .await
            .expect("good timestamp write");
        let error = session
            .simple_query("INSERT INTO t7 VALUES (3, 'zap')")
            .await
            .expect_err("type error must fail the statement");
        assert!(
            !error.message.contains("r0 is not hosted"),
            "aborting a failed transaction must not demand a local r0: {error:?}"
        );
        session
            .simple_query("ROLLBACK")
            .await
            .expect("rollback the failed transaction");
        session
            .simple_query("INSERT INTO t7 VALUES (5, 50)")
            .await
            .expect("fresh autocommit write after recovery");
        let rows = session
            .simple_query("SELECT id, note FROM t7")
            .await
            .expect("read recovered state");
        assert!(text_rows(&rows) == vec![vec![Some("5".to_string()), Some("50".to_string())]]);
    }

    #[tokio::test]
    async fn rn_only_single_shard_bypass_skips_the_global_oracle() {
        let (gateway, oracle) = rn_only_timestamp_gateway("rn_only_bypass").await;
        let mut session = gateway.connect();

        session
            .simple_query("INSERT INTO t7 VALUES (4, 40)")
            .await
            .expect("autocommit single-range write");

        assert!(oracle.write_leases.load(Ordering::SeqCst) == 0);
        assert!(oracle.commit_grants.load(Ordering::SeqCst) == 0);
        let rows = session
            .simple_query("SELECT id, note FROM t7")
            .await
            .expect("read bypass-committed row");
        assert!(text_rows(&rows) == vec![vec![Some("4".to_string()), Some("40".to_string())]]);
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

    async fn pending_engine_for_startup_test(
        start_ts: u64,
    ) -> (SqlEngine, SqlEngine, crabka_pgexec::TimestampTransactionId) {
        let engine = SqlEngine::new();
        let observer = engine.clone_handle();
        let start_ts = crabka_pgexec::TimestampTransactionId::new(start_ts).expect("timestamp");
        engine
            .begin_timestamp_transaction(&crabka_pgexec::TimestampTxnDescriptor::begun(
                start_ts,
                start_ts.get() + 1,
                vec![RangeId::COORDINATOR.as_u32()],
            ))
            .await
            .expect("pending descriptor");
        (engine, observer, start_ts)
    }

    #[tokio::test]
    async fn ordinary_startup_recovers_pending_timestamp_transactions() {
        let (mut engine, observer, start_ts) = pending_engine_for_startup_test(810).await;
        MultiRangeTenant::start_with_engine_factory(
            MultiRangeTenantConfig::from_boundaries(tenant(), "0").expect("config"),
            move |_data_dir, _range_id| Ok(std::mem::replace(&mut engine, SqlEngine::new())),
        )
        .expect("ordinary startup");
        assert_eq!(
            observer
                .primary_timestamp_decision(start_ts)
                .expect("ordinary startup decision"),
            crabka_pgexec::PrimaryTxnDecision::Aborted
        );
    }

    #[tokio::test]
    async fn paused_source_startup_preserves_pending_timestamp_transaction() {
        let (mut engine, observer, start_ts) = pending_engine_for_startup_test(820).await;
        MultiRangeTenant::start_with_engine_factory(
            MultiRangeTenantConfig::from_boundaries(tenant(), "0")
                .expect("config")
                .defer_timestamp_recovery(),
            move |_data_dir, _range_id| Ok(std::mem::replace(&mut engine, SqlEngine::new())),
        )
        .expect("activation startup");
        assert_eq!(
            observer
                .primary_timestamp_decision(start_ts)
                .expect("deferred startup decision"),
            crabka_pgexec::PrimaryTxnDecision::Pending
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
            bucket: None,
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
        let recovery_map = MultiRangeTenantConfig::from_boundaries(tenant(), "0,10")
            .expect("recovery map")
            .range_map;

        recover_durable_timestamp_transactions(&engines, &recovery_map, None)
            .expect("first recovery");
        assert!(
            participant_kv
                .scan_prefix(b"\0\0\0\0index/ts_intent/")
                .expect("scan global-index intents after first recovery")
                .is_empty()
        );
        recover_durable_timestamp_transactions(&engines, &recovery_map, None)
            .expect("repeat recovery");

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

    #[test]
    fn recovery_aborts_secondary_prewritten_before_primary_participant_ack() {
        let primary_kv: Arc<dyn Kv> = Arc::new(crabka_pgkv::MemKv::new());
        let secondary_kv: Arc<dyn Kv> = Arc::new(crabka_pgkv::MemKv::new());
        let primary = SqlEngine::with_kv(Arc::clone(&primary_kv)).expect("primary");
        let secondary = SqlEngine::with_kv(Arc::clone(&secondary_kv)).expect("secondary");
        let start_ts = crabka_pgexec::TimestampTransactionId::new(20).expect("start timestamp");
        let identity = crabka_pgexec::TimestampTxnIdentity {
            start_ts,
            global_xid: 21,
            primary_range: 1,
        };
        let primary_write = crabka_pgexec::TimestampWrite {
            table_id: 10,
            bucket: None,
            rowid: 11,
            row: vec![Datum::Int4(1)],
            delete: false,
            global_index_intents: Vec::new(),
        };
        let secondary_write = crabka_pgexec::TimestampWrite {
            table_id: 10,
            bucket: None,
            rowid: 12,
            row: vec![Datum::Int4(2)],
            delete: false,
            global_index_intents: Vec::new(),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            primary
                .timestamp_txn_participant(1)
                .prewrite_as_primary(identity, &[1], std::slice::from_ref(&primary_write))
                .await
                .expect("primary prewrite");
            secondary
                .timestamp_txn_participant(2)
                .prewrite_as_secondary(identity, std::slice::from_ref(&secondary_write))
                .await
                .expect("secondary prewrite before primary participant add/ack");
        });
        drop(runtime);

        let mut engines = BTreeMap::new();
        engines.insert(RangeId::new(1), primary);
        engines.insert(RangeId::new(2), secondary);
        let recovery_map = MultiRangeTenantConfig::from_boundaries(tenant(), "0,10,10:12")
            .expect("recovery map")
            .range_map;
        recover_durable_timestamp_transactions(&engines, &recovery_map, None)
            .expect("recovery aborts prewrite-before-ack orphan");

        let tuple_key = crabka_pgmvcc::version::version_key_ts(
            secondary_write.table_id,
            secondary_write.rowid,
            start_ts.get(),
        );
        let tuple = secondary_kv
            .get(&tuple_key)
            .expect("read secondary tuple")
            .expect("secondary tuple retained as aborted version");
        let tuple = crabka_pgmvcc::version::decode_ts_tuple(&tuple).expect("decode tuple");
        assert_eq!(tuple.state, crabka_pgmvcc::version::TsVersionState::Aborted);
        assert!(
            secondary_kv
                .scan_prefix(b"\0\0\0\0meta/ts_prewrite/")
                .expect("scan reservations")
                .is_empty()
        );
        assert!(
            secondary_kv
                .scan_prefix(b"\0\0\0\0meta/ts_intent/")
                .expect("scan identity sidecars")
                .is_empty()
        );
    }

    #[test]
    fn rn_only_recovery_settles_orphan_through_remote_primary() {
        let primary = SqlEngine::new();
        let secondary_kv: Arc<dyn Kv> = Arc::new(crabka_pgkv::MemKv::new());
        let secondary = SqlEngine::with_kv(Arc::clone(&secondary_kv)).expect("secondary");
        let start_ts = crabka_pgexec::TimestampTransactionId::new(30).expect("start timestamp");
        let identity = crabka_pgexec::TimestampTxnIdentity {
            start_ts,
            global_xid: 31,
            primary_range: 1,
        };
        let primary_write = crabka_pgexec::TimestampWrite {
            table_id: 10,
            bucket: None,
            rowid: 1,
            row: vec![Datum::Int4(1)],
            delete: false,
            global_index_intents: Vec::new(),
        };
        let secondary_write = crabka_pgexec::TimestampWrite {
            table_id: 10,
            bucket: None,
            rowid: 2,
            row: vec![Datum::Int4(2)],
            delete: false,
            global_index_intents: Vec::new(),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            primary
                .timestamp_txn_participant(1)
                .prewrite_as_primary(identity, &[1], std::slice::from_ref(&primary_write))
                .await
                .expect("primary prewrite");
            secondary
                .timestamp_txn_participant(2)
                .prewrite_as_secondary(identity, std::slice::from_ref(&secondary_write))
                .await
                .expect("secondary orphan");
        });
        drop(runtime);

        let (address_tx, address_rx) = std::sync::mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("server runtime");
            runtime.block_on(async move {
                let service =
                    Arc::new(crate::forward::HostedRangeService::new(BTreeMap::from([(
                        RangeId::new(1),
                        primary,
                    )])));
                let address = crate::transport::spawn_loopback(service)
                    .await
                    .expect("spawn primary service");
                address_tx.send(address).expect("publish address");
                std::future::pending::<()>().await;
            });
        });
        let address = address_rx.recv().expect("primary address");
        let record = crabka_gres_control::TenantRecord::new(
            1,
            crabka_gres_control::TenantId::try_from("tenant-rn-recovery").expect("tenant id"),
            crabka_gres_control::TenantName::try_from("tenant-rn-recovery").expect("tenant name"),
            crabka_gres_control::TenantState::Active,
            crabka_gres_control::SqlUser::try_from("alice").expect("user"),
            "SCRAM-SHA-256$4096:salt$stored:server".into(),
            1,
        )
        .expect("record")
        .with_range_layout(vec![crabka_gres_control::RangeLayoutEntry {
            range_id: 1,
            end_key: None,
            endpoint: address.to_string(),
            wal_generation: 1,
            lifecycle: crabka_gres_control::RangeLifecycle::default(),
            retirement: None,
        }])
        .expect("layout");
        let registry = RangeRegistry::from_tenant_record(&record).expect("registry");
        let engines = BTreeMap::from([(RangeId::new(2), secondary)]);
        let recovery_map = MultiRangeTenantConfig::from_boundaries(tenant(), "0,10,10:2")
            .expect("recovery map")
            .range_map;

        recover_durable_timestamp_transactions(
            &engines,
            &recovery_map,
            Some((&registry, &FramedTcpClient::default())),
        )
        .expect("rN-only recovery settles through authenticated primary RPC");

        assert!(
            secondary_kv
                .scan_prefix(b"\0\0\0\0meta/ts_intent/")
                .expect("scan identity sidecars")
                .is_empty()
        );
    }

    /// Sampler that reports a fixed committed range-0 end.
    struct FixedRange0End {
        end: i64,
    }

    #[async_trait::async_trait]
    impl crate::barrier::Range0EndSampler for FixedRange0End {
        async fn sample_end_after_call_begins(&self) -> Result<i64, crate::barrier::BarrierError> {
            Ok(self.end)
        }
    }

    fn ddl_layout_entry(
        range_id: u32,
        end_key: Option<crabka_gres_control::RangeBoundary>,
        endpoint: String,
    ) -> crabka_gres_control::RangeLayoutEntry {
        crabka_gres_control::RangeLayoutEntry {
            range_id,
            end_key,
            endpoint,
            wal_generation: 1,
            lifecycle: crabka_gres_control::RangeLifecycle::default(),
            retirement: None,
        }
    }

    fn ddl_registry(
        record_tenant: &str,
        layout: Vec<crabka_gres_control::RangeLayoutEntry>,
    ) -> RangeRegistry {
        let record = crabka_gres_control::TenantRecord::new(
            1,
            crabka_gres_control::TenantId::try_from(record_tenant).expect("tenant id"),
            crabka_gres_control::TenantName::try_from(record_tenant).expect("record tenant"),
            crabka_gres_control::TenantState::Active,
            crabka_gres_control::SqlUser::try_from("alice").expect("user"),
            "SCRAM-SHA-256$4096:salt$stored:server".to_string(),
            1,
        )
        .expect("record")
        .with_range_layout(layout)
        .expect("layout");
        RangeRegistry::from_tenant_record(&record).expect("registry")
    }

    /// A gateway hosting `{r0, r1}` whose registry maps r1 to
    /// `follower_endpoint`. The r0 entry's address is never dialed: local DDL
    /// needs no forward, and the follower fan-out only compares it.
    fn range0_host_gateway_with_follower(
        tenant_name: &str,
        record_tenant: &str,
        follower_endpoint: &str,
    ) -> MultiRangeTenant {
        let registry = ddl_registry(
            record_tenant,
            vec![
                ddl_layout_entry(
                    0,
                    Some(crabka_gres_control::RangeBoundary::table_start(60)),
                    "127.0.0.1:1".to_string(),
                ),
                ddl_layout_entry(1, None, follower_endpoint.to_string()),
            ],
        );
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse(tenant_name).expect("tenant"),
            "0,60",
        )
        .expect("config")
        .with_range_registry(registry)
        .with_range_client(FramedTcpClient::default());
        MultiRangeTenant::start(config)
            .expect("range-0 hosting gateway")
            .0
    }

    #[tokio::test]
    async fn ddl_on_rn_only_gateway_forwards_to_range0_owner() {
        let catalog_kv: Arc<dyn Kv> = Arc::new(MemKv::default());
        let range0_engine = SqlEngine::with_kv(Arc::clone(&catalog_kv)).expect("range-0 engine");
        // The gateway mints read timestamps through the registry TSO path, so
        // the r0 owner must serve the timestamp oracle as well as DDL.
        let horizon = MemoryTsoHorizon::new(range0_engine.kv_handle(), 1);
        let persisted_max_ts = horizon.load_max_ts().expect("load TSO horizon");
        let tso = tso_rpc_from_horizon(horizon.clone(), horizon, 1, persisted_max_ts)
            .expect("durable TSO rpc");
        let service = Arc::new(
            crate::forward::HostedRangeService::new(BTreeMap::from([(
                RangeId::COORDINATOR,
                range0_engine.clone_handle(),
            )]))
            .with_ddl_gate(Arc::new(Mutex::new(())))
            .with_tso(tso),
        );
        let address = crate::transport::spawn_loopback(service)
            .await
            .expect("spawn range-0 service");

        // Both layout entries share the r0 endpoint, so the follower fan-out
        // finds no distinct non-r0 endpoint to barrier.
        let registry = ddl_registry(
            "ddl-rn-only-forward",
            vec![
                ddl_layout_entry(
                    0,
                    Some(crabka_gres_control::RangeBoundary::table_start(60)),
                    address.to_string(),
                ),
                ddl_layout_entry(1, None, address.to_string()),
            ],
        );
        // The replica shares the r0 engine's store `Arc`, so it is always
        // current and the barrier needs no committed frames beyond offset -1.
        let replica = ReadOnlyRange0Replica::new(
            crate::range0_tail::Range0Tail::new(Arc::clone(&catalog_kv)),
            Arc::new(EmptyRange0End),
        );
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse("ddl_rn_only_forward").expect("tenant"),
            "0,60",
        )
        .expect("config")
        .with_hosted_ranges(vec![RangeId::new(1)])
        .expect("r1 host")
        .with_read_only_range0_replica(replica)
        .with_range_registry(registry)
        .with_range_client(FramedTcpClient::default());
        let (gateway, _handles) =
            MultiRangeTenant::start_with_engine_factory(config, |_dir, _id| Ok(SqlEngine::new()))
                .expect("rN-only gateway");
        let mut session = gateway.connect();

        let results = session
            .simple_query("CREATE TABLE t61 (id int4)")
            .await
            .expect("DDL forwarded to the range-0 owner");
        assert!(
            results
                == vec![QueryResult::Command {
                    tag: "CREATE TABLE".to_string()
                }]
        );

        // Behavioral proof the new table routes: DML and reads on the same
        // gateway connection see the forwarded catalog change.
        let results = session
            .simple_query("INSERT INTO t61 VALUES (7)")
            .await
            .expect("insert into the forwarded table");
        assert!(
            results
                == vec![QueryResult::Command {
                    tag: "INSERT 0 1".to_string()
                }]
        );
        let rows = session
            .simple_query("SELECT id FROM t61")
            .await
            .expect("read the forwarded table");
        assert!(text_rows(&rows) == vec![vec![Some("7".to_string())]]);
    }

    #[tokio::test]
    async fn notify_on_an_rn_only_gateway_publishes_on_the_range0_owner() {
        let catalog_kv: Arc<dyn Kv> = Arc::new(MemKv::default());
        let range0_engine = SqlEngine::with_kv(Arc::clone(&catalog_kv)).expect("range-0 engine");
        let service = Arc::new(crate::forward::HostedRangeService::new(BTreeMap::from([(
            RangeId::COORDINATOR,
            range0_engine.clone_handle(),
        )])));
        let address = crate::transport::spawn_loopback(service)
            .await
            .expect("spawn range-0 service");
        let registry = ddl_registry(
            "notify-rn-only-forward",
            vec![
                ddl_layout_entry(
                    0,
                    Some(crabka_gres_control::RangeBoundary::table_start(60)),
                    address.to_string(),
                ),
                ddl_layout_entry(1, None, address.to_string()),
            ],
        );
        let replica = ReadOnlyRange0Replica::new(
            crate::range0_tail::Range0Tail::new(Arc::clone(&catalog_kv)),
            Arc::new(EmptyRange0End),
        );
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse("notify_rn_only_forward").expect("tenant"),
            "0,60",
        )
        .expect("config")
        .with_hosted_ranges(vec![RangeId::new(1)])
        .expect("r1 host")
        .with_node_identity("rn-node")
        .with_read_only_range0_replica(replica)
        .with_range_registry(registry)
        .with_range_client(FramedTcpClient::default());
        let (gateway, _handles) =
            MultiRangeTenant::start_with_engine_factory(config, |_dir, _id| Ok(SqlEngine::new()))
                .expect("rN-only gateway");

        // A listener on the range-0 owner itself: reaching its bus is what puts
        // the notification on the log every other node reads.
        let mut owner_listener = range0_engine.connect_with_pid(4242);
        let mut owner_queue = owner_listener.take_notifications().expect("owner queue");
        owner_listener
            .simple_query("LISTEN forwarded_chan")
            .await
            .expect("listen on the owner");

        let results = gateway
            .connect_with_pid(909)
            .simple_query("NOTIFY forwarded_chan, 'from the rN gateway'")
            .await
            .expect("NOTIFY forwarded to the range-0 owner");
        assert!(
            results
                == vec![QueryResult::Command {
                    tag: "NOTIFY".to_string()
                }]
        );

        let delivered = owner_queue
            .try_recv()
            .expect("published on the owner's bus");
        assert!(
            delivered
                == Notification {
                    // The originating pid survives the hop: `SessionOpen`
                    // carries it, and the owner-side session adopts it, so a
                    // forwarded NOTIFY is stamped with the pid PostgreSQL
                    // would report rather than the owner session's own.
                    process_id: 909,
                    channel: "forwarded_chan".to_owned(),
                    payload: "from the rN gateway".to_owned(),
                }
        );
    }

    #[tokio::test]
    async fn ddl_from_range0_host_blocks_until_followers_apply() {
        let follower_tail = crate::range0_tail::Range0Tail::new(Arc::new(MemKv::default()));
        let follower_barrier = Arc::new(Range0Barrier::new(
            follower_tail.clone(),
            Arc::new(FixedRange0End { end: 3 }),
        ));
        let follower = Arc::new(
            crate::forward::HostedRangeService::new(BTreeMap::new())
                .with_catalog_follower(follower_barrier),
        );
        let follower_address = crate::transport::spawn_loopback(follower)
            .await
            .expect("spawn follower service");
        let gateway = range0_host_gateway_with_follower(
            "ddl_barrier_wait",
            "ddl-barrier-wait",
            &follower_address.to_string(),
        );

        let ddl = tokio::spawn({
            let gateway = gateway.clone();
            async move {
                gateway
                    .connect()
                    .simple_query("CREATE TABLE t61 (id int4)")
                    .await
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            !ddl.is_finished(),
            "DDL must wait for the lagging follower replica"
        );

        follower_tail
            .apply_committed(&crate::range0_tail::Range0Frame::new(3, Vec::new()))
            .expect("apply the outstanding catalog frames");
        let results = ddl
            .await
            .expect("join DDL")
            .expect("DDL returns once every follower covers the change");
        assert!(
            results
                == vec![QueryResult::Command {
                    tag: "CREATE TABLE".to_string()
                }]
        );
    }

    #[tokio::test]
    async fn ddl_reports_partial_visibility_when_a_follower_barrier_fails() {
        // A follower node with neither r0 nor a follower replica answers the
        // catalog barrier with a Failed error.
        let follower = Arc::new(crate::forward::HostedRangeService::new(BTreeMap::new()));
        let follower_address = crate::transport::spawn_loopback(follower)
            .await
            .expect("spawn broken follower service");
        let gateway = range0_host_gateway_with_follower(
            "ddl_barrier_failure",
            "ddl-barrier-failure",
            &follower_address.to_string(),
        );

        let mut session = gateway.connect();
        let error = session
            .simple_query("CREATE TABLE t61 (id int4)")
            .await
            .expect_err("failed follower barrier must surface");
        assert!(error.code == "58000");
        assert!(
            error.message.contains("committed"),
            "message must state the range-0 commit: {error:?}"
        );

        // Honest partial application: the DDL genuinely committed on range 0.
        let rows = gateway.hosted_range_engines()[&RangeId::COORDINATOR]
            .connect()
            .simple_query("SELECT id FROM t61")
            .await
            .expect("table exists on range 0 despite the failed barrier");
        assert!(text_rows(&rows).is_empty());
    }

    struct StalledRange0End;

    #[async_trait::async_trait]
    impl crate::barrier::Range0EndSampler for StalledRange0End {
        async fn sample_end_after_call_begins(&self) -> Result<i64, crate::barrier::BarrierError> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn ddl_bounds_a_stalled_local_replica_barrier() {
        let catalog_kv: Arc<dyn Kv> = Arc::new(MemKv::default());
        let range0_engine = SqlEngine::with_kv(Arc::clone(&catalog_kv)).expect("range-0 engine");
        let horizon = MemoryTsoHorizon::new(range0_engine.kv_handle(), 1);
        let persisted_max_ts = horizon.load_max_ts().expect("load TSO horizon");
        let tso = tso_rpc_from_horizon(horizon.clone(), horizon, 1, persisted_max_ts)
            .expect("durable TSO rpc");
        let service = Arc::new(
            crate::forward::HostedRangeService::new(BTreeMap::from([(
                RangeId::COORDINATOR,
                range0_engine.clone_handle(),
            )]))
            .with_ddl_gate(Arc::new(Mutex::new(())))
            .with_tso(tso),
        );
        let address = crate::transport::spawn_loopback(service)
            .await
            .expect("spawn range-0 service");
        let registry = ddl_registry(
            "ddl-stalled-local-barrier",
            vec![
                ddl_layout_entry(
                    0,
                    Some(crabka_gres_control::RangeBoundary::table_start(60)),
                    address.to_string(),
                ),
                ddl_layout_entry(1, None, address.to_string()),
            ],
        );
        // The replica's broker end sample never resolves, standing in for an
        // admin connection hang after the DDL has already committed on r0.
        let replica = ReadOnlyRange0Replica::new(
            crate::range0_tail::Range0Tail::new(Arc::clone(&catalog_kv)),
            Arc::new(StalledRange0End),
        );
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse("ddl_stalled_local_barrier").expect("tenant"),
            "0,60",
        )
        .expect("config")
        .with_hosted_ranges(vec![RangeId::new(1)])
        .expect("r1 host")
        .with_read_only_range0_replica(replica)
        .with_range_registry(registry)
        .with_range_client(FramedTcpClient::default());
        let (gateway, _handles) =
            MultiRangeTenant::start_with_engine_factory(config, |_dir, _id| Ok(SqlEngine::new()))
                .expect("rN-only gateway");
        let mut session = gateway.connect();

        let started = std::time::Instant::now();
        let error = session
            .simple_query("CREATE TABLE t61 (id int4)")
            .await
            .expect_err("a stalled local barrier must surface, not hang");
        assert!(error.code == "58000");
        assert!(
            error.message.contains("local catalog visibility"),
            "message must name the unconfirmed local barrier: {error:?}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(8),
            "the local barrier wait must share the follower reply budget"
        );

        // Honest partial application: the DDL genuinely committed on range 0.
        let rows = range0_engine
            .connect()
            .simple_query("SELECT id FROM t61")
            .await
            .expect("table exists on range 0 despite the stalled barrier");
        assert!(text_rows(&rows).is_empty());
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
            .catalog_table(&crabka_pgcatalog::RelationName::public("t"))
            .expect("table");
        let range0_versions = committed_timestamp_versions(
            serving.engines.get(&RangeId::COORDINATOR).expect("range 0"),
            table.id,
        );
        let range1_versions = committed_timestamp_versions(
            serving.engines.get(&RangeId::new(1)).expect("range 1"),
            table.id,
        );

        let mut all_versions = range0_versions
            .into_iter()
            .chain(range1_versions)
            .collect::<Vec<_>>();
        all_versions.sort_unstable_by_key(|version| version.commit_ts);

        assert!(
            all_versions
                == vec![
                    TimestampVersion::new(1, 3),
                    TimestampVersion::new(4, 6),
                    TimestampVersion::new(7, 9)
                ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn row_sharded_multi_row_insert_preserves_hidden_row_identity() {
        let config = MultiRangeTenantConfig::from_boundaries(tenant(), "0,0:2").expect("range map");
        let (gateway, _) = MultiRangeTenant::start(config).expect("tenant starts");
        let mut session = gateway.connect();
        session
            .simple_query("CREATE TABLE agg_sales (region text, amount int4) SHARDED")
            .await
            .expect("create sharded table");

        for statement in [
            "INSERT INTO agg_sales VALUES \
             ('west', 10), ('west', 20), ('east', 5), ('east', 5), ('north', 100)",
            "INSERT INTO agg_sales VALUES ('west', 10), ('south', 7)",
        ] {
            session
                .simple_query(statement)
                .await
                .expect("insert with repeated logical values");
        }

        let results = session
            .simple_query("SELECT region, amount FROM agg_sales ORDER BY region, amount")
            .await
            .expect("scatter scan");
        assert_eq!(
            text_rows(&results),
            vec![
                vec![Some("east".into()), Some("5".into())],
                vec![Some("east".into()), Some("5".into())],
                vec![Some("north".into()), Some("100".into())],
                vec![Some("south".into()), Some("7".into())],
                vec![Some("west".into()), Some("10".into())],
                vec![Some("west".into()), Some("10".into())],
                vec![Some("west".into()), Some("20".into())],
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn row_sharded_insert_coerces_untyped_datetime_literals() {
        let config = MultiRangeTenantConfig::from_boundaries(tenant(), "0,0:2").expect("range map");
        let (gateway, _) = MultiRangeTenant::start(config).expect("tenant starts");
        let mut session = gateway.connect();
        session
            .simple_query(
                "CREATE TABLE dt_demo (\
                     id int4, d date, tm time, ts timestamp, tz timestamptz, iv interval\
                 ) SHARDED",
            )
            .await
            .expect("create datetime table");
        session
            .simple_query(
                "INSERT INTO dt_demo VALUES (\
                     2, '2024-06-01', '08:00:00', '2024-06-01 08:00:00', \
                     '2024-06-01 12:00:00+00', '2 hours'\
                 )",
            )
            .await
            .expect("insert untyped datetime literals");

        let results = session
            .simple_query("SELECT id, d, tm, ts, tz, iv FROM dt_demo")
            .await
            .expect("scatter scan");
        assert_eq!(
            text_rows(&results),
            [vec![
                Some("2".into()),
                Some("2024-06-01".into()),
                Some("08:00:00".into()),
                Some("2024-06-01 08:00:00".into()),
                Some("2024-06-01 12:00:00+00".into()),
                Some("02:00:00".into()),
            ]]
        );
    }

    #[tokio::test]
    async fn local_secondary_rejects_commit_opposite_to_primary_abort() {
        let config = MultiRangeTenantConfig::from_boundaries(tenant(), "0,0:50").expect("cfg");
        let (gateway, _) = MultiRangeTenant::start(config).expect("tenant");
        let engines = gateway.hosted_range_engines();
        let primary = &engines[&RangeId::COORDINATOR];
        let secondary = &engines[&RangeId::new(1)];
        let start_ts = crabka_pgexec::TimestampTransactionId::new(500).expect("start timestamp");
        let identity = crabka_pgexec::TimestampTxnIdentity {
            start_ts,
            global_xid: 501,
            primary_range: 0,
        };
        let write = crabka_pgexec::TimestampWrite {
            table_id: 10,
            bucket: None,
            rowid: 60,
            row: vec![Datum::Int4(1)],
            delete: false,
            global_index_intents: Vec::new(),
        };
        primary
            .begin_timestamp_transaction(&crabka_pgexec::TimestampTxnDescriptor::begun(
                start_ts,
                identity.global_xid,
                vec![1],
            ))
            .await
            .expect("descriptor");
        secondary
            .timestamp_txn_participant(1)
            .prewrite_as_secondary(identity, std::slice::from_ref(&write))
            .await
            .expect("prewrite");
        primary
            .decide_timestamp_transaction(start_ts, crabka_pgexec::PrimaryTxnDecision::Aborted)
            .await
            .expect("abort primary");
        let session = gateway.connect();
        let commit_ts =
            crabka_pgexec::CommitTimestamp::after_start(start_ts, 502).expect("commit timestamp");
        assert!(
            session
                .timestamp_resolve(
                    RangeId::new(1),
                    identity,
                    crabka_pgexec::TimestampTxnDecision::Committed(commit_ts),
                    std::slice::from_ref(&write),
                )
                .await
                .is_err()
        );
        assert_eq!(
            local_timestamp_tuple_state(secondary, &write, start_ts),
            crabka_pgmvcc::version::TsVersionState::Intent
        );
    }

    #[tokio::test]
    async fn local_secondary_rejects_abort_while_primary_is_pending() {
        let config = MultiRangeTenantConfig::from_boundaries(tenant(), "0,0:50").expect("cfg");
        let (gateway, _) = MultiRangeTenant::start(config).expect("tenant");
        let engines = gateway.hosted_range_engines();
        let primary = &engines[&RangeId::COORDINATOR];
        let secondary = &engines[&RangeId::new(1)];
        let start_ts = crabka_pgexec::TimestampTransactionId::new(510).expect("start timestamp");
        let identity = crabka_pgexec::TimestampTxnIdentity {
            start_ts,
            global_xid: 511,
            primary_range: 0,
        };
        let write = crabka_pgexec::TimestampWrite {
            table_id: 10,
            bucket: None,
            rowid: 61,
            row: vec![Datum::Int4(1)],
            delete: false,
            global_index_intents: Vec::new(),
        };
        primary
            .begin_timestamp_transaction(&crabka_pgexec::TimestampTxnDescriptor::begun(
                start_ts,
                identity.global_xid,
                vec![1],
            ))
            .await
            .expect("pending descriptor");
        secondary
            .timestamp_txn_participant(1)
            .prewrite_as_secondary(identity, std::slice::from_ref(&write))
            .await
            .expect("prewrite");
        let session = gateway.connect();
        assert!(
            session
                .timestamp_resolve(
                    RangeId::new(1),
                    identity,
                    crabka_pgexec::TimestampTxnDecision::Aborted,
                    std::slice::from_ref(&write),
                )
                .await
                .is_err()
        );
        assert_eq!(
            primary
                .primary_timestamp_decision(start_ts)
                .expect("decision"),
            crabka_pgexec::PrimaryTxnDecision::Pending
        );
        assert_eq!(
            local_timestamp_tuple_state(secondary, &write, start_ts),
            crabka_pgmvcc::version::TsVersionState::Intent
        );
    }

    #[tokio::test]
    async fn local_secondary_resolves_valid_operations_in_descending_row_order() {
        let config = MultiRangeTenantConfig::from_boundaries(tenant(), "0,0:50").expect("cfg");
        let (gateway, _) = MultiRangeTenant::start(config).expect("tenant");
        let engines = gateway.hosted_range_engines();
        let primary = &engines[&RangeId::COORDINATOR];
        let secondary = &engines[&RangeId::new(1)];
        let start_ts = crabka_pgexec::TimestampTransactionId::new(610).expect("start timestamp");
        let identity = crabka_pgexec::TimestampTxnIdentity {
            start_ts,
            global_xid: 611,
            primary_range: 0,
        };
        let low = crabka_pgexec::TimestampWrite {
            table_id: 10,
            bucket: None,
            rowid: 60,
            row: vec![Datum::Int4(1)],
            delete: false,
            global_index_intents: Vec::new(),
        };
        let high = crabka_pgexec::TimestampWrite {
            rowid: 61,
            row: vec![Datum::Int4(2)],
            ..low.clone()
        };
        let operations = [&low, &high]
            .into_iter()
            .map(|write| crabka_pgexec::TimestampTxnOperation {
                range_id: 1,
                table_id: write.table_id,
                bucket: write.bucket,
                rowid: write.rowid,
                delete: write.delete,
            })
            .collect::<Vec<_>>();
        primary
            .begin_timestamp_transaction(&crabka_pgexec::TimestampTxnDescriptor::begun(
                start_ts,
                identity.global_xid,
                vec![1],
            ))
            .await
            .expect("descriptor");
        secondary
            .timestamp_txn_participant(1)
            .prewrite_as_secondary(identity, &[low.clone(), high.clone()])
            .await
            .expect("prewrite");
        primary
            .acknowledge_timestamp_participant_operations(start_ts, 1, &operations)
            .await
            .expect("ack operations");
        let commit_ts =
            crabka_pgexec::CommitTimestamp::after_start(start_ts, 612).expect("commit timestamp");
        primary
            .decide_timestamp_transaction(
                start_ts,
                crabka_pgexec::PrimaryTxnDecision::Committed(commit_ts),
            )
            .await
            .expect("commit primary");
        let session = gateway.connect();
        session
            .timestamp_resolve(
                RangeId::new(1),
                identity,
                crabka_pgexec::TimestampTxnDecision::Committed(commit_ts),
                &[high.clone(), low.clone()],
            )
            .await
            .expect("descending operations are canonicalized");
        assert!(matches!(
            local_timestamp_tuple_state(secondary, &low, start_ts),
            crabka_pgmvcc::version::TsVersionState::Committed { .. }
        ));
        assert!(matches!(
            local_timestamp_tuple_state(secondary, &high, start_ts),
            crabka_pgmvcc::version::TsVersionState::Committed { .. }
        ));
    }

    fn local_timestamp_tuple_state(
        engine: &SqlEngine,
        write: &crabka_pgexec::TimestampWrite,
        start_ts: crabka_pgexec::TimestampTransactionId,
    ) -> crabka_pgmvcc::version::TsVersionState {
        let key =
            crabka_pgmvcc::version::version_key_ts(write.table_id, write.rowid, start_ts.get());
        let bytes = engine
            .kv_handle()
            .get(&key)
            .expect("read tuple")
            .expect("tuple");
        crabka_pgmvcc::version::decode_ts_tuple(&bytes)
            .expect("decode tuple")
            .state
    }

    #[tokio::test]
    async fn tso_oracle_allocations_skip_above_observed_local_stamps() {
        use crabka_pgexec::TimestampSource as _;

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
        let rpc = Arc::new(InProcessTsoRpc { oracle });
        let timestamp_oracle = PgexecTsoOracle {
            client: BatchedTsoClient::new(rpc),
            observed_floor: std::sync::atomic::AtomicU64::new(0),
        };

        // A range's single-shard bypass has locally spent stamps up to 100;
        // once observed, every global allocation must land strictly above.
        timestamp_oracle.observe(100);
        let lease = timestamp_oracle
            .allocate_write_lease(2)
            .await
            .expect("floored write lease");
        assert!(lease.start_ts.get() > 100);
        assert!(lease.hidden_rowids == vec![lease.start_ts.get() + 1, lease.start_ts.get() + 2]);
        let commit = timestamp_oracle
            .allocate_commit_after(lease.start_ts)
            .await
            .expect("floored commit");
        assert!(commit.get() > lease.start_ts.get());

        // A stale (lower) observation never drags allocations backwards.
        timestamp_oracle.observe(5);
        let next = timestamp_oracle
            .allocate_transaction_id()
            .await
            .expect("monotone transaction id");
        assert!(next.get() > commit.get());
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
            observed_floor: std::sync::atomic::AtomicU64::new(0),
        };

        let error = crabka_pgexec::TimestampSource::allocate_transaction_id(&timestamp_oracle)
            .await
            .expect_err("fenced oracle rejects grants");

        assert!(error.to_string().contains("fenced"));
    }

    #[tokio::test]
    async fn hlc_grant_rpc_is_wall_anchored_and_restart_seeds_from_the_horizon() {
        use crabka_pgexec::hlc::unpack;

        let count = NonZeroU64::new(4).expect("count");
        let kv = Arc::new(crabka_pgkv::MemKv::new());
        let horizon = MemoryTsoHorizon::new(kv, 1);
        // Live-shaped assembly: the same builder the live boot path uses, with
        // a pinned wall clock so anchoring is deterministic.
        let predecessor_wall = Arc::new(crabka_pgexec::ManualWallClock::new(5_000));
        let rpc = hlc_tso_rpc_from_horizon(
            horizon.clone(),
            horizon.clone(),
            1,
            horizon.load_max_ts().expect("fresh horizon"),
            predecessor_wall as Arc<dyn crabka_pgexec::WallClock>,
        )
        .expect("hlc tso rpc");
        let before_restart = rpc.grant(count).await.expect("grant");
        assert!(unpack(before_restart.first_ts.get()).physical_ms == 5_000);

        // Simulated restart: rebuild from the persisted horizon with a wall
        // clock far BEHIND the predecessor's, proving horizon seeding — not
        // wall luck — provides monotonicity across the restart.
        drop(rpc);
        let persisted = horizon.load_max_ts().expect("persisted horizon");
        let successor_wall = Arc::new(crabka_pgexec::ManualWallClock::new(3));
        let restarted = hlc_tso_rpc_from_horizon(
            horizon.clone(),
            horizon.clone(),
            1,
            persisted,
            successor_wall as Arc<dyn crabka_pgexec::WallClock>,
        )
        .expect("restarted hlc tso rpc");
        let after_restart = restarted
            .grant(NonZeroU64::new(1).expect("count"))
            .await
            .expect("post-restart grant");

        assert!(persisted >= before_restart.last_ts().expect("last").get());
        assert!(after_restart.first_ts.get() > persisted);
        assert!(after_restart.first_ts > before_restart.last_ts().expect("last"));
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

    #[tokio::test]
    async fn notify_family_statements_route_local_to_the_coordinator() {
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse("notify-routing").expect("tenant"),
            "0,100",
        )
        .expect("config");
        let (gateway, _handles) = MultiRangeTenant::start(config).expect("start");
        let session = gateway.connect_with_pid(77);

        for statement in [
            "LISTEN gateway_chan",
            "NOTIFY gateway_chan, 'payload'",
            "UNLISTEN gateway_chan",
            "UNLISTEN *",
        ] {
            let route = session.route_statement(statement).expect("route");
            assert!(
                route
                    == StatementRoute {
                        kind: StatementKind::Local,
                        range_id: RangeId::COORDINATOR,
                        table_id: None,
                        scatter_ranges: None,
                    },
                "unexpected route for {statement}: {route:?}"
            );
        }
    }

    #[tokio::test]
    async fn gateway_sessions_exchange_notifications_through_the_coordinator_bus() {
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse("gateway-listen-notify").expect("tenant"),
            "0,100",
        )
        .expect("config");
        let (gateway, _handles) = MultiRangeTenant::start(config).expect("start");

        let mut listener = gateway.connect_with_pid(4242);
        let mut notifier = gateway.connect_with_pid(4343);
        let mut notifications = listener
            .take_notifications()
            .expect("a pid-bearing gateway session joins the coordinator bus");
        assert!(listener.take_notifications().is_none());

        listener
            .simple_query("LISTEN gateway_chan")
            .await
            .expect("listen");
        notifier
            .simple_query("NOTIFY gateway_chan, 'from the other session'")
            .await
            .expect("notify");

        let delivered = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            std::pin::pin!(notifications.recv()),
        )
        .await
        .expect("notification delivered before the deadline")
        .expect("notification queue stays open");
        assert!(
            delivered
                == Notification {
                    process_id: 4343,
                    channel: "gateway_chan".to_owned(),
                    payload: "from the other session".to_owned(),
                }
        );
    }

    #[tokio::test]
    async fn gateway_session_without_a_backend_pid_has_no_notification_queue() {
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse("gateway-listen-no-pid").expect("tenant"),
            "0,100",
        )
        .expect("config");
        let (gateway, _handles) = MultiRangeTenant::start(config).expect("start");
        let mut session = gateway.connect();

        assert!(session.take_notifications().is_none());
        let error = session
            .simple_query("LISTEN gateway_chan")
            .await
            .expect_err("an unregistered session cannot listen");
        assert!(error.code == sqlstate::FEATURE_NOT_SUPPORTED);
        assert!(
            error.message.contains("without a backend process id"),
            "unexpected refusal: {error:?}"
        );
    }

    /// An rN-only gateway plus the follower tail its node reads, so a test can
    /// push committed range-0 frames past the installed observer exactly as the
    /// live follower poll loop does.
    fn rn_only_notify_gateway(tenant_name: &str) -> (MultiRangeTenant, crate::Range0Tail) {
        let follower_kv: Arc<dyn Kv> = Arc::new(MemKv::default());
        let tail = crate::range0_tail::Range0Tail::new(follower_kv);
        let replica = ReadOnlyRange0Replica::new(tail.clone(), Arc::new(EmptyRange0End));
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse(tenant_name).expect("tenant"),
            "0,7",
        )
        .expect("config")
        .with_hosted_ranges(vec![RangeId::new(1)])
        .expect("r1 host")
        .with_node_identity(format!("{tenant_name}-node"))
        .with_read_only_range0_replica(replica);
        let (gateway, _) = MultiRangeTenant::start_with_engine_factory_and_timestamp_oracle(
            config,
            |_dir, _range_id| Ok(SqlEngine::new()),
            None,
        )
        .expect("rN-only assembly");
        (gateway, tail)
    }

    /// One committed frame carrying `records` as notify-keyed puts.
    fn notify_frame(offset: i64, records: &[(&str, i32, &str, &str)]) -> crate::Range0Frame {
        let ops = records
            .iter()
            .enumerate()
            .map(
                |(index, (origin, process_id, channel, payload))| crabka_pgkv::WriteOp::Put {
                    key: crabka_pgkv::key::notify_key(
                        u64::try_from(index).expect("index fits u64"),
                    ),
                    value: crabka_pgkv::NotifyRecord {
                        origin: (*origin).to_owned(),
                        process_id: *process_id,
                        channel: (*channel).to_owned(),
                        payload: (*payload).to_owned(),
                    }
                    .encode(),
                },
            )
            .collect();
        crate::Range0Frame::new(offset, ops)
    }

    #[tokio::test]
    async fn rn_only_gateway_session_subscribes_on_its_own_node() {
        let (gateway, _tail) = rn_only_notify_gateway("rn_only_listen_notify");
        let mut session = gateway.connect_with_pid(909);

        assert!(
            session.take_notifications().is_some(),
            "a pid-bearing session joins the node-local bus even without range 0"
        );
        for statement in ["LISTEN chan", "UNLISTEN chan", "UNLISTEN *"] {
            session
                .simple_query(statement)
                .await
                .unwrap_or_else(|error| panic!("{statement} on an rN gateway: {error:?}"));
        }

        // The subscription is registered on this node's bus, not forwarded to
        // range 0's: the seat is the hosted data range.
        session.simple_query("LISTEN chan").await.expect("listen");
        let route = session.route_statement("LISTEN chan").expect("route");
        assert!(route.range_id == RangeId::new(1));
        assert!(
            session
                .route_statement("NOTIFY chan, 'x'")
                .expect("route")
                .range_id
                == RangeId::COORDINATOR
        );
    }

    #[tokio::test]
    async fn range0_frames_deliver_remote_notifications_to_a_local_listener() {
        let (gateway, tail) = rn_only_notify_gateway("rn_only_remote_notify");
        let mut listener = gateway.connect_with_pid(11);
        let mut notifications = listener.take_notifications().expect("queue");
        listener
            .simple_query("LISTEN remote_chan")
            .await
            .expect("listen");

        tail.apply_committed(&notify_frame(
            0,
            &[
                ("other-node", 4343, "remote_chan", "from another gateway"),
                ("other-node", 4343, "unheard_chan", "nobody listens here"),
            ],
        ))
        .expect("frame applies");

        let delivered = notifications.try_recv().expect("notification delivered");
        assert!(
            delivered
                == Notification {
                    // The ORIGINATING backend pid, as PostgreSQL reports it.
                    process_id: 4343,
                    channel: "remote_chan".to_owned(),
                    payload: "from another gateway".to_owned(),
                }
        );
        assert!(notifications.try_recv().is_err());
    }

    #[tokio::test]
    async fn range0_frames_skip_this_nodes_own_records_and_undecodable_ones() {
        let (gateway, tail) = rn_only_notify_gateway("rn_only_notify_skips");
        let mut listener = gateway.connect_with_pid(12);
        let mut notifications = listener.take_notifications().expect("queue");
        listener
            .simple_query("LISTEN remote_chan")
            .await
            .expect("listen");

        let own_origin = gateway
            .node_notify_origin()
            .expect("a replicating node has an identity")
            .to_owned();
        tail.apply_committed(&notify_frame(
            0,
            &[(own_origin.as_str(), 7, "remote_chan", "published here")],
        ))
        .expect("frame applies");
        assert!(
            notifications.try_recv().is_err(),
            "a node must not re-deliver its own notification"
        );

        // A corrupt record costs one warning, not the frame around it: the
        // well-formed record that follows still arrives.
        tail.apply_committed(&crate::Range0Frame::new(
            1,
            vec![
                crabka_pgkv::WriteOp::Put {
                    key: crabka_pgkv::key::notify_key(9),
                    value: vec![0xff, 0x00, 0x01],
                },
                crabka_pgkv::WriteOp::Put {
                    key: crabka_pgkv::key::notify_key(10),
                    value: crabka_pgkv::NotifyRecord {
                        origin: "other-node".to_owned(),
                        process_id: 99,
                        channel: "remote_chan".to_owned(),
                        payload: "after the corrupt one".to_owned(),
                    }
                    .encode(),
                },
            ],
        ))
        .expect("frame with a corrupt record still applies");

        assert!(
            notifications.try_recv().expect("notification delivered")
                == Notification {
                    process_id: 99,
                    channel: "remote_chan".to_owned(),
                    payload: "after the corrupt one".to_owned(),
                }
        );
    }

    #[tokio::test]
    async fn pg_notify_calls_are_recognised_as_notification_statements() {
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse("gateway-pg-notify").expect("tenant"),
            "0,100",
        )
        .expect("config");
        let (gateway, _handles) = MultiRangeTenant::start(config).expect("start");
        let mut session = gateway.connect();

        // A session with no wire loop cannot be delivered to, so the function
        // form must be refused exactly as the statement form is.
        let error = session
            .simple_query("SELECT pg_notify('chan', 'payload')")
            .await
            .expect_err("an unregistered session cannot publish");
        assert!(error.code == sqlstate::FEATURE_NOT_SUPPORTED);
        assert!(
            error.message.contains("without a backend process id"),
            "unexpected refusal: {error:?}"
        );

        // An identifier that merely ends in the function's name is not a call.
        session
            .simple_query("SELECT 1 AS my_pg_notify_count")
            .await
            .expect("an unrelated statement stays unaffected");
    }

    #[test]
    fn timestamp_scatter_admits_plain_inserts_and_refuses_upserts() {
        ensure_timestamp_scatter_is_supported("INSERT INTO t7 VALUES (1, 10)")
            .expect("a plain insert scatters");

        for statement in [
            "INSERT INTO t7 VALUES (1, 10) ON CONFLICT DO NOTHING",
            "INSERT INTO t7 VALUES (1, 10) ON CONFLICT (id) DO UPDATE SET note = 11",
            "UPDATE t7 SET note = 11 WHERE id = 1",
            "DELETE FROM t7 WHERE id = 1",
        ] {
            let error = ensure_timestamp_scatter_is_supported(statement)
                .expect_err("only plain inserts scatter");
            assert!(error.code == sqlstate::FEATURE_NOT_SUPPORTED);
            assert!(
                error.message.contains("plain INSERT only"),
                "unexpected scatter refusal for {statement}: {error:?}"
            );
        }
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
