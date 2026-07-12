//! Typed range-map and routing primitives for Chapter Gres multi-range tenants.

pub mod barrier;
pub mod control;
pub mod coordinator;
pub mod forward;
mod ids;
mod lifecycle;
mod map;
pub mod meta;
mod naming;
pub mod prologue;
pub mod range0_tail;
pub mod registry;
pub mod split;
pub mod split_hooks;
pub mod tenant;
pub mod transfer;
pub mod transport;
pub mod tso;

#[cfg(test)]
pub use transport::{serve_tcp, spawn_loopback};

pub use self::{
    barrier::{BarrierError, Range0Barrier, Range0EndSampler},
    coordinator::{
        LocalCoordinator, LocalCoordinatorError, LocalTransactionRecord, NetCoordinator,
        NetDecision, PreparedParticipant, TransactionDecision, TransactionPhase, TxnRpc,
        TxnRpcError,
    },
    forward::{
        ForwardError, HostedRangeService, RangeScanService, RegistryRangeScanner,
        RegistryRemoteForward, RemoteForward,
    },
    ids::{KeyHash, MapEpoch, RangeId, ShardId, TableId, TenantName},
    lifecycle::{
        BarrierOffset, FenceFirstRecovery, RangeLifecyclePhase, RangePrologue, RangeTransition,
    },
    map::{
        CoLocationGroup, HashShardSpec, KeyRoute, MapValidationError, MergePlan, RangeKey,
        RangeMap, RangeScanSegment, RangeSpec, RouteIntent, RowInterval, SplitPlan, TableRoute,
    },
    meta::{
        LoadedRangeMap, RangeMapCommitter, RangeMapLoader, RangeMapMetadata, RangeMapMetadataError,
        RangeMapMetadataReader, RangeMapMetadataWriter,
    },
    naming::{CheckpointPrefix, TopicName, TransactionalId, checkpoint_prefix, txn_id, wal_topic},
    range0_tail::{Range0Frame, Range0Tail, Range0TailError},
    registry::{RangeEndpoint, RangeRegistry, RegistryError},
    split::{
        CheckpointManifest, ConvertTableCommand, InDoubtMarker, MergeRangeCommand,
        MoveRangeCommand, SplitCommand, SplitError, SplitHooks, SplitOperation, SplitOrchestrator,
        SplitState, SplitStateStore, SplitStep, SuccessorDescriptor, run_conversion, run_merge,
        run_move, run_split,
    },
    split_hooks::{
        CheckpointOperation, FilteredSuccessorRestoreOperation, InDoubtMarkerInheritanceOperation,
        PredecessorParkingOperation, RangeMapCommitOperation, SplitHookAdapter,
        SplitHookAdapterBuilder, SplitHookOperation, SuccessorPrologueOperation,
        WriteGateOperation,
    },
    tenant::{
        GatewayCommitFault, LocalSqlSplitError, MultiRangeTenant, MultiRangeTenantConfig,
        MultiRangeTenantHandles, ReadOnlyRange0Replica, RouteRecord, StatementKind, TenantError,
        pgexec_timestamp_oracle_from_rpc, tso_rpc_from_horizon,
    },
    transfer::{
        ClaimedStagedSuccessor, ClaimedStagedSuccessors, CommittedTailRecord, RangeTransferBarrier,
        RangeTransferCapability, RangeTransferError, StagedRangeSuccessor, StagedRangeSuccessors,
        TableTransferRequest, ValidatedSplitTransferPlan,
    },
    transport::{
        FramedTcpClient, RangeRequest, RangeResponse, RangeService, RangeTlsClientConfig,
        RangeTlsServerConfig, ScanCursorReq, ScanCursorResp, TransportError, TsoReq, TsoResp,
        TxnReq, TxnResp, WireErrorKind, serve_tls,
    },
    tso::{
        BatchedTsoClient, EpochHeartbeat, GrantLease, HeartbeatVerdict, MemoryTsoHorizon, TsoError,
        TsoHorizonCommitter, TsoOracle, TsoRpc, TsoTimestamp,
    },
};
