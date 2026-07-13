//! Framed range-compute transport for SQL forwarding and transaction RPC.

#[cfg(test)]
use std::net::SocketAddr;
use std::{collections::BTreeSet, future::Future, sync::Arc, time::Duration};

use async_trait::async_trait;
use crabka_pgwire::engine::{ResultPage, ResultSink};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::RangeId;

const MAX_FRAME_BYTES: usize = 1 << 20;
// Leave room for response structure, the final command tag, and JSON punctuation.
// Individual rows are measured exactly; this conservative envelope keeps every
// emitted frame below the hard decoder limit without accumulating an encoded copy.
const SQL_CHUNK_TARGET_BYTES: usize = MAX_FRAME_BYTES - (4 << 10);
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// Request sent between range computes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(
    clippy::large_enum_variant,
    reason = "range RPC keeps request shapes value-typed"
)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum RangeRequest {
    /// Forward one SQL statement to its owning range.
    Sql { range_id: RangeId, sql: String },
    /// Open one owner-side connection session.
    SessionOpen { range_id: RangeId },
    /// Execute one stateful protocol operation in an owner session.
    Session {
        range_id: RangeId,
        session_id: u64,
        operation: WireSessionOperation,
    },
    /// Release all statements, portals and transaction state for a session.
    SessionClose { range_id: RangeId, session_id: u64 },
    /// Replicate a durable global transaction decision to one range owner.
    GlobalDecision {
        range_id: RangeId,
        global_xid: u64,
        status: WireGlobalStatus,
    },
    /// Allocate and durably publish one global transaction id on range 0.
    GlobalBegin { range_id: RangeId },
    /// Acquire, renew, or release the range-0 ordinary transaction lease.
    ExplicitGate(ExplicitGateReq),
    /// Settle abandoned live owner sessions for one durable global xid.
    RecoverGlobal {
        range_id: RangeId,
        global_xid: u64,
        commit: bool,
    },
    /// Ask an owning range to scan a table rowid interval under caller snapshots.
    ScanRange(ScanRangeReq),
    /// Execute one typed join fragment on an owning range.
    JoinRange(JoinRangeReq),
    /// Pull one bounded page from an owner-issued range cursor token.
    ScanCursor(ScanCursorReq),
    /// Run one transaction-coordinator RPC.
    Txn(TxnReq),
    /// Run one timestamp-oracle RPC against range 0.
    Tso(TsoReq),
    /// Resolve a timestamp transaction through its primary range.
    ResolveTxn(ResolveTxnReq),
    /// Durably prewrite timestamp intents on an owning participant.
    TimestampPrewrite(TimestampPrewriteReq),
    /// Durably acknowledge secondary operations on the authenticated primary.
    TimestampPrimaryAck(TimestampPrimaryAckReq),
    /// Resolve timestamp intents after the primary has chosen a decision.
    TimestampResolve(TimestampResolveReq),
    /// Idempotently settle participant intents from a durable primary descriptor.
    TimestampRecover(TimestampRecoverReq),
    /// Authenticate and settle an orphan through its authoritative primary.
    TimestampPrimaryRecover(TimestampPrimaryRecoverReq),
    /// Read a validated primary descriptor without choosing a decision.
    TimestampPrimaryInspect(TimestampPrimaryRecoverReq),
    /// Inspect a bounded page of authoritative committed durable records.
    InspectDurableRecords(InspectDurableRecordsReq),
    /// Run one generation-fenced split control operation over authenticated transport.
    Control(RangeControlReq),
}

/// Response sent between range computes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum RangeResponse {
    /// SQL statement completed and returned a command tag/summary.
    Sql { result: String },
    /// Complete simple-query results, including row descriptions and encoded cells.
    SqlResults { results: Vec<WireQueryResult> },
    /// One bounded part of a SQL result stream. This is internal to the framed
    /// transport and is reassembled by [`FramedTcpClient::call`].
    SqlResultsChunk { chunk: WireSqlResultChunk },
    /// Terminates a bounded SQL result stream.
    SqlResultsDone,
    /// SQL execution failed with a `PostgreSQL` error preserved from the owner.
    SqlError { code: String, message: String },
    /// A newly allocated owner session.
    SessionOpened { session_id: u64 },
    /// Result of one stateful owner-session operation.
    SessionResult { result: WireSessionResult },
    /// Effective immutable global decision status.
    GlobalStatus { status: WireGlobalStatus },
    /// Newly allocated global transaction id.
    GlobalXid { global_xid: u64 },
    /// Range-0 ordinary transaction lease result.
    ExplicitGate(ExplicitGateResp),
    /// Abandoned live owner sessions were inspected and settled idempotently.
    GlobalRecovered,
    /// Visible rows returned by a range scan.
    ScanRange(ScanRangeResp),
    /// Deterministically encoded rows returned by a join fragment.
    JoinRange(JoinRangeResp),
    /// One bounded owner-cursor page.
    ScanCursor(ScanCursorResp),
    /// Range-scan execution failed with the owner's `PostgreSQL` error code.
    ScanRangeError { code: String, message: String },
    /// Transaction RPC response.
    Txn(TxnResp),
    /// Timestamp-oracle RPC response.
    Tso(TsoResp),
    /// Primary-range timestamp transaction resolution response.
    ResolveTxn(ResolveTxnResp),
    /// Timestamp participant operation completed.
    TimestampParticipantDone,
    /// Effective decision returned by the authenticated primary.
    TimestampPrimaryDecision {
        decision: WireTimestampDecision,
        operations: Vec<WireTimestampOperation>,
    },
    /// Read-only validated primary descriptor outcome.
    TimestampPrimaryOutcome {
        decision: WirePrimaryTxnDecision,
        operations: Vec<WireTimestampOperation>,
    },
    /// One bounded authoritative durable-record page.
    InspectDurableRecords(InspectDurableRecordsResp),
    /// Explicit result of a split control operation.
    Control(RangeControlResp),
    /// Range compute rejected the request.
    Error {
        error: WireErrorKind,
        message: String,
    },
}

/// Maximum records returned by one durable inspection page.
pub const MAX_DURABLE_INSPECT_RECORDS: u32 = 4_096;
/// Maximum raw key plus value bytes returned by one durable inspection page.
pub const MAX_DURABLE_INSPECT_BYTES: u32 = 128 * 1024;

/// Authenticated, generation-fenced durable-record inspection request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectDurableRecordsReq {
    pub tenant: String,
    pub range_id: RangeId,
    pub generation: u64,
    pub table_id: u32,
    /// Inclusive physical key bound within the requested table namespace.
    pub start_key: Vec<u8>,
    /// Exclusive physical key bound within the requested table namespace.
    pub end_key: Vec<u8>,
    pub max_records: u32,
    pub max_bytes: u32,
    /// Optional previously sampled committed offset for stable reassembly.
    pub snapshot_offset: Option<i64>,
    /// Opaque continuation token issued for exactly this request shape.
    pub cursor: Option<String>,
}

/// Exact durable record bytes and their last modifying WAL revision when known.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub source_offset: Option<i64>,
    pub source_revision: Option<u64>,
}

/// Durable source identity shared by every record in an inspection page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableInspectProvenance {
    pub sample_offset: i64,
    pub wal_generation: u64,
    pub replay_start_offset: i64,
    pub replayed_records: u64,
    pub checkpoint_pairs: u64,
    pub checkpoint_manifest_key: Option<String>,
    pub checkpoint_covered_offset: Option<i64>,
    pub checkpoint_journal_seq: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectDurableRecordsResp {
    pub records: Vec<DurableRecord>,
    pub next_cursor: Option<String>,
    pub provenance: DurableInspectProvenance,
}

/// Serializable `(table_id, rowid)` boundary used by split control.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireRangeKey {
    pub table_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<u32>,
    pub rowid: u64,
}

/// Serializable in-doubt marker returned during interval inheritance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireInDoubtMarker {
    pub transaction_id: u64,
    pub key: WireRangeKey,
}

/// One authenticated, generation-fenced split control request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeControlReq {
    pub tenant: String,
    pub range_id: RangeId,
    pub generation: u64,
    pub operation_id: String,
    pub operation: RangeControlOperation,
}

/// Idempotent side effect requested from one hosted range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "operation")]
pub enum RangeControlOperation {
    ForceCheckpoint,
    PauseAtCoveredOffset {
        manifest_key: String,
        covered_offset: i64,
    },
    Status,
    StageFilteredRestore {
        journal_revision: u64,
        journal_digest: String,
    },
    SuccessorFencePrologue {
        journal_revision: u64,
        journal_digest: String,
    },
    InheritMarkers {
        journal_revision: u64,
        journal_digest: String,
    },
    RetirePredecessor,
    Resume,
}

/// Explicit control result; callers never infer success from connection closure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "result")]
pub enum RangeControlResp {
    Applied,
    AlreadyApplied,
    Ambiguous {
        message: String,
    },
    Rejected {
        code: String,
        message: String,
    },
    Checkpoint {
        generation: u64,
        covered_offset: i64,
        manifest_key: String,
    },
    Paused {
        barrier_offset: i64,
    },
    Staged {
        tail_sha256: String,
    },
    Status {
        paused: bool,
        serving: bool,
        barrier_offset: Option<i64>,
    },
    Markers {
        markers: Vec<WireInDoubtMarker>,
        #[serde(default)]
        left_markers: Option<Vec<WireInDoubtMarker>>,
        #[serde(default)]
        right_markers: Option<Vec<WireInDoubtMarker>>,
        digest: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "operation")]
pub enum ExplicitGateReq {
    Acquire,
    Renew { token: u64 },
    Release { token: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "result")]
pub enum ExplicitGateResp {
    Acquired { token: u64, lease_millis: u64 },
    Renewed { lease_millis: u64 },
    Released,
    Stale,
}

/// Serializable simple-query result returned by a range owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum WireQueryResult {
    Rows {
        fields: Vec<WireFieldDescription>,
        rows: Vec<Vec<Option<WireCell>>>,
        tag: String,
    },
    Command {
        tag: String,
    },
    Empty,
}

/// A frame-bounded fragment of one simple-query result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum WireSqlResultChunk {
    Rows {
        result_index: u32,
        /// Present only on the first chunk, avoiding metadata amplification.
        fields: Option<Vec<WireFieldDescription>>,
        rows: Vec<Vec<Option<WireCell>>>,
        /// Present only on the final chunk for this result.
        tag: Option<String>,
    },
    Complete {
        result_index: u32,
        result: WireQueryResult,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireCell {
    pub text: Vec<u8>,
    pub binary: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireFieldDescription {
    pub name: String,
    pub table_oid: u32,
    pub column_id: i16,
    pub type_oid: u32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: i16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireBoundParam {
    pub type_oid: Option<u32>,
    pub format: i16,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum WireGlobalStatus {
    InProgress,
    Prepared { global_xid: u64 },
    Committed,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum WireSessionOperation {
    SimpleQuery {
        sql: String,
    },
    Parse {
        name: String,
        sql: String,
        parameter_types: Vec<u32>,
    },
    Bind {
        portal: String,
        statement: String,
        params: Vec<WireBoundParam>,
        result_formats: Vec<i16>,
    },
    DescribeStatement {
        name: String,
    },
    DescribePortal {
        name: String,
    },
    Execute {
        portal: String,
        max_rows: u32,
    },
    PrepareGlobal {
        global_xid: u64,
    },
    CommitGlobal {
        global_xid: u64,
    },
    AbortGlobal {
        global_xid: u64,
    },
    SetTimestampOwner {
        start_ts: Option<u64>,
    },
    CloseStatement {
        name: String,
    },
    ClosePortal {
        name: String,
    },
    Sync,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum WireSessionResult {
    Query {
        results: Vec<WireQueryResult>,
    },
    Prepared {
        parameter_types: Vec<u32>,
        fields: Vec<WireFieldDescription>,
    },
    Portal {
        fields: Vec<WireFieldDescription>,
    },
    Execute(WireExecuteOutcome),
    GlobalPrepared {
        global_xid: u64,
    },
    Closed,
    Synced {
        tx_status: u8,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum WireExecuteOutcome {
    Rows {
        rows: Vec<Vec<Option<Vec<u8>>>>,
        completion: Option<String>,
    },
    Command {
        tag: String,
    },
    Empty,
}

/// Timestamp-oracle RPC sent to range 0.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TsoReq {
    /// Grant `count` contiguous transaction timestamps.
    Grant { count: u64 },
}

/// Timestamp-oracle RPC response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TsoResp {
    /// Contiguous timestamp lease granted by range 0.
    Granted { first_ts: u64, count: u64 },
}

/// Primary-range timestamp transaction resolution request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveTxnReq {
    pub primary_range: RangeId,
    pub start_ts: u64,
}

/// Primary-range timestamp transaction resolution response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ResolveTxnResp {
    /// The primary has durably committed the transaction at `commit_ts`.
    Committed { commit_ts: u64 },
    /// The primary has durably aborted the transaction.
    Aborted,
    /// The primary has no terminal decision yet; the reader must exclude the
    /// intent or retry/push-abort via the caller's bounded-wait policy.
    Pending,
}

/// Serializable MVCC snapshot used by range-scan RPCs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireSnapshot {
    pub xmin: u64,
    pub xmax: u64,
    pub xip: Vec<u64>,
}

impl From<&crabka_pgmvcc::visibility::Snapshot> for WireSnapshot {
    fn from(value: &crabka_pgmvcc::visibility::Snapshot) -> Self {
        Self {
            xmin: value.xmin,
            xmax: value.xmax,
            xip: value.xip.clone(),
        }
    }
}

impl From<WireSnapshot> for crabka_pgmvcc::visibility::Snapshot {
    fn from(value: WireSnapshot) -> Self {
        Self {
            xmin: value.xmin,
            xmax: value.xmax,
            xip: value.xip,
        }
    }
}

/// Serializable rowid interval for range-scan RPCs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireRowInterval {
    pub start: Option<u64>,
    pub end: Option<u64>,
}

/// Serializable predicate pushdown for range-scan RPCs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum WirePredicatePushdown {
    FullScan,
    Conjunctive {
        predicates: Vec<WireColumnPredicate>,
    },
}

/// Serializable column/literal predicate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireColumnPredicate {
    pub column: usize,
    pub op: WirePredicateOp,
    pub value: WireDatum,
}

/// Serializable predicate operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WirePredicateOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Serializable literal subset used by predicate pushdown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum WireDatum {
    Null,
    Bool(bool),
    Int4(i32),
    Int8(i64),
    Text(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireTimestampIdentity {
    pub start_ts: u64,
    pub global_xid: u64,
    pub primary_range: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireTimestampWrite {
    pub table_id: u32,
    #[serde(default)]
    pub bucket: Option<u32>,
    pub rowid: u64,
    pub row: Vec<WireDatum>,
    pub delete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimestampPrewriteReq {
    pub range_id: RangeId,
    pub identity: WireTimestampIdentity,
    #[serde(default)]
    pub primary_participants: Vec<u32>,
    #[serde(default)]
    pub secondary: bool,
    #[serde(default)]
    pub existing_primary: bool,
    pub writes: Vec<WireTimestampWrite>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimestampPrimaryAckReq {
    pub primary_range: RangeId,
    pub identity: WireTimestampIdentity,
    pub participant_range: u32,
    pub operations: Vec<WireTimestampOperation>,
    #[serde(default)]
    pub add_participant: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum WireTimestampDecision {
    Aborted,
    Committed { commit_ts: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum WirePrimaryTxnDecision {
    Pending,
    Aborted,
    Committed { commit_ts: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimestampResolveReq {
    pub range_id: RangeId,
    pub identity: WireTimestampIdentity,
    pub decision: WireTimestampDecision,
    pub writes: Vec<WireTimestampWrite>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireTimestampOperation {
    pub range_id: u32,
    pub table_id: u32,
    #[serde(default)]
    pub bucket: Option<u32>,
    pub rowid: u64,
    pub delete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimestampRecoverReq {
    pub range_id: RangeId,
    pub identity: WireTimestampIdentity,
    pub decision: WireTimestampDecision,
    pub operations: Vec<WireTimestampOperation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimestampPrimaryRecoverReq {
    pub primary_range: RangeId,
    pub identity: WireTimestampIdentity,
}

/// Serializable projection pushdown for range-scan RPCs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum WireProjectionPushdown {
    All,
    Columns { columns: Vec<usize> },
}

/// Serializable partial aggregate request shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WirePartialAggregateSpec {
    pub function: WirePartialAggregateFunction,
    pub column: Option<usize>,
    #[serde(default)]
    pub group_by: Vec<usize>,
}

/// Serializable partial aggregate function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WirePartialAggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    AvgParts,
}

/// Serializable top-K request shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireTopKSpec {
    pub order_by: Vec<WireTopKColumn>,
    pub limit: u64,
}

/// Serializable top-K ordering key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireTopKColumn {
    pub column: usize,
    pub asc: bool,
}

/// Range-scan request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScanRangeReq {
    pub range_id: RangeId,
    pub table_name: String,
    pub interval: WireRowInterval,
    pub local_snapshot: WireSnapshot,
    pub global_snapshot: WireSnapshot,
    pub own_xid: Option<u64>,
    pub read_ts: Option<u64>,
    #[serde(default)]
    pub own_start_ts: Option<u64>,
    pub predicate: WirePredicatePushdown,
    pub projection: WireProjectionPushdown,
    pub partial_aggregate: Option<WirePartialAggregateSpec>,
    pub top_k: Option<WireTopKSpec>,
}

/// One encoded visible tuple returned by [`ScanRangeReq`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScanRangeRow {
    pub rowid: u64,
    pub xmin: u64,
    /// Tuple payload encoded with `crabka_pgmvcc::version::encode_tuple`.
    pub tuple: Vec<u8>,
}

/// Range-scan response. Rows are sorted by rowid by the owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScanRangeResp {
    pub rows: Vec<ScanRangeRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireJoinKind {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireJoinStrategy {
    BroadcastLeft,
    BroadcastRight,
    CoPartitioned,
    Gather,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireJoinTableInterval {
    pub table_id: u64,
    pub table_name: String,
    pub interval: WireRowInterval,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct JoinRangeRow {
    pub tuple: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinRangeReq {
    pub range_id: RangeId,
    pub local_snapshot: WireSnapshot,
    pub global_snapshot: WireSnapshot,
    pub read_ts: u64,
    pub own_xid: Option<u64>,
    pub own_start_ts: Option<u64>,
    pub kind: WireJoinKind,
    pub left_keys: Vec<usize>,
    pub right_keys: Vec<usize>,
    pub strategy: WireJoinStrategy,
    pub left: WireJoinTableInterval,
    pub right: WireJoinTableInterval,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broadcast_rows: Option<Vec<JoinRangeRow>>,
    pub left_filter: WirePredicatePushdown,
    pub right_filter: WirePredicatePushdown,
    pub projection: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinRangeResp {
    pub rows: Vec<JoinRangeRow>,
}

impl JoinRangeReq {
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn validate(&self) -> Result<(), crabka_pgexec::JoinValidationError> {
        self.to_pgexec().validate()
    }

    /// Whether the fully materialized request, including enum/request JSON
    /// overhead, fits the production bounded frame.
    #[must_use]
    pub fn fits_transport_frame(&self) -> bool {
        serde_json::to_vec(&RangeRequest::JoinRange(self.clone()))
            .is_ok_and(|bytes| bytes.len() <= MAX_FRAME_BYTES)
    }

    pub(crate) fn to_pgexec(&self) -> crabka_pgexec::JoinRangeRequest {
        use crabka_pgexec::{JoinExecutionStrategy as S, JoinKind as K};
        crabka_pgexec::JoinRangeRequest {
            local_snapshot: join_snapshot(&self.local_snapshot),
            global_snapshot: join_snapshot(&self.global_snapshot),
            read_ts: self.read_ts,
            own_xid: self.own_xid,
            own_start_ts: self.own_start_ts,
            kind: match self.kind {
                WireJoinKind::Inner => K::Inner,
                WireJoinKind::Left => K::Left,
                WireJoinKind::Right => K::Right,
                WireJoinKind::Full => K::Full,
            },
            left_keys: self.left_keys.clone(),
            right_keys: self.right_keys.clone(),
            strategy: match self.strategy {
                WireJoinStrategy::BroadcastLeft => S::BroadcastLeft,
                WireJoinStrategy::BroadcastRight => S::BroadcastRight,
                WireJoinStrategy::CoPartitioned => S::CoPartitioned,
                WireJoinStrategy::Gather => S::Gather,
            },
            left: join_table(&self.left),
            right: join_table(&self.right),
            broadcast_rows: self.broadcast_rows.as_ref().map(|rows| {
                rows.iter()
                    .map(|row| crabka_pgexec::JoinRow {
                        tuple: row.tuple.clone(),
                    })
                    .collect()
            }),
            left_filter: decode_predicate_for_join(&self.left_filter),
            right_filter: decode_predicate_for_join(&self.right_filter),
            projection: self.projection.clone(),
        }
    }
}

fn join_snapshot(snapshot: &WireSnapshot) -> crabka_pgexec::JoinSnapshot {
    crabka_pgexec::JoinSnapshot {
        xmin: snapshot.xmin,
        xmax: snapshot.xmax,
        xip: snapshot.xip.clone(),
    }
}

fn join_table(table: &WireJoinTableInterval) -> crabka_pgexec::JoinTableInterval {
    crabka_pgexec::JoinTableInterval {
        table_id: table.table_id,
        table_name: table.table_name.clone(),
        interval: crabka_pgexec::RowInterval {
            start: table.interval.start,
            end: table.interval.end,
        },
    }
}

fn decode_predicate_for_join(
    predicate: &WirePredicatePushdown,
) -> crabka_pgexec::PredicatePushdown {
    match predicate {
        WirePredicatePushdown::FullScan => crabka_pgexec::PredicatePushdown::FullScan,
        WirePredicatePushdown::Conjunctive { predicates } => {
            crabka_pgexec::PredicatePushdown::Conjunctive(
                predicates
                    .iter()
                    .map(|item| crabka_pgexec::ColumnPredicate {
                        column: item.column,
                        op: match item.op {
                            WirePredicateOp::Eq => crabka_pgexec::PredicateOp::Eq,
                            WirePredicateOp::Lt => crabka_pgexec::PredicateOp::Lt,
                            WirePredicateOp::Le => crabka_pgexec::PredicateOp::Le,
                            WirePredicateOp::Gt => crabka_pgexec::PredicateOp::Gt,
                            WirePredicateOp::Ge => crabka_pgexec::PredicateOp::Ge,
                        },
                        value: match &item.value {
                            WireDatum::Null => crabka_pgtypes::Datum::Null,
                            WireDatum::Bool(value) => crabka_pgtypes::Datum::Bool(*value),
                            WireDatum::Int4(value) => crabka_pgtypes::Datum::Int4(*value),
                            WireDatum::Int8(value) => crabka_pgtypes::Datum::Int8(*value),
                            WireDatum::Text(value) => crabka_pgtypes::Datum::Text(value.clone()),
                        },
                    })
                    .collect(),
            )
        }
    }
}

/// One pull against an owner-controlled cursor. `token` is opaque to gateways.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScanCursorReq {
    pub scan: Box<ScanRangeReq>,
    pub token: Option<Vec<u8>>,
    pub max_rows: usize,
}

/// One bounded cursor page and the token required for the next pull.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScanCursorResp {
    pub rows: Vec<ScanRangeRow>,
    pub token: Option<Vec<u8>>,
    pub is_last: bool,
}

/// Transaction RPC sent over [`RangeRequest::Txn`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TxnReq {
    /// Ask a participant to prepare a global transaction.
    Prepare { gtid: u64, range_id: RangeId },
    /// Ask a participant to commit a prepared transaction.
    Commit { gtid: u64, range_id: RangeId },
    /// Ask a participant to abort a prepared transaction.
    Abort { gtid: u64, range_id: RangeId },
    /// Ask range 0 for a substrate durability barrier.
    Barrier { range_id: RangeId },
}

/// Transaction RPC response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TxnResp {
    /// Participant prepared successfully.
    Prepared,
    /// Participant committed successfully.
    Committed,
    /// Participant aborted or refused prepare.
    Aborted,
    /// Range-0 substrate offset covered by the barrier.
    Barrier { substrate_offset: i64 },
}

/// Retry-visible error class returned by remote range computes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireErrorKind {
    /// The endpoint no longer owns the requested range.
    StaleEndpoint,
    /// The endpoint is not currently the range writer.
    NotLeader,
    /// Non-retryable transaction abort.
    Aborted,
    /// Non-retryable protocol/application failure.
    Failed,
}

impl WireErrorKind {
    /// Return whether the forwarding layer may re-resolve and retry exactly once.
    #[must_use]
    pub const fn permits_reresolve(self) -> bool {
        matches!(self, Self::StaleEndpoint | Self::NotLeader)
    }
}

/// Transport-level failure.
#[derive(Debug, Error)]
pub enum TransportError {
    /// Frame exceeded the protocol limit.
    #[error("range frame too large: {actual} bytes exceeds {limit}")]
    FrameTooLarge { actual: usize, limit: usize },
    /// JSON payload was invalid.
    #[error("range frame json error: {0}")]
    Json(#[from] serde_json::Error),
    /// Socket IO failed.
    #[error("range transport io error: {0}")]
    Io(#[from] std::io::Error),
    /// The peer was silent past the configured deadline.
    #[error("range transport timed out after {0:?}")]
    Timeout(Duration),
    /// The remote endpoint returned an application error.
    #[error("range endpoint returned {kind:?}: {message}")]
    Remote {
        kind: WireErrorKind,
        message: String,
    },
    /// The remote SQL engine returned a `PostgreSQL` error.
    #[error("remote SQL error {code}: {message}")]
    Sql { code: String, message: String },
    /// The peer returned the wrong response variant.
    #[error("range endpoint returned an unexpected response")]
    UnexpectedResponse,
    /// The peer violated framed stream ordering or shape invariants.
    #[error("range transport protocol error: {0}")]
    Protocol(String),
    /// TLS setup or handshake failed.
    #[error("range transport tls error: {0}")]
    Tls(String),
    /// The authenticated peer is not authorized for this tenant.
    #[error("range transport peer is not authorized for tenant {tenant}")]
    UnauthorizedPeer { tenant: String },
}

/// Required mTLS configuration for outbound range forwarding.
#[derive(Debug, Clone)]
pub struct RangeTlsClientConfig {
    /// TLS identity and trust roots. A client identity and trust roots are mandatory.
    pub tls: crabka_security::TlsConfig,
    /// DNS name verified against the remote server certificate and sent as SNI.
    pub server_name: String,
}

impl RangeTlsClientConfig {
    /// Build a client configuration that cannot use plaintext or anonymous TLS.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn build_connector(&self) -> Result<TlsConnector, TransportError> {
        if self.tls.trust_roots_path.is_none() {
            return Err(TransportError::Tls(
                "range TLS requires a server trust CA".to_string(),
            ));
        }
        if self.server_name.trim().is_empty() {
            return Err(TransportError::Tls(
                "range TLS requires a non-empty server name".to_string(),
            ));
        }
        self.tls
            .build_client_config_with_identity()
            .map(TlsConnector::from)
            .map_err(|error| TransportError::Tls(error.to_string()))
    }
}

/// Required mTLS and tenant authorization configuration for a range listener.
#[derive(Debug, Clone)]
pub struct RangeTlsServerConfig {
    /// Immutable tenant served by this listener.
    pub tenant: String,
    /// TLS server identity, client CA, and required client authentication.
    pub tls: crabka_security::TlsConfig,
    /// Subject DNs allowed to execute ordinary range-to-range RPCs for `tenant`.
    pub range_rpc_principals: BTreeSet<String>,
    /// Subject DNs allowed to execute destructive operator control RPCs.
    pub operator_control_principals: BTreeSet<String>,
}

impl RangeTlsServerConfig {
    /// Parse and validate the listener security boundary before binding a socket.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn build_acceptor(&self) -> Result<TlsAcceptor, TransportError> {
        if self.tenant.trim().is_empty() {
            return Err(TransportError::Tls(
                "range TLS requires a tenant".to_string(),
            ));
        }
        if self.tls.client_auth != crabka_security::ClientAuthMode::Required {
            return Err(TransportError::Tls(
                "range TLS requires client authentication".to_string(),
            ));
        }
        if self.tls.client_ca_path.is_none() {
            return Err(TransportError::Tls(
                "range TLS requires a client CA".to_string(),
            ));
        }
        if self.range_rpc_principals.is_empty() {
            return Err(TransportError::Tls(
                "range TLS requires at least one tenant-authorized principal".to_string(),
            ));
        }
        self.tls
            .build_server_config()
            .map(TlsAcceptor::from)
            .map_err(|error| TransportError::Tls(error.to_string()))
    }
}

/// Trait implemented by local range-compute request handlers.
#[async_trait]
pub trait RangeService: Send + Sync + 'static {
    /// Handle one decoded request.
    async fn handle(&self, request: RangeRequest) -> RangeResponse;

    /// Optionally consume a request while owning the live response writer.
    async fn handle_connection(
        &self,
        request: RangeRequest,
        _writer: &mut (dyn AsyncWrite + Unpin + Send),
    ) -> Result<Option<RangeResponse>, TransportError> {
        Ok(Some(self.handle(request).await))
    }
}

/// Authenticated client for framed TLS range RPC.
#[derive(Debug, Clone)]
pub struct FramedTcpClient {
    timeout: Duration,
    mode: RangeClientMode,
}

#[derive(Debug, Clone)]
enum RangeClientMode {
    Tls(RangeTlsClientConfig),
    PreparedTls {
        config: Arc<rustls::ClientConfig>,
        server_name: String,
    },
    #[cfg(test)]
    Plaintext,
}

/// Plaintext range transport exists only inside this crate's unit tests.
///
/// It is deliberately not exported from production builds: every production
/// range RPC must present an mTLS identity and verify its peer.
#[cfg(test)]
impl Default for FramedTcpClient {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_RPC_TIMEOUT,
            mode: RangeClientMode::Plaintext,
        }
    }
}

impl FramedTcpClient {
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn with_tls_pem(
        cert_chain_pem: &[u8],
        private_key_pem: &[u8],
        trust_roots_pem: &[u8],
        server_name: String,
    ) -> Result<Self, TransportError> {
        if server_name.trim().is_empty() {
            return Err(TransportError::Tls(
                "range TLS requires a non-empty server name".into(),
            ));
        }
        let config = crabka_security::TlsConfig::build_client_config_from_pem(
            cert_chain_pem,
            private_key_pem,
            trust_roots_pem,
        )
        .map_err(|error| TransportError::Tls(error.to_string()))?;
        Ok(Self {
            timeout: DEFAULT_RPC_TIMEOUT,
            mode: RangeClientMode::PreparedTls {
                config,
                server_name,
            },
        })
    }

    /// Build a plaintext client with an explicit wire-silence timeout for unit tests.
    #[cfg(test)]
    #[must_use]
    pub const fn with_timeout(timeout: Duration) -> Self {
        Self {
            timeout,
            mode: RangeClientMode::Plaintext,
        }
    }

    /// Build a TLS-only forwarding client. This path always presents a client
    /// identity and validates the remote certificate and SNI name.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn with_tls(config: RangeTlsClientConfig) -> Result<Self, TransportError> {
        config.build_connector()?;
        Ok(Self {
            timeout: DEFAULT_RPC_TIMEOUT,
            mode: RangeClientMode::Tls(config),
        })
    }

    /// Send one request and await one response.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn call(
        &self,
        endpoint: &str,
        request: &RangeRequest,
    ) -> Result<RangeResponse, TransportError> {
        let stream = timeout(self.timeout, TcpStream::connect(endpoint)).await??;
        match &self.mode {
            RangeClientMode::Tls(config) => {
                let connector = config.build_connector()?;
                let server_name =
                    rustls::pki_types::ServerName::try_from(config.server_name.as_str())
                        .map_err(|error| {
                            TransportError::Tls(format!("invalid range server name: {error}"))
                        })?
                        .to_owned();
                let stream = timeout(self.timeout, connector.connect(server_name, stream))
                    .await
                    .map_err(|_| TransportError::Timeout(self.timeout))?
                    .map_err(|error| TransportError::Tls(error.to_string()))?;
                call_stream(stream, request, self.timeout).await
            }
            RangeClientMode::PreparedTls {
                config,
                server_name,
            } => {
                let connector = TlsConnector::from(Arc::clone(config));
                let server_name = rustls::pki_types::ServerName::try_from(server_name.as_str())
                    .map_err(|error| {
                        TransportError::Tls(format!("invalid range server name: {error}"))
                    })?
                    .to_owned();
                let stream = timeout(self.timeout, connector.connect(server_name, stream))
                    .await
                    .map_err(|_| TransportError::Timeout(self.timeout))?
                    .map_err(|error| TransportError::Tls(error.to_string()))?;
                call_stream(stream, request, self.timeout).await
            }
            #[cfg(test)]
            RangeClientMode::Plaintext => call_stream(stream, request, self.timeout).await,
        }
    }

    /// Send one SQL request and forward bounded result pages as they arrive.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn call_sql_into(
        &self,
        endpoint: &str,
        request: &RangeRequest,
        sink: &mut dyn ResultSink,
    ) -> Result<(), TransportError> {
        let stream = timeout(self.timeout, TcpStream::connect(endpoint)).await??;
        match &self.mode {
            RangeClientMode::Tls(config) => {
                let connector = config.build_connector()?;
                let server_name =
                    rustls::pki_types::ServerName::try_from(config.server_name.as_str())
                        .map_err(|error| {
                            TransportError::Tls(format!("invalid range server name: {error}"))
                        })?
                        .to_owned();
                let stream = timeout(self.timeout, connector.connect(server_name, stream))
                    .await
                    .map_err(|_| TransportError::Timeout(self.timeout))?
                    .map_err(|error| TransportError::Tls(error.to_string()))?;
                call_sql_stream_into(stream, request, self.timeout, sink).await
            }
            RangeClientMode::PreparedTls {
                config,
                server_name,
            } => {
                let connector = TlsConnector::from(Arc::clone(config));
                let server_name = rustls::pki_types::ServerName::try_from(server_name.as_str())
                    .map_err(|error| {
                        TransportError::Tls(format!("invalid range server name: {error}"))
                    })?
                    .to_owned();
                let stream = timeout(self.timeout, connector.connect(server_name, stream))
                    .await
                    .map_err(|_| TransportError::Timeout(self.timeout))?
                    .map_err(|error| TransportError::Tls(error.to_string()))?;
                call_sql_stream_into(stream, request, self.timeout, sink).await
            }
            #[cfg(test)]
            RangeClientMode::Plaintext => {
                call_sql_stream_into(stream, request, self.timeout, sink).await
            }
        }
    }
}

async fn call_sql_stream_into<S>(
    mut stream: S,
    request: &RangeRequest,
    wait: Duration,
    sink: &mut dyn ResultSink,
) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    timeout(wait, write_frame(&mut stream, request)).await??;
    timeout(wait, stream.flush()).await??;
    loop {
        match timeout(wait, read_frame(&mut stream)).await?? {
            RangeResponse::SqlResultsChunk { chunk } => {
                sink.send(wire_chunk_to_result_page(chunk)?)
                    .await
                    .map_err(|error| TransportError::Remote {
                        kind: WireErrorKind::Failed,
                        message: error.message,
                    })?;
            }
            RangeResponse::SqlResultsDone => return Ok(()),
            RangeResponse::SqlError { code, message } => {
                return Err(TransportError::Sql { code, message });
            }
            RangeResponse::Error { error, message } => {
                return Err(TransportError::Remote {
                    kind: error,
                    message,
                });
            }
            _ => {
                return Err(TransportError::Protocol(
                    "unexpected SQL result stream frame".into(),
                ));
            }
        }
    }
}

fn wire_chunk_to_result_page(chunk: WireSqlResultChunk) -> Result<ResultPage, TransportError> {
    match chunk {
        WireSqlResultChunk::Complete {
            result_index,
            result,
        } => match result {
            WireQueryResult::Command { tag } => Ok(ResultPage::Command {
                result_index: result_index as usize,
                tag,
            }),
            WireQueryResult::Empty => Ok(ResultPage::Empty {
                result_index: result_index as usize,
            }),
            WireQueryResult::Rows { .. } => Err(TransportError::Protocol(
                "complete SQL chunk contained row result".into(),
            )),
        },
        WireSqlResultChunk::Rows {
            result_index,
            fields,
            rows,
            tag,
        } => Ok(ResultPage::Rows {
            result_index: result_index as usize,
            fields: fields.map(|fields| fields.into_iter().map(Into::into).collect()),
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(|cell| cell.map(Into::into)).collect())
                .collect(),
            tag,
        }),
    }
}

/// Serve plaintext framed requests in unit tests only.
///
/// Production range listeners must use [`serve_tls`]. This symbol is omitted
/// from non-test builds so a production binary cannot accidentally expose a
/// [`RangeService`] without mTLS authorization.
///
/// # Errors
///
/// Returns an error when the listener cannot accept or serve a connection.
#[cfg(test)]
pub async fn serve_tcp(
    listener: TcpListener,
    service: Arc<dyn RangeService>,
) -> Result<(), TransportError> {
    loop {
        let (stream, _) = listener.accept().await?;
        let service = Arc::clone(&service);
        tokio::spawn(async move {
            if let Err(error) = handle_stream(stream, service).await {
                tracing::warn!(%error, "range transport connection failed");
            }
        });
    }
}

/// Serve TLS-only, mutually-authenticated range RPCs for one immutable tenant.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn serve_tls(
    listener: TcpListener,
    service: Arc<dyn RangeService>,
    config: RangeTlsServerConfig,
) -> Result<(), TransportError> {
    let acceptor = config.build_acceptor()?;
    loop {
        let (stream, _) = listener.accept().await?;
        let service = Arc::clone(&service);
        let acceptor = acceptor.clone();
        let range_rpc_principals = config.range_rpc_principals.clone();
        let operator_control_principals = config.operator_control_principals.clone();
        let tenant = config.tenant.clone();
        tokio::spawn(async move {
            let result = async {
                let mut stream = acceptor
                    .accept(stream)
                    .await
                    .map_err(|error| TransportError::Tls(error.to_string()))?;
                let certificates = stream.get_ref().1.peer_certificates().ok_or_else(|| {
                    TransportError::UnauthorizedPeer {
                        tenant: tenant.clone(),
                    }
                })?;
                let certificate =
                    certificates
                        .first()
                        .ok_or_else(|| TransportError::UnauthorizedPeer {
                            tenant: tenant.clone(),
                        })?;
                let principal = crabka_security::extract_principal_from_cert(certificate.as_ref())
                    .ok_or_else(|| TransportError::UnauthorizedPeer {
                        tenant: tenant.clone(),
                    })?;
                let request = read_frame(&mut stream).await?;
                if !principal_authorized_for_request(
                    &principal,
                    &request,
                    &range_rpc_principals,
                    &operator_control_principals,
                ) {
                    return Err(TransportError::UnauthorizedPeer { tenant });
                }
                handle_request_on_stream(&mut stream, service, request).await
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(%error, "range TLS transport connection rejected");
            }
        });
    }
}

fn principal_authorized_for_request(
    principal: &str,
    request: &RangeRequest,
    range_rpc_principals: &BTreeSet<String>,
    operator_control_principals: &BTreeSet<String>,
) -> bool {
    match request {
        RangeRequest::Control(_) => operator_control_principals.contains(principal),
        _ => range_rpc_principals.contains(principal),
    }
}

/// Bind a plaintext loopback server for unit tests only.
///
/// # Errors
///
/// Returns an error when the loopback listener cannot be bound.
#[cfg(test)]
pub async fn spawn_loopback(service: Arc<dyn RangeService>) -> Result<SocketAddr, TransportError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(error) = serve_tcp(listener, service).await {
            tracing::warn!(%error, "range transport server stopped");
        }
    });
    Ok(addr)
}

async fn call_stream<S>(
    mut stream: S,
    request: &RangeRequest,
    wait: Duration,
) -> Result<RangeResponse, TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    timeout(wait, write_frame(&mut stream, request)).await??;
    timeout(wait, stream.flush()).await??;
    let first = timeout(wait, read_frame(&mut stream)).await??;
    let chunk = match first {
        RangeResponse::SqlResultsChunk { chunk } => chunk,
        RangeResponse::SqlResultsDone => return Ok(RangeResponse::SqlResults { results: vec![] }),
        response => return Ok(response),
    };
    let mut results = Vec::new();
    consume_sql_chunk(&mut results, chunk)?;
    loop {
        match timeout(wait, read_frame(&mut stream)).await?? {
            RangeResponse::SqlResultsChunk { chunk } => consume_sql_chunk(&mut results, chunk)?,
            RangeResponse::SqlResultsDone => return Ok(RangeResponse::SqlResults { results }),
            RangeResponse::SqlError { code, message } => {
                return Ok(RangeResponse::SqlError { code, message });
            }
            _ => {
                return Err(TransportError::Protocol(
                    "unexpected SQL result stream frame".into(),
                ));
            }
        }
    }
}

fn consume_sql_chunk(
    results: &mut Vec<WireQueryResult>,
    chunk: WireSqlResultChunk,
) -> Result<(), TransportError> {
    match chunk {
        WireSqlResultChunk::Complete {
            result_index,
            result,
        } => {
            if usize::try_from(result_index).ok() != Some(results.len()) {
                return Err(TransportError::Protocol(
                    "out-of-order SQL result chunk".into(),
                ));
            }
            results.push(result);
        }
        WireSqlResultChunk::Rows {
            result_index,
            fields,
            mut rows,
            tag,
        } => {
            let index = usize::try_from(result_index)
                .map_err(|_| TransportError::Protocol("invalid SQL result index".into()))?;
            if index == results.len() {
                let fields = fields.ok_or_else(|| {
                    TransportError::Protocol("first row chunk omitted fields".into())
                })?;
                results.push(WireQueryResult::Rows {
                    fields,
                    rows: Vec::new(),
                    tag: String::new(),
                });
            } else if index + 1 != results.len() || fields.is_some() {
                return Err(TransportError::Protocol(
                    "out-of-order SQL row chunk".into(),
                ));
            }
            let WireQueryResult::Rows {
                rows: accumulated,
                tag: accumulated_tag,
                ..
            } = &mut results[index]
            else {
                return Err(TransportError::Protocol(
                    "SQL row chunk changed result kind".into(),
                ));
            };
            accumulated.append(&mut rows);
            if let Some(tag) = tag {
                *accumulated_tag = tag;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
async fn handle_stream<S>(
    mut stream: S,
    service: Arc<dyn RangeService>,
) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let request = read_frame(&mut stream).await?;
    handle_request_on_stream(&mut stream, service, request).await
}

async fn handle_request_on_stream<S>(
    stream: &mut S,
    service: Arc<dyn RangeService>,
    request: RangeRequest,
) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    if let Some(response) = service.handle_connection(request, stream).await? {
        if let RangeResponse::SqlResults { results } = response {
            write_sql_results(stream, results).await?;
        } else {
            write_frame(stream, &response).await?;
        }
    }
    stream.flush().await?;
    Ok(())
}

async fn write_sql_results<W>(
    writer: &mut W,
    results: Vec<WireQueryResult>,
) -> Result<(), TransportError>
where
    W: AsyncWrite + Unpin,
{
    for (index, result) in results.into_iter().enumerate() {
        let result_index = u32::try_from(index).map_err(|_| TransportError::FrameTooLarge {
            actual: index,
            limit: u32::MAX as usize,
        })?;
        match result {
            WireQueryResult::Rows { fields, rows, tag } => {
                write_row_chunks(writer, result_index, fields, rows, tag).await?;
            }
            result => {
                let response = RangeResponse::SqlResultsChunk {
                    chunk: WireSqlResultChunk::Complete {
                        result_index,
                        result,
                    },
                };
                match write_frame(writer, &response).await {
                    Ok(()) => {}
                    Err(TransportError::FrameTooLarge { .. }) => {
                        write_frame(
                            writer,
                            &RangeResponse::SqlError {
                                code: "54000".into(),
                                message: "one remote SQL result exceeds the transport frame limit"
                                    .into(),
                            },
                        )
                        .await?;
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                }
            }
        }
    }
    write_frame(writer, &RangeResponse::SqlResultsDone).await
}

async fn write_row_chunks<W>(
    writer: &mut W,
    result_index: u32,
    fields: Vec<WireFieldDescription>,
    rows: Vec<Vec<Option<WireCell>>>,
    tag: String,
) -> Result<(), TransportError>
where
    W: AsyncWrite + Unpin,
{
    let mut fields = Some(fields);
    let mut page = Vec::new();
    let mut page_bytes = 0usize;
    for row in rows {
        let row_bytes = match serialize_json_bounded(&row, SQL_CHUNK_TARGET_BYTES) {
            Ok(bytes) => bytes.len().saturating_add(1),
            Err(TransportError::FrameTooLarge { .. }) => {
                write_frame(
                    writer,
                    &RangeResponse::SqlError {
                        code: "54000".into(),
                        message: "one remote SQL row exceeds the transport frame limit".into(),
                    },
                )
                .await?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let overhead = if page.is_empty() {
            let probe = RangeResponse::SqlResultsChunk {
                chunk: WireSqlResultChunk::Rows {
                    result_index,
                    fields: fields.clone(),
                    rows: Vec::new(),
                    tag: None,
                },
            };
            serialize_json_bounded(&probe, SQL_CHUNK_TARGET_BYTES)?.len()
        } else {
            0
        };
        if !page.is_empty() && page_bytes.saturating_add(row_bytes) > SQL_CHUNK_TARGET_BYTES {
            write_row_page(
                writer,
                result_index,
                fields.take(),
                std::mem::take(&mut page),
                None,
            )
            .await?;
            page_bytes = 0;
        }
        if page.is_empty() {
            page_bytes = overhead;
            if page_bytes.saturating_add(row_bytes) > SQL_CHUNK_TARGET_BYTES {
                write_frame(
                    writer,
                    &RangeResponse::SqlError {
                        code: "54000".into(),
                        message: "one remote SQL row exceeds the transport frame limit".into(),
                    },
                )
                .await?;
                return Ok(());
            }
        }
        page_bytes = page_bytes.saturating_add(row_bytes);
        page.push(row);
    }
    write_row_page(writer, result_index, fields.take(), page, Some(tag)).await
}

async fn write_row_page<W>(
    writer: &mut W,
    result_index: u32,
    fields: Option<Vec<WireFieldDescription>>,
    rows: Vec<Vec<Option<WireCell>>>,
    tag: Option<String>,
) -> Result<(), TransportError>
where
    W: AsyncWrite + Unpin,
{
    let response = RangeResponse::SqlResultsChunk {
        chunk: WireSqlResultChunk::Rows {
            result_index,
            fields,
            rows,
            tag,
        },
    };
    match write_frame(writer, &response).await {
        Ok(()) => Ok(()),
        Err(TransportError::FrameTooLarge { .. }) => write_frame(
            writer,
            &RangeResponse::SqlError {
                code: "54000".into(),
                message: "one remote SQL row description or command tag exceeds the transport frame limit"
                    .into(),
            },
        )
        .await,
        Err(error) => Err(error),
    }
}

pub(crate) async fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<(), TransportError>
where
    W: AsyncWrite + Unpin + ?Sized,
    T: Serialize,
{
    let bytes = serialize_json_bounded(value, MAX_FRAME_BYTES)?;
    let len = u32::try_from(bytes.len()).map_err(|_| TransportError::FrameTooLarge {
        actual: bytes.len(),
        limit: MAX_FRAME_BYTES,
    })?;
    writer.write_u32(len).await?;
    writer.write_all(&bytes).await?;
    Ok(())
}

fn serialize_json_bounded<T>(value: &T, limit: usize) -> Result<Vec<u8>, TransportError>
where
    T: Serialize,
{
    struct BoundedWriter {
        bytes: Vec<u8>,
        limit: usize,
        attempted: usize,
    }

    impl std::io::Write for BoundedWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.attempted = self.bytes.len().saturating_add(buf.len());
            if self.attempted > self.limit {
                return Err(std::io::Error::other("bounded JSON output exceeded limit"));
            }
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        limit,
        attempted: 0,
    };
    if let Err(error) = serde_json::to_writer(&mut writer, value) {
        if writer.attempted > limit {
            return Err(TransportError::FrameTooLarge {
                actual: writer.attempted,
                limit,
            });
        }
        return Err(TransportError::Json(error));
    }
    Ok(writer.bytes)
}

async fn read_frame<R, T>(reader: &mut R) -> Result<T, TransportError>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let len = reader.read_u32().await?;
    let len = usize::try_from(len).map_err(|_| TransportError::FrameTooLarge {
        actual: MAX_FRAME_BYTES.saturating_add(1),
        limit: MAX_FRAME_BYTES,
    })?;
    if len > MAX_FRAME_BYTES {
        return Err(TransportError::FrameTooLarge {
            actual: len,
            limit: MAX_FRAME_BYTES,
        });
    }
    let mut bytes = vec![0; len];
    reader.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn timeout<T>(wait: Duration, task: impl Future<Output = T>) -> Result<T, TransportError> {
    tokio::time::timeout(wait, task)
        .await
        .map_err(|_| TransportError::Timeout(wait))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn bounded_json_serialization_rejects_before_allocating_an_oversized_candidate() {
        let oversized = "x".repeat(4 * MAX_FRAME_BYTES);

        let error = serialize_json_bounded(&oversized, MAX_FRAME_BYTES)
            .expect_err("oversized JSON candidate is rejected at the limit");

        assert!(
            matches!(error, TransportError::FrameTooLarge { limit, .. } if limit == MAX_FRAME_BYTES)
        );
    }

    use super::*;

    fn join_request_fixture() -> JoinRangeReq {
        let snapshot = WireSnapshot {
            xmin: 1,
            xmax: 3,
            xip: vec![2],
        };
        JoinRangeReq {
            range_id: RangeId::new(1),
            local_snapshot: snapshot.clone(),
            global_snapshot: snapshot,
            read_ts: 9,
            own_xid: None,
            own_start_ts: None,
            kind: WireJoinKind::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            strategy: WireJoinStrategy::BroadcastRight,
            left: WireJoinTableInterval {
                table_id: 1,
                table_name: "l".into(),
                interval: WireRowInterval {
                    start: None,
                    end: None,
                },
            },
            right: WireJoinTableInterval {
                table_id: 2,
                table_name: "r".into(),
                interval: WireRowInterval {
                    start: None,
                    end: None,
                },
            },
            broadcast_rows: Some(vec![]),
            left_filter: WirePredicatePushdown::FullScan,
            right_filter: WirePredicatePushdown::FullScan,
            projection: vec![0, 1],
        }
    }

    #[test]
    fn join_range_wire_round_trip_preserves_whole_request_and_result() {
        let request = RangeRequest::JoinRange(join_request_fixture());
        let encoded = serde_json::to_vec(&request).expect("encode join request");
        assert_eq!(
            serde_json::from_slice::<RangeRequest>(&encoded).expect("decode join request"),
            request
        );

        let response = RangeResponse::JoinRange(JoinRangeResp {
            rows: vec![JoinRangeRow {
                tuple: vec![3, 1, 4],
            }],
        });
        let encoded = serde_json::to_vec(&response).expect("encode join response");
        assert_eq!(
            serde_json::from_slice::<RangeResponse>(&encoded).expect("decode join response"),
            response
        );
    }

    #[test]
    fn timestamp_bucket_wire_round_trip_distinguishes_absent_zero_and_max() {
        for bucket in [None, Some(0), Some(u32::MAX)] {
            let write = WireTimestampWrite {
                table_id: 7,
                bucket,
                rowid: 11,
                row: vec![WireDatum::Int4(42)],
                delete: false,
            };
            let encoded = serde_json::to_vec(&write).expect("encode timestamp write");
            assert_eq!(
                serde_json::from_slice::<WireTimestampWrite>(&encoded)
                    .expect("decode timestamp write"),
                write
            );

            let operation = WireTimestampOperation {
                range_id: 3,
                table_id: 7,
                bucket,
                rowid: 11,
                delete: false,
            };
            let encoded = serde_json::to_vec(&operation).expect("encode timestamp operation");
            assert_eq!(
                serde_json::from_slice::<WireTimestampOperation>(&encoded)
                    .expect("decode timestamp operation"),
                operation
            );
        }
    }

    #[test]
    fn legacy_timestamp_wire_decodes_as_explicitly_bucketless() {
        let write: WireTimestampWrite =
            serde_json::from_str(r#"{"table_id":7,"rowid":11,"row":[],"delete":false}"#)
                .expect("decode legacy timestamp write");
        assert_eq!(write.bucket, None);

        let operation: WireTimestampOperation =
            serde_json::from_str(r#"{"range_id":3,"table_id":7,"rowid":11,"delete":false}"#)
                .expect("decode legacy timestamp operation");
        assert_eq!(operation.bucket, None);
    }

    #[test]
    fn timestamp_request_encoding_distinguishes_absent_bucket_from_bucket_zero() {
        let operation = WireTimestampOperation {
            range_id: 3,
            table_id: 7,
            bucket: None,
            rowid: 11,
            delete: false,
        };
        let absent = serde_json::to_vec(&operation).expect("encode absent bucket");
        let zero = serde_json::to_vec(&WireTimestampOperation {
            bucket: Some(0),
            ..operation
        })
        .expect("encode bucket zero");
        assert_ne!(absent, zero);
    }

    #[test]
    fn join_range_accepts_near_limit_row_and_rejects_over_limit_row() {
        let mut request = join_request_fixture();
        request.broadcast_rows = Some(vec![JoinRangeRow {
            tuple: vec![0; crabka_pgexec::scanner::MAX_JOIN_ROW_BYTES],
        }]);
        request.validate().expect("near-limit row");
        request.broadcast_rows.as_mut().expect("broadcast")[0]
            .tuple
            .push(0);
        assert!(matches!(
            request.validate(),
            Err(crabka_pgexec::JoinValidationError::JoinRowTooLarge { .. })
        ));
    }

    #[test]
    fn bounded_framing_rejects_oversized_join_request() {
        let mut request = join_request_fixture();
        request.broadcast_rows = Some(vec![JoinRangeRow {
            tuple: vec![0; MAX_FRAME_BYTES],
        }]);
        let error = serialize_json_bounded(&RangeRequest::JoinRange(request), MAX_FRAME_BYTES)
            .expect_err("frame must be bounded");
        assert!(matches!(error, TransportError::FrameTooLarge { .. }));
    }

    #[test]
    fn join_request_transport_capacity_has_exact_materialized_boundary() {
        let mut request = join_request_fixture();
        request.broadcast_rows = Some(vec![JoinRangeRow { tuple: vec![] }]);
        let mut low = 0usize;
        let mut high = MAX_FRAME_BYTES;
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            request.broadcast_rows.as_mut().unwrap()[0]
                .tuple
                .resize(mid, 0);
            if request.fits_transport_frame() {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        request.broadcast_rows.as_mut().unwrap()[0]
            .tuple
            .resize(low, 0);
        assert!(request.fits_transport_frame());
        request.broadcast_rows.as_mut().unwrap()[0].tuple.push(0);
        assert!(!request.fits_transport_frame());
    }

    #[test]
    fn range_peer_principal_cannot_execute_destructive_control() {
        let peers = BTreeSet::from(["range-peer".to_string()]);
        let operators = BTreeSet::from(["operator".to_string()]);
        let control = RangeRequest::Control(RangeControlReq {
            tenant: "tenant-a".into(),
            range_id: RangeId::COORDINATOR,
            generation: 1,
            operation_id: "split-a".into(),
            operation: RangeControlOperation::RetirePredecessor,
        });

        assert!(!principal_authorized_for_request(
            "range-peer",
            &control,
            &peers,
            &operators
        ));
        assert!(principal_authorized_for_request(
            "operator", &control, &peers, &operators
        ));
        assert!(principal_authorized_for_request(
            "range-peer",
            &RangeRequest::Tso(TsoReq::Grant { count: 1 }),
            &peers,
            &operators
        ));
    }

    #[test]
    fn range_control_protocol_roundtrips_every_operation_and_explicit_outcome() {
        let operations = [
            RangeControlOperation::ForceCheckpoint,
            RangeControlOperation::PauseAtCoveredOffset {
                manifest_key: "tenant/r0/checkpoint.json".into(),
                covered_offset: 41,
            },
            RangeControlOperation::Status,
            RangeControlOperation::StageFilteredRestore {
                journal_revision: 3,
                journal_digest: "sealed".into(),
            },
            RangeControlOperation::SuccessorFencePrologue {
                journal_revision: 5,
                journal_digest: "sealed".into(),
            },
            RangeControlOperation::InheritMarkers {
                journal_revision: 4,
                journal_digest: "sealed".into(),
            },
            RangeControlOperation::RetirePredecessor,
            RangeControlOperation::Resume,
        ];
        for operation in operations {
            let request = RangeRequest::Control(RangeControlReq {
                tenant: "tenant-a".into(),
                range_id: RangeId::new(1),
                generation: 9,
                operation_id: "split-42".into(),
                operation,
            });
            let encoded = serde_json::to_vec(&request).expect("encode control request");
            assert_eq!(
                serde_json::from_slice::<RangeRequest>(&encoded).expect("decode control request"),
                request
            );
        }

        let outcomes = [
            RangeControlResp::Applied,
            RangeControlResp::AlreadyApplied,
            RangeControlResp::Ambiguous {
                message: "commit result unknown".into(),
            },
            RangeControlResp::Rejected {
                code: "stale_generation".into(),
                message: "expected 10".into(),
            },
            RangeControlResp::Checkpoint {
                generation: 9,
                covered_offset: 41,
                manifest_key: "manifest".into(),
            },
            RangeControlResp::Paused { barrier_offset: 44 },
            RangeControlResp::Staged {
                tail_sha256: "fixture-sha256".into(),
            },
            RangeControlResp::Status {
                paused: true,
                serving: false,
                barrier_offset: Some(44),
            },
            RangeControlResp::Markers {
                markers: vec![WireInDoubtMarker {
                    transaction_id: 5,
                    key: WireRangeKey {
                        table_id: 7,
                        bucket: None,
                        rowid: 12,
                    },
                }],
                left_markers: Some(vec![]),
                right_markers: Some(vec![]),
                digest: "fixture-digest".into(),
            },
        ];
        for outcome in outcomes {
            let response = RangeResponse::Control(outcome);
            let encoded = serde_json::to_vec(&response).expect("encode control response");
            assert_eq!(
                serde_json::from_slice::<RangeResponse>(&encoded).expect("decode control response"),
                response
            );
        }
    }

    #[test]
    fn legacy_marker_response_decodes_without_successor_partitions() {
        let response: RangeControlResp =
            serde_json::from_str(r#"{"result":"markers","markers":[],"digest":"legacy-digest"}"#)
                .expect("decode durable pre-partition marker receipt");
        assert_eq!(
            response,
            RangeControlResp::Markers {
                markers: vec![],
                left_markers: None,
                right_markers: None,
                digest: "legacy-digest".into(),
            }
        );
    }

    #[derive(Default)]
    struct EchoService {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl RangeService for EchoService {
        async fn handle(&self, request: RangeRequest) -> RangeResponse {
            self.calls.fetch_add(1, Ordering::Relaxed);
            match request {
                RangeRequest::Txn(TxnReq::Barrier { .. }) => RangeResponse::Txn(TxnResp::Barrier {
                    substrate_offset: 42,
                }),
                RangeRequest::Tso(TsoReq::Grant { count }) => {
                    RangeResponse::Tso(TsoResp::Granted {
                        first_ts: 10,
                        count,
                    })
                }
                RangeRequest::ResolveTxn(_request) => {
                    RangeResponse::ResolveTxn(ResolveTxnResp::Pending)
                }
                RangeRequest::Sql { sql, .. } => RangeResponse::Sql { result: sql },
                RangeRequest::ScanRange(request) => RangeResponse::ScanRange(ScanRangeResp {
                    rows: vec![ScanRangeRow {
                        rowid: request.interval.start.unwrap_or(1),
                        xmin: request.local_snapshot.xmin,
                        tuple: vec![1, 2, 3],
                    }],
                }),
                RangeRequest::JoinRange(_) => RangeResponse::Error {
                    error: WireErrorKind::Failed,
                    message: "join execution is not installed".into(),
                },
                RangeRequest::ScanCursor(request) => RangeResponse::ScanCursor(ScanCursorResp {
                    rows: Vec::new(),
                    token: request.token,
                    is_last: true,
                }),
                RangeRequest::SessionOpen { .. }
                | RangeRequest::Session { .. }
                | RangeRequest::SessionClose { .. }
                | RangeRequest::GlobalDecision { .. }
                | RangeRequest::GlobalBegin { .. }
                | RangeRequest::ExplicitGate(_)
                | RangeRequest::RecoverGlobal { .. }
                | RangeRequest::TimestampPrewrite(_)
                | RangeRequest::TimestampPrimaryAck(_)
                | RangeRequest::TimestampResolve(_)
                | RangeRequest::TimestampRecover(_)
                | RangeRequest::Control(_) => RangeResponse::Error {
                    error: WireErrorKind::Failed,
                    message: "wrong rpc".into(),
                },
                RangeRequest::Txn(_) => RangeResponse::Txn(TxnResp::Prepared),
                RangeRequest::TimestampPrimaryRecover(_) => {
                    RangeResponse::TimestampPrimaryDecision {
                        decision: WireTimestampDecision::Aborted,
                        operations: Vec::new(),
                    }
                }
                RangeRequest::TimestampPrimaryInspect(_) => {
                    RangeResponse::TimestampPrimaryOutcome {
                        decision: WirePrimaryTxnDecision::Pending,
                        operations: Vec::new(),
                    }
                }
                RangeRequest::InspectDurableRecords(request) => {
                    RangeResponse::InspectDurableRecords(InspectDurableRecordsResp {
                        records: Vec::new(),
                        next_cursor: request.cursor,
                        provenance: DurableInspectProvenance {
                            sample_offset: 4,
                            wal_generation: request.generation,
                            replay_start_offset: 0,
                            replayed_records: 1,
                            checkpoint_pairs: 0,
                            checkpoint_manifest_key: None,
                            checkpoint_covered_offset: None,
                            checkpoint_journal_seq: None,
                        },
                    })
                }
            }
        }
    }

    struct MtlsFixture {
        _dir: tempfile::TempDir,
        server: RangeTlsServerConfig,
        client: RangeTlsClientConfig,
    }

    impl MtlsFixture {
        fn new(allowed_principals: BTreeSet<String>) -> Self {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let dir = tempfile::tempdir().expect("temporary certificate directory");
            let server_cert = write_fixture(&dir, "server-cert.pem", "dev_cert.pem");
            let server_key = write_fixture(&dir, "server-key.pem", "dev_key.pem");
            let client_ca = write_fixture(&dir, "client-ca.pem", "dev_client_ca.pem");
            let client_cert = write_fixture(&dir, "client-cert.pem", "dev_client_cert.pem");
            let client_key = write_fixture(&dir, "client-key.pem", "dev_client_key.pem");
            let server_tls = crabka_security::TlsConfig {
                cert_chain_path: server_cert.clone(),
                private_key_path: server_key,
                trust_roots_path: Some(server_cert.clone()),
                client_ca_path: Some(client_ca),
                client_auth: crabka_security::ClientAuthMode::Required,
            };
            let client_tls = crabka_security::TlsConfig {
                cert_chain_path: client_cert,
                private_key_path: client_key,
                trust_roots_path: Some(server_cert),
                client_ca_path: None,
                client_auth: crabka_security::ClientAuthMode::Disabled,
            };
            Self {
                _dir: dir,
                server: RangeTlsServerConfig {
                    tenant: "tenant-a".to_string(),
                    tls: server_tls,
                    range_rpc_principals: allowed_principals.clone(),
                    operator_control_principals: allowed_principals,
                },
                client: RangeTlsClientConfig {
                    tls: client_tls,
                    server_name: "crabka-dev".to_string(),
                },
            }
        }
    }

    fn write_fixture(dir: &tempfile::TempDir, name: &str, fixture: &str) -> PathBuf {
        let path = dir.path().join(name);
        let contents: &[u8] = match fixture {
            "dev_cert.pem" => include_bytes!("../../security/tests/fixtures/dev_cert.pem"),
            "dev_key.pem" => include_bytes!("../../security/tests/fixtures/dev_key.pem"),
            "dev_client_ca.pem" => {
                include_bytes!("../../security/tests/fixtures/dev_client_ca.pem")
            }
            "dev_client_cert.pem" => {
                include_bytes!("../../security/tests/fixtures/dev_client_cert.pem")
            }
            "dev_client_key.pem" => {
                include_bytes!("../../security/tests/fixtures/dev_client_key.pem")
            }
            _ => unreachable!("fixture name is fixed by this module"),
        };
        std::fs::write(&path, contents).expect("write certificate fixture");
        path
    }

    async fn spawn_tls(service: Arc<dyn RangeService>, config: RangeTlsServerConfig) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind TLS listener");
        let address = listener.local_addr().expect("TLS listener address");
        tokio::spawn(async move {
            let _ = serve_tls(listener, service, config).await;
        });
        address
    }

    #[tokio::test]
    async fn mtls_allowlisted_principal_executes_sql_and_scan() {
        let fixture = MtlsFixture::new(BTreeSet::from([
            "CN=test-client,OU=integration,O=crabka".to_string()
        ]));
        let address = spawn_tls(Arc::new(EchoService::default()), fixture.server).await;
        let client = FramedTcpClient::with_tls(fixture.client).expect("mTLS client");

        let sql = client
            .call(
                &address.to_string(),
                &RangeRequest::Sql {
                    range_id: RangeId::new(1),
                    sql: "select 1".to_string(),
                },
            )
            .await
            .expect("allowlisted SQL RPC");
        let scan = client
            .call(
                &address.to_string(),
                &RangeRequest::ScanRange(ScanRangeReq {
                    range_id: RangeId::new(1),
                    table_name: "t".to_string(),
                    interval: WireRowInterval {
                        start: None,
                        end: None,
                    },
                    local_snapshot: WireSnapshot {
                        xmin: 1,
                        xmax: 2,
                        xip: vec![],
                    },
                    global_snapshot: WireSnapshot {
                        xmin: 1,
                        xmax: 2,
                        xip: vec![],
                    },
                    own_xid: None,
                    read_ts: None,
                    own_start_ts: None,
                    predicate: WirePredicatePushdown::FullScan,
                    projection: WireProjectionPushdown::All,
                    partial_aggregate: None,
                    top_k: None,
                }),
            )
            .await
            .expect("allowlisted scan RPC");

        assert!(matches!(sql, RangeResponse::Sql { .. }));
        assert!(matches!(scan, RangeResponse::ScanRange(_)));
    }

    struct AppliedControl;

    struct AllowControlIntent;

    #[async_trait]
    impl crate::control::SplitIntentAuthority for AllowControlIntent {
        async fn authorize_request(
            &self,
            _request: &RangeControlReq,
            _context: crate::control::IntentAuthorizationContext,
        ) -> Result<Option<crate::control::AuthorizedSplitIntent>, String> {
            Ok(Some(crate::control::authorized_test_fixture()))
        }
    }

    #[async_trait]
    impl crate::control::RangeControlExecutor for AppliedControl {
        async fn execute(
            &self,
            _request: &RangeControlReq,
            _intent: &crate::control::AuthorizedSplitIntent,
        ) -> RangeControlResp {
            RangeControlResp::Applied
        }
    }

    #[tokio::test]
    async fn mtls_allowlisted_principal_executes_generation_fenced_control() {
        let fixture = MtlsFixture::new(BTreeSet::from([
            "CN=test-client,OU=integration,O=crabka".to_string()
        ]));
        let control = Arc::new(crate::control::GenerationFencedRangeControl::new(
            "tenant-a",
            RangeId::new(1),
            9,
            Box::new(AppliedControl),
            Arc::new(AllowControlIntent),
        ));
        let service =
            crate::forward::HostedRangeService::new(std::collections::BTreeMap::default())
                .with_range_control(control);
        let address = spawn_tls(Arc::new(service), fixture.server).await;
        let client = FramedTcpClient::with_tls(fixture.client).expect("mTLS client");

        let response = client
            .call(
                &address.to_string(),
                &RangeRequest::Control(RangeControlReq {
                    tenant: "tenant-a".into(),
                    range_id: RangeId::new(1),
                    generation: 9,
                    operation_id: "split-a/checkpoint".into(),
                    operation: RangeControlOperation::ForceCheckpoint,
                }),
            )
            .await
            .expect("allowlisted control RPC");

        assert_eq!(response, RangeResponse::Control(RangeControlResp::Applied));
    }

    #[tokio::test]
    async fn mtls_authenticated_nonallowlisted_principal_never_invokes_service() {
        let fixture = MtlsFixture::new(BTreeSet::from(["CN=another-principal".to_string()]));
        let service = Arc::new(EchoService::default());
        let address = spawn_tls(service.clone(), fixture.server).await;
        let client = FramedTcpClient::with_tls(fixture.client).expect("mTLS client");

        let result = client
            .call(
                &address.to_string(),
                &RangeRequest::Sql {
                    range_id: RangeId::new(1),
                    sql: "select 1".to_string(),
                },
            )
            .await;

        assert!(result.is_err());
        assert_eq!(service.calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn plaintext_framing_cannot_invoke_tls_range_service() {
        let fixture = MtlsFixture::new(BTreeSet::from([
            "CN=test-client,OU=integration,O=crabka".to_string()
        ]));
        let service = Arc::new(EchoService::default());
        let address = spawn_tls(service.clone(), fixture.server).await;
        let mut stream = TcpStream::connect(address)
            .await
            .expect("connect plaintext socket");

        write_frame(
            &mut stream,
            &RangeRequest::Sql {
                range_id: RangeId::new(1),
                sql: "select 1".to_string(),
            },
        )
        .await
        .expect("write plaintext frame");
        stream.flush().await.expect("flush plaintext frame");
        let response = read_frame::<_, RangeResponse>(&mut stream).await;

        assert!(response.is_err());
        assert_eq!(service.calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn loopback_transport_round_trips_txn_barrier_offset() {
        let addr = spawn_loopback(Arc::new(EchoService::default()))
            .await
            .unwrap();
        let response = FramedTcpClient::default()
            .call(
                &addr.to_string(),
                &RangeRequest::Txn(TxnReq::Barrier {
                    range_id: RangeId::COORDINATOR,
                }),
            )
            .await
            .unwrap();

        assert_eq!(
            response,
            RangeResponse::Txn(TxnResp::Barrier {
                substrate_offset: 42
            })
        );
    }

    #[tokio::test]
    async fn loopback_transport_round_trips_scan_range_payload() {
        let addr = spawn_loopback(Arc::new(EchoService::default()))
            .await
            .unwrap();
        let response = FramedTcpClient::default()
            .call(
                &addr.to_string(),
                &RangeRequest::ScanRange(ScanRangeReq {
                    range_id: RangeId::new(7),
                    table_name: "t11".to_string(),
                    interval: WireRowInterval {
                        start: Some(9),
                        end: Some(20),
                    },
                    local_snapshot: WireSnapshot {
                        xmin: 5,
                        xmax: 12,
                        xip: vec![8],
                    },
                    global_snapshot: WireSnapshot {
                        xmin: 100,
                        xmax: 120,
                        xip: vec![108],
                    },
                    own_xid: Some(10),
                    read_ts: Some(22),
                    own_start_ts: None,
                    predicate: WirePredicatePushdown::Conjunctive {
                        predicates: vec![WireColumnPredicate {
                            column: 0,
                            op: WirePredicateOp::Ge,
                            value: WireDatum::Int4(3),
                        }],
                    },
                    projection: WireProjectionPushdown::Columns { columns: vec![0] },
                    partial_aggregate: Some(WirePartialAggregateSpec {
                        function: WirePartialAggregateFunction::Count,
                        column: None,
                        group_by: vec![0],
                    }),
                    top_k: Some(WireTopKSpec {
                        order_by: vec![WireTopKColumn {
                            column: 0,
                            asc: true,
                        }],
                        limit: 5,
                    }),
                }),
            )
            .await
            .unwrap();

        assert_eq!(
            response,
            RangeResponse::ScanRange(ScanRangeResp {
                rows: vec![ScanRangeRow {
                    rowid: 9,
                    xmin: 5,
                    tuple: vec![1, 2, 3],
                }]
            })
        );
    }

    #[tokio::test]
    async fn loopback_transport_round_trips_tso_grant() {
        let addr = spawn_loopback(Arc::new(EchoService::default()))
            .await
            .unwrap();
        let response = FramedTcpClient::default()
            .call(
                &addr.to_string(),
                &RangeRequest::Tso(TsoReq::Grant { count: 7 }),
            )
            .await
            .unwrap();

        assert_eq!(
            response,
            RangeResponse::Tso(TsoResp::Granted {
                first_ts: 10,
                count: 7
            })
        );
    }

    #[tokio::test]
    async fn loopback_transport_round_trips_resolve_txn_pending() {
        let addr = spawn_loopback(Arc::new(EchoService::default()))
            .await
            .unwrap();
        let response = FramedTcpClient::default()
            .call(
                &addr.to_string(),
                &RangeRequest::ResolveTxn(ResolveTxnReq {
                    primary_range: RangeId::new(7),
                    start_ts: 42,
                }),
            )
            .await
            .unwrap();

        assert_eq!(response, RangeResponse::ResolveTxn(ResolveTxnResp::Pending));
    }

    #[tokio::test]
    async fn silent_peer_times_out_instead_of_hanging() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_mins(1)).await;
        });

        let error = FramedTcpClient::with_timeout(Duration::from_millis(20))
            .call(
                &addr.to_string(),
                &RangeRequest::Sql {
                    range_id: RangeId::COORDINATOR,
                    sql: "select 1".to_string(),
                },
            )
            .await
            .expect_err("silent peer must timeout");

        assert!(matches!(error, TransportError::Timeout(_)));
    }

    #[tokio::test]
    async fn durable_inspection_round_trips_without_exceeding_frame_bound() {
        let request = InspectDurableRecordsReq {
            tenant: "tenant-a".into(),
            range_id: RangeId::new(2),
            generation: 7,
            table_id: 50,
            start_key: crabka_pgkv::key::table_prefix(50),
            end_key: {
                let mut end = crabka_pgkv::key::table_prefix(50);
                end.push(0xff);
                end
            },
            max_records: MAX_DURABLE_INSPECT_RECORDS,
            max_bytes: MAX_DURABLE_INSPECT_BYTES,
            snapshot_offset: None,
            cursor: Some("cursor".into()),
        };
        let encoded = serialize_json_bounded(
            &RangeRequest::InspectDurableRecords(request.clone()),
            MAX_FRAME_BYTES,
        )
        .expect("bounded request");
        assert_eq!(
            serde_json::from_slice::<RangeRequest>(&encoded).expect("request decode"),
            RangeRequest::InspectDurableRecords(request.clone())
        );
        let addr = spawn_loopback(Arc::new(EchoService::default()))
            .await
            .unwrap();
        let response = FramedTcpClient::default()
            .call(
                &addr.to_string(),
                &RangeRequest::InspectDurableRecords(request),
            )
            .await
            .expect("inspection response");
        assert!(matches!(response, RangeResponse::InspectDurableRecords(_)));
    }

    #[test]
    fn durable_inspection_uses_range_acl_not_destructive_peer_acl() {
        let request = RangeRequest::InspectDurableRecords(InspectDurableRecordsReq {
            tenant: "tenant-a".into(),
            range_id: RangeId::new(2),
            generation: 1,
            table_id: 9,
            start_key: vec![1],
            end_key: vec![2],
            max_records: 1,
            max_bytes: 1,
            snapshot_offset: None,
            cursor: None,
        });
        let range = BTreeSet::from(["range-peer".to_string()]);
        let operator = BTreeSet::from(["operator".to_string()]);
        assert!(principal_authorized_for_request(
            "range-peer",
            &request,
            &range,
            &operator
        ));
        assert!(!principal_authorized_for_request(
            "operator", &request, &range, &operator
        ));
        assert!(!principal_authorized_for_request(
            "stranger", &request, &range, &operator
        ));
    }
}
