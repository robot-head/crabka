//! Framed range-compute transport for SQL forwarding and transaction RPC.

#[cfg(test)]
use std::net::SocketAddr;
use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Instant,
};

use async_trait::async_trait;
use crabka_pgwire::engine::{ResultPage, ResultSink};
use crabka_trace_context::TraceCarrier;
use crabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, StdDurationExt as _, TimeExt as _},
    fmt::Human as _,
    kibibytes, mebibytes,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::Instrument as _;

use crate::{RangeId, telemetry};

/// Hard limit on one encoded frame, enforced by both the encoder and the
/// decoder.
const MAX_FRAME: ByteSize = mebibytes(1);

/// Bytes of [`MAX_FRAME`] reserved for what [`RangeEnvelope`] adds around a
/// request.
///
/// A W3C `traceparent` is 55 bytes and `crabka-trace-context` caps `tracestate`
/// at 512; the rest is the envelope's own JSON keys and quoting. Callers that
/// size a payload against the frame limit — [`JoinRangeReq::fits_transport_frame`]
/// is the one that matters — must subtract this, or a request sized to exactly
/// one frame turns a clean domain error into a [`TransportError::FrameTooLarge`]
/// the moment it is wrapped.
const ENVELOPE_RESERVE: ByteSize = kibibytes(1);

/// One request as it travels on the wire: the caller's trace context alongside
/// the request itself.
///
/// Private on purpose, constructed where a frame is written and destructured
/// where one is read, so neither the [`FramedTcpClient`] call sites nor any
/// [`RangeService`] implementation knows it exists.
///
/// `request` is a [`Cow`] so the write path borrows the caller's request rather
/// than deep-cloning it — a `JoinRange` payload runs to megabytes, and this is
/// the hot path for every cross-node scan. Decoding always yields
/// [`Cow::Owned`].
///
/// [`PartialEq`] is deliberately not derived. `RangeRequest` equality must stay
/// a pure function of the payload, and derived envelope equality would make two
/// identical requests compare unequal because they were traced differently.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RangeEnvelope<'a> {
    /// Omitted entirely when nothing is being traced, which is the common case
    /// and the hot path. A payload-size optimisation, **not** a compatibility
    /// shim — Crabka keeps none, see `CLAUDE.md`.
    #[serde(default, skip_serializing_if = "TraceCarrier::is_empty")]
    trace: TraceCarrier,
    request: Cow<'a, RangeRequest>,
}

impl<'a> RangeEnvelope<'a> {
    /// Wrap `request` with the trace context of the currently active span.
    ///
    /// The carrier is empty when nothing is being traced — no active span, an
    /// unsampled one, or OTLP switched off — which costs two `None`s and
    /// serialises to nothing.
    fn outgoing(request: &'a RangeRequest) -> Self {
        Self {
            trace: TraceCarrier::capture_current(),
            request: Cow::Borrowed(request),
        }
    }
}

/// `rpc.system` for every span this module emits. Constant, and the attribute an
/// operator filters on to isolate range RPC from the rest of the gateway tier.
const RPC_SYSTEM: &str = "crabka.range";

/// The `rpc.method` for a request: its [`RangeRequest`] variant name.
///
/// A closed set of `&'static str`, so the attribute stays low-cardinality and
/// costs nothing to derive on a disabled callsite.
const fn request_method(request: &RangeRequest) -> &'static str {
    match request {
        RangeRequest::Sql { .. } => "Sql",
        RangeRequest::Ddl { .. } => "Ddl",
        RangeRequest::Range0Barrier => "Range0Barrier",
        RangeRequest::SessionOpen { .. } => "SessionOpen",
        RangeRequest::Session { .. } => "Session",
        RangeRequest::SessionClose { .. } => "SessionClose",
        RangeRequest::GlobalDecision { .. } => "GlobalDecision",
        RangeRequest::GlobalBegin { .. } => "GlobalBegin",
        RangeRequest::RecoverGlobal { .. } => "RecoverGlobal",
        RangeRequest::ScanRange(_) => "ScanRange",
        RangeRequest::JoinRange(_) => "JoinRange",
        RangeRequest::ScanCursor(_) => "ScanCursor",
        RangeRequest::Txn(_) => "Txn",
        RangeRequest::Tso(_) => "Tso",
        RangeRequest::ResolveTxn(_) => "ResolveTxn",
        RangeRequest::TimestampPrewrite(_) => "TimestampPrewrite",
        RangeRequest::TimestampPrimaryAck(_) => "TimestampPrimaryAck",
        RangeRequest::TimestampResolve(_) => "TimestampResolve",
        RangeRequest::TimestampRecover(_) => "TimestampRecover",
        RangeRequest::TimestampPrimaryRecover(_) => "TimestampPrimaryRecover",
        RangeRequest::TimestampPrimaryInspect(_) => "TimestampPrimaryInspect",
        RangeRequest::InspectDurableRecords(_) => "InspectDurableRecords",
        RangeRequest::Control(_) => "Control",
    }
}

/// The range a request names, when it names one.
///
/// `None` for the requests addressed to a node rather than to a range — the
/// timestamp-oracle and primary-authenticated recovery RPCs, which carry a
/// transaction identity instead.
fn request_range_id(request: &RangeRequest) -> Option<RangeId> {
    match request {
        RangeRequest::Sql { range_id, .. }
        | RangeRequest::SessionOpen { range_id, .. }
        | RangeRequest::Session { range_id, .. }
        | RangeRequest::SessionClose { range_id, .. }
        | RangeRequest::GlobalDecision { range_id, .. }
        | RangeRequest::GlobalBegin { range_id }
        | RangeRequest::RecoverGlobal { range_id, .. } => Some(*range_id),
        RangeRequest::Ddl { .. } | RangeRequest::Range0Barrier => Some(RangeId::COORDINATOR),
        RangeRequest::ScanRange(request) => Some(request.range_id),
        RangeRequest::JoinRange(request) => Some(request.range_id),
        RangeRequest::ScanCursor(request) => Some(request.scan.range_id),
        RangeRequest::TimestampPrewrite(request) => Some(request.range_id),
        RangeRequest::TimestampResolve(request) => Some(request.range_id),
        RangeRequest::TimestampRecover(request) => Some(request.range_id),
        RangeRequest::Control(request) => Some(request.range_id),
        RangeRequest::Txn(request) => Some(match request {
            TxnReq::Prepare { range_id, .. }
            | TxnReq::Commit { range_id, .. }
            | TxnReq::Abort { range_id, .. }
            | TxnReq::Barrier { range_id } => *range_id,
        }),
        RangeRequest::Tso(_)
        | RangeRequest::ResolveTxn(_)
        | RangeRequest::TimestampPrimaryAck(_)
        | RangeRequest::TimestampPrimaryRecover(_)
        | RangeRequest::TimestampPrimaryInspect(_)
        | RangeRequest::InspectDurableRecords(_) => None,
    }
}

/// The `error.type` for a failed RPC: the [`TransportError`] discriminant.
///
/// A transport failure has no SQLSTATE — `TransportError::Sql` is the one
/// variant that carries one, and it is the peer's, already visible in the
/// status description. The discriminant is the low-cardinality thing to group
/// by, which is what `error.type` is for.
const fn transport_error_kind(error: &TransportError) -> &'static str {
    match error {
        TransportError::FrameTooLarge { .. } => "frame_too_large",
        TransportError::Json(_) => "json",
        TransportError::Io(_) => "io",
        TransportError::Timeout(_) => "timeout",
        TransportError::Remote { .. } => "remote",
        TransportError::Sql { .. } => "sql",
        TransportError::UnexpectedResponse => "unexpected_response",
        TransportError::Protocol(_) => "protocol",
        TransportError::Tls(_) => "tls",
        TransportError::UnauthorizedPeer { .. } => "unauthorized_peer",
    }
}

/// Record `error` on `span` under the `OTel` status contract.
fn record_rpc_error(span: &tracing::Span, error: &TransportError) {
    telemetry::record_error(span, transport_error_kind(error), &error.to_string());
}

/// Frame bytes moved by one client RPC.
///
/// Accumulated by the framing loop and recorded once when it returns. A span per
/// result page would emit hundreds of spans for one large scan, all of which the
/// exporter would drop; two counters answer the same question.
#[derive(Debug, Default)]
struct RpcBytes {
    request: usize,
    response: usize,
}

impl RpcBytes {
    fn record(&self, span: &tracing::Span) {
        if span.is_disabled() {
            return;
        }
        span.record("pg.request_bytes", telemetry::integer(self.request));
        span.record("pg.response_bytes", telemetry::integer(self.response));
    }
}

/// Build the client span covering one range RPC.
///
/// Every field is a constant or a field read, so the callsite needs no
/// `enabled!` guard. Building it here rather than at the ~two dozen call sites
/// is what keeps the change small — and what lets
/// [`TraceCarrier::capture_current`] inside the framing loop pick up a
/// client-kind parent without any of those callers knowing.
fn range_rpc_span(endpoint: &str, request: &RangeRequest) -> tracing::Span {
    let method = request_method(request);
    let span = tracing::debug_span!(
        target: telemetry::ROUTE_TARGET,
        "gres.range_rpc",
        otel.kind = "client",
        otel.name = method,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        error.type = tracing::field::Empty,
        rpc.system = RPC_SYSTEM,
        rpc.method = method,
        server.address = endpoint,
        pg.range_id = tracing::field::Empty,
        pg.request_bytes = tracing::field::Empty,
        pg.response_bytes = tracing::field::Empty,
        pg.pooled_connection = tracing::field::Empty,
    );
    if let Some(range_id) = request_range_id(request) {
        span.record("pg.range_id", telemetry::integer(range_id.as_u32()));
    }
    span
}

/// The authenticated identity of one served connection, recorded on every
/// `gres.range_serve` span it produces.
///
/// The peer certificate is validated once when the connection is established,
/// so the principal is constant for the connection's lifetime and is carried
/// here rather than re-extracted per request.
#[derive(Debug, Default)]
struct ServePeer {
    principal: String,
    tenant: String,
}

/// Build the server span covering one served range RPC.
///
/// The caller makes this the child of the client's `gres.range_rpc` span with
/// [`TraceCarrier::apply_to`], which is the hop that makes a distributed trace
/// distributed.
fn range_serve_span(request: &RangeRequest, peer: &ServePeer) -> tracing::Span {
    let method = request_method(request);
    let span = tracing::debug_span!(
        target: telemetry::ROUTE_TARGET,
        "gres.range_serve",
        otel.kind = "server",
        otel.name = method,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        error.type = tracing::field::Empty,
        rpc.system = RPC_SYSTEM,
        rpc.method = method,
        pg.principal = peer.principal.as_str(),
        pg.tenant = peer.tenant.as_str(),
        pg.range_id = tracing::field::Empty,
        pg.response_bytes = tracing::field::Empty,
    );
    if let Some(range_id) = request_range_id(request) {
        span.record("pg.range_id", telemetry::integer(range_id.as_u32()));
    }
    span
}

/// A stream that counts the bytes written through it.
///
/// Wrapped around the response half of one served request so `pg.response_bytes`
/// covers the streaming SQL path too, where the service writes result pages
/// straight to the socket. Counting here rather than at each write site is what
/// lets `gres.range_serve` report the whole response without a span per page.
struct CountingStream<'a, S> {
    inner: &'a mut S,
    written: usize,
}

impl<S> AsyncRead for CountingStream<'_, S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.inner).poll_read(context, buffer)
    }
}

impl<S> AsyncWrite for CountingStream<'_, S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let written = std::task::ready!(Pin::new(&mut *self.inner).poll_write(context, buffer));
        if let Ok(written) = &written {
            self.written = self.written.saturating_add(*written);
        }
        Poll::Ready(written)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.inner).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.inner).poll_shutdown(context)
    }
}

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
    /// Execute one data-definition statement on the range-0 catalog owner.
    Ddl { sql: String },
    /// Wait until this node's range-0 catalog view covers every write
    /// committed before this request was sent.
    Range0Barrier,
    /// Open one owner-side connection session.
    SessionOpen {
        range_id: RangeId,
        /// The originating connection's backend pid, when it holds a seat on
        /// its gateway's notification bus. The owner-side session adopts it so
        /// a forwarded `NOTIFY` is stamped with the pid `PostgreSQL` would
        /// report, not the owner session's own.
        #[serde(default)]
        notify_pid: Option<i32>,
    },
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
    /// The node's range-0 catalog view covers every prior committed write.
    Range0Barriered,
    /// A newly allocated owner session.
    SessionOpened { session_id: u64 },
    /// Result of one stateful owner-session operation.
    SessionResult { result: WireSessionResult },
    /// Effective immutable global decision status.
    GlobalStatus { status: WireGlobalStatus },
    /// Newly allocated global transaction id.
    GlobalXid { global_xid: u64 },
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
    InspectDurableRecords(Box<InspectDurableRecordsResp>),
    /// Explicit result of a split control operation.
    Control(RangeControlResp),
    /// Range compute rejected the request.
    Error {
        error: WireErrorKind,
        message: String,
    },
}

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
        /// Bound, in milliseconds, on every lock wait this statement performs.
        /// `Some` only for statements of a gateway transaction that has
        /// escalated past one range — the only sessions a cross-engine
        /// deadlock cycle can enlist; `None` keeps exact engine-local
        /// blocking for single-range and autocommit forwarding.
        lock_wait_cap_ms: Option<u64>,
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
        /// See [`WireSessionOperation::SimpleQuery::lock_wait_cap_ms`].
        lock_wait_cap_ms: Option<u64>,
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

    /// Validate against limits owned by the receiving process.
    ///
    /// # Errors
    /// Returns an error when the request exceeds or violates the policy.
    pub fn validate_with_policy(
        &self,
        policy: crabka_pgexec::scanner::JoinPolicy,
    ) -> Result<(), crabka_pgexec::JoinValidationError> {
        self.to_pgexec().validate_with_policy(policy)
    }

    /// Whether the fully materialized request, including enum/request JSON
    /// overhead, fits the production bounded frame.
    ///
    /// Sized against the frame limit less a reserve, because the frame the
    /// request actually rides in also carries the caller's trace context.
    /// Ignoring that reserve would let a join sized to exactly one frame pass
    /// here and then fail on the wire with a
    /// [`TransportError::FrameTooLarge`] instead of the planner's
    /// `JoinValidationError`.
    #[must_use]
    pub fn fits_transport_frame(&self) -> bool {
        let limit = (MAX_FRAME - ENVELOPE_RESERVE).bytes_usize();
        serde_json::to_vec(&RangeRequest::JoinRange(self.clone()))
            .is_ok_and(|bytes| bytes.len() <= limit)
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
    ///
    /// Both counts stay raw: they are measured buffer lengths, and one site
    /// reports a result-index overflow against `u32::MAX` rather than a byte
    /// magnitude. The dimensioned limit is [`MAX_FRAME`].
    #[error("range frame too large: {actual} bytes exceeds {limit}")]
    FrameTooLarge { actual: usize, limit: usize },
    /// JSON payload was invalid.
    #[error("range frame json error: {0}")]
    Json(#[from] serde_json::Error),
    /// Socket IO failed.
    #[error("range transport io error: {0}")]
    Io(#[from] std::io::Error),
    /// The peer was silent past the configured deadline.
    #[error("range transport timed out after {}", .0.human())]
    Timeout(Time),
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
    ///
    /// Returning `Ok(None)` asserts the implementation wrote one complete
    /// framed response stream (single frame, or SQL chunks ending with a
    /// terminal [`RangeResponse::SqlResultsDone`]/[`RangeResponse::SqlError`]
    /// frame) and left the stream at a frame boundary, so the transport keeps
    /// the connection alive for the peer's next request.
    async fn handle_connection(
        &self,
        request: RangeRequest,
        _writer: &mut (dyn AsyncWrite + Unpin + Send),
    ) -> Result<Option<RangeResponse>, TransportError> {
        Ok(Some(self.handle(request).await))
    }
}

/// Authenticated client for framed TLS range RPC.
///
/// Established connections are pooled per endpoint and reused across calls;
/// clones share one pool, so cloning this client is cheap and preserves reuse.
/// A connection returns to the pool only after its response was fully
/// consumed; any error, timeout, or partial read drops the connection and
/// surfaces the error unchanged — the client never retries on its own.
#[derive(Debug, Clone)]
pub struct FramedTcpClient {
    timeout: Time,
    max_frame: ByteSize,
    idle_ttl: Time,
    max_idle_per_endpoint: usize,
    mode: RangeClientMode,
    pool: Arc<ConnectionPool>,
}

#[derive(Debug, Clone)]
enum RangeClientMode {
    Tls {
        config: Arc<rustls::ClientConfig>,
        server_name: String,
    },
    #[cfg(test)]
    Plaintext,
}

/// One established client stream that can be parked in the connection pool.
enum RangeStream {
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
    #[cfg(test)]
    Plaintext(TcpStream),
}

impl RangeStream {
    fn tcp(&self) -> &TcpStream {
        match self {
            Self::Tls(stream) => stream.get_ref().0,
            #[cfg(test)]
            Self::Plaintext(stream) => stream,
        }
    }

    /// Non-blocking liveness probe on the underlying socket.
    ///
    /// This protocol never carries unsolicited server bytes, so any pending
    /// input while the connection is idle means the server closed the stream
    /// (EOF or TLS `close_notify`) or an error is pending, and the caller must
    /// discard the connection instead of reusing it. The probe uses a direct
    /// non-blocking `try_read` rather than tokio readiness (which caches a
    /// stale readable flag after a fully drained read): consuming a byte is
    /// harmless because every branch that observes input discards the
    /// connection, and `WouldBlock` — the only branch that permits reuse —
    /// consumes nothing.
    fn dead_while_idle(&self) -> bool {
        let mut scratch = [0_u8; 1];
        match self.tcp().try_read(&mut scratch) {
            Ok(_) => true,
            Err(error) => error.kind() != std::io::ErrorKind::WouldBlock,
        }
    }
}

impl AsyncRead for RangeStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
            #[cfg(test)]
            Self::Plaintext(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for RangeStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
            #[cfg(test)]
            Self::Plaintext(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
            #[cfg(test)]
            Self::Plaintext(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
            #[cfg(test)]
            Self::Plaintext(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// One pooled connection and the instant it last finished a call.
struct PooledConn {
    stream: RangeStream,
    last_used: Instant,
}

/// Idle connections shared by every clone of one [`FramedTcpClient`],
/// partitioned by the runtime that dialed them.
///
/// Tokio sockets are registered with the IO driver of the runtime that
/// created them and error once that runtime shuts down. Several engine paths
/// (the blocking [`crabka_pgexec::RangeScanner`] entry points, bounded cursor
/// collectors, timestamp-session cleanup) run range RPCs on short-lived
/// single-call runtimes, so a connection must only ever be reused by the
/// runtime that dialed it. Ephemeral runtimes therefore get no reuse — the
/// same behavior as before pooling existed — while long-lived runtimes reap
/// the full benefit. Expired entries are swept on every check-in so sockets
/// parked by runtimes that never return cannot accumulate.
#[derive(Default)]
struct ConnectionPool {
    idle: Mutex<HashMap<tokio::runtime::Id, HashMap<String, Vec<PooledConn>>>>,
}

type PoolGuard<'a> =
    std::sync::MutexGuard<'a, HashMap<tokio::runtime::Id, HashMap<String, Vec<PooledConn>>>>;

impl std::fmt::Debug for ConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionPool").finish_non_exhaustive()
    }
}

impl ConnectionPool {
    fn lock(&self) -> PoolGuard<'_> {
        self.idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Pop the most recently parked healthy connection this runtime dialed
    /// for `endpoint`.
    ///
    /// Connections idle past `idle_ttl`, and connections whose socket became
    /// readable while parked (the server closed or reset the stream), are
    /// dropped instead of returned.
    fn take(&self, endpoint: &str, idle_ttl: Time) -> Option<RangeStream> {
        let runtime = tokio::runtime::Handle::current().id();
        let mut idle = self.lock();
        let conns = idle.get_mut(&runtime)?.get_mut(endpoint)?;
        while let Some(conn) = conns.pop() {
            if conn.last_used.elapsed().as_time() > idle_ttl {
                continue;
            }
            if conn.stream.dead_while_idle() {
                continue;
            }
            return Some(conn.stream);
        }
        None
    }

    /// Park a connection whose response was fully consumed, and sweep
    /// expired connections across every runtime partition.
    ///
    /// When the endpoint already holds `max_idle` parked connections the
    /// overflow connection is dropped; check-in never blocks.
    fn put(&self, endpoint: &str, stream: RangeStream, max_idle: usize, idle_ttl: Time) {
        let runtime = tokio::runtime::Handle::current().id();
        let mut idle = self.lock();
        idle.retain(|_, endpoints| {
            endpoints.retain(|_, conns| {
                conns.retain(|conn| conn.last_used.elapsed().as_time() <= idle_ttl);
                !conns.is_empty()
            });
            !endpoints.is_empty()
        });
        let conns = idle
            .entry(runtime)
            .or_default()
            .entry(endpoint.to_string())
            .or_default();
        if conns.len() < max_idle {
            conns.push(PooledConn {
                stream,
                last_used: Instant::now(),
            });
        }
    }
}

/// Plaintext range transport exists only inside this crate's unit tests.
///
/// It is deliberately not exported from production builds: every production
/// range RPC must present an mTLS identity and verify its peer.
#[cfg(test)]
impl Default for FramedTcpClient {
    fn default() -> Self {
        Self::with_timeout(crate::RangeRuntimePolicy::default().rpc_request_timeout)
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
        Ok(Self::with_mode(RangeClientMode::Tls {
            config,
            server_name,
        }))
    }

    /// Build a plaintext client with an explicit wire-silence timeout for unit tests.
    #[cfg(test)]
    #[must_use]
    pub fn with_timeout(timeout: Time) -> Self {
        Self {
            timeout,
            ..Self::with_mode(RangeClientMode::Plaintext)
        }
    }

    /// Override pool tuning knobs for unit tests.
    #[cfg(test)]
    #[must_use]
    pub fn with_pool_tuning(mut self, idle_ttl: Time, max_idle_per_endpoint: usize) -> Self {
        self.idle_ttl = idle_ttl;
        self.max_idle_per_endpoint = max_idle_per_endpoint;
        self
    }

    /// Build a TLS-only forwarding client. This path always presents a client
    /// identity and validates the remote certificate and SNI name.
    ///
    /// The TLS connector configuration (certificate parsing included) is built
    /// once here and shared by every subsequent dial.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn with_tls(config: RangeTlsClientConfig) -> Result<Self, TransportError> {
        Self::with_tls_and_policy(config, &crate::RangeRuntimePolicy::default())
    }

    /// Build a TLS forwarding client with explicit runtime policy.
    /// # Errors
    /// Returns an error when TLS configuration is invalid.
    pub fn with_tls_and_policy(
        config: RangeTlsClientConfig,
        policy: &crate::RangeRuntimePolicy,
    ) -> Result<Self, TransportError> {
        if config.tls.trust_roots_path.is_none() {
            return Err(TransportError::Tls(
                "range TLS requires a server trust CA".to_string(),
            ));
        }
        if config.server_name.trim().is_empty() {
            return Err(TransportError::Tls(
                "range TLS requires a non-empty server name".to_string(),
            ));
        }
        let client_config = config
            .tls
            .build_client_config_with_identity()
            .map_err(|error| TransportError::Tls(error.to_string()))?;
        Ok(Self::with_mode_and_policy(
            RangeClientMode::Tls {
                config: client_config,
                server_name: config.server_name,
            },
            policy,
        ))
    }

    fn with_mode(mode: RangeClientMode) -> Self {
        Self::with_mode_and_policy(mode, &crate::RangeRuntimePolicy::default())
    }

    fn with_mode_and_policy(mode: RangeClientMode, policy: &crate::RangeRuntimePolicy) -> Self {
        Self {
            timeout: policy.rpc_request_timeout,
            max_frame: policy.rpc_frame_max,
            idle_ttl: policy.rpc_pool_idle_ttl,
            max_idle_per_endpoint: policy.rpc_pool_max_idle_per_endpoint.get(),
            mode,
            pool: Arc::default(),
        }
    }

    /// Send one request and await one response.
    ///
    /// Reuses a pooled connection to `endpoint` when a healthy one is parked,
    /// dialing (and handshaking) a fresh connection otherwise. The connection
    /// returns to the pool only after the response was fully consumed; any
    /// error drops it and surfaces unchanged.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn call(
        &self,
        endpoint: &str,
        request: &RangeRequest,
    ) -> Result<RangeResponse, TransportError> {
        let span = range_rpc_span(endpoint, request);
        let outcome = self
            .call_traced(endpoint, request)
            .instrument(span.clone())
            .await;
        if let Err(error) = &outcome {
            record_rpc_error(&span, error);
        }
        outcome
    }

    /// The body of [`FramedTcpClient::call`], run inside its `gres.range_rpc`
    /// span so the trace context captured while framing names that span as the
    /// remote parent.
    async fn call_traced(
        &self,
        endpoint: &str,
        request: &RangeRequest,
    ) -> Result<RangeResponse, TransportError> {
        let (mut stream, pooled) = self.checkout(endpoint).await?;
        tracing::Span::current().record("pg.pooled_connection", pooled);
        let response = call_stream(&mut stream, request, self.timeout, self.max_frame).await?;
        self.pool
            .put(endpoint, stream, self.max_idle_per_endpoint, self.idle_ttl);
        Ok(response)
    }

    /// Send one SQL request and forward bounded result pages as they arrive.
    ///
    /// Connection reuse matches [`FramedTcpClient::call`]: the connection is
    /// pooled only after the final SQL chunk was consumed.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn call_sql_into(
        &self,
        endpoint: &str,
        request: &RangeRequest,
        sink: &mut dyn ResultSink,
    ) -> Result<(), TransportError> {
        let span = range_rpc_span(endpoint, request);
        let outcome = self
            .call_sql_into_traced(endpoint, request, sink)
            .instrument(span.clone())
            .await;
        if let Err(error) = &outcome {
            record_rpc_error(&span, error);
        }
        outcome
    }

    /// The body of [`FramedTcpClient::call_sql_into`], run inside its
    /// `gres.range_rpc` span.
    async fn call_sql_into_traced(
        &self,
        endpoint: &str,
        request: &RangeRequest,
        sink: &mut dyn ResultSink,
    ) -> Result<(), TransportError> {
        let (mut stream, pooled) = self.checkout(endpoint).await?;
        tracing::Span::current().record("pg.pooled_connection", pooled);
        call_sql_stream_into(&mut stream, request, self.timeout, self.max_frame, sink).await?;
        self.pool
            .put(endpoint, stream, self.max_idle_per_endpoint, self.idle_ttl);
        Ok(())
    }

    /// Take a healthy pooled connection or dial and handshake a fresh one.
    ///
    /// The flag says which happened, and becomes `pg.pooled_connection`: a
    /// dialled connection pays a TCP and TLS handshake the pooled one does not,
    /// which is usually the whole explanation for an outlier RPC.
    async fn checkout(&self, endpoint: &str) -> Result<(RangeStream, bool), TransportError> {
        if let Some(stream) = self.pool.take(endpoint, self.idle_ttl) {
            return Ok((stream, true));
        }
        self.dial(endpoint).await.map(|stream| (stream, false))
    }

    async fn dial(&self, endpoint: &str) -> Result<RangeStream, TransportError> {
        let stream = timeout(self.timeout, TcpStream::connect(endpoint)).await??;
        // Persistent request/response connections interact badly with
        // Nagle + delayed ACK (a ~40 ms stall per reused-connection round
        // trip); RPC frames are latency-critical, so flush segments eagerly.
        stream.set_nodelay(true)?;
        match &self.mode {
            RangeClientMode::Tls {
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
                    .await?
                    .map_err(|error| TransportError::Tls(error.to_string()))?;
                Ok(RangeStream::Tls(Box::new(stream)))
            }
            #[cfg(test)]
            RangeClientMode::Plaintext => Ok(RangeStream::Plaintext(stream)),
        }
    }
}

async fn call_sql_stream_into<S>(
    stream: &mut S,
    request: &RangeRequest,
    wait: Time,
    max_frame: ByteSize,
    sink: &mut dyn ResultSink,
) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut bytes = RpcBytes::default();
    let outcome = call_sql_stream_frames(stream, request, wait, max_frame, sink, &mut bytes).await;
    bytes.record(&tracing::Span::current());
    outcome
}

async fn call_sql_stream_frames<S>(
    stream: &mut S,
    request: &RangeRequest,
    wait: Time,
    max_frame: ByteSize,
    sink: &mut dyn ResultSink,
    bytes: &mut RpcBytes,
) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let envelope = RangeEnvelope::outgoing(request);
    bytes.request = timeout(wait, write_frame_counted(stream, &envelope, max_frame)).await??;
    timeout(wait, stream.flush()).await??;
    loop {
        let (response, frame_bytes) =
            timeout(wait, read_frame_counted(stream, max_frame)).await??;
        bytes.response = bytes.response.saturating_add(frame_bytes);
        match response {
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
        let (mut stream, _) = listener.accept().await?;
        let _ = stream.set_nodelay(true);
        let service = Arc::downgrade(&service);
        tokio::spawn(async move {
            if let Err(error) = serve_frames(&mut stream, &service, |_| Ok(())).await {
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
    serve_tls_with_policy(
        listener,
        service,
        config,
        crate::RangeRuntimePolicy::default(),
    )
    .await
}

/// Serve TLS range RPCs with explicit runtime policy.
/// # Errors
/// Returns an error when the listener cannot accept or serve a connection.
pub async fn serve_tls_with_policy(
    listener: TcpListener,
    service: Arc<dyn RangeService>,
    config: RangeTlsServerConfig,
    policy: crate::RangeRuntimePolicy,
) -> Result<(), TransportError> {
    let acceptor = config.build_acceptor()?;
    loop {
        let (stream, _) = listener.accept().await?;
        let _ = stream.set_nodelay(true);
        let service = Arc::downgrade(&service);
        let acceptor = acceptor.clone();
        let range_rpc_principals = config.range_rpc_principals.clone();
        let operator_control_principals = config.operator_control_principals.clone();
        let tenant = config.tenant.clone();
        let server_idle_timeout = policy.rpc_server_idle_timeout;
        let max_frame = policy.rpc_frame_max;
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
                let peer = ServePeer {
                    principal: principal.clone(),
                    tenant: tenant.clone(),
                };
                serve_frames_with_idle_timeout(
                    &mut stream,
                    &service,
                    server_idle_timeout,
                    max_frame,
                    &peer,
                    |request: &RangeRequest| {
                        if principal_authorized_for_request(
                            &principal,
                            request,
                            &range_rpc_principals,
                            &operator_control_principals,
                        ) {
                            Ok(())
                        } else {
                            Err(TransportError::UnauthorizedPeer {
                                tenant: tenant.clone(),
                            })
                        }
                    },
                )
                .await
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(%error, "range TLS transport connection rejected");
            }
        });
    }
}

/// Serve framed requests on one authenticated stream until the peer
/// disconnects, the idle deadline lapses, the service shuts down, or a
/// request fails.
///
/// The peer certificate was authenticated once when the connection was
/// established; `authorize` re-checks the stored principal against every
/// decoded request, since authorization is request-type-dependent.
///
/// A peer that disconnects while the server is waiting for the next frame is
/// a normal end of a kept-alive connection and closes without error; pooled
/// clients drop idle connections this way as a matter of course.
///
/// The connection holds only a weak service reference while parked between
/// requests, so aborting the accept loop releases the service — and the
/// storage handles it owns — deterministically instead of pinning them until
/// every kept-alive peer disconnects.
#[cfg(test)]
async fn serve_frames<S, F>(
    stream: &mut S,
    service: &std::sync::Weak<dyn RangeService>,
    authorize: F,
) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    F: Fn(&RangeRequest) -> Result<(), TransportError>,
{
    serve_frames_with_idle_timeout(
        stream,
        service,
        crate::RangeRuntimePolicy::default().rpc_server_idle_timeout,
        crate::RangeRuntimePolicy::default().rpc_frame_max,
        &ServePeer::default(),
        authorize,
    )
    .await
}

async fn serve_frames_with_idle_timeout<S, F>(
    stream: &mut S,
    service: &std::sync::Weak<dyn RangeService>,
    server_idle_timeout: Time,
    max_frame: ByteSize,
    peer: &ServePeer,
    authorize: F,
) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    F: Fn(&RangeRequest) -> Result<(), TransportError>,
{
    loop {
        let next = tokio::time::timeout(
            server_idle_timeout.to_std(),
            read_request_or_eof_with_limit(stream, max_frame),
        )
        .await;
        let envelope = match next {
            // Idle past the deadline, or a clean peer disconnect: close quietly.
            Err(_) | Ok(Ok(None)) => return Ok(()),
            Ok(Ok(Some(envelope))) => envelope,
            Ok(Err(error)) => return Err(error),
        };
        let RangeEnvelope { trace, request } = envelope;
        let request = request.into_owned();

        // This is the cross-node hop: the caller's `gres.range_rpc` span becomes
        // the remote parent of the work this node is about to do, so one trace
        // spans both processes.
        let span = range_serve_span(&request, peer);
        trace.apply_to(&span);

        // Authorization is re-checked per request because it is request-type
        // dependent, and it is recorded on the span: a rejected peer is exactly
        // the thing an operator goes looking for.
        if let Err(error) = authorize(&request) {
            record_rpc_error(&span, &error);
            return Err(error);
        }
        let Some(service) = service.upgrade() else {
            // The listener was torn down; close as if the server went away.
            return Ok(());
        };
        let mut counted = CountingStream {
            inner: stream,
            written: 0,
        };
        let outcome =
            handle_request_on_stream_with_limit(&mut counted, &service, request, max_frame)
                .instrument(span.clone())
                .await;
        let written = counted.written;
        if !span.is_disabled() {
            span.record("pg.response_bytes", telemetry::integer(written));
        }
        if let Err(error) = &outcome {
            record_rpc_error(&span, error);
        }
        outcome?;
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
    stream: &mut S,
    request: &RangeRequest,
    wait: Time,
    max_frame: ByteSize,
) -> Result<RangeResponse, TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut bytes = RpcBytes::default();
    let outcome = call_stream_frames(stream, request, wait, max_frame, &mut bytes).await;
    bytes.record(&tracing::Span::current());
    outcome
}

async fn call_stream_frames<S>(
    stream: &mut S,
    request: &RangeRequest,
    wait: Time,
    max_frame: ByteSize,
    bytes: &mut RpcBytes,
) -> Result<RangeResponse, TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let envelope = RangeEnvelope::outgoing(request);
    bytes.request = timeout(wait, write_frame_counted(stream, &envelope, max_frame)).await??;
    timeout(wait, stream.flush()).await??;
    let (first, frame_bytes) = timeout(wait, read_frame_counted(stream, max_frame)).await??;
    bytes.response = bytes.response.saturating_add(frame_bytes);
    let chunk = match first {
        RangeResponse::SqlResultsChunk { chunk } => chunk,
        RangeResponse::SqlResultsDone => return Ok(RangeResponse::SqlResults { results: vec![] }),
        response => return Ok(response),
    };
    let mut results = Vec::new();
    consume_sql_chunk(&mut results, chunk)?;
    loop {
        let (response, frame_bytes) =
            timeout(wait, read_frame_counted(stream, max_frame)).await??;
        bytes.response = bytes.response.saturating_add(frame_bytes);
        match response {
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
    serve_frames(&mut stream, &Arc::downgrade(&service), |_| Ok(())).await
}

/// Handle one decoded request, leaving the stream at a frame boundary so the
/// caller's serve loop can read the peer's next request.
#[cfg(test)]
async fn handle_request_on_stream<S>(
    stream: &mut S,
    service: &Arc<dyn RangeService>,
    request: RangeRequest,
) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    handle_request_on_stream_with_limit(stream, service, request, MAX_FRAME).await
}

async fn handle_request_on_stream_with_limit<S>(
    stream: &mut S,
    service: &Arc<dyn RangeService>,
    request: RangeRequest,
    max_frame: ByteSize,
) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    if let Some(response) = service.handle_connection(request, stream).await? {
        if let RangeResponse::SqlResults { results } = response {
            write_sql_results(stream, results, max_frame).await?;
        } else {
            write_frame_with_limit(stream, &response, max_frame).await?;
        }
    }
    stream.flush().await?;
    Ok(())
}

async fn write_sql_results<W>(
    writer: &mut W,
    results: Vec<WireQueryResult>,
    max_frame: ByteSize,
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
                write_row_chunks(writer, result_index, fields, rows, tag, max_frame).await?;
            }
            result => {
                let response = RangeResponse::SqlResultsChunk {
                    chunk: WireSqlResultChunk::Complete {
                        result_index,
                        result,
                    },
                };
                match write_frame_with_limit(writer, &response, max_frame).await {
                    Ok(()) => {}
                    Err(TransportError::FrameTooLarge { .. }) => {
                        write_frame_with_limit(
                            writer,
                            &RangeResponse::SqlError {
                                code: "54000".into(),
                                message: "one remote SQL result exceeds the transport frame limit"
                                    .into(),
                            },
                            max_frame,
                        )
                        .await?;
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                }
            }
        }
    }
    write_frame_with_limit(writer, &RangeResponse::SqlResultsDone, max_frame).await
}

async fn write_row_chunks<W>(
    writer: &mut W,
    result_index: u32,
    fields: Vec<WireFieldDescription>,
    rows: Vec<Vec<Option<WireCell>>>,
    tag: String,
    max_frame: ByteSize,
) -> Result<(), TransportError>
where
    W: AsyncWrite + Unpin,
{
    let chunk_limit = max_frame - kibibytes(4);
    let chunk_target = chunk_limit.bytes_usize();
    let mut fields = Some(fields);
    let mut page = Vec::new();
    let mut page_bytes = 0usize;
    for row in rows {
        let row_bytes = match serialize_json_bounded(&row, chunk_limit) {
            Ok(bytes) => bytes.len().saturating_add(1),
            Err(TransportError::FrameTooLarge { .. }) => {
                write_frame_with_limit(
                    writer,
                    &RangeResponse::SqlError {
                        code: "54000".into(),
                        message: "one remote SQL row exceeds the transport frame limit".into(),
                    },
                    max_frame,
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
            serialize_json_bounded(&probe, chunk_limit)?.len()
        } else {
            0
        };
        if !page.is_empty() && page_bytes.saturating_add(row_bytes) > chunk_target {
            write_row_page(
                writer,
                result_index,
                fields.take(),
                std::mem::take(&mut page),
                None,
                max_frame,
            )
            .await?;
            page_bytes = 0;
        }
        if page.is_empty() {
            page_bytes = overhead;
            if page_bytes.saturating_add(row_bytes) > chunk_target {
                write_frame_with_limit(
                    writer,
                    &RangeResponse::SqlError {
                        code: "54000".into(),
                        message: "one remote SQL row exceeds the transport frame limit".into(),
                    },
                    max_frame,
                )
                .await?;
                return Ok(());
            }
        }
        page_bytes = page_bytes.saturating_add(row_bytes);
        page.push(row);
    }
    write_row_page(
        writer,
        result_index,
        fields.take(),
        page,
        Some(tag),
        max_frame,
    )
    .await
}

async fn write_row_page<W>(
    writer: &mut W,
    result_index: u32,
    fields: Option<Vec<WireFieldDescription>>,
    rows: Vec<Vec<Option<WireCell>>>,
    tag: Option<String>,
    max_frame: ByteSize,
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
    match write_frame_with_limit(writer, &response, max_frame).await {
        Ok(()) => Ok(()),
        Err(TransportError::FrameTooLarge { .. }) => write_frame_with_limit(
            writer,
            &RangeResponse::SqlError {
                code: "54000".into(),
                message: "one remote SQL row description or command tag exceeds the transport frame limit"
                    .into(),
            },
            max_frame,
        )
        .await,
        Err(error) => Err(error),
    }
}

#[cfg(test)]
pub(crate) async fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<(), TransportError>
where
    W: AsyncWrite + Unpin + ?Sized,
    T: Serialize,
{
    write_frame_with_limit(writer, value, MAX_FRAME).await
}

pub(crate) async fn write_frame_with_limit<W, T>(
    writer: &mut W,
    value: &T,
    max_frame: ByteSize,
) -> Result<(), TransportError>
where
    W: AsyncWrite + Unpin + ?Sized,
    T: Serialize,
{
    write_frame_counted(writer, value, max_frame)
        .await
        .map(drop)
}

/// Write one length-prefixed frame and report the bytes it put on the wire,
/// prefix included, for `pg.request_bytes`.
async fn write_frame_counted<W, T>(
    writer: &mut W,
    value: &T,
    max_frame: ByteSize,
) -> Result<usize, TransportError>
where
    W: AsyncWrite + Unpin + ?Sized,
    T: Serialize,
{
    let bytes = serialize_json_bounded(value, max_frame)?;
    let len = u32::try_from(bytes.len()).map_err(|_| TransportError::FrameTooLarge {
        actual: bytes.len(),
        limit: max_frame.bytes_usize(),
    })?;
    writer.write_u32(len).await?;
    writer.write_all(&bytes).await?;
    Ok(bytes.len().saturating_add(std::mem::size_of::<u32>()))
}

fn serialize_json_bounded<T>(value: &T, limit: ByteSize) -> Result<Vec<u8>, TransportError>
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

    let limit = limit.bytes_usize();
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

/// Read one length-prefixed request envelope, or `None` when the peer
/// disconnected at a frame boundary.
///
/// A clean EOF before any prefix byte, and an abrupt close reported before
/// any prefix byte (pooled TLS clients drop idle connections without sending
/// `close_notify`), are both normal ends of a kept-alive connection.
/// Disconnecting inside a frame is an error.
async fn read_request_or_eof_with_limit<R>(
    reader: &mut R,
    max_frame: ByteSize,
) -> Result<Option<RangeEnvelope<'static>>, TransportError>
where
    R: AsyncRead + Unpin,
{
    let mut prefix = [0_u8; 4];
    let mut filled = 0_usize;
    while filled < prefix.len() {
        let read = match reader.read(&mut prefix[filled..]).await {
            Ok(read) => read,
            Err(error) if filled == 0 && error.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        if read == 0 {
            if filled == 0 {
                return Ok(None);
            }
            return Err(TransportError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "peer closed the stream inside a frame length prefix",
            )));
        }
        filled += read;
    }
    let max_frame = max_frame.bytes_usize();
    let len =
        usize::try_from(u32::from_be_bytes(prefix)).map_err(|_| TransportError::FrameTooLarge {
            actual: max_frame.saturating_add(1),
            limit: max_frame,
        })?;
    if len > max_frame {
        return Err(TransportError::FrameTooLarge {
            actual: len,
            limit: max_frame,
        });
    }
    let mut bytes = vec![0; len];
    reader.read_exact(&mut bytes).await?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

#[cfg(test)]
async fn read_frame<R, T>(reader: &mut R) -> Result<T, TransportError>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    read_frame_counted(reader, MAX_FRAME)
        .await
        .map(|(value, _)| value)
}

/// Read one length-prefixed frame and report the bytes it took off the wire,
/// prefix included, for `pg.response_bytes`.
async fn read_frame_counted<R, T>(
    reader: &mut R,
    max_frame: ByteSize,
) -> Result<(T, usize), TransportError>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let max_frame = max_frame.bytes_usize();
    let len = reader.read_u32().await?;
    let len = usize::try_from(len).map_err(|_| TransportError::FrameTooLarge {
        actual: max_frame.saturating_add(1),
        limit: max_frame,
    })?;
    if len > max_frame {
        return Err(TransportError::FrameTooLarge {
            actual: len,
            limit: max_frame,
        });
    }
    let mut bytes = vec![0; len];
    reader.read_exact(&mut bytes).await?;
    let value = serde_json::from_slice(&bytes)?;
    Ok((value, len.saturating_add(std::mem::size_of::<u32>())))
}

async fn timeout<T>(wait: Time, task: impl Future<Output = T>) -> Result<T, TransportError> {
    tokio::time::timeout(wait.to_std(), task)
        .await
        .map_err(|_| TransportError::Timeout(wait))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    #[test]
    fn bounded_json_serialization_rejects_before_allocating_an_oversized_candidate() {
        let oversized = "x".repeat(4 * MAX_FRAME.bytes_usize());

        let error = serialize_json_bounded(&oversized, MAX_FRAME)
            .expect_err("oversized JSON candidate is rejected at the limit");

        let max_frame = MAX_FRAME.bytes_usize();
        assert!(matches!(
            error,
            TransportError::FrameTooLarge { limit, .. } if limit == max_frame
        ));
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
            tuple: vec![0; MAX_FRAME.bytes_usize()],
        }]);
        let error = serialize_json_bounded(&RangeRequest::JoinRange(request), MAX_FRAME)
            .expect_err("frame must be bounded");
        assert!(matches!(error, TransportError::FrameTooLarge { .. }));
    }

    /// The wire shape, pinned. Every request now rides inside a `RangeEnvelope`
    /// under a `request` key, and an RPC made with no active span must carry no
    /// `trace` key at all — not an empty object, and above all not a
    /// placeholder `traceparent` a peer would then try to parse.
    #[tokio::test]
    async fn untraced_request_frames_carry_the_request_and_no_trace_context() {
        use assert2::assert;
        let request = RangeRequest::Sql {
            range_id: RangeId::new(4),
            sql: "select 1".to_string(),
        };
        let mut frame = Vec::new();

        write_frame(&mut frame, &RangeEnvelope::outgoing(&request))
            .await
            .expect("write untraced frame");

        let json =
            serde_json::from_slice::<serde_json::Value>(&frame[4..]).expect("frame body is JSON");
        assert!(
            json == serde_json::json!({
                "request": {"type": "sql", "range_id": 4, "sql": "select 1"}
            })
        );
    }

    /// The backstop for "we forgot to inject": with a span active, the frame's
    /// JSON must carry *that* span's trace id and its sampled flag. Asserting a
    /// `traceparent` merely exists survives a mutant that injects a constant.
    #[tokio::test]
    async fn traced_request_frames_carry_the_active_trace_id() {
        use assert2::assert;
        use opentelemetry::trace::{TraceContextExt as _, TracerProvider as _};
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;
        use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};

        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
            .build();
        let layer = tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("transport-envelope"))
            .with_filter(tracing_subscriber::EnvFilter::new(
                "crabka_gres_ranges::route=debug",
            ));
        let subscriber = tracing_subscriber::registry().with(layer);

        let (trace_id, frame) = tracing::subscriber::with_default(subscriber, || {
            let span = range_rpc_span("range-7.internal:7443", &RangeRequest::Range0Barrier);
            let _guard = span.enter();
            let trace_id = tracing::Span::current()
                .context()
                .span()
                .span_context()
                .trace_id()
                .to_string();
            let envelope = RangeEnvelope::outgoing(&RangeRequest::Range0Barrier);
            let frame = serde_json::to_value(&envelope).expect("encode traced envelope");
            (trace_id, frame)
        });

        let traceparent = frame["trace"]["traceparent"]
            .as_str()
            .expect("traced frame carries a traceparent");
        assert!(traceparent.contains(&trace_id));
        assert!(traceparent.ends_with("-01"));
        assert!(frame["request"] == serde_json::json!({"type": "range0_barrier"}));
    }

    /// The reserve exists so a join accepted by the planner still fits once the
    /// envelope wraps it. Sized against the worst case the carrier permits: a
    /// 55-byte `traceparent` plus the 512-byte `tracestate` ceiling
    /// `crabka-trace-context` enforces.
    #[test]
    fn largest_accepted_join_still_fits_a_worst_case_traced_frame() {
        use assert2::assert;
        let mut request = join_request_fixture();
        request.broadcast_rows = Some(vec![JoinRangeRow {
            tuple: vec![0; largest_fitting_join_payload()],
        }]);
        assert!(request.fits_transport_frame());

        let request = RangeRequest::JoinRange(request);
        let envelope = RangeEnvelope {
            trace: TraceCarrier {
                traceparent: Some("0".repeat(55)),
                tracestate: Some("x".repeat(512)),
            },
            request: Cow::Borrowed(&request),
        };

        assert!(serialize_json_bounded(&envelope, MAX_FRAME).is_ok());
    }

    /// The largest `broadcast_rows` payload [`JoinRangeReq::fits_transport_frame`]
    /// still accepts, found by bisection so the test pins the real boundary
    /// rather than a copy of the arithmetic under test.
    fn largest_fitting_join_payload() -> usize {
        let mut request = join_request_fixture();
        request.broadcast_rows = Some(vec![JoinRangeRow { tuple: vec![] }]);
        let mut low = 0usize;
        let mut high = MAX_FRAME.bytes_usize();
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            request.broadcast_rows.as_mut().expect("broadcast")[0]
                .tuple
                .resize(mid, 0);
            if request.fits_transport_frame() {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        low
    }

    #[test]
    fn join_request_transport_capacity_has_exact_materialized_boundary() {
        let mut request = join_request_fixture();
        request.broadcast_rows = Some(vec![JoinRangeRow { tuple: vec![] }]);
        let mut low = 0usize;
        let mut high = MAX_FRAME.bytes_usize();
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
                RangeRequest::Ddl { sql } => RangeResponse::SqlResults {
                    results: vec![WireQueryResult::Command { tag: sql }],
                },
                RangeRequest::Range0Barrier => RangeResponse::Range0Barriered,
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
                    RangeResponse::InspectDurableRecords(Box::new(InspectDurableRecordsResp {
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
                    }))
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
    async fn ddl_request_roundtrips_over_loopback() {
        use assert2::assert;
        let addr = spawn_loopback(Arc::new(EchoService::default()))
            .await
            .unwrap();
        let response = FramedTcpClient::default()
            .call(
                &addr.to_string(),
                &RangeRequest::Ddl {
                    sql: "create table t (id int4)".to_string(),
                },
            )
            .await
            .unwrap();

        assert!(
            response
                == RangeResponse::SqlResults {
                    results: vec![WireQueryResult::Command {
                        tag: "create table t (id int4)".to_string(),
                    }],
                }
        );
    }

    #[tokio::test]
    async fn range0_barrier_roundtrips_over_loopback() {
        use assert2::assert;
        let addr = spawn_loopback(Arc::new(EchoService::default()))
            .await
            .unwrap();
        let response = FramedTcpClient::default()
            .call(&addr.to_string(), &RangeRequest::Range0Barrier)
            .await
            .unwrap();

        assert!(response == RangeResponse::Range0Barriered);
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

        let error = FramedTcpClient::with_timeout(crabka_units::millis(20))
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
        let policy = crate::RangeRuntimePolicy::default();
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
            max_records: policy.durable_inspect_max_records.get(),
            max_bytes: u32::try_from(policy.durable_inspect_max_size.bytes_u64()).unwrap(),
            snapshot_offset: None,
            cursor: Some("cursor".into()),
        };
        let encoded = serialize_json_bounded(
            &RangeRequest::InspectDurableRecords(request.clone()),
            MAX_FRAME,
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

    /// Bind a loopback server that counts accepted connections and serves the
    /// kept-alive frame loop on each of them.
    async fn spawn_counting_loopback(
        service: Arc<dyn RangeService>,
    ) -> (SocketAddr, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("loopback address");
        let accepts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepts);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                counter.fetch_add(1, Ordering::SeqCst);
                let service = Arc::clone(&service);
                tokio::spawn(async move {
                    let _ = handle_stream(stream, service).await;
                });
            }
        });
        (addr, accepts)
    }

    /// Bind a loopback server that answers exactly one request per connection
    /// and then closes it, imitating a server that does not keep connections.
    async fn spawn_one_shot_loopback(
        service: Arc<dyn RangeService>,
    ) -> (SocketAddr, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("loopback address");
        let accepts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepts);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                counter.fetch_add(1, Ordering::SeqCst);
                let service = Arc::clone(&service);
                tokio::spawn(async move {
                    if let Ok(envelope) = read_frame::<_, RangeEnvelope<'static>>(&mut stream).await
                    {
                        let request = envelope.request.into_owned();
                        let _ = handle_request_on_stream(&mut stream, &service, request).await;
                    }
                });
            }
        });
        (addr, accepts)
    }

    /// Service whose handlers block until the shared gate opens, forcing every
    /// concurrent call onto its own connection.
    struct GateService {
        entered: Arc<AtomicUsize>,
        gate: tokio::sync::watch::Receiver<bool>,
    }

    #[async_trait]
    impl RangeService for GateService {
        async fn handle(&self, _request: RangeRequest) -> RangeResponse {
            self.entered.fetch_add(1, Ordering::SeqCst);
            let mut gate = self.gate.clone();
            let _ = gate.wait_for(|open| *open).await.expect("gate sender");
            RangeResponse::Range0Barriered
        }
    }

    async fn wait_until(counter: &AtomicUsize, expected: usize) {
        while counter.load(Ordering::SeqCst) < expected {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    fn spawn_barrier_calls(
        client: &FramedTcpClient,
        addr: SocketAddr,
        count: usize,
    ) -> Vec<tokio::task::JoinHandle<Result<RangeResponse, TransportError>>> {
        (0..count)
            .map(|_| {
                let client = client.clone();
                let endpoint = addr.to_string();
                tokio::spawn(
                    async move { client.call(&endpoint, &RangeRequest::Range0Barrier).await },
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn sequential_calls_reuse_one_server_accepted_connection() {
        use assert2::assert;
        let (addr, accepts) = spawn_counting_loopback(Arc::new(EchoService::default())).await;
        let client = FramedTcpClient::default();

        for _ in 0..2 {
            let response = client
                .call(&addr.to_string(), &RangeRequest::Range0Barrier)
                .await
                .expect("pooled RPC");
            assert!(response == RangeResponse::Range0Barriered);
        }

        assert!(accepts.load(Ordering::SeqCst) == 1);
    }

    #[tokio::test]
    async fn concurrent_calls_pool_up_to_the_cap_and_reuse_pooled_connections() {
        use assert2::assert;
        let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
        let entered = Arc::new(AtomicUsize::new(0));
        let service = Arc::new(GateService {
            entered: Arc::clone(&entered),
            gate: gate_rx,
        });
        let (addr, accepts) = spawn_counting_loopback(service).await;
        let client = FramedTcpClient::default()
            .with_pool_tuning(crate::RangeRuntimePolicy::default().rpc_pool_idle_ttl, 2);

        // Four concurrent calls must each open their own connection.
        let handles = spawn_barrier_calls(&client, addr, 4);
        wait_until(&entered, 4).await;
        gate_tx.send(true).expect("open gate");
        for handle in handles {
            assert!(handle.await.expect("join call").is_ok());
        }
        assert!(accepts.load(Ordering::SeqCst) == 4);

        // Only the per-endpoint cap of connections was pooled: three more
        // concurrent calls reuse the two pooled connections and must dial
        // exactly one fresh connection.
        gate_tx.send(false).expect("close gate");
        entered.store(0, Ordering::SeqCst);
        let handles = spawn_barrier_calls(&client, addr, 3);
        wait_until(&entered, 3).await;
        gate_tx.send(true).expect("reopen gate");
        for handle in handles {
            assert!(handle.await.expect("join call").is_ok());
        }
        assert!(accepts.load(Ordering::SeqCst) == 5);
    }

    #[tokio::test]
    async fn staleness_probe_discards_server_closed_connection_without_error() {
        use assert2::assert;
        let (addr, accepts) = spawn_one_shot_loopback(Arc::new(EchoService::default())).await;
        let client = FramedTcpClient::default();

        let first = client
            .call(&addr.to_string(), &RangeRequest::Range0Barrier)
            .await
            .expect("first call");
        assert!(first == RangeResponse::Range0Barriered);
        // Let the server's close reach the pooled socket.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let second = client
            .call(&addr.to_string(), &RangeRequest::Range0Barrier)
            .await
            .expect("second call transparently dials fresh");
        assert!(second == RangeResponse::Range0Barriered);

        assert!(accepts.load(Ordering::SeqCst) == 2);
    }

    #[tokio::test]
    async fn idle_ttl_evicts_pooled_connection_instead_of_reusing_it() {
        use assert2::assert;
        let (addr, accepts) = spawn_counting_loopback(Arc::new(EchoService::default())).await;
        let client = FramedTcpClient::default().with_pool_tuning(
            crabka_units::millis(50),
            crate::RangeRuntimePolicy::default()
                .rpc_pool_max_idle_per_endpoint
                .get(),
        );

        for _ in 0..2 {
            let response = client
                .call(&addr.to_string(), &RangeRequest::Range0Barrier)
                .await
                .expect("pooled RPC");
            assert!(response == RangeResponse::Range0Barriered);
            tokio::time::sleep(Duration::from_millis(120)).await;
        }

        assert!(accepts.load(Ordering::SeqCst) == 2);
    }

    #[tokio::test]
    async fn mid_call_peer_drop_surfaces_error_and_is_not_pooled() {
        use assert2::assert;
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("loopback address");
        let accepts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepts);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let accepted = counter.fetch_add(1, Ordering::SeqCst) + 1;
                tokio::spawn(async move {
                    if accepted == 1 {
                        // Read the request, then drop the connection without
                        // responding.
                        let _ = read_frame::<_, RangeEnvelope<'static>>(&mut stream).await;
                    } else {
                        let _ = handle_stream(stream, Arc::new(EchoService::default())).await;
                    }
                });
            }
        });
        let client = FramedTcpClient::default();

        let error = client
            .call(&addr.to_string(), &RangeRequest::Range0Barrier)
            .await
            .expect_err("peer dropped after the request write");
        assert!(matches!(error, TransportError::Io(_)));
        let response = client
            .call(&addr.to_string(), &RangeRequest::Range0Barrier)
            .await
            .expect("fresh connection succeeds");
        assert!(response == RangeResponse::Range0Barriered);

        assert!(accepts.load(Ordering::SeqCst) == 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pooled_connections_never_cross_runtimes() {
        use assert2::assert;
        let (addr, accepts) = spawn_counting_loopback(Arc::new(EchoService::default())).await;
        let client = FramedTcpClient::default();

        let first = client
            .call(&addr.to_string(), &RangeRequest::Range0Barrier)
            .await
            .expect("main runtime call");
        assert!(first == RangeResponse::Range0Barriered);
        assert!(accepts.load(Ordering::SeqCst) == 1);

        // A short-lived scan runtime must not see the main runtime's pooled
        // connection (tokio sockets die with the runtime that dialed them),
        // and its own connection must not poison the shared pool.
        let ephemeral_client = client.clone();
        let endpoint = addr.to_string();
        let ephemeral = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("ephemeral runtime")
                .block_on(async move {
                    ephemeral_client
                        .call(&endpoint, &RangeRequest::Range0Barrier)
                        .await
                        .expect("ephemeral runtime call")
                })
        })
        .join()
        .expect("ephemeral runtime thread");
        assert!(ephemeral == RangeResponse::Range0Barriered);
        assert!(accepts.load(Ordering::SeqCst) == 2);

        // Back on the main runtime the original connection is still reusable.
        let third = client
            .call(&addr.to_string(), &RangeRequest::Range0Barrier)
            .await
            .expect("main runtime reuse");
        assert!(third == RangeResponse::Range0Barriered);
        assert!(accepts.load(Ordering::SeqCst) == 2);
    }

    /// Service returning a rows result large enough to span several SQL chunks.
    struct BigRowsService;

    #[async_trait]
    impl RangeService for BigRowsService {
        async fn handle(&self, _request: RangeRequest) -> RangeResponse {
            let cell = WireCell {
                text: vec![120; 150_000],
                binary: Vec::new(),
            };
            let row = vec![Some(cell)];
            RangeResponse::SqlResults {
                results: vec![WireQueryResult::Rows {
                    fields: vec![WireFieldDescription {
                        name: "c".into(),
                        table_oid: 0,
                        column_id: 0,
                        type_oid: 25,
                        type_size: -1,
                        type_modifier: -1,
                        format: 0,
                    }],
                    rows: vec![row.clone(), row.clone(), row],
                    tag: "SELECT 3".into(),
                }],
            }
        }
    }

    #[tokio::test]
    async fn call_sql_into_reuses_one_connection_across_multi_chunk_results() {
        use assert2::assert;
        let (addr, accepts) = spawn_counting_loopback(Arc::new(BigRowsService)).await;
        let client = FramedTcpClient::default();

        for _ in 0..2 {
            let mut sink = crabka_pgwire::engine::CollectingResultSink::default();
            client
                .call_sql_into(
                    &addr.to_string(),
                    &RangeRequest::Sql {
                        range_id: RangeId::new(1),
                        sql: "select big".into(),
                    },
                    &mut sink,
                )
                .await
                .expect("chunked SQL RPC");
            let row_pages = sink
                .pages()
                .iter()
                .filter(|page| matches!(page, ResultPage::Rows { .. }))
                .count();
            assert!(row_pages >= 2);
        }

        assert!(accepts.load(Ordering::SeqCst) == 1);
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
