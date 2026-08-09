//! Routing, statement and 2PC tracing for the multi-range gateway.
//!
//! This module emits every span it builds under the single [`ROUTE_TARGET`]
//! target, so an operator enables or silences the whole gateway tier with one
//! `EnvFilter` directive. Only the OTLP layer names that target, as
//! `crabka_gres_ranges::route=debug` in the gres default filter. The stdout
//! `fmt` layer deliberately does not name it, so a gateway that does not export
//! pays one disabled level check per statement and prints nothing.
//!
//! # Zero cost when disabled
//!
//! A disabled callsite costs a load and a branch, but its **field expressions
//! still evaluate**. Two rules follow, and both are load-bearing here:
//!
//! - Any span whose fields cost more than a field read sits behind
//!   `tracing::enabled!(target: ROUTE_TARGET, Level::DEBUG)` with a
//!   [`tracing::Span::none`] fallback. [`statement_span`] is the example. It
//!   derives a query summary from the SQL text.
//! - Every `record_*` helper below returns immediately when the span is
//!   disabled, *before* it formats anything. That keeps the 2PC participant
//!   list from [`record_scatter_plan`], a `String` built from a `Vec<RangeId>`,
//!   off the hot path of an unsampled write.
//!
//! The code builds spans by hand rather than with `#[instrument]`. The attribute
//! cannot express the `enabled!` guard, and every span here needs
//! [`tracing::Span::record`] for fields that are known only after the statement
//! runs: the commit timestamp, the 2PC outcome, and the error status.
//!
//! # SQL text
//!
//! `db.query.text` carries the statement verbatim. It is the only attribute here
//! that can export a literal such as a password, a national identifier, or a
//! customer name. It is **off** unless [`SQL_TEXT_ENV`] is set to a truthy
//! value. The code truncates it at [`MAX_SQL_TEXT_BYTES`] and records it only on
//! `db.statement`, never on a child. With it off, `db.query.summary`,
//! `db.operation.name`, `db.collection.name` and `pg.table_id` still identify
//! the statement well enough to attribute latency.
//!
//! # Error status
//!
//! [`record_error`] sets `otel.status_code = "ERROR"` and
//! `otel.status_description`, which `tracing-opentelemetry` maps onto the
//! `OTel` span status. See that function for the two spelling-and-ordering traps
//! in that contract. A successful span stays `Unset` on purpose. The code never
//! records `"OK"`, because in `OTel` that value means the application explicitly
//! asserts success, which is a stronger claim than a statement that returns rows
//! supports.

use std::sync::LazyLock;

use crate::ids::RangeId;

/// `tracing` target carrying every gateway routing, statement and 2PC span.
///
/// The spelling matches the directive in
/// `crabka_gres::telemetry::OTEL_DEFAULT_FILTER`. The two cannot share a
/// constant, because `crabka-gres` depends on this crate.
pub const ROUTE_TARGET: &str = "crabka_gres_ranges::route";

/// Environment variable gating verbatim SQL on `db.statement`.
pub const SQL_TEXT_ENV: &str = "CRABKA_OTLP_SQL_TEXT";

/// Cap on the `db.query.text` attribute. A generated `INSERT` can carry
/// megabytes of literals, and the collector would drop them anyway.
pub const MAX_SQL_TEXT_BYTES: usize = 4096;

/// Cap on `otel.status_description`. Error messages quote plan fragments and can
/// grow without bound.
pub const MAX_STATUS_DESCRIPTION_BYTES: usize = 512;

/// `pg.role` for the range holding the Percolator primary lock.
pub const ROLE_PRIMARY: &str = "primary";

/// `pg.role` for a range holding a secondary lock.
pub const ROLE_SECONDARY: &str = "secondary";

/// Fate of one timestamp-scatter round, recorded as `pg.outcome`.
///
/// A timestamp scatter is the Percolator-style 2PC round.
///
/// | Value | Meaning |
/// |---|---|
/// | `no_writes` | The statement planned no writes, so no 2PC round ran. |
/// | `aborted` | No durable effect. Nothing was prewritten, or every prewrite was resolved as aborted. |
/// | `prepared` | Prewrites are durable. The decision waits for the explicit `COMMIT`. |
/// | `indeterminate` | Prewrites can be durable and unresolved, and participants can still hold locks. |
/// | `committed` | The commit decision is durable at the primary. |
///
/// `indeterminate` is the value an operator looks for. It is the only one that
/// means a person has to look. The gateway threads a [`ScatterOutcome`] through
/// the scatter body and records it once on the way out. Every exit path
/// therefore lands one of these values on the span, including the error paths
/// that only propagate a `?`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScatterOutcome {
    /// The statement planned no writes, so no 2PC round ran.
    NoWrites,
    /// Nothing durable survives the statement.
    Aborted,
    /// Prewrites are durable and await an explicit `COMMIT`.
    Prepared,
    /// Prewrites may be durable and unresolved.
    Indeterminate,
    /// The commit decision is durable at the primary.
    Committed,
}

impl ScatterOutcome {
    /// The attribute value written to `pg.outcome`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoWrites => "no_writes",
            Self::Aborted => "aborted",
            Self::Prepared => "prepared",
            Self::Indeterminate => "indeterminate",
            Self::Committed => "committed",
        }
    }
}

/// Whether verbatim SQL may be attached to statement spans.
///
/// The code reads this flag from the environment once per process. The flag
/// decides whether text that can be sensitive leaves the node, so a mid-flight
/// environment change must not be able to flip it.
#[must_use]
pub fn sql_text_enabled() -> bool {
    static ENABLED: LazyLock<bool> =
        LazyLock::new(|| sql_text_flag(std::env::var(SQL_TEXT_ENV).ok().as_deref()));
    *ENABLED
}

/// Parse the [`SQL_TEXT_ENV`] value.
///
/// An absent value, an empty value, and any value that is not an affirmative
/// spelling all leave SQL text off. That is the safe direction.
fn sql_text_flag(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// Coerce an integer span field to `i64`, and saturate on overflow.
///
/// **Every numeric attribute in this module goes through here.** OTLP has no
/// unsigned integer type, so `tracing-opentelemetry` records a `u32`, `u64` or
/// `usize` field as a *string*. Tempo cannot compare, sort, or range-filter a
/// string attribute, so a query such as `pg.participants > 2` silently matches
/// nothing. A record of `i64` keeps the attribute numeric.
///
/// This function saturates rather than wraps. Only timestamps and global xids
/// could exceed `i64::MAX`, and they would need a clock 292 billion years out.
/// A clamp at least preserves the order an operator filters on.
/// `pg.participant_ranges` stays a string on purpose, because it is a
/// comma-joined list and not a quantity.
#[must_use]
pub fn integer<T: TryInto<i64>>(value: T) -> i64 {
    value.try_into().unwrap_or(i64::MAX)
}

/// Truncate `text` to at most `max_bytes`.
///
/// The function steps back to a character boundary, so the attribute is always
/// valid UTF-8.
#[must_use]
pub fn truncate(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// SQL verbs recognized for `db.operation.name`.
///
/// The gateway's own router keys off the same leading keywords, so this list
/// mirrors what the router can route. It is not the whole `PostgreSQL` grammar.
const OPERATIONS: [&str; 22] = [
    "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "TRUNCATE", "BEGIN",
    "START", "COMMIT", "ROLLBACK", "ABORT", "END", "SET", "SHOW", "COPY", "EXPLAIN", "WITH",
    "NOTIFY", "LISTEN", "UNLISTEN",
];

/// The `db.operation.name` for `sql`, which is its leading verb in the canonical
/// upper-case spelling.
///
/// An unrecognized verb becomes `"OTHER"` instead of the raw token. That keeps
/// the attribute low-cardinality even under a garbage statement.
#[must_use]
pub fn operation_name(sql: &str) -> &'static str {
    let word = sql
        .trim_start()
        .split(|character: char| !character.is_ascii_alphabetic())
        .next()
        .unwrap_or_default();
    OPERATIONS
        .into_iter()
        .find(|operation| operation.eq_ignore_ascii_case(word))
        .unwrap_or("OTHER")
}

/// The `db.query.summary` for an operation and its table.
///
/// The result follows the `OTel` convention of `"<operation> <target>"`, as in
/// `"SELECT orders"`.
#[must_use]
pub fn query_summary(operation: &str, collection: Option<&str>) -> String {
    collection.map_or_else(
        || operation.to_owned(),
        |collection| format!("{operation} {collection}"),
    )
}

/// Render range ids as the comma-joined `pg.participant_ranges` attribute.
///
/// The tenant's range count bounds the result. The result is exactly what an
/// operator needs to find the participant that stalled.
#[must_use]
pub fn join_ranges(ranges: &[RangeId]) -> String {
    ranges
        .iter()
        .map(|range_id| range_id.as_u32().to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// Build the routing span.
///
/// The span level is `TRACE`, because routing is cheap and runs on every
/// statement, including the statements that never leave the coordinator.
/// [`record_route`] records the resolved route after the router knows it.
#[must_use]
pub fn route_span(tenant: &str) -> tracing::Span {
    tracing::trace_span!(
        target: ROUTE_TARGET,
        "pg.route",
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        error.type = tracing::field::Empty,
        pg.tenant = tenant,
        pg.statement_kind = tracing::field::Empty,
        pg.range_id = tracing::field::Empty,
        pg.table_id = tracing::field::Empty,
        pg.scatter = tracing::field::Empty,
        pg.scatter_ranges = tracing::field::Empty,
    )
}

/// Record the resolved route on a [`route_span`].
pub fn record_route(
    span: &tracing::Span,
    kind: &'static str,
    range_id: RangeId,
    table_id: Option<u64>,
    scatter_ranges: Option<usize>,
) {
    if span.is_disabled() {
        return;
    }
    span.record("pg.statement_kind", kind);
    span.record("pg.range_id", integer(range_id.as_u32()));
    span.record("pg.scatter", scatter_ranges.is_some());
    span.record(
        "pg.scatter_ranges",
        integer(scatter_ranges.unwrap_or_default()),
    );
    if let Some(table_id) = table_id {
        span.record("pg.table_id", integer(table_id));
    }
}

/// Build the gateway's per-statement span.
///
/// This span is the analogue of the range-local `db.statement` span, and the
/// span an operator reads first.
///
/// Guard the call with `tracing::enabled!(target: ROUTE_TARGET, Level::DEBUG)`,
/// because this function walks the statement text to derive the summary.
#[must_use]
pub fn statement_span(
    tenant: &str,
    sql: &str,
    kind: &'static str,
    collection: Option<&str>,
    table_id: Option<u64>,
    fast_path: bool,
) -> tracing::Span {
    let operation = operation_name(sql);
    let summary = query_summary(operation, collection);
    let span = tracing::debug_span!(
        target: ROUTE_TARGET,
        "db.statement",
        otel.kind = "internal",
        otel.name = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        db.system.name = "postgresql",
        db.query.summary = summary.as_str(),
        db.operation.name = operation,
        db.collection.name = collection.unwrap_or_default(),
        db.query.text = tracing::field::Empty,
        db.response.status_code = tracing::field::Empty,
        db.response.returned_rows = tracing::field::Empty,
        error.type = tracing::field::Empty,
        pg.tenant = tenant,
        pg.statement_kind = kind,
        pg.table_id = tracing::field::Empty,
        pg.txn.global_xid = tracing::field::Empty,
        pg.rows_affected = tracing::field::Empty,
        pg.result_pages = tracing::field::Empty,
        pg.fast_path = fast_path,
    );
    span.record("otel.name", summary.as_str());
    if let Some(table_id) = table_id {
        span.record("pg.table_id", integer(table_id));
    }
    if sql_text_enabled() {
        span.record("db.query.text", truncate(sql, MAX_SQL_TEXT_BYTES));
    }
    span
}

/// Build the timestamp-scatter span that covers one Percolator-style 2PC round.
///
/// The round knows the participants, the primary, the timestamps, and the
/// outcome only part-way through. This function therefore declares them `Empty`,
/// and the `record_scatter_*` helpers fill them in.
#[must_use]
pub fn scatter_span(tenant: &str) -> tracing::Span {
    tracing::debug_span!(
        target: ROUTE_TARGET,
        "pg.timestamp_scatter",
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        error.type = tracing::field::Empty,
        pg.tenant = tenant,
        pg.table_id = tracing::field::Empty,
        pg.participants = tracing::field::Empty,
        pg.participant_ranges = tracing::field::Empty,
        pg.primary_range = tracing::field::Empty,
        pg.start_ts = tracing::field::Empty,
        pg.commit_ts = tracing::field::Empty,
        pg.global_xid = tracing::field::Empty,
        pg.autocommit = tracing::field::Empty,
        pg.single_shard_bypass = tracing::field::Empty,
        pg.writes = tracing::field::Empty,
        pg.outcome = tracing::field::Empty,
    )
}

/// Record how the gateway drives the round.
///
/// This records whether the round is an implicit single-statement transaction,
/// and whether it commits against one range's local sequence instead of the
/// global timestamp source.
pub fn record_scatter_mode(span: &tracing::Span, autocommit: bool, single_shard_bypass: bool) {
    if span.is_disabled() {
        return;
    }
    span.record("pg.autocommit", autocommit);
    span.record("pg.single_shard_bypass", single_shard_bypass);
}

/// Record the transaction identity once the write lease is held.
pub fn record_scatter_identity(span: &tracing::Span, start_ts: u64, global_xid: u64) {
    if span.is_disabled() {
        return;
    }
    span.record("pg.start_ts", integer(start_ts));
    span.record("pg.global_xid", integer(global_xid));
}

/// Record the participant set after planning routes every write.
///
/// This is the helper the disabled-span guard exists for. The function joins
/// `participants` into a `String`, and that must not happen on an unsampled
/// write.
pub fn record_scatter_plan(
    span: &tracing::Span,
    participants: &[RangeId],
    writes: usize,
    table_id: u64,
) {
    if span.is_disabled() {
        return;
    }
    span.record("pg.participants", integer(participants.len()));
    span.record("pg.participant_ranges", join_ranges(participants).as_str());
    span.record("pg.writes", integer(writes));
    span.record("pg.table_id", integer(table_id));
}

/// Record which participant holds the primary lock.
///
/// This is separate from [`record_scatter_plan`] because an autocommit round
/// elects its primary from the plan, while a statement that joins an open
/// transaction inherits the primary that transaction already committed to.
pub fn record_primary_range(span: &tracing::Span, primary_range: RangeId) {
    if span.is_disabled() {
        return;
    }
    span.record("pg.primary_range", integer(primary_range.as_u32()));
}

/// Record the commit timestamp after the gateway mints the decision.
pub fn record_commit_ts(span: &tracing::Span, commit_ts: u64) {
    if span.is_disabled() {
        return;
    }
    span.record("pg.commit_ts", integer(commit_ts));
}

/// Record the fate of a 2PC round.
///
/// The scatter path and the abort path call this on the way out, so it lands on
/// every exit, including the exits that propagate an error.
pub fn record_outcome(span: &tracing::Span, outcome: ScatterOutcome) {
    if span.is_disabled() {
        return;
    }
    span.record("pg.outcome", outcome.as_str());
}

/// Build the span for one participant's prewrite.
///
/// The span sets `otel.kind = "client"`, because the work happens on the range
/// owner, which can be another node. `pg.local` says which node it is.
/// [`record_local`] records that field after the body picks its path.
#[must_use]
pub fn prewrite_span(
    range_id: RangeId,
    role: &'static str,
    start_ts: u64,
    global_xid: u64,
    writes: usize,
) -> tracing::Span {
    tracing::debug_span!(
        target: ROUTE_TARGET,
        "pg.prewrite",
        otel.kind = "client",
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        error.type = tracing::field::Empty,
        pg.range_id = integer(range_id.as_u32()),
        pg.role = role,
        pg.start_ts = integer(start_ts),
        pg.global_xid = integer(global_xid),
        pg.writes = integer(writes),
        pg.local = tracing::field::Empty,
    )
}

/// Build the span for resolving one participant against the durable decision.
#[must_use]
pub fn resolve_span(
    range_id: RangeId,
    role: &'static str,
    start_ts: u64,
    global_xid: u64,
    decision: &'static str,
    writes: usize,
) -> tracing::Span {
    tracing::debug_span!(
        target: ROUTE_TARGET,
        "pg.resolve",
        otel.kind = "client",
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        error.type = tracing::field::Empty,
        pg.range_id = integer(range_id.as_u32()),
        pg.role = role,
        pg.start_ts = integer(start_ts),
        pg.global_xid = integer(global_xid),
        pg.decision = decision,
        pg.writes = integer(writes),
        pg.local = tracing::field::Empty,
    )
}

/// Build the span covering the abort round of a timestamp transaction.
///
/// This span is separate from [`scatter_span`], because an abort also runs from
/// `ROLLBACK` and from the failed-statement cleanup, and the scatter span is
/// long closed at those points. An abort that half-completes is exactly the
/// state that needs its own `pg.outcome`.
#[must_use]
pub fn abort_scatter_span(
    primary_range: RangeId,
    start_ts: u64,
    global_xid: u64,
    participants: usize,
) -> tracing::Span {
    tracing::debug_span!(
        target: ROUTE_TARGET,
        "pg.abort_scatter",
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        error.type = tracing::field::Empty,
        pg.primary_range = integer(primary_range.as_u32()),
        pg.participant_ranges = tracing::field::Empty,
        pg.start_ts = integer(start_ts),
        pg.global_xid = integer(global_xid),
        pg.participants = integer(participants),
        pg.outcome = tracing::field::Empty,
    )
}

/// Record the participant list on an [`abort_scatter_span`].
pub fn record_abort_participants(span: &tracing::Span, participants: &[RangeId]) {
    if span.is_disabled() {
        return;
    }
    span.record("pg.participant_ranges", join_ranges(participants).as_str());
}

/// Build the span that covers the global-xid 2PC commit.
///
/// That commit is the other, distinct, distributed-commit protocol the gateway
/// drives.
#[must_use]
pub fn commit_global_span(tenant: &str, participants: &[RangeId]) -> tracing::Span {
    let span = tracing::debug_span!(
        target: ROUTE_TARGET,
        "pg.commit_global",
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        error.type = tracing::field::Empty,
        pg.tenant = tenant,
        pg.participants = integer(participants.len()),
        pg.participant_ranges = tracing::field::Empty,
        pg.txn.global_xid = tracing::field::Empty,
        pg.outcome = tracing::field::Empty,
    );
    if !span.is_disabled() {
        span.record("pg.participant_ranges", join_ranges(participants).as_str());
    }
    span
}

/// Record the global transaction id after the coordinator mints it.
pub fn record_global_xid(span: &tracing::Span, global_xid: u64) {
    if span.is_disabled() {
        return;
    }
    span.record("pg.txn.global_xid", integer(global_xid));
}

/// Build the span for preparing one participant of a global-xid transaction.
#[must_use]
pub fn prepare_span(range_id: RangeId, global_xid: u64, local: bool) -> tracing::Span {
    tracing::debug_span!(
        target: ROUTE_TARGET,
        "pg.prepare",
        otel.kind = "client",
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        error.type = tracing::field::Empty,
        pg.range_id = integer(range_id.as_u32()),
        pg.txn.global_xid = integer(global_xid),
        pg.local = local,
    )
}

/// Build the span that covers the dispatch of a routed statement to its owning
/// range.
///
/// `pg.local` separates the in-process seat from the network hop that
/// [`remote_statement_span`] covers.
#[must_use]
pub fn routed_statement_span(range_id: RangeId, kind: &'static str, local: bool) -> tracing::Span {
    tracing::debug_span!(
        target: ROUTE_TARGET,
        "pg.routed_statement",
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        error.type = tracing::field::Empty,
        pg.range_id = integer(range_id.as_u32()),
        pg.statement_kind = kind,
        pg.local = local,
    )
}

/// Build the span that covers a statement forwarded to a range this node does
/// not host.
#[must_use]
pub fn remote_statement_span(range_id: RangeId, kind: &'static str) -> tracing::Span {
    tracing::debug_span!(
        target: ROUTE_TARGET,
        "pg.remote_statement",
        otel.kind = "client",
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        error.type = tracing::field::Empty,
        pg.range_id = integer(range_id.as_u32()),
        pg.statement_kind = kind,
    )
}

/// Build the span that covers DDL.
///
/// DDL always runs on the range-0 catalog owner and then waits for the
/// cluster-wide visibility barrier.
#[must_use]
pub fn ddl_span(tenant: &str) -> tracing::Span {
    tracing::debug_span!(
        target: ROUTE_TARGET,
        "pg.ddl",
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        error.type = tracing::field::Empty,
        pg.tenant = tenant,
        pg.range_id = integer(RangeId::COORDINATOR.as_u32()),
        pg.local = tracing::field::Empty,
    )
}

/// Record whether the DDL ran on an in-process range-0 seat or the gateway
/// forwarded it.
pub fn record_local(span: &tracing::Span, local: bool) {
    if span.is_disabled() {
        return;
    }
    span.record("pg.local", local);
}

/// Mark a span as failed.
///
/// This function sets `otel.status_code = "ERROR"`. It never sets `"OK"`, so a
/// successful span stays `Unset`. It also sets the SQLSTATE as both `error.type`
/// and `db.response.status_code`. The SQLSTATE is a five-character enumeration,
/// which makes it the right low-cardinality discriminator to group failures by.
/// A span ignores the fields it did not declare, so one helper serves every span
/// here.
///
/// Two details of the `tracing-opentelemetry` contract are load-bearing, and
/// both fail silently when broken. First, the message field is spelled
/// **`otel.status_description`**. `tracing-opentelemetry` does not recognize
/// `otel.status_message` and exports it as an ordinary attribute, which leaves
/// the status description empty. Second, the code must record the status code
/// **first**. A record of the status code installs a status with an empty
/// description, which would erase a description recorded before it.
pub fn record_error(span: &tracing::Span, sqlstate: &str, message: &str) {
    if span.is_disabled() {
        return;
    }
    span.record("otel.status_code", "ERROR");
    span.record(
        "otel.status_description",
        truncate(message, MAX_STATUS_DESCRIPTION_BYTES),
    );
    span.record("error.type", sqlstate);
    span.record("db.response.status_code", sqlstate);
}

/// Record the row counts a statement produced.
///
/// The caller accumulates the counters and records them once. A page-level span
/// would emit hundreds of spans for one large result, and the exporter would
/// drop all of them.
pub fn record_rows(span: &tracing::Span, returned_rows: Option<u64>, result_pages: Option<usize>) {
    if span.is_disabled() {
        return;
    }
    if let Some(returned_rows) = returned_rows {
        span.record("db.response.returned_rows", integer(returned_rows));
    }
    if let Some(result_pages) = result_pages {
        span.record("pg.result_pages", integer(result_pages));
    }
}

/// Record the rows a statement affected, and the transaction it ran under.
pub fn record_statement_outcome(
    span: &tracing::Span,
    rows_affected: Option<u64>,
    global_xid: Option<u64>,
) {
    if span.is_disabled() {
        return;
    }
    if let Some(rows_affected) = rows_affected {
        span.record("pg.rows_affected", integer(rows_affected));
    }
    if let Some(global_xid) = global_xid {
        span.record("pg.txn.global_xid", integer(global_xid));
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn sql_text_flag_defaults_off_and_accepts_affirmatives() {
        for (value, want) in [
            (None, false),
            (Some(""), false),
            (Some("0"), false),
            (Some("false"), false),
            (Some("no"), false),
            (Some("maybe"), false),
            (Some("1"), true),
            (Some("true"), true),
            (Some(" TRUE "), true),
            (Some("Yes"), true),
            (Some("on"), true),
        ] {
            check!(sql_text_flag(value) == want, "value {value:?}");
        }
    }

    #[test]
    fn operation_name_maps_leading_verb() {
        for (sql, want) in [
            ("select id from t1", "SELECT"),
            ("  SeLeCt 1", "SELECT"),
            ("INSERT INTO t1 VALUES (1)", "INSERT"),
            ("update t1 set a = 1", "UPDATE"),
            ("delete from t1", "DELETE"),
            ("create table t1 (id int4)", "CREATE"),
            ("start transaction", "START"),
            ("vacuum", "OTHER"),
            ("", "OTHER"),
            ("42", "OTHER"),
        ] {
            check!(operation_name(sql) == want, "sql {sql:?}");
        }
    }

    #[test]
    fn query_summary_pairs_operation_with_collection() {
        check!(query_summary("SELECT", Some("orders")) == "SELECT orders");
        check!(query_summary("COMMIT", None) == "COMMIT");
    }

    #[test]
    fn join_ranges_renders_comma_separated_ids() {
        check!(join_ranges(&[]).is_empty());
        check!(join_ranges(&[RangeId::new(7)]) == "7");
        check!(join_ranges(&[RangeId::new(0), RangeId::new(3), RangeId::new(11)]) == "0,3,11");
    }

    #[test]
    fn integer_saturates_instead_of_wrapping() {
        check!(integer(0_u32) == 0);
        check!(integer(150_u64) == 150);
        check!(integer(u64::MAX) == i64::MAX);
        check!(integer(usize::MAX) == i64::MAX);
    }

    #[test]
    fn truncate_stops_on_a_character_boundary() {
        check!(truncate("abc", 8) == "abc");
        check!(truncate("abcdef", 3) == "abc");
        // Cutting at 2 lands mid-codepoint of the 3-byte '€', so the helper
        // steps back rather than producing invalid UTF-8.
        check!(truncate("€uro", 2).is_empty());
        check!(truncate("€uro", 3) == "€");
    }

    #[test]
    fn scatter_outcome_values_are_stable() {
        for (outcome, want) in [
            (ScatterOutcome::NoWrites, "no_writes"),
            (ScatterOutcome::Aborted, "aborted"),
            (ScatterOutcome::Prepared, "prepared"),
            (ScatterOutcome::Indeterminate, "indeterminate"),
            (ScatterOutcome::Committed, "committed"),
        ] {
            check!(outcome.as_str() == want);
        }
    }

    /// Every span builder must be disabled when no subscriber is installed, and
    /// every `record_*` helper must tolerate that. Every statement on a gateway
    /// that exports nothing would pay for a panic or an allocation here.
    #[test]
    fn builders_are_inert_without_a_subscriber() {
        let spans = [
            route_span("t"),
            statement_span("t", "select 1", "query", Some("t1"), Some(1u64), false),
            scatter_span("t"),
            prewrite_span(RangeId::new(1), ROLE_PRIMARY, 1, 2, 3),
            resolve_span(RangeId::new(1), ROLE_SECONDARY, 1, 2, "committed", 3),
            abort_scatter_span(RangeId::new(1), 1, 2, 3),
            commit_global_span("t", &[RangeId::new(1)]),
            prepare_span(RangeId::new(1), 2, false),
            routed_statement_span(RangeId::new(1), "dml", true),
            remote_statement_span(RangeId::new(1), "dml"),
            ddl_span("t"),
        ];
        for span in &spans {
            check!(span.is_disabled());
            record_route(span, "query", RangeId::new(1), Some(2), Some(3));
            record_scatter_mode(span, true, false);
            record_scatter_identity(span, 1, 2);
            record_scatter_plan(span, &[RangeId::new(1)], 1, 2);
            record_primary_range(span, RangeId::new(1));
            record_commit_ts(span, 9);
            record_outcome(span, ScatterOutcome::Committed);
            record_abort_participants(span, &[RangeId::new(1)]);
            record_global_xid(span, 3);
            record_local(span, true);
            record_rows(span, Some(4), Some(2));
            record_statement_outcome(span, Some(1), Some(2));
            record_error(span, "40001", "conflict");
        }
    }

    /// The target string is what an operator types into `CRABKA_OTLP_FILTER`,
    /// and what `crabka-gres` names in its default filter. A rename that misses
    /// either side silently stops the export of the whole gateway tier.
    #[test]
    fn route_target_is_the_documented_directive() {
        check!(ROUTE_TARGET == "crabka_gres_ranges::route");
        assert!(let Some(("crabka_gres_ranges", "route")) = ROUTE_TARGET.split_once("::"));
    }
}
