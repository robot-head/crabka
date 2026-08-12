//! Executor tracing: targets, span builders, and the SQL-text policy.
//!
//! The executor creates every span it emits under one of two dedicated
//! `tracing` targets. [`STATEMENT_TARGET`] carries the per-statement tier and
//! [`EXEC_TARGET`] carries the finer-grained internals. Neither target appears
//! on a process's stdout `fmt` filter, so a `gres` that runs without OTLP pays
//! one disabled level check per callsite and nothing else.
//!
//! ## Zero cost when off
//!
//! A disabled callsite is a load and a branch, but a span macro's *field
//! expressions still evaluate*. Anything more expensive than a field read
//! therefore sits behind an explicit
//! `tracing::enabled!(target: STATEMENT_TARGET, Level::DEBUG)` guard at the
//! callsite, with a [`tracing::Span::none`] fallback. Two such expressions are
//! [`query_summary`], which allocates, and the catalog lookup behind
//! `pg.table_id`. Spans whose fields are scalars already available, such as a
//! read timestamp or a snapshot bound, use the bare macro.
//!
//! The span builders here are plain functions and not `#[instrument]`. The
//! attribute cannot express that guard, and nearly every span below needs
//! [`tracing::Span::record`] for outcome fields that are only known once the
//! statement has run.
//!
//! ## SQL text
//!
//! Three tiers, of which the default is the middle one:
//!
//! 1. `db.query.summary`, for example `"SELECT orders"`, is always on.
//!    [`query_summary`] derives it from the already-parsed [`Statement`], so it
//!    costs no second parse and leaks no literals.
//! 2. `db.operation.name`, `db.collection.name`, `db.namespace` and
//!    `pg.table_id` are always on.
//! 3. `db.query.text`, the verbatim SQL, is **off by default**. It sits behind
//!    [`sql_text_enabled`] (`CRABKA_OTLP_SQL_TEXT`) and is truncated at
//!    [`MAX_SQL_TEXT_BYTES`]. It is the one attribute here that can carry
//!    secrets, as in `INSERT INTO users VALUES ('<ssn>', …)` and
//!    `ALTER ROLE … PASSWORD '…'`.
//!
//! SQL is recorded on `db.statement` only. Children inherit it through the
//! trace, so a repeat multiplies the exported bytes for no extra signal.

use std::sync::LazyLock;

use crabka_pgparser::ast::{
    QueryBody, QueryExpr, RelationRef, RoutineObject, SelectStmt, SetExpr, Statement, TableExpr,
    UtilityStatement,
};

/// `tracing` target that carries the per-statement span tier: `pg.parse.sql`,
/// `db.statement`, `pg.select` and `pg.write`.
pub const STATEMENT_TARGET: &str = "crabka_pgexec::statement";

/// `tracing` target that carries the executor's internals: the timestamp
/// grant, the read-context gate, and, from the executor proper, scans and row
/// locks.
pub const EXEC_TARGET: &str = "crabka_pgexec::exec";

/// Session GUC a client sets to give the engine a W3C `traceparent`.
///
/// This GUC is for drivers that cannot append a sqlcommenter to the statement
/// itself.
///
/// `SET crabka.traceparent = '00-…-01'` and
/// `SELECT set_config('crabka.traceparent', '…', true)` both work without any
/// dedicated GUC machinery. A two-part `extension.parameter` name is accepted
/// as a customized option and stored as a string.
pub const TRACEPARENT_GUC: &str = "crabka.traceparent";

/// Companion of [`TRACEPARENT_GUC`] that carries W3C `tracestate`.
///
/// It is read only when a `traceparent` is present, because vendor state alone
/// carries no trace.
pub const TRACESTATE_GUC: &str = "crabka.tracestate";

/// Byte cap on `db.query.text`.
///
/// A longer statement is truncated on a UTF-8 boundary and not dropped, because
/// its head still identifies it.
pub const MAX_SQL_TEXT_BYTES: usize = 4096;

/// Byte cap on `otel.status_description`.
///
/// A long error message is a diagnostic, not a discriminator. Queries group by
/// the SQLSTATE on `error.type`.
pub const MAX_STATUS_MESSAGE_BYTES: usize = 512;

/// Whether `db.query.text` may carry verbatim SQL.
///
/// The switch is read once from `CRABKA_OTLP_SQL_TEXT`. It is kept here, and
/// not in `crabka-telemetry`, so that this crate stays publishable. It then
/// needs no dependency on the unpublished OTLP pipeline crate to answer the
/// question.
#[must_use]
pub fn sql_text_enabled() -> bool {
    static ENABLED: LazyLock<bool> =
        LazyLock::new(|| env_flag(std::env::var("CRABKA_OTLP_SQL_TEXT").ok().as_deref()));
    *ENABLED
}

/// Interpret an environment switch.
///
/// Every value other than an affirmative spelling leaves the switch off,
/// including an unset or empty variable. The PII-bearing tier therefore cannot
/// be turned on by accident.
fn env_flag(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// Render an integer attribute as the `i64` OTLP can actually carry.
///
/// OTLP has no unsigned integer type, so `tracing-opentelemetry` stringifies a
/// `u64` or `usize` field instead of an export as a number. A string attribute
/// cannot be compared, sorted or range-filtered, so a Grafana query such as
/// `pg.rows_affected > 1000` silently matches nothing. Every numeric attribute
/// in this module therefore goes through this function first.
///
/// The conversion saturates, and it neither wraps nor fails. An xid and a read
/// timestamp are monotonic, so a value above `i64::MAX` is beyond anything a
/// real cluster reaches. A clamp keeps such a value sorted at the top, where it
/// belongs, instead of flipped negative or lost.
pub(crate) fn integer<T: TryInto<i64>>(value: T) -> i64 {
    // Every type used with this is unsigned, so the only conversion that can
    // fail is one that overflows upward.
    value.try_into().unwrap_or(i64::MAX)
}

/// Truncate `text` to at most `max` bytes, cutting on a UTF-8 boundary.
#[must_use]
pub fn truncate(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// The SQL command a statement runs, as `db.operation.name`.
///
/// This match is exhaustive on purpose. A newly supported statement kind has to
/// name itself here, and it cannot silently export as something generic.
#[must_use]
pub fn statement_operation(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::CompatibilityRefusal(command) => command.command_name(),
        Statement::CreateTrigger(_) => "CREATE TRIGGER",
        Statement::AlterTrigger { .. } => "ALTER TRIGGER",
        Statement::DropTrigger { .. } => "DROP TRIGGER",
        Statement::CreateEventTrigger(_) => "CREATE EVENT TRIGGER",
        Statement::AlterEventTrigger { .. } => "ALTER EVENT TRIGGER",
        Statement::DropEventTrigger { .. } => "DROP EVENT TRIGGER",
        Statement::CreateTable { .. } => "CREATE TABLE",
        Statement::CreateIndex { .. } => "CREATE INDEX",
        Statement::Comment { .. } => "COMMENT",
        Statement::DropIndex { .. } => "DROP INDEX",
        Statement::CreateView { .. } => "CREATE VIEW",
        Statement::DropTable { .. } => "DROP TABLE",
        Statement::DropView { .. } => "DROP VIEW",
        Statement::CreateSchema { .. } => "CREATE SCHEMA",
        Statement::AlterSchema { .. } => "ALTER SCHEMA",
        Statement::DropSchema { .. } => "DROP SCHEMA",
        Statement::AlterTable { .. } => "ALTER TABLE",
        Statement::Insert { .. } => "INSERT",
        Statement::Query(_) => "SELECT",
        Statement::Begin { .. } => "BEGIN",
        Statement::Commit { .. } => "COMMIT",
        Statement::Rollback { .. } => "ROLLBACK",
        Statement::Update { .. } => "UPDATE",
        Statement::Delete { .. } => "DELETE",
        Statement::Merge { .. } => "MERGE",
        Statement::CreateTableAs { .. } => "CREATE TABLE AS",
        Statement::Vacuum(_) => "VACUUM",
        Statement::Truncate { .. } => "TRUNCATE TABLE",
        Statement::Set { .. } => "SET",
        Statement::Show { .. } => "SHOW",
        Statement::Reset { .. } => "RESET",
        Statement::CreateRole { .. } => "CREATE ROLE",
        Statement::DropRole { .. } => "DROP ROLE",
        Statement::GrantTablePrivileges { .. } => "GRANT",
        Statement::RevokeTablePrivileges { .. } => "REVOKE",
        Statement::SetRole { .. } => "SET ROLE",
        Statement::CreateFdw { .. } => "CREATE FOREIGN DATA WRAPPER",
        Statement::DropFdw { .. } => "DROP FOREIGN DATA WRAPPER",
        Statement::CreateServer { .. } => "CREATE SERVER",
        Statement::AlterServer { .. } => "ALTER SERVER",
        Statement::DropServer { .. } => "DROP SERVER",
        Statement::CreateUserMapping { .. } => "CREATE USER MAPPING",
        Statement::AlterUserMapping { .. } => "ALTER USER MAPPING",
        Statement::DropUserMapping { .. } => "DROP USER MAPPING",
        Statement::CreateForeignTable { .. } => "CREATE FOREIGN TABLE",
        Statement::DropForeignTable { .. } => "DROP FOREIGN TABLE",
        Statement::ImportForeignSchema { .. } => "IMPORT FOREIGN SCHEMA",
        Statement::Listen { .. } => "LISTEN",
        Statement::Notify { .. } => "NOTIFY",
        Statement::Unlisten { .. } => "UNLISTEN",
        Statement::Savepoint { .. } => "SAVEPOINT",
        Statement::RollbackToSavepoint { .. } => "ROLLBACK TO SAVEPOINT",
        Statement::ReleaseSavepoint { .. } => "RELEASE",
        Statement::DeclareCursor { .. } => "DECLARE CURSOR",
        Statement::FetchCursor { move_only, .. } => {
            if *move_only {
                "MOVE"
            } else {
                "FETCH"
            }
        }
        Statement::CloseCursor { .. } => "CLOSE CURSOR",
        Statement::PrepareStatement { .. } => "PREPARE",
        Statement::ExecuteStatement { .. } => "EXECUTE",
        Statement::Deallocate { .. } => "DEALLOCATE",
        Statement::LockTable { .. } => "LOCK TABLE",
        Statement::Explain { .. } => "EXPLAIN",
        Statement::Discard { .. } => "DISCARD",
        Statement::CreateRoutine(routine) => match routine.object {
            RoutineObject::Function => "CREATE FUNCTION",
            RoutineObject::Procedure => "CREATE PROCEDURE",
            RoutineObject::Routine => "CREATE ROUTINE",
        },
        Statement::DropRoutine { object, .. } => match object {
            RoutineObject::Function => "DROP FUNCTION",
            RoutineObject::Procedure => "DROP PROCEDURE",
            RoutineObject::Routine => "DROP ROUTINE",
        },
        Statement::AlterRoutine { object, .. } => match object {
            RoutineObject::Function => "ALTER FUNCTION",
            RoutineObject::Procedure => "ALTER PROCEDURE",
            RoutineObject::Routine => "ALTER ROUTINE",
        },
        Statement::Call { .. } => "CALL",
        Statement::DoBlock { .. } => "DO",
        Statement::CreateType { .. } => "CREATE TYPE",
        Statement::AlterType { .. } => "ALTER TYPE",
        Statement::DropType { .. } => "DROP TYPE",
        Statement::CreateDomain { .. } => "CREATE DOMAIN",
        Statement::AlterDomain { .. } => "ALTER DOMAIN",
        Statement::DropDomain { .. } => "DROP DOMAIN",
        Statement::Cluster(_) => "CLUSTER",
        Statement::AlterRole { .. } => "ALTER ROLE",
        Statement::GrantSchemaPrivileges { .. } => "GRANT",
        Statement::RevokeSchemaPrivileges { .. } => "REVOKE",
        Statement::GrantRoles { .. } => "GRANT ROLES",
        Statement::RevokeRoles { .. } => "REVOKE ROLES",
        Statement::AlterIndex { .. } => "ALTER INDEX",
        Statement::AlterView { .. } => "ALTER VIEW",
        Statement::CreateMaterializedView { .. } => "CREATE MATERIALIZED VIEW",
        Statement::RefreshMaterializedView { .. } => "REFRESH MATERIALIZED VIEW",
        Statement::DropMaterializedView { .. } => "DROP MATERIALIZED VIEW",
        Statement::Copy(_) => "COPY",
        Statement::CreateAggregate(_) => "CREATE AGGREGATE",
        Statement::DropAggregate { .. } => "DROP AGGREGATE",
        Statement::AlterAggregate { .. } => "ALTER AGGREGATE",
        Statement::CreatePolicy(_) => "CREATE POLICY",
        Statement::AlterPolicy { .. } => "ALTER POLICY",
        Statement::DropPolicy { .. } => "DROP POLICY",
        Statement::Utility(utility) => match utility {
            UtilityStatement::Analyze(_) => "ANALYZE",
            UtilityStatement::Reindex(_) => "REINDEX",
            UtilityStatement::CreateOperatorFamily { .. } => "CREATE OPERATOR FAMILY",
            UtilityStatement::CreateOperatorClass { .. } => "CREATE OPERATOR CLASS",
            // The operator *objects* are not the operator: their tags name the
            // kind, and the bare `ALTER`/`DROP OPERATOR` tags belong to the
            // operator itself.
            UtilityStatement::AlterOperatorObject { kind, .. } => match kind {
                crabka_pgparser::ast::OperatorObjectKind::Class => "ALTER OPERATOR CLASS",
                crabka_pgparser::ast::OperatorObjectKind::Family => "ALTER OPERATOR FAMILY",
            },
            UtilityStatement::DropOperatorObject { kind, .. } => match kind {
                crabka_pgparser::ast::OperatorObjectKind::Class => "DROP OPERATOR CLASS",
                crabka_pgparser::ast::OperatorObjectKind::Family => "DROP OPERATOR FAMILY",
            },
            UtilityStatement::CreateOperator(_) => "CREATE OPERATOR",
            UtilityStatement::DropOperator { .. } => "DROP OPERATOR",
            UtilityStatement::Load { .. } => "LOAD",
            UtilityStatement::SecurityLabel { .. } => "SECURITY LABEL",
            UtilityStatement::CreateTablespace { .. } => "CREATE TABLESPACE",
            UtilityStatement::DropTablespace { .. } => "DROP TABLESPACE",
            UtilityStatement::AlterTablespace { .. } => "ALTER TABLESPACE",
            UtilityStatement::TextSearch(_) => "TEXT SEARCH",
            UtilityStatement::Checkpoint => "CHECKPOINT",
            UtilityStatement::AlterSystem { .. } => "ALTER SYSTEM",
            UtilityStatement::SetConstraints { .. } => "SET CONSTRAINTS",
            UtilityStatement::SetSessionAuthorization { .. } => "SET SESSION AUTHORIZATION",
        },
    }
}

/// The relation a statement names, when it names exactly one worth reporting as
/// `db.collection.name`.
///
/// A multi-relation statement, such as `DROP TABLE a, b` or a join, reports the
/// first relation. `PostgreSQL`'s own error messages do the same, and that is
/// what makes a summary such as `"SELECT orders"` stable for grouping.
/// Statements that name no relation, such as `BEGIN`, `SET` and `DO`, return
/// `None`, and their summary is the bare operation.
#[must_use]
pub fn statement_relation(stmt: &Statement) -> Option<&RelationRef> {
    match stmt {
        Statement::Insert { table, .. }
        | Statement::Update { table, .. }
        | Statement::Delete { table, .. }
        | Statement::Merge { table, .. }
        | Statement::AlterTable { table, .. }
        | Statement::AlterTrigger { table, .. }
        | Statement::DropTrigger { table, .. }
        | Statement::CreateIndex { table, .. } => Some(table),
        Statement::GrantTablePrivileges { tables, .. }
        | Statement::RevokeTablePrivileges { tables, .. } => tables.first(),
        Statement::CreateTable { name, .. }
        | Statement::CreateTableAs { name, .. }
        | Statement::CreateView { name, .. }
        | Statement::DropView { name, .. }
        | Statement::DropIndex { name, .. }
        | Statement::CreateForeignTable { name, .. }
        | Statement::DropForeignTable { name, .. }
        | Statement::CreateType { name, .. }
        | Statement::AlterType { name, .. }
        | Statement::CreateDomain { name, .. }
        | Statement::AlterDomain { name, .. } => Some(name),
        Statement::Truncate { targets, .. } => targets.first().map(|t| &t.name),
        Statement::DropTable { names, .. }
        | Statement::LockTable { tables: names, .. }
        | Statement::DropType { names, .. }
        | Statement::DropDomain { names, .. } => names.first(),
        Statement::Query(query) => query_relation(query),
        Statement::DeclareCursor { query, .. } => query_relation(query),
        // EXPLAIN and EXECUTE report the relation of the statement they wrap;
        // that is the one an operator is looking for.
        Statement::Explain { statement, .. } => statement_relation(statement),
        Statement::PrepareStatement { statement, .. } => statement_relation(statement),
        _ => None,
    }
}

/// The first base relation a query reads. This function follows set operations
/// and nested bodies down their left spine.
fn query_relation(query: &QueryExpr) -> Option<&RelationRef> {
    fn from_set_expr(body: &SetExpr) -> Option<&RelationRef> {
        match body {
            SetExpr::Query(QueryBody::Select(select)) => from_select(select),
            SetExpr::Query(QueryBody::Values(_)) => None,
            SetExpr::Query(QueryBody::Nested(nested)) => from_set_expr(&nested.body),
            SetExpr::SetOp { left, .. } => from_set_expr(left),
        }
    }

    fn from_select(select: &SelectStmt) -> Option<&RelationRef> {
        select.from.iter().find_map(from_table_expr)
    }

    fn from_table_expr(item: &TableExpr) -> Option<&RelationRef> {
        match item {
            TableExpr::Table { name, .. } => Some(name),
            TableExpr::Join { left, .. } => from_table_expr(left),
            TableExpr::Derived { subquery, .. } => from_set_expr(&subquery.body),
            TableExpr::Function { .. } | TableExpr::JsonTable(_) => None,
        }
    }

    from_set_expr(&query.body)
}

/// The low-cardinality `db.query.summary` an operator groups by.
///
/// The summary is `"SELECT orders"`, `"INSERT orders"`, or a bare
/// `"CREATE TABLE"` when the statement names no relation.
///
/// It is deliberately *not* a normalized statement such as `WHERE id = ?`. A
/// normalized statement would cost a second pass over the SQL for grouping
/// power the operation-plus-relation pair already gives, and it would re-open
/// the literal-leak question the summary exists to close.
#[must_use]
pub fn query_summary(operation: &str, collection: Option<&str>) -> String {
    match collection {
        Some(collection) if !collection.is_empty() => format!("{operation} {collection}"),
        _ => operation.to_owned(),
    }
}

/// The attributes `db.statement` carries from the moment it opens.
///
/// These fields are grouped into a struct because they are gathered together.
/// The summary and the resolved relation come from one pass over the parsed
/// statement. A nine-argument builder would also be unreadable at the
/// callsite.
#[derive(Debug, Clone, Copy, Default)]
pub struct StatementFields<'a> {
    /// `db.query.summary`, and the span's `otel.name`.
    pub summary: &'a str,
    /// `db.operation.name`.
    pub operation: &'a str,
    /// `db.collection.name`, the resolved relation name, when there is one.
    pub collection: Option<&'a str>,
    /// `db.namespace`, the schema the relation resolved in.
    pub namespace: Option<&'a str>,
    /// `pg.table_id`, when the relation exists in the catalog.
    pub table_id: Option<u32>,
    /// Whether the statement opened its own transaction instead of running
    /// inside a client-opened block.
    pub implicit_txn: bool,
    /// Verbatim SQL for `db.query.text`.
    ///
    /// Callers pass `None` unless [`sql_text_enabled`] is true.
    /// [`statement_span`] truncates what it is given.
    pub sql: Option<&'a str>,
}

/// Build the per-statement span.
///
/// The caller is responsible for the [`tracing::enabled!`] guard. A gather of
/// [`StatementFields`] costs an allocation and a catalog lookup, and neither
/// must happen when the target is off.
#[must_use]
pub fn statement_span(fields: &StatementFields<'_>) -> tracing::Span {
    let span = tracing::debug_span!(
        target: STATEMENT_TARGET,
        "db.statement",
        otel.kind = "internal",
        otel.name = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        db.system.name = "postgresql",
        db.query.summary = tracing::field::Empty,
        db.operation.name = tracing::field::Empty,
        db.collection.name = tracing::field::Empty,
        db.namespace = tracing::field::Empty,
        db.query.text = tracing::field::Empty,
        db.response.status_code = tracing::field::Empty,
        db.response.returned_rows = tracing::field::Empty,
        "error.type" = tracing::field::Empty,
        pg.table_id = tracing::field::Empty,
        pg.txn.implicit = fields.implicit_txn,
        pg.txn.xid = tracing::field::Empty,
        pg.txn.global_xid = tracing::field::Empty,
        pg.rows_affected = tracing::field::Empty,
    );
    span.record("otel.name", fields.summary);
    span.record("db.query.summary", fields.summary);
    span.record("db.operation.name", fields.operation);
    if let Some(collection) = fields.collection {
        span.record("db.collection.name", collection);
    }
    if let Some(namespace) = fields.namespace {
        span.record("db.namespace", namespace);
    }
    if let Some(table_id) = fields.table_id {
        span.record("pg.table_id", integer(table_id));
    }
    if let Some(sql) = fields.sql {
        span.record("db.query.text", truncate(sql, MAX_SQL_TEXT_BYTES));
    }
    span
}

/// Build the span covering `crabka_pgparser::parse`.
#[must_use]
pub fn parse_span(sql_bytes: usize) -> tracing::Span {
    tracing::debug_span!(
        target: STATEMENT_TARGET,
        "pg.parse.sql",
        otel.kind = "internal",
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        db.response.status_code = tracing::field::Empty,
        "error.type" = tracing::field::Empty,
        pg.sql.bytes = integer(sql_bytes),
        pg.statements = tracing::field::Empty,
    )
}

/// Build the span covering a read statement's execution.
///
/// `fast_path` tells the streaming cursor from the materializing path. The
/// streaming cursor projects and filters row by row directly onto the wire. The
/// snapshot and read timestamp are settled only once the span is open, so this
/// function declares them empty and [`record_select_snapshot`] fills them in.
#[must_use]
pub fn select_span(fast_path: bool) -> tracing::Span {
    tracing::debug_span!(
        target: STATEMENT_TARGET,
        "pg.select",
        otel.kind = "internal",
        pg.read_ts = tracing::field::Empty,
        pg.snapshot.xmin = tracing::field::Empty,
        pg.snapshot.xmax = tracing::field::Empty,
        pg.repeatable_read = tracing::field::Empty,
        pg.fast_path = fast_path,
    )
}

/// Fill in the snapshot a read resolved against, on a span from
/// [`select_span`].
pub fn record_select_snapshot(
    span: &tracing::Span,
    read_ts: u64,
    snapshot_xmin: u64,
    snapshot_xmax: u64,
    repeatable_read: bool,
) {
    span.record("pg.read_ts", integer(read_ts));
    span.record("pg.snapshot.xmin", integer(snapshot_xmin));
    span.record("pg.snapshot.xmax", integer(snapshot_xmax));
    span.record("pg.repeatable_read", repeatable_read);
}

/// Fill in the snapshot a read context established, on a span from
/// [`read_context_span`].
pub fn record_read_context(span: &tracing::Span, snapshot_xmin: u64, snapshot_xmax: u64) {
    span.record("pg.snapshot.xmin", integer(snapshot_xmin));
    span.record("pg.snapshot.xmax", integer(snapshot_xmax));
}

/// Fill in the statement count a parse produced, on a span from
/// [`parse_span`].
pub fn record_parse_statements(span: &tracing::Span, statements: usize) {
    span.record("pg.statements", integer(statements));
}

/// Fill in the timestamp a grant returned, on a span from
/// [`timestamp_read_span`].
///
/// `local_fallback` marks the grant that came from this range's own sequence
/// because a single-shard bypass commit had lifted the durable horizon past the
/// global source. That is the one case where a read timestamp did not cross the
/// network.
pub fn record_timestamp_read(span: &tracing::Span, read_ts: u64, local_fallback: bool) {
    span.record("pg.read_ts", integer(read_ts));
    span.record("pg.timestamp.local_fallback", local_fallback);
}

/// Fill in the transaction identity a statement ran under.
///
/// Both xids are assigned lazily, so neither is known when the span opens.
pub fn record_transaction(span: &tracing::Span, xid: Option<u64>, global_xid: Option<u64>) {
    if let Some(xid) = xid {
        span.record("pg.txn.xid", integer(xid));
    }
    if let Some(global_xid) = global_xid {
        span.record("pg.txn.global_xid", integer(global_xid));
    }
}

/// Fill in a statement's row counts.
///
/// The counts are how many rows the statement returned to the client, and how
/// many rows its command tag reports it affected.
pub fn record_rows(span: &tracing::Span, returned_rows: Option<usize>, rows_affected: Option<u64>) {
    if let Some(returned_rows) = returned_rows {
        span.record("db.response.returned_rows", integer(returned_rows));
    }
    if let Some(rows_affected) = rows_affected {
        span.record("pg.rows_affected", integer(rows_affected));
    }
}

/// The row count a `PostgreSQL` command tag ends with, for example `5` for
/// `INSERT 0 5` and `3` for `UPDATE 3`.
///
/// The result is `None` for a tag whose last word is not a count, as in
/// `CREATE TABLE`. This function feeds [`record_rows`]'s `rows_affected`, which
/// is why it is here and not beside either of its callers.
pub(crate) fn command_tag_row_count(tag: &str) -> Option<u64> {
    tag.rsplit_once(' ')
        .and_then(|(_, count)| count.parse().ok())
}

/// Build the span covering a write statement's execution.
#[must_use]
pub fn write_span(implicit_txn: bool) -> tracing::Span {
    tracing::debug_span!(
        target: STATEMENT_TARGET,
        "pg.write",
        otel.kind = "internal",
        pg.txn.implicit = implicit_txn,
        pg.txn.xid = tracing::field::Empty,
        pg.rows_affected = tracing::field::Empty,
    )
}

/// Build the span covering a statement's read-timestamp grant.
///
/// `otel.kind = "client"`, because this is the one point on a read path that
/// can block on the network. The global timestamp source is remote whenever the
/// engine does not run solo.
#[must_use]
pub fn timestamp_read_span() -> tracing::Span {
    tracing::debug_span!(
        target: EXEC_TARGET,
        "pg.timestamp.read",
        otel.kind = "client",
        pg.read_ts = tracing::field::Empty,
        pg.timestamp.local_fallback = tracing::field::Empty,
    )
}

/// Build the span marking a hand-off onto the blocking pool.
///
/// `tokio::task::spawn_blocking` runs its closure on a pool thread with no
/// ambient `tracing` span, so the session re-enters the statement's span there
/// and opens this span beneath it. This makes the thread hop visible in a
/// waterfall. This span is also the parent that everything the executor opens
/// on that thread attaches to, including scans, row locks and commits.
///
/// `kind` names the worker. It is a static string, so the attribute stays
/// low-cardinality.
#[must_use]
pub fn blocking_worker_span(kind: &'static str) -> tracing::Span {
    tracing::trace_span!(
        target: EXEC_TARGET,
        "pg.blocking_worker",
        otel.kind = "internal",
        pg.worker = kind,
    )
}

/// Build the span covering snapshot establishment and the linearizability gate.
#[must_use]
pub fn read_context_span() -> tracing::Span {
    tracing::trace_span!(
        target: EXEC_TARGET,
        "pg.read_context",
        otel.kind = "internal",
        pg.snapshot.xmin = tracing::field::Empty,
        pg.snapshot.xmax = tracing::field::Empty,
    )
}

/// Mark `span` as failed, with the SQLSTATE as both the response status and the
/// error discriminator.
///
/// Only `ERROR` is ever recorded. A span that succeeded stays `Unset`, which is
/// what the OpenTelemetry specification asks of a non-client span and what
/// keeps "spans with a status" a useful query.
///
/// `sqlstate` is the 5-character code, which is correctly low-cardinality.
/// `message` is truncated to [`MAX_STATUS_MESSAGE_BYTES`].
///
/// The message goes on `otel.status_description`, which is the field name
/// `tracing-opentelemetry` maps onto the OpenTelemetry status description.
/// `otel.status_message` is not recognised and would be exported as an ordinary
/// attribute, which would leave the status description empty.
///
/// Order matters. A record of the code alone gives `Error` with an empty
/// description, and the description overwrites it, so the description must be
/// recorded second.
pub fn record_error(span: &tracing::Span, sqlstate: &str, message: &str) {
    span.record("otel.status_code", "ERROR");
    span.record(
        "otel.status_description",
        truncate(message, MAX_STATUS_MESSAGE_BYTES),
    );
    span.record("db.response.status_code", sqlstate);
    span.record("error.type", sqlstate);
}

/// Test-only scaffolding shared with the other span tests in this crate.
///
/// See [`tests::install_interest_floor`].
#[cfg(test)]
pub(crate) use tests::install_interest_floor;

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc, Mutex, Once,
            atomic::{AtomicBool, Ordering},
            mpsc::sync_channel,
        },
    };

    use assert2::check;
    use crabka_pgparser::parse;
    use tracing::{
        field::{Field, Visit},
        subscriber::Interest,
    };
    use tracing_subscriber::{Layer, layer::Context, prelude::*};

    use super::*;

    /// Every field a span was created with or later recorded, rendered as text.
    type Fields = BTreeMap<String, String>;

    struct Recorder<'a>(&'a mut Fields);

    impl Visit for Recorder<'_> {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().to_owned(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }
    }

    struct Capture(Arc<Mutex<Fields>>);

    impl<S: tracing::Subscriber> Layer<S> for Capture {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::Id,
            _ctx: Context<'_, S>,
        ) {
            attrs.record(&mut Recorder(&mut self.0.lock().expect("captured fields")));
        }

        fn on_record(
            &self,
            _id: &tracing::Id,
            values: &tracing::span::Record<'_>,
            _ctx: Context<'_, S>,
        ) {
            values.record(&mut Recorder(&mut self.0.lock().expect("captured fields")));
        }
    }

    /// A subscriber that collects nothing and enables nothing, installed once
    /// as the default for the whole test binary.
    ///
    /// `tracing` caches each callsite's [`Interest`] in a slot that belongs to
    /// the process, not to a subscriber. A cached `never` stops the span macro
    /// before it asks any subscriber at all, so a thread-local subscriber
    /// cannot overrule it. The thread that reaches a callsite first fills that
    /// slot, and a test thread with no subscriber of its own answers through
    /// `NoSubscriber`, whose answer is `never`. A span test on another thread
    /// then builds the same span under a perfectly good capturing subscriber
    /// and captures nothing.
    ///
    /// `InterestFloor` answers `sometimes` for every callsite, and
    /// `Interest::and` widens any disagreement to `sometimes`, so no callsite
    /// in this binary can be cached as `never`. Each span creation therefore
    /// asks the subscriber that is current *on the building thread*. A thread
    /// with no subscriber of its own reaches `enabled` here, which is `false`,
    /// so it still builds nothing.
    struct InterestFloor;

    impl tracing::Subscriber for InterestFloor {
        fn register_callsite(&self, _: &'static tracing::Metadata<'static>) -> Interest {
            Interest::sometimes()
        }

        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            false
        }

        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

        fn event(&self, _: &tracing::Event<'_>) {}

        fn enter(&self, _: &tracing::span::Id) {}

        fn exit(&self, _: &tracing::span::Id) {}
    }

    /// Put the [`InterestFloor`] under every callsite in the test binary.
    ///
    /// Call this before you install a thread-local subscriber and read spans
    /// back from it. Registering a subscriber rebuilds the cached interest of
    /// every callsite already known to `tracing`, so the first call also
    /// repairs any callsite that a subscriber-less thread already cached as
    /// `never`. Later calls do nothing.
    pub(crate) fn install_interest_floor() {
        static INSTALLED: Once = Once::new();
        INSTALLED.call_once(|| {
            tracing::subscriber::set_global_default(InterestFloor)
                .expect("the lib test binary installs no other global subscriber");
        });
    }

    /// Build a span under a capturing subscriber and return the fields that
    /// reached it.
    ///
    /// `tracing` silently drops a `record` for a field the callsite never
    /// declared, so this helper proves that the declarations and the recordings
    /// agree.
    fn captured(build: impl FnOnce()) -> Fields {
        install_interest_floor();
        let fields = Arc::new(Mutex::new(Fields::new()));
        let subscriber = tracing_subscriber::registry().with(
            Capture(Arc::clone(&fields))
                .with_filter(tracing_subscriber::filter::LevelFilter::TRACE),
        );
        tracing::subscriber::with_default(subscriber, build);
        // The subscriber is thread-local and dies with the closure, so nothing
        // can add to `fields` from here on. Sole ownership of the buffer is a
        // different question, and not one this helper may ask: `tracing_core`
        // keeps a weak reference to every live dispatcher and upgrades it,
        // briefly and from whichever thread happens to be registering a
        // subscriber of its own. Take the contents, not the allocation.
        std::mem::take(&mut *fields.lock().expect("captured fields"))
    }

    /// A callsite of this test's own, so that no other test decides its cached
    /// interest first. See [`InterestFloor`].
    fn isolated_probe_span() -> tracing::Span {
        tracing::debug_span!(target: STATEMENT_TARGET, "pg.test.probe", pg.probe = 7_i64)
    }

    /// A neighbour registering a subscriber of its own must not cost this test
    /// its captured fields.
    ///
    /// Every `Dispatch::new` anywhere in the process walks the registry of live
    /// dispatchers and upgrades each weak reference in turn, so the strong
    /// count of a subscriber this thread has already dropped is not this
    /// thread's to predict. The loop below forces that overlap rather than
    /// waiting for it.
    #[test]
    fn a_neighbours_subscriber_does_not_strand_the_captured_fields() {
        let stop = Arc::new(AtomicBool::new(false));
        let neighbours: Vec<_> = (0..4)
            .map(|_| {
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        drop(tracing::Dispatch::new(tracing_subscriber::registry()));
                    }
                })
            })
            .collect();

        for _ in 0..2048 {
            let fields = captured(|| {
                let _span = parse_span(11);
            });
            check!(fields.get("pg.sql.bytes").map(String::as_str) == Some("11"));
        }

        stop.store(true, Ordering::Relaxed);
        for neighbour in neighbours {
            neighbour
                .join()
                .expect("the neighbour thread does not panic");
        }
    }

    /// A subscriber-less neighbour that reaches a callsite first must not
    /// silence it for a thread that is capturing.
    ///
    /// The two channels pin the order that the flake used to reach by luck: the
    /// capturing subscriber is already installed here, then the bare thread
    /// registers the callsite, and only then does this thread build the span.
    #[test]
    fn a_subscriber_less_neighbour_cannot_silence_a_captured_callsite() {
        let (installed_tx, installed_rx) = sync_channel::<()>(0);
        let (registered_tx, registered_rx) = sync_channel::<()>(0);
        let neighbour = std::thread::spawn(move || {
            installed_rx
                .recv()
                .expect("the capturing subscriber is installed");
            // This thread has no subscriber, so it answers for the callsite
            // through the process default and not through the capture below.
            let _span = isolated_probe_span();
            registered_tx.send(()).expect("the callsite is registered");
        });

        let fields = captured(|| {
            installed_tx
                .send(())
                .expect("the capturing subscriber is installed");
            registered_rx.recv().expect("the callsite is registered");
            let _span = isolated_probe_span();
        });

        neighbour
            .join()
            .expect("the neighbour thread does not panic");
        check!(fields.get("pg.probe").map(String::as_str) == Some("7"));
    }

    fn only(sql: &str) -> Statement {
        parse(sql)
            .unwrap_or_else(|error| panic!("parse {sql}: {error}"))
            .pop()
            .unwrap_or_else(|| panic!("no statement in {sql}"))
    }

    #[test]
    fn summary_names_the_operation_and_its_relation() {
        let cases: [(&str, &str, &str); 12] = [
            ("SELECT * FROM orders", "SELECT", "SELECT orders"),
            (
                "SELECT * FROM sales.orders o JOIN items i ON o.id = i.id",
                "SELECT",
                "SELECT orders",
            ),
            ("SELECT 1", "SELECT", "SELECT"),
            ("INSERT INTO orders VALUES (1)", "INSERT", "INSERT orders"),
            ("UPDATE orders SET id = 1", "UPDATE", "UPDATE orders"),
            ("DELETE FROM orders", "DELETE", "DELETE orders"),
            (
                "CREATE TABLE orders (id int4)",
                "CREATE TABLE",
                "CREATE TABLE orders",
            ),
            ("DROP TABLE a, b", "DROP TABLE", "DROP TABLE a"),
            ("TRUNCATE orders", "TRUNCATE TABLE", "TRUNCATE TABLE orders"),
            ("BEGIN", "BEGIN", "BEGIN"),
            ("COMMIT", "COMMIT", "COMMIT"),
            ("SET search_path = public", "SET", "SET"),
        ];

        for (sql, operation, summary) in cases {
            let stmt = only(sql);
            let actual_operation = statement_operation(&stmt);
            let collection = statement_relation(&stmt).map(|relation| relation.name.as_str());
            check!(actual_operation == operation, "{sql}");
            check!(
                query_summary(actual_operation, collection) == summary,
                "{sql}"
            );
        }
    }

    #[test]
    fn a_set_operation_reports_its_left_spine() {
        let stmt = only("SELECT id FROM orders UNION ALL SELECT id FROM archive");
        check!(statement_relation(&stmt).map(|r| r.name.as_str()) == Some("orders"));
    }

    #[test]
    fn explain_reports_the_statement_it_wraps() {
        let stmt = only("EXPLAIN SELECT * FROM orders");
        check!(statement_operation(&stmt) == "EXPLAIN");
        check!(statement_relation(&stmt).map(|r| r.name.as_str()) == Some("orders"));
    }

    #[test]
    fn env_flag_only_accepts_affirmative_spellings() {
        for value in ["1", "true", "TRUE", "Yes", "on", " true "] {
            check!(env_flag(Some(value)), "{value}");
        }
        for value in ["", "0", "false", "no", "off", "maybe"] {
            check!(!env_flag(Some(value)), "{value}");
        }
        check!(!env_flag(None));
    }

    #[test]
    fn the_statement_span_carries_its_attributes_and_omits_the_absent_ones() {
        let fields = captured(|| {
            let _span = statement_span(&StatementFields {
                summary: "SELECT orders",
                operation: "SELECT",
                collection: Some("orders"),
                namespace: Some("public"),
                table_id: Some(42),
                implicit_txn: true,
                sql: None,
            });
        });

        check!(fields.get("otel.name").map(String::as_str) == Some("SELECT orders"));
        check!(fields.get("db.query.summary").map(String::as_str) == Some("SELECT orders"));
        check!(fields.get("db.operation.name").map(String::as_str) == Some("SELECT"));
        check!(fields.get("db.collection.name").map(String::as_str) == Some("orders"));
        check!(fields.get("db.namespace").map(String::as_str) == Some("public"));
        check!(fields.get("db.system.name").map(String::as_str) == Some("postgresql"));
        check!(fields.get("pg.table_id").map(String::as_str) == Some("42"));
        check!(fields.get("pg.txn.implicit").map(String::as_str) == Some("true"));
        // Tier 3 stays off unless the caller hands over the text.
        check!(!fields.contains_key("db.query.text"));
    }

    #[test]
    fn verbatim_sql_is_recorded_when_supplied_and_truncated_at_the_cap() {
        let fields = captured(|| {
            let _span = statement_span(&StatementFields {
                summary: "SELECT",
                operation: "SELECT",
                sql: Some("SELECT 1"),
                ..StatementFields::default()
            });
        });
        check!(fields.get("db.query.text").map(String::as_str) == Some("SELECT 1"));

        let oversized = "x".repeat(MAX_SQL_TEXT_BYTES + 64);
        let fields = captured(|| {
            let _span = statement_span(&StatementFields {
                summary: "SELECT",
                operation: "SELECT",
                sql: Some(&oversized),
                ..StatementFields::default()
            });
        });
        check!(fields.get("db.query.text").map(String::len) == Some(MAX_SQL_TEXT_BYTES));
    }

    #[test]
    fn record_error_sets_the_status_and_both_sqlstate_fields() {
        let long_message = "e".repeat(MAX_STATUS_MESSAGE_BYTES + 64);
        let fields = captured(|| {
            let span = statement_span(&StatementFields {
                summary: "SELECT orders",
                operation: "SELECT",
                ..StatementFields::default()
            });
            record_error(&span, "42P01", &long_message);
        });

        check!(fields.get("otel.status_code").map(String::as_str) == Some("ERROR"));
        check!(fields.get("db.response.status_code").map(String::as_str) == Some("42P01"));
        check!(fields.get("error.type").map(String::as_str) == Some("42P01"));
        check!(
            fields.get("otel.status_description").map(String::len)
                == Some(MAX_STATUS_MESSAGE_BYTES)
        );
    }

    #[test]
    fn the_parse_span_can_carry_a_syntax_error() {
        let fields = captured(|| {
            let span = parse_span(11);
            record_error(&span, "42601", "syntax error");
        });

        check!(fields.get("pg.sql.bytes").map(String::as_str) == Some("11"));
        check!(fields.get("otel.status_code").map(String::as_str) == Some("ERROR"));
        check!(fields.get("error.type").map(String::as_str) == Some("42601"));
    }

    #[test]
    fn the_select_span_distinguishes_the_streaming_fast_path() {
        let fields = captured(|| {
            let span = select_span(true);
            record_select_snapshot(&span, 7, 3, 9, true);
        });

        check!(fields.get("pg.fast_path").map(String::as_str) == Some("true"));
        check!(fields.get("pg.read_ts").map(String::as_str) == Some("7"));
        check!(fields.get("pg.snapshot.xmin").map(String::as_str) == Some("3"));
        check!(fields.get("pg.snapshot.xmax").map(String::as_str) == Some("9"));
        check!(fields.get("pg.repeatable_read").map(String::as_str) == Some("true"));

        let fields = captured(|| {
            let _span = select_span(false);
        });
        check!(fields.get("pg.fast_path").map(String::as_str) == Some("false"));
    }

    #[test]
    fn a_command_tag_yields_its_trailing_row_count() {
        // `INSERT` is the one tag with an oid before the count, so a naive
        // "second word" split would read 0 rows for every insert.
        let cases: [(&str, Option<u64>); 10] = [
            ("INSERT 0 5", Some(5)),
            ("INSERT 0 0", Some(0)),
            ("UPDATE 3", Some(3)),
            ("DELETE 12", Some(12)),
            ("SELECT 1", Some(1)),
            ("MERGE 7", Some(7)),
            // Tags whose last word is not a count, and single-word tags with
            // nothing to split on at all.
            ("CREATE TABLE", None),
            ("TRUNCATE TABLE", None),
            ("BEGIN", None),
            ("", None),
        ];
        for (tag, want) in cases {
            check!(command_tag_row_count(tag) == want, "tag {tag:?}");
        }
    }

    #[test]
    fn integer_saturates_rather_than_wrapping() {
        check!(integer(0_u64) == 0);
        check!(integer(42_u32) == 42);
        check!(integer(usize::MAX) == i64::MAX);
        // An xid or read timestamp above `i64::MAX` clamps to the top rather
        // than flipping negative, so it still sorts as the largest value.
        check!(integer(u64::MAX) == i64::MAX);
        let highest_exact = u64::try_from(i64::MAX).expect("i64::MAX is non-negative");
        check!(integer(highest_exact) == i64::MAX);
        check!(integer(highest_exact - 1) == i64::MAX - 1);
    }

    #[test]
    fn numeric_fields_are_recorded_as_signed_integers() {
        // `tracing` dispatches `u64` and `i64` to different visitor methods,
        // and only the signed one survives as an OTLP integer. Capturing the
        // debug rendering cannot tell them apart, so this pins the call path
        // instead: every builder below must route through `integer`.
        let fields = captured(|| {
            let span = statement_span(&StatementFields {
                summary: "SELECT orders",
                operation: "SELECT",
                table_id: Some(7),
                ..StatementFields::default()
            });
            record_transaction(&span, Some(u64::MAX), Some(9));
            record_rows(&span, Some(3), Some(5));
        });

        check!(fields.get("pg.table_id").map(String::as_str) == Some("7"));
        check!(fields.get("pg.txn.xid").map(String::as_str) == Some(&i64::MAX.to_string()[..]));
        check!(fields.get("pg.txn.global_xid").map(String::as_str) == Some("9"));
        check!(fields.get("db.response.returned_rows").map(String::as_str) == Some("3"));
        check!(fields.get("pg.rows_affected").map(String::as_str) == Some("5"));
    }

    #[test]
    fn truncate_cuts_on_a_utf8_boundary() {
        check!(truncate("abc", 8) == "abc");
        check!(truncate("abcdef", 3) == "abc");
        // "é" is two bytes, so a cut at 3 has to fall back to 2.
        check!(truncate("aébc", 3) == "aé");
        check!(truncate("é", 1).is_empty());
    }
}
