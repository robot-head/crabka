//! Map lower-crate error enums onto wire `PgError`s with the right SQLSTATE.

use crabka_pgcatalog::CatalogError;
use crabka_pgkv::KvError;
use crabka_pgparser::ParseError;
use crabka_pgtypes::TypeError;
use crabka_pgwire::error::PgError;

/// Executor-level error; converts to a non-fatal `PgError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecError {
    /// Deliberately recognized compatibility refusal with centralized wire metadata.
    CompatibilityRefusal(crabka_pgparser::ast::RefusalCommand),
    /// An execution error returned by a remote range owner.
    Remote(PgError),
    Parse(ParseError),
    Catalog(CatalogError),
    Type(TypeError),
    Kv(KvError),
    /// Column referenced that the row/table doesn't have (42703).
    UndefinedColumn(String),
    /// A column reference matched more than one table in scope (42702).
    AmbiguousColumn(String),
    /// A qualified reference named a table not in the FROM clause (42P01).
    MissingFromEntry(String),
    /// The same table name/alias appears twice in one FROM clause (42712).
    DuplicateAlias(String),
    /// In-grammar but unimplemented (0A000) — e.g. $1 parameters.
    Unsupported(String),
    /// Wrong type in a context that demands a specific one (42804) — e.g. a
    /// non-boolean WHERE.
    TypeMismatch(String),
    /// A NULL value was assigned to a NOT NULL column (23502).
    NotNullViolation(String),
    /// An existing row holds NULL in a column being constrained NOT NULL
    /// (23502) — `ALTER TABLE … ADD PRIMARY KEY` validation over stored rows.
    ColumnContainsNullValues {
        column: String,
        table: String,
    },
    /// The table definition itself is invalid (42P16) — e.g. adding a second
    /// primary key.
    InvalidTableDefinition(String),
    /// A row would duplicate a visible row in a unique index (23505).
    UniqueViolation(String),
    /// A grouping/aggregation rule was violated (42803) — e.g. a column that is
    /// neither grouped nor inside an aggregate, or a nested aggregate.
    Grouping(String),
    /// A call to a function that does not exist (42883) — e.g. an unknown name
    /// or an aggregate applied to an argument type/arity it does not accept.
    UndefinedFunction(String),
    /// An object was used in a way its kind does not allow (42809) — e.g.
    /// `DISTINCT`/`ALL` applied to a scalar (non-aggregate) function.
    WrongObjectType(String),
    /// A scalar subquery returned more than one row (21000).
    CardinalityViolation,
    /// A subquery used as an expression / IN / quantified source returned more than
    /// one column (42601).
    SubqueryColumns,
    /// PostgreSQL syntax/parse-analysis error surfaced by executor analysis
    /// (42601), used for SQL92 ORDER BY integer constants that cannot fit in
    /// a positional reference.
    Syntax(String),
    /// A bare ORDER BY output label matched more than one projected column
    /// (42702). PostgreSQL's message differs from generic column ambiguity.
    AmbiguousOrderBy(String),
    /// SP38: the branches of a UNION/INTERSECT/EXCEPT have different column counts
    /// (42601). `op` names the specific operator for the PG-exact message; `left`/
    /// `right` are kept for internal use (the message does not print them).
    SetOpColumnCount {
        op: crabka_pgparser::ast::SetOp,
        left: usize,
        right: usize,
    },
    /// SP39: VALUES rows have different column counts (42601).
    ValuesColumnCount,
    /// SP39: a derived table column alias list has the wrong number of names (42601).
    DerivedColumnAliasCount {
        expected: usize,
        got: usize,
    },
    /// SP38: an `ORDER BY <n>` positional reference is 0 or past the number of
    /// output columns (42P10 — invalid_column_reference).
    InvalidColumnReference(String),
    /// A statement was issued in an aborted transaction block (25P02): every
    /// command after an error (until COMMIT/ROLLBACK) is rejected.
    InFailedTransaction,
    /// A command forbidden inside an explicit transaction block (25001).
    ActiveSqlTransaction(String),
    /// A write conflicted with a concurrently-committed change under REPEATABLE
    /// READ (40001) — the client should retry the transaction.
    SerializationFailure,
    /// A deadlock was detected and this transaction was chosen as the victim (40P01).
    Deadlock,
    /// A lock wait by a cross-range transaction outlived its bounded-wait cap
    /// (40P01). A cycle spanning ranges is invisible to any single engine's
    /// wait-for graph, so the expired cap is treated as a presumed distributed
    /// deadlock; retrying is safe exactly as for a locally-detected one.
    LockWaitCapExpired,
    /// The write hit a node that is not the Raft leader; the client should retry.
    NotLeader,
    /// The write could not reach a majority (partition/timeout); no partial state
    /// was applied; the client should retry.
    Unavailable,
    /// SP37: a `SET`/`RESET` supplied a value the parameter cannot accept (22023) —
    /// e.g. an unknown time-zone name, or a non-default `datestyle`.
    InvalidParameterValue(String),
    /// SP37: a `SET`/`SHOW`/`RESET` named a configuration parameter that does not
    /// exist (42704).
    UnrecognizedParameter(String),
    /// An expression nested more deeply than the evaluator's `MAX_EVAL_DEPTH`
    /// (54001 / statement_too_complex). Defense-in-depth: the parser already caps
    /// the AST depth at parse time, so a tree this deep should never reach `eval`;
    /// this guard ensures that even if one did, evaluation returns a clean error
    /// rather than overflowing the stack and aborting the server process.
    StackDepthExceeded,
    /// A sequence advanced outside its configured bounds (2200H).
    SequenceLimit(String),
    /// Object state does not satisfy a command precondition (55000).
    ObjectNotInPrerequisiteState(String),
}

impl ExecError {
    pub fn into_pg(self) -> PgError {
        match self {
            ExecError::CompatibilityRefusal(command) => {
                PgError::error(command.sqlstate(), command.message())
            }
            ExecError::Remote(error) => error,
            ExecError::Parse(e) => PgError::error(e.sqlstate(), e.to_string()),
            ExecError::Catalog(e) => PgError::error(e.sqlstate(), e.to_string()),
            ExecError::Type(e) => PgError::error(e.sqlstate(), e.to_string()),
            ExecError::Kv(e) => match e {
                crabka_pgkv::KvError::Io(msg) => {
                    PgError::error("58030", format!("storage I/O error: {msg}"))
                }
                crabka_pgkv::KvError::CorruptRow(msg) => {
                    PgError::error("XX000", format!("corrupt storage: {msg}"))
                }
                crabka_pgkv::KvError::RestoreTargetNotEmpty => {
                    PgError::error("XX000", "restore target is not empty")
                }
                crabka_pgkv::KvError::UnsortedSnapshot => {
                    PgError::error("XX000", "snapshot keys are not strictly ascending")
                }
                crabka_pgkv::KvError::ConditionalPutUnsupported => PgError::error(
                    "0A000",
                    "the configured storage backend cannot fence timestamp transactions",
                ),
            },
            ExecError::UndefinedColumn(c) => {
                PgError::error("42703", format!("column \"{c}\" does not exist"))
            }
            ExecError::AmbiguousColumn(c) => {
                PgError::error("42702", format!("column reference \"{c}\" is ambiguous"))
            }
            ExecError::CardinalityViolation => PgError::error(
                "21000",
                "more than one row returned by a subquery used as an expression",
            ),
            ExecError::SubqueryColumns => {
                PgError::error("42601", "subquery must return only one column")
            }
            ExecError::Syntax(m) => PgError::error("42601", m),
            ExecError::AmbiguousOrderBy(n) => {
                PgError::error("42702", format!("ORDER BY \"{n}\" is ambiguous"))
            }
            ExecError::SetOpColumnCount { op, .. } => {
                // PG-exact: the message names the specific operator and has no count,
                // e.g. "each UNION query must have the same number of columns".
                let op_name = match op {
                    crabka_pgparser::ast::SetOp::Union => "UNION",
                    crabka_pgparser::ast::SetOp::Intersect => "INTERSECT",
                    crabka_pgparser::ast::SetOp::Except => "EXCEPT",
                };
                PgError::error(
                    "42601",
                    format!("each {op_name} query must have the same number of columns"),
                )
            }
            ExecError::ValuesColumnCount => {
                PgError::error("42601", "VALUES lists must all be the same length")
            }
            ExecError::DerivedColumnAliasCount { expected, got } => PgError::error(
                "42601",
                format!("table has {expected} columns available but {got} columns specified"),
            ),
            ExecError::InvalidColumnReference(m) => PgError::error("42P10", m),
            ExecError::MissingFromEntry(t) => PgError::error(
                "42P01",
                format!("missing FROM-clause entry for table \"{t}\""),
            ),
            ExecError::DuplicateAlias(t) => PgError::error(
                "42712",
                format!("table name \"{t}\" specified more than once"),
            ),
            ExecError::Unsupported(m) => PgError::error("0A000", m),
            ExecError::TypeMismatch(m) => PgError::error("42804", m),
            ExecError::NotNullViolation(column) => PgError::error(
                "23502",
                format!("null value in column \"{column}\" violates not-null constraint"),
            ),
            ExecError::ColumnContainsNullValues { column, table } => PgError::error(
                "23502",
                format!("column \"{column}\" of relation \"{table}\" contains null values"),
            ),
            ExecError::InvalidTableDefinition(m) => PgError::error("42P16", m),
            ExecError::UniqueViolation(index) => PgError::error(
                "23505",
                format!("duplicate key value violates unique constraint \"{index}\""),
            ),
            ExecError::Grouping(m) => PgError::error("42803", m),
            ExecError::UndefinedFunction(m) => PgError::error("42883", m),
            ExecError::WrongObjectType(m) => PgError::error("42809", m),
            ExecError::InFailedTransaction => PgError::error(
                "25P02",
                "current transaction is aborted, commands ignored until end of transaction block",
            ),
            ExecError::ActiveSqlTransaction(message) => PgError::error("25001", message),
            ExecError::SerializationFailure => PgError::error(
                "40001",
                "could not serialize access due to concurrent update",
            ),
            ExecError::Deadlock => PgError::error("40P01", "deadlock detected"),
            ExecError::LockWaitCapExpired => PgError::error(
                "40P01",
                "lock wait by a cross-range transaction exceeded its cap; \
                 presumed distributed deadlock",
            ),
            ExecError::NotLeader => {
                PgError::error("40001", "could not complete: not the leader, retry")
            }
            ExecError::Unavailable => PgError::error("08006", "connection failure: no quorum"),
            ExecError::InvalidParameterValue(v) => {
                PgError::error("22023", format!("invalid value for parameter: \"{v}\""))
            }
            ExecError::UnrecognizedParameter(n) => PgError::error(
                "42704",
                format!("unrecognized configuration parameter \"{n}\""),
            ),
            ExecError::StackDepthExceeded => PgError::error("54001", "stack depth limit exceeded"),
            ExecError::SequenceLimit(m) => PgError::error("2200H", m),
            ExecError::ObjectNotInPrerequisiteState(m) => PgError::error("55000", m),
        }
    }
}

impl From<ParseError> for ExecError {
    fn from(e: ParseError) -> Self {
        ExecError::Parse(e)
    }
}
impl From<CatalogError> for ExecError {
    fn from(e: CatalogError) -> Self {
        ExecError::Catalog(e)
    }
}
impl From<TypeError> for ExecError {
    fn from(e: TypeError) -> Self {
        ExecError::Type(e)
    }
}
impl From<KvError> for ExecError {
    fn from(e: KvError) -> Self {
        ExecError::Kv(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn syntax_maps_to_42601() {
        let pg = ExecError::Syntax("non-integer constant in ORDER BY".into()).into_pg();
        assert_eq!(pg.code, "42601");
        assert_eq!(pg.message, "non-integer constant in ORDER BY");
    }

    #[test]
    fn ambiguous_order_by_maps_to_pg_message() {
        let pg = ExecError::AmbiguousOrderBy("x".into()).into_pg();
        assert_eq!(pg.code, "42702");
        assert_eq!(pg.message, "ORDER BY \"x\" is ambiguous");
    }
}
