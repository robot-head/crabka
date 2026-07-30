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
    /// A `WITH RECURSIVE` item breaks one of PostgreSQL's recursion rules
    /// (42P19) — a missing or misplaced self-reference, or an unsupported
    /// construct in the recursive term.
    InvalidRecursion(String),
    /// A NULL value was assigned to a NOT NULL column (23502).
    NotNullViolation {
        column: String,
        table: String,
    },
    /// An existing row holds NULL in a column being constrained NOT NULL
    /// (23502) — `ALTER TABLE … ADD PRIMARY KEY` validation over stored rows.
    ColumnContainsNullValues {
        column: String,
        table: String,
    },
    /// The table definition itself is invalid (42P16) — e.g. adding a second
    /// primary key.
    InvalidTableDefinition(String),
    /// A written row failed a `CHECK` constraint (23514).
    CheckViolation {
        table: String,
        constraint: String,
    },
    /// An already-stored row fails a `CHECK` being added by `ALTER TABLE` (23514).
    CheckViolationOnExistingRows {
        table: String,
        constraint: String,
    },
    /// A column reference named a column the relation does not have (42703).
    /// `PostgreSQL` names the relation in DDL contexts, unlike the bare
    /// [`ExecError::UndefinedColumn`] used for query analysis.
    UndefinedTableColumn {
        column: String,
        table: String,
    },
    /// `ALTER TABLE … ADD COLUMN` / `RENAME COLUMN` collided with an existing
    /// column (42701).
    DuplicateColumn {
        column: String,
        table: String,
    },
    /// A relation being defined would have two columns of the same name (42701)
    /// — `CREATE TABLE … AS SELECT id, id FROM t`. Unlike
    /// [`ExecError::DuplicateColumn`] there is no relation to name yet.
    DuplicateOutputColumn(String),
    /// A named object already exists (42710).
    DuplicateObject(String),
    /// A named object does not exist (42704).
    UndefinedObject(String),
    /// A `DROP <kind>` named a relation that does not exist (42P01).
    /// `PostgreSQL` names the kind the statement asked for ("table", "view",
    /// "sequence") rather than the generic "relation".
    UndefinedRelationOfKind {
        kind: &'static str,
        name: String,
    },
    /// An expression's type cannot be determined (42P18) — e.g. `ARRAY[]` with
    /// no cast to supply the element type.
    IndeterminateType(String),
    /// A row would duplicate a visible row in a unique index (23505).
    UniqueViolation(String),
    /// Existing rows hold a duplicate key for a unique index being built
    /// (23505). `PostgreSQL` reports the index build, not a row insertion, so
    /// the message differs from [`ExecError::UniqueViolation`].
    UniqueIndexBuildViolation(String),
    /// An object's definition is self-inconsistent (42P17) — e.g. a generated
    /// column whose expression reads another generated column.
    InvalidObjectDefinition(String),
    /// Other catalog objects depend on the one being dropped or altered
    /// (2BP01), and no `CASCADE` was given.
    DependentObjectsStillExist(String),
    /// No unique index arbitrates the `ON CONFLICT` specification (42P10) — the
    /// inference column set matches no unique index on the target table.
    OnConflictNoArbiter,
    /// One `INSERT … ON CONFLICT DO UPDATE` statement tried to update the same
    /// row twice (21000) — either two conflicting rows in the same statement or
    /// a row already updated by this statement.
    OnConflictAffectsRowTwice,
    /// `ON CONFLICT ON CONSTRAINT <name>` / `ALTER TABLE … RENAME CONSTRAINT`
    /// named a constraint the target table does not have (42704).
    UndefinedConstraint {
        name: String,
        table: String,
    },
    /// The same 42704 in the spelling `PostgreSQL` uses for `ALTER TABLE …
    /// DROP CONSTRAINT` and `VALIDATE CONSTRAINT` — "of relation" there,
    /// "for table" for [`ExecError::UndefinedConstraint`].
    UndefinedRelationConstraint {
        name: String,
        table: String,
    },
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
    /// A derived-table or function column alias list names more columns than the
    /// item has (42P10 — `PostgreSQL`'s `buildRelationAliases` check).
    DerivedColumnAliasCount {
        table: String,
        expected: usize,
        got: usize,
    },
    /// SP38: an `ORDER BY <n>` positional reference is 0 or past the number of
    /// output columns (42P10 — invalid_column_reference).
    InvalidColumnReference(String),
    /// A statement was issued in an aborted transaction block (25P02): every
    /// command after an error (until COMMIT/ROLLBACK) is rejected.
    InFailedTransaction,
    /// A statement that would change rows or catalog state was issued in a
    /// `READ ONLY` transaction (25006). Carries the command tag PostgreSQL names.
    ReadOnlyTransaction(&'static str),
    /// S1/S2/S3: a command that requires an explicit transaction block was
    /// issued outside one (25P01).
    NoActiveSqlTransaction(String),
    /// S1: `ROLLBACK TO`/`RELEASE` named a savepoint that is not open (3B001).
    InvalidSavepoint(String),
    /// S2: `DECLARE` reused an open cursor's name (42P03).
    DuplicateCursor(String),
    /// S2: `FETCH`/`MOVE`/`CLOSE` named a cursor that is not open (34000).
    UndefinedCursor(String),
    /// S2: `PREPARE` reused an existing prepared statement's name (42P05).
    DuplicatePreparedStatement(String),
    /// S2: `EXECUTE`/`DEALLOCATE` named an unknown prepared statement (26000).
    UndefinedPreparedStatement(String),
    /// S3: a lock could not be taken without waiting (55P03).
    LockNotAvailable(String),
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
    /// F-1: a `SET` value the named parameter's own parser rejected (22023).
    InvalidGucValue {
        name: String,
        value: String,
    },
    /// F-1: a numeric `SET` value outside the named parameter's declared range
    /// (22023). Value and bounds are already rendered in base units, with the
    /// parameter's unit suffix.
    /// Boxed because it is by far the widest variant, and `ExecError`'s size is
    /// multiplied by the recursive evaluator's frame — four inline `String`s
    /// here cost every nested expression the same 96 bytes of stack.
    GucValueOutOfRange(Box<GucRangeViolation>),
    /// `ALTER TABLE … ATTACH PARTITION` that would make the partition metadata
    /// cyclic (42P07). PostgreSQL spells this "circular inheritance not
    /// allowed"; it must be refused rather than stored, because a cycle turns
    /// every later walk of the partition tree into an unbounded loop.
    CircularInheritance,
    /// F-1: a `SET` against a parameter whose `pg_settings.context` forbids
    /// session assignment (55P02).
    CannotChangeParameter(String),
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
    /// A scalar function's own error, carrying the SQLSTATE and the message
    /// PostgreSQL spells out at that call site — `setseed`'s range check
    /// (22023), `format`'s specifier diagnostics (22023/22004), `encode`'s
    /// unknown encoding, `split_part`'s zero field position. Both parts vary
    /// per call, so neither can be baked into a dedicated variant.
    FunctionError {
        sqlstate: &'static str,
        message: String,
    },
    /// A `PARTITION BY` key names a column the relation does not have (42703).
    /// `PostgreSQL`'s message names the partition key, unlike the bare
    /// [`ExecError::UndefinedColumn`].
    UndefinedPartitionKeyColumn(String),
    /// `PARTITION BY <word>` named a strategy that does not exist (22023).
    UnrecognizedPartitionStrategy(String),
    /// An inserted row's partition key matched no partition of the target
    /// partitioned table, and there is no `DEFAULT` partition (23514).
    NoPartitionForRow(String),
    /// A row written straight into a leaf partition falls outside that
    /// partition's own bound (23514).
    PartitionConstraintViolation(String),
    /// `ATTACH PARTITION` found stored rows outside the bound being attached
    /// (23514).
    PartitionConstraintViolationOnExistingRows(String),
    /// `DETACH PARTITION` named a relation that is not a partition of the
    /// target (42P01).
    NotAPartitionOf {
        partition: String,
        parent: String,
    },
    /// `ATTACH PARTITION` found the candidate lacks a column the parent has
    /// (42804).
    ChildMissingColumn(String),
    /// `PARTITION OF` named a relation that is not partitioned (42P17).
    NotPartitioned(String),
}

/// The payload of [`ExecError::GucValueOutOfRange`], boxed to keep the error
/// enum narrow. Every field is already rendered in the parameter's base units.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GucRangeViolation {
    pub name: String,
    pub value: String,
    pub min: String,
    pub max: String,
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
            ExecError::DerivedColumnAliasCount {
                table,
                expected,
                got,
            } => PgError::error(
                "42P10",
                format!(
                    "table \"{table}\" has {expected} columns available but {got} columns specified"
                ),
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
            ExecError::NotNullViolation { column, table } => PgError::error(
                "23502",
                format!(
                    "null value in column \"{column}\" of relation \"{table}\" \
                     violates not-null constraint"
                ),
            ),
            ExecError::ColumnContainsNullValues { column, table } => PgError::error(
                "23502",
                format!("column \"{column}\" of relation \"{table}\" contains null values"),
            ),
            ExecError::InvalidTableDefinition(m) => PgError::error("42P16", m),
            ExecError::CheckViolation { table, constraint } => PgError::error(
                "23514",
                format!(
                    "new row for relation \"{table}\" violates check constraint \"{constraint}\""
                ),
            ),
            ExecError::CheckViolationOnExistingRows { table, constraint } => PgError::error(
                "23514",
                format!(
                    "check constraint \"{constraint}\" of relation \"{table}\" is violated by some row"
                ),
            ),
            ExecError::UndefinedTableColumn { column, table } => PgError::error(
                "42703",
                format!("column \"{column}\" of relation \"{table}\" does not exist"),
            ),
            ExecError::DuplicateColumn { column, table } => PgError::error(
                "42701",
                format!("column \"{column}\" of relation \"{table}\" already exists"),
            ),
            ExecError::DuplicateOutputColumn(column) => PgError::error(
                "42701",
                format!("column \"{column}\" specified more than once"),
            ),
            ExecError::DuplicateObject(m) => PgError::error("42710", m),
            ExecError::UndefinedObject(m) => PgError::error("42704", m),
            ExecError::UndefinedRelationOfKind { kind, name } => {
                PgError::error("42P01", format!("{kind} \"{name}\" does not exist"))
            }
            ExecError::IndeterminateType(m) => PgError::error("42P18", m),
            ExecError::UniqueViolation(index) => PgError::error(
                "23505",
                format!("duplicate key value violates unique constraint \"{index}\""),
            ),
            ExecError::UniqueIndexBuildViolation(index) => PgError::error(
                "23505",
                format!("could not create unique index \"{index}\""),
            ),
            ExecError::InvalidObjectDefinition(m) => PgError::error("42P17", m),
            ExecError::DependentObjectsStillExist(m) => PgError::error("2BP01", m),
            ExecError::OnConflictNoArbiter => PgError::error(
                "42P10",
                "there is no unique or exclusion constraint matching the ON CONFLICT specification",
            ),
            ExecError::OnConflictAffectsRowTwice => PgError::error(
                "21000",
                "ON CONFLICT DO UPDATE command cannot affect row a second time",
            ),
            ExecError::UndefinedConstraint { name, table } => PgError::error(
                "42704",
                format!("constraint \"{name}\" for table \"{table}\" does not exist"),
            ),
            ExecError::UndefinedRelationConstraint { name, table } => PgError::error(
                "42704",
                format!("constraint \"{name}\" of relation \"{table}\" does not exist"),
            ),
            ExecError::Grouping(m) => PgError::error("42803", m),
            ExecError::InvalidRecursion(m) => PgError::error("42P19", m),
            ExecError::UndefinedFunction(m) => PgError::error("42883", m),
            ExecError::WrongObjectType(m) => PgError::error("42809", m),
            ExecError::InFailedTransaction => PgError::error(
                "25P02",
                "current transaction is aborted, commands ignored until end of transaction block",
            ),
            ExecError::ReadOnlyTransaction(tag) => PgError::error(
                "25006",
                format!("cannot execute {tag} in a read-only transaction"),
            ),
            ExecError::ActiveSqlTransaction(message) => PgError::error("25001", message),
            ExecError::NoActiveSqlTransaction(message) => PgError::error("25P01", message),
            ExecError::InvalidSavepoint(name) => {
                PgError::error("3B001", format!("savepoint \"{name}\" does not exist"))
            }
            ExecError::DuplicateCursor(name) => {
                PgError::error("42P03", format!("cursor \"{name}\" already exists"))
            }
            ExecError::UndefinedCursor(name) => {
                PgError::error("34000", format!("cursor \"{name}\" does not exist"))
            }
            ExecError::DuplicatePreparedStatement(name) => PgError::error(
                "42P05",
                format!("prepared statement \"{name}\" already exists"),
            ),
            ExecError::UndefinedPreparedStatement(name) => PgError::error(
                "26000",
                format!("prepared statement \"{name}\" does not exist"),
            ),
            ExecError::LockNotAvailable(message) => PgError::error("55P03", message),
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
            ExecError::InvalidGucValue { name, value } => PgError::error(
                "22023",
                format!("invalid value for parameter \"{name}\": \"{value}\""),
            ),
            ExecError::GucValueOutOfRange(range) => {
                let GucRangeViolation {
                    name,
                    value,
                    min,
                    max,
                } = range.as_ref();
                PgError::error(
                    "22023",
                    format!(
                        "{value} is outside the valid range for parameter \"{name}\" ({min} .. {max})"
                    ),
                )
            }
            ExecError::CircularInheritance => {
                PgError::error("42P07", "circular inheritance not allowed")
            }
            ExecError::CannotChangeParameter(m) => PgError::error("55P02", m),
            ExecError::StackDepthExceeded => PgError::error("54001", "stack depth limit exceeded"),
            ExecError::SequenceLimit(m) => PgError::error("2200H", m),
            ExecError::ObjectNotInPrerequisiteState(m) => PgError::error("55000", m),
            ExecError::FunctionError { sqlstate, message } => PgError::error(sqlstate, message),
            ExecError::UndefinedPartitionKeyColumn(column) => PgError::error(
                "42703",
                format!("column \"{column}\" named in partition key does not exist"),
            ),
            ExecError::UnrecognizedPartitionStrategy(strategy) => PgError::error(
                "22023",
                format!("unrecognized partitioning strategy \"{strategy}\""),
            ),
            ExecError::NoPartitionForRow(relation) => PgError::error(
                "23514",
                format!("no partition of relation \"{relation}\" found for row"),
            ),
            ExecError::PartitionConstraintViolation(relation) => PgError::error(
                "23514",
                format!("new row for relation \"{relation}\" violates partition constraint"),
            ),
            ExecError::PartitionConstraintViolationOnExistingRows(relation) => PgError::error(
                "23514",
                format!("partition constraint of relation \"{relation}\" is violated by some row"),
            ),
            ExecError::NotAPartitionOf { partition, parent } => PgError::error(
                "42P01",
                format!("relation \"{partition}\" is not a partition of relation \"{parent}\""),
            ),
            ExecError::ChildMissingColumn(column) => PgError::error(
                "42804",
                format!("child table is missing column \"{column}\""),
            ),
            ExecError::NotPartitioned(relation) => {
                PgError::error("42P17", format!("\"{relation}\" is not partitioned"))
            }
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
    use assert2::assert;

    use super::*;

    /// Each row is (error, expected SQLSTATE, expected PG-exact message). The
    /// ON CONFLICT rows are diffed against a real PostgreSQL oracle by the
    /// conformance harness, so their texts are byte-for-byte PG's.
    #[test]
    fn errors_map_to_sqlstate_and_pg_message() {
        let cases: Vec<(ExecError, &str, &str)> = vec![
            (
                ExecError::Syntax("non-integer constant in ORDER BY".into()),
                "42601",
                "non-integer constant in ORDER BY",
            ),
            (
                ExecError::AmbiguousOrderBy("x".into()),
                "42702",
                "ORDER BY \"x\" is ambiguous",
            ),
            (
                ExecError::IndeterminateType("cannot determine type of empty array".into()),
                "42P18",
                "cannot determine type of empty array",
            ),
            (
                ExecError::OnConflictNoArbiter,
                "42P10",
                "there is no unique or exclusion constraint matching the ON CONFLICT specification",
            ),
            (
                ExecError::OnConflictAffectsRowTwice,
                "21000",
                "ON CONFLICT DO UPDATE command cannot affect row a second time",
            ),
            (
                ExecError::UndefinedConstraint {
                    name: "t_k_key".into(),
                    table: "t".into(),
                },
                "42704",
                "constraint \"t_k_key\" for table \"t\" does not exist",
            ),
        ];

        for (error, code, message) in cases {
            let pg = error.clone().into_pg();
            assert!(
                (pg.code.as_str(), pg.message.as_str()) == (code, message),
                "unexpected wire error for {error:?}"
            );
        }
    }
}
