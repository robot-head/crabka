//! Map lower-crate error enums onto wire `PgError`s with the right SQLSTATE.

use crabka_pgcatalog::CatalogError;
use crabka_pgkv::KvError;
use crabka_pgparser::ParseError;
use crabka_pgtypes::TypeError;
use crabka_pgwire::error::PgError;

/// Executor-level error. It converts to a non-fatal `PgError`.
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
    /// A qualified reference named a FROM-clause entry that *is* at this query
    /// level but is not visible from the part of the query making the reference
    /// (42P01) — a sibling FROM item read without `LATERAL`, the target of an
    /// `UPDATE`/`DELETE`, or a lateral item on the nullable side of a join.
    InvalidFromEntry {
        table: String,
        note: FromEntryNote,
    },
    /// An unqualified reference named a column that a FROM-clause entry of this
    /// query level does have, but which is not visible from the part of the
    /// query naming it (42703). The primary message is
    /// [`ExecError::UndefinedColumn`]'s; only the explanation differs.
    InaccessibleColumn {
        column: String,
        table: String,
        /// Whether marking the sub-select `LATERAL` would make the column
        /// visible, which is what decides whether `PostgreSQL` offers a remedy.
        lateral_would_help: bool,
    },
    /// The same table name/alias appears twice in one FROM clause (42712).
    DuplicateAlias(String),
    /// In-grammar but unimplemented (0A000), for example $1 parameters.
    Unsupported(String),
    /// The same condition as [`ExecError::Unsupported`] where `PostgreSQL`
    /// writes a DETAIL line beside the message. `date_trunc('week', interval)`
    /// is one: the unit is refused, and the DETAIL says why a month has no
    /// whole number of weeks.
    UnsupportedWithDetail {
        message: String,
        detail: String,
    },
    /// Wrong type in a context that demands a specific one (42804), for example
    /// a non-boolean WHERE.
    TypeMismatch(String),
    /// A `WITH RECURSIVE` item breaks one of PostgreSQL's recursion rules
    /// (42P19): a missing or misplaced self-reference, or an unsupported
    /// construct in the recursive term.
    InvalidRecursion(String),
    /// A statement assigned a NULL value to a NOT NULL column (23502).
    NotNullViolation {
        column: String,
        table: String,
    },
    /// An existing row holds NULL in a column being constrained NOT NULL
    /// (23502), found by `ALTER TABLE … ADD PRIMARY KEY` validation over stored
    /// rows.
    ColumnContainsNullValues {
        column: String,
        table: String,
    },
    /// The table definition itself is invalid (42P16), for example a second
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
    /// An index-backed constraint names a column that does not exist (42703).
    /// PostgreSQL's index-analysis message calls this a column "named in key".
    UndefinedIndexColumn(String),
    /// `PRIMARY KEY`/`UNIQUE (c WITHOUT OVERLAPS)` with nothing but the
    /// temporal column (42601). The clause holds rows apart *within* a scalar
    /// key, so a key made only of it would forbid every overlap in the table.
    WithoutOverlapsNeedsTwoColumns,
    /// The column a `WITHOUT OVERLAPS` clause names is neither a range nor a
    /// multirange, so there is no `&&` to compare it with (42804).
    WithoutOverlapsNotRange(String),
    /// A row reached a `WITHOUT OVERLAPS` key with an empty range in the
    /// temporal column (23514). An empty range overlaps nothing, so it would
    /// silently escape the constraint; `PostgreSQL` refuses it outright.
    EmptyWithoutOverlapsValue {
        column: String,
        relation: String,
    },
    /// Only one side of a `FOREIGN KEY (…, PERIOD c) REFERENCES t (…, PERIOD
    /// c)` wrote `PERIOD` (42830). A temporal foreign key is temporal on both
    /// sides or on neither.
    ForeignKeyPeriodMismatch {
        /// True when `PERIOD` was written on the referencing side only.
        on_referencing: bool,
    },
    /// A plain foreign key named the columns of a `WITHOUT OVERLAPS` key
    /// (42830). Those columns are held apart by `&&`, not `=`, so an equality
    /// probe against them would not prove the parent row unique.
    ForeignKeyNeedsPeriod,
    /// `ALTER TABLE … ADD COLUMN` / `RENAME COLUMN` collided with an existing
    /// column (42701).
    DuplicateColumn {
        column: String,
        table: String,
    },
    /// A column list names the same column twice (42701) — a relation being
    /// defined with two columns of one name (`CREATE TABLE … AS SELECT id, id
    /// FROM t`), or an `INSERT` naming a target twice. Unlike
    /// [`ExecError::DuplicateColumn`] there is no relation to name: the first
    /// has none yet, and `PostgreSQL` leaves it out of the second.
    DuplicateOutputColumn(String),
    /// A relation with storage declared a column named after one of
    /// `PostgreSQL`'s six system columns (42701).
    ///
    /// A 42701 and not a 42939 because `PostgreSQL` reads it as the name being
    /// taken: `CheckAttributeNamesTypes` raises `ERRCODE_DUPLICATE_COLUMN`
    /// beside the two duplicate cases above. Views and composite types are
    /// exempt there, and are exempt here — `CREATE VIEW v AS SELECT 1 AS ctid`
    /// is valid `PostgreSQL` and `tid.sql` writes it.
    SystemColumnName(String),
    /// A `SET` list assigned to one of `PostgreSQL`'s six system columns
    /// (0A000).
    ///
    /// A 0A000 and not the 42703 an unknown column gets, because the name is
    /// not unknown: `parse_target.c` resolves it, finds a negative `attnum` and
    /// raises `ERRCODE_FEATURE_NOT_SUPPORTED`. The distinction became worth
    /// making when a `ctid` in the same statement's `WHERE` started resolving —
    /// "does not exist" is a poor answer about a column the statement just
    /// read. Only a relation that declares no column of the name reaches this;
    /// a view may declare one, and its own updatability rule answers instead.
    AssignSystemColumn(String),
    /// An `ANALYZE`/`VACUUM ANALYZE` column list names one column twice
    /// (42701). Distinct from [`ExecError::DuplicateColumn`], which reports a
    /// collision with a column that is already there; here both mentions are
    /// in the statement.
    RepeatedMaintenanceColumn {
        column: String,
        table: String,
    },
    /// A named object already exists (42710).
    DuplicateObject(String),
    /// A named object does not exist (42704).
    UndefinedObject(String),
    /// `CREATE` with an unqualified name while no schema on the `search_path`
    /// exists (3F000). `PostgreSQL` skips a nonexistent path entry instead of
    /// refusing it, so this is what `SET search_path = notme; CREATE TABLE t`
    /// reports: a path that is not empty but names nowhere to create in.
    NoSchemaSelected,
    /// A `DROP <kind>` named a relation that does not exist (42P01).
    /// `PostgreSQL` names the kind the statement asked for ("table", "view",
    /// "sequence") instead of the generic "relation".
    UndefinedRelationOfKind {
        kind: &'static str,
        name: String,
    },
    /// An expression's type cannot be determined (42P18), for example `ARRAY[]`
    /// with no cast to supply the element type.
    IndeterminateType(String),
    /// A row would duplicate a visible row in a unique index (23505).
    UniqueViolation(String),
    /// Existing rows hold a duplicate key for a unique index being built
    /// (23505). `PostgreSQL` reports the index build, not a row insertion, so
    /// the message differs from [`ExecError::UniqueViolation`].
    UniqueIndexBuildViolation(String),
    /// An object's definition is self-inconsistent (42P17), for example a
    /// generated column whose expression reads another generated column.
    InvalidObjectDefinition(String),
    /// Other catalog objects depend on the one being dropped or altered
    /// (2BP01), and the statement gave no `CASCADE`.
    DependentObjectsStillExist(String),
    /// No unique index arbitrates the `ON CONFLICT` specification (42P10): the
    /// inference column set matches no unique index on the target table.
    OnConflictNoArbiter,
    /// One `INSERT … ON CONFLICT DO UPDATE` statement tried to update the same
    /// row twice (21000): either two conflicting rows in the same statement, or
    /// a row this statement already updated.
    OnConflictAffectsRowTwice,
    /// `ON CONFLICT ON CONSTRAINT <name>` / `ALTER TABLE … RENAME CONSTRAINT`
    /// named a constraint the target table does not have (42704).
    UndefinedConstraint {
        name: String,
        table: String,
    },
    /// The same 42704 in the spelling `PostgreSQL` uses for `ALTER TABLE …
    /// DROP CONSTRAINT` and `VALIDATE CONSTRAINT`. That spelling says
    /// "of relation", where [`ExecError::UndefinedConstraint`] says "for table".
    UndefinedRelationConstraint {
        name: String,
        table: String,
    },
    /// A statement broke a grouping/aggregation rule (42803), for example a
    /// column that is neither grouped nor inside an aggregate, or a nested
    /// aggregate.
    Grouping(String),
    /// A call to a function that does not exist (42883), for example an unknown
    /// name, or an aggregate applied to an argument type/arity it does not
    /// accept.
    UndefinedFunction(String),
    /// A statement used an object in a way its kind does not allow (42809), for
    /// example `DISTINCT`/`ALL` applied to a scalar (non-aggregate) function.
    WrongObjectType(String),
    /// A scalar subquery returned more than one row (21000).
    CardinalityViolation,
    /// A subquery used as an expression / IN / quantified source returned more than
    /// one column (42601).
    SubqueryColumns,
    /// PostgreSQL syntax/parse-analysis error that executor analysis raises
    /// (42601). It covers SQL92 ORDER BY integer constants that cannot fit in
    /// a positional reference.
    Syntax(String),
    /// A bare ORDER BY output label matched more than one projected column
    /// (42702). PostgreSQL's message differs from generic column ambiguity.
    AmbiguousOrderBy(String),
    /// SP38: the branches of a UNION/INTERSECT/EXCEPT have different column
    /// counts (42601). `op` names the specific operator for the PG-exact
    /// message. `left` and `right` stay for internal use, and the message does
    /// not print them.
    SetOpColumnCount {
        op: crabka_pgparser::ast::SetOp,
        left: usize,
        right: usize,
    },
    /// SP39: VALUES rows have different column counts (42601).
    ValuesColumnCount,
    /// A derived-table or function column alias list names more columns than the
    /// item has (42P10, `PostgreSQL`'s `buildRelationAliases` check).
    DerivedColumnAliasCount {
        table: String,
        expected: usize,
        got: usize,
    },
    /// SP38: an `ORDER BY <n>` positional reference is 0 or past the number of
    /// output columns (42P10, invalid_column_reference).
    InvalidColumnReference(String),
    /// A client issued a statement in an aborted transaction block (25P02). The
    /// engine rejects every command after an error, until COMMIT/ROLLBACK.
    InFailedTransaction,
    /// A client issued a statement that would change rows or catalog state in a
    /// `READ ONLY` transaction (25006). Carries the command tag PostgreSQL names.
    ReadOnlyTransaction(&'static str),
    /// S1/S2/S3: a client issued a command that requires an explicit transaction
    /// block outside one (25P01).
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
    /// S3: the engine could not take a lock without a wait (55P03).
    LockNotAvailable(String),
    /// A relation cannot be rewritten while the transaction still owes checks
    /// that identify its rows by position (55006).
    ObjectInUse(String),
    /// A command forbidden inside an explicit transaction block (25001).
    ActiveSqlTransaction(String),
    /// A write conflicted with a concurrently-committed change under REPEATABLE
    /// READ (40001). The client should retry the transaction.
    SerializationFailure,
    /// The engine detected a deadlock and chose this transaction as the victim
    /// (40P01).
    Deadlock,
    /// A lock wait by a cross-range transaction outlived its bounded-wait cap
    /// (40P01). A cycle that spans ranges is invisible to any single engine's
    /// wait-for graph, so the engine treats the expired cap as a presumed
    /// distributed deadlock. A retry is safe exactly as for a locally-detected
    /// deadlock.
    LockWaitCapExpired,
    /// The write hit a node that is not the Raft leader. The client should retry.
    NotLeader,
    /// The write could not reach a majority (partition/timeout). The engine
    /// applied no partial state, and the client should retry.
    Unavailable,
    /// SP37: a `SET`/`RESET` supplied a value the parameter cannot accept
    /// (22023), for example an unknown time-zone name, or a non-default
    /// `datestyle`.
    InvalidParameterValue(String),
    /// The same 22023 with the message written in full rather than wrapped in
    /// the `SET`/`RESET` sentence [`ExecError::InvalidParameterValue`] adds.
    /// `PostgreSQL` raises this SQLSTATE well outside the configuration
    /// family — `EXTRACT`'s unrecognised unit names the unit and the source
    /// type, and nothing else.
    InvalidParameterValueMessage(String),
    /// SP37: a `SET`/`SHOW`/`RESET` named a configuration parameter that does not
    /// exist (42704).
    UnrecognizedParameter(String),
    /// SP37: a zone specification no zone entry point recognises — neither an
    /// abbreviation, nor a name the bundled database knows, nor a well-formed
    /// `POSIX` `TZ` specification (22023).
    UnknownTimeZone(String),
    /// SP37: `make_timestamptz`'s zone argument began with a digit, which
    /// `PostgreSQL` refuses outright so that the `POSIX` grammar cannot claim a
    /// spelling the numeric-offset grammar would have rejected (22023).
    NumericTimeZoneSyntax(String),
    /// SP37: `make_timestamptz`'s zone argument was a numeric offset naming a
    /// displacement beyond ±15:59:59 (22023).
    NumericTimeZoneOutOfRange(String),
    /// F-1: a `SET` value the named parameter's own parser rejected (22023).
    InvalidGucValue {
        name: String,
        value: String,
    },
    /// F-1: a numeric `SET` value outside the named parameter's declared range
    /// (22023). Value and bounds are already rendered in base units, with the
    /// parameter's unit suffix.
    /// Boxed because it is by far the widest variant, and the recursive
    /// evaluator's frame multiplies `ExecError`'s size. Four inline `String`s
    /// here cost every nested expression the same 96 bytes of stack.
    GucValueOutOfRange(Box<GucRangeViolation>),
    /// `ALTER TABLE … ATTACH PARTITION` that would make the partition metadata
    /// cyclic (42P07). PostgreSQL spells this "circular inheritance not
    /// allowed". The engine must refuse it and must not store it, because a
    /// cycle turns every later walk of the partition tree into an unbounded
    /// loop.
    CircularInheritance,
    /// F-1: a `SET` against a parameter whose `pg_settings.context` forbids
    /// session assignment (55P02).
    CannotChangeParameter(String),
    /// An expression nested more deeply than the evaluator's `MAX_EVAL_DEPTH`
    /// (54001 / statement_too_complex). This is defense in depth. The parser
    /// already caps the AST depth at parse time, so a tree this deep should
    /// never reach `eval`. If one did, this guard makes sure evaluation returns
    /// a clean error and does not overflow the stack and abort the server
    /// process.
    StackDepthExceeded,
    /// A sequence advanced outside its configured bounds (2200H).
    SequenceLimit(String),
    /// Object state does not satisfy a command precondition (55000).
    ObjectNotInPrerequisiteState(String),
    /// A write named a view whose body is not simple enough to rewrite onto the
    /// relation underneath it (55000).
    ///
    /// The `DETAIL` names the clause that disqualified the view and the `HINT`
    /// names what would make the write work; both are `PostgreSQL`'s wording,
    /// and the `DETAIL` is the only thing that tells a user *which* clause is
    /// the problem, so it is carried rather than folded into the message.
    ViewNotUpdatable {
        /// `cannot insert into view "v"`, and its update/delete spellings.
        message: String,
        detail: &'static str,
        hint: &'static str,
    },
    /// A read reached a materialized view whose contents have never been
    /// computed — one created `WITH NO DATA`, or refreshed `WITH NO DATA`
    /// (55000).
    ///
    /// It is an error rather than an empty result because the two are not the
    /// same answer: a matview with no rows and a matview that was never
    /// populated look identical to a scan, and `PostgreSQL` refuses to let a
    /// query silently read the second as if it were the first. The `HINT` names
    /// the command that fixes it, which is the whole of the recovery.
    MaterializedViewNotPopulated(String),
    /// A write assigned to a view column that is not a column of the relation
    /// underneath — a computed, system, or whole-row column (0A000).
    ///
    /// Distinct from [`Self::ViewNotUpdatable`] because the view *is* updatable:
    /// SQL:1999 feature T111 admits a mix, and only the assignment is refused.
    ViewColumnNotUpdatable {
        /// `cannot insert into column "c" of view "v"`, and its update spelling.
        message: String,
        detail: &'static str,
    },
    /// `WITH CHECK OPTION` was written on a view that no write could ever be
    /// rewritten through, so the option could never fire (0A000).
    ///
    /// Refused where the view is defined rather than where a write reaches it,
    /// which is what stops a user from believing a check is in force. The
    /// payload is the clause that disqualified the body, which `PostgreSQL`
    /// reports as the `HINT`.
    CheckOptionUnsupported(&'static str),
    /// A row written through a view failed that view's `WITH CHECK OPTION`
    /// (44000).
    ViewCheckOptionViolation {
        /// The view whose option rejected the row, which for a chain of views
        /// is the innermost one that rejected it rather than the one written.
        view: String,
        /// The rendered row, already parenthesized, and `None` when the caller
        /// may not be shown it — see `crate::rls::describe_row`.
        row: Option<String>,
    },
    /// `row_security = off` and the named relation has a row-security policy
    /// that would have applied (42501). `PostgreSQL` fails the statement rather
    /// than quietly returning a filtered result the caller did not ask for.
    RowSecurityRefused(String),
    /// A row a statement wrote does not satisfy the relation's row-security
    /// policies (42501).
    RowSecurityCheckViolation {
        relation: String,
        /// The policy that rejected the row, when exactly one produced the
        /// qual. `PostgreSQL` leaves the name out of a violation folded from
        /// several policies.
        policy: Option<String>,
        /// The qual came from the policy's `USING` clause, because the policy
        /// declares no `WITH CHECK` of its own.
        using_expression: bool,
        /// The row was found by the statement rather than composed by it — the
        /// conflicting row an `ON CONFLICT DO UPDATE` is about to change.
        /// `PostgreSQL` calls that one the *target* row.
        target_row: bool,
    },
    /// A row-security policy qual reads the relation its own policy protects
    /// (42P17). The qual is user-supplied SQL, so following the recursion would
    /// be a remotely triggerable stack overflow.
    PolicyRecursion(String),
    /// The session holds no grant for what it asked to do with a relation
    /// (42501). `PostgreSQL` names the *kind* of relation, not the command, and
    /// never says which privilege was missing — telling an unprivileged caller
    /// which grant would have worked is itself a disclosure.
    PermissionDenied {
        /// `PostgreSQL`'s noun for the relation: `table`, `view`.
        kind: &'static str,
        /// The relation's bare name, unqualified, the way `PostgreSQL` spells
        /// it here even when the statement named a schema.
        relation: String,
    },
    /// An event trigger was created by, or handed to, a role that is not a
    /// superuser (42501).
    ///
    /// An event trigger runs its function for every DDL command anyone issues
    /// in the database, so the role behind it can act on statements it did not
    /// write; `PostgreSQL` closes that off by admitting only superusers, and
    /// spells the reason out in a `HINT` because the rule is not one a caller
    /// would infer from "permission denied". The two sites word both lines
    /// differently, so both travel with the error.
    EventTriggerPrivilege {
        /// `permission denied to create event trigger "t"`, or the
        /// change-owner spelling.
        message: String,
        /// PostgreSQL's `HINT`, naming the superuser rule.
        hint: &'static str,
    },
    /// A scalar function's own error, carrying the SQLSTATE and the message
    /// PostgreSQL spells out at that call site — `setseed`'s range check
    /// (22023), `format`'s specifier diagnostics (22023/22004), `encode`'s
    /// unknown encoding, and `split_part`'s zero field position. Both parts vary
    /// per call, so neither can go into a dedicated variant.
    FunctionError {
        sqlstate: &'static str,
        message: String,
    },
    /// A SQL/JSON diagnostic that carries `PostgreSQL`'s `DETAIL`/`HINT` lines
    /// alongside its SQLSTATE — `JSON_TABLE`'s coercion failures name the
    /// underlying type error in `DETAIL`, and its no-wrapper error hints at
    /// `WITH WRAPPER`. Boxed so the payload's three strings do not widen every
    /// other variant.
    SqlJson(Box<SqlJsonError>),
    /// A write supplied a value of its own for a `GENERATED ALWAYS` column
    /// (428C9). `PostgreSQL` words `INSERT` and `UPDATE` differently but gives
    /// both the same `DETAIL`, so only the message varies here.
    GeneratedColumnWrite {
        message: String,
        column: String,
    },
    /// `ALTER TABLE … ALTER COLUMN … SET/DROP EXPRESSION` named a column that
    /// carries no generation expression (42611).
    NotAGeneratedColumn {
        column: String,
        table: String,
    },
    /// One of the restrictions `PostgreSQL` 18 places on a `VIRTUAL` generated
    /// column, whose value is recomputed on every read and therefore cannot be
    /// rewritten in place (0A000). Both the message and the `DETAIL` are fixed
    /// by the subcommand, so only the subcommand and the column travel here.
    UnsupportedOnVirtualGenerated {
        subcommand: VirtualGeneratedSubcommand,
        column: String,
        table: String,
    },
    /// A `PARTITION BY` key names a column the relation does not have (42703).
    /// `PostgreSQL`'s message names the partition key, unlike the bare
    /// [`ExecError::UndefinedColumn`].
    UndefinedPartitionKeyColumn(String),
    /// `PARTITION BY <word>` named a strategy that does not exist (22023).
    UnrecognizedPartitionStrategy(String),
    /// An inserted row's partition key matched no partition of the target
    /// partitioned table, and there is no `DEFAULT` partition (23514).
    ///
    /// `key` is the rendered partition key of the failing row, and `None` when
    /// the caller may not be shown it. See `exec::may_describe_row`.
    NoPartitionForRow {
        relation: String,
        key: Option<String>,
    },
    /// A row written straight into a leaf partition falls outside the bound of
    /// that partition or of one of its ancestors (23514).
    ///
    /// `row` is the rendered failing row, and `None` when the caller may not be
    /// shown it. See `exec::may_describe_row`.
    PartitionConstraintViolation {
        relation: String,
        row: Option<String>,
    },
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
    /// A descendant already declares the column an `ALTER TABLE … ADD COLUMN`
    /// is propagating, under an incompatible type (42804). `PostgreSQL` merges
    /// the two definitions when the types agree and reports this when they do
    /// not.
    ChildColumnTypeMismatch {
        child: String,
        column: String,
    },
    /// A table being attached as a partition declares a column with a different
    /// collation from the parent's (42P21). Every collation this engine has
    /// orders text by byte value, so the two would in fact sort alike — but
    /// `PostgreSQL` compares the declared collations, not their behaviour, and a
    /// partitioned table whose children disagree about one is malformed.
    ChildColumnCollationMismatch {
        child: String,
        column: String,
    },
    /// `ONLY` suppressed a recursion `PostgreSQL` requires, because the
    /// relation has descendants that the subcommand would put out of step
    /// (42P16). The wording is per-subcommand, and two of them carry a hint.
    OnlyWouldSkipDescendants {
        message: String,
        hint: Option<String>,
    },
    /// `TRUNCATE ONLY` named a partitioned parent (42809). It owns no storage
    /// to empty, so `PostgreSQL` treats the statement as a mistake rather than
    /// as the no-op that `SELECT`/`UPDATE`/`DELETE … ONLY` over one become, and
    /// hints at the two spellings that do something.
    TruncateOnlyPartitioned,
    /// `PARTITION OF` named a relation that is not partitioned (42P17).
    NotPartitioned(String),
    /// A row write, or a back-validation scan, broke referential integrity.
    /// The SQLSTATE is 23503 on three of the four sides, and 23001
    /// (`restrict_violation`) when `RESTRICT` is what refused a parent-side
    /// delete or update. [`ForeignKeyViolationSide`] is the discriminator:
    /// `RESTRICT` and `NO ACTION` read as synonyms but differ in both SQLSTATE
    /// and wording, so the two must not be collapsed.
    ///
    /// `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY` over stored rows reports
    /// [`ForeignKeyViolationSide::KeyNotPresent`] as well. `PostgreSQL` reuses
    /// the row-write message verbatim for back-validation, so there is no
    /// separate on-existing-rows variant here (unlike
    /// [`ExecError::CheckViolationOnExistingRows`]).
    ///
    /// Boxed for the same reason as [`ExecError::GucValueOutOfRange`]: the
    /// payload's four inline `String`s would otherwise widen every frame of the
    /// recursive evaluator.
    ForeignKeyViolation(Box<ForeignKeyViolation>),
    /// No unique constraint or unique index covers the referenced columns
    /// (42830). Carries the referenced table.
    NoUniqueConstraintForReferencedTable(String),
    /// The referencing and referenced column lists have different lengths
    /// (42830).
    ForeignKeyColumnCountMismatch,
    /// The referenced-column list names one column twice (42830).
    DuplicateForeignKeyReferencedColumn,
    /// `REFERENCES` named a relation that is not a table (42809), for example a
    /// view. `PostgreSQL`'s message calls it the "referenced relation",
    /// unlike the general-purpose [`ExecError::WrongObjectType`].
    ReferencedRelationNotATable(String),
    /// A `FOREIGN KEY (…)` list names a column the referencing table does not
    /// have (42703). `PostgreSQL`'s message names the foreign key, unlike the
    /// bare [`ExecError::UndefinedColumn`].
    UndefinedForeignKeyColumn(String),
    /// A referencing column's type is not comparable with its referenced
    /// column's (42804). The primary message names only the constraint, so the
    /// column and type names land in `DETAIL`. Boxed: five inline `String`s.
    ForeignKeyTypeMismatch(Box<ForeignKeyTypeMismatch>),
    /// `ON DELETE SET NULL (…)` / `ON DELETE SET DEFAULT (…)` named a column
    /// outside the foreign key (42P10). `PostgreSQL` spells the action
    /// "ON DELETE SET" for both forms. Carries the offending column.
    ForeignKeySetColumnNotInKey(String),
    /// A constraint name collides with one the relation already carries
    /// (42710). The counterpart of [`ExecError::UndefinedConstraint`], though
    /// `PostgreSQL` says "for relation" here where the undefined-constraint
    /// message says "for table".
    DuplicateConstraint {
        name: String,
        table: String,
    },
    /// `TRUNCATE` named a table that a foreign key references, without also
    /// naming the referencing table and without `CASCADE` (0A000).
    /// `PostgreSQL` raises `ERRCODE_FEATURE_NOT_SUPPORTED` here, but unlike
    /// [`ExecError::Unsupported`] it also emits `DETAIL` and `HINT`.
    TruncateReferencedByForeignKey {
        referencing_table: String,
        referenced_table: String,
    },
    /// The engine refused a drop because foreign keys depend on the object
    /// (2BP01) and the statement gave no `CASCADE`. Unlike
    /// [`ExecError::DependentObjectsStillExist`], which carries a bare message,
    /// this names every dependent constraint in `DETAIL`, one per line, and
    /// hints at `CASCADE`. Boxed: the payload holds a [`DroppedObject`] and a
    /// `Vec` of dependents.
    DependentForeignKeys(Box<ForeignKeyDependents>),
}

/// The `ALTER TABLE` subcommand a `VIRTUAL` generated column refuses, and the
/// message `PostgreSQL` 18 words the refusal with.
///
/// Kept as a code rather than a string so [`ExecError`] stays narrow: the enum
/// is returned by every function in the executor, and the recursion depth an
/// engine can reach is measured in its size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VirtualGeneratedSubcommand {
    /// `SET EXPRESSION` on a relation that carries `CHECK` constraints: the
    /// constraints would have to be revalidated against values that are stored
    /// nowhere.
    SetExpressionWithChecks,
    /// `DROP EXPRESSION`: the column would have to keep the values it computed,
    /// and a virtual column has never written any down.
    DropExpression,
}

impl VirtualGeneratedSubcommand {
    /// The `ERROR` line `PostgreSQL` 18 prints for this refusal.
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            Self::SetExpressionWithChecks => {
                "ALTER TABLE / SET EXPRESSION is not supported for virtual generated columns in \
                 tables with check constraints"
            }
            Self::DropExpression => {
                "ALTER TABLE / DROP EXPRESSION is not supported for virtual generated columns"
            }
        }
    }
}

/// Why an [`ExecError::InvalidFromEntry`] entry is out of reach, which is the
/// only thing separating `PostgreSQL`'s four wordings of it.
///
/// The message line is identical in all four; what changes is whether the
/// explanation arrives as `DETAIL` or as `HINT`, and whether a remedy is offered
/// at all. `PostgreSQL` splits them across two call sites — `errorMissingRTE`
/// for a reference that never entered the namespace and `check_lateral_ref_ok`
/// for one that entered it but is disallowed — so the split is reproduced here
/// rather than derived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FromEntryNote {
    /// A sibling FROM item of the same query level: `LATERAL` would bring it
    /// into view, so the situation is the `DETAIL` and the remedy the `HINT`.
    MarkSubqueryLateral,
    /// The relation an `UPDATE`/`DELETE` is targeting, which no `LATERAL` can
    /// reach. The same sentence, with no remedy to add.
    TargetRelation,
    /// The `UPDATE`/`DELETE` target reached from an item already written
    /// `LATERAL`. `PostgreSQL` states the very same sentence as a `HINT` here,
    /// because the check that rejects it is a different one.
    LateralTargetRelation,
    /// A `LATERAL` item on the nullable side of a `RIGHT`/`FULL` join, which
    /// SQL:2008 forbids outright.
    CombiningJoinType,
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

/// The payload of [`ExecError::ForeignKeyViolation`], boxed to keep the error
/// enum narrow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyViolation {
    /// The table the failing statement wrote, as the primary message names it:
    /// the referencing table on the child side, the referenced table on the
    /// parent side.
    pub table: String,
    /// The constraint's name, as `pg_constraint.conname` holds it.
    pub constraint: String,
    /// Which side the write violated. This selects the SQLSTATE and the wording
    /// of both the message and the `DETAIL`.
    pub side: ForeignKeyViolationSide,
}

/// Which side of a foreign key a runtime violation came from and, on the parent
/// side, which referential action refused the delete or update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForeignKeyViolationSide {
    /// The written row's key is absent from the referenced table (23503):
    /// `Key (a, b)=(1, 1) is not present in table "p".` A back-validation scan
    /// reports the same shape.
    KeyNotPresent {
        /// The `Key (a, b)=(1, 1)` fragment, already rendered. Build it with
        /// [`ForeignKeyViolationSide::render_key`], which assembles everything
        /// but the values themselves.
        key: String,
        /// The referenced table the key is absent from.
        referenced_table: String,
    },
    /// A `MATCH FULL` key mixed null and non-null columns (23503). The primary
    /// message is the same as [`ForeignKeyViolationSide::KeyNotPresent`]'s. Only
    /// the `DETAIL` differs, and it names no key.
    MatchFullMixedNulls,
    /// `NO ACTION` refused a delete or key update of a row that is still
    /// referenced (23503): `Key (id)=(1) is still referenced from table "c".`
    StillReferenced {
        /// The `Key (id)=(1)` fragment, already rendered. See
        /// [`ForeignKeyViolationSide::render_key`].
        key: String,
        /// The referencing table that still holds a matching row. Named in both
        /// the message and the `DETAIL`.
        referencing_table: String,
    },
    /// `RESTRICT` refused it. This differs from
    /// [`ForeignKeyViolationSide::StillReferenced`] in three ways. The SQLSTATE
    /// is 23001 (`restrict_violation`), not 23503. The message says
    /// "violates RESTRICT setting of". The `DETAIL` says "is referenced", where
    /// `NO ACTION` says "is still referenced".
    Restricted {
        /// The `Key (id)=(1)` fragment, already rendered. See
        /// [`ForeignKeyViolationSide::render_key`].
        key: String,
        /// The referencing table that holds a matching row. Named in both the
        /// message and the `DETAIL`.
        referencing_table: String,
    },
}

/// The payload of [`ExecError::ForeignKeyTypeMismatch`], boxed to keep the error
/// enum narrow. Every field lands in the `DETAIL` line, and the primary message
/// names only the constraint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyTypeMismatch {
    /// The constraint being defined, quoted in the primary message.
    pub constraint: String,
    /// The referencing table's column, quoted in the `DETAIL`.
    pub referencing_column: String,
    /// The referenced table's column, quoted in the `DETAIL`.
    pub referenced_column: String,
    /// The referencing column's type name, unquoted in the `DETAIL`.
    pub referencing_type: String,
    /// The referenced column's type name, unquoted in the `DETAIL`.
    pub referenced_type: String,
}

/// The payload of [`ExecError::DependentForeignKeys`], boxed to keep the error
/// enum narrow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyDependents {
    /// The object the statement tried to drop.
    pub dropped: DroppedObject,
    /// The constraints that depend on it, one `DETAIL` line each, in the order
    /// the message should list them.
    pub dependents: Vec<DependentForeignKey>,
}

/// The object a refused drop named. `PostgreSQL` spells it out in the primary
/// message and again in every `DETAIL` line, and the two spellings differ for a
/// dropped constraint, so the choice cannot be reduced to one string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DroppedObject {
    /// `DROP TABLE`. Named `table p1` in both the message and the `DETAIL`.
    Table(String),
    /// `DROP INDEX`. Named `index uniq_a` in both.
    Index(String),
    /// `ALTER TABLE … DROP CONSTRAINT`. Named `constraint p_pkey on table p` in
    /// the message, but `index p_pkey` in the `DETAIL`, because the dependents
    /// hang off the constraint's backing index, not the constraint.
    Constraint { name: String, table: String },
}

/// One foreign key that blocks a drop, rendered as a single `DETAIL` line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependentForeignKey {
    /// The dependent constraint's name, unquoted in the `DETAIL`.
    pub constraint: String,
    /// The table that constraint is defined on, unquoted in the `DETAIL`.
    pub table: crabka_pgcatalog::RelationName,
}

impl DroppedObject {
    /// How the primary message names the object.
    fn describe(&self) -> String {
        match self {
            DroppedObject::Table(name) => format!("table {name}"),
            DroppedObject::Index(name) => format!("index {name}"),
            DroppedObject::Constraint { name, table } => {
                format!("constraint {name} on table {table}")
            }
        }
    }

    /// How a `DETAIL` line names the object its constraint depends on.
    fn depended_on(&self) -> String {
        match self {
            DroppedObject::Table(name) => format!("table {name}"),
            DroppedObject::Index(name) | DroppedObject::Constraint { name, .. } => {
                format!("index {name}")
            }
        }
    }
}

impl ForeignKeyViolationSide {
    /// Render the `Key (a, b)=(1, 2)` fragment every keyed foreign-key `DETAIL`
    /// line opens with, from the key's columns and their already-formatted
    /// values.
    ///
    /// The type layer owns the format of a value into its text representation,
    /// so this method takes text and only assembles it. Columns and values each
    /// join with `", "`, in the order the `FOREIGN KEY` clause writes them,
    /// which is not necessarily the referenced index's order. The result carries
    /// no trailing period. The sentence built around it supplies that.
    #[must_use]
    pub fn render_key<C: AsRef<str>, V: AsRef<str>>(columns: &[C], values: &[V]) -> String {
        let columns: Vec<&str> = columns.iter().map(AsRef::as_ref).collect();
        let values: Vec<&str> = values.iter().map(AsRef::as_ref).collect();
        format!("Key ({})=({})", columns.join(", "), values.join(", "))
    }
}

/// A SQL/JSON diagnostic with `PostgreSQL`'s optional `DETAIL` and `HINT` lines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlJsonError {
    pub sqlstate: &'static str,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

/// The 23503 message both child-side violations share.
fn referencing_row_message(table: &str, constraint: &str) -> PgError {
    PgError::error(
        "23503",
        format!(
            "insert or update on table \"{table}\" violates foreign key constraint \"{constraint}\""
        ),
    )
}

/// `PostgreSQL` attaches a standing HINT to every "does not exist" 42883, one
/// wording for functions and one for operators. It is attached here rather than
/// at each raise site so that the two always travel together — but only for a
/// message in PostgreSQL's own shape, because a message crabka phrases
/// differently is a divergence the HINT would merely decorate.
fn undefined_function_hint(message: &str) -> Option<&'static str> {
    // `could not identify an ordering operator for type X` is the one message
    // in this family PostgreSQL hints on; the equality and comparison-function
    // wordings carry none.
    if message.starts_with("could not identify an ordering operator for type ") {
        return Some("Use an explicit ordering operator or modify the query.");
    }
    if let Some(operands) = message.strip_prefix("operator does not exist: ") {
        // PostgreSQL words the hint in the singular for a PREFIX operator. Both
        // renderings put the operand types and the spelling in one
        // space-separated run, so a two-word tail is `<op> <type>` — the prefix
        // form — and a three-word tail is `<type> <op> <type>`.
        return Some(if operands.split_whitespace().count() <= 2 {
            "No operator matches the given name and argument type. You might need to add an \
             explicit type cast."
        } else {
            "No operator matches the given name and argument types. You might need to add \
             explicit type casts."
        });
    }
    // Only when the argument types are actually rendered. `function f(...) does
    // not exist` is our placeholder for a name we could not resolve at all, and
    // PostgreSQL never writes it -- it names every argument type. Hinting there
    // adds a second wrong line to an error that is usually wrong to begin with,
    // because the statement PostgreSQL runs successfully is one we cannot run.
    if message.starts_with("function ")
        && message.ends_with(" does not exist")
        && !message.contains("(...)")
    {
        return Some(
            "No function matches the given name and argument types. You might need to add \
             explicit type casts.",
        );
    }
    None
}

/// `PostgreSQL` attaches a standing HINT to the 42725 it raises when operator
/// resolution kept more than one candidate, and words it for an *operator*
/// rather than for a function. Like [`undefined_function_hint`] it is attached
/// once here so that the message and its HINT cannot drift apart, and only for
/// a message in PostgreSQL's own shape.
fn ambiguous_operator_hint(message: &str) -> Option<&'static str> {
    message.starts_with("operator is not unique: ").then_some(
        "Could not choose a best candidate operator. You might need to add explicit type casts.",
    )
}

impl ExecError {
    pub fn into_pg(self) -> PgError {
        match self {
            ExecError::CompatibilityRefusal(command) => {
                PgError::error(command.sqlstate(), command.message())
            }
            ExecError::Remote(error) => error,
            // A rejected `WITH (…)` list is raised from the parser but worded
            // the way `reloptions.c` words it, DETAIL and HINT included.
            ExecError::Parse(e) => {
                let rendered = PgError::error(e.sqlstate(), e.to_string());
                let rendered = match e.detail() {
                    Some(detail) => rendered.with_detail(detail),
                    None => rendered,
                };
                match e.hint() {
                    Some(hint) => rendered.with_hint(hint),
                    None => rendered,
                }
            }
            ExecError::Catalog(e) => {
                let rendered = PgError::error(e.sqlstate(), e.to_string());
                // The only catalog error PostgreSQL gives a DETAIL of its own.
                if matches!(e, crabka_pgcatalog::CatalogError::ReservedSchemaName(_)) {
                    rendered.with_detail(format!(
                        "The prefix \"{}\" is reserved for system schemas.",
                        crabka_pgcatalog::RESERVED_SCHEMA_PREFIX
                    ))
                } else {
                    rendered
                }
            }
            ExecError::Type(e) => {
                let rendered = PgError::error(e.sqlstate(), e.to_string());
                // `json_in`/`jsonb_in` are the only type-layer errors with a
                // CONTEXT: PostgreSQL prints the line of the document the lexer
                // stopped on, which is often the only way to find the mistake.
                let rendered = match e.context() {
                    Some(context) => rendered.with_context(context),
                    None => rendered,
                };
                let rendered = match e.detail() {
                    Some(detail) => rendered.with_detail(detail),
                    None => rendered,
                };
                match e.hint() {
                    Some(hint) => rendered.with_hint(hint),
                    None => rendered,
                }
            }
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
            ExecError::InvalidFromEntry { table, note } => {
                // `PostgreSQL` reaches this message from two checks with two
                // SQLSTATEs: `errorMissingRTE` (undefined_table) for a name that
                // never entered the namespace, `check_lateral_ref_ok`
                // (invalid_column_reference) for one that entered it and was
                // then disallowed.
                let sqlstate = match note {
                    FromEntryNote::MarkSubqueryLateral | FromEntryNote::TargetRelation => "42P01",
                    FromEntryNote::LateralTargetRelation | FromEntryNote::CombiningJoinType => {
                        "42P10"
                    }
                };
                let error = PgError::error(
                    sqlstate,
                    format!("invalid reference to FROM-clause entry for table \"{table}\""),
                );
                let unreachable = format!(
                    "There is an entry for table \"{table}\", but it cannot be referenced from \
                     this part of the query."
                );
                match note {
                    FromEntryNote::MarkSubqueryLateral => error
                        .with_detail(unreachable)
                        .with_hint("To reference that table, you must mark this subquery with LATERAL."),
                    FromEntryNote::TargetRelation => error.with_detail(unreachable),
                    FromEntryNote::LateralTargetRelation => error.with_hint(unreachable),
                    FromEntryNote::CombiningJoinType => error.with_detail(
                        "The combining JOIN type must be INNER or LEFT for a LATERAL reference.",
                    ),
                }
            }
            ExecError::InaccessibleColumn {
                column,
                table,
                lateral_would_help,
            } => {
                let error = PgError::error("42703", format!("column \"{column}\" does not exist"))
                    .with_detail(format!(
                        "There is a column named \"{column}\" in table \"{table}\", but it cannot \
                         be referenced from this part of the query."
                    ));
                if lateral_would_help {
                    error.with_hint(
                        "To reference that column, you must mark this subquery with LATERAL.",
                    )
                } else {
                    error
                }
            }
            ExecError::DuplicateAlias(t) => PgError::error(
                "42712",
                format!("table name \"{t}\" specified more than once"),
            ),
            ExecError::Unsupported(m) => PgError::error("0A000", m),
            ExecError::UnsupportedWithDetail { message, detail } => {
                PgError::error("0A000", message).with_detail(detail)
            }
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
            ExecError::UndefinedIndexColumn(column) => PgError::error(
                "42703",
                format!("column \"{column}\" named in key does not exist"),
            ),
            ExecError::WithoutOverlapsNeedsTwoColumns => PgError::error(
                "42601",
                "constraint using WITHOUT OVERLAPS needs at least two columns",
            ),
            ExecError::WithoutOverlapsNotRange(column) => PgError::error(
                "42804",
                format!(
                    "column \"{column}\" in WITHOUT OVERLAPS is not a range or multirange type"
                ),
            ),
            ExecError::EmptyWithoutOverlapsValue { column, relation } => PgError::error(
                "23514",
                format!(
                    "empty WITHOUT OVERLAPS value found in column \"{column}\" in relation \
                     \"{relation}\""
                ),
            ),
            ExecError::ForeignKeyPeriodMismatch { on_referencing } => {
                let (with, without) = if on_referencing {
                    ("referencing", "referenced")
                } else {
                    ("referenced", "referencing")
                };
                PgError::error(
                    "42830",
                    format!("foreign key uses PERIOD on the {with} table but not the {without} table"),
                )
            }
            ExecError::ForeignKeyNeedsPeriod => PgError::error(
                "42830",
                "foreign key must use PERIOD when referencing a primary key using WITHOUT OVERLAPS",
            ),
            ExecError::DuplicateColumn { column, table } => PgError::error(
                "42701",
                format!("column \"{column}\" of relation \"{table}\" already exists"),
            ),
            ExecError::DuplicateOutputColumn(column) => PgError::error(
                "42701",
                format!("column \"{column}\" specified more than once"),
            ),
            ExecError::SystemColumnName(column) => PgError::error(
                "42701",
                format!("column name \"{column}\" conflicts with a system column name"),
            ),
            ExecError::AssignSystemColumn(column) => PgError::error(
                "0A000",
                format!("cannot assign to system column \"{column}\""),
            ),
            ExecError::RepeatedMaintenanceColumn { column, table } => PgError::error(
                "42701",
                format!("column \"{column}\" of relation \"{table}\" appears more than once"),
            ),
            ExecError::DuplicateObject(m) => PgError::error("42710", m),
            ExecError::UndefinedObject(m) => PgError::error("42704", m),
            ExecError::NoSchemaSelected => {
                PgError::error("3F000", "no schema has been selected to create in")
            }
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
            ExecError::UndefinedFunction(m) => {
                let hint = undefined_function_hint(&m);
                let error = PgError::error("42883", m);
                match hint {
                    Some(hint) => error.with_hint(hint),
                    None => error,
                }
            }
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
            ExecError::ObjectInUse(message) => PgError::error("55006", message),
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
            ExecError::InvalidParameterValueMessage(m) => PgError::error("22023", m),
            ExecError::UnrecognizedParameter(n) => PgError::error(
                "42704",
                format!("unrecognized configuration parameter \"{n}\""),
            ),
            ExecError::UnknownTimeZone(zone) => {
                PgError::error("22023", format!("time zone \"{zone}\" not recognized"))
            }
            ExecError::NumericTimeZoneSyntax(zone) => PgError::error(
                "22023",
                format!("invalid input syntax for type numeric time zone: \"{zone}\""),
            )
            .with_hint("Numeric time zones must have \"-\" or \"+\" as first character."),
            ExecError::NumericTimeZoneOutOfRange(zone) => PgError::error(
                "22023",
                format!("numeric time zone \"{zone}\" out of range"),
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
            ExecError::ViewNotUpdatable {
                message,
                detail,
                hint,
            } => PgError::error("55000", message)
                .with_detail(detail)
                .with_hint(hint),
            ExecError::TruncateOnlyPartitioned => {
                PgError::error("42809", "cannot truncate only a partitioned table").with_hint(
                    "Do not specify the ONLY keyword, or use TRUNCATE ONLY on the partitions \
                     directly.",
                )
            }
            ExecError::MaterializedViewNotPopulated(relation) => PgError::error(
                "55000",
                format!("materialized view \"{relation}\" has not been populated"),
            )
            .with_hint("Use the REFRESH MATERIALIZED VIEW command."),
            ExecError::ViewColumnNotUpdatable { message, detail } => {
                PgError::error("0A000", message).with_detail(detail)
            }
            ExecError::CheckOptionUnsupported(hint) => PgError::error(
                "0A000",
                "WITH CHECK OPTION is supported only on automatically updatable views",
            )
            .with_hint(hint),
            ExecError::ViewCheckOptionViolation { view, row } => {
                let error = PgError::error(
                    "44000",
                    format!("new row violates check option for view \"{view}\""),
                );
                match row {
                    Some(row) => error.with_detail(format!("Failing row contains {row}.")),
                    None => error,
                }
            }
            ExecError::PermissionDenied { kind, relation } => {
                PgError::error("42501", format!("permission denied for {kind} {relation}"))
            }
            ExecError::EventTriggerPrivilege { message, hint } => {
                PgError::error("42501", message).with_hint(hint)
            }
            ExecError::RowSecurityRefused(relation) => PgError::error(
                "42501",
                format!(
                    "query would be affected by row-level security policy for table \"{relation}\""
                ),
            ),
            ExecError::RowSecurityCheckViolation {
                relation,
                policy,
                using_expression,
                target_row,
            } => {
                let subject = if target_row { "target" } else { "new" };
                let named = policy
                    .as_ref()
                    .map_or_else(String::new, |name| format!(" \"{name}\""));
                let using = if using_expression {
                    " (USING expression)"
                } else {
                    ""
                };
                PgError::error(
                    "42501",
                    format!(
                        "{subject} row violates row-level security policy{named}{using} for table \"{relation}\""
                    ),
                )
            }
            ExecError::PolicyRecursion(relation) => PgError::error(
                "42P17",
                format!("infinite recursion detected in policy for relation \"{relation}\""),
            ),
            ExecError::FunctionError { sqlstate, message } => {
                let hint = ambiguous_operator_hint(&message);
                let rendered = PgError::error(sqlstate, message);
                match hint {
                    Some(hint) => rendered.with_hint(hint),
                    None => rendered,
                }
            }
            ExecError::SqlJson(error) => {
                let mut rendered = PgError::error(error.sqlstate, error.message);
                if let Some(detail) = error.detail {
                    rendered = rendered.with_detail(detail);
                }
                match error.hint {
                    Some(hint) => rendered.with_hint(hint),
                    None => rendered,
                }
            }
            ExecError::GeneratedColumnWrite { message, column } => PgError::error("428C9", message)
                .with_detail(format!("Column \"{column}\" is a generated column.")),
            ExecError::NotAGeneratedColumn { column, table } => PgError::error(
                "42611",
                format!("column \"{column}\" of relation \"{table}\" is not a generated column"),
            ),
            ExecError::UnsupportedOnVirtualGenerated {
                subcommand,
                column,
                table,
            } => PgError::error("0A000", subcommand.message()).with_detail(format!(
                "Column \"{column}\" of relation \"{table}\" is a virtual generated column."
            )),
            ExecError::UndefinedPartitionKeyColumn(column) => PgError::error(
                "42703",
                format!("column \"{column}\" named in partition key does not exist"),
            ),
            ExecError::UnrecognizedPartitionStrategy(strategy) => PgError::error(
                "22023",
                format!("unrecognized partitioning strategy \"{strategy}\""),
            ),
            ExecError::NoPartitionForRow { relation, key } => {
                let error = PgError::error(
                    "23514",
                    format!("no partition of relation \"{relation}\" found for row"),
                );
                match key {
                    Some(key) => error
                        .with_detail(format!("Partition key of the failing row contains {key}.")),
                    None => error,
                }
            }
            ExecError::PartitionConstraintViolation { relation, row } => {
                let error = PgError::error(
                    "23514",
                    format!("new row for relation \"{relation}\" violates partition constraint"),
                );
                match row {
                    Some(row) => error.with_detail(format!("Failing row contains {row}.")),
                    None => error,
                }
            }
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
            ExecError::ChildColumnTypeMismatch { child, column } => PgError::error(
                "42804",
                format!("child table \"{child}\" has different type for column \"{column}\""),
            ),
            ExecError::ChildColumnCollationMismatch { child, column } => PgError::error(
                "42P21",
                format!("child table \"{child}\" has different collation for column \"{column}\""),
            ),
            ExecError::OnlyWouldSkipDescendants { message, hint } => {
                let error = PgError::error("42P16", message);
                match hint {
                    Some(hint) => error.with_hint(hint),
                    None => error,
                }
            }
            ExecError::NotPartitioned(relation) => {
                PgError::error("42P17", format!("\"{relation}\" is not partitioned"))
            }
            ExecError::ForeignKeyViolation(violation) => {
                let ForeignKeyViolation {
                    table,
                    constraint,
                    side,
                } = *violation;
                match side {
                    ForeignKeyViolationSide::KeyNotPresent {
                        key,
                        referenced_table,
                    } => referencing_row_message(&table, &constraint)
                        .with_detail(format!("{key} is not present in table \"{referenced_table}\".")),
                    ForeignKeyViolationSide::MatchFullMixedNulls => {
                        referencing_row_message(&table, &constraint).with_detail(
                            "MATCH FULL does not allow mixing of null and nonnull key values.",
                        )
                    }
                    ForeignKeyViolationSide::StillReferenced {
                        key,
                        referencing_table,
                    } => PgError::error(
                        "23503",
                        format!(
                            "update or delete on table \"{table}\" violates foreign key \
                             constraint \"{constraint}\" on table \"{referencing_table}\""
                        ),
                    )
                    .with_detail(format!(
                        "{key} is still referenced from table \"{referencing_table}\"."
                    )),
                    ForeignKeyViolationSide::Restricted {
                        key,
                        referencing_table,
                    } => PgError::error(
                        "23001",
                        format!(
                            "update or delete on table \"{table}\" violates RESTRICT setting of \
                             foreign key constraint \"{constraint}\" on table \"{referencing_table}\""
                        ),
                    )
                    .with_detail(format!(
                        "{key} is referenced from table \"{referencing_table}\"."
                    )),
                }
            }
            ExecError::NoUniqueConstraintForReferencedTable(table) => PgError::error(
                "42830",
                format!(
                    "there is no unique constraint matching given keys for referenced table \"{table}\""
                ),
            ),
            ExecError::ForeignKeyColumnCountMismatch => PgError::error(
                "42830",
                "number of referencing and referenced columns for foreign key disagree",
            ),
            ExecError::DuplicateForeignKeyReferencedColumn => PgError::error(
                "42830",
                "foreign key referenced-columns list must not contain duplicates",
            ),
            ExecError::ReferencedRelationNotATable(relation) => PgError::error(
                "42809",
                format!("referenced relation \"{relation}\" is not a table"),
            ),
            ExecError::UndefinedForeignKeyColumn(column) => PgError::error(
                "42703",
                format!("column \"{column}\" referenced in foreign key constraint does not exist"),
            ),
            ExecError::ForeignKeyTypeMismatch(mismatch) => {
                let ForeignKeyTypeMismatch {
                    constraint,
                    referencing_column,
                    referenced_column,
                    referencing_type,
                    referenced_type,
                } = *mismatch;
                PgError::error(
                    "42804",
                    format!("foreign key constraint \"{constraint}\" cannot be implemented"),
                )
                .with_detail(format!(
                    "Key columns \"{referencing_column}\" of the referencing table and \
                     \"{referenced_column}\" of the referenced table are of incompatible types: \
                     {referencing_type} and {referenced_type}."
                ))
            }
            ExecError::ForeignKeySetColumnNotInKey(column) => PgError::error(
                "42P10",
                format!(
                    "column \"{column}\" referenced in ON DELETE SET action must be part of foreign key"
                ),
            ),
            ExecError::DuplicateConstraint { name, table } => PgError::error(
                "42710",
                format!("constraint \"{name}\" for relation \"{table}\" already exists"),
            ),
            ExecError::TruncateReferencedByForeignKey {
                referencing_table,
                referenced_table,
            } => PgError::error(
                "0A000",
                "cannot truncate a table referenced in a foreign key constraint",
            )
            .with_detail(format!(
                "Table \"{referencing_table}\" references \"{referenced_table}\"."
            ))
            .with_hint(format!(
                "Truncate table \"{referencing_table}\" at the same time, or use TRUNCATE ... CASCADE."
            )),
            ExecError::DependentForeignKeys(blocked) => {
                let ForeignKeyDependents {
                    dropped,
                    dependents,
                } = *blocked;
                let depended_on = dropped.depended_on();
                let detail = dependents
                    .iter()
                    .map(|dependent| {
                        format!(
                            "constraint {} on table {} depends on {depended_on}",
                            dependent.constraint, dependent.table
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                PgError::error(
                    "2BP01",
                    format!(
                        "cannot drop {} because other objects depend on it",
                        dropped.describe()
                    ),
                )
                .with_detail(detail)
                .with_hint("Use DROP ... CASCADE to drop the dependent objects too.")
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
    use crabka_pgcatalog::RelationName;

    use super::*;

    /// Each row is (error, expected SQLSTATE, expected PG-exact message). The
    /// conformance harness diffs the ON CONFLICT rows against a real PostgreSQL
    /// oracle, so their texts are byte-for-byte PG's.
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
            (
                ExecError::UndefinedIndexColumn("missing".into()),
                "42703",
                "column \"missing\" named in key does not exist",
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

    fn violation(table: &str, constraint: &str, side: ForeignKeyViolationSide) -> ExecError {
        ExecError::ForeignKeyViolation(Box::new(ForeignKeyViolation {
            table: table.into(),
            constraint: constraint.into(),
            side,
        }))
    }

    /// Every message, `DETAIL` and `HINT` below comes from a live PostgreSQL
    /// 18.4, so this test compares each row as a whole [`PgError`]. The severity
    /// and the absence or presence of the secondary fields are part of what has
    /// to match.
    #[test]
    fn foreign_key_errors_map_to_sqlstate_message_detail_and_hint() {
        let cases: Vec<(ExecError, PgError)> = vec![
            (
                violation(
                    "c12",
                    "c12_a_b_fkey",
                    ForeignKeyViolationSide::KeyNotPresent {
                        key: "Key (a, b)=(1, 1)".into(),
                        referenced_table: "p".into(),
                    },
                ),
                PgError::error(
                    "23503",
                    "insert or update on table \"c12\" violates foreign key constraint \
                     \"c12_a_b_fkey\"",
                )
                .with_detail("Key (a, b)=(1, 1) is not present in table \"p\"."),
            ),
            (
                violation(
                    "cfull",
                    "cfull_a_b_fkey",
                    ForeignKeyViolationSide::MatchFullMixedNulls,
                ),
                PgError::error(
                    "23503",
                    "insert or update on table \"cfull\" violates foreign key constraint \
                     \"cfull_a_b_fkey\"",
                )
                .with_detail("MATCH FULL does not allow mixing of null and nonnull key values."),
            ),
            (
                violation(
                    "p1",
                    "cdel_a_fkey",
                    ForeignKeyViolationSide::StillReferenced {
                        key: "Key (id)=(1)".into(),
                        referencing_table: "cdel".into(),
                    },
                ),
                PgError::error(
                    "23503",
                    "update or delete on table \"p1\" violates foreign key constraint \
                     \"cdel_a_fkey\" on table \"cdel\"",
                )
                .with_detail("Key (id)=(1) is still referenced from table \"cdel\"."),
            ),
            (
                violation(
                    "pre",
                    "cre_a_fkey",
                    ForeignKeyViolationSide::Restricted {
                        key: "Key (id)=(1)".into(),
                        referencing_table: "cre".into(),
                    },
                ),
                PgError::error(
                    "23001",
                    "update or delete on table \"pre\" violates RESTRICT setting of foreign key \
                     constraint \"cre_a_fkey\" on table \"cre\"",
                )
                .with_detail("Key (id)=(1) is referenced from table \"cre\"."),
            ),
            (
                ExecError::NoUniqueConstraintForReferencedTable("nopk".into()),
                PgError::error(
                    "42830",
                    "there is no unique constraint matching given keys for referenced table \
                     \"nopk\"",
                ),
            ),
            (
                ExecError::ForeignKeyColumnCountMismatch,
                PgError::error(
                    "42830",
                    "number of referencing and referenced columns for foreign key disagree",
                ),
            ),
            (
                ExecError::DuplicateForeignKeyReferencedColumn,
                PgError::error(
                    "42830",
                    "foreign key referenced-columns list must not contain duplicates",
                ),
            ),
            (
                ExecError::ReferencedRelationNotATable("v".into()),
                PgError::error("42809", "referenced relation \"v\" is not a table"),
            ),
            (
                ExecError::UndefinedForeignKeyColumn("nope".into()),
                PgError::error(
                    "42703",
                    "column \"nope\" referenced in foreign key constraint does not exist",
                ),
            ),
            (
                ExecError::ForeignKeyTypeMismatch(Box::new(ForeignKeyTypeMismatch {
                    constraint: "c6_a_fkey".into(),
                    referencing_column: "a".into(),
                    referenced_column: "id".into(),
                    referencing_type: "text".into(),
                    referenced_type: "integer".into(),
                })),
                PgError::error(
                    "42804",
                    "foreign key constraint \"c6_a_fkey\" cannot be implemented",
                )
                .with_detail(
                    "Key columns \"a\" of the referencing table and \"id\" of the referenced \
                     table are of incompatible types: text and integer.",
                ),
            ),
            (
                ExecError::ForeignKeySetColumnNotInKey("b".into()),
                PgError::error(
                    "42P10",
                    "column \"b\" referenced in ON DELETE SET action must be part of foreign key",
                ),
            ),
            (
                ExecError::DuplicateConstraint {
                    name: "dupname".into(),
                    table: "dup".into(),
                },
                PgError::error(
                    "42710",
                    "constraint \"dupname\" for relation \"dup\" already exists",
                ),
            ),
            (
                ExecError::TruncateReferencedByForeignKey {
                    referencing_table: "cdel".into(),
                    referenced_table: "p1".into(),
                },
                PgError::error(
                    "0A000",
                    "cannot truncate a table referenced in a foreign key constraint",
                )
                .with_detail("Table \"cdel\" references \"p1\".")
                .with_hint(
                    "Truncate table \"cdel\" at the same time, or use TRUNCATE ... CASCADE.",
                ),
            ),
            (
                ExecError::DependentForeignKeys(Box::new(ForeignKeyDependents {
                    dropped: DroppedObject::Table("p1".into()),
                    dependents: vec![DependentForeignKey {
                        constraint: "cdel_a_fkey".into(),
                        table: RelationName::public("cdel"),
                    }],
                })),
                PgError::error(
                    "2BP01",
                    "cannot drop table p1 because other objects depend on it",
                )
                .with_detail("constraint cdel_a_fkey on table cdel depends on table p1")
                .with_hint("Use DROP ... CASCADE to drop the dependent objects too."),
            ),
            (
                ExecError::DependentForeignKeys(Box::new(ForeignKeyDependents {
                    dropped: DroppedObject::Index("uniqidx_a_uq".into()),
                    dependents: vec![DependentForeignKey {
                        constraint: "c11_a_fkey".into(),
                        table: RelationName::public("c11"),
                    }],
                })),
                PgError::error(
                    "2BP01",
                    "cannot drop index uniqidx_a_uq because other objects depend on it",
                )
                .with_detail("constraint c11_a_fkey on table c11 depends on index uniqidx_a_uq")
                .with_hint("Use DROP ... CASCADE to drop the dependent objects too."),
            ),
            (
                // Two dependents share one DETAIL, one per line, and a dropped
                // constraint is reported as its backing index there.
                ExecError::DependentForeignKeys(Box::new(ForeignKeyDependents {
                    dropped: DroppedObject::Constraint {
                        name: "p_pkey".into(),
                        table: "p".into(),
                    },
                    dependents: vec![
                        DependentForeignKey {
                            constraint: "c12_a_b_fkey".into(),
                            table: RelationName::public("c12"),
                        },
                        DependentForeignKey {
                            constraint: "cfull_a_b_fkey".into(),
                            table: RelationName::public("cfull"),
                        },
                    ],
                })),
                PgError::error(
                    "2BP01",
                    "cannot drop constraint p_pkey on table p because other objects depend on it",
                )
                .with_detail(
                    "constraint c12_a_b_fkey on table c12 depends on index p_pkey\n\
                     constraint cfull_a_b_fkey on table cfull depends on index p_pkey",
                )
                .with_hint("Use DROP ... CASCADE to drop the dependent objects too."),
            ),
        ];

        for (error, expected) in cases {
            let pg = error.clone().into_pg();
            assert!(pg == expected, "unexpected wire error for {error:?}");
        }
    }

    /// DETAIL and HINT exist for the foreign-key errors only. This wave does
    /// not widen them to the rest of the executor's errors.
    #[test]
    fn non_foreign_key_errors_carry_no_detail_or_hint() {
        let cases = vec![
            ExecError::Unsupported("cannot truncate".into()),
            ExecError::DependentObjectsStillExist(
                "cannot drop view v because other objects depend on it".into(),
            ),
            ExecError::UniqueViolation("t_pkey".into()),
            ExecError::CheckViolation {
                table: "t".into(),
                constraint: "t_a_check".into(),
            },
        ];

        for error in cases {
            let pg = error.clone().into_pg();
            assert!(
                pg.diagnostics
                    .as_deref()
                    .is_none_or(|fields| fields.detail.is_none() && fields.hint.is_none()),
                "unexpected secondary fields for {error:?}"
            );
        }
    }

    #[test]
    fn key_fragment_renders_single_and_composite_keys() {
        assert!(ForeignKeyViolationSide::render_key(&["id"], &["1"]) == "Key (id)=(1)");
        assert!(
            ForeignKeyViolationSide::render_key(&["a", "b"], &["1", "1"]) == "Key (a, b)=(1, 1)"
        );
        // Column order follows the FOREIGN KEY clause, not the referenced index.
        assert!(
            ForeignKeyViolationSide::render_key(&["b".to_string(), "a".to_string()], &["1", "2"])
                == "Key (b, a)=(1, 2)"
        );
    }
}
